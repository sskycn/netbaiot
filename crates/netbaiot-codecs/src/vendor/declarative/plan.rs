use super::*;
use crate::common::valid_text;
use crate::vendor::numeric::{Decimal, FloatConversion, IntegerConversion};
use std::{collections::BTreeMap, ops::Range};
use wire::{Check, Integer, Value};

pub(super) struct Layout {
    pub size: usize,
    reserved: Vec<Range<usize>>,
    checksum: Option<Check>,
}
impl Layout {
    pub fn wire_length(&self, adjustment: i32) -> Result<u64, CodecError> {
        u64::try_from(self.size as i64 - i64::from(adjustment)).map_err(|_| CodecError)
    }
    pub fn validate(
        &self,
        bytes: &[u8],
        length: Option<&(Integer, i32)>,
    ) -> Result<(), CodecError> {
        if bytes.len() != self.size {
            return Err(CodecError);
        }
        if let Some((field, adjustment)) = length
            && field.read(bytes)? != self.wire_length(*adjustment)?
        {
            return Err(CodecError);
        }
        for range in &self.reserved {
            if bytes
                .get(range.clone())
                .ok_or(CodecError)?
                .iter()
                .any(|b| *b != 0)
            {
                return Err(CodecError);
            }
        }
        if let Some(check) = &self.checksum
            && check.value.read(bytes)? != check.calculate(bytes)?
        {
            return Err(CodecError);
        }
        Ok(())
    }
    pub fn write_checksum(&self, bytes: &mut [u8]) -> Result<(), CodecError> {
        if let Some(check) = &self.checksum {
            check.value.write(bytes, check.calculate(bytes)?)?;
        }
        Ok(())
    }
}

pub(super) enum Kind {
    Telemetry(Vec<(String, Value)>),
    Heartbeat(Integer),
    Event(String, Option<Value>),
    CommandAck {
        id: Range<usize>,
        execution: Integer,
        states: [u64; 3],
    },
}
impl Kind {
    pub fn read(&self, bytes: &[u8]) -> Result<DeviceEventKind, CodecError> {
        Ok(match self {
            Self::Telemetry(fields) => {
                let mut values = BTreeMap::new();
                for (name, field) in fields {
                    values.insert(name.clone(), field.read(bytes)?);
                }
                DeviceEventKind::Telemetry(values)
            }
            Self::Heartbeat(field) => DeviceEventKind::Heartbeat(Heartbeat {
                sequence: field.read(bytes)?,
            }),
            Self::Event(name, field) => DeviceEventKind::DeviceEvent(DeviceEventPayload {
                name: name.clone(),
                value: field.as_ref().map(|f| f.read(bytes)).transpose()?,
            }),
            Self::CommandAck {
                id,
                execution,
                states,
            } => {
                let value = execution.read(bytes)?;
                let execution = if value == states[0] {
                    ExecutionState::Running
                } else if value == states[1] {
                    ExecutionState::Succeeded
                } else if value == states[2] {
                    ExecutionState::Failed
                } else {
                    return Err(CodecError);
                };
                DeviceEventKind::CommandAck(CommandAck {
                    command_id: CommandId(
                        uuid::Uuid::from_slice(bytes.get(id.clone()).ok_or(CodecError)?)
                            .map_err(|_| CodecError)?,
                    ),
                    execution,
                })
            }
        })
    }
}
pub(super) struct MessagePlan {
    pub tag: u64,
    pub layout: Layout,
    pub source: Value,
    pub occurred_at: Option<Integer>,
    pub kind: Kind,
}
pub(super) struct CommandPlan {
    pub name: String,
    pub tag: u64,
    pub layout: Layout,
    pub command_id: Range<usize>,
    pub expires_at: Option<Integer>,
    pub fields: Vec<(String, Value)>,
}

// Startup-only builder. Check before allocating a plan, and never allocate a frame-sized bitmap.
struct Builder<'a> {
    size: usize,
    occupied: Vec<Range<usize>>,
    limits: &'a CodecLimits,
    budget: usize,
}
impl<'a> Builder<'a> {
    fn new(
        config: &DeclarativeConfig,
        size: usize,
        limits: &'a CodecLimits,
        path: &str,
    ) -> Result<Self, ConfigError> {
        if size == 0
            || size < config.header.len()
            || size > MAX_FRAME_BYTES
            || size > limits.input_bytes
            || size > limits.decoded_bytes
        {
            return Err(ConfigError::new(
                format!("{path}.size"),
                "Layout size exceeds codec byte limits or is zero.",
            ));
        }
        let mut b = Self {
            size,
            occupied: Vec::new(),
            limits,
            budget: 512,
        };
        b.claim(0, config.header.len())
            .map_err(|e| e.at("header"))?;
        b.integer(&config.message_type)
            .map_err(|e| e.at("message_type"))?;
        if let Some(length) = &config.length {
            b.integer(&length.field).map_err(|e| e.at("length.field"))?;
            let n = u64::try_from(size as i64 - i64::from(length.adjustment)).map_err(|_| {
                ConfigError::new("length.adjustment", "Length adjustment is out of range.")
            })?;
            if !length.field.data_type.fits(n) {
                return Err(ConfigError::new(
                    "length.adjustment",
                    "Adjusted length does not fit the length field.",
                ));
            }
        }
        Ok(b)
    }
    fn charge(&mut self, bytes: usize) -> Result<(), ConfigError> {
        self.budget = self
            .budget
            .checked_add(bytes)
            .ok_or_else(|| ConfigError::new("", "Layout arithmetic exceeds its bound."))?;
        if self.budget > self.limits.decoded_bytes {
            return Err(ConfigError::new(
                "",
                "Compiled layout exceeds decoded byte budget.",
            ));
        }
        Ok(())
    }
    fn claim(&mut self, start: usize, len: usize) -> Result<Range<usize>, ConfigError> {
        let end = start
            .checked_add(len)
            .ok_or_else(|| ConfigError::new("offset", "Field end overflows."))?;
        if len == 0
            || end > self.size
            || self.occupied.iter().any(|r| start < r.end && r.start < end)
        {
            return Err(ConfigError::new(
                "offset",
                "Field is empty, overlaps another field, or exceeds the layout.",
            ));
        }
        let range = start..end;
        self.occupied.push(range.clone());
        Ok(range)
    }
    fn integer(&mut self, f: &IntegerField) -> Result<Integer, ConfigError> {
        Ok(Integer {
            range: self.claim(f.offset, f.data_type.width())?,
            endian: f.endian,
        })
    }
    fn value(&mut self, f: &Field) -> Result<Value, ConfigError> {
        let width = match f.data_type {
            DataType::U8 | DataType::I8 | DataType::Bool => 1,
            DataType::U16 | DataType::I16 => 2,
            DataType::U32 | DataType::I32 | DataType::F32 => 4,
            DataType::U64 | DataType::I64 | DataType::F64 => 8,
            DataType::Utf8 => {
                let n = f.length.ok_or_else(|| {
                    ConfigError::new("length", "UTF-8 requires an exact byte length.")
                })?;
                if n == 0 || n > self.limits.field_bytes.min(MAX_FIELD_BYTES) {
                    return Err(ConfigError::new(
                        "length",
                        "UTF-8 length exceeds field limits or is zero.",
                    ));
                }
                n
            }
        };
        if !matches!(f.data_type, DataType::Utf8) && f.length.is_some() {
            return Err(ConfigError::new(
                "length",
                "Only UTF-8 fields have a configurable length.",
            ));
        }
        let integer_bounds = match f.data_type {
            DataType::U8 => Some((0, i128::from(u8::MAX))),
            DataType::U16 => Some((0, i128::from(u16::MAX))),
            DataType::U32 => Some((0, i128::from(u32::MAX))),
            DataType::U64 => Some((0, i128::from(u64::MAX))),
            DataType::I8 => Some((i128::from(i8::MIN), i128::from(i8::MAX))),
            DataType::I16 => Some((i128::from(i16::MIN), i128::from(i16::MAX))),
            DataType::I32 => Some((i128::from(i32::MIN), i128::from(i32::MAX))),
            DataType::I64 => Some((i128::from(i64::MIN), i128::from(i64::MAX))),
            _ => None,
        };
        if integer_bounds.is_some() {
            if f.scale.is_some() {
                return Err(ConfigError::new(
                    "scale",
                    "Integer fields no longer support floating-point scale; migrate to conversion with mode decimal_exact and string scale/bias values.",
                ));
            }
            if f.bias.is_some() {
                return Err(ConfigError::new(
                    "bias",
                    "Integer fields no longer support floating-point bias; migrate to conversion with mode decimal_exact and string scale/bias values.",
                ));
            }
        }
        let float_scale = f.scale.unwrap_or(1.0);
        let float_bias = f.bias.unwrap_or(0.0);
        if integer_bounds.is_none() && (!float_scale.is_finite() || float_scale == 0.0) {
            return Err(ConfigError::new(
                "scale",
                "Scale must be finite and nonzero.",
            ));
        }
        if integer_bounds.is_none() && !float_bias.is_finite() {
            return Err(ConfigError::new("bias", "Bias must be finite."));
        }
        if matches!(f.data_type, DataType::Bool | DataType::Utf8) {
            if f.conversion.is_some() {
                return Err(ConfigError::new(
                    "conversion",
                    "Non-numeric fields cannot have numeric conversion settings.",
                ));
            }
            if float_scale != 1.0 {
                return Err(ConfigError::new(
                    "scale",
                    "Non-numeric fields cannot be scaled.",
                ));
            }
            if float_bias != 0.0 {
                return Err(ConfigError::new(
                    "bias",
                    "Non-numeric fields cannot have a bias.",
                ));
            }
        }
        let integer = integer_bounds
            .map(|(minimum, maximum)| {
                let (scale, bias) = if let Some(conversion) = &f.conversion {
                    let scale = Decimal::parse_parameter(&conversion.scale)
                        .map_err(|message| ConfigError::new("conversion.scale", message))?;
                    let bias = Decimal::parse_parameter(&conversion.bias)
                        .map_err(|message| ConfigError::new("conversion.bias", message))?;
                    (scale, bias)
                } else {
                    (Decimal::ONE, Decimal::ZERO)
                };
                IntegerConversion::compile(scale, bias, minimum, maximum)
                    .map_err(|message| ConfigError::new("conversion", message))
            })
            .transpose()?;
        let float = match f.data_type {
            DataType::F32 | DataType::F64 => {
                if f.conversion.is_some() {
                    return Err(ConfigError::new(
                        "conversion",
                        "Exact decimal conversion is supported only for integer wire types.",
                    ));
                }
                Some(
                    FloatConversion::compile(float_scale, float_bias)
                        .map_err(|message| ConfigError::new("scale", message))?,
                )
            }
            _ => None,
        };
        self.charge(128 + 3 * width)?;
        Ok(Value {
            range: self.claim(f.offset, width)?,
            data_type: f.data_type,
            endian: f.endian,
            integer,
            float,
        })
    }
    fn name(&mut self, name: &str) -> Result<String, ConfigError> {
        if name.is_empty() || !valid_text(name, self.limits.field_bytes.min(MAX_FIELD_BYTES)) {
            return Err(ConfigError::new("", "Invalid binary payload layout."));
        }
        self.charge(3 * name.len())?;
        Ok(name.to_owned())
    }
    fn fields(&mut self, fields: &[NamedField]) -> Result<Vec<(String, Value)>, ConfigError> {
        if fields.len() > self.limits.fields.min(MAX_FIELDS) {
            return Err(ConfigError::new("", "Invalid binary payload layout."));
        }
        let mut compiled = Vec::with_capacity(fields.len());
        for (index, f) in fields.iter().enumerate() {
            let path = format!("[{index}]");
            if fields[..index].iter().any(|other| other.name == f.name) {
                return Err(ConfigError::new(
                    format!("{path}.name"),
                    "Duplicate field name.",
                ));
            }
            compiled.push((
                self.name(&f.name)
                    .map_err(|e| e.at(&format!("{path}.name")))?,
                self.value(&f.field)
                    .map_err(|e| e.at(&format!("{path}.field")))?,
            ));
        }
        compiled.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Ok(compiled)
    }
    fn finish(
        mut self,
        checksum: &Option<Checksum>,
        total: &mut usize,
    ) -> Result<Layout, ConfigError> {
        let checksum = checksum
            .as_ref()
            .map(|c| {
                let width = match c.algorithm {
                    ChecksumAlgorithm::Crc16Modbus => 2,
                    _ => 1,
                };
                let range = self.claim(c.offset, width).map_err(|e| e.at("checksum"))?;
                if c.start >= c.end
                    || c.end > self.size
                    || (c.start < range.end && range.start < c.end)
                {
                    return Err(ConfigError::new(
                        "checksum",
                        "Checksum coverage is empty, out of bounds, or overlaps its own bytes.",
                    ));
                }
                Ok(Check {
                    value: Integer {
                        range,
                        endian: c.endian,
                    },
                    covered: c.start..c.end,
                    algorithm: c.algorithm,
                })
            })
            .transpose()?;
        // Bound cumulative retained plan structures, including metadata and reserved ranges.
        *total = total
            .checked_add(self.budget + self.occupied.len() * 64)
            .ok_or_else(|| ConfigError::new("", "Layout arithmetic exceeds its bound."))?;
        if *total > MAX_CONFIG_BYTES {
            return Err(ConfigError::new("", "Invalid binary payload layout."));
        }
        self.occupied.sort_unstable_by_key(|r| r.start);
        let mut reserved = Vec::new();
        let mut end = 0;
        for r in self.occupied {
            if end < r.start {
                reserved.push(end..r.start);
            }
            end = r.end;
        }
        if end < self.size {
            reserved.push(end..self.size);
        }
        Ok(Layout {
            size: self.size,
            reserved,
            checksum,
        })
    }
}

impl DeclarativeCodec {
    /// Validate all layouts, compile checked ranges, sort dispatch tables and discard JSON.
    pub fn compile(config: &DeclarativeConfig, limits: CodecLimits) -> Result<Self, CodecError> {
        Self::compile_diagnostic(config, limits).map_err(|_| CodecError)
    }

    /// Same compiler as `compile`, with schema-only paths for configuration tools.
    pub fn compile_diagnostic(
        config: &DeclarativeConfig,
        limits: CodecLimits,
    ) -> Result<Self, ConfigError> {
        if config.version == 0 {
            return Err(ConfigError::new(
                "version",
                "Codec version must be nonzero.",
            ));
        }
        super::super::vendor_id(&config.id, config.version)
            .map_err(|_| ConfigError::new("id", "Codec ID must be a valid vendor ID."))?;
        if config.header.is_empty() || config.header.len() > 64 {
            return Err(ConfigError::new(
                "header",
                "Header length must be between 1 and 64 bytes.",
            ));
        }
        if config.messages.is_empty() || config.messages.len() > MAX_LAYOUTS {
            return Err(ConfigError::new(
                "messages",
                "At least one message and at most 64 layouts are required.",
            ));
        }
        if config.commands.len() > MAX_LAYOUTS
            || config.messages.len() + config.commands.len() > MAX_LAYOUTS
        {
            return Err(ConfigError::new(
                "commands",
                "Combined message and command layout count exceeds 64.",
            ));
        }
        if limits.output_messages == 0
            || limits.fields == 0
            || limits.field_bytes == 0
            || limits.decoded_bytes < 512
        {
            return Err(ConfigError::new(
                "",
                "Codec resource limits are insufficient.",
            ));
        }
        let integer = |f: &IntegerField, path: &str| -> Result<Integer, ConfigError> {
            Ok(Integer {
                range: f.offset..f.offset.checked_add(f.data_type.width()).ok_or_else(|| {
                    ConfigError::new(format!("{path}.offset"), "Field end overflows.")
                })?,
                endian: f.endian,
            })
        };
        let message_type = integer(&config.message_type, "message_type")?;
        let length = config
            .length
            .as_ref()
            .map(|l| Ok((integer(&l.field, "length.field")?, l.adjustment)))
            .transpose()?;
        let mut total = config.header.len();
        let mut messages = Vec::with_capacity(config.messages.len());
        for (index, m) in config.messages.iter().enumerate() {
            let path = format!("messages[{index}]");
            let mut b = Builder::new(config, m.size, &limits, &path)?;
            let plan = (|| -> Result<MessagePlan, ConfigError> {
                if !config.message_type.data_type.fits(m.tag)
                    || config.messages[..index]
                        .iter()
                        .any(|other| other.tag == m.tag)
                {
                    return Err(ConfigError::new(
                        "tag",
                        "Message tag is duplicated or does not fit its integer type.",
                    ));
                }
                let s = &m.source_message_id;
                if !matches!(
                    s.data_type,
                    DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::Utf8
                ) {
                    return Err(ConfigError::new(
                        "source_message_id.type",
                        "Source IDs require an unsigned integer or UTF-8.",
                    ));
                }
                if s.scale.is_some_and(|scale| scale != 1.0) {
                    return Err(ConfigError::new(
                        "source_message_id.scale",
                        "Source IDs cannot be scaled.",
                    ));
                }
                if s.bias.is_some_and(|bias| bias != 0.0) {
                    return Err(ConfigError::new(
                        "source_message_id.bias",
                        "Source IDs cannot have a bias.",
                    ));
                }
                if s.conversion.is_some() {
                    return Err(ConfigError::new(
                        "source_message_id.conversion",
                        "Source IDs cannot have a numeric conversion.",
                    ));
                }
                if s.length.is_some_and(|n| n > 64) {
                    return Err(ConfigError::new(
                        "source_message_id.length",
                        "Source IDs are limited to 64 bytes.",
                    ));
                }
                let source = b.value(s).map_err(|e| e.at("source_message_id"))?;
                let occurred_at = m
                    .occurred_at
                    .as_ref()
                    .map(|f| b.integer(f).map_err(|e| e.at("occurred_at")))
                    .transpose()?;
                let kind = match &m.payload {
                    MessagePayload::Telemetry { fields } => {
                        if fields.is_empty() {
                            return Err(ConfigError::new(
                                "payload.fields",
                                "Telemetry requires at least one field.",
                            ));
                        }
                        Kind::Telemetry(b.fields(fields).map_err(|e| e.at("payload.fields"))?)
                    }
                    MessagePayload::Heartbeat { sequence } => {
                        Kind::Heartbeat(b.integer(sequence).map_err(|e| e.at("payload.sequence"))?)
                    }
                    MessagePayload::Event { name, value } => Kind::Event(
                        b.name(name).map_err(|e| e.at("payload.name"))?,
                        value
                            .as_ref()
                            .map(|f| b.value(f).map_err(|e| e.at("payload.value")))
                            .transpose()?,
                    ),
                    MessagePayload::CommandAck {
                        command_id_offset,
                        execution,
                        running,
                        succeeded,
                        failed,
                    } => {
                        if running == succeeded
                            || running == failed
                            || succeeded == failed
                            || [running, succeeded, failed]
                                .into_iter()
                                .any(|n| !execution.data_type.fits(*n))
                        {
                            return Err(ConfigError::new(
                                "payload.execution",
                                "Execution values must be distinct and fit their integer type.",
                            ));
                        }
                        Kind::CommandAck {
                            id: b.claim(*command_id_offset, 16).map_err(|_| {
                                ConfigError::new(
                                    "payload.command_id_offset",
                                    "Command UUID overlaps or exceeds the layout.",
                                )
                            })?,
                            execution: b
                                .integer(execution)
                                .map_err(|e| e.at("payload.execution"))?,
                            states: [*running, *succeeded, *failed],
                        }
                    }
                };
                Ok(MessagePlan {
                    tag: m.tag,
                    source,
                    occurred_at,
                    kind,
                    layout: b.finish(&m.checksum, &mut total)?,
                })
            })()
            .map_err(|e| e.at(&path))?;
            messages.push(plan);
        }
        messages.sort_unstable_by_key(|m| m.tag);
        let mut commands = Vec::with_capacity(config.commands.len());
        for (index, c) in config.commands.iter().enumerate() {
            let path = format!("commands[{index}]");
            let mut b = Builder::new(config, c.size, &limits, &path)?;
            let plan = (|| -> Result<CommandPlan, ConfigError> {
                if !config.message_type.data_type.fits(c.tag) || config.commands[..index].iter().any(|other| other.tag == c.tag) {
                    return Err(ConfigError::new("tag", "Command tag is duplicated or does not fit its integer type."));
                }
                if config.commands[..index].iter().any(|other| other.name == c.name) {
                    return Err(ConfigError::new("name", "Duplicate command name."));
                }
                // Expiry is an absolute nonnegative i64 millisecond timestamp, not a TTL.
                if c.expires_at.as_ref().is_some_and(|f| !f.data_type.fits(i64::MAX as u64)) {
                    return Err(ConfigError::new("expires_at.type", "Expiry requires u64 to represent the complete nonnegative i64 millisecond range."));
                }
                Ok(CommandPlan {
                    name: b.name(&c.name).map_err(|e| e.at("name"))?, tag: c.tag,
                    command_id: b.claim(c.command_id_offset, 16).map_err(|_| ConfigError::new("command_id_offset", "Command UUID overlaps or exceeds the layout."))?,
                    expires_at: c.expires_at.as_ref().map(|f| b.integer(f).map_err(|e| e.at("expires_at"))).transpose()?,
                    fields: b.fields(&c.fields).map_err(|e| e.at("fields"))?,
                    layout: b.finish(&c.checksum, &mut total)?,
                })
            })().map_err(|e| e.at(&path))?;
            commands.push(plan);
        }
        commands.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        Ok(Self {
            limits,
            header: config.header.clone().into_boxed_slice(),
            message_type,
            length,
            messages,
            commands,
        })
    }
}
