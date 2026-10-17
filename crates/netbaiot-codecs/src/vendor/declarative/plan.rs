use super::*;
use crate::common::valid_text;
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
    ) -> Result<Self, CodecError> {
        if size == 0
            || size > MAX_FRAME_BYTES
            || size > limits.input_bytes
            || size > limits.decoded_bytes
        {
            return Err(CodecError);
        }
        let mut b = Self {
            size,
            occupied: Vec::new(),
            limits,
            budget: 512,
        };
        b.claim(0, config.header.len())?;
        b.integer(&config.message_type)?;
        if let Some(length) = &config.length {
            b.integer(&length.field)?;
            let n = u64::try_from(size as i64 - i64::from(length.adjustment))
                .map_err(|_| CodecError)?;
            if !length.field.data_type.fits(n) {
                return Err(CodecError);
            }
        }
        Ok(b)
    }
    fn charge(&mut self, bytes: usize) -> Result<(), CodecError> {
        self.budget = self.budget.checked_add(bytes).ok_or(CodecError)?;
        if self.budget > self.limits.decoded_bytes {
            return Err(CodecError);
        }
        Ok(())
    }
    fn claim(&mut self, start: usize, len: usize) -> Result<Range<usize>, CodecError> {
        let end = start.checked_add(len).ok_or(CodecError)?;
        if len == 0
            || end > self.size
            || self.occupied.iter().any(|r| start < r.end && r.start < end)
        {
            return Err(CodecError);
        }
        let range = start..end;
        self.occupied.push(range.clone());
        Ok(range)
    }
    fn integer(&mut self, f: &IntegerField) -> Result<Integer, CodecError> {
        Ok(Integer {
            range: self.claim(f.offset, f.data_type.width())?,
            endian: f.endian,
        })
    }
    fn value(&mut self, f: &Field) -> Result<Value, CodecError> {
        if !f.scale.is_finite() || f.scale == 0.0 || !f.bias.is_finite() {
            return Err(CodecError);
        }
        let width = match f.data_type {
            DataType::U8 | DataType::I8 | DataType::Bool => 1,
            DataType::U16 | DataType::I16 => 2,
            DataType::U32 | DataType::I32 | DataType::F32 => 4,
            DataType::U64 | DataType::I64 | DataType::F64 => 8,
            DataType::Utf8 => {
                let n = f.length.ok_or(CodecError)?;
                if n == 0 || n > self.limits.field_bytes.min(MAX_FIELD_BYTES) {
                    return Err(CodecError);
                }
                n
            }
        };
        if (!matches!(f.data_type, DataType::Utf8) && f.length.is_some())
            || (matches!(f.data_type, DataType::Bool | DataType::Utf8)
                && (f.scale != 1.0 || f.bias != 0.0))
        {
            return Err(CodecError);
        }
        self.charge(128 + 3 * width)?;
        Ok(Value {
            range: self.claim(f.offset, width)?,
            data_type: f.data_type,
            endian: f.endian,
            scale: f.scale,
            bias: f.bias,
        })
    }
    fn name(&mut self, name: &str) -> Result<String, CodecError> {
        if name.is_empty() || !valid_text(name, self.limits.field_bytes.min(MAX_FIELD_BYTES)) {
            return Err(CodecError);
        }
        self.charge(3 * name.len())?;
        Ok(name.to_owned())
    }
    fn fields(&mut self, fields: &[NamedField]) -> Result<Vec<(String, Value)>, CodecError> {
        if fields.len() > self.limits.fields.min(MAX_FIELDS) {
            return Err(CodecError);
        }
        let mut compiled = Vec::with_capacity(fields.len());
        for f in fields {
            compiled.push((self.name(&f.name)?, self.value(&f.field)?));
        }
        compiled.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        if compiled.windows(2).any(|p| p[0].0 == p[1].0) {
            return Err(CodecError);
        }
        Ok(compiled)
    }
    fn finish(
        mut self,
        checksum: &Option<Checksum>,
        total: &mut usize,
    ) -> Result<Layout, CodecError> {
        let checksum = checksum
            .as_ref()
            .map(|c| {
                let width = match c.algorithm {
                    ChecksumAlgorithm::Crc16Modbus => 2,
                    _ => 1,
                };
                let range = self.claim(c.offset, width)?;
                if c.start >= c.end
                    || c.end > self.size
                    || (c.start < range.end && range.start < c.end)
                {
                    return Err(CodecError);
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
            .ok_or(CodecError)?;
        if *total > MAX_CONFIG_BYTES {
            return Err(CodecError);
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
        super::super::vendor_id(&config.id, config.version)?;
        if config.header.is_empty()
            || config.header.len() > 64
            || config.messages.is_empty()
            || config.messages.len() > MAX_LAYOUTS
            || config.commands.len() > MAX_LAYOUTS
            || config.messages.len() + config.commands.len() > MAX_LAYOUTS
            || limits.output_messages == 0
            || limits.fields == 0
            || limits.field_bytes == 0
            || limits.decoded_bytes < 512
        {
            return Err(CodecError);
        }
        let message_type = Integer {
            range: config.message_type.offset
                ..config
                    .message_type
                    .offset
                    .checked_add(config.message_type.data_type.width())
                    .ok_or(CodecError)?,
            endian: config.message_type.endian,
        };
        let length = config
            .length
            .as_ref()
            .map(|l| {
                Ok((
                    Integer {
                        range: l.field.offset
                            ..l.field
                                .offset
                                .checked_add(l.field.data_type.width())
                                .ok_or(CodecError)?,
                        endian: l.field.endian,
                    },
                    l.adjustment,
                ))
            })
            .transpose()?;
        let mut total = config.header.len();
        let mut messages = Vec::with_capacity(config.messages.len());
        for m in &config.messages {
            if !config.message_type.data_type.fits(m.tag) {
                return Err(CodecError);
            }
            let mut b = Builder::new(config, m.size, &limits)?;
            let s = &m.source_message_id;
            if !matches!(
                s.data_type,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::Utf8
            ) || s.scale != 1.0
                || s.bias != 0.0
                || s.length.is_some_and(|n| n > 64)
            {
                return Err(CodecError);
            }
            let source = b.value(s)?;
            let occurred_at = m.occurred_at.as_ref().map(|f| b.integer(f)).transpose()?;
            let kind = match &m.payload {
                MessagePayload::Telemetry { fields } => {
                    if fields.is_empty() {
                        return Err(CodecError);
                    }
                    Kind::Telemetry(b.fields(fields)?)
                }
                MessagePayload::Heartbeat { sequence } => Kind::Heartbeat(b.integer(sequence)?),
                MessagePayload::Event { name, value } => Kind::Event(
                    b.name(name)?,
                    value.as_ref().map(|f| b.value(f)).transpose()?,
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
                        return Err(CodecError);
                    }
                    Kind::CommandAck {
                        id: b.claim(*command_id_offset, 16)?,
                        execution: b.integer(execution)?,
                        states: [*running, *succeeded, *failed],
                    }
                }
            };
            messages.push(MessagePlan {
                tag: m.tag,
                source,
                occurred_at,
                kind,
                layout: b.finish(&m.checksum, &mut total)?,
            });
        }
        messages.sort_unstable_by_key(|m| m.tag);
        if messages.windows(2).any(|p| p[0].tag == p[1].tag) {
            return Err(CodecError);
        }
        let mut commands = Vec::with_capacity(config.commands.len());
        for c in &config.commands {
            if !config.message_type.data_type.fits(c.tag) {
                return Err(CodecError);
            }
            let mut b = Builder::new(config, c.size, &limits)?;
            commands.push(CommandPlan {
                name: b.name(&c.name)?,
                tag: c.tag,
                command_id: b.claim(c.command_id_offset, 16)?,
                expires_at: c.expires_at.as_ref().map(|f| b.integer(f)).transpose()?,
                fields: b.fields(&c.fields)?,
                layout: b.finish(&c.checksum, &mut total)?,
            });
        }
        commands.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        if commands.windows(2).any(|p| p[0].name == p[1].name)
            || commands
                .iter()
                .enumerate()
                .any(|(i, c)| commands[..i].iter().any(|other| c.tag == other.tag))
        {
            return Err(CodecError);
        }
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
