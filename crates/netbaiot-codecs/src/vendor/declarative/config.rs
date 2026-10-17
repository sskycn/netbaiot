//! Versioned, bounded binary payload descriptions. Offsets are payload-relative.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DeclarativeConfig {
    /// Explicit authentication-bound codec ID, e.g. vendor-acme. Never inferred from bytes.
    pub id: String,
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub version: u16,
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 64)))]
    pub header: Vec<u8>,
    pub message_type: IntegerField,
    pub length: Option<LengthField>,
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = 64)))]
    pub messages: Vec<Message>,
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(length(max = 64)))]
    pub commands: Vec<Command>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum Endian {
    #[default]
    Big,
    Little,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum IntegerType {
    U8,
    U16,
    U32,
    U64,
}
impl IntegerType {
    pub(super) fn width(self) -> usize {
        match self {
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 => 4,
            Self::U64 => 8,
        }
    }
    pub(super) fn fits(self, n: u64) -> bool {
        self.width() == 8 || n < (1u64 << (self.width() * 8))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct IntegerField {
    #[cfg_attr(feature = "schema", schemars(range(max = 65535)))]
    pub offset: usize,
    #[serde(rename = "type")]
    pub data_type: IntegerType,
    #[serde(default)]
    pub endian: Endian,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LengthField {
    pub field: IntegerField,
    /// Wire value + adjustment must equal the complete payload length.
    #[serde(default)]
    pub adjustment: i32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum DataType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bool,
    Utf8,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum ConversionMode {
    DecimalExact,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DecimalConversion {
    pub mode: ConversionMode,
    /// Exact integer scale. At most 38 significant digits and exponent magnitude 38.
    pub scale: String,
    /// Exact integer bias. At most 38 significant digits and exponent magnitude 38.
    #[serde(default = "zero_decimal")]
    pub bias: String,
}

fn zero_decimal() -> String {
    "0".into()
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Field {
    #[cfg_attr(feature = "schema", schemars(range(max = 65535)))]
    pub offset: usize,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default)]
    pub endian: Endian,
    /// Required only for UTF-8: exact byte length, without padding or terminator.
    pub length: Option<usize>,
    /// IEEE 754 floating-point scale. Valid only for f32/f64; omission means 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<f64>,
    /// IEEE 754 floating-point bias. Valid only for f32/f64; omission means 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bias: Option<f64>,
    /// Exact-decimal conversion for integer wire types only; omission is exact identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversion: Option<DecimalConversion>,
}

// Presence is needed only while parsing. Public Field values retain ordinary
// Option<f64> semantics, including null -> None for IEEE 754 fields.
#[derive(Default)]
enum LegacyFloat {
    #[default]
    Missing,
    Present(Option<f64>),
}
impl<'de> Deserialize<'de> for LegacyFloat {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<f64>::deserialize(deserializer).map(Self::Present)
    }
}
impl LegacyFloat {
    fn value(self) -> Option<f64> {
        match self {
            Self::Missing => None,
            Self::Present(value) => value,
        }
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            offset: usize,
            #[serde(rename = "type")]
            data_type: DataType,
            #[serde(default)]
            endian: Endian,
            length: Option<usize>,
            #[serde(default)]
            scale: LegacyFloat,
            #[serde(default)]
            bias: LegacyFloat,
            conversion: Option<DecimalConversion>,
        }
        let input = Input::deserialize(deserializer)?;
        if matches!(
            input.data_type,
            DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
                | DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
        ) {
            if matches!(input.scale, LegacyFloat::Present(_)) {
                return Err(serde::de::Error::custom(
                    "scale: Integer fields cannot contain legacy scale; migrate to conversion with mode decimal_exact and string scale/bias values.",
                ));
            }
            if matches!(input.bias, LegacyFloat::Present(_)) {
                return Err(serde::de::Error::custom(
                    "bias: Integer fields cannot contain legacy bias; migrate to conversion with mode decimal_exact and string scale/bias values.",
                ));
            }
        }
        Ok(Self {
            offset: input.offset,
            data_type: input.data_type,
            endian: input.endian,
            length: input.length,
            scale: input.scale.value(),
            bias: input.bias.value(),
            conversion: input.conversion,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct NamedField {
    pub name: String,
    pub field: Field,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum MessagePayload {
    Telemetry {
        fields: Vec<NamedField>,
    },
    Heartbeat {
        sequence: IntegerField,
    },
    Event {
        name: String,
        value: Option<Field>,
    },
    CommandAck {
        /// Offset of the complete 16-byte UUID in RFC 4122 byte order.
        command_id_offset: usize,
        execution: IntegerField,
        running: u64,
        succeeded: u64,
        failed: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Message {
    pub tag: u64,
    #[cfg_attr(feature = "schema", schemars(range(min = 1, max = 65536)))]
    pub size: usize,
    /// Unscaled unsigned integer (decimal ID) or exact UTF-8 identifier, at most 64 bytes.
    pub source_message_id: Field,
    pub occurred_at: Option<IntegerField>,
    pub payload: MessagePayload,
    pub checksum: Option<Checksum>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Command {
    pub name: String,
    pub tag: u64,
    #[cfg_attr(feature = "schema", schemars(range(min = 1, max = 65536)))]
    pub size: usize,
    pub command_id_offset: usize,
    pub expires_at: Option<IntegerField>,
    pub fields: Vec<NamedField>,
    pub checksum: Option<Checksum>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum ChecksumAlgorithm {
    Sum8,
    Xor8,
    Crc16Modbus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    #[cfg_attr(feature = "schema", schemars(range(max = 65535)))]
    pub offset: usize,
    /// Inclusive start and exclusive end; the checksum bytes must be outside this range.
    pub start: usize,
    pub end: usize,
    #[serde(default)]
    pub endian: Endian,
}
