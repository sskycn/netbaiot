//! Bounded netbaiot-json-v1 codec; no transport or runtime dependencies.
use netbaiot_core::*;
use serde::Deserialize;

pub struct JsonV1 {
    limits: CodecLimits,
}
impl JsonV1 {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }
}
impl Default for JsonV1 {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireMessage<'a> {
    schema_version: u16,
    source_message_id: SourceMessageId,
    #[serde(default)]
    occurred_at: Option<Timestamp>,
    kind: Kind,
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Telemetry,
    Event,
    Heartbeat,
    CommandAck,
}
struct UniqueFields(std::collections::BTreeMap<String, Scalar>);
impl<'de> Deserialize<'de> for UniqueFields {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueFields;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("unique scalar fields")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut out = std::collections::BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, Scalar>()? {
                    if out.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate field"));
                    }
                }
                Ok(UniqueFields(out))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}
// Count members before allocation; JSON punctuation inside strings does not count.
fn check_members(input: &[u8], maximum: usize) -> Result<(), CodecError> {
    let (mut quoted, mut escaped, mut members) = (false, false, 0usize);
    for byte in input {
        if quoted {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                quoted = false;
            }
        } else if *byte == b'"' {
            quoted = true;
        } else if *byte == b'[' {
            // JSON v1 has no array-valued fields; reject before untagged scalar buffering.
            return Err(CodecError);
        } else if *byte == b':' {
            members = members.checked_add(1).ok_or(CodecError)?;
            if members > maximum {
                return Err(CodecError);
            }
        }
    }
    Ok(())
}

/// Bound nesting before serde can recurse. Braces inside strings are ignored.
pub fn check_json_depth(input: &[u8], maximum: usize) -> Result<(), CodecError> {
    let (mut depth, mut string, mut escape) = (0usize, false, false);
    for b in input {
        if string {
            if escape {
                escape = false;
            } else if *b == b'\\' {
                escape = true;
            } else if *b == b'"' {
                string = false;
            }
        } else {
            match b {
                b'"' => string = true,
                b'{' | b'[' => {
                    depth = depth.checked_add(1).ok_or(CodecError)?;
                    if depth > maximum {
                        return Err(CodecError);
                    }
                }
                b'}' | b']' => {
                    depth = depth.checked_sub(1).ok_or(CodecError)?;
                }
                _ => {}
            }
        }
    }
    if depth != 0 || string {
        return Err(CodecError);
    }
    Ok(())
}
fn valid_text(s: &str, limit: usize) -> bool {
    s.len() <= limit && !s.chars().any(char::is_control)
}
fn scalar(s: &Scalar, limit: usize) -> bool {
    match s {
        Scalar::Text(s) => valid_text(s, limit),
        Scalar::Number(n) => n.is_finite(),
        Scalar::Boolean(_) => true,
    }
}
impl JsonV1 {
    fn validate(&self, payload: &DevicePayload) -> bool {
        let l = &self.limits;
        match payload {
            DevicePayload::Telemetry(fields) => {
                !fields.is_empty()
                    && fields.len() <= l.fields
                    && fields.iter().all(|(k, v)| {
                        !k.is_empty() && valid_text(k, l.field_bytes) && scalar(v, l.field_bytes)
                    })
            }
            DevicePayload::Event(e) => {
                !e.name.is_empty()
                    && valid_text(&e.name, l.field_bytes)
                    && e.value.as_ref().is_none_or(|v| scalar(v, l.field_bytes))
            }
            DevicePayload::Heartbeat(_) => true,
            DevicePayload::CommandAck(a) => a.execution != ExecutionState::Unknown,
        }
    }
}
impl DeviceCodec for JsonV1 {
    fn decode(
        &self,
        ctx: &DecodeContext<'_>,
        payload: &[u8],
    ) -> Result<Vec<DeviceMessage>, CodecError> {
        if payload.len() > self.limits.input_bytes
            || payload.len() > self.limits.decoded_bytes
            || self.limits.output_messages == 0
        {
            return Err(CodecError);
        }
        check_json_depth(payload, self.limits.nesting_depth)?;
        check_members(payload, self.limits.fields.saturating_add(8))?;
        let wire: WireMessage<'_> = serde_json::from_slice(payload).map_err(|_| CodecError)?;
        let decoded = match wire.kind {
            Kind::Telemetry => DevicePayload::Telemetry(
                serde_json::from_str::<UniqueFields>(wire.data.get())
                    .map_err(|_| CodecError)?
                    .0,
            ),
            Kind::Event => {
                DevicePayload::Event(serde_json::from_str(wire.data.get()).map_err(|_| CodecError)?)
            }
            Kind::Heartbeat => DevicePayload::Heartbeat(
                serde_json::from_str(wire.data.get()).map_err(|_| CodecError)?,
            ),
            Kind::CommandAck => DevicePayload::CommandAck(
                serde_json::from_str(wire.data.get()).map_err(|_| CodecError)?,
            ),
        };
        if wire.schema_version != 1
            || !self.validate(&decoded)
            || wire.occurred_at.is_some_and(|t| t < 0)
        {
            return Err(CodecError);
        }
        Ok(vec![DeviceMessage {
            message_id: MessageId::generate(),
            source_message_id: wire.source_message_id,
            device: ctx.device.clone(),
            received_at: ctx.received_at,
            occurred_at: wire.occurred_at,
            payload: decoded,
        }])
    }
    fn encode(
        &self,
        ctx: &EncodeContext<'_>,
        command: &DeviceCommand,
    ) -> Result<Vec<u8>, CodecError> {
        if &command.device != ctx.device
            || command.payload.name.is_empty()
            || !valid_text(&command.payload.name, self.limits.field_bytes)
            || command.payload.arguments.len() > self.limits.fields
            || !command.payload.arguments.iter().all(|(k, v)| {
                valid_text(k, self.limits.field_bytes) && scalar(v, self.limits.field_bytes)
            })
        {
            return Err(CodecError);
        }
        let data = serde_json::to_vec(command).map_err(|_| CodecError)?;
        if data.len() > self.limits.decoded_bytes {
            return Err(CodecError);
        }
        Ok(data)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn key() -> DeviceKey {
        DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        }
    }
    #[test]
    fn all_payloads_and_bounds() {
        let k = key();
        let ctx = DecodeContext {
            device: &k,
            received_at: 1,
        };
        let codec = JsonV1::default();
        for data in [
            r#""kind":"telemetry","data":{"temperature":25.3}"#,
            r#""kind":"event","data":{"name":"boot","value":true}"#,
            r#""kind":"heartbeat","data":{"sequence":1}"#,
            r#""kind":"command_ack","data":{"command_id":"00000000-0000-0000-0000-000000000001","execution":"succeeded"}"#,
        ] {
            let wire = format!(r#"{{"schema_version":1,"source_message_id":"boot:1",{data}}}"#);
            assert_eq!(codec.decode(&ctx, wire.as_bytes()).unwrap().len(), 1);
        }
        for bad in [
            b"{".as_slice(),
            b"[]",
            b"\xff",
            b"{\"data\": [[[[[[[[[0]]]]]]]]]}",
        ] {
            assert!(codec.decode(&ctx, bad).is_err());
        }
        assert!(codec.decode(&ctx, &vec![b' '; 65537]).is_err());
        assert!(check_json_depth(br#"{"x":"{{{{{{{{{{{{{{"}"#, 2).is_ok());
    }
}

#[cfg(test)]
mod hostile {
    use super::*;
    #[test]
    fn unknown_identity_duplicate_fields_and_huge_maps_are_rejected() {
        let key = DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        };
        let ctx = DecodeContext {
            device: &key,
            received_at: 0,
        };
        let codec = JsonV1::default();
        for data in [
            r#"{"schema_version":1,"source_message_id":"1","device":"spoof","kind":"heartbeat","data":{"sequence":1}}"#,
            r#"{"schema_version":1,"source_message_id":"1","kind":"telemetry","data":{"x":1,"x":2}}"#,
        ] {
            assert!(codec.decode(&ctx, data.as_bytes()).is_err());
        }
        let fields = (0..1000)
            .map(|i| format!("\"f{i}\":0"))
            .collect::<Vec<_>>()
            .join(",");
        let wire = format!(
            r#"{{"schema_version":1,"source_message_id":"1","kind":"telemetry","data":{{{fields}}}}}"#
        );
        assert!(codec.decode(&ctx, wire.as_bytes()).is_err());
        let fields = (0..64)
            .map(|i| format!("\"f{i}\":0"))
            .collect::<Vec<_>>()
            .join(",");
        let wire = format!(
            r#"{{"schema_version":1,"source_message_id":"1","kind":"telemetry","data":{{{fields}}}}}"#
        );
        assert!(codec.decode(&ctx, wire.as_bytes()).is_ok());
    }
}
