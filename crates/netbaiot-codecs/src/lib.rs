//! Bounded netbaiot-json-v1 codec; no transport or runtime dependencies.
use netbaiot_core::*;
use serde::Deserialize;

pub struct JsonV1 { limits: CodecLimits }
impl JsonV1 { pub fn new(limits: CodecLimits) -> Self { Self { limits } } }
impl Default for JsonV1 { fn default() -> Self { Self::new(CodecLimits::default()) } }
#[derive(Deserialize)]
struct WireMessage { schema_version: u16, source_message_id: SourceMessageId, #[serde(default)] occurred_at: Option<Timestamp>, #[serde(flatten)] payload: DevicePayload }

/// Bound nesting before serde can recurse. Braces inside strings are ignored.
pub fn check_json_depth(input: &[u8], maximum: usize) -> Result<(), CodecError> {
    let (mut depth, mut string, mut escape) = (0usize, false, false);
    for b in input {
        if string { if escape { escape = false; } else if *b == b'\\' { escape = true; } else if *b == b'"' { string = false; } }
        else { match b { b'"' => string = true, b'{' | b'[' => { depth = depth.checked_add(1).ok_or(CodecError)?; if depth > maximum { return Err(CodecError); } }, b'}' | b']' => { depth = depth.checked_sub(1).ok_or(CodecError)?; }, _ => {} } }
    }
    if depth != 0 || string { return Err(CodecError); } Ok(())
}
fn valid_text(s: &str, limit: usize) -> bool { s.len() <= limit && !s.chars().any(char::is_control) }
fn scalar(s: &Scalar, limit: usize) -> bool { match s { Scalar::Text(s) => valid_text(s, limit), Scalar::Number(n) => n.is_finite(), Scalar::Boolean(_) => true } }
impl JsonV1 {
    fn validate(&self, payload: &DevicePayload) -> bool {
        let l = &self.limits;
        match payload {
            DevicePayload::Telemetry(fields) => !fields.is_empty() && fields.len() <= l.fields && fields.iter().all(|(k,v)| !k.is_empty() && valid_text(k, l.field_bytes) && scalar(v, l.field_bytes)),
            DevicePayload::Event(e) => !e.name.is_empty() && valid_text(&e.name, l.field_bytes) && e.value.as_ref().is_none_or(|v| scalar(v,l.field_bytes)),
            DevicePayload::Heartbeat(_) => true,
            DevicePayload::CommandAck(a) => a.execution != ExecutionState::Unknown,
        }
    }
}
impl DeviceCodec for JsonV1 {
    fn decode(&self, ctx: &DecodeContext<'_>, payload: &[u8]) -> Result<Vec<DeviceMessage>, CodecError> {
        if payload.len() > self.limits.input_bytes || payload.len() > self.limits.decoded_bytes || self.limits.output_messages == 0 { return Err(CodecError); }
        check_json_depth(payload, self.limits.nesting_depth)?;
        let wire: WireMessage = serde_json::from_slice(payload).map_err(|_| CodecError)?;
        if wire.schema_version != 1 || !self.validate(&wire.payload) || wire.occurred_at.is_some_and(|t| t < 0) { return Err(CodecError); }
        Ok(vec![DeviceMessage { message_id: MessageId::generate(), source_message_id: wire.source_message_id, device: ctx.device.clone(), received_at: ctx.received_at, occurred_at: wire.occurred_at, payload: wire.payload }])
    }
    fn encode(&self, ctx: &EncodeContext<'_>, command: &DeviceCommand) -> Result<Vec<u8>, CodecError> {
        if &command.device != ctx.device || command.payload.name.is_empty() || !valid_text(&command.payload.name,self.limits.field_bytes) || command.payload.arguments.len() > self.limits.fields || !command.payload.arguments.iter().all(|(k,v)| valid_text(k,self.limits.field_bytes) && scalar(v,self.limits.field_bytes)) { return Err(CodecError); }
        let data = serde_json::to_vec(command).map_err(|_| CodecError)?;
        if data.len() > self.limits.decoded_bytes { return Err(CodecError); } Ok(data)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn key() -> DeviceKey { DeviceKey { tenant_id: TenantId::new("t").unwrap(), product_id: ProductId::new("p").unwrap(), device_id: DeviceId::new("d").unwrap() } }
    #[test] fn all_payloads_and_bounds() {
        let k=key(); let ctx=DecodeContext{device:&k,received_at:1}; let codec=JsonV1::default();
        for data in [r#""kind":"telemetry","data":{"temperature":25.3}"#,r#""kind":"event","data":{"name":"boot","value":true}"#,r#""kind":"heartbeat","data":{"sequence":1}"#,r#""kind":"command_ack","data":{"command_id":"00000000-0000-0000-0000-000000000001","execution":"succeeded"}"#] {
            let wire=format!(r#"{{"schema_version":1,"source_message_id":"boot:1",{data}}}"#);
            assert_eq!(codec.decode(&ctx,wire.as_bytes()).unwrap().len(),1);
        }
        for bad in [b"{".as_slice(),b"[]",b"\xff",b"{\"data\": [[[[[[[[[0]]]]]]]]]}"] { assert!(codec.decode(&ctx,bad).is_err()); }
        assert!(codec.decode(&ctx,&vec![b' ';65537]).is_err());
        assert!(check_json_depth(br#"{"x":"{{{{{{{{{{{{{{"}"#,2).is_ok());
    }
}
