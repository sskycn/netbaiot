//! Independent native implementation of the ACME example telemetry and set_temperature payloads.
//! Explicit opt-in only; this is not a built-in or a production industrial protocol.
use crate::common::{Parsed, event, valid_text, validate_command};
use crate::vendor::numeric::IntegerConversion;
use netbaiot_core::*;

pub struct AcmeV1 {
    limits: CodecLimits,
    temperature: IntegerConversion,
    humidity: IntegerConversion,
}
impl AcmeV1 {
    pub fn new(limits: CodecLimits) -> Result<Self, CodecError> {
        if limits.input_bytes < 19
            || limits.decoded_bytes < 2048
            || limits.fields < 4
            || limits.field_bytes < 15
            || limits.output_messages == 0
        {
            return Err(CodecError);
        }
        Ok(Self {
            limits,
            temperature: IntegerConversion::legacy(
                0.1,
                0.0,
                i128::from(i16::MIN),
                i128::from(i16::MAX),
            )
            .map_err(|_| CodecError)?,
            humidity: IntegerConversion::legacy(0.1, 0.0, 0, i128::from(u16::MAX))
                .map_err(|_| CodecError)?,
        })
    }
    fn parse(&self, p: &[u8]) -> Result<Parsed, CodecError> {
        if p.len() != 19
            || p[..5] != [165, 90, 0, 19, 1]
            || p[..18].iter().fold(0u8, |s, b| s.wrapping_add(*b)) != p[18]
            || p[13] > 1
        {
            return Err(CodecError);
        }
        let model = std::str::from_utf8(&p[14..18]).map_err(|_| CodecError)?;
        if !valid_text(model, 4) {
            return Err(CodecError);
        }
        Ok((
            super::sequence_id(u64::from(u32::from_le_bytes([p[5], p[6], p[7], p[8]])))?,
            None,
            DeviceEventKind::Telemetry(
                [
                    (
                        "temperature".into(),
                        self.temperature
                            .decode(i128::from(i16::from_le_bytes([p[9], p[10]])))?,
                    ),
                    (
                        "humidity".into(),
                        self.humidity
                            .decode(i128::from(u16::from_be_bytes([p[11], p[12]])))?,
                    ),
                    ("alarm".into(), Scalar::Boolean(p[13] == 1)),
                    ("model".into(), Scalar::Text(model.to_owned())),
                ]
                .into(),
            ),
        ))
    }
}
impl DeviceCodec for AcmeV1 {
    fn validate_payload(
        &self,
        _: &DecodeContext<'_>,
        payload: &[u8],
    ) -> Result<Vec<DeviceEventKind>, CodecError> {
        Ok(vec![self.parse(payload)?.2])
    }
    fn decode(
        &self,
        ctx: &DecodeContext<'_>,
        payload: &[u8],
    ) -> Result<Vec<DeviceEvent>, CodecError> {
        Ok(event(ctx, self.parse(payload)?))
    }
    fn encode(
        &self,
        ctx: &EncodeContext<'_>,
        command: &DeviceCommand,
    ) -> Result<Vec<u8>, CodecError> {
        if !validate_command(ctx, command, &self.limits)
            || self.limits.input_bytes < 32
            || command.payload.name != "set_temperature"
            || command.payload.arguments.len() != 1
        {
            return Err(CodecError);
        }
        let raw = i16::try_from(
            self.temperature.encode(
                command
                    .payload
                    .arguments
                    .get("temperature")
                    .ok_or(CodecError)?,
            )?,
        )
        .map_err(|_| CodecError)?;
        let expires = u64::try_from(command.expires_at.unwrap_or(0)).map_err(|_| CodecError)?;
        let mut out = vec![0; 32];
        out[..5].copy_from_slice(&[165, 90, 0, 32, 129]);
        out[5..21].copy_from_slice(command.command_id.0.as_bytes());
        out[21..23].copy_from_slice(&raw.to_le_bytes());
        out[23..31].copy_from_slice(&expires.to_be_bytes());
        out[31] = out[..31].iter().fold(0u8, |s, b| s.wrapping_add(*b));
        Ok(out)
    }
}
