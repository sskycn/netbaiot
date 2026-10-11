//! RFC 8949 definite-length text-keyed maps; no tags, arrays or byte strings in V1.
use crate::{
    binary::{Format, preflight},
    common::*,
};
use netbaiot_core::*;
pub struct CborV1 {
    limits: CodecLimits,
}
impl CborV1 {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }
    fn parse(&self, payload: &[u8]) -> Result<Parsed, CodecError> {
        preflight(payload, &self.limits, Format::Cbor)?;
        let wire: MapUplink = ciborium::from_reader(payload).map_err(|_| CodecError)?;
        wire.into_event(&self.limits)
    }
}
impl Default for CborV1 {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}
impl DeviceCodec for CborV1 {
    fn validate_payload(
        &self,
        _: &DecodeContext<'_>,
        p: &[u8],
    ) -> Result<Vec<DeviceEventKind>, CodecError> {
        Ok(vec![self.parse(p)?.2])
    }
    fn decode(&self, ctx: &DecodeContext<'_>, p: &[u8]) -> Result<Vec<DeviceEvent>, CodecError> {
        Ok(event(ctx, self.parse(p)?))
    }
    fn encode(&self, ctx: &EncodeContext<'_>, c: &DeviceCommand) -> Result<Vec<u8>, CodecError> {
        if !validate_command(ctx, c, &self.limits) {
            return Err(CodecError);
        }
        let mut writer = BoundedWriter::new(self.limits.decoded_bytes);
        ciborium::into_writer(&MapCommand::from(c), &mut writer).map_err(|_| CodecError)?;
        Ok(writer.bytes)
    }
}
