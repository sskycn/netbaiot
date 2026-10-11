//! Named MessagePack maps; arrays, binary and extensions are outside the V1 profile.
use crate::{
    binary::{Format, preflight},
    common::*,
};
use netbaiot_core::*;
use serde::Serialize;
pub struct MsgpackV1 {
    limits: CodecLimits,
}
impl MsgpackV1 {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }
    fn parse(&self, payload: &[u8]) -> Result<Parsed, CodecError> {
        preflight(payload, &self.limits, Format::Msgpack)?;
        let wire: MapUplink = rmp_serde::from_slice(payload).map_err(|_| CodecError)?;
        wire.into_event(&self.limits)
    }
}
impl Default for MsgpackV1 {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}
impl DeviceCodec for MsgpackV1 {
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
        MapCommand::from(c)
            .serialize(&mut rmp_serde::Serializer::new(&mut writer).with_struct_map())
            .map_err(|_| CodecError)?;
        Ok(writer.bytes)
    }
}
