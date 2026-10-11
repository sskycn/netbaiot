//! Synchronous, bounded device codecs. Transport framing remains outside this crate.
mod binary;
mod common;
pub mod json {
    pub mod v1;
}
pub mod cbor {
    pub mod v1;
}
pub mod msgpack {
    pub mod v1;
}
pub mod protobuf {
    pub mod v1;
}
pub mod vendor;
pub use cbor::v1::CborV1;
pub use json::v1::{JsonV1, check_json_depth};
pub use msgpack::v1::MsgpackV1;
use netbaiot_core::*;
pub use protobuf::v1::ProtobufV1;
use std::sync::Arc;
pub type CodecRegistration = (CodecId, u16, Arc<dyn DeviceCodec>);

/// One startup catalog, shared by server composition and operator diagnostics.
pub fn builtins(limits: CodecLimits) -> Result<Vec<CodecRegistration>, CodecError> {
    let codecs: [(&str, Arc<dyn DeviceCodec>); 4] = [
        ("netbaiot-json", Arc::new(JsonV1::new(limits.clone()))),
        ("netbaiot-cbor", Arc::new(CborV1::new(limits.clone()))),
        ("netbaiot-msgpack", Arc::new(MsgpackV1::new(limits.clone()))),
        ("netbaiot-protobuf", Arc::new(ProtobufV1::new(limits))),
    ];
    codecs
        .into_iter()
        .map(|(id, codec)| Ok((CodecId::new(id).map_err(|_| CodecError)?, 1, codec)))
        .collect()
}
