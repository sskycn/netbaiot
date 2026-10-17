//! cargo run -p netbaiot-server --example native_vendor
use netbaiot_codecs::vendor::{demo::AcmeV1, registration};
use netbaiot_core::CodecLimits;
use netbaiot_server::Config;
use std::sync::Arc;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/vendor-codec.json"))?;
    config.payload_codecs.clear();
    // An independently implemented DeviceCodec; no file, script or dynamic code loader.
    config.native_codecs.push(registration(
        "vendor-acme",
        1,
        Arc::new(AcmeV1::new(CodecLimits::default())?),
    )?);
    config.validate()?;
    // Pass this Config to netbaiot_server::run(config, stop) in an embedded gateway.
    // Several versions can coexist; duplicate registrations and built-in replacement fail.
    println!("Native vendor-acme v1 registered; authentication and product profiles validated.");
    Ok(())
}
