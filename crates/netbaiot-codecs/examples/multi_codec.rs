//! cargo run -p netbaiot-codecs --example multi_codec
use netbaiot_core::*;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    for (id, _, codec) in netbaiot_codecs::builtins(CodecLimits::default())? {
        let format = id.as_str().strip_prefix("netbaiot-").ok_or(CodecError)?;
        let key = DeviceKey {
            tenant_id: TenantId::new("demo")?,
            product_id: ProductId::new(format)?,
            device_id: DeviceId::new("sensor-1")?,
        };
        let p = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("tests/fixtures/telemetry.{format}")),
        )?;
        let event = codec.decode(
            &DecodeContext {
                device: &key,
                received_at: 1000,
            },
            &p,
        )?;
        println!("{}: {}", id.as_str(), serde_json::to_string(&event[0])?);
    }
    Ok(())
}
