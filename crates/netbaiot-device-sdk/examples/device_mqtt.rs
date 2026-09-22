use netbaiot_device_sdk::{DeviceClient, DeviceCredentials};
use netbaiot_protocol::{DeviceId, DeviceKey, ProductId, Scalar, TenantId};
use std::collections::BTreeMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let device = DeviceClient::builder()
        .device(DeviceKey {
            tenant_id: TenantId::new("demo")?,
            product_id: ProductId::new("sensor")?,
            device_id: DeviceId::new("device-1")?,
        })
        .credentials(DeviceCredentials::new(
            std::env::var("NETBAIOT_DEVICE_CREDENTIAL_ID")?,
            std::env::var("NETBAIOT_DEVICE_SECRET")?,
        )?)
        .mqtt_endpoint(std::env::var("NETBAIOT_MQTT_ENDPOINT")?)
        .connect()
        .await?;
    device
        .publish_telemetry(BTreeMap::from([(
            "temperature".into(),
            Scalar::Number(21.5),
        )]))
        .await?;
    // `publish_telemetry` accepts into the SDK's bounded channel. Keep this short-lived
    // example alive long enough for its owned MQTT event loop to write the packet.
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}
