use netbaiot_device_sdk::{DeviceClient, DeviceCredentials};
use netbaiot_protocol::{DeviceId, DeviceKey, ProductId, Scalar, TenantId};
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let device = DeviceClient::builder()
        .device(DeviceKey {
            tenant_id: TenantId::new("tenant-a")?,
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
    Ok(())
}
