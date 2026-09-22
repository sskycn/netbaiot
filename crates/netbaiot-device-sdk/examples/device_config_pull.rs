use netbaiot_device_sdk::{ConfigUpdate, DeviceClient, DeviceCredentials};
use netbaiot_protocol::{ConfigApplyStatus, DeviceId, DeviceKey, ProductId, TenantId};

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
        .http_endpoint(std::env::var("NETBAIOT_DEVICE_HTTP_ENDPOINT")?)
        .connect()
        .await?;
    if let ConfigUpdate::Updated(config) = device.config().check(None).await? {
        println!("apply revision {}: {}", config.revision, config.payload);
        device
            .config()
            .ack(config.revision, ConfigApplyStatus::Applied, None)
            .await?;
    }
    Ok(())
}
