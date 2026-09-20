use netbaiot_device_sdk::{DeviceClient, DeviceCredentials};
use netbaiot_protocol::{
    DeviceId, DeviceKey, DeviceUplink, DeviceUplinkKind, Heartbeat, ProductId, SourceMessageId,
    TenantId,
};

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
        .http_endpoint(std::env::var("NETBAIOT_DEVICE_HTTP_ENDPOINT")?)
        .connect()
        .await?;
    let accepted = device
        .upload_data(&DeviceUplink::new(
            SourceMessageId::new("boot-1")?,
            DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 1 }),
        ))
        .await?;
    println!("accepted event {}", accepted.event_id);
    Ok(())
}
