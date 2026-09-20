use netbaiot_client::NetbaIoTClient;
use netbaiot_protocol::{ConfigRevision, DeviceConfig, DeviceId, DeviceKey, ProductId, TenantId};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NetbaIoTClient::builder()
        .endpoint(std::env::var("NETBAIOT_ENDPOINT")?)
        .token(std::env::var("NETBAIOT_TOKEN")?)
        .connect()
        .await?;
    client
        .configs()
        .set_device_config(&DeviceConfig {
            device: DeviceKey {
                tenant_id: TenantId::new("tenant-a")?,
                product_id: ProductId::new("sensor")?,
                device_id: DeviceId::new("device-1")?,
            },
            revision: ConfigRevision::new(42).expect("non-zero example revision"),
            payload: Arc::new(serde_json::json!({"interval_seconds": 30})),
        })
        .await?;
    Ok(())
}
