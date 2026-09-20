use netbaiot_client::NetbaIoTClient;
use netbaiot_protocol::{
    CommandId, DeviceCommand, DeviceCommandPayload, DeviceId, DeviceKey, ProductId, TenantId,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NetbaIoTClient::builder()
        .endpoint(std::env::var("NETBAIOT_ENDPOINT")?)
        .token(std::env::var("NETBAIOT_TOKEN")?)
        .connect()
        .await?;
    let command = DeviceCommand {
        command_id: CommandId::generate(),
        device: DeviceKey {
            tenant_id: TenantId::new("tenant-a")?,
            product_id: ProductId::new("sensor")?,
            device_id: DeviceId::new("device-1")?,
        },
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "sample".into(),
            arguments: Default::default(),
        },
    };
    println!("{:?}", client.commands().send(&command).await?);
    Ok(())
}
