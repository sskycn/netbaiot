//! Send one online command over Business RPC V3. Run only against a V3-enabled gateway.
use netbaiot_client::business_rpc::{BusinessRpcV3Client, BusinessRpcV3ClientConfig};
use netbaiot_protocol::{
    CommandId, DeviceCommand, DeviceCommandPayload, DeviceId, DeviceKey, ProductId, TenantId,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("NETBAIOT_BUSINESS_RPC_ADDRESS")?.parse()?;
    let token = std::env::var("NETBAIOT_BUSINESS_RPC_TOKEN")?;
    let mut config = BusinessRpcV3ClientConfig::development(address, token);
    config.provider = false;
    config.events = false;
    let (business, _) = BusinessRpcV3Client::connect(config, None)?;
    business.wait_ready().await?;

    // Persist the command_id in the business application before submission.
    let command = DeviceCommand {
        command_id: CommandId::generate(),
        device: DeviceKey {
            tenant_id: TenantId::new("demo")?,
            product_id: ProductId::new("sensor")?,
            device_id: DeviceId::new("device-1")?,
        },
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "reboot".into(),
            arguments: Default::default(),
        },
    };
    let receipt = business.send_command(&command).await?;
    println!(
        "command_id={} state={:?}",
        receipt.command_id, receipt.state
    );
    // Queued is a gateway receipt. Match the later CommandAck event by command_id.
    // If the RPC outcome is unknown, reconnect and retry this exact command and ID.
    business.shutdown().await;
    Ok(())
}
