//! Loopback current Business RPC command example. Production deployments use mTLS.
use netbaiot_client::business_rpc::{BusinessRpcV3Client, BusinessRpcV3ClientConfig};
use netbaiot_protocol::{
    CommandId, DeviceCommand, DeviceCommandPayload, DeviceEventKind, DeviceId, DeviceKey,
    ProductId, TenantId, business_rpc::BusinessRole,
};
use std::{env, net::SocketAddr};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = env::var("NETBAIOT_BUSINESS_RPC_ADDRESS")?.parse()?;
    let token = env::var("NETBAIOT_BUSINESS_RPC_TOKEN")?;
    let command = DeviceCommand {
        command_id: CommandId::generate(),
        device: DeviceKey {
            tenant_id: TenantId::new(env::var("DEMO_TENANT_ID")?)?,
            product_id: ProductId::new(env::var("DEMO_PRODUCT_ID")?)?,
            device_id: DeviceId::new(env::var("DEMO_DEVICE_ID")?)?,
        },
        expires_at: None,
        payload: DeviceCommandPayload {
            name: env::var("DEMO_COMMAND_NAME")?,
            arguments: Default::default(),
        },
    };
    let (client, mut events) = BusinessRpcV3Client::connect(
        current_config(address, token, BusinessRole::Application),
        None,
    )?;
    client.wait_ready().await?;
    let dispatch = client.send_command(&command).await?;
    println!(
        "gateway dispatch: {:?} {}",
        dispatch.state, dispatch.command_id
    );
    while let Some(delivery) = events.recv().await {
        if let DeviceEventKind::CommandAck(ack) = &delivery.delivery.event.kind
            && ack.command_id == dispatch.command_id
        {
            println!("device execution: {:?}", ack.execution);
            // Commit application work before confirming this event.
            delivery.ack().await?;
            break;
        }
        delivery.ack().await?;
    }
    client.shutdown().await;
    Ok(())
}

fn current_config(
    address: std::net::SocketAddr,
    token: String,
    role: BusinessRole,
) -> BusinessRpcV3ClientConfig {
    let mut settings = BusinessRpcV3ClientConfig::development(address, token);
    settings.provider = role.auth_control();
    settings.events = role.events();
    settings
}
