use netbaiot_client::NetbaIoTClient;
use netbaiot_protocol::{
    CommandId, DeviceCommand, DeviceCommandPayload, DeviceId, DeviceKey, ProductId, Scalar,
    TenantId,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NetbaIoTClient::builder()
        .endpoint(std::env::var("NETBAIOT_ENDPOINT")?)
        .token(std::env::var("NETBAIOT_TOKEN")?)
        .connect()
        .await?;
    // The application persists desired/reported state externally and defines this name.
    // The gateway treats it like any other online command; offline is an explicit error.
    let command = DeviceCommand {
        command_id: CommandId::generate(),
        device: DeviceKey {
            tenant_id: TenantId::new("demo")?,
            product_id: ProductId::new("sensor")?,
            device_id: DeviceId::new("device-1")?,
        },
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "apply_config".into(),
            arguments: [
                ("revision".into(), Scalar::Number(42.0)),
                ("sample_interval_seconds".into(), Scalar::Number(5.0)),
            ]
            .into(),
        },
    };
    let dispatch = client.commands().send(&command).await?;
    println!(
        "command_id={} state={:?}",
        dispatch.command_id, dispatch.state
    );
    // A dispatch receipt is not device execution or desired/reported convergence.
    // Consume CommandAck through events(), then let the application update its state.
    Ok(())
}
