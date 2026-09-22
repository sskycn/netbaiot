use futures_util::StreamExt;
use netbaiot_client::NetbaIoTClient;
use netbaiot_protocol::{
    CommandId, DeviceCommand, DeviceCommandPayload, DeviceId, DeviceKey, EventFilter, ProductId,
    TenantId,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NetbaIoTClient::builder()
        .endpoint(std::env::var("NETBAIOT_ENDPOINT")?)
        .token(std::env::var("NETBAIOT_TOKEN")?)
        .event_token(std::env::var("NETBAIOT_EVENT_TOKEN")?)
        .event_address(std::env::var("NETBAIOT_EVENT_ADDRESS")?.parse()?)
        .connect()
        .await?;
    let mut events = client.events().subscribe(EventFilter::default()).await?;

    // Commands are never retried automatically and require a live MQTT/TCP device session.
    let dispatch = client
        .commands()
        .send(&DeviceCommand {
            command_id: CommandId::generate(),
            device: DeviceKey {
                tenant_id: TenantId::new("demo")?,
                product_id: ProductId::new("sensor")?,
                device_id: DeviceId::new("device-1")?,
            },
            expires_at: None,
            payload: DeviceCommandPayload {
                name: "sample_now".into(),
                arguments: Default::default(),
            },
        })
        .await?;
    println!("command {} is {:?}", dispatch.command_id, dispatch.state);

    // EventStream reconnects with bounded backoff. The same event_id can reappear after
    // reconnect, so replace this print with a durable idempotent business transaction.
    if let Some(delivery) = events.next().await {
        let delivery = delivery?;
        println!("processed event {}", delivery.event_id());
        delivery.ack().await?;
    }
    events.close();
    client.shutdown();
    Ok(())
}
