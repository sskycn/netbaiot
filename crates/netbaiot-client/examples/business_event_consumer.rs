use futures_util::StreamExt;
use netbaiot_client::NetbaIoTClient;
use netbaiot_protocol::EventFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NetbaIoTClient::builder()
        .endpoint(std::env::var("NETBAIOT_ENDPOINT")?)
        .token(std::env::var("NETBAIOT_TOKEN")?)
        .event_address(std::env::var("NETBAIOT_EVENT_ADDRESS")?.parse()?)
        .connect()
        .await?;
    let mut events = client.events().subscribe(EventFilter::default()).await?;
    while let Some(delivery) = events.next().await {
        let delivery = delivery?;
        println!("event {}", delivery.event_id());
        delivery.ack().await?;
    }
    Ok(())
}
