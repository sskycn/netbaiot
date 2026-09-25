use netbaiot_device_sdk::{DeviceClient, DeviceCredentials, MqttProtocolVersion, PublishResult};
use netbaiot_protocol::{DeviceId, DeviceKey, ExecutionState, ProductId, Scalar, TenantId};
use std::collections::BTreeMap;
use std::io::Write;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = DeviceClient::builder()
        .device(DeviceKey {
            tenant_id: TenantId::new("demo")?,
            product_id: ProductId::new("sensor")?,
            device_id: DeviceId::new("device-1")?,
        })
        .credentials(DeviceCredentials::new(
            std::env::var("NETBAIOT_DEVICE_CREDENTIAL_ID")?,
            std::env::var("NETBAIOT_DEVICE_SECRET")?,
        )?)
        .mqtt_endpoint(std::env::var("NETBAIOT_MQTT_ENDPOINT")?);
    if std::env::var("NETBAIOT_MQTT_VERSION").as_deref() == Ok("5") {
        builder = builder.protocol_version(MqttProtocolVersion::V5);
    }
    if let Ok(ca) = std::env::var("NETBAIOT_MQTT_CA_PEM") {
        builder = builder.mqtt_ca_pem(ca);
    }
    let device = builder.connect().await?;
    if let Ok(mode) = std::env::var("NETBAIOT_PROBE_MODE") {
        if mode == "puback" {
            let mut receipts = device.publish_receipts();
            let total_start = Instant::now();
            let mut latencies = Vec::with_capacity(200);
            for sequence in 0..200 {
                let start = Instant::now();
                device
                    .publish_telemetry(BTreeMap::from([(
                        "sequence".into(),
                        Scalar::Number(f64::from(sequence)),
                    )]))
                    .await?;
                let receipt =
                    tokio::time::timeout(Duration::from_secs(5), receipts.recv()).await??;
                if receipt.result != PublishResult::Puback {
                    return Err(format!("publish result: {:?}", receipt.result).into());
                }
                latencies.push(start.elapsed().as_micros());
            }
            let elapsed = total_start.elapsed().as_secs_f64();
            latencies.sort_unstable();
            println!(
                "PUBACK_PROBE count=200 rate={:.1}/s p50={}us p95={}us p99={}us",
                200.0 / elapsed,
                latencies[100],
                latencies[190],
                latencies[198]
            );
            device.shutdown_with_timeout(Duration::from_secs(5)).await?;
            return Ok(());
        }
        let mut accepted = 0;
        if mode == "saturated" {
            let telemetry = BTreeMap::from([("probe".into(), Scalar::Text("x".repeat(60_000)))]);
            for _ in 0..64 {
                match device.publish_telemetry(telemetry.clone()).await {
                    Ok(()) => accepted += 1,
                    Err(netbaiot_device_sdk::DeviceSdkError::Overloaded) => break,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let owned_tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        println!("PROBE_READY {mode} {accepted} tasks={owned_tasks}");
        std::io::stdout().flush()?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        device.shutdown();
        return Ok(());
    }
    let mut receipts = device.publish_receipts();
    device
        .publish_telemetry(BTreeMap::from([(
            "temperature".into(),
            Scalar::Number(21.5),
        )]))
        .await?;
    let receipt = tokio::time::timeout(Duration::from_secs(5), receipts.recv()).await??;
    if receipt.result != PublishResult::Puback {
        return Err(format!("publish result: {:?}", receipt.result).into());
    }
    if std::env::var("NETBAIOT_RECONNECT_CHECK").as_deref() == Ok("1") {
        let cycles = std::env::var("NETBAIOT_RECONNECT_CYCLES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1)
            .min(20);
        for _ in 0..cycles {
            println!("READY_FOR_RESTART");
            std::io::stdout().flush()?;
            tokio::time::timeout(Duration::from_secs(10), async {
                while device.mqtt_connected() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            device.wait_until_connected(Duration::from_secs(15)).await?;
            device
                .publish_telemetry(BTreeMap::from([(
                    "temperature".into(),
                    Scalar::Number(22.0),
                )]))
                .await?;
            let receipt = tokio::time::timeout(Duration::from_secs(5), receipts.recv()).await??;
            if receipt.result != PublishResult::Puback {
                return Err(format!("reconnect publish result: {:?}", receipt.result).into());
            }
            println!("RECONNECTED");
            std::io::stdout().flush()?;
        }
    }
    if std::env::var("NETBAIOT_WAIT_COMMAND").as_deref() == Ok("1") {
        let mut commands = device.commands()?;
        let command = tokio::time::timeout(Duration::from_secs(30), commands.recv())
            .await?
            .ok_or("command stream closed")?;
        // Only this example's no-op command is successfully executed here.
        let execution = if command.payload.name == "example_noop" {
            ExecutionState::Succeeded
        } else {
            ExecutionState::Failed
        };
        device.ack_command(command.command_id, execution).await?;
    }
    device.shutdown_with_timeout(Duration::from_secs(5)).await?;
    Ok(())
}
