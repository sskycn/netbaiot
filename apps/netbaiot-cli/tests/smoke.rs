use futures_util::StreamExt;
use netbaiot_device_sdk::{DeviceClient, DeviceCredentials};
use netbaiot_protocol::{CommandId, DeviceEvent, DeviceId, DeviceKey, ProductId, TenantId};
use netbaiot_server::{Config, run_with_credentials};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, UdpSocket},
    process::{Child, Command},
};
use tokio_util::sync::CancellationToken;

const ADMIN: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
const STREAM_TOKEN: &str = "cli-stream-token";
const DEVICE_SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

async fn config(root: &Path) -> Config {
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..5 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    config.device_http = addresses[0];
    config.management_http = addresses[1];
    config.mqtt = addresses[2];
    config.tcp = addresses[3];
    config.business_tcp = Some(addresses[4]);
    config.udp = udp.local_addr().unwrap();
    config.delivery_url = None;
    config.spool_directory = root.join("spool");
    drop(reservations);
    drop(udp);
    config
}

fn command(config: &Config) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_netbaiot"));
    command
        .env(
            "NETBAIOT_ENDPOINT",
            format!("http://{}", config.management_http),
        )
        .env("NETBAIOT_TOKEN", ADMIN)
        .env("NETBAIOT_EVENT_TOKEN", STREAM_TOKEN)
        .env(
            "NETBAIOT_EVENT_ADDRESS",
            config.business_tcp.unwrap().to_string(),
        )
        .env("NETBAIOT_TENANT", "demo")
        .env("NETBAIOT_PRODUCT", "sensor")
        .kill_on_drop(true);
    command
}

async fn cli(config: &Config, arguments: &[&str]) -> std::process::Output {
    command(config).args(arguments).output().await.unwrap()
}

async fn json_cli(config: &Config, arguments: &[&str]) -> serde_json::Value {
    let output = cli(config, arguments).await;
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

async fn wait_status(config: &Config) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let output = cli(config, &["--output", "json", "server", "status"]).await;
            if output.status.success() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

fn device_key() -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new("demo").unwrap(),
        product_id: ProductId::new("sensor").unwrap(),
        device_id: DeviceId::new("device-1").unwrap(),
    }
}

#[tokio::test]
async fn cli_smoke_covers_status_device_command_config_events_auth_and_drain() {
    let root = std::env::temp_dir().join(format!("netbaiot-cli-smoke-{}", uuid::Uuid::new_v4()));
    let config = config(&root).await;
    let stop = CancellationToken::new();
    let server = tokio::spawn(run_with_credentials(
        config.clone_for_test(),
        stop,
        Some(ADMIN.into()),
        Some(STREAM_TOKEN.into()),
    ));
    wait_status(&config).await;
    let status = json_cli(&config, &["--output", "json", "server", "status"]).await;
    assert_eq!(status["lifecycle"], "running");

    let device = DeviceClient::builder()
        .device(device_key())
        .credentials(DeviceCredentials::new("demo-device", DEVICE_SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{}", config.mqtt))
        .http_endpoint(format!("http://{}", config.device_http))
        .client_id("cli-smoke-device")
        .connect()
        .await
        .unwrap();
    let mut commands = device.commands().unwrap();
    let device_status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let output = cli(
                &config,
                &["--output", "json", "device", "status", "device-1"],
            )
            .await;
            if output.status.success()
                && serde_json::from_slice::<serde_json::Value>(&output.stdout)
                    .is_ok_and(|value| value["connected"] == true)
            {
                break output;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(device_status.status.success());

    let command_output = json_cli(
        &config,
        &[
            "--output",
            "json",
            "command",
            "send",
            "device-1",
            "--json",
            r#"{"name":"relay","arguments":{}}"#,
        ],
    )
    .await;
    let command_id: CommandId =
        serde_json::from_value(command_output["command_id"].clone()).unwrap();
    let received = tokio::time::timeout(Duration::from_secs(5), commands.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.command_id, command_id);

    let initial = json_cli(&config, &["--output", "json", "config", "get", "device-1"]).await;
    assert_eq!(initial["revision"], 1);
    let config_file = root.join("device-config.json");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(&config_file, br#"{"sample_interval_seconds":15}"#).unwrap();
    let updated = cli(
        &config,
        &[
            "config",
            "set",
            "device-1",
            "--file",
            config_file.to_str().unwrap(),
            "--revision",
            "2",
        ],
    )
    .await;
    assert!(updated.status.success());

    let mut monitor_command = command(&config);
    monitor_command
        .args([
            "--output",
            "json",
            "events",
            "subscribe",
            "--type",
            "heartbeat",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut monitor: Child = monitor_command.spawn().unwrap();
    let stdout = monitor.stdout.take().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let accepted = device.heartbeat(99).await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(stdout).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let event: DeviceEvent = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(event.event_id, accepted.event_id);
    tokio::time::sleep(Duration::from_millis(50)).await;
    monitor.kill().await.unwrap();
    let _ = monitor.wait().await;

    let invalidated = json_cli(
        &config,
        &[
            "--output",
            "json",
            "auth",
            "invalidate",
            "--device",
            "device-1",
        ],
    )
    .await;
    assert!(invalidated["invalidated"].as_u64().unwrap() >= 1);
    device.shutdown();
    let drained = cli(&config, &["server", "drain", "--yes"]).await;
    assert!(drained.status.success());
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let _ = std::fs::remove_dir_all(root);
}

trait CloneConfigForTest {
    fn clone_for_test(&self) -> Config;
}

impl CloneConfigForTest for Config {
    fn clone_for_test(&self) -> Config {
        serde_json::from_value(serde_json::to_value(self).unwrap()).unwrap()
    }
}
