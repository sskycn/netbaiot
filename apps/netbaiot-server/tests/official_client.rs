use futures_util::StreamExt;
use netbaiot_client::{ClientError, NetbaIoTClient};
use netbaiot_device_sdk::{ConfigUpdate, DeviceClient, DeviceCredentials};
use netbaiot_protocol::*;
use netbaiot_server::Config;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    process::{Child, Command},
};

const DEVICE_SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

async fn test_config(root: &Path) -> Config {
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
    drop(reservations);
    drop(udp);
    config.delivery_url = None;
    config.spool_directory = root.join("spool");
    config.limits.sink_timeout_ms = 2_000;
    config.limits.shutdown_drain_timeout_ms = 50;
    config
}

fn write_config(root: &Path, config: &Config) -> PathBuf {
    std::fs::create_dir_all(root).unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec_pretty(config).unwrap()).unwrap();
    path
}

fn start_server(path: &Path, admin: &str, stream_token: &str) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"));
    command
        .arg(path)
        .env("NETBAIOT_ADMIN_SECRET", admin)
        .env("NETBAIOT_BUSINESS_STREAM_TOKEN", stream_token)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command.spawn().unwrap()
}

async fn business_client(config: &Config, admin: &str, stream_token: &str) -> NetbaIoTClient {
    NetbaIoTClient::builder()
        .endpoint(format!("http://{}", config.management_http))
        .token(admin)
        .event_token(stream_token)
        .event_address(config.business_tcp.unwrap())
        .stream_handshake_timeout(Duration::from_millis(200))
        .connect()
        .await
        .unwrap()
}

async fn wait_ready(client: &NetbaIoTClient) {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client.runtime().status().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "server did not become ready");
}

fn device_key() -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new("demo").unwrap(),
        product_id: ProductId::new("sensor").unwrap(),
        device_id: DeviceId::new("device-1").unwrap(),
    }
}

async fn device_client(config: &Config) -> DeviceClient {
    DeviceClient::builder()
        .device(device_key())
        .credentials(DeviceCredentials::new("demo-device", DEVICE_SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{}", config.mqtt))
        .http_endpoint(format!("http://{}", config.device_http))
        .client_id("official-sdk-e2e")
        .connect()
        .await
        .unwrap()
}

async fn next_event(events: &mut netbaiot_client::EventStream) -> netbaiot_client::Delivery {
    tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

fn percentile(values: &mut [u128], percentile: usize) -> u128 {
    values.sort_unstable();
    let index = values
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(values.len().saturating_sub(1));
    values[index]
}

fn rss_kib(pid: u32) -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

#[tokio::test]
async fn official_clients_cover_event_command_config_status_and_offline_contracts() {
    let root = std::env::temp_dir().join(format!("netbaiot-official-e2e-{}", uuid::Uuid::new_v4()));
    let config = test_config(&root).await;
    let path = write_config(&root, &config);
    let admin = "d".repeat(64);
    let stream_token = "business-e2e-secret";
    let mut server = start_server(&path, &admin, stream_token);
    let business = business_client(&config, &admin, stream_token).await;
    wait_ready(&business).await;

    let mut malformed = TcpStream::connect(config.business_tcp.unwrap())
        .await
        .unwrap();
    malformed.write_all(&[0, 0, 0, 1, b'{']).await.unwrap();
    let mut response_length = [0u8; 4];
    malformed.read_exact(&mut response_length).await.unwrap();
    let mut response = vec![0; usize::try_from(u32::from_be_bytes(response_length)).unwrap()];
    malformed.read_exact(&mut response).await.unwrap();
    assert!(matches!(
        serde_json::from_slice::<StreamServerFrame>(&response).unwrap(),
        StreamServerFrame::Error {
            error: ApiError {
                code: ErrorCode::InvalidRequest,
                ..
            },
            ..
        }
    ));

    let rejected = business_client(&config, &admin, "incorrect-stream-token").await;
    let rejected = rejected.events().subscribe(EventFilter::default()).await;
    assert!(matches!(rejected, Err(ClientError::Unauthenticated { .. })));

    let rejected_device = DeviceClient::builder()
        .device(device_key())
        .credentials(DeviceCredentials::new("unknown-device", DEVICE_SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{}", config.mqtt))
        .client_id("official-sdk-rejected")
        .mqtt_connect_timeout(Duration::from_secs(2))
        .connect()
        .await;
    assert!(
        matches!(
            &rejected_device,
            Err(netbaiot_device_sdk::DeviceSdkError::Unauthenticated)
        ),
        "unexpected rejected-device result: {rejected_device:?}"
    );

    let mut events = business
        .events()
        .subscribe(EventFilter::default())
        .await
        .unwrap();
    let device = device_client(&config).await;
    let mut commands = device.commands().unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if business
                .devices()
                .connection(&device_key())
                .await
                .is_ok_and(|value| value.connected && value.connected_at.is_some())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let accepted = device.heartbeat(7).await.unwrap();
    let delivery = next_event(&mut events).await;
    assert_eq!(delivery.event_id(), accepted.event_id);
    assert!(matches!(
        delivery.event().kind,
        DeviceEventKind::Heartbeat(_)
    ));
    delivery.ack().await.unwrap();

    let command_id = CommandId::generate();
    let dispatch = business
        .commands()
        .send(&DeviceCommand {
            command_id,
            device: device_key(),
            expires_at: None,
            payload: DeviceCommandPayload {
                name: "relay".into(),
                arguments: Default::default(),
            },
        })
        .await
        .unwrap();
    assert_eq!(dispatch.command_id, command_id);
    let command = tokio::time::timeout(Duration::from_secs(5), commands.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.command_id, command_id);
    device
        .ack_command(command_id, ExecutionState::Succeeded)
        .await
        .unwrap();
    let delivery = next_event(&mut events).await;
    let DeviceEventKind::CommandAck(ack) = &delivery.event().kind else {
        panic!("expected command ACK event");
    };
    assert_eq!(ack.command_id, command_id);
    delivery.ack().await.unwrap();

    let config_value = DeviceConfig {
        device: device_key(),
        revision: ConfigRevision::new(42).unwrap(),
        payload: Arc::new(serde_json::json!({"sample_interval_seconds": 5})),
    };
    business
        .configs()
        .set_device_config(&config_value)
        .await
        .unwrap();
    let read_back = business
        .configs()
        .get_device_config(&device_key())
        .await
        .unwrap();
    assert_eq!(read_back.revision, ConfigRevision::new(42).unwrap());
    let ConfigUpdate::Updated(device_config) = device.config().check(None).await.unwrap() else {
        panic!("expected updated config");
    };
    assert_eq!(device_config.revision, ConfigRevision::new(42).unwrap());
    device
        .config()
        .ack(
            ConfigRevision::new(42).unwrap(),
            ConfigApplyStatus::Applied,
            None,
        )
        .await
        .unwrap();
    let delivery = next_event(&mut events).await;
    assert!(matches!(
        &delivery.event().kind,
        DeviceEventKind::ConfigAck(ack)
            if ack.revision == ConfigRevision::new(42).unwrap()
                && ack.status == ConfigApplyStatus::Applied
    ));
    delivery.ack().await.unwrap();
    assert_eq!(
        business.runtime().status().await.unwrap().lifecycle,
        LifecycleState::Running
    );

    device.shutdown();
    drop(device);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if business
                .devices()
                .connection(&device_key())
                .await
                .is_ok_and(|value| !value.connected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let offline = business
        .commands()
        .send(&DeviceCommand {
            command_id: CommandId::generate(),
            device: device_key(),
            expires_at: None,
            payload: DeviceCommandPayload {
                name: "offline".into(),
                arguments: Default::default(),
            },
        })
        .await;
    assert!(matches!(offline, Err(ClientError::DeviceOffline { .. })));

    events.close();
    business.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), server.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn official_client_reconnects_and_replays_unacked_event_with_same_id() {
    let root =
        std::env::temp_dir().join(format!("netbaiot-client-replay-{}", uuid::Uuid::new_v4()));
    let config = test_config(&root).await;
    let path = write_config(&root, &config);
    let admin = "e".repeat(64);
    let stream_token = "business-replay-secret";
    let mut first = start_server(&path, &admin, stream_token);
    let business = business_client(&config, &admin, stream_token).await;
    wait_ready(&business).await;
    let mut events = business
        .events()
        .subscribe(EventFilter::default())
        .await
        .unwrap();
    let device = device_client(&config).await;
    let mut commands = device.commands().unwrap();
    let accepted = device.heartbeat(9).await.unwrap();
    let first_delivery = next_event(&mut events).await;
    assert_eq!(first_delivery.event_id(), accepted.event_id);

    business.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), first.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let mut second = start_server(&path, &admin, stream_token);
    wait_ready(&business).await;
    let replay = next_event(&mut events).await;
    assert_eq!(replay.event_id(), first_delivery.event_id());
    assert_ne!(replay.delivery_id(), first_delivery.delivery_id());
    assert!(matches!(
        first_delivery.ack().await,
        Err(ClientError::ConnectionLost)
    ));
    replay.ack().await.unwrap();

    // The server registers the MQTT session before the SDK receives SUBACK for its command
    // subscription. Server-side presence therefore cannot prove that the SDK publish path is
    // ready; wait on the SDK's stricter connection boundary before the first post-restart send.
    device
        .wait_until_connected(Duration::from_secs(5))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if business
                .devices()
                .connection(&device_key())
                .await
                .is_ok_and(|value| value.connected && value.connected_at.is_some())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    device
        .publish_telemetry(BTreeMap::from([(
            "restart_probe".into(),
            Scalar::Boolean(true),
        )]))
        .await
        .unwrap();
    let after_restart = next_event(&mut events).await;
    assert!(matches!(
        after_restart.event().kind,
        DeviceEventKind::Telemetry(_)
    ));
    after_restart.ack().await.unwrap();

    let command_id = CommandId::generate();
    business
        .commands()
        .send(&DeviceCommand {
            command_id,
            device: device_key(),
            expires_at: None,
            payload: DeviceCommandPayload {
                name: "after-restart".into(),
                arguments: Default::default(),
            },
        })
        .await
        .unwrap();
    let command = tokio::time::timeout(Duration::from_secs(5), commands.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.command_id, command_id);
    assert!(device.metrics().mqtt_reconnects >= 1);

    device.shutdown();
    events.close();
    business.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), second.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert_eq!(business.metrics().events_received, 3);
    assert!(business.metrics().reconnects >= 1);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn official_client_throughput_latency_and_idle_memory_measurement() {
    const EVENTS: u64 = 200;
    let root = std::env::temp_dir().join(format!("netbaiot-client-perf-{}", uuid::Uuid::new_v4()));
    let mut config = test_config(&root).await;
    config.limits.requests_per_second = 10_000;
    config.limits.requests_per_ip_second = 10_000;
    config.limits.messages_per_device_second = 10_000;
    config.limits.messages_per_tenant_second = 10_000;
    let path = write_config(&root, &config);
    let admin = "a".repeat(64);
    let stream_token = "business-performance-secret";
    let mut server = start_server(&path, &admin, stream_token);
    let process_before_client = rss_kib(std::process::id());
    let business = business_client(&config, &admin, stream_token).await;
    wait_ready(&business).await;
    let process_with_client = rss_kib(std::process::id());
    let mut events = business
        .events()
        .subscribe(EventFilter::default())
        .await
        .unwrap();
    let device = device_client(&config).await;
    let process_with_device = rss_kib(std::process::id());
    let server_rss = server.id().and_then(rss_kib);
    let mut delivery_us = Vec::with_capacity(EVENTS as usize);
    let mut ack_us = Vec::with_capacity(EVENTS as usize);
    let started = Instant::now();
    for sequence in 0..EVENTS {
        let accepted = device.heartbeat(sequence).await.unwrap();
        let accepted_at = Instant::now();
        let delivery = next_event(&mut events).await;
        assert_eq!(delivery.event_id(), accepted.event_id);
        delivery_us.push(accepted_at.elapsed().as_micros());
        let ack_started = Instant::now();
        delivery.ack().await.unwrap();
        ack_us.push(ack_started.elapsed().as_micros());
    }
    let elapsed = started.elapsed();
    let throughput = EVENTS as f64 / elapsed.as_secs_f64();
    let (delivery_p50, delivery_p95, delivery_p99) = (
        percentile(&mut delivery_us.clone(), 50),
        percentile(&mut delivery_us.clone(), 95),
        percentile(&mut delivery_us, 99),
    );
    let (ack_p50, ack_p95, ack_p99) = (
        percentile(&mut ack_us.clone(), 50),
        percentile(&mut ack_us.clone(), 95),
        percentile(&mut ack_us, 99),
    );
    println!(
        "SDK_PERF events={EVENTS} events_per_second={throughput:.2} delivery_us_p50={delivery_p50} delivery_us_p95={delivery_p95} delivery_us_p99={delivery_p99} ack_us_p50={ack_p50} ack_us_p95={ack_p95} ack_us_p99={ack_p99} process_rss_before_client_kib={process_before_client:?} process_rss_with_client_kib={process_with_client:?} process_rss_with_device_kib={process_with_device:?} server_rss_kib={server_rss:?}"
    );
    device.shutdown();
    events.close();
    business.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), server.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let _ = std::fs::remove_dir_all(root);
}
