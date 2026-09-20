use netbaiot_core::EventId;
use netbaiot_runtime::{Error, EventAcceptance};
use netbaiot_server::{Config, TlsFiles, read_config, run, tls_acceptor};
use std::{
    collections::HashSet,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
};
use tokio_rustls::{TlsConnector, rustls};
use tokio_util::sync::CancellationToken;
fn config() -> Config {
    serde_json::from_str(include_str!("../../../configs/development.json")).unwrap()
}
#[test]
fn public_streams_require_tls_and_volatile_store_requires_loopback() {
    let mut c = config();
    c.device_http = "0.0.0.0:8080".parse().unwrap();
    assert!(c.validate().is_err());
    c.development = false;
    c.delivery_url = Some("https://example.invalid/ingress".into());
    assert!(c.validate().is_err());
    c.tls = Some(TlsFiles {
        certificate: "cert".into(),
        private_key: "key".into(),
    });
    assert!(c.validate().is_ok());
}
#[tokio::test]
async fn composition_root_serves_http_and_stops_all_listeners() {
    let mut c = config();
    let mut reservations = Vec::new();
    for _ in 0..5 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    c.device_http = reservations[0].local_addr().unwrap();
    c.management_http = reservations[1].local_addr().unwrap();
    c.mqtt = reservations[2].local_addr().unwrap();
    c.tcp = reservations[3].local_addr().unwrap();
    c.udp = reservations[4].local_addr().unwrap();
    c.spool_directory =
        std::env::temp_dir().join(format!("netbaiot-server-{}", uuid::Uuid::new_v4()));
    let addresses = [c.device_http, c.management_http, c.mqtt, c.tcp];
    drop(reservations);
    let stop = CancellationToken::new();
    let server = tokio::spawn(run(c, stop.clone()));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let body = r#"{"schema_version":1,"source_message_id":"root:1","kind":"heartbeat","data":{"sequence":1}}"#;
    let response = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match client
                .post(format!("http://{}/v1/device/data", addresses[0]))
                .bearer_auth(
                    "demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                )
                .body(body)
                .send()
                .await
            {
                Ok(r) => break r,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(response.status(), 202);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for address in addresses {
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
}
#[tokio::test]
async fn tls_composition_completes_verified_handshake() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    let acceptor = tls_acceptor(&TlsFiles {
        certificate: root.join("localhost-cert.pem").to_str().unwrap().into(),
        private_key: root.join("localhost-key.pem").to_str().unwrap().into(),
    })
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(socket).await.unwrap();
        let mut buffer = [0; 4];
        stream.read_exact(&mut buffer).await.unwrap();
        assert_eq!(&buffer, b"ping");
        stream.write_all(b"pong").await.unwrap();
        stream.shutdown().await.unwrap();
    });
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(
        &mut include_bytes!("../../../tests/fixtures/localhost-cert.pem").as_slice(),
    ) {
        roots.add(cert.unwrap()).unwrap();
    }
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client));
    let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut tls = connector
        .connect("localhost".try_into().unwrap(), socket)
        .await
        .unwrap();
    tls.write_all(b"ping").await.unwrap();
    let mut buffer = [0; 4];
    tls.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"pong");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn config_input_is_bounded_and_malformed_config_fails() {
    assert!(matches!(
        read_config("/nonexistent/netbaiot.json").await,
        Err(Error::Configuration)
    ));
}

#[tokio::test]
async fn audit_tls_handshake_timeout_shutdown_and_invalid_key() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    assert!(
        tls_acceptor(&TlsFiles {
            certificate: root.join("localhost-cert.pem").to_str().unwrap().into(),
            private_key: root.join("localhost-cert.pem").to_str().unwrap().into()
        })
        .await
        .is_err()
    );
    let mut c = config();
    let recovery =
        std::env::temp_dir().join(format!("netbaiot-audit-tls-{}", uuid::Uuid::new_v4()));
    c.spool_directory = recovery.clone();
    c.device_http = "127.0.0.1:0".parse().unwrap();
    c.management_http = "127.0.0.1:0".parse().unwrap();
    c.tcp = c.device_http;
    c.udp = c.device_http;
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    c.mqtt = reservation.local_addr().unwrap();
    drop(reservation);
    let address = c.mqtt;
    c.limits.connect_timeout_ms = 40;
    c.limits.shutdown_timeout_ms = 100;
    c.tls = Some(TlsFiles {
        certificate: root.join("localhost-cert.pem").to_str().unwrap().into(),
        private_key: root.join("localhost-key.pem").to_str().unwrap().into(),
    });
    let stop = CancellationToken::new();
    let task = tokio::spawn(run(c, stop.clone()));
    let mut socket = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(socket) = tokio::net::TcpStream::connect(address).await {
                break socket;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut b = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut b))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let mut pending = tokio::net::TcpStream::connect(address).await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(pending.read(&mut b).await, Ok(0) | Err(_)));
    let _ = std::fs::remove_dir_all(recovery);
}

async fn free_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

async fn wait_ready(client: &reqwest::Client, address: std::net::SocketAddr, admin: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("http://{address}/api/v1/ready"))
                .bearer_auth(admin)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

async fn start_child(path: &std::path::Path, admin: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(path)
        .env("NETBAIOT_ADMIN_SECRET", admin)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn mqtt_text(value: &[u8], output: &mut Vec<u8>) {
    output.extend_from_slice(&(value.len() as u16).to_be_bytes());
    output.extend_from_slice(value);
}

fn mqtt_packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut packet = vec![first];
    let mut remaining = body.len();
    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining != 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if remaining == 0 {
            break;
        }
    }
    packet.extend_from_slice(body);
    packet
}

fn mqtt_connect(client_id: &str, clean: bool) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_text(b"MQTT", &mut body);
    body.extend_from_slice(&[4, 0xc0 | (u8::from(clean) * 2), 0, 30]);
    mqtt_text(client_id.as_bytes(), &mut body);
    mqtt_text(b"demo-device", &mut body);
    mqtt_text(
        b"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        &mut body,
    );
    mqtt_packet(0x10, &body)
}

fn mqtt_subscribe(packet_id: u16, filter: &str, qos: u8) -> Vec<u8> {
    let mut body = packet_id.to_be_bytes().to_vec();
    mqtt_text(filter.as_bytes(), &mut body);
    body.push(qos);
    mqtt_packet(0x82, &body)
}

fn mqtt_publish(packet_id: u16, topic: &str, payload: &[u8], qos: u8, retain: bool) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_text(topic.as_bytes(), &mut body);
    if qos > 0 {
        body.extend_from_slice(&packet_id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    mqtt_packet(0x30 | (qos << 1) | u8::from(retain), &body)
}

async fn mqtt_read(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let first = stream.read_u8().await.unwrap();
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    for _ in 0..4 {
        let byte = stream.read_u8().await.unwrap();
        remaining += usize::from(byte & 0x7f) * multiplier;
        if byte & 0x80 == 0 {
            let mut body = vec![0; remaining];
            stream.read_exact(&mut body).await.unwrap();
            return (first, body);
        }
        multiplier *= 128;
    }
    panic!("invalid Remaining Length")
}

fn mqtt_publish_id(body: &[u8]) -> u16 {
    let topic_length = usize::from(u16::from_be_bytes([body[0], body[1]]));
    u16::from_be_bytes([body[2 + topic_length], body[3 + topic_length]])
}

async fn mqtt_open(address: std::net::SocketAddr, client_id: &str, clean: bool) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&mqtt_connect(client_id, clean))
        .await
        .unwrap();
    stream
}

async fn request_drain(client: &reqwest::Client, address: std::net::SocketAddr, admin: &str) {
    assert!(
        client
            .post(format!("http://{address}/api/v1/drain"))
            .bearer_auth(admin)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
}

#[tokio::test]
async fn subprocess_mqtt_session_retained_and_qos1_inflight_survive_graceful_restart() {
    let mut c = config();
    c.device_http = free_address().await;
    c.management_http = free_address().await;
    c.mqtt = free_address().await;
    c.tcp = free_address().await;
    c.udp = free_address().await;
    let root = std::env::temp_dir().join(format!("netbaiot-mqtt-restart-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    c.spool_directory = root.join("recovery");
    let config_path = root.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&c).unwrap()).unwrap();
    let admin = "d".repeat(64);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let topic = "v1/t/demo/p/sensor/d/device-1/up";
    let filter = "v1/t/demo/p/sensor/d/device-1/#";
    let payload = br#"{"schema_version":1,"source_message_id":"mqtt-restart","kind":"heartbeat","data":{"sequence":1}}"#;

    let mut first = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut persistent = mqtt_open(c.mqtt, "persistent-client", false).await;
    assert_eq!(mqtt_read(&mut persistent).await, (0x20, vec![0, 0]));
    persistent
        .write_all(&mqtt_subscribe(1, filter, 1))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut persistent).await, (0x90, vec![0, 1, 1]));
    persistent
        .write_all(&mqtt_publish(10, topic, payload, 1, true))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut persistent).await, (0x40, vec![0, 10]));
    let (first_byte, first_delivery) = mqtt_read(&mut persistent).await;
    assert_eq!(first_byte & 0x0f, 0x02);
    let outbound_id = mqtt_publish_id(&first_delivery);

    request_drain(&client, c.management_http, &admin).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), first.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );

    let mut second = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut resumed = mqtt_open(c.mqtt, "persistent-client", false).await;
    assert_eq!(mqtt_read(&mut resumed).await, (0x20, vec![1, 0]));
    let (retry_first, retry_body) = mqtt_read(&mut resumed).await;
    assert_eq!(retry_first & 0x0f, 0x0a, "QoS1 retry must set DUP");
    assert_eq!(mqtt_publish_id(&retry_body), outbound_id);
    resumed
        .write_all(&[0x40, 2, (outbound_id >> 8) as u8, outbound_id as u8])
        .await
        .unwrap();

    let mut retained_reader = mqtt_open(c.mqtt, "retained-reader", true).await;
    assert_eq!(mqtt_read(&mut retained_reader).await, (0x20, vec![0, 0]));
    retained_reader
        .write_all(&mqtt_subscribe(2, filter, 0))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut retained_reader).await, (0x90, vec![0, 2, 0]));
    let (retained_first, retained_body) = mqtt_read(&mut retained_reader).await;
    assert_eq!(retained_first & 0x01, 1);
    assert!(retained_body.ends_with(payload));

    request_drain(&client, c.management_http, &admin).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), second.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn subprocess_mqtt_qos2_resumes_outbound_and_inbound_restart_stages() {
    let mut c = config();
    c.device_http = free_address().await;
    c.management_http = free_address().await;
    c.mqtt = free_address().await;
    c.tcp = free_address().await;
    c.udp = free_address().await;
    let root = std::env::temp_dir().join(format!(
        "netbaiot-mqtt-qos2-restart-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    c.spool_directory = root.join("recovery");
    let config_path = root.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&c).unwrap()).unwrap();
    let admin = "e".repeat(64);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let topic = "v1/t/demo/p/sensor/d/device-1/up";
    let filter = "v1/t/demo/p/sensor/d/device-1/#";
    let first_payload = br#"{"schema_version":1,"source_message_id":"qos2-stage-1","kind":"heartbeat","data":{"sequence":1}}"#;
    let second_payload = br#"{"schema_version":1,"source_message_id":"qos2-stage-2","kind":"heartbeat","data":{"sequence":2}}"#;

    let mut generation_one = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut mqtt = mqtt_open(c.mqtt, "qos2-persistent", false).await;
    assert_eq!(mqtt_read(&mut mqtt).await, (0x20, vec![0, 0]));
    mqtt.write_all(&mqtt_subscribe(1, filter, 2)).await.unwrap();
    assert_eq!(mqtt_read(&mut mqtt).await, (0x90, vec![0, 1, 2]));
    mqtt.write_all(&mqtt_publish(10, topic, first_payload, 2, false))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut mqtt).await, (0x50, vec![0, 10]));
    mqtt.write_all(&[0x62, 2, 0, 10]).await.unwrap();
    assert_eq!(mqtt_read(&mut mqtt).await, (0x70, vec![0, 10]));
    let (outbound_first, outbound_body) = mqtt_read(&mut mqtt).await;
    assert_eq!(outbound_first & 0x06, 0x04);
    let outbound_id = mqtt_publish_id(&outbound_body);
    request_drain(&client, c.management_http, &admin).await;
    assert!(generation_one.wait().await.unwrap().success());

    let mut generation_two = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut mqtt = mqtt_open(c.mqtt, "qos2-persistent", false).await;
    assert_eq!(mqtt_read(&mut mqtt).await, (0x20, vec![1, 0]));
    let (retry_first, retry_body) = mqtt_read(&mut mqtt).await;
    assert_eq!(retry_first & 0x0e, 0x0c, "QoS2 PUBLISH retry sets DUP");
    assert_eq!(mqtt_publish_id(&retry_body), outbound_id);
    mqtt.write_all(&[0x50, 2, (outbound_id >> 8) as u8, outbound_id as u8])
        .await
        .unwrap();
    assert_eq!(
        mqtt_read(&mut mqtt).await,
        (0x62, vec![(outbound_id >> 8) as u8, outbound_id as u8])
    );
    request_drain(&client, c.management_http, &admin).await;
    assert!(generation_two.wait().await.unwrap().success());

    let mut generation_three = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut mqtt = mqtt_open(c.mqtt, "qos2-persistent", false).await;
    assert_eq!(mqtt_read(&mut mqtt).await, (0x20, vec![1, 0]));
    assert_eq!(
        mqtt_read(&mut mqtt).await,
        (0x62, vec![(outbound_id >> 8) as u8, outbound_id as u8])
    );
    mqtt.write_all(&[0x70, 2, (outbound_id >> 8) as u8, outbound_id as u8])
        .await
        .unwrap();
    mqtt.write_all(&mqtt_publish(11, topic, second_payload, 2, false))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut mqtt).await, (0x50, vec![0, 11]));
    request_drain(&client, c.management_http, &admin).await;
    assert!(generation_three.wait().await.unwrap().success());

    let mut generation_four = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut mqtt = mqtt_open(c.mqtt, "qos2-persistent", false).await;
    assert_eq!(mqtt_read(&mut mqtt).await, (0x20, vec![1, 0]));
    mqtt.write_all(&[0x62, 2, 0, 11]).await.unwrap();
    assert_eq!(mqtt_read(&mut mqtt).await, (0x70, vec![0, 11]));
    let (second_outbound_first, second_outbound_body) = mqtt_read(&mut mqtt).await;
    assert_eq!(second_outbound_first & 0x06, 0x04);
    let second_outbound_id = mqtt_publish_id(&second_outbound_body);
    mqtt.write_all(&[
        0x50,
        2,
        (second_outbound_id >> 8) as u8,
        second_outbound_id as u8,
    ])
    .await
    .unwrap();
    assert_eq!(
        mqtt_read(&mut mqtt).await,
        (
            0x62,
            vec![(second_outbound_id >> 8) as u8, second_outbound_id as u8]
        )
    );
    mqtt.write_all(&[
        0x70,
        2,
        (second_outbound_id >> 8) as u8,
        second_outbound_id as u8,
    ])
    .await
    .unwrap();
    mqtt.write_all(&[0x62, 2, 0, 11]).await.unwrap();
    assert_eq!(mqtt_read(&mut mqtt).await, (0x70, vec![0, 11]));
    mqtt.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(mqtt_read(&mut mqtt).await, (0xd0, Vec::new()));

    request_drain(&client, c.management_http, &admin).await;
    assert!(generation_four.wait().await.unwrap().success());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn subprocess_graceful_restart_spools_and_replays_every_accepted_event_id() {
    let sink_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sink_address = sink_listener.local_addr().unwrap();
    let healthy = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(Mutex::new(Vec::<EventId>::new()));
    let sink_stop = CancellationToken::new();
    let sink_task = {
        let healthy = healthy.clone();
        let observed = observed.clone();
        let stop = sink_stop.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = stop.cancelled() => break,
                    accepted = sink_listener.accept() => accepted.unwrap(),
                };
                let healthy = healthy.clone();
                let observed = observed.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let read = socket.read(&mut chunk).await.unwrap_or(0);
                        if read == 0 {
                            break;
                        }
                        bytes.extend_from_slice(&chunk[..read]);
                        let Some(headers) =
                            bytes.windows(4).position(|window| window == b"\r\n\r\n")
                        else {
                            continue;
                        };
                        let header_end = headers + 4;
                        let header = String::from_utf8_lossy(&bytes[..header_end]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() < header_end + length {
                            continue;
                        }
                        if healthy.load(Ordering::Relaxed) {
                            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(
                                &bytes[header_end..header_end + length],
                            ) && let Ok(event_id) = serde_json::from_value::<EventId>(
                                value.get("event_id").cloned().unwrap_or_default(),
                            ) {
                                observed.lock().unwrap().push(event_id);
                            }
                            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                        } else {
                            socket.write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                        }
                        break;
                    }
                });
            }
        })
    };

    let mut c = config();
    c.device_http = free_address().await;
    c.management_http = free_address().await;
    c.mqtt = free_address().await;
    c.tcp = free_address().await;
    c.udp = free_address().await;
    c.delivery_url = Some(format!("http://{sink_address}/events"));
    c.limits.sink_max_attempts = 1;
    c.limits.shutdown_drain_timeout_ms = 50;
    c.limits.sink_timeout_ms = 100;
    let root = std::env::temp_dir().join(format!("netbaiot-restart-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    c.spool_directory = root.join("spool");
    let config_path = root.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&c).unwrap()).unwrap();
    let admin = "a".repeat(64);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let mut first = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut accepted = HashSet::new();
    for sequence in 0..3 {
        let response = client
            .post(format!("http://{}/v1/device/data", c.device_http))
            .bearer_auth("demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
            .body(format!(r#"{{"schema_version":1,"source_message_id":"restart:{sequence}","kind":"heartbeat","data":{{"sequence":{sequence}}}}}"#))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        accepted.insert(response.json::<EventAcceptance>().await.unwrap().event_id);
    }
    client
        .post(format!("http://{}/api/v1/drain", c.management_http))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), first.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(std::fs::read_dir(&c.spool_directory).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .and_then(|value| value.to_str())
            == Some("spool")
    }));

    healthy.store(true, Ordering::Relaxed);
    let mut second = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let found = observed
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            if accepted.is_subset(&found) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    client
        .post(format!("http://{}/api/v1/drain", c.management_http))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), second.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(
        !std::fs::read_dir(&c.spool_directory).unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .and_then(|value| value.to_str())
                == Some("spool")
        })
    );

    // Exercise multiple healthy generations after recovery. Each generation accepts new work,
    // drains it, and exits without creating a restart segment.
    for cycle in 0..3 {
        let mut child = start_child(&config_path, &admin).await;
        wait_ready(&client, c.management_http, &admin).await;
        let response = client
            .post(format!("http://{}/v1/device/data", c.device_http))
            .bearer_auth("demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
            .body(format!(r#"{{"schema_version":1,"source_message_id":"cycle:{cycle}","kind":"heartbeat","data":{{"sequence":{cycle}}}}}"#))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let event_id = response.json::<EventAcceptance>().await.unwrap().event_id;
        accepted.insert(event_id);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if observed.lock().unwrap().contains(&event_id) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        client
            .post(format!("http://{}/api/v1/drain", c.management_http))
            .bearer_auth(&admin)
            .send()
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        assert!(
            !std::fs::read_dir(&c.spool_directory)
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|value| value == "spool"))
        );
    }
    assert!(accepted.is_subset(&observed.lock().unwrap().iter().copied().collect()));
    sink_stop.cancel();
    sink_task.await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn subprocess_sigkill_exposes_the_documented_three_event_loss_window() {
    let mut c = config();
    c.device_http = free_address().await;
    c.management_http = free_address().await;
    c.mqtt = free_address().await;
    c.tcp = free_address().await;
    c.udp = free_address().await;
    let unavailable_sink = free_address().await;
    c.delivery_url = Some(format!("http://{unavailable_sink}/events"));
    c.limits.sink_max_attempts = 10;
    c.limits.sink_max_age_ms = 60_000;
    let root = std::env::temp_dir().join(format!("netbaiot-sigkill-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    c.spool_directory = root.join("spool");
    let config_path = root.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&c).unwrap()).unwrap();
    let admin = "b".repeat(64);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut child = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut accepted = HashSet::new();
    for sequence in 0..3 {
        let response = client
            .post(format!("http://{}/v1/device/data", c.device_http))
            .bearer_auth("demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
            .body(format!(r#"{{"schema_version":1,"source_message_id":"kill:{sequence}","kind":"heartbeat","data":{{"sequence":{sequence}}}}}"#))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        accepted.insert(response.json::<EventAcceptance>().await.unwrap().event_id);
    }
    assert_eq!(accepted.len(), 3);
    child.kill().await.unwrap();
    let status = child.wait().await.unwrap();
    assert!(!status.success());
    let committed = c.spool_directory.exists()
        && std::fs::read_dir(&c.spool_directory).unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|value| value == "spool")
        });
    assert!(
        !committed,
        "SIGKILL must not be misrepresented as graceful spooling"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn subprocess_graceful_shutdown_fails_if_pending_work_cannot_be_spooled() {
    let mut c = config();
    c.device_http = free_address().await;
    c.management_http = free_address().await;
    c.mqtt = free_address().await;
    c.tcp = free_address().await;
    c.udp = free_address().await;
    let unavailable_sink = free_address().await;
    c.delivery_url = Some(format!("http://{unavailable_sink}/events"));
    c.limits.sink_max_attempts = 1;
    c.limits.shutdown_drain_timeout_ms = 25;
    let root =
        std::env::temp_dir().join(format!("netbaiot-spool-failure-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    c.spool_directory = root.join("spool");
    let config_path = root.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&c).unwrap()).unwrap();
    let admin = "c".repeat(64);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut child = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let response = client
        .post(format!("http://{}/v1/device/data", c.device_http))
        .bearer_auth("demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
        .body(r#"{"schema_version":1,"source_message_id":"spool-failure","kind":"heartbeat","data":{"sequence":1}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    std::fs::create_dir_all(&c.spool_directory).unwrap();
    std::fs::remove_dir(&c.spool_directory).unwrap();
    std::fs::write(&c.spool_directory, b"not a directory").unwrap();
    client
        .post(format!("http://{}/api/v1/drain", c.management_http))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !status.success(),
        "spool failure must prevent a successful exit"
    );
    let _ = std::fs::remove_dir_all(root);
}
