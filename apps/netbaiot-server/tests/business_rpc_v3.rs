use async_trait::async_trait;
use netbaiot_client::NetbaIoTClient;
use netbaiot_client::business_rpc::{
    BusinessAuthHandler, BusinessRpcClient, BusinessRpcClientConfig, BusinessRpcClientError,
    BusinessRpcV3Client, BusinessRpcV3ClientConfig,
};
use netbaiot_core::{
    AuthInvalidation, CodecId, CommandId, DeliveryState, DeviceCommand, DeviceCommandPayload,
    DeviceEventKind, DeviceId, DeviceKey, DeviceUplink, DeviceUplinkKind, ExecutionState,
    Heartbeat, ProductId, Scalar, SourceMessageId, TenantId,
    business_rpc::{
        AuthenticatedDeviceWire, BusinessRole, DeviceAuthenticateRequest, DeviceCommandSendRequest,
        ResolveVerifierRequest, ResolveVerifierResponse, RpcError, RpcErrorCode,
    },
    business_rpc_v3::{
        V3_END_STREAM, V3Bootstrap, V3FrameHeader, V3FrameType, V3Limits, V3Open, V3Reset,
        V3ResetCode, V3Response,
    },
};
use netbaiot_device_sdk::{DeviceClient, DeviceCredentials, PublishQos};
use netbaiot_server::{BusinessRpcConfig, Config, DeviceAuthSource, EventDeliverySource};
use std::{
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
};

const SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
struct Handler {
    calls: AtomicUsize,
    inflight: AtomicUsize,
    peak_inflight: AtomicUsize,
}
#[async_trait]
impl BusinessAuthHandler for Handler {
    async fn authenticate(
        &self,
        request: DeviceAuthenticateRequest,
    ) -> Result<AuthenticatedDeviceWire, RpcError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let active = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_inflight.fetch_max(active, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        let Some(device_id) = request.credential_id.strip_prefix("cred-") else {
            return Err(RpcError::new(
                netbaiot_core::business_rpc::RpcErrorCode::DeviceRejected,
                "unknown credential",
            ));
        };
        if device_id != "v3" && !device_id.starts_with("v3-concurrent-") {
            return Err(RpcError::new(
                netbaiot_core::business_rpc::RpcErrorCode::DeviceRejected,
                "unknown credential",
            ));
        }
        let mut identity = identity_for(device_id);
        identity.auth_revision = request.min_auth_revision.max(1);
        Ok(identity)
    }
    async fn resolve_verifier(
        &self,
        _request: ResolveVerifierRequest,
    ) -> Result<ResolveVerifierResponse, RpcError> {
        Ok(ResolveVerifierResponse {
            identity: identity(),
            verifier_key_hex: SECRET.into(),
        })
    }
}
fn identity() -> AuthenticatedDeviceWire {
    identity_for("v3")
}
fn identity_for(device_id: &str) -> AuthenticatedDeviceWire {
    AuthenticatedDeviceWire {
        device_key: DeviceKey {
            tenant_id: TenantId::new("demo").unwrap(),
            product_id: ProductId::new("sensor").unwrap(),
            device_id: DeviceId::new(device_id).unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        publish: true,
        commands: true,
        auth_revision: 1,
    }
}

fn command(name: &str) -> DeviceCommand {
    DeviceCommand {
        command_id: CommandId::generate(),
        device: DeviceKey {
            tenant_id: TenantId::new("demo").unwrap(),
            product_id: ProductId::new("sensor").unwrap(),
            device_id: DeviceId::new("device-1").unwrap(),
        },
        expires_at: None,
        payload: DeviceCommandPayload {
            name: name.into(),
            arguments: Default::default(),
        },
    }
}

async fn read_mqtt_packet(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut first = [0u8; 1];
    socket.read_exact(&mut first).await.unwrap();
    let mut remaining = 0usize;
    let mut multiplier = 1usize;
    loop {
        let mut digit = [0u8; 1];
        socket.read_exact(&mut digit).await.unwrap();
        remaining += usize::from(digit[0] & 0x7f) * multiplier;
        assert!(remaining <= 16_384);
        if digit[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
    }
    let mut body = vec![0; remaining];
    socket.read_exact(&mut body).await.unwrap();
    (first[0], body)
}

async fn write_v3_frame(socket: &mut TcpStream, id: u32, ty: V3FrameType, flags: u8, body: &[u8]) {
    socket
        .write_all(
            &V3FrameHeader {
                payload_len: body.len() as u32,
                stream_id: id,
                frame_type: ty,
                flags,
            }
            .encode(),
        )
        .await
        .unwrap();
    socket.write_all(body).await.unwrap();
}

async fn read_v3_frame(socket: &mut TcpStream) -> (V3FrameHeader, Vec<u8>) {
    let mut header = [0u8; 12];
    socket.read_exact(&mut header).await.unwrap();
    let header = V3FrameHeader::parse(&header, 8192).unwrap();
    let mut body = vec![0u8; header.payload_len as usize];
    socket.read_exact(&mut body).await.unwrap();
    (header, body)
}

async fn read_tcp_device(socket: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.unwrap();
    let mut body = vec![0u8; u32::from_be_bytes(header) as usize];
    socket.read_exact(&mut body).await.unwrap();
    body
}

#[tokio::test]
async fn v3_command_real_tcp_short_dedup_ttl_and_shorter_command_ttl() {
    let root = std::env::temp_dir().join(format!("netbaiot-v3-tcp-ttl-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let reservations = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let addresses = reservations
        .each_ref()
        .map(|socket| socket.local_addr().unwrap());
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::Static);
    config.event_delivery = Some(EventDeliverySource::DevelopmentAudit);
    config.limits.command_ttl_ms = 100;
    config.limits.command_dedup_ttl_ms = 500;
    config.spool_directory = root.join("spool");
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: Some(V3Limits::default()),
        v3_send_ahead: None,
        v3_experiment_socket_send_buffer_bytes: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: Some(BusinessRole::Application),
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 0,
    });
    config.validate().unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    drop(reservations);
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .env("NETBAIOT_BUSINESS_RPC_TOKEN", "v3-ttl-token")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut client_config =
        BusinessRpcV3ClientConfig::development(addresses[2], "v3-ttl-token".into());
    client_config.provider = false;
    client_config.events = false;
    let (business, _) = BusinessRpcV3Client::connect(client_config, None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let mut device = TcpStream::connect(addresses[0]).await.unwrap();
    let hello = serde_json::to_vec(&serde_json::json!({
        "credential_id": "demo-device",
        "secret": SECRET,
    }))
    .unwrap();
    device
        .write_all(&(hello.len() as u32).to_be_bytes())
        .await
        .unwrap();
    device.write_all(&hello).await.unwrap();
    assert_eq!(
        read_tcp_device(&mut device).await,
        br#"{"authenticated":true}"#
    );

    let command = command("short-ttl");
    let first = business.send_command(&command).await.unwrap();
    assert_eq!(first.state, DeliveryState::Queued);
    let delivered: DeviceCommand =
        serde_json::from_slice(&read_tcp_device(&mut device).await).unwrap();
    assert_eq!(delivered.command_id, command.command_id);
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The 100 ms execution TTL has elapsed, but the 500 ms dedup receipt lives.
    assert_eq!(business.send_command(&command).await.unwrap(), first);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), read_tcp_device(&mut device))
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Once retention expires, the same caller input may establish new work.
    assert_eq!(business.send_command(&command).await.unwrap(), first);
    let again: DeviceCommand = serde_json::from_slice(&read_tcp_device(&mut device).await).unwrap();
    assert_eq!(again.command_id, command.command_id);
    business.shutdown().await;
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn v3_planned_restart_replays_event_but_resets_command_dedup() {
    let root = std::env::temp_dir().join(format!("netbaiot-v3-restart-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let reservations = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let addresses = reservations
        .each_ref()
        .map(|socket| socket.local_addr().unwrap());
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::Static);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    config.limits.sink_timeout_ms = 300;
    config.limits.shutdown_drain_timeout_ms = 300;
    config.spool_directory = root.join("spool");
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: Some(V3Limits::default()),
        v3_send_ahead: None,
        v3_experiment_socket_send_buffer_bytes: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: Some(BusinessRole::Application),
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 0,
    });
    config.validate().unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    drop(reservations);
    let start = || {
        Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
            .arg(&path)
            .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
            .env("NETBAIOT_BUSINESS_RPC_TOKEN", "v3-restart-token")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    };
    let client_config = || {
        let mut value =
            BusinessRpcV3ClientConfig::development(addresses[2], "v3-restart-token".into());
        value.provider = false;
        value
    };
    let connect_device = || async {
        let mut device = TcpStream::connect(addresses[0]).await.unwrap();
        let hello = serde_json::to_vec(&serde_json::json!({
            "credential_id": "demo-device",
            "secret": SECRET,
        }))
        .unwrap();
        device
            .write_all(&(hello.len() as u32).to_be_bytes())
            .await
            .unwrap();
        device.write_all(&hello).await.unwrap();
        assert_eq!(
            read_tcp_device(&mut device).await,
            br#"{"authenticated":true}"#
        );
        device
    };
    let mut first = start();
    let (business, mut events) = BusinessRpcV3Client::connect(client_config(), None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let mut device = connect_device().await;
    let command = command("restart-duplicate-is-allowed");
    assert_eq!(
        business.send_command(&command).await.unwrap().state,
        DeliveryState::Queued
    );
    let delivered: DeviceCommand =
        serde_json::from_slice(&read_tcp_device(&mut device).await).unwrap();
    assert_eq!(delivered.command_id, command.command_id);
    let uplink = DeviceUplink::new(
        SourceMessageId::new("v3-restart-heartbeat").unwrap(),
        DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 1 }),
    );
    let body = serde_json::to_vec(&uplink).unwrap();
    device
        .write_all(&(body.len() as u32).to_be_bytes())
        .await
        .unwrap();
    device.write_all(&body).await.unwrap();
    let _accepted = read_tcp_device(&mut device).await;
    let first_delivery = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    let event_id = first_delivery.delivery.event.event_id;
    let admin = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses[1]))
        .token("a".repeat(64))
        .connect()
        .await
        .unwrap();
    admin.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(8), first.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    drop(first_delivery);
    business.shutdown().await;
    drop(device);

    let mut second = start();
    let (business, mut events) = BusinessRpcV3Client::connect(client_config(), None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let replay = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.delivery.event.event_id, event_id);
    replay.ack().await.unwrap();
    let metrics = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("http://{}/api/v1/metrics", addresses[1]))
        .bearer_auth("a".repeat(64))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("netbaiot_command_dedup_entries 0\n"));
    let mut device = connect_device().await;
    assert_eq!(
        business.send_command(&command).await.unwrap().state,
        DeliveryState::Queued
    );
    let second_delivery: DeviceCommand =
        serde_json::from_slice(&read_tcp_device(&mut device).await).unwrap();
    assert_eq!(second_delivery.command_id, command.command_id);
    business.shutdown().await;
    second.start_kill().unwrap();
    let _ = second.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn v3_command_real_tcp_tenant_scope_and_capacity() {
    use netbaiot_client::business_rpc::BusinessRpcTls;
    use netbaiot_server::{BusinessRpcIdentityConfig, ManagementTlsFiles};
    use sha2::{Digest, Sha256};

    let root =
        std::env::temp_dir().join(format!("netbaiot-v3-tcp-command-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses: Vec<_> = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::Static);
    config.event_delivery = Some(EventDeliverySource::DevelopmentAudit);
    config.limits.max_pending_commands_per_device = 1;
    config.limits.max_pending_commands_per_tenant = 1;
    config.limits.max_pending_commands = 1;
    config.limits.command_dedup_max_entries = 2;
    config.spool_directory = root.join("spool");
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    let pem = std::fs::read(fixtures.join("management-client.pem")).unwrap();
    let cert = rustls_pemfile::certs(&mut pem.as_slice())
        .next()
        .unwrap()
        .unwrap();
    let fingerprint = Sha256::digest(cert.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: Some(V3Limits::default()),
        v3_send_ahead: None,
        v3_experiment_socket_send_buffer_bytes: None,
        tls: Some(ManagementTlsFiles {
            certificate: fixtures.join("localhost-cert.pem").to_string_lossy().into(),
            private_key: fixtures.join("localhost-key.pem").to_string_lossy().into(),
            client_ca: Some(fixtures.join("management-ca.pem").to_string_lossy().into()),
            require_client_certificate: true,
        }),
        identities: vec![BusinessRpcIdentityConfig {
            certificate_sha256: fingerprint,
            principal_id: "v3-command".into(),
            role: BusinessRole::Commands,
            provider_id: None,
            sink_id: None,
            provide_methods: Vec::new(),
            call_methods: vec!["device.command.send".into()],
            global: false,
            tenants: vec![TenantId::new("demo").unwrap()],
            expires_at_ms: None,
        }],
        development_token_env: None,
        development_role: None,
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 0,
    });
    config.validate().unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    drop(reservations);
    let stderr = std::fs::File::create(root.join("server.log")).unwrap();
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut client_config = BusinessRpcV3ClientConfig::development(addresses[2], "unused".into());
    client_config.token = None;
    client_config.tls = Some(BusinessRpcTls {
        server_name: "localhost".into(),
        ca_pem: fixtures.join("localhost-cert.pem"),
        certificate_pem: fixtures.join("management-client.pem"),
        private_key_pem: fixtures.join("management-client-key.pem"),
    });
    client_config.provider = false;
    client_config.events = false;
    let (business, _) = BusinessRpcV3Client::connect(client_config, None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let mut v2_config =
        BusinessRpcClientConfig::development(addresses[2], "unused".into(), BusinessRole::Commands);
    v2_config.token = None;
    v2_config.tls = Some(BusinessRpcTls {
        server_name: "localhost".into(),
        ca_pem: fixtures.join("localhost-cert.pem"),
        certificate_pem: fixtures.join("management-client.pem"),
        private_key_pem: fixtures.join("management-client-key.pem"),
    });
    let (v2, _) = BusinessRpcClient::connect(v2_config, None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), v2.wait_ready())
        .await
        .unwrap()
        .unwrap();

    let mut socket = TcpStream::connect(addresses[0]).await.unwrap();
    let handshake =
        serde_json::to_vec(&serde_json::json!({"credential_id":"demo-device","secret":SECRET}))
            .unwrap();
    socket
        .write_all(&(handshake.len() as u32).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(&handshake).await.unwrap();
    assert_eq!(
        read_tcp_device(&mut socket).await,
        br#"{"authenticated":true}"#
    );
    let accepted_command = command("v3-tcp");
    let mut forbidden = accepted_command.clone();
    forbidden.device.tenant_id = TenantId::new("other").unwrap();
    assert!(matches!(
        business.send_command(&forbidden).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Forbidden))
    ));
    let accepted = business.send_command(&accepted_command).await.unwrap();
    assert_eq!(accepted.state, DeliveryState::Queued);
    let received: DeviceCommand =
        serde_json::from_slice(&read_tcp_device(&mut socket).await).unwrap();
    assert_eq!(received.command_id, accepted_command.command_id);
    assert_eq!(received.payload, accepted_command.payload);
    assert_eq!(v2.send_command(&accepted_command).await.unwrap(), accepted);
    let v2_first = command("v2-first");
    let v2_receipt = v2.send_command(&v2_first).await.unwrap();
    let received: DeviceCommand =
        serde_json::from_slice(&read_tcp_device(&mut socket).await).unwrap();
    assert_eq!(received.command_id, v2_first.command_id);
    assert_eq!(business.send_command(&v2_first).await.unwrap(), v2_receipt);
    let full = business.send_command(&command("capacity")).await;
    assert!(
        matches!(
            full,
            Err(BusinessRpcClientError::Remote(RpcErrorCode::Overloaded))
        ),
        "{full:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), read_tcp_device(&mut socket))
            .await
            .is_err()
    );
    v2.shutdown().await;
    business.shutdown().await;
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn v3_command_real_mqtt_dedup_http_ack_and_lost_response() {
    let root = std::env::temp_dir().join(format!("netbaiot-v3-command-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses: Vec<_> = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::Static);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    config.spool_directory = root.join("spool");
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: Some(V3Limits::default()),
        v3_send_ahead: None,
        v3_experiment_socket_send_buffer_bytes: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: Some(BusinessRole::Application),
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 32,
        max_auth_control_offline_ms: 0,
    });
    config.validate().unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    drop(reservations);
    let stderr = std::fs::File::create(root.join("server.log")).unwrap();
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .env("NETBAIOT_BUSINESS_RPC_TOKEN", "v3-command-token")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut client_config =
        BusinessRpcV3ClientConfig::development(addresses[2], "v3-command-token".into());
    client_config.provider = false;
    let (business, mut events) = BusinessRpcV3Client::connect(client_config, None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let admin = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses[1]))
        .token("a".repeat(64))
        .connect()
        .await
        .unwrap();

    let retry = command("offline-retry");
    assert!(matches!(
        business.send_command(&retry).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Unavailable))
    ));
    let mut invalid = retry.clone();
    invalid.command_id = CommandId(uuid::Uuid::nil());
    assert!(matches!(
        business.send_command(&invalid).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::InvalidRequest))
    ));

    let mut old_mqtt = TcpStream::connect(addresses[0]).await.unwrap();
    let mut connect_body = Vec::new();
    for text in [
        b"MQTT".as_slice(),
        b"v3-unready",
        b"demo-device",
        SECRET.as_bytes(),
    ] {
        connect_body.extend_from_slice(&(text.len() as u16).to_be_bytes());
        connect_body.extend_from_slice(text);
        if text == b"MQTT" {
            connect_body.extend_from_slice(&[4, 0xc2, 0, 30]);
        }
    }
    let mut packet = vec![0x10, connect_body.len() as u8];
    packet.extend_from_slice(&connect_body);
    old_mqtt.write_all(&packet).await.unwrap();
    let mut connack = [0u8; 4];
    old_mqtt.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack, [0x20, 2, 0, 0]);
    assert!(matches!(
        business.send_command(&retry).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Unavailable))
    ));
    let down = b"v1/t/demo/p/sensor/d/device-1/down";
    let mut subscription = vec![0, 1];
    subscription.extend_from_slice(&(down.len() as u16).to_be_bytes());
    subscription.extend_from_slice(down);
    subscription.push(1);
    let mut subscribe = vec![0x82, subscription.len() as u8];
    subscribe.extend_from_slice(&subscription);
    old_mqtt.write_all(&subscribe).await.unwrap();
    assert_eq!(read_mqtt_packet(&mut old_mqtt).await.0, 0x90);
    let old = command("old-generation");
    business.send_command(&old).await.unwrap();
    let (header, body) = read_mqtt_packet(&mut old_mqtt).await;
    assert_eq!(header & 0xf0, 0x30);
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let packet_id_at = topic_len + 2;
    let packet_id = u16::from_be_bytes([body[packet_id_at], body[packet_id_at + 1]]);
    let delivered: DeviceCommand = serde_json::from_slice(&body[packet_id_at + 2..]).unwrap();
    assert_eq!(delivered.command_id, old.command_id);
    old_mqtt
        .write_all(&[0x40, 2, (packet_id >> 8) as u8, packet_id as u8])
        .await
        .unwrap();

    let device = DeviceClient::builder()
        .device(retry.device.clone())
        .credentials(DeviceCredentials::new("demo-device", SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{}", addresses[0]))
        .client_id("v3-command-mqtt")
        .connect()
        .await
        .unwrap();
    let mut commands = device.commands().unwrap();
    device
        .wait_until_connected(Duration::from_secs(5))
        .await
        .unwrap();
    let mut stale = [0u8; 1];
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), old_mqtt.read(&mut stale)).await,
        Ok(Ok(0)) | Ok(Err(_))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), commands.recv())
            .await
            .is_err()
    );
    let accepted = business.send_command(&retry).await.unwrap();
    assert_eq!(accepted.state, DeliveryState::Queued);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        retry.command_id
    );
    assert_eq!(business.send_command(&retry).await.unwrap(), accepted);
    assert_eq!(admin.commands().send(&retry).await.unwrap(), accepted);

    let from_http = command("http-first");
    let http_dispatch = admin.commands().send(&from_http).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        from_http.command_id
    );
    assert_eq!(
        business.send_command(&from_http).await.unwrap(),
        http_dispatch
    );

    let mut fragmented = command("fragmented-v3-data");
    for index in 0..40 {
        fragmented
            .payload
            .arguments
            .insert(format!("field-{index}"), Scalar::Text("x".repeat(220)));
    }
    assert_eq!(
        business.send_command(&fragmented).await.unwrap().state,
        DeliveryState::Queued
    );
    let large_received = tokio::time::timeout(Duration::from_secs(5), commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(large_received.command_id, fragmented.command_id);
    assert_eq!(large_received.payload, fragmented.payload);

    let simultaneous = command("concurrent-first");
    let mut concurrent = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let business = business.clone();
        let cmd = simultaneous.clone();
        concurrent.spawn(async move { business.send_command(&cmd).await });
    }
    while let Some(result) = concurrent.join_next().await {
        assert_eq!(result.unwrap().unwrap().command_id, simultaneous.command_id);
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        simultaneous.command_id
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), commands.recv())
            .await
            .is_err()
    );
    let mut changed = retry.clone();
    changed.payload.name = "different".into();
    assert!(matches!(
        business.send_command(&changed).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Conflict))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), commands.recv())
            .await
            .is_err()
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(200), events.recv())
            .await
            .is_err()
    );
    device
        .ack_command(retry.command_id, ExecutionState::Succeeded)
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&delivery.delivery.event.kind, DeviceEventKind::CommandAck(ack) if ack.command_id == retry.command_id && ack.execution == ExecutionState::Succeeded)
    );
    delivery.ack().await.unwrap();

    let lost = command("lost-response");
    let mut raw = TcpStream::connect(addresses[2]).await.unwrap();
    let hello = serde_json::to_vec(&V3Bootstrap::Hello {
        version: 3,
        token: Some("v3-command-token".into()),
        limits: V3Limits::default(),
    })
    .unwrap();
    raw.write_all(&(hello.len() as u32).to_be_bytes())
        .await
        .unwrap();
    raw.write_all(&hello).await.unwrap();
    let mut prefix = [0u8; 4];
    raw.read_exact(&mut prefix).await.unwrap();
    let mut ready = vec![0u8; u32::from_be_bytes(prefix) as usize];
    raw.read_exact(&mut ready).await.unwrap();
    assert!(matches!(
        serde_json::from_slice::<V3Bootstrap>(&ready).unwrap(),
        V3Bootstrap::Ready { .. }
    ));
    let malformed = br#"{"command":"bad"}"#;
    let malformed_open = V3Open::Rpc {
        parent_stream_id: None,
        request_id: uuid::Uuid::new_v4(),
        method: "device.command.send".into(),
        deadline_ms: 5000,
        content_length: malformed.len() as u32,
    };
    write_v3_frame(
        &mut raw,
        1,
        V3FrameType::Open,
        0,
        &serde_json::to_vec(&malformed_open).unwrap(),
    )
    .await;
    write_v3_frame(&mut raw, 1, V3FrameType::Data, V3_END_STREAM, malformed).await;
    let (header, response) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = read_v3_frame(&mut raw).await;
            if frame.0.stream_id == 1 && frame.0.frame_type == V3FrameType::Response {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(header.stream_id, 1);
    assert!(matches!(
        serde_json::from_slice::<V3Response>(&response)
            .unwrap()
            .error,
        Some(RpcError {
            code: RpcErrorCode::InvalidRequest,
            ..
        })
    ));
    let cancelled = command("cancel-before-body");
    let cancelled_body = serde_json::to_vec(&DeviceCommandSendRequest {
        command: cancelled.clone(),
    })
    .unwrap();
    let cancelled_open = V3Open::Rpc {
        parent_stream_id: None,
        request_id: uuid::Uuid::new_v4(),
        method: "device.command.send".into(),
        deadline_ms: 5000,
        content_length: cancelled_body.len() as u32,
    };
    write_v3_frame(
        &mut raw,
        3,
        V3FrameType::Open,
        0,
        &serde_json::to_vec(&cancelled_open).unwrap(),
    )
    .await;
    let cancel = serde_json::to_vec(&V3Reset {
        code: V3ResetCode::Cancel,
        message: String::new(),
    })
    .unwrap();
    write_v3_frame(&mut raw, 3, V3FrameType::ResetStream, 0, &cancel).await;
    assert_eq!(
        business.send_command(&cancelled).await.unwrap().state,
        DeliveryState::Queued
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        cancelled.command_id
    );
    let expired = command("body-deadline");
    let expired_body = serde_json::to_vec(&DeviceCommandSendRequest {
        command: expired.clone(),
    })
    .unwrap();
    let expired_open = V3Open::Rpc {
        parent_stream_id: None,
        request_id: uuid::Uuid::new_v4(),
        method: "device.command.send".into(),
        deadline_ms: 1,
        content_length: expired_body.len() as u32,
    };
    write_v3_frame(
        &mut raw,
        5,
        V3FrameType::Open,
        0,
        &serde_json::to_vec(&expired_open).unwrap(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        business.send_command(&expired).await.unwrap().state,
        DeliveryState::Queued
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        expired.command_id
    );

    let body = serde_json::to_vec(&DeviceCommandSendRequest {
        command: lost.clone(),
    })
    .unwrap();
    let open = V3Open::Rpc {
        parent_stream_id: None,
        request_id: uuid::Uuid::new_v4(),
        method: "device.command.send".into(),
        deadline_ms: 5000,
        content_length: body.len() as u32,
    };
    write_v3_frame(
        &mut raw,
        7,
        V3FrameType::Open,
        0,
        &serde_json::to_vec(&open).unwrap(),
    )
    .await;
    write_v3_frame(&mut raw, 7, V3FrameType::Data, V3_END_STREAM, &body).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        lost.command_id
    );
    write_v3_frame(&mut raw, 7, V3FrameType::ResetStream, 0, &cancel).await;
    drop(raw);
    assert_eq!(
        business.send_command(&lost).await.unwrap().command_id,
        lost.command_id
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), commands.recv())
            .await
            .is_err()
    );

    business.shutdown().await;
    device.shutdown();
    // Closing the V3 client must release every command RPC stream, queued body,
    // and reassembly reservation, including the reset and lost-response paths.
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut observed = String::new();
    let cleanup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let request = http
                .get(format!("http://{}/api/v1/metrics", addresses[1]))
                .bearer_auth("a".repeat(64))
                .send()
                .await;
            let Ok(metrics) = request else {
                observed = format!("request error: {:?}", request.err());
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            assert_eq!(metrics.status(), reqwest::StatusCode::OK);
            let Ok(body) = metrics.text().await else {
                observed = "response body error".into();
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            observed = body
                .lines()
                .filter(|line| line.starts_with("netbaiot_business_rpc_v3_"))
                .collect::<Vec<_>>()
                .join("; ");
            if [
                "netbaiot_business_rpc_v3_active_connections 0\n",
                "netbaiot_business_rpc_v3_active_streams 0\n",
                "netbaiot_business_rpc_v3_queued_bytes 0\n",
                "netbaiot_business_rpc_v3_reassembly_reserved_bytes 0\n",
            ]
            .iter()
            .all(|line| body.contains(line))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(cleanup.is_ok(), "V3 cleanup timed out: {observed}");
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn real_socket_v3_provider_event_and_invalidation() {
    let root = std::env::temp_dir().join(format!("netbaiot-v3-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses: Vec<_> = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::BusinessRpc);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    config.spool_directory = root.join("spool");
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: Some(V3Limits::default()),
        v3_send_ahead: None,
        v3_experiment_socket_send_buffer_bytes: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: Some(BusinessRole::Multiplexed),
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 30_000,
    });
    config.validate().unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    drop(reservations);
    let stderr = std::fs::File::create(root.join("server.log")).unwrap();
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .env("NETBAIOT_BUSINESS_RPC_TOKEN", "v3-test-token")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let handler = Arc::new(Handler {
        calls: AtomicUsize::new(0),
        inflight: AtomicUsize::new(0),
        peak_inflight: AtomicUsize::new(0),
    });
    let v2_config = BusinessRpcClientConfig::development(
        addresses[2],
        "v3-test-token".into(),
        BusinessRole::Multiplexed,
    );
    let (v2, _) = BusinessRpcClient::connect(v2_config, Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), v2.wait_ready())
        .await
        .expect("V2 coexistence deadline")
        .unwrap();
    v2.shutdown().await;
    let client_config =
        BusinessRpcV3ClientConfig::development(addresses[2], "v3-test-token".into());
    let (client, mut events) =
        BusinessRpcV3Client::connect(client_config, Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), client.wait_ready())
        .await
        .expect("V3 readiness deadline")
        .unwrap();
    assert!(matches!(
        client.send_command(&command("method-forbidden")).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Forbidden))
    ));
    let mut replacement_config =
        BusinessRpcV3ClientConfig::development(addresses[2], "v3-test-token".into());
    replacement_config.events = false;
    let (replacement, _) =
        BusinessRpcV3Client::connect(replacement_config, Some(handler.clone())).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), replacement.wait_ready())
            .await
            .is_err()
    );
    let device = DeviceClient::builder()
        .device(identity().device_key)
        .credentials(DeviceCredentials::new("cred-v3", SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{}", addresses[0]))
        .client_id("v3-e2e")
        .connect()
        .await
        .unwrap();
    device
        .publish(
            DeviceUplink::new(
                SourceMessageId::new("v3-heartbeat").unwrap(),
                DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 1 }),
            ),
            PublishQos::AtLeastOnce,
        )
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        delivery.delivery.event.kind,
        DeviceEventKind::Heartbeat(_)
    ));
    assert!(!delivery.delivery.event.event_id.0.is_nil());
    delivery.ack().await.unwrap();
    device
        .publish(
            DeviceUplink::new(
                SourceMessageId::new("v3-heartbeat-retry").unwrap(),
                DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 2 }),
            ),
            PublishQos::AtLeastOnce,
        )
        .await
        .unwrap();
    let first_attempt = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    let retry_event_id = first_attempt.delivery.event.event_id;
    let first_delivery_id = first_attempt.delivery.delivery_id;
    first_attempt.nack().await.unwrap();
    let second_attempt = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_attempt.delivery.event.event_id, retry_event_id);
    assert_ne!(second_attempt.delivery.delivery_id, first_delivery_id);
    second_attempt.ack().await.unwrap();
    assert!(handler.calls.load(Ordering::Relaxed) >= 1);
    let _ = client
        .invalidate(
            2,
            AuthInvalidation::Device {
                device: identity().device_key,
            },
        )
        .await
        .unwrap();
    let mut concurrent = tokio::task::JoinSet::new();
    for index in 0..8 {
        let endpoint = addresses[0];
        concurrent.spawn(async move {
            let id = format!("v3-concurrent-{index}");
            let device = DeviceClient::builder()
                .device(identity_for(&id).device_key)
                .credentials(DeviceCredentials::new(format!("cred-{id}"), SECRET).unwrap())
                .mqtt_endpoint(format!("mqtt://{endpoint}"))
                .client_id(format!("v3-concurrent-client-{index}"))
                .connect()
                .await
                .unwrap();
            device.shutdown();
        });
    }
    let concurrent_result = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = concurrent.join_next().await {
            result.unwrap();
        }
    })
    .await;
    assert!(
        concurrent_result.is_ok(),
        "concurrent auth timed out: calls={}, peak={}, ready={}",
        handler.calls.load(Ordering::Relaxed),
        handler.peak_inflight.load(Ordering::Relaxed),
        client.ready()
    );
    assert!(handler.peak_inflight.load(Ordering::Relaxed) >= 2);
    assert!(handler.calls.load(Ordering::Relaxed) >= 9);
    client.shutdown().await;
    tokio::time::timeout(Duration::from_secs(10), replacement.wait_ready())
        .await
        .expect("replacement provider readiness deadline")
        .unwrap();
    replacement.shutdown().await;
    device.shutdown();
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn v3_hello_is_rejected_when_v3_is_disabled() {
    let root = std::env::temp_dir().join(format!("netbaiot-v3-disabled-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses: Vec<_> = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.spool_directory = root.join("spool");
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        v3_send_ahead: None,
        v3_experiment_socket_send_buffer_bytes: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: Some(BusinessRole::Multiplexed),
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 30_000,
    });
    config.validate().unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    drop(reservations);
    let stderr = std::fs::File::create(root.join("server.log")).unwrap();
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .env("NETBAIOT_BUSINESS_RPC_TOKEN", "v3-test-token")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut socket = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(socket) = TcpStream::connect(addresses[2]).await {
                break socket;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let hello = serde_json::to_vec(&V3Bootstrap::Hello {
        version: 3,
        token: Some("v3-test-token".into()),
        limits: V3Limits::default(),
    })
    .unwrap();
    socket
        .write_all(&(hello.len() as u32).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(&hello).await.unwrap();
    let mut response = [0; 1];
    let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut response))
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(n, 0, "disabled V3 must close without Ready");
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn v3_mtls_authenticates_before_role_free_stream_authorization() {
    use netbaiot_client::business_rpc::BusinessRpcTls;
    use netbaiot_server::{BusinessRpcIdentityConfig, ManagementTlsFiles};
    use sha2::{Digest, Sha256};
    use std::path::Path;

    let root = std::env::temp_dir().join(format!("netbaiot-v3-mtls-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    let certificate = std::fs::read(fixtures.join("management-client.pem")).unwrap();
    let certificate = rustls_pemfile::certs(&mut certificate.as_slice())
        .next()
        .unwrap()
        .unwrap();
    let fingerprint = Sha256::digest(certificate.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses: Vec<_> = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::BusinessRpc);
    config.event_delivery = Some(EventDeliverySource::DevelopmentAudit);
    config.spool_directory = root.join("spool");
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: Some(V3Limits::default()),
        v3_send_ahead: None,
        v3_experiment_socket_send_buffer_bytes: None,
        tls: Some(ManagementTlsFiles {
            certificate: fixtures.join("localhost-cert.pem").to_string_lossy().into(),
            private_key: fixtures.join("localhost-key.pem").to_string_lossy().into(),
            client_ca: Some(fixtures.join("management-ca.pem").to_string_lossy().into()),
            require_client_certificate: true,
        }),
        identities: vec![BusinessRpcIdentityConfig {
            certificate_sha256: fingerprint,
            principal_id: "v3-provider".into(),
            role: BusinessRole::AuthControl,
            provider_id: Some("primary".into()),
            sink_id: None,
            provide_methods: vec![
                "device.authenticate".into(),
                "device.resolve_verifier".into(),
            ],
            call_methods: vec!["auth.sync".into(), "auth.invalidate".into()],
            global: false,
            tenants: vec![TenantId::new("demo").unwrap()],
            expires_at_ms: None,
        }],
        development_token_env: None,
        development_role: None,
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 0,
    });
    config.validate().unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    drop(reservations);
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let handler = Arc::new(Handler {
        calls: AtomicUsize::new(0),
        inflight: AtomicUsize::new(0),
        peak_inflight: AtomicUsize::new(0),
    });
    let mut client_config = BusinessRpcV3ClientConfig::development(addresses[2], String::new());
    client_config.token = None;
    client_config.events = false;
    client_config.tls = Some(BusinessRpcTls {
        server_name: "localhost".into(),
        ca_pem: fixtures.join("localhost-cert.pem"),
        certificate_pem: fixtures.join("management-client.pem"),
        private_key_pem: fixtures.join("management-client-key.pem"),
    });
    let (provider, _) =
        BusinessRpcV3Client::connect(client_config.clone(), Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), provider.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let mut unauthorized = client_config.clone();
    unauthorized.provider = false;
    unauthorized.events = true;
    let (events_only, _) = BusinessRpcV3Client::connect(unauthorized, None).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), events_only.wait_ready())
            .await
            .is_err()
    );
    let mut wrong_name = client_config;
    wrong_name.tls.as_mut().unwrap().server_name = "wrong.local".into();
    let (wrong_server, _) =
        BusinessRpcV3Client::connect(wrong_name, Some(handler.clone())).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), wrong_server.wait_ready())
            .await
            .is_err()
    );
    let device = DeviceClient::builder()
        .device(identity().device_key)
        .credentials(DeviceCredentials::new("cred-v3", SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{}", addresses[0]))
        .client_id("v3-mtls-device")
        .connect()
        .await
        .unwrap();
    assert!(handler.calls.load(Ordering::Relaxed) >= 1);
    device.shutdown();
    wrong_server.shutdown().await;
    events_only.shutdown().await;
    provider.shutdown().await;
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}
