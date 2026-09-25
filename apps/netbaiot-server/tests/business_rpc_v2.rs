use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_client::NetbaIoTClient;
use netbaiot_client::business_rpc::{
    BusinessAuthHandler, BusinessRpcClient, BusinessRpcClientConfig,
};
use netbaiot_core::{
    AuthInvalidation, CodecId, CommandId, DeliveryState, DeviceCommand, DeviceCommandPayload,
    DeviceEventKind, DeviceId, DeviceKey, DeviceUplink, DeviceUplinkKind, ExecutionState,
    Heartbeat, ProductId, SourceMessageId, TenantId,
};
use netbaiot_device_sdk::{DeviceClient, DeviceCredentials, PublishQos};
use netbaiot_protocol::business_rpc::{
    AuthenticatedDeviceWire, BusinessRole, DeviceAuthenticateRequest, ResolveVerifierRequest,
    ResolveVerifierResponse, RpcError, RpcErrorCode,
};
use netbaiot_server::{BusinessRpcConfig, Config, DeviceAuthSource, EventDeliverySource};
use std::{
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    process::Command,
};

async fn write_rpc(
    socket: &mut TcpStream,
    frame: &netbaiot_protocol::business_rpc::BusinessRpcFrame,
) {
    let data = serde_json::to_vec(frame).unwrap();
    socket
        .write_all(&(data.len() as u32).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(&data).await.unwrap();
}

async fn read_rpc(socket: &mut TcpStream) -> netbaiot_protocol::business_rpc::BusinessRpcFrame {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.unwrap();
    let mut body = vec![0; u32::from_be_bytes(header) as usize];
    socket.read_exact(&mut body).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn read_tcp_device(socket: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await.unwrap();
    let mut body = vec![0; u32::from_be_bytes(header) as usize];
    socket.read_exact(&mut body).await.unwrap();
    body
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
        assert!(multiplier <= 128 * 128 * 128);
    }
    let mut body = vec![0; remaining];
    socket.read_exact(&mut body).await.unwrap();
    (first[0], body)
}

#[tokio::test]
async fn command_pressure_preserves_auth_invalidation_and_event_ack() {
    use netbaiot_client::business_rpc::{BusinessRpcClientError, BusinessRpcTls};
    use netbaiot_server::{BusinessRpcIdentityConfig, ManagementTlsFiles};
    use sha2::{Digest, Sha256};

    let root = std::env::temp_dir().join(format!("netbaiot-rpc-pressure-{}", uuid::Uuid::new_v4()));
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::BusinessRpc);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    config.limits.max_pending_commands_per_device = 8;
    config.limits.max_pending_commands_per_tenant = 8;
    config.limits.max_pending_commands = 8;
    config.spool_directory = root.join("spool");
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    let fingerprint = |file: &str| {
        let pem = std::fs::read(fixtures.join(file)).unwrap();
        let cert = rustls_pemfile::certs(&mut pem.as_slice())
            .next()
            .unwrap()
            .unwrap();
        Sha256::digest(cert.as_ref())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        tls: Some(ManagementTlsFiles {
            certificate: fixtures.join("localhost-cert.pem").to_string_lossy().into(),
            private_key: fixtures.join("localhost-key.pem").to_string_lossy().into(),
            client_ca: Some(
                fixtures
                    .join("business-rpc-test-client-cas.pem")
                    .to_string_lossy()
                    .into(),
            ),
            require_client_certificate: true,
        }),
        identities: vec![
            BusinessRpcIdentityConfig {
                certificate_sha256: fingerprint("management-client.pem"),
                principal_id: "provider".into(),
                role: BusinessRole::AuthControl,
                provider_id: Some("primary".into()),
                sink_id: None,
                provide_methods: vec![
                    "device.authenticate".into(),
                    "device.resolve_verifier".into(),
                ],
                call_methods: vec!["auth.sync".into(), "auth.invalidate".into()],
                global: true,
                tenants: Vec::new(),
                expires_at_ms: None,
            },
            BusinessRpcIdentityConfig {
                certificate_sha256: fingerprint("business-command-client.pem"),
                principal_id: "application".into(),
                role: BusinessRole::Application,
                provider_id: None,
                sink_id: Some("tcp-rpc".into()),
                provide_methods: Vec::new(),
                call_methods: vec!["device.command.send".into()],
                global: true,
                tenants: Vec::new(),
                expires_at_ms: None,
            },
        ],
        development_token_env: None,
        development_role: None,
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 30_000,
    });
    drop(reservations);
    let path = write_config(&root, &config);
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let tls = |cert: &str, key: &str| BusinessRpcTls {
        server_name: "localhost".into(),
        ca_pem: fixtures.join("localhost-cert.pem"),
        certificate_pem: fixtures.join(cert),
        private_key_pem: fixtures.join(key),
    };
    let handler = Arc::new(Handler {
        calls: AtomicUsize::new(0),
        verifier_calls: AtomicUsize::new(0),
        revision: AtomicU64::new(1),
        allowed: AtomicBool::new(true),
    });
    let mut auth_config = BusinessRpcClientConfig::development(
        addresses[2],
        "unused".into(),
        BusinessRole::AuthControl,
    );
    auth_config.token = None;
    auth_config.tls = Some(tls("management-client.pem", "management-client-key.pem"));
    let (auth, _) = BusinessRpcClient::connect(auth_config, Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), auth.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let mut app_config = BusinessRpcClientConfig::development(
        addresses[2],
        "unused".into(),
        BusinessRole::Application,
    );
    app_config.token = None;
    app_config.tls = Some(tls(
        "business-command-client.pem",
        "business-command-client-key.pem",
    ));
    app_config.heartbeat = Duration::from_millis(100);
    let (application, mut events) = BusinessRpcClient::connect(app_config, None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), application.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let device = device(addresses[0], "pressure").await;
    let mut command_stream = device.commands().unwrap();
    device
        .wait_until_connected(Duration::from_secs(5))
        .await
        .unwrap();
    let command_drain = tokio::spawn(async move { while command_stream.recv().await.is_some() {} });
    let stop = Arc::new(AtomicBool::new(false));
    let rejects = Arc::new(AtomicUsize::new(0));
    let mut floods = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let client = application.clone();
        let stop = stop.clone();
        let rejects = rejects.clone();
        floods.spawn(async move {
            while !stop.load(Ordering::Acquire) {
                let mut command = demo_command("pressure");
                command.device.device_id = DeviceId::new("pressure").unwrap();
                if matches!(
                    client.send_command(&command).await,
                    Err(BusinessRpcClientError::Remote(RpcErrorCode::Overloaded))
                ) {
                    rejects.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while rejects.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    publish(&device, 301).await;
    let delivery = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        delivery.delivery.event.kind,
        DeviceEventKind::Heartbeat(_)
    ));
    delivery.ack().await.unwrap();
    handler.revision.store(2, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_secs(5),
        auth.invalidate(
            2,
            AuthInvalidation::Device {
                device: identity("pressure").device_key,
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    stop.store(true, Ordering::Release);
    floods.abort_all();
    while floods.join_next().await.is_some() {}
    assert!(rejects.load(Ordering::Relaxed) > 0);
    application.shutdown().await;
    auth.shutdown().await;
    device.shutdown();
    command_drain.abort();
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn commands_role_dispatches_to_real_tcp_and_shares_http_dedup() {
    use netbaiot_client::business_rpc::{BusinessRpcClientError, BusinessRpcTls};
    use netbaiot_server::{BusinessRpcIdentityConfig, ManagementTlsFiles};
    use sha2::{Digest, Sha256};
    let root =
        std::env::temp_dir().join(format!("netbaiot-rpc-tcp-command-{}", uuid::Uuid::new_v4()));
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::Static);
    config.event_delivery = Some(EventDeliverySource::DevelopmentAudit);
    config.limits.max_pending_commands_per_device = 1;
    config.limits.max_pending_commands_per_tenant = 1;
    config.limits.max_pending_commands = 1;
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    let cert_pem = std::fs::read(fixtures.join("management-client.pem")).unwrap();
    let cert = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .next()
        .unwrap()
        .unwrap();
    let fingerprint = Sha256::digest(cert.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        tls: Some(ManagementTlsFiles {
            certificate: fixtures.join("localhost-cert.pem").to_string_lossy().into(),
            private_key: fixtures.join("localhost-key.pem").to_string_lossy().into(),
            client_ca: Some(fixtures.join("management-ca.pem").to_string_lossy().into()),
            require_client_certificate: true,
        }),
        identities: vec![BusinessRpcIdentityConfig {
            certificate_sha256: fingerprint,
            principal_id: "command-service".into(),
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
    config.spool_directory = root.join("spool");
    drop(reservations);
    let path = write_config(&root, &config);
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut client_config =
        BusinessRpcClientConfig::development(addresses[2], "unused".into(), BusinessRole::Commands);
    client_config.token = None;
    client_config.tls = Some(BusinessRpcTls {
        server_name: "localhost".into(),
        ca_pem: fixtures.join("localhost-cert.pem"),
        certificate_pem: fixtures.join("management-client.pem"),
        private_key_pem: fixtures.join("management-client-key.pem"),
    });
    let (business, mut events) = BusinessRpcClient::connect(client_config, None).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        business.invalidate(2, AuthInvalidation::All).await,
        Err(BusinessRpcClientError::Unauthorized)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err()
    );
    let mut socket = TcpStream::connect(addresses[0]).await.unwrap();
    let handshake = serde_json::to_vec(&serde_json::json!({
        "credential_id": "demo-device", "secret": SECRET,
    }))
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
    let command = demo_command("tcp-rpc");
    let mut forbidden = command.clone();
    forbidden.device.tenant_id = TenantId::new("other").unwrap();
    assert!(matches!(
        business.send_command(&forbidden).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Forbidden))
    ));
    let accepted = business.send_command(&command).await.unwrap();
    assert_eq!(accepted.state, DeliveryState::Queued);
    let received: DeviceCommand =
        serde_json::from_slice(&read_tcp_device(&mut socket).await).unwrap();
    assert_eq!(received.command_id, command.command_id);
    assert_eq!(received.payload, command.payload);
    let admin = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses[1]))
        .token("a".repeat(64))
        .connect()
        .await
        .unwrap();
    let duplicate = admin.commands().send(&command).await.unwrap();
    assert_eq!(duplicate, accepted);
    assert!(matches!(
        business.send_command(&demo_command("capacity")).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Overloaded))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), read_tcp_device(&mut socket))
            .await
            .is_err()
    );
    business.shutdown().await;
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn business_rpc_command_mqtt_dedup_and_ack_use_real_sockets() {
    use netbaiot_client::business_rpc::BusinessRpcClientError;
    use netbaiot_protocol::business_rpc::{
        BusinessLimits, BusinessRpcFrame, DeviceCommandSendRequest,
    };

    let root = std::env::temp_dir().join(format!("netbaiot-rpc-command-{}", uuid::Uuid::new_v4()));
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::Static);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: Some(BusinessRole::Application),
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 32,
        max_auth_control_offline_ms: 0,
    });
    config.spool_directory = root.join("spool");
    drop(reservations);
    let path = write_config(&root, &config);
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .env("NETBAIOT_BUSINESS_RPC_TOKEN", "rpc-command-token")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (business, mut events) = BusinessRpcClient::connect(
        BusinessRpcClientConfig::development(
            addresses[2],
            "rpc-command-token".into(),
            BusinessRole::Application,
        ),
        None,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();

    let retry = demo_command("offline-retry");
    assert!(matches!(
        business.send_command(&retry).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::Unavailable))
    ));
    let mut invalid_id = retry.clone();
    invalid_id.command_id = CommandId(uuid::Uuid::nil());
    assert!(matches!(
        business.send_command(&invalid_id).await,
        Err(BusinessRpcClientError::Remote(RpcErrorCode::InvalidRequest))
    ));
    // A real MQTT connection without a /down subscription is online but not command ready.
    let mut old_mqtt = TcpStream::connect(addresses[0]).await.unwrap();
    let mut connect_body = Vec::new();
    for text in [
        b"MQTT".as_slice(),
        b"rpc-unready",
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
    let old_command = demo_command("old-generation");
    business.send_command(&old_command).await.unwrap();
    let (publish_header, publish_body) = read_mqtt_packet(&mut old_mqtt).await;
    assert_eq!(publish_header & 0xf0, 0x30);
    let topic_bytes = u16::from_be_bytes([publish_body[0], publish_body[1]]) as usize;
    let packet_id_at = topic_bytes + 2;
    let packet_id =
        u16::from_be_bytes([publish_body[packet_id_at], publish_body[packet_id_at + 1]]);
    let delivered: DeviceCommand =
        serde_json::from_slice(&publish_body[packet_id_at + 2..]).unwrap();
    assert_eq!(delivered.command_id, old_command.command_id);
    old_mqtt
        .write_all(&[0x40, 2, (packet_id >> 8) as u8, packet_id as u8])
        .await
        .unwrap();
    let device = DeviceClient::builder()
        .device(retry.device.clone())
        .credentials(DeviceCredentials::new("demo-device", SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{}", addresses[0]))
        .client_id("rpc-command-mqtt")
        .connect()
        .await
        .unwrap();
    let mut commands = device.commands().unwrap();
    device
        .wait_until_connected(Duration::from_secs(5))
        .await
        .unwrap();
    let mut stale_byte = [0u8; 1];
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), old_mqtt.read(&mut stale_byte)).await,
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

    let same = business.send_command(&retry).await.unwrap();
    assert_eq!(same, accepted);
    // All attempts for this new ID race before any caller receives a dispatch.
    let simultaneous = demo_command("concurrent-first");
    let mut concurrent = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let business = business.clone();
        let simultaneous = simultaneous.clone();
        concurrent.spawn(async move { business.send_command(&simultaneous).await });
    }
    let mut concurrent_dispatch = None;
    while let Some(result) = concurrent.join_next().await {
        let dispatch = result.unwrap().unwrap();
        assert_eq!(dispatch.command_id, simultaneous.command_id);
        if let Some(previous) = concurrent_dispatch {
            assert_eq!(dispatch, previous);
        }
        concurrent_dispatch = Some(dispatch);
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        simultaneous.command_id
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

    // The transport ACK generated by the SDK precedes the application CommandAck.
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
        matches!(&delivery.delivery.event.kind, DeviceEventKind::CommandAck(ack)
        if ack.command_id == retry.command_id && ack.execution == ExecutionState::Succeeded)
    );
    delivery.ack().await.unwrap();

    // A downgraded events-only Hello is denied before its malformed command DTO is decoded.
    let mut events_only = TcpStream::connect(addresses[2]).await.unwrap();
    write_rpc(
        &mut events_only,
        &BusinessRpcFrame::Hello {
            version: 2,
            role: BusinessRole::Events,
            token: Some("rpc-command-token".into()),
            limits: BusinessLimits {
                max_frame_bytes: 65_536,
                auth_max_inflight: 16,
                event_max_inflight: 1,
                heartbeat_ms: 5_000,
            },
        },
    )
    .await;
    assert!(matches!(
        read_rpc(&mut events_only).await,
        BusinessRpcFrame::Ready { .. }
    ));
    write_rpc(
        &mut events_only,
        &BusinessRpcFrame::Request {
            request_id: uuid::Uuid::new_v4(),
            method: "device.command.send".into(),
            deadline_ms: 5_000,
            body: serde_json::json!({ "command": "malformed" }),
        },
    )
    .await;
    assert!(matches!(
        read_rpc(&mut events_only).await,
        BusinessRpcFrame::Response {
            error: Some(RpcError {
                code: RpcErrorCode::Forbidden,
                ..
            }),
            ..
        }
    ));
    drop(events_only);

    // A raw RPC connection can disappear after admission and before reading Response.
    let lost = demo_command("lost-response");
    let mut socket = TcpStream::connect(addresses[2]).await.unwrap();
    write_rpc(
        &mut socket,
        &BusinessRpcFrame::Hello {
            version: 2,
            role: BusinessRole::Application,
            token: Some("rpc-command-token".into()),
            limits: BusinessLimits {
                max_frame_bytes: 65_536,
                auth_max_inflight: 16,
                event_max_inflight: 1,
                heartbeat_ms: 5_000,
            },
        },
    )
    .await;
    assert!(matches!(
        read_rpc(&mut socket).await,
        BusinessRpcFrame::Ready { .. }
    ));
    write_rpc(
        &mut socket,
        &BusinessRpcFrame::Request {
            request_id: uuid::Uuid::new_v4(),
            method: "device.command.send".into(),
            deadline_ms: 5_000,
            body: serde_json::to_value(DeviceCommandSendRequest {
                command: lost.clone(),
            })
            .unwrap(),
        },
    )
    .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .unwrap()
            .unwrap()
            .command_id,
        lost.command_id
    );
    drop(socket);
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
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

fn demo_command(name: &str) -> DeviceCommand {
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

const SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
struct Handler {
    calls: AtomicUsize,
    verifier_calls: AtomicUsize,
    revision: AtomicU64,
    allowed: AtomicBool,
}
#[async_trait]
impl BusinessAuthHandler for Handler {
    async fn authenticate(
        &self,
        request: DeviceAuthenticateRequest,
    ) -> Result<AuthenticatedDeviceWire, RpcError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if request.secret_hex
            != SECRET
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
            || !request.credential_id.starts_with("cred-")
        {
            return Err(RpcError::new(
                RpcErrorCode::DeviceRejected,
                "invalid credentials",
            ));
        }
        if !self.allowed.load(Ordering::SeqCst) {
            return Err(RpcError::new(
                RpcErrorCode::DeviceRejected,
                "device disabled",
            ));
        }
        let id = request.credential_id.trim_start_matches("cred-");
        let mut value = identity(id);
        value.auth_revision = self.revision.load(Ordering::SeqCst);
        Ok(value)
    }
    async fn resolve_verifier(
        &self,
        request: ResolveVerifierRequest,
    ) -> Result<ResolveVerifierResponse, RpcError> {
        self.verifier_calls.fetch_add(1, Ordering::Relaxed);
        if !request.credential_id.starts_with("cred-") {
            return Err(RpcError::new(
                RpcErrorCode::DeviceRejected,
                "invalid credentials",
            ));
        }
        let mut value = identity(request.credential_id.trim_start_matches("cred-"));
        value.auth_revision = self.revision.load(Ordering::SeqCst);
        Ok(ResolveVerifierResponse {
            identity: value,
            verifier_key_hex: SECRET.into(),
        })
    }
}
fn identity(id: &str) -> AuthenticatedDeviceWire {
    AuthenticatedDeviceWire {
        device_key: DeviceKey {
            tenant_id: TenantId::new("demo").unwrap(),
            product_id: ProductId::new("sensor").unwrap(),
            device_id: DeviceId::new(id).unwrap(),
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
async fn device_result(
    address: std::net::SocketAddr,
    id: &str,
) -> Result<DeviceClient, netbaiot_device_sdk::DeviceSdkError> {
    DeviceClient::builder()
        .device(identity(id).device_key)
        .credentials(DeviceCredentials::new(format!("cred-{id}"), SECRET).unwrap())
        .mqtt_endpoint(format!("mqtt://{address}"))
        .client_id(format!("rpc-{id}"))
        .connect()
        .await
}
async fn device(address: std::net::SocketAddr, id: &str) -> DeviceClient {
    device_result(address, id).await.unwrap()
}
async fn publish(device: &DeviceClient, sequence: u64) {
    device
        .publish(
            DeviceUplink::new(
                SourceMessageId::new(format!("v2-{sequence}")).unwrap(),
                DeviceUplinkKind::Heartbeat(Heartbeat { sequence }),
            ),
            PublishQos::AtLeastOnce,
        )
        .await
        .unwrap();
}
async fn publish_udp(socket: &UdpSocket, address: std::net::SocketAddr, sequence: u64) {
    let payload = serde_json::to_vec(&DeviceUplink::new(
        SourceMessageId::new(format!("v2-udp-{sequence}")).unwrap(),
        DeviceUplinkKind::Heartbeat(Heartbeat { sequence }),
    ))
    .unwrap();
    let credential = b"cred-udp";
    let mut datagram = b"NBI1".to_vec();
    datagram.push(credential.len() as u8);
    datagram.extend_from_slice(credential);
    datagram.extend_from_slice(&1u32.to_be_bytes());
    datagram.extend_from_slice(&[9; 16]);
    datagram.extend_from_slice(&sequence.to_be_bytes());
    datagram.extend_from_slice(&netbaiot_runtime::now_ms().to_be_bytes());
    datagram.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    datagram.extend_from_slice(&payload);
    let key = (0..32u8).collect::<Vec<_>>();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
    mac.update(&datagram);
    datagram.extend_from_slice(&mac.finalize().into_bytes());
    socket.send_to(&datagram, address).await.unwrap();
    let mut ack = [0u8; 64];
    let (length, _) = tokio::time::timeout(Duration::from_secs(3), socket.recv_from(&mut ack))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(length, 64);
    assert_eq!(&ack[..4], b"NBA1");
}
fn write_config(root: &Path, config: &Config) -> std::path::PathBuf {
    std::fs::create_dir_all(root).unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(config).unwrap()).unwrap();
    path
}
#[tokio::test]
async fn one_socket_authentication_progresses_while_event_ack_waits() {
    let root =
        std::env::temp_dir().join(format!("netbaiot-business-rpc-v2-{}", uuid::Uuid::new_v4()));
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::BusinessRpc);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: None,
        allow_v1: true,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 1_500,
    });
    config.spool_directory = root.join("spool");
    config.limits.sink_timeout_ms = 8_000;
    drop(reservations);
    let path = write_config(&root, &config);
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .env("NETBAIOT_BUSINESS_RPC_TOKEN", "rpc-test-token")
        .env("NETBAIOT_BUSINESS_STREAM_TOKEN", "legacy-token")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let handler = Arc::new(Handler {
        calls: AtomicUsize::new(0),
        verifier_calls: AtomicUsize::new(0),
        revision: AtomicU64::new(1),
        allowed: AtomicBool::new(true),
    });
    let mut client_config = BusinessRpcClientConfig::development(
        addresses[2],
        "rpc-test-token".into(),
        BusinessRole::Multiplexed,
    );
    client_config.reconnect_initial = Duration::from_millis(1);
    client_config.reconnect_max = Duration::from_millis(1);
    let (business, mut events) =
        BusinessRpcClient::connect(client_config, Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let first = device(addresses[0], "one").await;
    publish(&first, 1).await;
    let delivery = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    // The event is deliberately unacknowledged while two fresh cache misses use this socket.
    let (two, three) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(device(addresses[0], "two"), device(addresses[0], "three"))
    })
    .await
    .unwrap();
    assert_eq!(handler.calls.load(Ordering::Relaxed), 3);
    delivery.ack().await.unwrap();
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for sequence in 1..=2 {
        publish_udp(&udp, addresses[0], sequence).await;
        let delivery = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            delivery.delivery.event.device.device_id,
            DeviceId::new("udp").unwrap()
        );
        delivery.ack().await.unwrap();
    }
    assert_eq!(handler.verifier_calls.load(Ordering::Relaxed), 1);
    publish(&two, 2).await;
    // No events.recv() call is made while a fourth authentication executes.
    let _four = tokio::time::timeout(Duration::from_secs(5), device(addresses[0], "four"))
        .await
        .unwrap();
    assert_eq!(handler.calls.load(Ordering::Relaxed), 4);
    let legacy = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses[1]))
        .token("a".repeat(64))
        .event_token("legacy-token")
        .event_address(addresses[2])
        .connect()
        .await
        .unwrap();
    assert!(
        legacy
            .events()
            .subscribe(netbaiot_core::EventFilter::default())
            .await
            .is_err(),
        "V1 must not steal V2 sink ownership"
    );
    handler.allowed.store(false, Ordering::SeqCst);
    handler.revision.store(2, Ordering::SeqCst);
    let invalidated = business
        .invalidate(
            2,
            AuthInvalidation::Device {
                device: identity("one").device_key,
            },
        )
        .await
        .unwrap();
    assert_eq!(invalidated.applied_revision, 2);
    assert!(device_result(addresses[0], "one").await.is_err());
    handler.allowed.store(true, Ordering::SeqCst);
    handler.revision.store(3, Ordering::SeqCst);
    business.invalidate(3, AuthInvalidation::All).await.unwrap();
    let duplicate = business.invalidate(3, AuthInvalidation::All).await.unwrap();
    assert_eq!(duplicate.applied_revision, 3);
    assert_eq!(duplicate.disconnected_connections, 0);
    let _recovered = device(addresses[0], "one").await;
    handler.revision.store(5, Ordering::SeqCst);
    assert!(matches!(
        business.invalidate(5, AuthInvalidation::All).await,
        Err(
            netbaiot_client::business_rpc::BusinessRpcClientError::Remote(
                RpcErrorCode::StaleRevision
            )
        )
    ));
    // A gap invalidates live authorization before the SDK's reset sync can
    // return the replacement provider to Serving.
    let mut recovery_attempts = 0usize;
    let mut last_recovery_error = None;
    let recovered_after_gap = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            recovery_attempts += 1;
            if business.ready() {
                match device_result(addresses[0], "one").await {
                    Ok(recovered) => break recovered,
                    Err(error) => last_recovery_error = Some(format!("{error:?}")),
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let _recovered_after_gap = recovered_after_gap.unwrap_or_else(|_| {
        panic!(
            "auth provider did not recover: ready={}, attempts={recovery_attempts}, last_device_error={last_recovery_error:?}, last_connection_error={:?}, protocol_context={:?}, timing={:?}",
            business.ready(),
            business.last_connection_error(),
            business.last_protocol_context(),
            business.connection_timing()
        )
    });
    drop(three);
    business.shutdown().await;
    assert!(
        legacy
            .devices()
            .connection(&identity("one").device_key)
            .await
            .unwrap()
            .connected
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = legacy
                .devices()
                .connection(&identity("one").device_key)
                .await;
            if status.is_ok_and(|state| !state.connected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let (auth_only, _unused) = BusinessRpcClient::connect(
        BusinessRpcClientConfig::development(
            addresses[2],
            "rpc-test-token".into(),
            BusinessRole::AuthControl,
        ),
        Some(handler.clone()),
    )
    .unwrap();
    let (events_only, mut dual_events) = BusinessRpcClient::connect(
        BusinessRpcClientConfig::development(
            addresses[2],
            "rpc-test-token".into(),
            BusinessRole::Events,
        ),
        None,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        auth_only.wait_ready().await.unwrap();
        events_only.wait_ready().await.unwrap();
    })
    .await
    .unwrap();
    let fifth = device(addresses[0], "five").await;
    publish(&fifth, 5).await;
    let delivery = tokio::time::timeout(Duration::from_secs(5), dual_events.recv())
        .await
        .unwrap()
        .unwrap();
    delivery.ack().await.unwrap();
    events_only.shutdown().await;
    auth_only.shutdown().await;
    let mut legacy_events = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match legacy
                .events()
                .subscribe(netbaiot_core::EventFilter::default())
                .await
            {
                Ok(stream) => break stream,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap();
    publish(&fifth, 6).await;
    let legacy_delivery = tokio::time::timeout(Duration::from_secs(5), legacy_events.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    legacy_delivery.ack().await.unwrap();
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn zero_offline_grace_revokes_live_session_and_requires_reset_sync() {
    let root = std::env::temp_dir().join(format!(
        "netbaiot-business-rpc-zero-{}",
        uuid::Uuid::new_v4()
    ));
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::BusinessRpc);
    config.event_delivery = Some(EventDeliverySource::DevelopmentAudit);
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: None,
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 0,
    });
    config.spool_directory = root.join("spool");
    drop(reservations);
    let path = write_config(&root, &config);
    let mut server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .env("NETBAIOT_BUSINESS_RPC_TOKEN", "rpc-test-token")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let handler = Arc::new(Handler {
        calls: AtomicUsize::new(0),
        verifier_calls: AtomicUsize::new(0),
        revision: AtomicU64::new(1),
        allowed: AtomicBool::new(true),
    });
    let settings = BusinessRpcClientConfig::development(
        addresses[2],
        "rpc-test-token".into(),
        BusinessRole::AuthControl,
    );
    let (business, _) =
        BusinessRpcClient::connect(settings.clone(), Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let old = device(addresses[0], "one").await;
    let admin = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses[1]))
        .token("a".repeat(64))
        .connect()
        .await
        .unwrap();
    assert!(
        admin
            .devices()
            .connection(&identity("one").device_key)
            .await
            .unwrap()
            .connected
    );
    business.shutdown().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if admin
                .devices()
                .connection(&identity("one").device_key)
                .await
                .is_ok_and(|state| !state.connected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        device_result(addresses[0], "one").await.is_err(),
        "old positive cache must be invalidated"
    );
    assert!(
        device_result(addresses[0], "two").await.is_err(),
        "new miss must fail closed"
    );
    handler.revision.store(2, Ordering::SeqCst);
    let (replacement, _) = BusinessRpcClient::connect(settings, Some(handler)).unwrap();
    tokio::time::timeout(Duration::from_secs(10), replacement.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let _fresh = device(addresses[0], "one").await;
    assert!(
        admin
            .devices()
            .connection(&identity("one").device_key)
            .await
            .unwrap()
            .connected
    );
    replacement.shutdown().await;
    drop(old);
    admin.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(8), server.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mtls_verifies_server_and_maps_exact_client_certificate() {
    use netbaiot_client::business_rpc::BusinessRpcTls;
    use netbaiot_server::{BusinessRpcIdentityConfig, ManagementTlsFiles};
    use sha2::{Digest, Sha256};
    let root = std::env::temp_dir().join(format!(
        "netbaiot-business-rpc-mtls-{}",
        uuid::Uuid::new_v4()
    ));
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.device_auth = Some(DeviceAuthSource::BusinessRpc);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    config.spool_directory = root.join("spool");
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    let cert_pem = std::fs::read(fixtures.join("management-client.pem")).unwrap();
    let cert = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .next()
        .unwrap()
        .unwrap();
    let fingerprint = Sha256::digest(cert.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let principal_expiry = netbaiot_runtime::now_ms().saturating_add(12_000);
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        tls: Some(ManagementTlsFiles {
            certificate: fixtures.join("localhost-cert.pem").to_string_lossy().into(),
            private_key: fixtures.join("localhost-key.pem").to_string_lossy().into(),
            client_ca: Some(fixtures.join("management-ca.pem").to_string_lossy().into()),
            require_client_certificate: true,
        }),
        identities: vec![BusinessRpcIdentityConfig {
            certificate_sha256: fingerprint,
            principal_id: "test-provider".into(),
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
            expires_at_ms: Some(principal_expiry),
        }],
        development_token_env: None,
        development_role: None,
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 0,
    });
    drop(reservations);
    let path = write_config(&root, &config);
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
        verifier_calls: AtomicUsize::new(0),
        revision: AtomicU64::new(1),
        allowed: AtomicBool::new(true),
    });
    let mut client_config = BusinessRpcClientConfig::development(
        addresses[2],
        "unused".into(),
        BusinessRole::AuthControl,
    );
    client_config.token = None;
    client_config.tls = Some(BusinessRpcTls {
        server_name: "localhost".into(),
        ca_pem: fixtures.join("localhost-cert.pem"),
        certificate_pem: fixtures.join("management-client.pem"),
        private_key_pem: fixtures.join("management-client-key.pem"),
    });
    let (business, _) =
        BusinessRpcClient::connect(client_config.clone(), Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), business.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let _first = device(addresses[0], "tls").await;
    assert_eq!(handler.calls.load(Ordering::Relaxed), 1);
    assert!(matches!(
        business.invalidate(2, AuthInvalidation::All).await,
        Err(netbaiot_client::business_rpc::BusinessRpcClientError::Remote(RpcErrorCode::Forbidden))
    ));
    let mut unauthorized_role = client_config.clone();
    unauthorized_role.role = BusinessRole::Events;
    let (event_only, _) = BusinessRpcClient::connect(unauthorized_role, None).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), event_only.wait_ready())
            .await
            .unwrap()
            .is_err(),
        "an auth-only mTLS principal cannot subscribe to events"
    );
    event_only.shutdown().await;
    let mut wrong_name = client_config.clone();
    wrong_name.tls.as_mut().unwrap().server_name = "not-localhost.example".into();
    let (untrusted, _) = BusinessRpcClient::connect(wrong_name, Some(handler.clone())).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), untrusted.wait_ready())
            .await
            .unwrap()
            .is_err()
    );
    untrusted.shutdown().await;
    for (case, ca, cert, key) in [
        (
            "untrusted CA",
            "management-ca.pem",
            "management-client.pem",
            "management-client-key.pem",
        ),
        (
            "wrong client certificate",
            "localhost-cert.pem",
            "localhost-cert.pem",
            "localhost-key.pem",
        ),
        (
            "expired client certificate",
            "localhost-cert.pem",
            "management-expired.pem",
            "management-client-key.pem",
        ),
        (
            "missing client certificate",
            "localhost-cert.pem",
            "missing-client.pem",
            "management-client-key.pem",
        ),
        (
            "known CA but unmapped principal",
            "localhost-cert.pem",
            "management-unmapped.pem",
            "management-client-key.pem",
        ),
    ] {
        let mut invalid = client_config.clone();
        let tls = invalid.tls.as_mut().unwrap();
        tls.ca_pem = fixtures.join(ca);
        tls.certificate_pem = fixtures.join(cert);
        tls.private_key_pem = fixtures.join(key);
        let (client, _) = BusinessRpcClient::connect(invalid, Some(handler.clone())).unwrap();
        assert!(
            !matches!(
                tokio::time::timeout(Duration::from_secs(1), client.wait_ready()).await,
                Ok(Ok(()))
            ),
            "{case} must not reach Ready"
        );
        client.shutdown().await;
    }
    assert_eq!(
        handler.calls.load(Ordering::Relaxed),
        1,
        "TLS failures cannot reach device auth"
    );
    tokio::time::timeout(Duration::from_secs(13), async {
        while business.ready() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("principal expiry must close an idle Serving connection");
    assert!(netbaiot_runtime::now_ms() >= principal_expiry);
    assert!(
        !matches!(
            tokio::time::timeout(
                Duration::from_secs(3),
                device_result(addresses[0], "after-expiry")
            )
            .await,
            Ok(Ok(_))
        ),
        "an expired principal cannot authorize a new device"
    );
    assert_eq!(handler.calls.load(Ordering::Relaxed), 1);
    business.shutdown().await;
    server.start_kill().unwrap();
    let _ = server.wait().await;
    let replacement_cert = std::fs::read(fixtures.join("management-unmapped.pem")).unwrap();
    let replacement_der = rustls_pemfile::certs(&mut replacement_cert.as_slice())
        .next()
        .unwrap()
        .unwrap();
    config.business_rpc.as_mut().unwrap().identities[0].certificate_sha256 =
        Sha256::digest(replacement_der.as_ref())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
    config.business_rpc.as_mut().unwrap().identities[0].expires_at_ms = None;
    let replacement_path = write_config(&root, &config);
    let mut replacement_server = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&replacement_path)
        .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut replacement_config = client_config.clone();
    replacement_config.tls.as_mut().unwrap().certificate_pem =
        fixtures.join("management-unmapped.pem");
    let (replacement, _) =
        BusinessRpcClient::connect(replacement_config, Some(handler.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(10), replacement.wait_ready())
        .await
        .unwrap()
        .unwrap();
    device_result(addresses[0], "rotated").await.unwrap();
    let (old_certificate, _) =
        BusinessRpcClient::connect(client_config, Some(handler.clone())).unwrap();
    assert!(!matches!(
        tokio::time::timeout(Duration::from_secs(1), old_certificate.wait_ready()).await,
        Ok(Ok(()))
    ));
    old_certificate.shutdown().await;
    replacement.shutdown().await;
    replacement_server.start_kill().unwrap();
    let _ = replacement_server.wait().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn v1_spooled_required_event_replays_to_v2_with_stable_event_id() {
    let root = std::env::temp_dir().join(format!(
        "netbaiot-business-upgrade-{}",
        uuid::Uuid::new_v4()
    ));
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect::<Vec<_>>();
    config.device_ingress = addresses[0];
    config.management_http = addresses[1];
    config.business_tcp = Some(addresses[2]);
    config.spool_directory = root.join("spool");
    config.credentials[0].credential_id = "cred-one".into();
    config.credentials[0].identity.device_key = identity("one").device_key;
    config.limits.sink_timeout_ms = 300;
    config.limits.shutdown_drain_timeout_ms = 300;
    drop(reservations);
    let path = write_config(&root, &config);
    let start = || {
        Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
            .arg(&path)
            .env("NETBAIOT_ADMIN_SECRET", "a".repeat(64))
            .env("NETBAIOT_BUSINESS_STREAM_TOKEN", "legacy-token")
            .env("NETBAIOT_BUSINESS_RPC_TOKEN", "rpc-test-token")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    };
    let mut first = start();
    let legacy = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses[1]))
        .token("a".repeat(64))
        .event_token("legacy-token")
        .event_address(addresses[2])
        .connect()
        .await
        .unwrap();
    let mut stream = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(stream) = legacy
                .events()
                .subscribe(netbaiot_core::EventFilter::default())
                .await
            {
                break stream;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let device = device(addresses[0], "one").await;
    publish(&device, 91).await;
    let first_delivery = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event_id = first_delivery.event_id();
    legacy.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(8), first.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    drop(first_delivery);
    stream.close();
    drop(legacy);

    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
        v3: None,
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
        development_role: None,
        allow_v1: false,
        max_connections: 8,
        auth_max_inflight: 16,
        max_auth_control_offline_ms: 0,
    });
    config.device_auth = Some(DeviceAuthSource::Static);
    config.event_delivery = Some(EventDeliverySource::BusinessRpc);
    write_config(&root, &config);
    let mut second = start();
    let (events, mut receiver) = BusinessRpcClient::connect(
        BusinessRpcClientConfig::development(
            addresses[2],
            "rpc-test-token".into(),
            BusinessRole::Events,
        ),
        None,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), events.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let replay = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.delivery.event.event_id, event_id);
    replay.ack().await.unwrap();
    events.shutdown().await;
    let admin = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses[1]))
        .token("a".repeat(64))
        .connect()
        .await
        .unwrap();
    admin.runtime().drain().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(8), second.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let _ = std::fs::remove_dir_all(root);
}
