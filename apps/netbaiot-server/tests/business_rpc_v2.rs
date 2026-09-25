use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_client::NetbaIoTClient;
use netbaiot_client::business_rpc::{
    BusinessAuthHandler, BusinessRpcClient, BusinessRpcClientConfig,
};
use netbaiot_core::{
    AuthInvalidation, CodecId, DeviceId, DeviceKey, DeviceUplink, DeviceUplinkKind, Heartbeat,
    ProductId, SourceMessageId, TenantId,
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
    net::{TcpListener, UdpSocket},
    process::Command,
};

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
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
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
    let client_config = BusinessRpcClientConfig::development(
        addresses[2],
        "rpc-test-token".into(),
        BusinessRole::Multiplexed,
    );
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
    let _recovered = device(addresses[0], "one").await;
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
    config.business_rpc = Some(BusinessRpcConfig {
        version: 2,
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
            expires_at_ms: None,
        }],
        development_token_env: None,
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
    let mut wrong_name = client_config;
    wrong_name.tls.as_mut().unwrap().server_name = "not-localhost.example".into();
    let (untrusted, _) = BusinessRpcClient::connect(wrong_name, Some(handler)).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), untrusted.wait_ready())
            .await
            .unwrap()
            .is_err()
    );
    untrusted.shutdown().await;
    business.shutdown().await;
    server.start_kill().unwrap();
    let _ = server.wait().await;
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
        tls: None,
        identities: Vec::new(),
        development_token_env: Some("NETBAIOT_BUSINESS_RPC_TOKEN".into()),
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
