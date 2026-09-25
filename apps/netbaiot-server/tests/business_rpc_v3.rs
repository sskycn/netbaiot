use async_trait::async_trait;
use netbaiot_client::business_rpc::{
    BusinessAuthHandler, BusinessRpcClient, BusinessRpcClientConfig, BusinessRpcV3Client,
    BusinessRpcV3ClientConfig,
};
use netbaiot_core::{
    AuthInvalidation, CodecId, DeviceEventKind, DeviceId, DeviceKey, DeviceUplink,
    DeviceUplinkKind, Heartbeat, ProductId, SourceMessageId, TenantId,
    business_rpc::{
        AuthenticatedDeviceWire, BusinessRole, DeviceAuthenticateRequest, ResolveVerifierRequest,
        ResolveVerifierResponse, RpcError,
    },
    business_rpc_v3::{V3Bootstrap, V3Limits},
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
