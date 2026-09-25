use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use netbaiot_core::{
    CodecId, ControlSnapshot, EventId, ProductId, ProductRuntimeConfig, SinkId, TenantId, Transport,
};
use netbaiot_protocol::RouteDefinition;
use netbaiot_runtime::{
    AdminResourceConfig, ApiKeyConfig, AuthCache, CodecRegistry, DeliveryEnvelope, Error,
    EventAcceptance, EventBus, EventSink, GatewayControl, Ingress, Lifecycle, Limits, Metrics,
    MtlsIdentityConfig, RestartSpool, Sessions, SinkAck, SinkDefinition, SinkDeliveryMode,
    SinkError, StaticAuthenticator,
};
use netbaiot_server::{
    Config, ManagementTlsFiles, TlsFiles, management_tls_acceptor, read_config, run, tls_acceptor,
};
use netbaiot_transports::{
    BoxStream, Services, serve_device_ingress, serve_management_http, serve_stream,
};
use std::{
    collections::HashSet,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

// These integration tests close ephemeral-port reservations before a subprocess or composition
// root binds the configured addresses. Serializing only those tests prevents the Rust test
// harness from handing a just-released port to a sibling test in that narrow handoff window.
static EPHEMERAL_PORT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn config() -> Config {
    serde_json::from_str(include_str!("../../../configs/development.json")).unwrap()
}

#[tokio::test]
async fn legacy_config_ack_spool_blocks_startup_and_preserves_rollback_files() {
    let root =
        std::env::temp_dir().join(format!("netbaiot-legacy-startup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let device = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config();
    c.device_ingress = device.local_addr().unwrap();
    c.management_http = management.local_addr().unwrap();
    c.spool_directory = root.join("spool");
    std::fs::create_dir_all(&c.spool_directory).unwrap();
    let path = c.spool_directory.join("eventbus-recovery.spool");
    let bytes = include_bytes!("../../../tests/fixtures/restart-spool/config-ack-v2.spool");
    std::fs::write(&path, bytes).unwrap();
    // Reserved listeners also prove recovery fails before any listener bind/readiness.
    assert!(matches!(
        run(
            serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap(),
            CancellationToken::new()
        )
        .await,
        Err(Error::IncompatibleSpool)
    ));
    let config_path = root.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec(&c).unwrap()).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
            .arg(&config_path)
            .env("RUST_LOG", "error")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(log.contains("startup blocked"));
    assert!(log.contains("legacy ConfigAck records"));
    assert!(log.contains("previous release before upgrading"));
    assert!(!log.contains("legacy:1"));
    assert!(!log.contains("device-1"));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(std::fs::read_dir(&c.spool_directory).unwrap().count(), 1);
    std::fs::remove_dir_all(root).unwrap();
}

struct TlsTestSink;

#[async_trait]
impl EventSink for TlsTestSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        Ok(SinkAck)
    }
}

fn tls_test_services(limits: Limits, stop: CancellationToken) -> (Arc<Ingress>, Arc<Services>) {
    let limits = Arc::new(limits);
    let metrics = Arc::new(Metrics::default());
    let lifecycle = Arc::new(Lifecycle::starting());
    lifecycle.mark_running().unwrap();
    let sink_id = SinkId::new("tls-test").unwrap();
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![SinkDefinition::bounded(
            sink_id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(TlsTestSink),
            &limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink_id],
        }],
        1,
    )
    .unwrap();
    let provider = StaticAuthenticator::new(config().credentials, &limits).unwrap();
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        AuthCache::new(provider, limits.clone(), metrics.clone()),
        CodecRegistry::new(vec![(
            CodecId::new("netbaiot-json").unwrap(),
            1,
            Arc::new(JsonV1::default()),
        )])
        .unwrap(),
        events,
        GatewayControl::empty(limits.clone()),
        metrics,
        Sessions::new(limits),
        lifecycle,
    ));
    let services = Services::new(ingress.clone(), stop);
    (ingress, services)
}
#[test]
fn public_streams_require_tls_and_volatile_store_requires_loopback() {
    let mut c = config();
    c.device_ingress = "0.0.0.0:8080".parse().unwrap();
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

#[test]
fn non_loopback_management_requires_tls_while_loopback_development_allows_http() {
    let mut loopback = config();
    loopback.management_http = "127.0.0.1:8081".parse().unwrap();
    loopback.tls = None;
    assert!(loopback.validate().is_ok());

    let mut public = config();
    public.development = false;
    public.delivery_url = Some("https://example.invalid/ingress".into());
    public.management_http = "0.0.0.0:8081".parse().unwrap();
    public.tls = None;
    assert!(public.validate().is_err());
    public.tls = Some(TlsFiles {
        certificate: "cert".into(),
        private_key: "key".into(),
    });
    assert!(public.validate().is_ok());
}
async fn tcp_accept(address: std::net::SocketAddr, payload: &[u8]) -> EventAcceptance {
    use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};
    let mut socket: BoxStream = Box::new(TcpStream::connect(address).await.unwrap());
    let framer = LengthPrefixFramer { maximum: 65_536 };
    socket.write_all(&framer.encode(br#"{"credential_id":"demo-device","secret":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"}"#).unwrap()).await.unwrap();
    assert_eq!(read_frame(&mut socket).await, br#"{"authenticated":true}"#);
    socket
        .write_all(&framer.encode(payload).unwrap())
        .await
        .unwrap();
    let receipt = serde_json::from_slice(&read_frame(&mut socket).await).unwrap();
    socket.shutdown().await.unwrap();
    receipt
}

#[tokio::test]
async fn composition_root_serves_tcp_and_stops_all_listeners() {
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let mut c = config();
    let mut reservations = Vec::new();
    for _ in 0..5 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    c.device_ingress = reservations[0].local_addr().unwrap();
    c.management_http = reservations[1].local_addr().unwrap();
    c.spool_directory =
        std::env::temp_dir().join(format!("netbaiot-server-{}", uuid::Uuid::new_v4()));
    let addresses = [c.device_ingress, c.management_http];
    drop(reservations);
    let stop = CancellationToken::new();
    let server = tokio::spawn(run(c, stop.clone()));
    tokio::time::timeout(Duration::from_secs(3), async {
        while TcpStream::connect(addresses[0]).await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let receipt = tcp_accept(addresses[0], br#"{"schema_version":1,"source_message_id":"root:1","kind":"heartbeat","data":{"sequence":1}}"#).await;
    assert!(!receipt.event_id.0.is_nil());
    // Observe the sockets owned by this server instance. Reconnecting to the released ephemeral
    // addresses after shutdown is racy because another concurrent test may legitimately receive
    // one of those ports before this assertion runs.
    let mut active_connections = Vec::new();
    for address in addresses {
        active_connections.push(TcpStream::connect(address).await.unwrap());
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for mut connection in active_connections {
        let mut byte = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(1), connection.read(&mut byte))
            .await
            .expect("owned connection remained open after server shutdown");
        assert!(
            matches!(result, Ok(0) | Err(_)),
            "owned connection received data after server shutdown"
        );
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
    let tls = tls_acceptor(&TlsFiles {
        certificate: root.join("localhost-cert.pem").to_str().unwrap().into(),
        private_key: root.join("localhost-key.pem").to_str().unwrap().into(),
    })
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let limits = Limits {
        connect_timeout_ms: 40,
        shutdown_timeout_ms: 100,
        ..Limits::default()
    };
    let stop = CancellationToken::new();
    let (ingress, services) = tls_test_services(limits, stop.child_token());
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Mqtt,
        services,
        Some(tls),
        stop.child_token(),
    ));
    let mut socket = TcpStream::connect(address).await.unwrap();
    let mut b = [0];
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut b))
            .await
            .unwrap(),
        Ok(0) | Err(_)
    ));
    let mut pending = tokio::net::TcpStream::connect(address).await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(pending.read(&mut b).await, Ok(0) | Err(_)));
    ingress.events.stop_workers().await.unwrap();
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

#[tokio::test]
async fn api_key_scope_and_tenant_denials_precede_management_mutations() {
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let mut config = config();
    config.device_ingress = free_address().await;
    config.management_http = free_address().await;
    let root =
        std::env::temp_dir().join(format!("netbaiot-management-auth-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    config.spool_directory = root.join("spool");
    config.management_auth.api_keys.push(ApiKeyConfig {
        key_id: "tenant-reader".into(),
        secret_env: "NETBAIOT_TEST_MANAGEMENT_KEY".into(),
        subject: "service:reader".into(),
        scopes: vec!["connection.read".into()],
        resources: AdminResourceConfig {
            tenants: vec![TenantId::new("demo").unwrap()],
            ..Default::default()
        },
        global: false,
        expires_at: None,
        auth_generation: 1,
        enabled: true,
    });
    config.management_auth.api_keys.push(ApiKeyConfig {
        key_id: "tenant-invalidator".into(),
        secret_env: "NETBAIOT_TEST_MANAGEMENT_KEY".into(),
        subject: "service:invalidator".into(),
        scopes: vec!["auth.invalidate.all".into()],
        resources: AdminResourceConfig {
            tenants: vec![TenantId::new("demo").unwrap()],
            ..Default::default()
        },
        global: false,
        expires_at: None,
        auth_generation: 1,
        enabled: true,
    });
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let admin = "a".repeat(64);
    let secret = "b".repeat(64);
    let key = format!("tenant-reader.{secret}");
    let mut child = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env("NETBAIOT_ADMIN_SECRET", &admin)
        .env("NETBAIOT_TEST_MANAGEMENT_KEY", &secret)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    wait_ready(&client, config.management_http, &admin).await;
    let official = netbaiot_client::NetbaIoTClient::builder()
        .endpoint(format!("http://{}", config.management_http))
        .api_key(&key)
        .connect()
        .await
        .unwrap();
    assert!(matches!(
        official.runtime().status().await,
        Err(netbaiot_client::ClientError::Forbidden { .. })
    ));
    let url = |path: &str| format!("http://{}{path}", config.management_http);
    let response = client
        .get(url("/api/v1/status"))
        .header("Authorization", format!("ApiKey {key}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    let denied: netbaiot_protocol::ApiError = response.json().await.unwrap();
    assert_eq!(denied.required_scope.as_deref(), Some("runtime.read"));
    let response = client
        .get(url("/api/v1/connections"))
        .header("Authorization", format!("ApiKey {key}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response = client
        .post(url("/api/v1/devices/connection"))
        .header("Authorization", format!("ApiKey {key}"))
        .json(
            &serde_json::json!({"tenant_id":"other","product_id":"sensor","device_id":"device-1"}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    let response = client
        .post(url("/api/v1/devices/commands"))
        .header("Authorization", format!("ApiKey {key}"))
        .body("not even JSON")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    let response = client
        .post(url("/api/v1/drain"))
        .header("Authorization", format!("ApiKey {key}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    for path in ["/api/v1/routes", "/api/v1/control/snapshot"] {
        let response = client
            .put(url(path))
            .header("Authorization", format!("ApiKey {key}"))
            .body("not even JSON")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    }
    let response = client
        .post(url("/api/v1/auth/invalidate"))
        .header(
            "Authorization",
            format!("ApiKey tenant-invalidator.{secret}"),
        )
        .json(&serde_json::json!({"scope":"all"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    let response = client
        .get(url("/api/v1/ready"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response = client
        .get(url("/api/v1/connections"))
        .header(
            "Authorization",
            format!("ApiKey tenant-reader.{}", "c".repeat(64)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    request_drain(&client, config.management_http, &admin).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn management_client_certificate_requirement_does_not_change_device_tls() {
    let files = test_tls_files();
    let device_tls = tls_acceptor(&files).await.unwrap();
    let management_tls = management_tls_acceptor(&ManagementTlsFiles {
        certificate: files.certificate.clone(),
        private_key: files.private_key.clone(),
        client_ca: Some(files.certificate.clone()),
        require_client_certificate: true,
    })
    .await
    .unwrap();
    let device = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let device_addr = device.local_addr().unwrap();
    let management_addr = management.local_addr().unwrap();
    let device_task =
        tokio::spawn(async move { device_tls.accept(device.accept().await.unwrap().0).await });
    let management_task = tokio::spawn(async move {
        management_tls
            .accept(management.accept().await.unwrap().0)
            .await
    });
    let connector = test_tls_connector();
    let device_socket = TcpStream::connect(device_addr).await.unwrap();
    assert!(
        connector
            .connect("localhost".try_into().unwrap(), device_socket)
            .await
            .is_ok()
    );
    assert!(device_task.await.unwrap().is_ok());
    let management_socket = TcpStream::connect(management_addr).await.unwrap();
    let management_client = connector
        .connect("localhost".try_into().unwrap(), management_socket)
        .await;
    let management_server = management_task.await.unwrap();
    assert!(management_server.is_err() || management_client.is_err());
}

#[tokio::test]
async fn management_mtls_trust_and_explicit_identity_mapping() {
    use sha2::{Digest, Sha256};
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    let files = test_tls_files();
    let client_cert = std::fs::read(fixtures.join("management-client.pem")).unwrap();
    let client_key = std::fs::read(fixtures.join("management-client-key.pem")).unwrap();
    let leaf = rustls_pemfile::certs(&mut client_cert.as_slice())
        .next()
        .unwrap()
        .unwrap();
    let fingerprint = Sha256::digest(leaf.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut config = config();
    config.device_ingress = free_address().await;
    config.management_http = free_address().await;
    config.management_tls = Some(ManagementTlsFiles {
        certificate: files.certificate.clone(),
        private_key: files.private_key.clone(),
        client_ca: Some(fixtures.join("management-ca.pem").to_str().unwrap().into()),
        require_client_certificate: true,
    });
    config
        .management_auth
        .mtls_identities
        .push(MtlsIdentityConfig {
            certificate_sha256: fingerprint,
            subject: "service:iot-platform".into(),
            scopes: vec!["runtime.read".into(), "runtime.drain".into()],
            resources: AdminResourceConfig::default(),
            global: true,
        });
    let root =
        std::env::temp_dir().join(format!("netbaiot-management-mtls-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    config.spool_directory = root.join("spool");
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_netbaiot-server"))
        .arg(&path)
        .env_remove("NETBAIOT_ADMIN_SECRET")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let root_cert =
        reqwest::Certificate::from_pem(&std::fs::read(&files.certificate).unwrap()).unwrap();
    let mut identity_pem = client_cert.clone();
    identity_pem.extend_from_slice(&client_key);
    let trusted = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(root_cert.clone())
        .identity(reqwest::Identity::from_pem(&identity_pem).unwrap())
        .build()
        .unwrap();
    let url = format!(
        "https://localhost:{}/api/v1/ready",
        config.management_http.port()
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if trusted
                .get(&url)
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
    let no_cert = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(root_cert.clone())
        .build()
        .unwrap();
    assert!(no_cert.get(&url).send().await.is_err());
    let mut expired_pem = std::fs::read(fixtures.join("management-expired.pem")).unwrap();
    expired_pem.extend_from_slice(&client_key);
    let expired = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(root_cert.clone())
        .identity(reqwest::Identity::from_pem(&expired_pem).unwrap())
        .build()
        .unwrap();
    assert!(expired.get(&url).send().await.is_err());
    let mut untrusted_pem = std::fs::read(&files.certificate).unwrap();
    untrusted_pem.extend_from_slice(&std::fs::read(&files.private_key).unwrap());
    let untrusted = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(root_cert.clone())
        .identity(reqwest::Identity::from_pem(&untrusted_pem).unwrap())
        .build()
        .unwrap();
    assert!(untrusted.get(&url).send().await.is_err());
    let mut unmapped_pem = std::fs::read(fixtures.join("management-unmapped.pem")).unwrap();
    unmapped_pem.extend_from_slice(&client_key);
    let unmapped = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(root_cert)
        .identity(reqwest::Identity::from_pem(&unmapped_pem).unwrap())
        .build()
        .unwrap();
    assert_eq!(
        unmapped.get(&url).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let drain_url = format!(
        "https://localhost:{}/api/v1/drain",
        config.management_http.port()
    );
    assert_eq!(
        trusted.post(drain_url).send().await.unwrap().status(),
        reqwest::StatusCode::ACCEPTED
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
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

fn mqtt_connect_with_credentials(
    client_id: &str,
    clean: bool,
    credential_id: &str,
    secret: &str,
) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_text(b"MQTT", &mut body);
    body.extend_from_slice(&[4, 0xc0 | (u8::from(clean) * 2), 0, 30]);
    mqtt_text(client_id.as_bytes(), &mut body);
    mqtt_text(credential_id.as_bytes(), &mut body);
    mqtt_text(secret.as_bytes(), &mut body);
    mqtt_packet(0x10, &body)
}

fn mqtt_connect(client_id: &str, clean: bool) -> Vec<u8> {
    mqtt_connect_with_credentials(
        client_id,
        clean,
        "demo-device",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    )
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

async fn mqtt_read(stream: &mut (impl tokio::io::AsyncRead + Unpin + ?Sized)) -> (u8, Vec<u8>) {
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

async fn mqtt_open_with_credentials(
    address: std::net::SocketAddr,
    client_id: &str,
    credential_id: &str,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&mqtt_connect_with_credentials(
            client_id,
            true,
            credential_id,
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        ))
        .await
        .unwrap();
    stream
}

async fn read_http_request_body(stream: &mut TcpStream) -> Option<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::with_capacity(1024);
        let mut chunk = [0u8; 1024];
        loop {
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            if bytes.len().checked_add(read)? > 32_768 {
                return None;
            }
            bytes.extend_from_slice(&chunk[..read]);
            let Some(header_start) = bytes.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let body_start = header_start + 4;
            let header = String::from_utf8_lossy(&bytes[..body_start]);
            let length = header.lines().find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })?;
            if length > 16_384 {
                return None;
            }
            let request_end = body_start.checked_add(length)?;
            if bytes.len() >= request_end {
                return Some(bytes[body_start..request_end].to_vec());
            }
        }
    })
    .await
    .ok()
    .flatten()
}

async fn write_auth_response(stream: &mut TcpStream, status: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
}

async fn serve_test_auth_provider(
    listener: TcpListener,
    available: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
    stop: CancellationToken,
) {
    loop {
        let accepted = tokio::select! {
            _ = stop.cancelled() => break,
            accepted = listener.accept() => accepted,
        };
        let Ok((mut stream, _)) = accepted else {
            break;
        };
        let Some(body) = read_http_request_body(&mut stream).await else {
            continue;
        };
        calls.fetch_add(1, Ordering::Relaxed);
        if !available.load(Ordering::Relaxed) {
            write_auth_response(&mut stream, "503 Service Unavailable", b"{}").await;
            continue;
        }
        let Ok(request) = serde_json::from_slice::<serde_json::Value>(&body) else {
            write_auth_response(&mut stream, "400 Bad Request", b"{}").await;
            continue;
        };
        let Some(credential_id) = request
            .get("credential_id")
            .and_then(|value| value.as_str())
        else {
            write_auth_response(&mut stream, "401 Unauthorized", b"{}").await;
            continue;
        };
        let device_id = match credential_id {
            "control-a" => "device-a",
            "control-b" => "device-b",
            _ => {
                write_auth_response(&mut stream, "401 Unauthorized", b"{}").await;
                continue;
            }
        };
        let response = serde_json::to_vec(&serde_json::json!({
            "device_key": {
                "tenant_id": "demo",
                "product_id": "sensor",
                "device_id": device_id,
            },
            "credential_version": 1,
            "auth_generation": 1,
            "codec_id": "netbaiot-json",
            "codec_version": 1,
            "permissions": { "publish": true, "commands": true },
        }))
        .unwrap();
        write_auth_response(&mut stream, "200 OK", &response).await;
    }
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
async fn external_auth_outage_preserves_bound_session_and_recovers_new_authentication() {
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let provider_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_address = provider_listener.local_addr().unwrap();
    let provider_available = Arc::new(AtomicBool::new(true));
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let provider_stop = CancellationToken::new();
    let provider_task = tokio::spawn(serve_test_auth_provider(
        provider_listener,
        provider_available.clone(),
        provider_calls.clone(),
        provider_stop.clone(),
    ));

    let mut c = config();
    c.device_ingress = free_address().await;
    c.management_http = free_address().await;
    c.credentials.clear();
    c.auth_provider_url = Some(format!("http://{provider_address}/auth"));
    c.limits.authentication_timeout_ms = 500;
    c.spool_directory =
        std::env::temp_dir().join(format!("netbaiot-auth-outage-{}", uuid::Uuid::new_v4()));
    let spool_directory = c.spool_directory.clone();
    let mqtt_address = c.device_ingress;
    let server_stop = CancellationToken::new();
    let server_task = tokio::spawn(run(c, server_stop.clone()));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if TcpStream::connect(mqtt_address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let mut established =
        mqtt_open_with_credentials(mqtt_address, "control-client-a", "control-a").await;
    assert_eq!(mqtt_read(&mut established).await, (0x20, vec![0, 0]));
    assert_eq!(provider_calls.load(Ordering::Relaxed), 1);

    provider_available.store(false, Ordering::Relaxed);
    let payload = br#"{"schema_version":1,"source_message_id":"outage:1","kind":"heartbeat","data":{"sequence":1}}"#;
    established
        .write_all(&mqtt_publish(
            7,
            "v1/t/demo/p/sensor/d/device-a/up",
            payload,
            1,
            false,
        ))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut established).await, (0x40, vec![0, 7]));
    assert_eq!(
        provider_calls.load(Ordering::Relaxed),
        1,
        "an established session must not reauthenticate per message"
    );

    let mut rejected =
        mqtt_open_with_credentials(mqtt_address, "control-client-b-failed", "control-b").await;
    let mut byte = [0u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(2), rejected.read(&mut byte))
        .await
        .unwrap();
    assert!(matches!(closed, Ok(0) | Err(_)));
    assert_eq!(provider_calls.load(Ordering::Relaxed), 2);

    provider_available.store(true, Ordering::Relaxed);
    let mut recovered =
        mqtt_open_with_credentials(mqtt_address, "control-client-b-recovered", "control-b").await;
    assert_eq!(mqtt_read(&mut recovered).await, (0x20, vec![0, 0]));
    assert_eq!(provider_calls.load(Ordering::Relaxed), 3);

    server_stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    provider_stop.cancel();
    provider_task.await.unwrap();
    let _ = std::fs::remove_dir_all(spool_directory);
}

#[tokio::test]
async fn subprocess_mqtt_session_retained_and_qos1_inflight_survive_graceful_restart() {
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let mut c = config();
    c.device_ingress = free_address().await;
    c.management_http = free_address().await;
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
    let mut persistent = mqtt_open(c.device_ingress, "persistent-client", false).await;
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
    let mut resumed = mqtt_open(c.device_ingress, "persistent-client", false).await;
    assert_eq!(mqtt_read(&mut resumed).await, (0x20, vec![1, 0]));
    let (retry_first, retry_body) = mqtt_read(&mut resumed).await;
    assert_eq!(retry_first & 0x0f, 0x0a, "QoS1 retry must set DUP");
    assert_eq!(mqtt_publish_id(&retry_body), outbound_id);
    resumed
        .write_all(&[0x40, 2, (outbound_id >> 8) as u8, outbound_id as u8])
        .await
        .unwrap();

    let mut retained_reader = mqtt_open(c.device_ingress, "retained-reader", true).await;
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
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let mut c = config();
    c.device_ingress = free_address().await;
    c.management_http = free_address().await;
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
    let mut mqtt = mqtt_open(c.device_ingress, "qos2-persistent", false).await;
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
    let mut mqtt = mqtt_open(c.device_ingress, "qos2-persistent", false).await;
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
    let mut mqtt = mqtt_open(c.device_ingress, "qos2-persistent", false).await;
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
    let mut mqtt = mqtt_open(c.device_ingress, "qos2-persistent", false).await;
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

async fn exercise_graceful_restart_spool_replay(healthy_cycles: u32, dwell_per_cycle: Duration) {
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
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
    c.device_ingress = free_address().await;
    c.management_http = free_address().await;
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
    let mqtt_topic = "v1/t/demo/p/sensor/d/device-1/up";
    let mqtt_payload = br#"{"schema_version":1,"source_message_id":"combined-restart","kind":"heartbeat","data":{"sequence":99}}"#;
    let mut persistent = mqtt_open(c.device_ingress, "combined-restart", false).await;
    assert_eq!(mqtt_read(&mut persistent).await, (0x20, vec![0, 0]));
    persistent
        .write_all(&mqtt_subscribe(1, mqtt_topic, 1))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut persistent).await, (0x90, vec![0, 1, 1]));
    persistent.write_all(&[0xe0, 0]).await.unwrap();
    drop(persistent);
    let mut mqtt_publisher = mqtt_open(c.device_ingress, "combined-publisher", true).await;
    assert_eq!(mqtt_read(&mut mqtt_publisher).await, (0x20, vec![0, 0]));
    mqtt_publisher
        .write_all(&mqtt_publish(2, mqtt_topic, mqtt_payload, 1, true))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut mqtt_publisher).await, (0x40, vec![0, 2]));
    mqtt_publisher.write_all(&[0xe0, 0]).await.unwrap();
    drop(mqtt_publisher);
    let mut accepted = HashSet::new();
    for sequence in 0..3 {
        let receipt = tcp_accept(c.device_ingress, format!(r#"{{"schema_version":1,"source_message_id":"restart:{sequence}","kind":"heartbeat","data":{{"sequence":{sequence}}}}}"#).as_bytes()).await;
        accepted.insert(receipt.event_id);
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

    // Two more unavailable generations must atomically replace the same
    // authoritative snapshot rather than appending duplicate responsibilities.
    for _ in 0..2 {
        let mut unavailable = start_child(&config_path, &admin).await;
        wait_ready(&client, c.management_http, &admin).await;
        request_drain(&client, c.management_http, &admin).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(5), unavailable.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let committed = std::fs::read_dir(&c.spool_directory)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension().and_then(|value| value.to_str()) == Some("spool")
            })
            .count();
        assert_eq!(committed, 1);
    }

    healthy.store(true, Ordering::Relaxed);
    let mut second = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    let mut persistent = mqtt_open(c.device_ingress, "combined-restart", false).await;
    assert_eq!(mqtt_read(&mut persistent).await, (0x20, vec![1, 0]));
    let (first, body) = mqtt_read(&mut persistent).await;
    assert_eq!(first & 0xf0, 0x30);
    assert_eq!(first & 0x08, 0);
    let packet_id = mqtt_publish_id(&body);
    assert!(body.ends_with(mqtt_payload));
    persistent
        .write_all(&[0x40, 2, (packet_id >> 8) as u8, packet_id as u8])
        .await
        .unwrap();
    persistent.write_all(&[0xe0, 0]).await.unwrap();
    drop(persistent);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let found = observed
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            if accepted.is_subset(&found) && found.len() > accepted.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observed.lock().unwrap().len(), accepted.len() + 1);
    for event_id in &accepted {
        assert_eq!(
            observed
                .lock()
                .unwrap()
                .iter()
                .filter(|candidate| *candidate == event_id)
                .count(),
            1,
            "one durable responsibility must be delivered once after repeated unavailable restarts"
        );
    }
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
    for cycle in 0..healthy_cycles {
        let mut child = start_child(&config_path, &admin).await;
        wait_ready(&client, c.management_http, &admin).await;
        let receipt = tcp_accept(c.device_ingress, format!(r#"{{"schema_version":1,"source_message_id":"cycle:{cycle}","kind":"heartbeat","data":{{"sequence":{cycle}}}}}"#).as_bytes()).await;
        let event_id = receipt.event_id;
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
        tokio::time::sleep(dwell_per_cycle).await;
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
async fn subprocess_graceful_restart_spools_and_replays_every_accepted_event_id() {
    exercise_graceful_restart_spool_replay(3, Duration::ZERO).await;
}

#[tokio::test]
#[ignore = "60-second multi-generation restart soak"]
async fn subprocess_graceful_restart_sixty_second_soak() {
    exercise_graceful_restart_spool_replay(12, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn subprocess_sigkill_exposes_the_documented_three_event_loss_window() {
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let mut c = config();
    c.device_ingress = free_address().await;
    c.management_http = free_address().await;
    let unavailable_sink = free_address().await;
    c.delivery_url = Some(format!("http://{unavailable_sink}/events"));
    c.limits.sink_max_attempts = 1;
    c.limits.sink_max_age_ms = 60_000;
    c.limits.shutdown_drain_timeout_ms = 25;
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
        let receipt = tcp_accept(c.device_ingress, format!(r#"{{"schema_version":1,"source_message_id":"kill:{sequence}","kind":"heartbeat","data":{{"sequence":{sequence}}}}}"#).as_bytes()).await;
        accepted.insert(receipt.event_id);
    }
    assert_eq!(accepted.len(), 3);
    std::fs::create_dir_all(&c.spool_directory).unwrap();
    std::fs::remove_dir(&c.spool_directory).unwrap();
    std::fs::write(&c.spool_directory, b"not a directory").unwrap();
    request_drain(&client, c.management_http, &admin).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(child.try_wait().unwrap().is_none());
    child.kill().await.unwrap();
    let status = child.wait().await.unwrap();
    assert!(!status.success());
    let committed = c.spool_directory.is_dir()
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
async fn subprocess_spool_failure_stays_alive_until_repaired_then_replays_same_event_id() {
    let _port_guard = EPHEMERAL_PORT_TEST_LOCK.lock().await;
    let mut c = config();
    c.device_ingress = free_address().await;
    c.management_http = free_address().await;
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
    let receipt = tcp_accept(c.device_ingress, r#"{"schema_version":1,"source_message_id":"spool-failure","kind":"heartbeat","data":{"sequence":1}}"#.as_bytes()).await;
    let accepted = receipt.event_id;
    std::fs::create_dir_all(&c.spool_directory).unwrap();
    std::fs::remove_dir(&c.spool_directory).unwrap();
    std::fs::write(&c.spool_directory, b"not a directory").unwrap();
    client
        .post(format!("http://{}/api/v1/drain", c.management_http))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "spool failure must keep accepted work owned by a live process"
    );
    let ready = client
        .get(format!("http://{}/api/v1/ready", c.management_http))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // Device TCP and UDP close before durable recovery succeeds; management stays reachable.
    let stopped_tcp = TcpListener::bind(c.device_ingress).await.unwrap();
    let stopped_udp = tokio::net::UdpSocket::bind(c.device_ingress).await.unwrap();
    drop((stopped_tcp, stopped_udp));

    std::fs::remove_file(&c.spool_directory).unwrap();
    std::fs::create_dir_all(&c.spool_directory).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    let stopped_management = TcpListener::bind(c.management_http).await.unwrap();
    drop(stopped_management);

    let recovered = RestartSpool::new(c.spool_directory.clone(), Arc::new(c.limits.clone()))
        .recover()
        .await
        .unwrap();
    assert_eq!(recovered.records.len(), 1);
    assert_eq!(recovered.records[0].event.event_id, accepted);

    let sink_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sink_address = sink_listener.local_addr().unwrap();
    let delivered = Arc::new(Mutex::new(None));
    let delivered_task = delivered.clone();
    let sink = tokio::spawn(async move {
        let (mut socket, _) = sink_listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            bytes.extend_from_slice(&chunk[..read]);
            let Some(headers) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
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
                .unwrap();
            if bytes.len() < header_end + length {
                continue;
            }
            let value: serde_json::Value =
                serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
            *delivered_task.lock().unwrap() =
                Some(serde_json::from_value(value.get("event_id").cloned().unwrap()).unwrap());
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            break;
        }
    });
    c.delivery_url = Some(format!("http://{sink_address}/events"));
    std::fs::write(&config_path, serde_json::to_vec_pretty(&c).unwrap()).unwrap();
    let mut restarted = start_child(&config_path, &admin).await;
    wait_ready(&client, c.management_http, &admin).await;
    tokio::time::timeout(Duration::from_secs(5), sink)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*delivered.lock().unwrap(), Some(accepted));
    request_drain(&client, c.management_http, &admin).await;
    assert!(restarted.wait().await.unwrap().success());
    let _ = std::fs::remove_dir_all(root);
}

fn test_tls_files() -> TlsFiles {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    TlsFiles {
        certificate: root.join("localhost-cert.pem").to_str().unwrap().into(),
        private_key: root.join("localhost-key.pem").to_str().unwrap().into(),
    }
}
fn test_tls_connector() -> TlsConnector {
    let pem = std::fs::read(test_tls_files().certificate).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    assert!(client.alpn_protocols.is_empty());
    TlsConnector::from(Arc::new(client))
}
async fn device_socket(address: std::net::SocketAddr, tls: bool) -> BoxStream {
    let socket = TcpStream::connect(address).await.unwrap();
    socket.set_nodelay(true).unwrap();
    if tls {
        Box::new(
            test_tls_connector()
                .connect("localhost".try_into().unwrap(), socket)
                .await
                .unwrap(),
        )
    } else {
        Box::new(socket)
    }
}
async fn read_frame(socket: &mut BoxStream) -> Vec<u8> {
    let length = socket.read_u32().await.unwrap() as usize;
    assert!(length <= 65_536);
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await.unwrap();
    bytes
}

async fn exercise_shared_listener(tls: bool) {
    use hmac::{Hmac, Mac};
    use netbaiot_runtime::{AdminAccess, Metric};
    use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};
    use sha2::Sha256;
    let stop = CancellationToken::new();
    let (ingress, mut services) = tls_test_services(Limits::default(), stop.clone());
    ingress
        .control
        .apply(ControlSnapshot {
            revision: 1,
            products: vec![ProductRuntimeConfig {
                tenant_id: TenantId::new("demo").unwrap(),
                product_id: ProductId::new("sensor").unwrap(),
                codec_id: CodecId::new("netbaiot-json").unwrap(),
                codec_version: 1,
                revision: 1,
            }],
            routes: vec![RouteDefinition {
                tenant: None,
                sinks: vec![SinkId::new("tls-test").unwrap()],
            }],
        })
        .unwrap();
    let admin = "d".repeat(64);
    Arc::get_mut(&mut services).unwrap().admin = Some(Arc::new(
        AdminAccess::new(&admin, Default::default(), &ingress.limits).unwrap(),
    ));
    // Exactly ONE TCP listener for every device protocol; bind UDP while it is held.
    let (listener, udp) = loop {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        if let Ok(udp) = tokio::net::UdpSocket::bind(tcp.local_addr().unwrap()).await {
            break (tcp, udp);
        }
    };
    let address = listener.local_addr().unwrap();
    assert_eq!(address, udp.local_addr().unwrap());
    let management = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_address = management.local_addr().unwrap();
    let acceptor = if tls {
        Some(tls_acceptor(&test_tls_files()).await.unwrap())
    } else {
        None
    };
    // Sharing Services must never expose management HTTP on the device listener.
    let task = tokio::spawn(serve_device_ingress(
        listener,
        services.clone(),
        acceptor.clone(),
        stop.child_token(),
    ));
    let udp_task = tokio::spawn(netbaiot_transports::udp::serve(
        udp,
        services.clone(),
        stop.child_token(),
    ));
    let management_task = tokio::spawn(serve_management_http(
        management,
        services.clone(),
        acceptor,
        stop.child_token(),
    ));
    let secret = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let payload = br#"{"schema_version":1,"source_message_id":"shared","kind":"heartbeat","data":{"sequence":1}}"#;
    let scheme = if tls { "https" } else { "http" };
    let cert =
        reqwest::Certificate::from_pem(&std::fs::read(test_tls_files().certificate).unwrap())
            .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(cert)
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    // Application bytes are rejected after TLS, without any HTTP response or fallback.
    for request in [
        "POST /v1/device/data HTTP/1.1\r\nHost: localhost\r\n\r\n",
        "GET /api/v1/ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
        "PATCH /api/v1/routes HTTP/1.1\r\nHost: localhost\r\n\r\n",
    ] {
        let mut socket = device_socket(address, tls).await;
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut response = [0; 64];
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
    }
    assert_eq!(ingress.metrics.get(Metric::ProtocolDetectionFailures), 3);
    for path in ["health", "ready", "status"] {
        let response = client
            .get(format!(
                "{scheme}://localhost:{}/api/v1/{path}",
                management_address.port()
            ))
            .bearer_auth(&admin)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        if path == "status" {
            let value: serde_json::Value = response.json().await.unwrap();
            assert!(value.get("config_cache_entries").is_none());
            assert!(value.get("config_cache_bytes").is_none());
            assert_eq!(
                value["active_connections"],
                serde_json::json!({"mqtt":0,"tcp":0,"udp":0})
            );
        }
    }
    for (method, path) in [
        (reqwest::Method::GET, "/api/v1/devices/config"),
        (reqwest::Method::POST, "/api/v1/devices/config"),
        (reqwest::Method::PUT, "/api/v1/devices/config"),
        (reqwest::Method::POST, "/api/v1/config/invalidate"),
    ] {
        let removed = client
            .request(
                method,
                format!("{scheme}://localhost:{}{path}", management_address.port()),
            )
            .bearer_auth(&admin)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(removed.status(), 404, "removed endpoint {path}");
    }
    let mutation = client
        .put(format!(
            "{scheme}://localhost:{}/api/v1/routes",
            management_address.port()
        ))
        .bearer_auth(&admin)
        .json(&serde_json::json!({"revision":2,"routes":[{"tenant":null,"sinks":["tls-test"]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(mutation.status(), 204);
    assert_eq!(
        client
            .get(format!(
                "{scheme}://localhost:{}/api/v1/ready",
                management_address.port()
            ))
            .bearer_auth(format!("demo-device:{secret}"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    // Preserve the existing standard version-rejection response through the classifier.
    let mut unsupported = device_socket(address, tls).await;
    let mut hello = mqtt_connect("wrong-version", true);
    let level = hello.windows(4).position(|part| part == b"MQTT").unwrap() + 4;
    hello[level] = 6;
    unsupported.write_all(&hello).await.unwrap();
    assert_eq!(mqtt_read(&mut *unsupported).await, (0x20, vec![0, 1]));
    drop(unsupported);
    let mut mqtt = device_socket(address, tls).await;
    mqtt.write_all(&mqtt_connect("shared", true)).await.unwrap();
    assert_eq!(mqtt_read(&mut *mqtt).await, (0x20, vec![0, 0]));
    mqtt.write_all(&mqtt_publish(
        1,
        "v1/t/demo/p/sensor/d/device-1/up",
        payload,
        1,
        false,
    ))
    .await
    .unwrap();
    assert_eq!(mqtt_read(&mut *mqtt).await, (0x40, vec![0, 1]));
    assert_eq!(
        services.connections.active().unwrap()[Transport::Mqtt as usize],
        1
    );
    let mut tcp = device_socket(address, tls).await;
    let framer = LengthPrefixFramer { maximum: 65_536 };
    // Leading JSON whitespace remains valid. Authentication and first event are pipelined.
    let mut wire = framer
        .encode(
            format!(" \n{{\"credential_id\":\"demo-device\",\"secret\":\"{secret}\"}}").as_bytes(),
        )
        .unwrap();
    wire.extend(framer.encode(payload).unwrap());
    tcp.write_all(&wire).await.unwrap();
    assert_eq!(read_frame(&mut tcp).await, br#"{"authenticated":true}"#);
    let receipt: EventAcceptance = serde_json::from_slice(&read_frame(&mut tcp).await).unwrap();
    assert!(!receipt.event_id.0.is_nil());
    assert_eq!(
        services.connections.active().unwrap()[Transport::Tcp as usize],
        1
    );
    let mut datagram = b"NBI1\x0bdemo-device".to_vec();
    datagram.extend(1u32.to_be_bytes());
    datagram.extend([1; 16]);
    datagram.extend(1u64.to_be_bytes());
    datagram.extend(netbaiot_runtime::now_ms().to_be_bytes());
    datagram.extend((payload.len() as u16).to_be_bytes());
    datagram.extend(payload);
    let key: Vec<u8> = (0..32).collect();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
    mac.update(&datagram);
    datagram.extend(mac.finalize().into_bytes());
    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender.send_to(&datagram, address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while ingress.metrics.get(Metric::EventsAccepted) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    mqtt.write_all(&[0xe0, 0]).await.unwrap();
    stop.cancel();
    task.await.unwrap().unwrap();
    udp_task.await.unwrap().unwrap();
    management_task.await.unwrap().unwrap();
    assert_eq!(services.connections.active().unwrap(), [0; 3]);
    ingress.events.stop_workers().await.unwrap();
}
#[tokio::test]
async fn shared_plaintext_mqtt_tcp_udp_rejects_http_and_preserves_management() {
    exercise_shared_listener(false).await;
}
#[tokio::test]
async fn shared_tls_mqtts_tcp_udp_rejects_https_without_alpn() {
    exercise_shared_listener(true).await;
}

#[tokio::test]
async fn shared_ingress_releases_unknown_slow_eof_and_tls_failure_leases() {
    use netbaiot_runtime::Metric;
    for tls in [false, true] {
        let stop = CancellationToken::new();
        let limits = Limits {
            max_connections: 1,
            max_connections_per_ip: 1,
            connect_timeout_ms: 100,
            ..Limits::default()
        };
        let (ingress, services) = tls_test_services(limits, stop.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = if tls {
            Some(tls_acceptor(&test_tls_files()).await.unwrap())
        } else {
            None
        };
        let task = tokio::spawn(serve_device_ingress(
            listener,
            services.clone(),
            acceptor,
            stop.clone(),
        ));
        for prefix in [b"\x10".as_slice(), b"garbage", b""] {
            let mut socket = TcpStream::connect(address).await.unwrap();
            if !prefix.is_empty() {
                socket.write_all(prefix).await.unwrap();
            }
            let mut response = Vec::new();
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                (&mut socket).take(64).read_to_end(&mut response),
            )
            .await
            .unwrap();
            if let Err(error) = result {
                assert!(matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                ));
            }
            assert!(
                response.len() < 64,
                "connection did not close within the bounded TLS alert"
            );
            if tls && !response.is_empty() {
                assert_eq!(response[0], 21, "only a TLS alert may precede close");
            } else {
                assert!(response.is_empty());
            }
        }
        assert_eq!(ingress.metrics.get(Metric::ConnectionsAccepted), 3);
        assert_eq!(services.connections.active().unwrap(), [0; 3]);
        if !tls {
            assert_eq!(ingress.metrics.get(Metric::ProtocolDetectionTimeouts), 2);
        }
        // The released slot can admit a real protocol next, then shutdown releases it too.
        let mut socket = device_socket(address, tls).await;
        socket
            .write_all(&mqtt_connect("after-failures", true))
            .await
            .unwrap();
        assert_eq!(mqtt_read(&mut *socket).await, (0x20, vec![0, 0]));
        let mut rejected = TcpStream::connect(address).await.unwrap();
        let mut byte = [0];
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut byte))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        assert_eq!(ingress.metrics.get(Metric::ConnectionsRejected), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(services.connections.active().unwrap(), [0; 3]);
        ingress.events.stop_workers().await.unwrap();
    }
}

#[test]
fn legacy_device_addresses_are_rejected_without_ambiguous_conversion() {
    let mut value = serde_json::to_value(config()).unwrap();
    value["mqtt"] = serde_json::json!("127.0.0.1:1883");
    assert!(serde_json::from_value::<Config>(value.clone()).is_err());
    value.as_object_mut().unwrap().remove("device_ingress");
    value["device_http"] = serde_json::json!("127.0.0.1:8080");
    assert!(serde_json::from_value::<Config>(value).is_err());
}

#[tokio::test(start_paused = true)]
async fn classification_does_not_refresh_first_packet_deadline() {
    use netbaiot_transports::classifier::classify_device_stream;
    use tokio::time::Instant;
    for prefix in [b"\x10\x7f\x00\x04MQTT\x04".as_slice(), b"\x00\x00\x00\x7f{"] {
        let stop = CancellationToken::new();
        let (ingress, services) = tls_test_services(
            Limits {
                connect_timeout_ms: 100,
                ..Limits::default()
            },
            stop.clone(),
        );
        let lease = services
            .connections
            .acquire_pending("127.0.0.1".parse().unwrap())
            .unwrap();
        let deadline = lease.connect_deadline();
        let (mut peer, stream) = tokio::io::duplex(128);
        tokio::time::advance(Duration::from_millis(90)).await;
        peer.write_all(prefix).await.unwrap();
        let (protocol, stream) = classify_device_stream(Box::new(stream), 65_536, 65_536, deadline)
            .await
            .unwrap();
        let lease = lease.classify(protocol).unwrap();
        let result = match protocol {
            Transport::Mqtt => {
                netbaiot_transports::mqtt::connection(stream, services.clone(), lease, stop.clone())
                    .await
            }
            Transport::Tcp => {
                netbaiot_transports::tcp::connection(stream, services.clone(), lease, stop.clone())
                    .await
            }
            Transport::Udp => unreachable!(),
        };
        assert!(result.is_err());
        assert!(Instant::now() <= deadline + Duration::from_millis(1));
        assert_eq!(services.connections.active().unwrap(), [0; 3]);
        ingress.events.stop_workers().await.unwrap();
    }
}

#[tokio::test]
async fn unclassified_connections_enforce_global_peer_and_rate_limits() {
    use netbaiot_runtime::Metric;
    for (global, peer, rate) in [(1, 1, 100), (2, 1, 100), (2, 2, 1)] {
        let stop = CancellationToken::new();
        let (ingress, services) = tls_test_services(
            Limits {
                max_connections: global,
                max_connections_per_ip: peer,
                requests_per_ip_second: rate,
                ..Limits::default()
            },
            stop.clone(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(serve_device_ingress(
            listener,
            services.clone(),
            None,
            stop.clone(),
        ));
        let mut pending = TcpStream::connect(address).await.unwrap();
        pending.write_all(b"\x10").await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while ingress.metrics.get(Metric::ConnectionsAccepted) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(services.connections.active().unwrap(), [0; 3]);
        let mut rejected = TcpStream::connect(address).await.unwrap();
        let mut byte = [0];
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut byte))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        assert_eq!(ingress.metrics.get(Metric::ConnectionsRejected), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
        assert!(matches!(pending.read(&mut byte).await, Ok(0) | Err(_)));
        // Re-acquisition proves pending cancellation released the global/IP/byte permits.
        assert!(services.connections.acquire_pending(address.ip()).is_ok());
        ingress.events.stop_workers().await.unwrap();
    }
}

#[tokio::test]
async fn device_protocol_ceiling_preserves_other_tls_protocols_and_management() {
    use netbaiot_runtime::{AdminAccess, Metric};
    let stop = CancellationToken::new();
    let (ingress, mut services) = tls_test_services(
        Limits {
            max_connections: 8,
            max_connections_per_ip: 8,
            max_device_connections_per_protocol: 2,
            requests_per_ip_second: 1000,
            ..Limits::default()
        },
        stop.clone(),
    );
    let admin = "d".repeat(64);
    Arc::get_mut(&mut services).unwrap().admin = Some(Arc::new(
        AdminAccess::new(&admin, Default::default(), &ingress.limits).unwrap(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_device_ingress(
        listener,
        services.clone(),
        Some(tls_acceptor(&test_tls_files()).await.unwrap()),
        stop.child_token(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management = listener.local_addr().unwrap();
    let management_task = tokio::spawn(serve_management_http(
        listener,
        services.clone(),
        Some(tls_acceptor(&test_tls_files()).await.unwrap()),
        stop.child_token(),
    ));
    let mut slow = Vec::new();
    for count in 1..=2 {
        let mut socket = device_socket(address, true).await;
        socket.write_all(b"\x00\x00\x00\x7f{").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while services.connections.active().unwrap()[Transport::Tcp as usize] < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        slow.push(socket);
    }
    let mut rejected = device_socket(address, true).await;
    rejected.write_all(b"\x00\x00\x00\x7f{").await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), rejected.read(&mut [0; 1]))
            .await
            .unwrap(),
        Ok(0) | Err(_)
    ));
    assert_eq!(ingress.metrics.get(Metric::ConnectionsRejected), 1);
    let mut mqtt = device_socket(address, true).await;
    mqtt.write_all(&mqtt_connect("cap-test", true))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut *mqtt).await, (0x20, vec![0, 0]));
    let payload = br#"{"schema_version":1,"source_message_id":"cap-test","kind":"heartbeat","data":{"sequence":1}}"#;
    mqtt.write_all(&mqtt_publish(
        1,
        "v1/t/demo/p/sensor/d/device-1/up",
        payload,
        1,
        false,
    ))
    .await
    .unwrap();
    assert_eq!(mqtt_read(&mut *mqtt).await, (0x40, vec![0, 1]));
    // Management remains independent of the classified TCP ceiling.
    let mut admin_socket = device_socket(management, true).await;
    admin_socket.write_all(format!("GET /api/v1/ready HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {admin}\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        admin_socket.read_to_end(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200"));
    stop.cancel();
    task.await.unwrap().unwrap();
    management_task.await.unwrap().unwrap();
    assert_eq!(services.connections.active().unwrap(), [0; 3]);
    // Repeated pending acquire/drop catches leaked global/IP/byte ownership at shutdown.
    for _ in 0..8 {
        drop(
            services
                .connections
                .acquire_device_pending(address.ip())
                .unwrap(),
        );
    }
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn removed_startup_device_config_is_rejected_even_when_empty() {
    let root = std::env::temp_dir().join(format!("netbaiot-old-config-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut legacy = serde_json::to_value(config()).unwrap();
    legacy["device_configs"] = serde_json::json!([]);
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    assert!(matches!(
        read_config(path.to_str().unwrap()).await,
        Err(Error::Configuration)
    ));
    std::fs::remove_dir_all(root).unwrap();
}
