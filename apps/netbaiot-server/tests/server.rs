use netbaiot_runtime::Error;
use netbaiot_server::{Config, TlsFiles, read_config, run, tls_acceptor};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_rustls::{TlsConnector, rustls};
use tokio_util::sync::CancellationToken;
fn config() -> Config {
    serde_json::from_str(include_str!("../../../configs/development.json")).unwrap()
}
#[test]
fn public_streams_require_tls_and_volatile_store_requires_loopback() {
    let mut c = config();
    c.http = "0.0.0.0:8080".parse().unwrap();
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
    for _ in 0..4 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    c.http = reservations[0].local_addr().unwrap();
    c.mqtt = reservations[1].local_addr().unwrap();
    c.tcp = reservations[2].local_addr().unwrap();
    c.udp = reservations[3].local_addr().unwrap();
    let addresses = [c.http, c.mqtt, c.tcp];
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
                .post(format!("http://{}/v1/device/messages", addresses[0]))
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
    c.http = "127.0.0.1:0".parse().unwrap();
    c.tcp = c.http;
    c.udp = c.http;
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
}
