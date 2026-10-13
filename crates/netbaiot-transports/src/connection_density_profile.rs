//! Isolated release-mode allocation/size probe; never a connection capacity claim.
//! Run with one test thread so the instrumented allocator is attributable.
use super::*;
use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use std::{future, mem::size_of_val};

struct Provider;
#[async_trait]
impl DeviceAuthenticator for Provider {
    async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
        Err(Error::Authentication)
    }
    async fn resolve_verifier(&self, _: &str) -> Result<DeviceVerifier> {
        Err(Error::Authentication)
    }
}
struct Sink;
#[async_trait]
impl EventSink for Sink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        Ok(SinkAck)
    }
}
fn services() -> Arc<Services> {
    let limits = Arc::new(Limits::default());
    let metrics = Arc::new(Metrics::default());
    let sink = SinkId::new("profile").unwrap();
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![SinkDefinition::bounded(
            sink.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(Sink),
            &limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink],
        }],
        1,
    )
    .unwrap();
    let lifecycle = Arc::new(Lifecycle::starting());
    lifecycle.mark_running().unwrap();
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        AuthCache::new(Arc::new(Provider), limits.clone(), metrics.clone()),
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
    Services::new(ingress, CancellationToken::new())
}
fn delta(name: &str, region: &stats_alloc::Region<'_, std::alloc::System>, count: usize) {
    let change = region.change();
    println!(
        "DENSITY_ALLOC,{name},{count},{},{},{},{},{}",
        change.allocations,
        change.deallocations,
        change.bytes_allocated,
        change.bytes_deallocated,
        change.bytes_allocated as i128 - change.bytes_deallocated as i128
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual release allocation probe; run in isolation with --test-threads=1"]
async fn density_task_future_profile() {
    let s = services();
    for transport in [Transport::Mqtt, Transport::Tcp] {
        let (_, duplex) = tokio::io::duplex(64);
        let stream: BoxStream = Box::new(duplex);
        let lease = s
            .connections
            .acquire("127.0.0.1".parse().unwrap(), transport)
            .unwrap();
        let size = match transport {
            Transport::Mqtt => size_of_val(&crate::mqtt::connection(
                stream,
                s.clone(),
                lease,
                CancellationToken::new(),
            )),
            Transport::Tcp => size_of_val(&crate::tcp::connection(
                stream,
                s.clone(),
                lease,
                CancellationToken::new(),
            )),
            _ => unreachable!(),
        };
        println!("DENSITY_SIZE,connection_future_{transport:?},{size}");
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = tokio::spawn(TcpStream::connect(listener.local_addr().unwrap()));
    let (socket, address) = listener.accept().await.unwrap();
    let _peer = peer.await.unwrap().unwrap();
    let lease = s.connections.acquire_device_pending(address.ip()).unwrap();
    let accepted = serve_accepted(
        socket,
        address,
        ListenerKind::DeviceIngress,
        s,
        lease,
        None,
        CancellationToken::new(),
    );
    println!(
        "DENSITY_SIZE,serve_accepted_future,{}",
        size_of_val(&accepted)
    );
    drop(accepted);
    println!(
        "DENSITY_SIZE,sleep_future,{}",
        size_of_val(&tokio::time::sleep(Duration::from_secs(60)))
    );
    println!(
        "DENSITY_SIZE,tls_server_connection,{}",
        std::mem::size_of::<tokio_rustls::rustls::ServerConnection>()
    );
    println!(
        "DENSITY_SIZE,tls_accept_future,{}",
        std::mem::size_of::<tokio_rustls::Accept<TcpStream>>()
    );
    let root = CancellationToken::new();
    let count = 1000;
    let mut tokens = Vec::with_capacity(count);
    let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
    for _ in 0..count {
        tokens.push(root.child_token());
    }
    delta("child_tokens", &region, count);
    drop(tokens);
    let mut clones = Vec::with_capacity(count);
    let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
    for _ in 0..count {
        clones.push(root.clone());
    }
    delta("token_clones", &region, count);
    drop(clones);
    let mut handles = Vec::with_capacity(count);
    let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
    for _ in 0..count {
        handles.push(tokio::spawn(future::pending::<()>()));
    }
    tokio::task::yield_now().await;
    delta("pending_tasks", &region, count);
    for handle in &handles {
        handle.abort();
    }
    for handle in handles {
        let _ = handle.await;
    }
    let mut tasks = JoinSet::new();
    let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
    for _ in 0..count {
        tasks.spawn(future::pending::<()>());
    }
    tokio::task::yield_now().await;
    delta("pending_tasks_joinset", &region, count);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let mut tasks = JoinSet::new();
    let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
    for _ in 0..count {
        tasks.spawn(tokio::time::sleep(Duration::from_secs(600)));
    }
    tokio::task::yield_now().await;
    delta("sleep_tasks_joinset", &region, count);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[derive(Debug)]
struct NoCertificate;
impl tokio_rustls::rustls::server::ResolvesServerCert for NoCertificate {
    fn resolve(
        &self,
        _: tokio_rustls::rustls::server::ClientHello<'_>,
    ) -> Option<Arc<tokio_rustls::rustls::sign::CertifiedKey>> {
        None
    }
}

#[tokio::test]
async fn tls_handshake_saturation_closes_immediately_and_shutdown_releases_slot() {
    let mut s = services();
    Arc::get_mut(&mut s).unwrap().tls_handshakes = Arc::new(tokio::sync::Semaphore::new(1));
    let config = tokio_rustls::rustls::ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_cert_resolver(Arc::new(NoCertificate));
    let tls = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let first = TcpStream::connect(address).await.unwrap();
    let (socket, peer) = listener.accept().await.unwrap();
    let lease = s.connections.acquire_device_pending(peer.ip()).unwrap();
    let stop = CancellationToken::new();
    let held = tokio::spawn(serve_accepted(
        socket,
        peer,
        ListenerKind::DeviceIngress,
        s.clone(),
        lease,
        Some(tls.clone()),
        stop.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while s.tls_handshakes.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut second = TcpStream::connect(address).await.unwrap();
    let (socket, peer) = listener.accept().await.unwrap();
    let lease = s.connections.acquire_device_pending(peer.ip()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        serve_accepted(
            socket,
            peer,
            ListenerKind::DeviceIngress,
            s.clone(),
            lease,
            Some(tls),
            stop.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(second.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(s.ingress.metrics.get(Metric::ConnectionsRejected), 1);
    assert_eq!(s.tls_handshakes.available_permits(), 0);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), held)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.tls_handshakes.available_permits(), 1);
    drop(first);
}
