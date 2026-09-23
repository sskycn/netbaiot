use super::*;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_codecs::JsonV1;
use sha2::Sha256;
use std::{
    io,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Provider {
    calls: AtomicUsize,
    auth: Mutex<AuthenticatedDevice>,
}
#[async_trait]
impl DeviceAuthenticator for Provider {
    async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
        Err(Error::Authentication)
    }
    async fn resolve_verifier(&self, id: &str) -> Result<DeviceVerifier> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if id != "a" {
            return Err(Error::Authentication);
        }
        Ok(DeviceVerifier::new(
            self.auth.lock().unwrap().clone(),
            [7; 32],
        ))
    }
}
struct PendingSink;
#[async_trait]
impl EventSink for PendingSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        std::future::pending().await
    }
}
fn auth() -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: false,
        },
    }
}
fn fixture(limits: Limits) -> (Arc<Ingress>, Arc<Provider>) {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        auth: Mutex::new(auth()),
    });
    let limits = Arc::new(limits);
    let metrics = Arc::new(Metrics::default());
    let lifecycle = Arc::new(Lifecycle::starting());
    lifecycle.mark_running().unwrap();
    let sink_id = SinkId::new("pending").unwrap();
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![SinkDefinition::bounded(
            sink_id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(PendingSink),
            &limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink_id],
        }],
        1,
    )
    .unwrap();
    (
        Arc::new(Ingress::new(
            limits.clone(),
            AuthCache::new(provider.clone(), limits.clone(), metrics.clone()),
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
        )),
        provider,
    )
}
const PAYLOAD: &[u8] =
    br#"{"schema_version":1,"source_message_id":"udp:42","kind":"heartbeat","data":{"sequence":42}}"#;
fn packet(id: &str, version: u32, sequence: u64, timestamp: i64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = b"NBI1".to_vec();
    bytes.push(id.len() as u8);
    bytes.extend_from_slice(id.as_bytes());
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&[3; 16]);
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    bytes.extend_from_slice(payload);
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&bytes);
    bytes.extend_from_slice(&mac.finalize().into_bytes());
    bytes
}
fn assert_ack(bytes: &[u8], version: u32, sequence: u64) {
    assert_eq!(bytes.len(), 64);
    assert_eq!(&bytes[..4], b"NBA1");
    assert_eq!(&bytes[4..8], &version.to_be_bytes());
    assert_eq!(&bytes[8..24], &[3; 16]);
    assert_eq!(&bytes[24..32], &sequence.to_be_bytes());
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&bytes[..32]);
    mac.verify_slice(&bytes[32..]).unwrap();
}
#[test]
fn wire_hmac_tampering_and_no_byte_amplification() {
    let verifier = DeviceVerifier::new(auth(), [7; 32]);
    let ack = encode_ack(&verifier, 0x01020304, [3; 16], 0x0102030405060708).unwrap();
    assert_ack(&ack, 0x01020304, 0x0102030405060708);
    for offset in 0..64 {
        let mut corrupt = ack;
        corrupt[offset] ^= 1;
        assert!(verifier.verify(&corrupt[..32], &corrupt[32..]).is_err());
    }
    let minimum = packet("a", 1, 0, 0, b"");
    assert_eq!(minimum.len(), MIN_ENVELOPE_SIZE);
    assert!(decode(&minimum, 1200).is_ok());
    assert!(ack.len() <= minimum.len());
}
#[test]
fn replay_decisions_versions_clock_edges_and_memory() {
    let limits = Arc::new(Limits::default());
    let mut replay = ReplayWindow::new(limits.clone());
    let device = auth().device_key;
    for seq in [100, 102, 101] {
        assert_eq!(
            replay
                .check(&device, 1, [3; 16], seq, 50_000, 50_000)
                .unwrap(),
            ReplayDecision::New
        );
        replay.commit(device.clone(), 1, [3; 16], seq, 50_000);
    }
    for seq in [100, 102, 101] {
        assert_eq!(
            replay
                .check(&device, 1, [3; 16], seq, 50_000, 50_000)
                .unwrap(),
            ReplayDecision::AcceptedDuplicate
        );
    }
    for timestamp in [20_000, 80_000] {
        assert_eq!(
            replay
                .check(&device, 1, [3; 16], 101, timestamp, 50_000)
                .unwrap(),
            ReplayDecision::AcceptedDuplicate
        );
    }
    for timestamp in [19_999, 80_001, i64::MIN, i64::MAX] {
        assert!(
            replay
                .check(&device, 1, [3; 16], 101, timestamp, 50_000)
                .is_err()
        );
    }
    assert_eq!(
        replay
            .check(&device, 2, [3; 16], 101, 50_000, 50_000)
            .unwrap(),
        ReplayDecision::New
    );
    assert!(
        replay
            .check(&device, 1, [3; 16], 38, 50_000, 50_000)
            .is_err()
    );
    assert_eq!(
        replay
            .check(&device, 1, [3; 16], 39, 50_000, 50_000)
            .unwrap(),
        ReplayDecision::New
    );
    let before = std::mem::size_of::<((DeviceKey, [u8; 16]), Replay)>();
    let after = std::mem::size_of::<((DeviceKey, [u8; 16], u32), Replay)>();
    println!(
        "Replay bucket payload: baseline={before}, final={after}, delta={}, max_entry_delta={}",
        after - before,
        (after - before) * limits.max_replay_entries
    );
    assert!(after - before <= 8);
}
#[tokio::test]
async fn send_failure_commits_and_ten_thousand_duplicates_do_not_ingest() {
    let (ingress, provider) = fixture(Limits::default());
    let mut replay = ReplayWindow::new(ingress.limits.clone());
    let bytes = packet("a", 1, 42, now_ms(), PAYLOAD);
    accept_and_ack(&bytes, &ingress, &mut replay, |_| {
        Err(io::ErrorKind::WouldBlock.into())
    })
    .await
    .unwrap();
    let original = ingress.events.spool_records().unwrap();
    assert_eq!(original.len(), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAckSendFailures), 1);
    let expires = replay.entries.values().next().unwrap().expires;
    for _ in 0..10_000 {
        accept_and_ack(&bytes, &ingress, &mut replay, |ack| {
            assert_ack(ack, 1, 42);
            Ok(ack.len())
        })
        .await
        .unwrap();
    }
    assert_eq!(replay.entries.len(), 1);
    assert_eq!(replay.entries.values().next().unwrap().expires, expires);
    assert_eq!(
        ingress.events.spool_records().unwrap()[0].event.event_id,
        original[0].event.event_id
    );
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAcceptedDuplicates), 10_000);
    assert_eq!(ingress.metrics.get(Metric::UdpAcksSent), 10_000);
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    // No ACK work participates in quiesce or the pending required delivery spool.
    tokio::time::timeout(
        Duration::from_millis(100),
        ingress.lifecycle.begin_quiesce(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        accept_and_ack(&bytes, &ingress, &mut replay, |_| panic!("draining ACK")).await,
        Err(Error::Draining)
    ));
    assert_eq!(ingress.events.usage().unwrap().pending_required, 1);
    ingress.events.stop_workers().await.unwrap();
}
#[tokio::test]
async fn failed_ingest_is_new_and_first_accepted_content_wins() {
    let (ingress, _) = fixture(Limits::default());
    let mut replay = ReplayWindow::new(ingress.limits.clone());
    let bad = packet("a", 1, 200, now_ms(), b"invalid JSON");
    assert!(
        accept_and_ack(&bad, &ingress, &mut replay, |_| panic!("invalid ACK"))
            .await
            .is_err()
    );
    assert!(replay.entries.is_empty());
    let good = packet("a", 1, 200, now_ms(), PAYLOAD);
    accept_and_ack(&good, &ingress, &mut replay, |a| Ok(a.len()))
        .await
        .unwrap();
    // A client violates message immutability here: the first accepted message still wins.
    accept_and_ack(&bad, &ingress, &mut replay, |a| Ok(a.len()))
        .await
        .unwrap();
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::CodecFailures), 1);
    ingress.events.stop_workers().await.unwrap();
}
#[tokio::test]
async fn rotation_invalidation_and_inflight_signer_are_fenced() {
    let (ingress, provider) = fixture(Limits::default());
    let mut replay = ReplayWindow::new(ingress.limits.clone());
    let old = packet("a", 1, 42, now_ms(), PAYLOAD);
    accept_and_ack(&old, &ingress, &mut replay, |a| Ok(a.len()))
        .await
        .unwrap();
    let env = decode(&old, 1200).unwrap();
    let verified = ingress
        .auth_cache
        .verify_signed_with_verifier("a", env.signed, env.tag)
        .await
        .unwrap();
    provider.auth.lock().unwrap().credential_version = 2;
    ingress.invalidate_auth(&AuthInvalidation::All).unwrap();
    assert!(
        ingress
            .auth_cache
            .with_current_verifier::<()>(&verified, |_| panic!("stale signer"))
            .is_err()
    );
    assert!(
        accept_and_ack(&old, &ingress, &mut replay, |_| panic!("old version ACK"))
            .await
            .is_err()
    );
    let new = packet("a", 2, 42, now_ms(), PAYLOAD);
    accept_and_ack(&new, &ingress, &mut replay, |a| {
        assert_ack(a, 2, 42);
        Ok(a.len())
    })
    .await
    .unwrap();
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 2);
    assert_eq!(provider.calls.load(Ordering::Relaxed), 2);
    provider.auth.lock().unwrap().permissions.publish = false;
    ingress.invalidate_auth(&AuthInvalidation::All).unwrap();
    assert!(
        accept_and_ack(&new, &ingress, &mut replay, |_| panic!(
            "revoked permission ACK"
        ))
        .await
        .is_err()
    );
    ingress.events.stop_workers().await.unwrap();
}
async fn sockets(
    ingress: Arc<Ingress>,
) -> (
    UdpSocket,
    CancellationToken,
    tokio::task::JoinHandle<Result<()>>,
) {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let stop = CancellationToken::new();
    let services = Services::new(ingress, stop.clone());
    let task = tokio::spawn(serve(server, services, stop.clone()));
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(address).await.unwrap();
    (client, stop, task)
}
async fn receive(client: &UdpSocket) -> Vec<u8> {
    let mut bytes = [0; 128];
    let len = tokio::time::timeout(Duration::from_secs(1), client.recv(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    bytes[..len].to_vec()
}
async fn silent(client: &UdpSocket, bytes: &[u8]) {
    client.send(bytes).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(35), client.recv(&mut [0; 128]))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn real_socket_lost_ack_retries_once_and_shutdown_is_silent() {
    let (ingress, provider) = fixture(Limits::default());
    let (client, stop, task) = sockets(ingress.clone()).await;
    let bytes = packet("a", 1, 42, now_ms(), PAYLOAD);
    client.send(&bytes).await.unwrap();
    let lost_ack = receive(&client).await; // Deliberately discard this receipt at the application.
    assert_ack(&lost_ack, 1, 42);
    client.send(&bytes).await.unwrap();
    assert_eq!(receive(&client).await, lost_ack);
    assert_eq!(ingress.events.spool_records().unwrap().len(), 1);
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAcksSent), 2);
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    ingress.lifecycle.begin_quiesce().await.unwrap();
    silent(&client, &bytes).await;
    silent(&client, &packet("a", 1, 43, now_ms(), PAYLOAD)).await;
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}
#[tokio::test]
async fn real_socket_invalid_auth_codec_clock_old_replay_and_full_bus_are_silent() {
    let (ingress, _) = fixture(Limits {
        sink_queue_max_count: 1,
        sink_delivery_concurrency: 1,
        ..Limits::default()
    });
    let (client, stop, task) = sockets(ingress.clone()).await;
    let good = packet("a", 1, 100, now_ms(), PAYLOAD);
    let mut bad_hmac = good.clone();
    *bad_hmac.last_mut().unwrap() ^= 1;
    let mut bad_magic = good.clone();
    bad_magic[0] = b'X';
    let mut malformed = good.clone();
    malformed[42..44].copy_from_slice(&u16::MAX.to_be_bytes());
    let forbidden = br#"{"schema_version":1,"source_message_id":"cmd:1","kind":"command_ack","data":{"command_id":"00000000-0000-4000-8000-000000000001","execution":"succeeded"}}"#;
    let mut replay = ReplayWindow::new(ingress.limits.clone());
    assert!(matches!(
        accept_and_ack(
            &packet("a", 1, 99, now_ms(), forbidden),
            &ingress,
            &mut replay,
            |_| panic!("forbidden ACK")
        )
        .await,
        Err(Error::Forbidden)
    ));
    for bytes in [
        b"random".to_vec(),
        bad_magic,
        malformed,
        bad_hmac,
        packet("unknown", 1, 100, now_ms(), PAYLOAD),
        packet("a", 2, 100, now_ms(), PAYLOAD),
        packet("a", 1, 100, now_ms(), b"{"),
        packet("a", 1, 100, now_ms(), forbidden),
        packet("a", 1, 100, now_ms() - 31_000, PAYLOAD),
        packet("a", 1, 100, now_ms() + 31_000, PAYLOAD),
        vec![0; 1201],
    ] {
        silent(&client, &bytes).await;
    }
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 0);
    client.send(&good).await.unwrap();
    assert_ack(&receive(&client).await, 1, 100);
    silent(&client, &packet("a", 1, 36, now_ms(), PAYLOAD)).await;
    silent(&client, &packet("a", 1, 101, now_ms(), PAYLOAD)).await; // required sink full
    ingress.events.close_admission().unwrap();
    silent(&client, &packet("a", 1, 102, now_ms(), PAYLOAD)).await;
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}
#[tokio::test]
async fn real_socket_duplicate_does_not_bypass_source_rate_limit() {
    let (ingress, _) = fixture(Limits {
        requests_per_second: 1,
        requests_per_ip_second: 1,
        ..Limits::default()
    });
    let (client, stop, task) = sockets(ingress.clone()).await;
    let good = packet("a", 1, 1, now_ms(), PAYLOAD);
    client.send(&good).await.unwrap();
    receive(&client).await;
    silent(&client, &good).await;
    assert_eq!(ingress.metrics.get(Metric::UdpAcksSent), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}

#[test]
fn replay_versions_share_device_tenant_and_process_capacity() {
    let limits = Arc::new(Limits {
        max_replay_entries: 3,
        max_replay_entries_per_tenant: 2,
        max_replay_entries_per_device: 1,
        ..Limits::default()
    });
    let mut replay = ReplayWindow::new(limits);
    let a = auth().device_key;
    let mut b = a.clone();
    b.device_id = DeviceId::new("b").unwrap();
    let mut c = a.clone();
    c.device_id = DeviceId::new("c").unwrap();
    let mut other = a.clone();
    other.tenant_id = TenantId::new("other").unwrap();
    replay.commit(a.clone(), 1, [0; 16], 1, 1000);
    assert!(matches!(
        replay.check(&a, 2, [0; 16], 1, 1000, 1000),
        Err(Error::Overloaded)
    ));
    replay.commit(b, 1, [0; 16], 1, 1000);
    assert!(matches!(
        replay.check(&c, 1, [0; 16], 1, 1000, 1000),
        Err(Error::Overloaded)
    ));
    replay.check(&other, 1, [0; 16], 1, 1000, 1000).unwrap();
    replay.commit(other, 1, [0; 16], 1, 1000);
    c.tenant_id = TenantId::new("third").unwrap();
    assert!(matches!(
        replay.check(&c, 1, [0; 16], 1, 1000, 1000),
        Err(Error::Overloaded)
    ));
    assert_eq!(
        replay.check(&a, 1, [0; 16], 1, 1000, 1000).unwrap(),
        ReplayDecision::AcceptedDuplicate
    );
    assert_eq!(replay.entries.len(), 3);
}

#[tokio::test]
async fn real_socket_admission_rejection_does_not_consume_sequence() {
    let (ingress, _) = fixture(Limits {
        max_ingress_per_device: 1,
        ingress_wait_timeout_ms: 5,
        ..Limits::default()
    });
    let (client, stop, task) = sockets(ingress.clone()).await;
    let lease = ingress.admission.acquire(&auth().device_key, 100).unwrap();
    let good = packet("a", 1, 1, now_ms(), PAYLOAD);
    silent(&client, &good).await;
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 0);
    drop(lease);
    client.send(&good).await.unwrap();
    assert_ack(&receive(&client).await, 1, 1);
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAcceptedDuplicates), 0);
    assert!(
        !ingress
            .sessions
            .connection(&auth().device_key)
            .unwrap()
            .connected
    );
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}
