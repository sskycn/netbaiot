use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{BoxStream, Services};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "baseline")]
#[path = "full_ready_auth_impl/baseline.rs"]
mod implementation;
#[cfg(not(feature = "baseline"))]
#[path = "full_ready_auth_impl/candidate.rs"]
mod implementation;

struct CountingProvider {
    calls: AtomicUsize,
    auth: AuthenticatedDevice,
    delay: bool,
}

#[async_trait]
impl DeviceAuthenticator for CountingProvider {
    async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.delay {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Ok(self.auth.clone())
    }

    async fn resolve_verifier(&self, _: &str) -> Result<DeviceVerifier> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.delay {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Ok(DeviceVerifier::new(self.auth.clone(), [7; 32]))
    }
}

struct AckSink;
#[async_trait]
impl EventSink for AckSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        Ok(SinkAck)
    }
}

fn auth() -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("a").unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    }
}

fn runtime(
    limits: Limits,
    provider: Arc<CountingProvider>,
) -> (Arc<Ingress>, Arc<Services>, CancellationToken) {
    runtime_with_sink(limits, provider, Arc::new(AckSink))
}

fn runtime_with_sink(
    limits: Limits,
    provider: Arc<CountingProvider>,
    sink: Arc<dyn EventSink>,
) -> (Arc<Ingress>, Arc<Services>, CancellationToken) {
    let limits = Arc::new(limits);
    let metrics = Arc::new(Metrics::default());
    let lifecycle = Arc::new(Lifecycle::starting());
    lifecycle.mark_running().unwrap();
    let sink_id = SinkId::new("sink").unwrap();
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![SinkDefinition::bounded(
            sink_id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            sink,
            &limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink_id],
        }],
        1,
    )
    .unwrap();
    let config = GatewayControl::empty(limits.clone());
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
        config,
        metrics,
        Sessions::new(limits),
        lifecycle,
    ));
    let stop = CancellationToken::new();
    let services = Services::new(ingress.clone(), stop.clone());
    (ingress, services, stop)
}

#[tokio::test]
async fn full_prefilled_reader_ready_authentication() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0), auth: auth(), delay: false,
    });
    let (_, services, stop) = runtime(Limits::default(), provider.clone());
    // No quota, grow, incomplete frame or remote provider delay participates.
    let mut reader = implementation::Reader::new(8, 30_000);
    reader.read_more_to(
        &mut &[7; 8][..],
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        8,
    ).await.unwrap();
    assert_eq!(reader.buffer.len(), 8);
    assert_eq!(reader.buffer.capacity(), 8);
    let (_peer_kept_open, stream) = tokio::io::duplex(8);
    let mut stream: BoxStream = Box::new(stream);
    let result = implementation::authenticate_stream(
        &services,
        AuthenticationRequest::Secret { credential_id: "a", secret: b"secret" },
        &mut reader, &mut stream, &stop,
    ).await;
    let calls = provider.calls.load(Ordering::Relaxed);
    println!("full_ready_auth result_success={} error={:?} provider_calls={} buffer_len={} capacity={}",
        result.is_ok(), result.as_ref().err(), calls, reader.buffer.len(), reader.buffer.capacity());
    assert!(result.is_ok(), "full reader rejected ready authentication: {:?}; provider_calls={calls}", result.as_ref().err());
    assert_eq!(calls, 1);
    assert_eq!(&reader.buffer[..], &[7; 8]);
}
