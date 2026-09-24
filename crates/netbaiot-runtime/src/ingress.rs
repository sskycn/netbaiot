use crate::*;
use netbaiot_core::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

pub struct CodecRegistry {
    codecs: HashMap<(CodecId, u16), Arc<dyn DeviceCodec>>,
}

impl CodecRegistry {
    pub fn new(entries: Vec<(CodecId, u16, Arc<dyn DeviceCodec>)>) -> Result<Self> {
        if entries.is_empty() || entries.len() > 64 {
            return Err(Error::Configuration);
        }
        let mut codecs = HashMap::new();
        for (id, version, codec) in entries {
            if version == 0 || codecs.insert((id, version), codec).is_some() {
                return Err(Error::Configuration);
            }
        }
        Ok(Self { codecs })
    }

    pub fn get(&self, auth: &AuthenticatedDevice) -> Result<&Arc<dyn DeviceCodec>> {
        self.codecs
            .get(&(auth.codec_id.clone(), auth.codec_version))
            .ok_or(Error::Codec)
    }
}

pub struct IngressEnvelope<'a> {
    pub transport: Transport,
    pub payload: &'a [u8],
    pub require_command_ack: bool,
    pub validated_at: Instant,
    pub validation_us: u64,
}

pub struct IngressAcceptance {
    pub receipt: EventAcceptance,
    pub accepted_at: Instant,
}

pub struct Ingress {
    pub limits: Arc<Limits>,
    pub auth_cache: Arc<AuthCache>,
    pub codecs: CodecRegistry,
    pub events: Arc<EventBus>,
    pub control: Arc<GatewayControl>,
    pub metrics: Arc<Metrics>,
    pub sessions: Arc<Sessions>,
    pub admission: Arc<Admission>,
    pub lifecycle: Arc<Lifecycle>,
    auth_registration: Mutex<()>,
}

impl Ingress {
    /// Check deterministic codec and authorization failures before MQTT QoS 2
    /// transfers ownership. This does not reserve a sink or accept an event.
    pub fn validate_mqtt_qos2_payload(
        &self,
        auth: &AuthenticatedDevice,
        payload: &[u8],
        require_command_ack: bool,
    ) -> Result<()> {
        if !auth.permissions.publish {
            return Err(Error::Forbidden);
        }
        let kinds = self
            .codecs
            .get(auth)?
            .validate_payload(
                &DecodeContext {
                    device: &auth.device_key,
                    received_at: now_ms(),
                },
                payload,
            )
            .map_err(|_| Error::Codec)?;
        if kinds.len() != 1 {
            return Err(Error::Codec);
        }
        let kind = kinds.into_iter().next().ok_or(Error::Codec)?;
        if require_command_ack && !matches!(kind, DeviceEventKind::CommandAck(_)) {
            return Err(Error::Invalid);
        }
        if matches!(kind, DeviceEventKind::CommandAck(_)) && !auth.permissions.commands {
            return Err(Error::Forbidden);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        limits: Arc<Limits>,
        auth_cache: Arc<AuthCache>,
        codecs: CodecRegistry,
        events: Arc<EventBus>,
        control: Arc<GatewayControl>,
        metrics: Arc<Metrics>,
        sessions: Arc<Sessions>,
        lifecycle: Arc<Lifecycle>,
    ) -> Self {
        Self {
            admission: Admission::new(limits.clone()),
            limits,
            auth_cache,
            codecs,
            events,
            control,
            metrics,
            sessions,
            lifecycle,
            auth_registration: Mutex::new(()),
        }
    }

    pub fn is_draining(&self) -> bool {
        !self.lifecycle.ready()
    }

    pub async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        if !self.lifecycle.ready() {
            return Err(Error::Draining);
        }
        self.auth_cache
            .authenticate(request)
            .await
            .inspect_err(|_| {
                self.metrics.inc(Metric::AuthFailures);
            })
    }

    pub async fn authenticate_session(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedSessionCandidate> {
        if !self.lifecycle.ready() {
            return Err(Error::Draining);
        }
        self.auth_cache
            .authenticate_candidate(request)
            .await
            .inspect_err(|_| self.metrics.inc(Metric::AuthFailures))
    }

    pub fn register_session(
        &self,
        candidate: AuthenticatedSessionCandidate,
        transport: Transport,
    ) -> Result<(SessionLease, tokio::sync::mpsc::Receiver<QueuedCommand>)> {
        let (lease, receiver, ()) =
            self.register_session_with(candidate, transport, |_, _| Ok(()))?;
        Ok((lease, receiver))
    }

    /// Atomically fences the final transport-specific session establishment step against auth
    /// invalidation. Lock order is auth_registration -> AuthCache -> Sessions -> finalizer.
    pub fn register_session_with<T>(
        &self,
        candidate: AuthenticatedSessionCandidate,
        transport: Transport,
        finalize: impl FnOnce(&AuthenticatedDevice, u64) -> Result<T>,
    ) -> Result<(SessionLease, tokio::sync::mpsc::Receiver<QueuedCommand>, T)> {
        let _gate = lock(&self.auth_registration)?;
        if !self.auth_cache.candidate_is_current(&candidate)? {
            self.metrics.inc(Metric::AuthFailures);
            return Err(Error::Authentication);
        }
        let auth = Arc::new(candidate.auth);
        self.sessions.register_with(auth, transport, finalize)
    }

    pub fn invalidate_auth(
        &self,
        invalidation: &AuthInvalidation,
    ) -> Result<(Vec<DeviceKey>, usize)> {
        let (devices, disconnected, ()) = self.invalidate_auth_with(invalidation, || Ok(()))?;
        Ok((devices, disconnected))
    }

    /// Invalidates transport-specific persistent state under the same ordering gate as auth-cache
    /// invalidation and active-session cancellation.
    pub fn invalidate_auth_with<T>(
        &self,
        invalidation: &AuthInvalidation,
        finalize: impl FnOnce() -> Result<T>,
    ) -> Result<(Vec<DeviceKey>, usize, T)> {
        let _gate = lock(&self.auth_registration)?;
        let devices = self.auth_cache.invalidate(invalidation)?;
        let disconnected = self.sessions.disconnect_matching(invalidation)?;
        let finalized = finalize()?;
        Ok((devices, disconnected, finalized))
    }

    pub async fn ingest(
        &self,
        auth: &AuthenticatedDevice,
        envelope: IngressEnvelope<'_>,
    ) -> Result<IngressAcceptance> {
        let result = self.ingest_inner(auth, envelope).await;
        if result.is_err() {
            self.metrics.inc(Metric::IngressRejected);
            self.metrics.inc(Metric::EventsRejected);
        }
        result
    }

    async fn ingest_inner(
        &self,
        auth: &AuthenticatedDevice,
        envelope: IngressEnvelope<'_>,
    ) -> Result<IngressAcceptance> {
        let _gate = self.lifecycle.begin_admission()?;
        if !auth.permissions.publish {
            return Err(Error::Forbidden);
        }
        self.metrics
            .observe(Histogram::MqttProtocolValidation, envelope.validation_us);
        self.metrics.observe(
            Histogram::ValidationToAdmission,
            envelope.validated_at.elapsed().as_micros() as u64,
        );
        let admission = self
            .admission
            .acquire_wait(&auth.device_key, envelope.payload.len())
            .await
            .inspect_err(|_| self.metrics.inc(Metric::IngressAdmissionRejects))?;
        self.metrics
            .observe(Histogram::AdmissionWait, admission.wait_us());
        self.metrics
            .observe(Histogram::AdmissionLockWait, admission.lock_wait_us());
        self.metrics
            .observe(Histogram::AdmissionLockHold, admission.lock_hold_us());
        let codec_started = Instant::now();
        let events = self
            .codecs
            .get(auth)?
            .decode(
                &DecodeContext {
                    device: &auth.device_key,
                    received_at: now_ms(),
                },
                envelope.payload,
            )
            .map_err(|_| {
                self.metrics.inc(Metric::CodecFailures);
                Error::Codec
            })?;
        if events.len() != 1 {
            return Err(Error::Codec);
        }
        let event = events.into_iter().next().ok_or(Error::Codec)?;
        if event.device != auth.device_key {
            return Err(Error::Forbidden);
        }
        if envelope.require_command_ack && !matches!(event.kind, DeviceEventKind::CommandAck(_)) {
            return Err(Error::Invalid);
        }
        if matches!(event.kind, DeviceEventKind::CommandAck(_)) && !auth.permissions.commands {
            return Err(Error::Forbidden);
        }
        self.metrics.observe(
            Histogram::AuthenticationToCodec,
            codec_started.elapsed().as_micros() as u64,
        );
        self.metrics
            .add(Metric::IngressBytes, envelope.payload.len() as u64);
        // Presence is bounded bookkeeping and may reject a new historical identity. Keep that
        // fallible boundary before EventAccepted so a producer is never told failure after the
        // required business responsibility has been admitted.
        self.sessions.touch(&auth.device_key, envelope.transport)?;
        let publish_started = Instant::now();
        let receipt = self.events.publish(event)?;
        let accepted_at = Instant::now();
        self.metrics.observe(
            Histogram::CodecToEventAccepted,
            publish_started.elapsed().as_micros() as u64,
        );
        self.metrics.inc(Metric::IngressAccepted);
        if envelope.require_command_ack {
            self.metrics.inc(Metric::CommandAcked);
        }
        Ok(IngressAcceptance {
            receipt,
            accepted_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct TestCodec;
    impl DeviceCodec for TestCodec {
        fn decode(
            &self,
            ctx: &DecodeContext<'_>,
            _: &[u8],
        ) -> std::result::Result<Vec<DeviceEvent>, CodecError> {
            Ok(vec![DeviceEvent {
                event_id: EventId::generate(),
                source_message_id: SourceMessageId::new("presence-boundary").unwrap(),
                device: ctx.device.clone(),
                received_at: now_ms(),
                occurred_at: None,
                kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
            }])
        }

        fn encode(
            &self,
            _: &EncodeContext<'_>,
            _: &DeviceCommand,
        ) -> std::result::Result<Vec<u8>, CodecError> {
            Ok(Vec::new())
        }
    }

    struct TestSink;
    #[async_trait]
    impl EventSink for TestSink {
        async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
            Ok(SinkAck)
        }
    }

    fn identity(device: &str) -> AuthenticatedDevice {
        AuthenticatedDevice {
            device_key: DeviceKey {
                tenant_id: TenantId::new("tenant").unwrap(),
                product_id: ProductId::new("product").unwrap(),
                device_id: DeviceId::new(device).unwrap(),
            },
            credential_version: 1,
            auth_generation: 1,
            codec_id: CodecId::new("test").unwrap(),
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        }
    }

    #[tokio::test]
    async fn presence_capacity_failure_occurs_before_event_accepted() {
        let limits = Arc::new(Limits {
            max_devices: 1,
            max_devices_per_tenant: 1,
            max_connections: 1,
            max_connections_per_tenant: 1,
            ..Limits::default()
        });
        let metrics = Arc::new(Metrics::default());
        let active = identity("active");
        let candidate = identity("candidate");
        let sessions = Sessions::new(limits.clone());
        let (_lease, _) = sessions
            .register(Arc::new(active.clone()), Transport::Mqtt)
            .unwrap();
        let sink_id = SinkId::new("required").unwrap();
        let events = EventBus::new(
            limits.clone(),
            metrics.clone(),
            vec![SinkDefinition::bounded(
                sink_id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                Arc::new(TestSink),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![sink_id],
            }],
            1,
        )
        .unwrap();
        let provider = StaticAuthenticator::new(
            vec![Credential {
                credential_id: "candidate".into(),
                secret_hex: "00".repeat(32),
                identity: candidate.clone(),
            }],
            &limits,
        )
        .unwrap();
        let lifecycle = Arc::new(Lifecycle::starting());
        lifecycle.mark_running().unwrap();
        let ingress = Ingress::new(
            limits.clone(),
            AuthCache::new(provider, limits.clone(), metrics.clone()),
            CodecRegistry::new(vec![(
                CodecId::new("test").unwrap(),
                1,
                Arc::new(TestCodec),
            )])
            .unwrap(),
            events.clone(),
            GatewayControl::empty(limits),
            metrics,
            sessions,
            lifecycle,
        );
        assert!(
            ingress
                .ingest(
                    &candidate,
                    IngressEnvelope {
                        transport: Transport::Tcp,
                        payload: b"x",
                        require_command_ack: false,
                        validated_at: Instant::now(),
                        validation_us: 0,
                    },
                )
                .await
                .is_err()
        );
        assert_eq!(events.usage().unwrap().events, 0);
        events.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn auth_invalidation_epoch_fences_session_registration_for_every_scope() {
        let limits = Arc::new(Limits::default());
        let metrics = Arc::new(Metrics::default());
        let identity = identity("revoked");
        let provider = StaticAuthenticator::new(
            vec![Credential {
                credential_id: "revoked".into(),
                secret_hex: "00".repeat(32),
                identity: identity.clone(),
            }],
            &limits,
        )
        .unwrap();
        let sink_id = SinkId::new("auth-race").unwrap();
        let events = EventBus::new(
            limits.clone(),
            metrics.clone(),
            vec![SinkDefinition::bounded(
                sink_id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                Arc::new(TestSink),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![sink_id],
            }],
            1,
        )
        .unwrap();
        let lifecycle = Arc::new(Lifecycle::starting());
        lifecycle.mark_running().unwrap();
        let ingress = Ingress::new(
            limits.clone(),
            AuthCache::new(provider, limits.clone(), metrics.clone()),
            CodecRegistry::new(vec![(
                CodecId::new("test").unwrap(),
                1,
                Arc::new(TestCodec),
            )])
            .unwrap(),
            events.clone(),
            GatewayControl::empty(limits.clone()),
            metrics,
            Sessions::new(limits),
            lifecycle,
        );
        let invalidations = [
            AuthInvalidation::Device {
                device: identity.device_key.clone(),
            },
            AuthInvalidation::Product {
                tenant_id: identity.device_key.tenant_id.clone(),
                product_id: identity.device_key.product_id.clone(),
            },
            AuthInvalidation::Tenant {
                tenant_id: identity.device_key.tenant_id.clone(),
            },
            AuthInvalidation::CredentialVersion { version: 1 },
            AuthInvalidation::AuthGeneration { generation: 1 },
            AuthInvalidation::All,
        ];
        for invalidation in invalidations {
            let candidate = ingress
                .authenticate_session(AuthenticationRequest::Secret {
                    credential_id: "revoked",
                    secret: b"0000000000000000000000000000000000000000000000000000000000000000",
                })
                .await
                .unwrap();
            ingress.invalidate_auth(&invalidation).unwrap();
            assert!(
                matches!(
                    ingress.register_session(candidate, Transport::Mqtt),
                    Err(Error::Authentication)
                ),
                "a pre-invalidation candidate must never become a live session"
            );
        }
        events.stop_workers().await.unwrap();
    }
}
