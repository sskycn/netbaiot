use crate::*;
use netbaiot_core::*;
use std::{collections::HashMap, sync::Arc, time::Instant};

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
    pub require_config_ack: bool,
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
    pub config: Arc<ConfigCache>,
    pub metrics: Arc<Metrics>,
    pub sessions: Arc<Sessions>,
    pub admission: Arc<Admission>,
    pub lifecycle: Arc<Lifecycle>,
}

impl Ingress {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        limits: Arc<Limits>,
        auth_cache: Arc<AuthCache>,
        codecs: CodecRegistry,
        events: Arc<EventBus>,
        config: Arc<ConfigCache>,
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
            config,
            metrics,
            sessions,
            lifecycle,
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
        if envelope.require_config_ack && !matches!(event.kind, DeviceEventKind::ConfigAck(_)) {
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
        let publish_started = Instant::now();
        let receipt = self.events.publish(event)?;
        let accepted_at = Instant::now();
        self.metrics.observe(
            Histogram::CodecToEventAccepted,
            publish_started.elapsed().as_micros() as u64,
        );
        self.sessions.touch(&auth.device_key, envelope.transport)?;
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
