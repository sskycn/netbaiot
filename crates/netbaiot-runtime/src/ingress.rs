use crate::*;
use netbaiot_core::*;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
        for (id, v, c) in entries {
            if v == 0 || codecs.insert((id, v), c).is_some() {
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
    pub receipt: IngressReceipt,
    /// The store call has crossed its configured persistence boundary at this instant.
    pub persisted_at: Instant,
}
pub struct Ingress {
    pub limits: Arc<Limits>,
    pub authenticator: Arc<dyn DeviceAuthenticator>,
    pub codecs: CodecRegistry,
    pub store: Arc<dyn Store>,
    pub metrics: Arc<Metrics>,
    pub sessions: Arc<Sessions>,
    pub admission: Arc<Admission>,
    draining: AtomicBool,
}
impl Ingress {
    pub fn new(
        limits: Arc<Limits>,
        authenticator: Arc<dyn DeviceAuthenticator>,
        codecs: CodecRegistry,
        store: Arc<dyn Store>,
        metrics: Arc<Metrics>,
        sessions: Arc<Sessions>,
    ) -> Self {
        Self {
            admission: Admission::new(limits.clone()),
            limits,
            authenticator,
            codecs,
            store,
            metrics,
            sessions,
            draining: AtomicBool::new(false),
        }
    }
    pub fn drain(&self) {
        self.draining.store(true, Ordering::Release);
    }
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
    pub async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        if self.is_draining() {
            return Err(Error::Draining);
        }
        let result = deadline(
            self.limits.authentication_timeout_ms,
            self.authenticator.authenticate(request),
        )
        .await;
        if result.is_err() {
            self.metrics.inc(Metric::AuthFailures);
        }
        result
    }
    pub async fn ingest(
        &self,
        auth: &AuthenticatedDevice,
        envelope: IngressEnvelope<'_>,
    ) -> Result<IngressAcceptance> {
        let result = self.ingest_inner(auth, envelope).await;
        if result.is_err() {
            self.metrics.inc(Metric::IngressRejected);
        }
        if matches!(result, Err(Error::Timeout)) {
            self.metrics.inc(Metric::Timeouts);
        }
        result
    }
    async fn ingest_inner(
        &self,
        auth: &AuthenticatedDevice,
        envelope: IngressEnvelope<'_>,
    ) -> Result<IngressAcceptance> {
        if self.is_draining() {
            return Err(Error::Draining);
        }
        if !auth.permissions.publish {
            return Err(Error::Forbidden);
        }
        self.metrics
            .observe(Histogram::MqttProtocolValidation, envelope.validation_us);
        self.metrics.observe(
            Histogram::ValidationToAdmission,
            envelope.validated_at.elapsed().as_micros() as u64,
        );
        let admission = match self
            .admission
            .acquire_wait(&auth.device_key, envelope.payload.len())
            .await
        {
            Ok(admission) => admission,
            Err(error) => {
                self.metrics.inc(Metric::IngressAdmissionRejects);
                return Err(error);
            }
        };
        self.metrics
            .observe(Histogram::AdmissionWait, admission.wait_us());
        self.metrics
            .observe(Histogram::AdmissionLockWait, admission.lock_wait_us());
        self.metrics
            .observe(Histogram::AdmissionLockHold, admission.lock_hold_us());
        // Stream authentication is cached on the connection before per-message admission.
        self.metrics
            .observe(Histogram::AdmissionToAuthentication, 0);
        let codec_started = Instant::now();
        let codec = self.codecs.get(auth)?;
        let messages = codec
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
        // Single-message acceptance avoids partial receipts across a non-atomic codec batch.
        if messages.len() != 1 {
            return Err(Error::Codec);
        }
        let message = messages.into_iter().next().ok_or(Error::Codec)?;
        if message.device != auth.device_key {
            return Err(Error::Forbidden);
        }
        if envelope.require_command_ack && !matches!(message.payload, DevicePayload::CommandAck(_))
        {
            return Err(Error::Invalid);
        }
        if matches!(message.payload, DevicePayload::CommandAck(_)) && !auth.permissions.commands {
            return Err(Error::Forbidden);
        }
        let command_ack = matches!(message.payload, DevicePayload::CommandAck(_));
        self.metrics.observe(
            Histogram::AuthenticationToCodec,
            codec_started.elapsed().as_micros() as u64,
        );
        self.metrics
            .add(Metric::IngressBytes, envelope.payload.len() as u64);
        let codec_complete = Instant::now();
        let canonical = canonical(&message)?;
        if canonical.len()
            > self
                .limits
                .max_http_body_size
                .max(self.limits.max_mqtt_packet_size)
                .saturating_mul(2)
        {
            return Err(Error::Codec);
        }
        let codec_to_store_us = codec_complete.elapsed().as_micros() as u64;
        let start = Instant::now();
        let acceptance = deadline(
            self.limits.external_timeout_ms,
            self.store.accept(StoredIngress { message, canonical }),
        )
        .await?;
        let persisted_at = Instant::now();
        let timings = acceptance.timings;
        self.metrics.observe(
            Histogram::CodecToPool,
            codec_to_store_us.saturating_add(timings.call_to_pool_us),
        );
        self.metrics
            .observe(Histogram::DatabasePoolWait, timings.pool_wait_us);
        self.metrics
            .observe(Histogram::TransactionStart, timings.transaction_start_us);
        self.metrics
            .observe(Histogram::QuotaWait, timings.quota_wait_us);
        self.metrics
            .observe(Histogram::QuotaAccounting, timings.quota_accounting_us);
        self.metrics
            .observe(Histogram::QuotaLockHold, timings.quota_lock_hold_us);
        self.metrics.observe(Histogram::Dedup, timings.dedup_us);
        self.metrics
            .observe(Histogram::PersistenceWrites, timings.writes_us);
        self.metrics.observe(Histogram::Commit, timings.commit_us);
        self.metrics
            .observe(Histogram::Transaction, timings.transaction_us);
        self.metrics.add(
            Metric::DatabaseLatencyMs,
            start.elapsed().as_millis() as u64,
        );
        self.sessions.touch(&auth.device_key, envelope.transport)?;
        self.metrics.inc(Metric::IngressAccepted);
        let receipt = acceptance.receipt;
        if command_ack && !receipt.duplicate {
            self.metrics.inc(Metric::CommandAcked);
        }
        if receipt.duplicate {
            self.metrics.inc(Metric::DedupHits);
        }
        Ok(IngressAcceptance {
            receipt,
            persisted_at,
        })
    }
}
