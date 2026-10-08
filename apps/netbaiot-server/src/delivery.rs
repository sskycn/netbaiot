use super::*;

/// The existing webhook schema, borrowing the accepted event for this request.
/// No second payload tree or event-lifetime JSON buffer is retained.
#[derive(Serialize)]
struct WebhookEnvelope<'a> {
    event_id: &'a EventId,
    source_message_id: &'a SourceMessageId,
    tenant_id: &'a TenantId,
    product_id: &'a ProductId,
    device_id: &'a DeviceId,
    event_type: EventType,
    received_at: Timestamp,
    occurred_at: Option<Timestamp>,
    payload: &'a DeviceEventKind,
}

impl<'a> From<&'a DeviceEvent> for WebhookEnvelope<'a> {
    fn from(event: &'a DeviceEvent) -> Self {
        Self {
            event_id: &event.event_id,
            source_message_id: &event.source_message_id,
            tenant_id: &event.device.tenant_id,
            product_id: &event.device.product_id,
            device_id: &event.device.device_id,
            event_type: event.kind.event_type(),
            received_at: event.received_at,
            occurred_at: event.occurred_at,
            payload: &event.kind,
        }
    }
}

pub(crate) struct AuditSink;
#[async_trait]
impl EventSink for AuditSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        Ok(SinkAck)
    }
}

pub(crate) struct HttpSink {
    client: reqwest::Client,
    url: reqwest::Url,
    token: Option<String>,
    maximum_response_bytes: usize,
}

impl HttpSink {
    pub(crate) fn new(url: &str, limits: &Limits) -> Result<Self> {
        let url = reqwest::Url::parse(url).map_err(|_| Error::Configuration)?;
        if !crate::diagnostics::safe_http_url(&url) {
            return Err(Error::Configuration);
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(limits.sink_timeout_ms))
            .connect_timeout(Duration::from_millis(limits.sink_timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(limits.sink_delivery_concurrency)
            .build()
            .map_err(|_| Error::Configuration)?;
        Ok(Self {
            client,
            url,
            token: std::env::var("NETBAIOT_DELIVERY_TOKEN").ok(),
            maximum_response_bytes: 4_096,
        })
    }
}

#[async_trait]
impl EventSink for HttpSink {
    async fn deliver(&self, delivery: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        let webhook = WebhookEnvelope::from(delivery.event.as_ref());
        let mut request = self
            .client
            .post(self.url.clone())
            .header("Idempotency-Key", delivery.event.event_id.0.to_string())
            .json(&webhook);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let mut response = request.send().await.map_err(|_| SinkError::Retryable)?;
        if response
            .content_length()
            .is_some_and(|length| length > self.maximum_response_bytes as u64)
        {
            return Err(SinkError::Permanent);
        }
        let mut response_bytes = 0usize;
        while let Some(chunk) = response.chunk().await.map_err(|_| SinkError::Retryable)? {
            response_bytes = response_bytes.saturating_add(chunk.len());
            if response_bytes > self.maximum_response_bytes {
                return Err(SinkError::Permanent);
            }
        }
        if response.status().is_success() {
            Ok(SinkAck)
        } else if response.status().is_server_error()
            || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            Err(SinkError::Retryable)
        } else {
            Err(SinkError::Permanent)
        }
    }
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
