use super::*;

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
        let webhook = serde_json::json!({
            "event_id": delivery.event.event_id,
            "source_message_id": delivery.event.source_message_id,
            "tenant_id": delivery.event.device.tenant_id,
            "product_id": delivery.event.device.product_id,
            "device_id": delivery.event.device.device_id,
            "event_type": delivery.event.kind.event_type(),
            "received_at": delivery.event.received_at,
            "occurred_at": delivery.event.occurred_at,
            "payload": delivery.event.kind,
        });
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
