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
    retry_after_limit: Duration,
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
            retry_after_limit: Duration::from_millis(limits.retry_max_ms),
        })
    }
}

impl HttpSink {
    async fn deliver_request(
        &self,
        delivery: DeliveryEnvelope,
    ) -> std::result::Result<SinkAck, SinkFailure> {
        let webhook = WebhookEnvelope::from(delivery.event.as_ref());
        let mut request = self
            .client
            .post(self.url.clone())
            .header("Idempotency-Key", delivery.event.event_id.0.to_string())
            .json(&webhook);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let mut response = request.send().await.map_err(|error| SinkFailure {
            error: SinkError::Retryable,
            reason: if error.is_timeout() {
                SinkFailureReason::Timeout
            } else if error.is_connect() {
                SinkFailureReason::Network
            } else {
                SinkFailureReason::InvalidResponse
            },
            retry_after: None,
        })?;
        let status = response.status();
        let retry_after = if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| {
                    let value = value.trim();
                    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                        return None;
                    }
                    value.parse::<u64>().ok()
                })
                .map(|seconds| Duration::from_secs(seconds).min(self.retry_after_limit))
        } else {
            None
        };
        let failure = |error, reason| SinkFailure {
            error,
            reason,
            retry_after,
        };
        if response
            .content_length()
            .is_some_and(|length| length > self.maximum_response_bytes as u64)
        {
            return Err(failure(
                SinkError::Permanent,
                SinkFailureReason::ResponseTooLarge,
            ));
        }
        let mut response_bytes = 0usize;
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            failure(
                SinkError::Retryable,
                if error.is_timeout() {
                    SinkFailureReason::Timeout
                } else {
                    SinkFailureReason::InvalidResponse
                },
            )
        })? {
            response_bytes = response_bytes.checked_add(chunk.len()).ok_or_else(|| {
                failure(SinkError::Permanent, SinkFailureReason::ResponseTooLarge)
            })?;
            if response_bytes > self.maximum_response_bytes {
                return Err(failure(
                    SinkError::Permanent,
                    SinkFailureReason::ResponseTooLarge,
                ));
            }
        }
        if status.is_success() {
            Ok(SinkAck)
        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Err(failure(SinkError::Retryable, SinkFailureReason::Http429))
        } else if status.is_server_error() {
            Err(failure(SinkError::Retryable, SinkFailureReason::Http5xx))
        } else if status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
        {
            Err(failure(SinkError::Permanent, SinkFailureReason::HttpAuth))
        } else if status.is_client_error() {
            Err(failure(SinkError::Permanent, SinkFailureReason::Http4xx))
        } else {
            Err(failure(
                SinkError::Permanent,
                SinkFailureReason::InvalidResponse,
            ))
        }
    }
}

#[async_trait]
impl EventSink for HttpSink {
    async fn deliver(&self, delivery: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        self.deliver_request(delivery)
            .await
            .map_err(|failure| failure.error)
    }
    async fn deliver_detailed(
        &self,
        delivery: DeliveryEnvelope,
    ) -> std::result::Result<SinkAck, SinkFailure> {
        self.deliver_request(delivery).await
    }
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "delivery_failure_tests.rs"]
mod failure_tests;
