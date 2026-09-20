use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{
    HttpRole, Services,
    mqtt::broker::MqttBroker,
    serve_stream,
    tcp::{LengthPrefixFramer, TcpFramer},
    udp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub device_http: SocketAddr,
    pub management_http: SocketAddr,
    pub mqtt: SocketAddr,
    pub tcp: SocketAddr,
    pub udp: SocketAddr,
    pub business_tcp: Option<SocketAddr>,
    #[serde(default)]
    pub development: bool,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub credentials: Vec<Credential>,
    pub tls: Option<TlsFiles>,
    pub delivery_url: Option<String>,
    pub auth_provider_url: Option<String>,
    pub spool_directory: PathBuf,
    #[serde(default)]
    pub device_configs: Vec<DeviceConfigSnapshot>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    pub certificate: String,
    pub private_key: String,
}

async fn read_bounded(path: &str, maximum: u64) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| Error::Configuration)?;
    let mut bytes = Vec::new();
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| Error::Configuration)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::Configuration);
    }
    Ok(bytes)
}

pub async fn read_config(path: &str) -> Result<Config> {
    serde_json::from_slice(&read_bounded(path, 16_777_216).await?).map_err(|_| Error::Configuration)
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        let listeners = [
            self.device_http,
            self.management_http,
            self.mqtt,
            self.tcp,
            self.udp,
        ];
        if self.development && listeners.iter().any(|address| !address.ip().is_loopback()) {
            return Err(Error::Configuration);
        }
        if [self.device_http, self.mqtt, self.tcp]
            .iter()
            .any(|address| !address.ip().is_loopback())
            && self.tls.is_none()
        {
            return Err(Error::Configuration);
        }
        if !self.management_http.ip().is_loopback()
            && std::env::var("NETBAIOT_ADMIN_SECRET").is_err()
        {
            return Err(Error::Configuration);
        }
        // The framed business stream currently authenticates with a bearer secret. Keep it
        // node-local until transport TLS is implemented for this optional integration.
        if self
            .business_tcp
            .is_some_and(|address| !address.ip().is_loopback())
        {
            return Err(Error::Configuration);
        }
        if self.credentials.is_empty() && self.auth_provider_url.is_none() {
            return Err(Error::Configuration);
        }
        if !self.development && self.delivery_url.is_none() && self.business_tcp.is_none() {
            return Err(Error::Configuration);
        }
        if self.spool_directory.as_os_str().is_empty() {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

pub async fn tls_acceptor(files: &TlsFiles) -> Result<TlsAcceptor> {
    let cert = read_bounded(&files.certificate, 1_048_576).await?;
    let key = read_bounded(&files.private_key, 65_536).await?;
    let certificates = rustls_pemfile::certs(&mut cert.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::Configuration)?;
    let private = rustls_pemfile::private_key(&mut key.as_slice())
        .map_err(|_| Error::Configuration)?
        .ok_or(Error::Configuration)?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| Error::Configuration)?
    .with_no_client_auth()
    .with_single_cert(certificates, private)
    .map_err(|_| Error::Configuration)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

struct AuditSink;
#[async_trait]
impl EventSink for AuditSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        Ok(SinkAck)
    }
}

struct HttpSink {
    client: reqwest::Client,
    url: reqwest::Url,
    token: Option<String>,
    maximum_response_bytes: usize,
}

impl HttpSink {
    fn new(url: &str, limits: &Limits) -> Result<Self> {
        let url = reqwest::Url::parse(url).map_err(|_| Error::Configuration)?;
        if !url.username().is_empty()
            || url.password().is_some()
            || (url.scheme() != "https"
                && !(url.scheme() == "http"
                    && url.host_str().is_some_and(|host| {
                        host == "localhost"
                            || host
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())
                    })))
        {
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

struct StreamRequest {
    delivery: DeliveryEnvelope,
    result: oneshot::Sender<std::result::Result<SinkAck, SinkError>>,
}

struct TcpStreamSink {
    active: Mutex<Option<mpsc::Sender<StreamRequest>>>,
}

impl TcpStreamSink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            active: Mutex::new(None),
        })
    }
}

#[async_trait]
impl EventSink for TcpStreamSink {
    async fn deliver(&self, delivery: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        let sender = self
            .active
            .lock()
            .map_err(|_| SinkError::Permanent)?
            .clone()
            .ok_or(SinkError::Retryable)?;
        let (result, receive) = oneshot::channel();
        sender
            .try_send(StreamRequest { delivery, result })
            .map_err(|_| SinkError::Retryable)?;
        receive.await.map_err(|_| SinkError::Retryable)?
    }
}

async fn read_frame(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    timeout_ms: u64,
) -> Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let mut length = [0u8; 4];
        stream
            .read_exact(&mut length)
            .await
            .map_err(|_| Error::Unavailable)?;
        let length = usize::try_from(u32::from_be_bytes(length)).map_err(|_| Error::Invalid)?;
        if length == 0 || length > framer.maximum {
            return Err(Error::Invalid);
        }
        let mut payload = vec![0; length];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(payload)
    })
    .await
    .map_err(|_| Error::Timeout)?
}

async fn write_frame(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    frame: &StreamServerFrame,
    timeout_ms: u64,
) -> Result<()> {
    let payload = serde_json::to_vec(frame).map_err(|_| Error::Internal)?;
    let wire = framer.encode(&payload)?;
    tokio::time::timeout(Duration::from_millis(timeout_ms), stream.write_all(&wire))
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Unavailable)
}

async fn write_stream_error(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    limits: &Limits,
    code: ErrorCode,
    message: &str,
) {
    let _ = write_frame(
        stream,
        framer,
        &StreamServerFrame::Error {
            version: PROTOCOL_VERSION,
            error: ApiError {
                code,
                message: message.to_owned(),
                request_id: Some(EventId::generate().to_string()),
                required_scope: None,
            },
        },
        limits.write_timeout_ms,
    )
    .await;
}

async fn business_handshake(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    token_hash: &[u8; 32],
    limits: &Limits,
) -> Result<(SubscriptionId, EventFilter)> {
    let payload = read_frame(stream, framer, limits.connect_timeout_ms).await?;
    let hello = match serde_json::from_slice::<StreamClientFrame>(&payload) {
        Ok(frame) => frame,
        Err(_) => {
            write_stream_error(
                stream,
                framer,
                limits,
                ErrorCode::InvalidRequest,
                "invalid business stream hello",
            )
            .await;
            return Err(Error::Invalid);
        }
    };
    let StreamClientFrame::Hello { version, token } = hello else {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidRequest,
            "hello must be the first business stream frame",
        )
        .await;
        return Err(Error::Invalid);
    };
    if version != PROTOCOL_VERSION {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidProtocolVersion,
            "unsupported business stream protocol version",
        )
        .await;
        return Err(Error::Invalid);
    }
    if !bool::from(
        Sha256::digest(token.as_bytes())
            .as_slice()
            .ct_eq(token_hash),
    ) {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::Unauthenticated,
            "business stream authentication failed",
        )
        .await;
        return Err(Error::Authentication);
    }

    let payload = read_frame(stream, framer, limits.connect_timeout_ms).await?;
    let subscribe = match serde_json::from_slice::<StreamClientFrame>(&payload) {
        Ok(frame) => frame,
        Err(_) => {
            write_stream_error(
                stream,
                framer,
                limits,
                ErrorCode::InvalidRequest,
                "invalid business stream subscription",
            )
            .await;
            return Err(Error::Invalid);
        }
    };
    let StreamClientFrame::Subscribe {
        version,
        subscription_id,
        filter,
    } = subscribe
    else {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidRequest,
            "subscribe must follow the business stream hello",
        )
        .await;
        return Err(Error::Invalid);
    };
    if version != PROTOCOL_VERSION {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidProtocolVersion,
            "unsupported business stream protocol version",
        )
        .await;
        return Err(Error::Invalid);
    }
    if filter.validate().is_err() {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidRequest,
            "business stream filter exceeds protocol bounds",
        )
        .await;
        return Err(Error::Invalid);
    }
    Ok((subscription_id, filter))
}

async fn serve_business_stream(
    listener: TcpListener,
    sink: Arc<TcpStreamSink>,
    token_hash: [u8; 32],
    limits: Arc<Limits>,
    stop: CancellationToken,
) -> Result<()> {
    loop {
        let (mut stream, _) = tokio::select! {
            _ = stop.cancelled() => break,
            accepted = listener.accept() => accepted.map_err(|_| Error::Unavailable)?,
        };
        let framer = LengthPrefixFramer {
            maximum: limits.max_tcp_frame_size,
        };
        let Ok((subscription_id, filter)) =
            business_handshake(&mut stream, &framer, &token_hash, &limits).await
        else {
            continue;
        };
        let (sender, mut receiver) = mpsc::channel(limits.sink_delivery_concurrency);
        *sink.active.lock().map_err(|_| Error::Internal)? = Some(sender);
        if write_frame(
            &mut stream,
            &framer,
            &StreamServerFrame::Ready {
                version: PROTOCOL_VERSION,
                subscription_id,
            },
            limits.write_timeout_ms,
        )
        .await
        .is_err()
        {
            *sink.active.lock().map_err(|_| Error::Internal)? = None;
            continue;
        }
        while let Some(request) = tokio::select! {
            _ = stop.cancelled() => None,
            request = receiver.recv() => request,
        } {
            if !filter.matches(&request.delivery.event) {
                let _ = request.result.send(Ok(SinkAck));
                continue;
            }
            let delivery_id = DeliveryId::generate();
            let event_id = request.delivery.event.event_id;
            let frame = StreamServerFrame::Event {
                version: PROTOCOL_VERSION,
                delivery: EventDelivery {
                    delivery_id,
                    subscription_id,
                    event: (*request.delivery.event).clone(),
                    attempt: request.delivery.attempt,
                },
            };
            let delivered = tokio::select! {
                _ = stop.cancelled() => Err(Error::Draining),
                result = async {
                    write_frame(&mut stream, &framer, &frame, limits.write_timeout_ms).await?;
                    let ack: StreamClientFrame =
                        serde_json::from_slice(&read_frame(
                            &mut stream,
                            &framer,
                            limits.sink_timeout_ms,
                        ).await?)
                            .map_err(|_| Error::Invalid)?;
                    match ack {
                        StreamClientFrame::Ack { version, ack }
                            if version == PROTOCOL_VERSION
                                && ack.delivery_id == delivery_id
                                && ack.subscription_id == subscription_id
                                && ack.event_id == event_id =>
                        {
                            Ok(SinkAck)
                        }
                        _ => Err(Error::Invalid),
                    }
                } => result,
            };
            let failed = delivered.is_err();
            let _ = request.result.send(delivered.map_err(|error| {
                if matches!(error, Error::Invalid) {
                    SinkError::Permanent
                } else {
                    SinkError::Retryable
                }
            }));
            if failed {
                break;
            }
        }
        *sink.active.lock().map_err(|_| Error::Internal)? = None;
    }
    Ok(())
}

struct HttpAuthProvider {
    client: reqwest::Client,
    url: reqwest::Url,
    slots: Arc<tokio::sync::Semaphore>,
}

impl HttpAuthProvider {
    fn new(url: &str, limits: &Limits) -> Result<Arc<Self>> {
        let url = reqwest::Url::parse(url).map_err(|_| Error::Configuration)?;
        if url.scheme() != "https"
            && !(url.scheme() == "http"
                && url.host_str().is_some_and(|host| {
                    host == "localhost"
                        || host
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|address| address.is_loopback())
                }))
        {
            return Err(Error::Configuration);
        }
        Ok(Arc::new(Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_millis(limits.authentication_timeout_ms))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| Error::Configuration)?,
            url,
            slots: Arc::new(tokio::sync::Semaphore::new(limits.max_ingress)),
        }))
    }
}

#[async_trait]
impl DeviceAuthenticator for HttpAuthProvider {
    async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let value = match request {
            AuthenticationRequest::Secret {
                credential_id,
                secret,
            } => serde_json::json!({
                "kind": "secret", "credential_id": credential_id, "secret_hex": encode_hex(secret),
            }),
            AuthenticationRequest::Signed {
                credential_id,
                message,
                tag,
            } => serde_json::json!({
                "kind": "signed", "credential_id": credential_id,
                "message_hex": encode_hex(message), "tag_hex": encode_hex(tag),
            }),
        };
        let response = self
            .client
            .post(self.url.clone())
            .json(&value)
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::FORBIDDEN
        {
            return Err(Error::Authentication);
        }
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|length| length > 16_384)
        {
            return Err(Error::Invalid);
        }
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
            let length = bytes.len().checked_add(chunk.len()).ok_or(Error::Invalid)?;
            if length > 16_384 {
                return Err(Error::Invalid);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)
    }
}

fn bootstrap_snapshot(config: &Config, sink_id: SinkId) -> Result<ControlSnapshot> {
    let mut products = HashMap::new();
    for credential in &config.credentials {
        let identity = &credential.identity;
        products
            .entry((
                identity.device_key.tenant_id.clone(),
                identity.device_key.product_id.clone(),
            ))
            .or_insert(ProductRuntimeConfig {
                tenant_id: identity.device_key.tenant_id.clone(),
                product_id: identity.device_key.product_id.clone(),
                codec_id: identity.codec_id.clone(),
                codec_version: identity.codec_version,
                revision: 1,
            });
    }
    Ok(ControlSnapshot {
        revision: 1,
        products: products.into_values().collect(),
        devices: config.device_configs.clone(),
        routes: vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink_id],
        }],
    })
}

pub async fn run(config: Config, stop: CancellationToken) -> Result<()> {
    run_with_credentials(
        config,
        stop,
        std::env::var("NETBAIOT_ADMIN_SECRET").ok(),
        std::env::var("NETBAIOT_BUSINESS_STREAM_TOKEN").ok(),
    )
    .await
}

/// Composition entry point for embedded/test hosts that inject secrets without
/// mutating process-global environment state.
pub async fn run_with_credentials(
    config: Config,
    stop: CancellationToken,
    admin_secret: Option<String>,
    business_stream_token: Option<String>,
) -> Result<()> {
    config.validate()?;
    let limits = Arc::new(config.limits.clone());
    let metrics = Arc::new(Metrics::default());
    let lifecycle = Arc::new(Lifecycle::starting());
    let identities: HashMap<_, _> = config
        .credentials
        .iter()
        .map(|credential| {
            (
                credential.identity.device_key.clone(),
                credential.identity.clone(),
            )
        })
        .collect();
    let provider: Arc<dyn DeviceAuthenticator> = if let Some(url) = &config.auth_provider_url {
        HttpAuthProvider::new(url, &limits)?
    } else {
        StaticAuthenticator::new(config.credentials.clone(), &limits)?
    };
    let auth_cache = AuthCache::new(provider, limits.clone(), metrics.clone());
    let codec_limits = CodecLimits {
        input_bytes: limits
            .max_http_body_size
            .max(limits.max_mqtt_packet_size)
            .max(limits.max_tcp_frame_size),
        decoded_bytes: limits
            .max_http_body_size
            .max(limits.max_mqtt_packet_size)
            .max(limits.max_tcp_frame_size),
        ..CodecLimits::default()
    };
    let registry = CodecRegistry::new(vec![(
        CodecId::new("netbaiot-json").map_err(|_| Error::Configuration)?,
        1,
        Arc::new(JsonV1::new(codec_limits)),
    )])?;
    for auth in identities.values() {
        registry.get(auth)?;
    }

    let (sink_id, mut sink_definition, tcp_sink) = if let Some(address) = config.business_tcp {
        let sink = TcpStreamSink::new();
        let id = SinkId::new("tcp-rpc").map_err(|_| Error::Configuration)?;
        let mut definition = SinkDefinition::bounded(
            id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            sink.clone(),
            &limits,
        );
        definition.concurrency = 1;
        (id, definition, Some((address, sink)))
    } else if let Some(url) = &config.delivery_url {
        let id = SinkId::new("webhook").map_err(|_| Error::Configuration)?;
        let sink = Arc::new(HttpSink::new(url, &limits)?);
        (
            id.clone(),
            SinkDefinition::bounded(id, SinkDeliveryMode::ConfirmedRequired, sink, &limits),
            None,
        )
    } else {
        let id = SinkId::new("development-audit").map_err(|_| Error::Configuration)?;
        (
            id.clone(),
            SinkDefinition::bounded(
                id,
                SinkDeliveryMode::ConfirmedRequired,
                Arc::new(AuditSink),
                &limits,
            ),
            None,
        )
    };
    if tcp_sink.is_some() {
        sink_definition.timeout = Duration::from_millis(limits.sink_timeout_ms);
    }
    let snapshot = bootstrap_snapshot(&config, sink_id)?;
    let config_cache = ConfigCache::empty(limits.clone());
    config_cache.apply(snapshot.clone())?;
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![sink_definition],
        snapshot.routes,
        snapshot.revision,
    )?;
    let spool = RestartSpool::new(config.spool_directory.clone(), limits.clone());
    let mqtt_broker = MqttBroker::new(limits.clone());
    mqtt_broker.recover_from(&config.spool_directory).await?;
    let recovery = spool.recover().await?;
    let recovered_files = recovery.committed_files;
    let recovered_count = events.restore(recovery.records)?;
    metrics.add(Metric::RecoveryRecords, recovered_count as u64);
    let sessions = Sessions::new(limits.clone());
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        auth_cache,
        registry,
        events.clone(),
        config_cache,
        metrics.clone(),
        sessions,
        lifecycle.clone(),
    ));
    let shutdown = stop.child_token();
    let mut base_services =
        Services::new_with_mqtt(ingress.clone(), shutdown.clone(), mqtt_broker.clone());
    if let Some(secret) = admin_secret {
        let admin = Arc::new(AdminAccess::new(&secret, identities, &limits)?);
        Arc::get_mut(&mut base_services)
            .ok_or(Error::Internal)?
            .admin = Some(admin);
    }
    let device_services = base_services.with_http_role(HttpRole::Device);
    let management_services = base_services.with_http_role(HttpRole::Management);
    let tls = if let Some(files) = &config.tls {
        Some(tls_acceptor(files).await?)
    } else {
        None
    };

    let device_http = TcpListener::bind(config.device_http)
        .await
        .map_err(|_| Error::Unavailable)?;
    let management_http = TcpListener::bind(config.management_http)
        .await
        .map_err(|_| Error::Unavailable)?;
    let mqtt = TcpListener::bind(config.mqtt)
        .await
        .map_err(|_| Error::Unavailable)?;
    let tcp = TcpListener::bind(config.tcp)
        .await
        .map_err(|_| Error::Unavailable)?;
    let udp = UdpSocket::bind(config.udp)
        .await
        .map_err(|_| Error::Unavailable)?;
    let business = if let Some((address, sink)) = tcp_sink {
        Some((
            TcpListener::bind(address)
                .await
                .map_err(|_| Error::Unavailable)?,
            sink,
        ))
    } else {
        None
    };

    lifecycle.mark_running()?;
    let listeners = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(serve_stream(
        device_http,
        Transport::Http,
        device_services,
        tls.clone(),
        listeners.child_token(),
    ));
    tasks.spawn(serve_stream(
        management_http,
        Transport::Http,
        management_services,
        tls.clone(),
        listeners.child_token(),
    ));
    tasks.spawn(serve_stream(
        mqtt,
        Transport::Mqtt,
        base_services.clone(),
        tls.clone(),
        listeners.child_token(),
    ));
    tasks.spawn(serve_stream(
        tcp,
        Transport::Tcp,
        base_services.clone(),
        tls,
        listeners.child_token(),
    ));
    tasks.spawn(udp::serve(udp, base_services, listeners.child_token()));
    if let Some((listener, sink)) = business {
        let secret = business_stream_token.ok_or(Error::Configuration)?;
        let hash: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
        tasks.spawn(serve_business_stream(
            listener,
            sink,
            hash,
            limits.clone(),
            listeners.child_token(),
        ));
    }
    tracing::info!(device_http=%config.device_http,management_http=%config.management_http,mqtt=%config.mqtt,tcp=%config.tcp,udp=%config.udp,"runtime ready");
    let failure = tokio::select! {
        _ = shutdown.cancelled() => None,
        task = tasks.join_next() => Some(match task { Some(Ok(Err(error))) => error, _ => Error::Internal }),
    };
    lifecycle.begin_quiesce().await?;
    events.close_admission()?;
    listeners.cancel();
    while tasks.join_next().await.is_some() {}

    // All network owners have detached. Snapshot MQTT protocol state as one versioned,
    // fsynced image before claiming a successful planned shutdown.
    mqtt_broker.commit_to(&config.spool_directory).await?;

    let drained = events
        .wait_required_drained(Duration::from_millis(limits.shutdown_drain_timeout_ms))
        .await;
    if drained {
        events.stop_workers().await?;
        if !recovered_files.is_empty() {
            spool.remove_committed(recovered_files).await?;
        }
    } else {
        events.stop_workers().await?;
        lifecycle.mark_spooling()?;
        let pending = events.spool_records()?;
        let encoded_bytes = pending.iter().try_fold(0usize, |total, record| {
            total
                .checked_add(
                    serde_json::to_vec(record)
                        .map_err(|_| Error::Internal)?
                        .len(),
                )
                .ok_or(Error::Overloaded)
        })?;
        spool.commit(pending.clone()).await?;
        metrics.add(Metric::SpoolRecords, pending.len() as u64);
        metrics.add(Metric::SpoolBytes, encoded_bytes as u64);
    }
    lifecycle.mark_drained()?;
    tracing::info!("shutdown complete");
    failure.map_or(Ok(()), Err)
}
