use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::{
    worker::{DeliveryError, DeliverySink, command_worker, delivery_worker},
    *,
};
use netbaiot_storage::{MemoryStore, PgStore};
use netbaiot_transports::{Services, serve_stream, udp};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, UdpSocket},
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::sync::CancellationToken;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub http: SocketAddr,
    pub mqtt: SocketAddr,
    pub tcp: SocketAddr,
    pub udp: SocketAddr,
    #[serde(default)]
    pub development: bool,
    #[serde(default)]
    pub limits: Limits,
    pub credentials: Vec<Credential>,
    pub tls: Option<TlsFiles>,
    pub delivery_url: Option<String>,
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
    let bytes = read_bounded(path, 1_048_576).await?;
    serde_json::from_slice(&bytes).map_err(|_| Error::Configuration)
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.development
            && [self.http, self.mqtt, self.tcp, self.udp]
                .iter()
                .any(|s| !s.ip().is_loopback())
        {
            return Err(Error::Configuration);
        }
        if [self.http, self.mqtt, self.tcp]
            .iter()
            .any(|s| !s.ip().is_loopback())
            && self.tls.is_none()
        {
            return Err(Error::Configuration);
        }
        if !self.development && self.delivery_url.is_none() {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
pub async fn tls_acceptor(files: &TlsFiles) -> Result<TlsAcceptor> {
    let cert = read_bounded(&files.certificate, 1_048_576).await?;
    let key = read_bounded(&files.private_key, 65536).await?;
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
impl DeliverySink for AuditSink {
    async fn deliver(&self, message: &DeviceMessage) -> std::result::Result<(), DeliveryError> {
        tracing::info!(message_id=%message.message_id.0,"development delivery observed");
        Ok(())
    }
}
struct HttpSink {
    client: reqwest::Client,
    url: reqwest::Url,
    token: Option<String>,
}
impl HttpSink {
    fn new(url: &str, limits: &Limits) -> Result<Self> {
        let url = reqwest::Url::parse(url).map_err(|_| Error::Configuration)?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(Error::Configuration);
        }
        if url.scheme() != "https"
            && !(url.scheme() == "http"
                && url.host_str().is_some_and(|h| {
                    h == "localhost"
                        || h.parse::<std::net::IpAddr>()
                            .is_ok_and(|ip| ip.is_loopback())
                }))
        {
            return Err(Error::Configuration);
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(limits.external_timeout_ms))
            .connect_timeout(Duration::from_millis(limits.external_timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(1)
            .build()
            .map_err(|_| Error::Configuration)?;
        Ok(Self {
            client,
            url,
            token: std::env::var("NETBAIOT_DELIVERY_TOKEN").ok(),
        })
    }
}
#[async_trait]
impl DeliverySink for HttpSink {
    async fn deliver(&self, message: &DeviceMessage) -> std::result::Result<(), DeliveryError> {
        let mut request = self
            .client
            .post(self.url.clone())
            .header("Idempotency-Key", message.message_id.0.to_string())
            .json(message);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|_| DeliveryError::Retryable)?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Err(DeliveryError::Retryable)
        } else {
            Err(DeliveryError::Permanent)
        }
    }
}

pub async fn run(config: Config, stop: CancellationToken) -> Result<()> {
    config.validate()?;
    let limits = Arc::new(config.limits.clone());
    let authenticator = StaticAuthenticator::new(config.credentials.clone(), &limits)?;
    let identities: HashMap<_, _> = config
        .credentials
        .iter()
        .map(|c| (c.identity.device_key.clone(), c.identity.clone()))
        .collect();
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
    let store: Arc<dyn Store> = if config.development {
        tracing::warn!("development mode: receipts are volatile and delivery is not durable");
        MemoryStore::new(limits.clone())
    } else {
        let url = std::env::var("DATABASE_URL").map_err(|_| Error::Configuration)?;
        let pg = deadline(
            limits.external_timeout_ms,
            PgStore::connect(&url, limits.clone()),
        )
        .await?;
        deadline(limits.external_timeout_ms, pg.migrate()).await?;
        deadline(
            limits.external_timeout_ms,
            pg.provision(&config.credentials),
        )
        .await?;
        pg
    };
    let metrics = Arc::new(Metrics::default());
    let sessions = Sessions::new(limits.clone());
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        authenticator,
        registry,
        store,
        metrics,
        sessions,
    ));
    let mut services = Services::new(ingress.clone());
    if let Ok(secret) = std::env::var("NETBAIOT_ADMIN_SECRET") {
        let admin = AdminAccess::new(&secret, identities.clone(), &limits)?;
        Arc::get_mut(&mut services).ok_or(Error::Internal)?.admin = Some(admin);
    }
    let sink: Arc<dyn DeliverySink> = match &config.delivery_url {
        Some(url) => Arc::new(HttpSink::new(url, &limits)?),
        None => Arc::new(AuditSink),
    };
    let tls = if let Some(files) = &config.tls {
        Some(tls_acceptor(files).await?)
    } else {
        None
    };
    // Bind every socket before starting workers, so partial startup cannot leak tasks.
    let http = TcpListener::bind(config.http)
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
    let listeners = CancellationToken::new();
    let workers = CancellationToken::new();
    let mut tasks = JoinSet::new();
    for (listener, transport) in [
        (http, Transport::Http),
        (mqtt, Transport::Mqtt),
        (tcp, Transport::Tcp),
    ] {
        tasks.spawn(serve_stream(
            listener,
            transport,
            services.clone(),
            tls.clone(),
            listeners.child_token(),
        ));
    }
    tasks.spawn(udp::serve(udp, services.clone(), listeners.child_token()));
    tasks.spawn(delivery_worker(
        ingress.clone(),
        sink,
        workers.child_token(),
    ));
    tasks.spawn(command_worker(
        services.router.clone(),
        identities,
        workers.child_token(),
    ));
    tracing::info!(http=%config.http,mqtt=%config.mqtt,tcp=%config.tcp,udp=%config.udp,"listeners ready");
    let failure = tokio::select! {_=stop.cancelled()=>None,task=tasks.join_next()=>Some(match task{Some(Ok(Err(e)))=>e,_=>Error::Internal})};
    ingress.drain();
    listeners.cancel();
    workers.cancel();
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(error=%e,"service stopped with failure"),
                Err(e) => tracing::warn!(error=%e,"service task failed"),
            }
        }
    };
    if tokio::time::timeout(Duration::from_millis(limits.shutdown_timeout_ms), drain)
        .await
        .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    tracing::info!("shutdown complete");
    if let Some(e) = failure {
        Err(e)
    } else {
        Ok(())
    }
}
