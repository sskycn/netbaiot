mod business_stream_v1;
use async_trait::async_trait;
#[cfg(test)]
use business_stream_v1::TcpStreamSink;
use business_stream_v1::{serve_business_connection, serve_business_stream};
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{
    Services,
    business_rpc::{
        self, BusinessIdentity, BusinessPrincipal, BusinessRpcServices, BusinessRpcTransportConfig,
        V3SendAhead,
    },
    mqtt::broker::MqttBroker,
    serve_device_ingress, serve_management_http,
    tcp::{LengthPrefixFramer, TcpFramer},
    udp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::mpsc,
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub device_ingress: SocketAddr,
    pub management_http: SocketAddr,
    pub business_tcp: Option<SocketAddr>,
    #[serde(default)]
    pub business_rpc: Option<BusinessRpcConfig>,
    #[serde(default)]
    pub device_auth: Option<DeviceAuthSource>,
    #[serde(default)]
    pub event_delivery: Option<EventDeliverySource>,
    #[serde(default)]
    pub development: bool,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub credentials: Vec<Credential>,
    pub tls: Option<TlsFiles>,
    #[serde(default)]
    pub management_tls: Option<ManagementTlsFiles>,
    #[serde(default)]
    pub management_auth: ManagementAuthConfig,
    pub delivery_url: Option<String>,
    pub auth_provider_url: Option<String>,
    pub spool_directory: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    pub certificate: String,
    pub private_key: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementTlsFiles {
    pub certificate: String,
    pub private_key: String,
    pub client_ca: Option<String>,
    #[serde(default)]
    pub require_client_certificate: bool,
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceAuthSource {
    Static,
    Http,
    BusinessRpc,
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventDeliverySource {
    Http,
    BusinessRpc,
    DevelopmentAudit,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessRpcConfig {
    pub version: u16,
    /// V3 is available on the same listener only when explicitly configured.
    #[serde(default)]
    pub v3: Option<business_rpc_v3::V3Limits>,
    /// Local sender policy, separate from the negotiated V3 receive windows.
    #[serde(default)]
    pub v3_send_ahead: Option<V3SendAhead>,
    #[serde(default)]
    pub v3_experiment_socket_send_buffer_bytes: Option<usize>,
    pub tls: Option<ManagementTlsFiles>,
    #[serde(default)]
    pub identities: Vec<BusinessRpcIdentityConfig>,
    pub development_token_env: Option<String>,
    #[serde(default)]
    pub development_role: Option<BusinessRole>,
    #[serde(default)]
    pub allow_v1: bool,
    #[serde(default = "default_business_connections")]
    pub max_connections: usize,
    #[serde(default = "default_business_auth_inflight")]
    pub auth_max_inflight: usize,
    #[serde(default = "default_business_offline_ms")]
    pub max_auth_control_offline_ms: u64,
}
fn default_business_connections() -> usize {
    8
}
fn default_business_auth_inflight() -> usize {
    128
}
fn default_business_offline_ms() -> u64 {
    30_000
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessRpcIdentityConfig {
    pub certificate_sha256: String,
    pub principal_id: String,
    pub role: BusinessRole,
    pub provider_id: Option<String>,
    pub sink_id: Option<String>,
    pub provide_methods: Vec<String>,
    pub call_methods: Vec<String>,
    #[serde(default)]
    pub global: bool,
    #[serde(default)]
    pub tenants: Vec<TenantId>,
    pub expires_at_ms: Option<i64>,
}

fn parse_hex_32(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        return Err(Error::Configuration);
    }
    let mut bytes = [0u8; 32];
    for (index, pair) in text.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = (pair[0] as char).to_digit(16).ok_or(Error::Configuration)? as u8;
        let low = (pair[1] as char).to_digit(16).ok_or(Error::Configuration)? as u8;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
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
        let listeners = [self.device_ingress, self.management_http];
        if self.development && listeners.iter().any(|address| !address.ip().is_loopback()) {
            return Err(Error::Configuration);
        }
        if !self.device_ingress.ip().is_loopback() && self.tls.is_none() {
            return Err(Error::Configuration);
        }
        if !self.management_http.ip().is_loopback()
            && self.tls.is_none()
            && self.management_tls.is_none()
        {
            return Err(Error::Configuration);
        }
        if self
            .management_tls
            .as_ref()
            .is_some_and(|tls| tls.require_client_certificate && tls.client_ca.is_none())
        {
            return Err(Error::Configuration);
        }
        if self
            .management_tls
            .as_ref()
            .is_some_and(|tls| tls.require_client_certificate)
            && self.management_auth.mtls_identities.is_empty()
        {
            return Err(Error::Configuration);
        }
        if !self.management_auth.mtls_identities.is_empty()
            && !self
                .management_tls
                .as_ref()
                .is_some_and(|tls| tls.require_client_certificate)
        {
            return Err(Error::Configuration);
        }
        if let Some(rpc) = &self.business_rpc {
            if rpc.version != BUSINESS_RPC_VERSION
                || rpc.v3.as_ref().is_some_and(|v3| v3.validate().is_err())
                || rpc.v3_send_ahead.is_some_and(|policy| {
                    rpc.v3
                        .as_ref()
                        .is_none_or(|limits| policy.as_mux().validate(limits).is_err())
                })
                || rpc
                    .v3_experiment_socket_send_buffer_bytes
                    .is_some_and(|size| {
                        rpc.v3.is_none() || !(4096..=4 * 1024 * 1024).contains(&size)
                    })
                || self.business_tcp.is_none()
                || rpc.max_connections == 0
                || rpc.auth_max_inflight == 0
                || rpc.auth_max_inflight > 1024
                || rpc.max_connections > 1024
                || rpc.max_auth_control_offline_ms > 86_400_000
            {
                return Err(Error::Configuration);
            }
            if rpc.tls.is_some() {
                if rpc.development_role.is_some() {
                    return Err(Error::Configuration);
                }
                if rpc.development_token_env.is_some()
                    || rpc.allow_v1
                    || rpc.identities.is_empty()
                    || rpc.tls.as_ref().is_none_or(|tls| {
                        !tls.require_client_certificate || tls.client_ca.is_none()
                    })
                {
                    return Err(Error::Configuration);
                }
            } else if self
                .business_tcp
                .is_none_or(|addr| !addr.ip().is_loopback())
                || rpc.development_token_env.is_none()
                || !rpc.identities.is_empty()
                || (self.device_auth == Some(DeviceAuthSource::BusinessRpc)
                    && !rpc
                        .development_role
                        .unwrap_or(BusinessRole::Multiplexed)
                        .auth_control())
            {
                return Err(Error::Configuration);
            }
            if self.development
                && self
                    .business_tcp
                    .is_some_and(|addr| !addr.ip().is_loopback())
            {
                return Err(Error::Configuration);
            }
            if rpc.tls.is_some() {
                let mut fingerprints = std::collections::HashSet::new();
                for identity in &rpc.identities {
                    let fingerprint = parse_hex_32(&identity.certificate_sha256)?;
                    if !fingerprints.insert(fingerprint)
                        || identity.principal_id.is_empty()
                        || identity.principal_id.len() > 64
                        || identity
                            .expires_at_ms
                            .is_some_and(|expiry| expiry <= now_ms())
                        || identity.global != identity.tenants.is_empty()
                        || identity.tenants.len() > 64
                        || identity.provide_methods.len() > 2
                        || identity.call_methods.len() > 2
                        || identity.provide_methods.iter().any(|method| {
                            !matches!(
                                method.as_str(),
                                "device.authenticate" | "device.resolve_verifier"
                            )
                        })
                        || identity.call_methods.iter().any(|method| {
                            !matches!(
                                method.as_str(),
                                "auth.sync" | "auth.invalidate" | "device.command.send"
                            )
                        })
                        || (identity.role.auth_control()
                            && (identity.provider_id.as_deref() != Some("primary")
                                || !identity
                                    .provide_methods
                                    .iter()
                                    .any(|method| method == "device.authenticate")
                                || !identity
                                    .provide_methods
                                    .iter()
                                    .any(|method| method == "device.resolve_verifier")
                                || !identity
                                    .call_methods
                                    .iter()
                                    .any(|method| method == "auth.sync")
                                || !identity
                                    .call_methods
                                    .iter()
                                    .any(|method| method == "auth.invalidate")))
                        || (!identity.role.auth_control()
                            && (identity.provider_id.is_some()
                                || !identity.provide_methods.is_empty()
                                || identity
                                    .call_methods
                                    .iter()
                                    .any(|method| method != "device.command.send")))
                        || (identity.role.commands()
                            && !identity
                                .call_methods
                                .iter()
                                .any(|method| method == "device.command.send"))
                        || (!identity.role.commands()
                            && identity
                                .call_methods
                                .iter()
                                .any(|method| method == "device.command.send"))
                        || (identity.role.auth_control()
                            && identity
                                .call_methods
                                .iter()
                                .any(|method| method == "device.command.send"))
                        || (identity.role.events()
                            && identity.sink_id.as_deref() != Some("tcp-rpc"))
                        || (!identity.role.events() && identity.sink_id.is_some())
                    {
                        return Err(Error::Configuration);
                    }
                }
            } else if rpc.development_token_env.as_deref().is_none_or(|name| {
                name.is_empty()
                    || name.len() > 64
                    || name == "NETBAIOT_ADMIN_SECRET"
                    || name == "NETBAIOT_BUSINESS_STREAM_TOKEN"
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
            }) {
                return Err(Error::Configuration);
            }
            if self.auth_provider_url.is_some() && self.device_auth != Some(DeviceAuthSource::Http)
            {
                return Err(Error::Configuration);
            }
            if self.delivery_url.is_some() && self.event_delivery != Some(EventDeliverySource::Http)
            {
                return Err(Error::Configuration);
            }
            if self.device_auth.is_none() && self.auth_provider_url.is_some() {
                return Err(Error::Configuration);
            }
            if self.event_delivery.is_none() && self.delivery_url.is_some() {
                return Err(Error::Configuration);
            }
        } else {
            if self
                .business_tcp
                .is_some_and(|addr| !addr.ip().is_loopback())
            {
                return Err(Error::Configuration);
            }
            if self.device_auth.is_some() || self.event_delivery.is_some() {
                return Err(Error::Configuration);
            }
        }
        let auth = self
            .device_auth
            .unwrap_or(if self.auth_provider_url.is_some() {
                DeviceAuthSource::Http
            } else {
                DeviceAuthSource::Static
            });
        if auth == DeviceAuthSource::Static && self.credentials.is_empty() {
            return Err(Error::Configuration);
        }
        if auth == DeviceAuthSource::Http && self.auth_provider_url.is_none() {
            return Err(Error::Configuration);
        }
        if auth == DeviceAuthSource::BusinessRpc && self.business_rpc.is_none() {
            return Err(Error::Configuration);
        }
        let delivery = self
            .event_delivery
            .unwrap_or(if self.business_tcp.is_some() {
                EventDeliverySource::BusinessRpc
            } else if self.delivery_url.is_some() {
                EventDeliverySource::Http
            } else {
                EventDeliverySource::DevelopmentAudit
            });
        if delivery == EventDeliverySource::BusinessRpc && self.business_tcp.is_none() {
            return Err(Error::Configuration);
        }
        if delivery == EventDeliverySource::Http && self.delivery_url.is_none() {
            return Err(Error::Configuration);
        }
        if !self.development && delivery == EventDeliverySource::DevelopmentAudit {
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

pub async fn management_tls_acceptor(files: &ManagementTlsFiles) -> Result<TlsAcceptor> {
    let cert = read_bounded(&files.certificate, 1_048_576).await?;
    let key = read_bounded(&files.private_key, 65_536).await?;
    let certificates = rustls_pemfile::certs(&mut cert.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::Configuration)?;
    let private = rustls_pemfile::private_key(&mut key.as_slice())
        .map_err(|_| Error::Configuration)?
        .ok_or(Error::Configuration)?;
    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| Error::Configuration)?;
    let builder = if files.require_client_certificate {
        let ca = read_bounded(
            files.client_ca.as_deref().ok_or(Error::Configuration)?,
            1_048_576,
        )
        .await?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca.as_slice()) {
            roots
                .add(cert.map_err(|_| Error::Configuration)?)
                .map_err(|_| Error::Configuration)?;
        }
        if roots.is_empty() {
            return Err(Error::Configuration);
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(|_| Error::Configuration)?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let config = builder
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

struct HttpAuthProvider {
    client: reqwest::Client,
    url: reqwest::Url,
    slots: Arc<tokio::sync::Semaphore>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifierResponse {
    identity: AuthenticatedDevice,
    verifier_key_hex: String,
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

    async fn resolve_verifier(&self, credential_id: &str) -> Result<DeviceVerifier> {
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let response = self
            .client
            .post(self.url.clone())
            .json(&serde_json::json!({
                "kind": "verifier", "credential_id": credential_id,
            }))
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            return Err(Error::Authentication);
        }
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > 16_384)
        {
            return Err(Error::Unavailable);
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
        let response: VerifierResponse =
            serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        let key: [u8; 32] = decode_hex(&response.verifier_key_hex)?
            .try_into()
            .map_err(|_| Error::Invalid)?;
        Ok(DeviceVerifier::new(response.identity, key))
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
    let metrics = Arc::new(
        if matches!(
            std::env::var("NETBAIOT_PERF_LOCK_METRICS").as_deref(),
            Ok("1")
        ) {
            Metrics::with_lock_timing()
        } else {
            Metrics::default()
        },
    );
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
    let rpc_registry = if let Some(rpc) = &config.business_rpc {
        Some(BusinessRpcRegistry::new_with_metrics(
            rpc.auth_max_inflight,
            rpc.auth_max_inflight
                .checked_mul(32 * 1024)
                .ok_or(Error::Configuration)?,
            Duration::from_millis(limits.authentication_timeout_ms),
            metrics.clone(),
        )?)
    } else {
        None
    };
    let auth_source = config
        .device_auth
        .unwrap_or(if config.auth_provider_url.is_some() {
            DeviceAuthSource::Http
        } else {
            DeviceAuthSource::Static
        });
    let provider: Arc<dyn DeviceAuthenticator> = match auth_source {
        DeviceAuthSource::Static => StaticAuthenticator::new(config.credentials.clone(), &limits)?,
        DeviceAuthSource::Http => HttpAuthProvider::new(
            config
                .auth_provider_url
                .as_deref()
                .ok_or(Error::Configuration)?,
            &limits,
        )?,
        DeviceAuthSource::BusinessRpc => {
            BusinessRpcAuthProvider::new(rpc_registry.as_ref().ok_or(Error::Configuration)?.clone())
        }
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

    let delivery_source = config
        .event_delivery
        .unwrap_or(if config.business_tcp.is_some() {
            EventDeliverySource::BusinessRpc
        } else if config.delivery_url.is_some() {
            EventDeliverySource::Http
        } else {
            EventDeliverySource::DevelopmentAudit
        });
    let business_sink = config.business_tcp.map(|_| BusinessRpcEventSink::new());
    let (sink_id, mut sink_definition) = match delivery_source {
        EventDeliverySource::BusinessRpc => {
            let id = SinkId::new("tcp-rpc").map_err(|_| Error::Configuration)?;
            let sink = business_sink.as_ref().ok_or(Error::Configuration)?.clone();
            let mut definition = SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink,
                &limits,
            );
            definition.concurrency = 1;
            (id, definition)
        }
        EventDeliverySource::Http => {
            let id = SinkId::new("webhook").map_err(|_| Error::Configuration)?;
            let sink = Arc::new(HttpSink::new(
                config.delivery_url.as_deref().ok_or(Error::Configuration)?,
                &limits,
            )?);
            (
                id.clone(),
                SinkDefinition::bounded(id, SinkDeliveryMode::ConfirmedRequired, sink, &limits),
            )
        }
        EventDeliverySource::DevelopmentAudit => {
            let id = SinkId::new("development-audit").map_err(|_| Error::Configuration)?;
            (
                id.clone(),
                SinkDefinition::bounded(
                    id,
                    SinkDeliveryMode::ConfirmedRequired,
                    Arc::new(AuditSink),
                    &limits,
                ),
            )
        }
    };
    if delivery_source == EventDeliverySource::BusinessRpc {
        sink_definition.timeout = Duration::from_millis(limits.sink_timeout_ms);
    }
    let snapshot = bootstrap_snapshot(&config, sink_id)?;
    let control = GatewayControl::empty(limits.clone());
    control.apply(snapshot.clone())?;
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![sink_definition],
        snapshot.routes,
        snapshot.revision,
    )?;
    let spool = RestartSpool::new(config.spool_directory.clone(), limits.clone());
    let mqtt_broker = MqttBroker::new_with_metrics(limits.clone(), metrics.clone());
    mqtt_broker.recover_from(&config.spool_directory).await?;
    let recovery = spool.recover().await.inspect_err(|error| {
        // Display only our typed diagnostic, never the serialized record or serde error.
        tracing::error!(%error, "EventBus restart recovery failed; startup blocked");
    })?;
    let recovered_files = recovery.committed_files;
    let recovered_count = events.restore(recovery.records)?;
    metrics.add(Metric::RecoveryRecords, recovered_count as u64);
    let sessions = Sessions::new(limits.clone());
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        auth_cache,
        registry,
        events.clone(),
        control,
        metrics.clone(),
        sessions,
        lifecycle.clone(),
    ));
    let shutdown = stop.child_token();
    let mut base_services =
        Services::new_with_mqtt(ingress.clone(), shutdown.clone(), mqtt_broker.clone());
    if auth_source == DeviceAuthSource::BusinessRpc {
        Arc::get_mut(&mut base_services)
            .ok_or(Error::Internal)?
            .business_auth = rpc_registry.clone();
    }
    if let Some(secret) = admin_secret {
        let admin = Arc::new(AdminAccess::new(&secret, identities, &limits)?);
        Arc::get_mut(&mut base_services)
            .ok_or(Error::Internal)?
            .admin = Some(admin);
    }
    let management_auth = ManagementAuthService::new(
        config.management_auth.clone(),
        base_services.admin.clone(),
        limits.clone(),
    )?
    .with_metrics(metrics.clone());
    if !config.management_http.ip().is_loopback()
        && !(if config
            .management_tls
            .as_ref()
            .is_some_and(|tls| tls.require_client_certificate)
        {
            management_auth.has_mtls_provider()
        } else {
            management_auth.has_provider()
        })
    {
        return Err(Error::Configuration);
    }
    if management_auth.has_legacy_provider() && management_auth.has_usable_scoped_provider() {
        tracing::warn!(
            "legacy bootstrap management token remains enabled together with scoped management authentication providers"
        );
    }
    Arc::get_mut(&mut base_services)
        .ok_or(Error::Internal)?
        .management_auth = Some(Arc::new(management_auth));
    let tls = if let Some(files) = &config.tls {
        Some(tls_acceptor(files).await?)
    } else {
        None
    };
    let management_tls = if let Some(files) = &config.management_tls {
        Some(management_tls_acceptor(files).await?)
    } else if let Some(files) = &config.tls {
        // Legacy certificate configuration is loaded separately for the management listener.
        Some(tls_acceptor(files).await?)
    } else {
        None
    };

    let device_ingress = TcpListener::bind(config.device_ingress)
        .await
        .map_err(|_| Error::Unavailable)?;
    let management_http = TcpListener::bind(config.management_http)
        .await
        .map_err(|_| Error::Unavailable)?;
    // Resolve port zero once: UDP must use the port actually assigned to TCP.
    let device_address = device_ingress
        .local_addr()
        .map_err(|_| Error::Unavailable)?;
    let udp = UdpSocket::bind(device_address)
        .await
        .map_err(|_| Error::Unavailable)?;
    let business = if let Some(address) = config.business_tcp {
        Some((
            TcpListener::bind(address)
                .await
                .map_err(|_| Error::Unavailable)?,
            business_sink.ok_or(Error::Internal)?,
        ))
    } else {
        None
    };

    lifecycle.mark_running()?;
    let work_listeners = CancellationToken::new();
    let management_listener = CancellationToken::new();
    let mut work_tasks = JoinSet::new();
    let maintenance_broker = mqtt_broker.clone();
    let maintenance_stop = work_listeners.child_token();
    work_tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = maintenance_stop.cancelled() => return Ok(()),
                _ = interval.tick() => maintenance_broker.tick()?,
            }
        }
    });
    if auth_source == DeviceAuthSource::BusinessRpc {
        let authority = rpc_registry.as_ref().ok_or(Error::Internal)?.clone();
        let ingress_for_offline = ingress.clone();
        let mqtt_for_offline = mqtt_broker.clone();
        let grace = Duration::from_millis(
            config
                .business_rpc
                .as_ref()
                .ok_or(Error::Internal)?
                .max_auth_control_offline_ms,
        );
        let offline_stop = work_listeners.child_token();
        work_tasks.spawn(watch_business_auth_offline(
            authority.subscribe_status(),
            grace,
            offline_stop,
            move |observed| {
                if authority
                    .invalidate_if_offline(observed, || {
                        let invalidate = AuthInvalidation::All;
                        ingress_for_offline.invalidate_auth_with(&invalidate, || {
                            mqtt_for_offline.invalidate_sessions(&invalidate)
                        })?;
                        Ok(())
                    })?
                    .is_some()
                {
                    ingress_for_offline
                        .metrics
                        .inc(Metric::BusinessRpcOfflineGraceExpirations);
                }
                Ok(())
            },
        ));
    }
    work_tasks.spawn(serve_device_ingress(
        device_ingress,
        base_services.clone(),
        tls.clone(),
        work_listeners.child_token(),
    ));
    let mut management_task = tokio::spawn(serve_management_http(
        management_http,
        base_services.clone(),
        management_tls,
        management_listener.child_token(),
    ));
    work_tasks.spawn(udp::serve(
        udp,
        base_services.clone(),
        work_listeners.child_token(),
    ));
    let business_accept_stop = CancellationToken::new();
    let business_connection_stop = CancellationToken::new();
    let mut business_task = None;
    if let Some((listener, sink)) = business {
        if let Some(rpc) = &config.business_rpc {
            let identity = if let Some(tls) = &rpc.tls {
                let identities = rpc
                    .identities
                    .iter()
                    .map(|configured| {
                        Ok((
                            parse_hex_32(&configured.certificate_sha256)?,
                            BusinessPrincipal {
                                id: configured.principal_id.clone(),
                                role: configured.role,
                                provider_id: configured.provider_id.clone(),
                                sink_id: configured.sink_id.clone(),
                                provide_methods: configured.provide_methods.clone(),
                                call_methods: configured.call_methods.clone(),
                                global: configured.global,
                                tenants: configured.tenants.clone(),
                                expires_at_ms: configured.expires_at_ms,
                            },
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let _ = tls;
                BusinessIdentity::Mtls { identities }
            } else {
                let name = rpc
                    .development_token_env
                    .as_deref()
                    .ok_or(Error::Configuration)?;
                let token = std::env::var(name).map_err(|_| Error::Configuration)?;
                if token.is_empty() || token.len() > BUSINESS_RPC_MAX_TOKEN_BYTES {
                    return Err(Error::Configuration);
                }
                let role = rpc.development_role.unwrap_or(BusinessRole::Multiplexed);
                BusinessIdentity::Development {
                    token_hash: Sha256::digest(token.as_bytes()).into(),
                    principal: BusinessPrincipal {
                        id: "development".into(),
                        role,
                        provider_id: role.auth_control().then(|| "primary".into()),
                        sink_id: role.events().then(|| "tcp-rpc".into()),
                        provide_methods: if role.auth_control() {
                            vec![
                                "device.authenticate".into(),
                                "device.resolve_verifier".into(),
                            ]
                        } else {
                            Vec::new()
                        },
                        call_methods: if role.auth_control() {
                            vec!["auth.sync".into(), "auth.invalidate".into()]
                        } else if role.commands() {
                            vec!["device.command.send".into()]
                        } else {
                            Vec::new()
                        },
                        global: true,
                        tenants: Vec::new(),
                        expires_at_ms: None,
                    },
                }
            };
            let tls = if let Some(files) = &rpc.tls {
                Some(management_tls_acceptor(files).await?)
            } else {
                None
            };
            let transport = BusinessRpcTransportConfig {
                identity,
                tls,
                v3: rpc.v3.clone(),
                v3_send_ahead: rpc.v3_send_ahead,
                v3_experiment_socket_send_buffer_bytes: rpc.v3_experiment_socket_send_buffer_bytes,
                max_connections: rpc.max_connections,
                max_frame_bytes: 8 * 1024 * 1024,
                auth_max_inflight: rpc.auth_max_inflight,
                heartbeat_ms: 5_000,
                handshake_timeout: Duration::from_millis(limits.connect_timeout_ms),
                read_timeout: Duration::from_millis(limits.packet_read_timeout_ms.max(15_000)),
                write_timeout: Duration::from_millis(limits.write_timeout_ms),
                event_ack_timeout: Duration::from_millis(limits.sink_timeout_ms),
            };
            let services = Arc::new(BusinessRpcServices {
                registry: rpc_registry.as_ref().ok_or(Error::Internal)?.clone(),
                sink: sink.clone(),
                ingress: ingress.clone(),
                mqtt: mqtt_broker.clone(),
                commands: base_services.router.clone(),
            });
            if rpc.allow_v1 {
                let secret = business_stream_token
                    .as_deref()
                    .ok_or(Error::Configuration)?;
                if secret.is_empty() || secret.len() > 256 {
                    return Err(Error::Configuration);
                }
                let legacy_hash: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
                business_task = Some(tokio::spawn(serve_business_mixed(
                    listener,
                    sink.clone(),
                    legacy_hash,
                    transport,
                    services,
                    limits.clone(),
                    (
                        business_accept_stop.child_token(),
                        business_connection_stop.child_token(),
                    ),
                )));
            } else {
                business_task = Some(tokio::spawn(business_rpc::serve(
                    listener,
                    transport,
                    services,
                    business_accept_stop.child_token(),
                    business_connection_stop.child_token(),
                )));
            }
        } else {
            let secret = business_stream_token.ok_or(Error::Configuration)?;
            let hash: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
            work_tasks.spawn(serve_business_stream(
                listener,
                sink,
                hash,
                limits.clone(),
                work_listeners.child_token(),
            ));
        }
    }
    tracing::info!(device_ingress=%device_address,management_http=%config.management_http,business_tcp=?config.business_tcp,"runtime ready");
    let mut management_running = true;
    let failure = tokio::select! {
        _ = shutdown.cancelled() => None,
        task = work_tasks.join_next() => Some(match task { Some(Ok(Err(error))) => error, _ => Error::Internal }),
        task = &mut management_task => {
            management_running = false;
            Some(match task { Ok(Err(error)) => error, _ => Error::Internal })
        },
    };
    lifecycle.begin_quiesce().await?;
    events.close_admission()?;
    work_listeners.cancel();
    business_accept_stop.cancel();
    while work_tasks.join_next().await.is_some() {}

    // All network owners have detached. Snapshot MQTT protocol state as one versioned,
    // fsynced image before claiming a successful planned shutdown. A storage failure blocks the
    // voluntary shutdown: the process stays alive and unready so an operator can repair storage.
    let retry_delay = Duration::from_millis(limits.retry_max_ms.min(1_000));
    let mut mqtt_structural_failure = None;
    loop {
        match mqtt_broker.commit_to(&config.spool_directory).await {
            Ok(_) => break,
            Err(error @ (Error::Overloaded | Error::Configuration | Error::Invalid)) => {
                // Limits validation proves that every admitted legal state fits the recovery
                // image. Retrying cannot repair a structural violation, but unrelated required
                // EventBus work must still be drained or spooled before exit is blocked.
                tracing::error!(error=%error, "MQTT recovery invariant violated");
                mqtt_structural_failure = Some(error);
                break;
            }
            Err(error) => {
                tracing::error!(error=%error, "MQTT recovery commit failed; shutdown remains blocked");
                tokio::time::sleep(retry_delay).await;
            }
        }
    }

    let drained = events
        .wait_required_drained(Duration::from_millis(limits.shutdown_drain_timeout_ms))
        .await?;
    if drained {
        events.stop_workers().await?;
        if !recovered_files.is_empty() {
            spool.remove_committed(recovered_files).await?;
        }
    } else {
        lifecycle.mark_spooling()?;
        loop {
            let pending = events.spool_records()?;
            if pending.is_empty() {
                events.stop_workers().await?;
                if !recovered_files.is_empty() {
                    spool.remove_committed(recovered_files.clone()).await?;
                }
                break;
            }
            let encoded_bytes = pending.iter().try_fold(0usize, |total, record| {
                total
                    .checked_add(
                        serde_json::to_vec(record)
                            .map_err(|_| Error::Internal)?
                            .len(),
                    )
                    .ok_or(Error::Overloaded)
            })?;
            match spool.commit(pending.clone()).await {
                Ok(_) => {
                    events.stop_workers().await?;
                    metrics.add(Metric::SpoolRecords, pending.len() as u64);
                    metrics.add(Metric::SpoolBytes, encoded_bytes as u64);
                    break;
                }
                Err(error) => {
                    tracing::error!(error=%error, pending=pending.len(), "event spool commit failed; shutdown remains blocked");
                    if events.wait_required_drained(retry_delay).await? {
                        events.stop_workers().await?;
                        if !recovered_files.is_empty() {
                            spool.remove_committed(recovered_files.clone()).await?;
                        }
                        break;
                    }
                }
            }
        }
    }
    if !shutdown_can_finish(mqtt_structural_failure.is_none(), true)
        && let Some(error) = mqtt_structural_failure
    {
        tracing::error!(error=%error, "critical MQTT recovery fault; required EventBus work is safe, process remains alive and unready");
        std::future::pending::<()>().await;
        return Err(error);
    }
    business_connection_stop.cancel();
    if let Some(task) = business_task.take() {
        let _ = task.await;
    }
    lifecycle.mark_drained()?;
    management_listener.cancel();
    if management_running {
        let _ = management_task.await;
    }
    tracing::info!("shutdown complete");
    failure.map_or(Ok(()), Err)
}

async fn watch_business_auth_offline(
    mut status: tokio::sync::watch::Receiver<ProviderStatus>,
    grace: Duration,
    stop: CancellationToken,
    mut invalidate: impl FnMut(ProviderStatus) -> Result<()>,
) -> Result<()> {
    let mut offline_state = None;
    let mut invalidated = false;
    loop {
        let current = *status.borrow();
        if current.serving {
            offline_state = None;
            invalidated = false;
        } else if offline_state != Some((current.transition, current.changed_at)) {
            offline_state = Some((current.transition, current.changed_at));
            invalidated = false;
        }
        let deadline = offline_state.map(|(_, since)| since + grace);
        if let Some(expires) = deadline.filter(|_| !invalidated)
            && tokio::time::Instant::now() >= expires
        {
            // A newly synchronized generation wins an exact-deadline race.
            let observed = *status.borrow();
            if !observed.serving && observed.transition == current.transition {
                invalidate(observed)?;
            }
            invalidated = true;
            continue;
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            changed = status.changed() => {
                if changed.is_err() { return Ok(()); }
            }
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now)), if deadline.is_some() && !invalidated => {}
        }
    }
}

async fn serve_business_mixed(
    listener: TcpListener,
    sink: Arc<BusinessRpcEventSink>,
    legacy_token_hash: [u8; 32],
    transport: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    limits: Arc<Limits>,
    stops: (CancellationToken, CancellationToken),
) -> Result<()> {
    let (stop_accepting, stop_connections) = stops;
    transport.validate(listener.local_addr().map_err(|_| Error::Unavailable)?)?;
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = stop_accepting.cancelled() => break,
            completed = tasks.join_next(), if !tasks.is_empty() => { if let Some(Err(error)) = completed { tracing::warn!(%error, "business mixed connection failed"); } continue; },
            accepted = listener.accept() => accepted.map_err(|_| Error::Unavailable)?,
        };
        if tasks.len() >= transport.max_connections {
            drop(accepted.0);
            continue;
        }
        let (mut stream, _) = accepted;
        let sink = sink.clone();
        let services = services.clone();
        let transport = transport.clone();
        let limits = limits.clone();
        let stop = stop_connections.child_token();
        tasks.spawn(async move {
            let first =
                tokio::time::timeout(Duration::from_millis(limits.connect_timeout_ms), async {
                    let mut header = [0u8; 4];
                    stream
                        .read_exact(&mut header)
                        .await
                        .map_err(|_| Error::Unavailable)?;
                    let length =
                        usize::try_from(u32::from_be_bytes(header)).map_err(|_| Error::Invalid)?;
                    if length == 0 || length > limits.max_tcp_frame_size {
                        return Err(Error::Invalid);
                    }
                    let mut payload = vec![0u8; length];
                    stream
                        .read_exact(&mut payload)
                        .await
                        .map_err(|_| Error::Unavailable)?;
                    Ok::<_, Error>(payload)
                })
                .await
                .map_err(|_| Error::Timeout)??;
            let header: serde_json::Value =
                serde_json::from_slice(&first).map_err(|_| Error::Invalid)?;
            match header.get("version").and_then(|value| value.as_u64()) {
                Some(1) => {
                    serve_business_connection(
                        stream,
                        sink,
                        legacy_token_hash,
                        limits,
                        stop,
                        Some(first),
                    )
                    .await
                }
                Some(2 | 3) if first.len() <= BUSINESS_RPC_HELLO_MAX_BYTES => {
                    business_rpc::serve_accepted(stream, transport, services, stop, first).await
                }
                _ => Err(Error::Invalid),
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}

fn shutdown_can_finish(mqtt_recovery_safe: bool, eventbus_required_work_safe: bool) -> bool {
    mqtt_recovery_safe && eventbus_required_work_safe
}

#[cfg(test)]
mod reliability_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn business_rpc_v3_config_is_explicit_and_limits_are_checked() {
        let mut config: Config =
            serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
        config.business_tcp = Some("127.0.0.1:19002".parse().unwrap());
        let legacy: BusinessRpcConfig = serde_json::from_value(serde_json::json!({
            "version": 2,
            "tls": null,
            "development_token_env": "NETBAIOT_BUSINESS_RPC_TOKEN"
        }))
        .unwrap();
        assert!(legacy.v3.is_none());
        config.business_rpc = Some(legacy);
        assert!(config.validate().is_ok());

        let mut limits = business_rpc_v3::V3Limits::default();
        config.business_rpc.as_mut().unwrap().v3 = Some(limits.clone());
        assert!(config.validate().is_ok());

        limits.max_frame_payload_bytes = 1024;
        config.business_rpc.as_mut().unwrap().v3 = Some(limits.clone());
        assert!(config.validate().is_err());
        limits.max_frame_payload_bytes = 8192;
        limits.max_concurrent_streams = 0;
        config.business_rpc.as_mut().unwrap().v3 = Some(limits.clone());
        assert!(config.validate().is_err());
        limits.max_concurrent_streams = 256;
        limits.initial_connection_window_bytes = limits.initial_stream_window_bytes - 1;
        config.business_rpc.as_mut().unwrap().v3 = Some(limits);
        assert!(config.validate().is_err());

        let rpc = config.business_rpc.as_mut().unwrap();
        rpc.v3 = Some(business_rpc_v3::V3Limits::default());
        rpc.v3_send_ahead = Some(V3SendAhead {
            stream_bytes: 1,
            connection_bytes: 8192,
        });
        assert!(config.validate().is_err());
        config.business_rpc.as_mut().unwrap().v3_send_ahead = Some(V3SendAhead {
            stream_bytes: 8192,
            connection_bytes: 131072,
        });
        assert!(config.validate().is_ok());
        config
            .business_rpc
            .as_mut()
            .unwrap()
            .v3_experiment_socket_send_buffer_bytes = Some(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn business_rpc_command_roles_validate_without_changing_existing_roles() {
        let config_for = |role,
                          provider: Option<&str>,
                          sink: Option<&str>,
                          provide: Vec<&str>,
                          call: Vec<&str>| {
            let mut config: Config =
                serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
            config.business_tcp = Some("127.0.0.1:19002".parse().unwrap());
            config.business_rpc = Some(BusinessRpcConfig {
                version: 2,
                v3: None,
                v3_send_ahead: None,
                v3_experiment_socket_send_buffer_bytes: None,
                tls: Some(ManagementTlsFiles {
                    certificate: "cert".into(),
                    private_key: "key".into(),
                    client_ca: Some("ca".into()),
                    require_client_certificate: true,
                }),
                identities: vec![BusinessRpcIdentityConfig {
                    certificate_sha256: "00".repeat(32),
                    principal_id: "test".into(),
                    role,
                    provider_id: provider.map(str::to_owned),
                    sink_id: sink.map(str::to_owned),
                    provide_methods: provide.into_iter().map(str::to_owned).collect(),
                    call_methods: call.into_iter().map(str::to_owned).collect(),
                    global: true,
                    tenants: Vec::new(),
                    expires_at_ms: None,
                }],
                development_token_env: None,
                development_role: None,
                allow_v1: false,
                max_connections: 8,
                auth_max_inflight: 16,
                max_auth_control_offline_ms: 30_000,
            });
            config.validate().is_ok()
        };
        assert!(config_for(
            BusinessRole::Commands,
            None,
            None,
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::Commands,
            None,
            None,
            vec!["device.authenticate"],
            vec!["device.command.send"]
        ));
        assert!(config_for(
            BusinessRole::Application,
            None,
            Some("tcp-rpc"),
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::Application,
            Some("primary"),
            Some("tcp-rpc"),
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::Events,
            None,
            Some("tcp-rpc"),
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::AuthControl,
            Some("primary"),
            None,
            vec!["device.authenticate", "device.resolve_verifier"],
            vec!["device.command.send"]
        ));
        assert!(config_for(
            BusinessRole::Multiplexed,
            Some("primary"),
            Some("tcp-rpc"),
            vec!["device.authenticate", "device.resolve_verifier"],
            vec!["auth.sync", "auth.invalidate"]
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn business_auth_zero_grace_invalidates_without_clock_advance() {
        let (status, receiver) = tokio::sync::watch::channel(ProviderStatus {
            epoch: 1,
            serving: true,
            transition: 1,
            changed_at: tokio::time::Instant::now(),
        });
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let stop = CancellationToken::new();
        let task = tokio::spawn(watch_business_auth_offline(
            receiver,
            Duration::ZERO,
            stop.clone(),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        tokio::task::yield_now().await;
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: false,
            transition: 2,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn business_auth_grace_deadline_and_reconnect_are_generation_fenced() {
        let (status, receiver) = tokio::sync::watch::channel(ProviderStatus {
            epoch: 1,
            serving: true,
            transition: 1,
            changed_at: tokio::time::Instant::now(),
        });
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let stop = CancellationToken::new();
        let task = tokio::spawn(watch_business_auth_offline(
            receiver,
            Duration::from_secs(30),
            stop.clone(),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        tokio::task::yield_now().await;
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: false,
            transition: 2,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(29_999)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        status.send_replace(ProviderStatus {
            epoch: 2,
            serving: true,
            transition: 3,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        status.send_replace(ProviderStatus {
            epoch: 2,
            serving: false,
            transition: 4,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        status.send_replace(ProviderStatus {
            epoch: 3,
            serving: true,
            transition: 5,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(21)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        status.send_replace(ProviderStatus {
            epoch: 3,
            serving: false,
            transition: 6,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        status.send_replace(ProviderStatus {
            epoch: 4,
            serving: true,
            transition: 7,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn collapsed_serving_disconnect_starts_grace_at_actual_disconnect() {
        let (status, receiver) = tokio::sync::watch::channel(ProviderStatus {
            epoch: 0,
            serving: false,
            transition: 0,
            changed_at: tokio::time::Instant::now(),
        });
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let stop = CancellationToken::new();
        let task = tokio::spawn(watch_business_auth_offline(
            receiver,
            Duration::from_secs(30),
            stop.clone(),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: true,
            transition: 1,
            changed_at: tokio::time::Instant::now(),
        });
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: false,
            transition: 2,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[test]
    fn business_rpc_auth_and_event_delivery_are_independent() {
        let base: Config =
            serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
        for (auth, delivery) in [
            (
                DeviceAuthSource::BusinessRpc,
                EventDeliverySource::BusinessRpc,
            ),
            (DeviceAuthSource::BusinessRpc, EventDeliverySource::Http),
            (DeviceAuthSource::Http, EventDeliverySource::BusinessRpc),
            (DeviceAuthSource::Static, EventDeliverySource::BusinessRpc),
        ] {
            let mut value = serde_json::to_value(&base).unwrap();
            value["business_tcp"] = serde_json::json!("127.0.0.1:19002");
            value["business_rpc"] = serde_json::json!({
                "version": 2,
                "tls": null,
                "development_token_env": "NETBAIOT_BUSINESS_RPC_TOKEN"
            });
            value["device_auth"] = serde_json::to_value(auth).unwrap();
            value["event_delivery"] = serde_json::to_value(delivery).unwrap();
            value["auth_provider_url"] = if auth == DeviceAuthSource::Http {
                serde_json::json!("http://127.0.0.1:19003")
            } else {
                serde_json::Value::Null
            };
            value["delivery_url"] = if delivery == EventDeliverySource::Http {
                serde_json::json!("http://127.0.0.1:19004")
            } else {
                serde_json::Value::Null
            };
            let config: Config = serde_json::from_value(value).unwrap();
            assert!(
                config.validate().is_ok(),
                "combination {:?} {:?}",
                auth as u8,
                delivery as u8
            );
        }
    }

    #[test]
    fn mqtt_recovery_structural_eventbus_safety_001() {
        assert!(!shutdown_can_finish(false, false));
        assert!(!shutdown_can_finish(false, true));
        assert!(!shutdown_can_finish(true, false));
        assert!(shutdown_can_finish(true, true));
    }

    fn delivery() -> DeliveryEnvelope {
        DeliveryEnvelope {
            event: Arc::new(DeviceEvent {
                event_id: EventId::generate(),
                source_message_id: SourceMessageId::new("business-filter").unwrap(),
                device: DeviceKey {
                    tenant_id: TenantId::new("tenant-a").unwrap(),
                    product_id: ProductId::new("product").unwrap(),
                    device_id: DeviceId::new("device").unwrap(),
                },
                received_at: 1,
                occurred_at: None,
                kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
            }),
            sink_id: SinkId::new("tcp-rpc").unwrap(),
            attempt: 1,
            accepted_at: 1,
        }
    }

    #[tokio::test]
    async fn eventbus_tcp_absence_filter_change_and_reconnect_preserve_isolation() {
        let limits = Arc::new(Limits::default());
        let metrics = Arc::new(Metrics::with_lock_timing());
        let tcp = TcpStreamSink::new();
        let fast_id = SinkId::new("fast").unwrap();
        let tcp_id = SinkId::new("tcp").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            metrics.clone(),
            vec![
                SinkDefinition::bounded(
                    fast_id.clone(),
                    SinkDeliveryMode::ConfirmedRequired,
                    Arc::new(AuditSink),
                    &limits,
                ),
                SinkDefinition::bounded(
                    tcp_id.clone(),
                    SinkDeliveryMode::ConfirmedRequired,
                    tcp.clone(),
                    &limits,
                ),
            ],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![fast_id, tcp_id],
            }],
            1,
        )
        .unwrap();
        let event = (*delivery().event).clone();
        let expected = event.event_id;
        bus.publish(event).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.get(Metric::SinkAcks) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(bus.usage().unwrap().pending_required, 1);
        let (sender, mut mismatched) = mpsc::channel(1);
        let generation = tcp
            .claim(
                sender,
                EventFilter {
                    tenant: Some(TenantId::new("tenant-b").unwrap()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(mismatched.try_recv().is_err());
        assert_eq!(metrics.get(Metric::SinkAcks), 1);
        assert_eq!(metrics.get(Metric::SinkRetries), 0);
        assert!(
            metrics
                .render()
                .contains("netbaiot_event_bus_probe_wake_timer_total 0\n")
        );
        tcp.release(generation).unwrap();
        let (sender, mut requests) = mpsc::channel(1);
        let generation = tcp.claim(sender, EventFilter::default()).unwrap();
        let request = tokio::time::timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.delivery.event.event_id, expected);
        request.result.send(Ok(SinkAck)).unwrap();
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        tcp.release(generation).unwrap();
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn active_subscriber_is_rejected_and_filter_mismatch_is_not_acknowledged() {
        let sink = TcpStreamSink::new();
        let (sender, _receiver) = mpsc::channel(1);
        let generation = sink
            .claim(
                sender,
                EventFilter {
                    tenant: Some(TenantId::new("tenant-b").unwrap()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        let (other, _other_receiver) = mpsc::channel(1);
        assert!(matches!(
            sink.claim(other, EventFilter::default()),
            Err(Error::Conflict)
        ));
        let pending = delivery();
        let expected = pending.event.event_id;
        let sink_task = {
            let sink = sink.clone();
            tokio::spawn(async move { sink.deliver(pending).await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(20), async {
                while !sink_task.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "a non-matching subscriber must not acknowledge required work"
        );
        sink.release(generation.wrapping_add(1)).unwrap();
        assert!(sink.has_owner().unwrap());
        sink.release(generation).unwrap();
        assert!(!sink.has_owner().unwrap());

        // The same already-accepted responsibility survives the filter revision and is ACKed
        // only after a later eligible subscriber explicitly confirms it.
        let (matching, mut requests) = mpsc::channel(1);
        let matching_generation = sink
            .claim(
                matching,
                EventFilter {
                    tenant: Some(TenantId::new("tenant-a").unwrap()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        let request = requests.recv().await.unwrap();
        assert_eq!(request.delivery.event.event_id, expected);
        request.result.send(Ok(SinkAck)).unwrap();
        assert!(matches!(sink_task.await.unwrap(), Ok(SinkAck)));
        sink.release(matching_generation).unwrap();
    }
}
