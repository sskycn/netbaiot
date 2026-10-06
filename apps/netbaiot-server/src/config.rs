use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Config {
    /// Device MQTT/framed TCP listener; UDP uses the same actual port. Public TCP requires TLS.
    pub device_ingress: SocketAddr,
    /// Independent authenticated management listener. Public management requires TLS.
    pub management_http: SocketAddr,
    /// Optional confirmed business listener; role/TLS constraints require config check.
    pub business_tcp: Option<SocketAddr>,
    #[serde(default)]
    pub business_rpc: Option<BusinessRpcConfig>,
    #[serde(default)]
    pub device_auth: Option<DeviceAuthSource>,
    #[serde(default)]
    pub event_delivery: Option<EventDeliverySource>,
    #[serde(default)]
    /// Development mode requires loopback listeners; never reuse demo credentials in production.
    pub development: bool,
    #[serde(default)]
    /// Bounded runtime resource limits, defaulted from Limits::default().
    pub limits: Limits,
    #[serde(default)]
    /// Static device credentials; secrets must not be logged or reused from development.
    pub credentials: Vec<Credential>,
    /// Device server PEM certificate and matching private-key paths.
    pub tls: Option<TlsFiles>,
    #[serde(default)]
    /// Independent management TLS and optional mapped client certificate authentication.
    pub management_tls: Option<ManagementTlsFiles>,
    #[serde(default)]
    /// Management providers, scoped identities and protected secret-source names.
    pub management_auth: ManagementAuthConfig,
    /// Required HTTP sink URL; HTTPS or loopback HTTP, without URL credentials.
    pub delivery_url: Option<String>,
    /// Optional HTTP device-auth provider URL; default checks do not contact it.
    pub auth_provider_url: Option<String>,
    /// Dedicated local planned-restart recovery directory; one gateway owner only.
    pub spool_directory: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TlsFiles {
    pub certificate: String,
    pub private_key: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagementTlsFiles {
    pub certificate: String,
    pub private_key: String,
    pub client_ca: Option<String>,
    #[serde(default)]
    pub require_client_certificate: bool,
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum DeviceAuthSource {
    Static,
    Http,
    BusinessRpc,
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum EventDeliverySource {
    Http,
    BusinessRpc,
    DevelopmentAudit,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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

pub(crate) fn parse_hex_32(text: &str) -> Result<[u8; 32]> {
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

pub(crate) async fn read_bounded(path: &str, maximum: u64) -> Result<Vec<u8>> {
    read_bounded_path(std::path::Path::new(path), maximum).await
}

pub(crate) async fn read_bounded_path(path: &std::path::Path, maximum: u64) -> Result<Vec<u8>> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|_| Error::Configuration)?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(Error::Configuration);
    }
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
        if self.diagnostics().is_empty() {
            Ok(())
        } else {
            Err(Error::Configuration)
        }
    }

    pub(crate) fn validate_business(&self) -> Result<()> {
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
        Ok(())
    }
}

impl BusinessRpcConfig {
    pub(crate) fn development_token(&self) -> Result<Option<String>> {
        if self.tls.is_some() {
            return Ok(None);
        }
        let name = self
            .development_token_env
            .as_deref()
            .ok_or(Error::Configuration)?;
        let token = std::env::var(name).map_err(|_| Error::Configuration)?;
        if token.is_empty() || token.len() > BUSINESS_RPC_MAX_TOKEN_BYTES {
            return Err(Error::Configuration);
        }
        Ok(Some(token))
    }
}
