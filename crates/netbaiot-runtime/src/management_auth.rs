//! Management identities and authorization are independent of device authentication.
use crate::{AdminAccess, Error, Limits, Metrics, Result};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use netbaiot_core::{DeviceKey, ProductId, TenantId, Timestamp};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdminAuthMethod {
    StaticToken,
    ApiKey,
    Jwt,
    Mtls,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AdminScope {
    RuntimeRead,
    MetricsRead,
    ConnectionRead,
    DeviceCommand,
    AuthInvalidate,
    AuthInvalidateAll,
    ControlRead,
    ControlWrite,
    RoutesRead,
    RoutesWrite,
    RuntimeDrain,
    AdminAll,
}
impl AdminScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeRead => "runtime.read",
            Self::MetricsRead => "metrics.read",
            Self::ConnectionRead => "connection.read",
            Self::DeviceCommand => "device.command",
            Self::AuthInvalidate => "auth.invalidate",
            Self::AuthInvalidateAll => "auth.invalidate.all",
            Self::ControlRead => "control.read",
            Self::ControlWrite => "control.write",
            Self::RoutesRead => "routes.read",
            Self::RoutesWrite => "routes.write",
            Self::RuntimeDrain => "runtime.drain",
            Self::AdminAll => "admin.*",
        }
    }
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "runtime.read" => Self::RuntimeRead,
            "metrics.read" => Self::MetricsRead,
            "connection.read" => Self::ConnectionRead,
            "device.command" => Self::DeviceCommand,
            "auth.invalidate" => Self::AuthInvalidate,
            "auth.invalidate.all" => Self::AuthInvalidateAll,
            "control.read" => Self::ControlRead,
            "control.write" => Self::ControlWrite,
            "routes.read" => Self::RoutesRead,
            "routes.write" => Self::RoutesWrite,
            "runtime.drain" => Self::RuntimeDrain,
            "admin.*" => Self::AdminAll,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ScopeSet(Vec<AdminScope>);
impl ScopeSet {
    fn parse(values: &[String], limits: &Limits) -> Result<Self> {
        let bytes = values
            .iter()
            .try_fold(0usize, |n, value| n.checked_add(value.len()))
            .ok_or(Error::Configuration)?;
        if values.len() > limits.management_auth_max_scopes
            || bytes > limits.management_auth_max_scope_bytes
        {
            return Err(Error::Configuration);
        }
        let mut scopes = Vec::with_capacity(values.len());
        for value in values {
            let scope = AdminScope::parse(value).ok_or(Error::Configuration)?;
            if scopes.contains(&scope) {
                return Err(Error::Configuration);
            }
            scopes.push(scope);
        }
        Ok(Self(scopes))
    }
    fn allows(&self, scope: AdminScope) -> bool {
        self.0.contains(&AdminScope::AdminAll) || self.0.contains(&scope)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminResourceConfig {
    pub tenants: Vec<TenantId>,
    pub products: Vec<AdminProduct>,
    pub devices: Vec<DeviceKey>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminProduct {
    pub tenant_id: TenantId,
    pub product_id: ProductId,
}

#[derive(Clone, Debug)]
pub enum AdminResourceScope {
    Global,
    Restricted(AdminResourceConfig),
}
impl AdminResourceScope {
    fn from_config(config: &AdminResourceConfig, global: bool, limits: &Limits) -> Result<Self> {
        let count = config
            .tenants
            .len()
            .checked_add(config.products.len())
            .and_then(|n| n.checked_add(config.devices.len()))
            .ok_or(Error::Configuration)?;
        let bytes = config
            .tenants
            .iter()
            .try_fold(0usize, |n, t| n.checked_add(t.as_str().len()))
            .and_then(|n| {
                config.products.iter().try_fold(n, |n, p| {
                    n.checked_add(p.tenant_id.as_str().len())?
                        .checked_add(p.product_id.as_str().len())
                })
            })
            .and_then(|n| {
                config.devices.iter().try_fold(n, |n, d| {
                    n.checked_add(d.tenant_id.as_str().len())?
                        .checked_add(d.product_id.as_str().len())?
                        .checked_add(d.device_id.as_str().len())
                })
            })
            .ok_or(Error::Configuration)?;
        if count > limits.management_auth_max_resource_entries
            || bytes > limits.management_auth_max_resource_bytes
        {
            return Err(Error::Configuration);
        }
        if global {
            return Ok(Self::Global);
        }
        Ok(Self::Restricted(config.clone()))
    }
    pub fn allows_tenant(&self, tenant: &TenantId) -> bool {
        match self {
            Self::Global => true,
            Self::Restricted(r) => r.tenants.contains(tenant),
        }
    }
    pub fn allows_product(&self, tenant: &TenantId, product: &ProductId) -> bool {
        match self {
            Self::Global => true,
            Self::Restricted(r) => {
                r.tenants.contains(tenant)
                    || r.products
                        .iter()
                        .any(|p| &p.tenant_id == tenant && &p.product_id == product)
            }
        }
    }
    pub fn allows_device(&self, device: &DeviceKey) -> bool {
        match self {
            Self::Global => true,
            Self::Restricted(r) => {
                self.allows_product(&device.tenant_id, &device.product_id)
                    || r.devices.contains(device)
            }
        }
    }
    pub fn is_global(&self) -> bool {
        matches!(self, Self::Global)
    }
}

#[derive(Clone, Debug)]
pub struct AdminPrincipal {
    pub subject: String,
    pub auth_method: AdminAuthMethod,
    pub scopes: ScopeSet,
    pub resource_scope: AdminResourceScope,
    pub expires_at: Option<Timestamp>,
    pub credential_id: Option<String>,
    pub auth_generation: u64,
}
impl AdminPrincipal {
    pub fn require_scope(&self, scope: AdminScope) -> Result<()> {
        if self.scopes.allows(scope) {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
    pub fn require_global(&self) -> Result<()> {
        if self.resource_scope.is_global() {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
    pub fn require_device(&self, device: &DeviceKey) -> Result<()> {
        if self.resource_scope.allows_device(device) {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
    pub fn require_product(&self, tenant: &TenantId, product: &ProductId) -> Result<()> {
        if self.resource_scope.allows_product(tenant, product) {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
    pub fn require_tenant(&self, tenant: &TenantId) -> Result<()> {
        if self.resource_scope.allows_tenant(tenant) {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
    pub fn bootstrap() -> Self {
        Self {
            subject: "bootstrap-admin".into(),
            auth_method: AdminAuthMethod::StaticToken,
            scopes: ScopeSet(vec![AdminScope::AdminAll]),
            resource_scope: AdminResourceScope::Global,
            expires_at: None,
            credential_id: None,
            auth_generation: 0,
        }
    }
}

fn valid_name(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:/@".contains(&b))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ManagementAuthConfig {
    pub legacy_static_token_enabled: Option<bool>,
    pub api_keys: Vec<ApiKeyConfig>,
    pub jwt: Option<JwtConfig>,
    pub mtls_identities: Vec<MtlsIdentityConfig>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyConfig {
    pub key_id: String,
    pub secret_env: String,
    pub subject: String,
    pub scopes: Vec<String>,
    #[serde(default)]
    pub resources: AdminResourceConfig,
    #[serde(default)]
    pub global: bool,
    pub expires_at: Option<Timestamp>,
    #[serde(default)]
    pub auth_generation: u64,
    #[serde(default = "enabled_default")]
    pub enabled: bool,
}
fn enabled_default() -> bool {
    true
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MtlsIdentityConfig {
    pub certificate_sha256: String,
    pub subject: String,
    pub scopes: Vec<String>,
    #[serde(default)]
    pub resources: AdminResourceConfig,
    #[serde(default)]
    pub global: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    #[serde(default = "default_sub")]
    pub subject_claim: String,
    #[serde(default = "default_scope")]
    pub scope_claim: String,
    #[serde(default = "default_roles")]
    pub roles_claim: String,
    #[serde(default = "default_tenants")]
    pub tenant_claim: String,
    #[serde(default, deserialize_with = "deserialize_role_scopes")]
    pub role_scopes: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub global_roles: Vec<String>,
}
fn default_sub() -> String {
    "sub".into()
}
fn default_scope() -> String {
    "scope".into()
}
fn default_roles() -> String {
    "roles".into()
}
fn default_tenants() -> String {
    "tenants".into()
}
fn deserialize_role_scopes<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<String, Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct RoleVisitor;
    impl<'de> Visitor<'de> for RoleVisitor {
        type Value = HashMap<String, Vec<String>>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a role-to-scopes object with unique role names")
        }
        fn visit_map<M: MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut roles = HashMap::new();
            while let Some((role, scopes)) = map.next_entry()? {
                if roles.insert(role, scopes).is_some() {
                    return Err(serde::de::Error::custom("duplicate role"));
                }
            }
            Ok(roles)
        }
    }
    deserializer.deserialize_map(RoleVisitor)
}

struct ApiKeyRecord {
    hash: [u8; 32],
    principal: AdminPrincipal,
    enabled: bool,
}
struct JwksState {
    keys: HashMap<String, DecodingKey>,
    expires: Option<Instant>,
    last_refresh: Option<Instant>,
}
struct JwtProvider {
    config: JwtConfig,
    client: reqwest::Client,
    cache: Mutex<JwksState>,
    refresh: Mutex<()>,
    limits: Arc<Limits>,
    metrics: Option<Arc<Metrics>>,
}

pub struct ManagementAuthService {
    legacy: Option<Arc<AdminAccess>>,
    api_keys: HashMap<String, ApiKeyRecord>,
    jwt: Option<JwtProvider>,
    mtls: HashMap<[u8; 32], AdminPrincipal>,
    jwt_max_bytes: usize,
}

impl ManagementAuthService {
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        if let Some(jwt) = &mut self.jwt {
            jwt.metrics = Some(metrics);
        }
        self
    }
    pub fn new(
        config: ManagementAuthConfig,
        legacy: Option<Arc<AdminAccess>>,
        limits: Arc<Limits>,
    ) -> Result<Self> {
        Self::new_with_resolver(config, legacy, limits, |name| std::env::var(name).ok())
    }

    fn new_with_resolver(
        config: ManagementAuthConfig,
        legacy: Option<Arc<AdminAccess>>,
        limits: Arc<Limits>,
        resolve: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        limits.validate()?;
        let legacy = if config.legacy_static_token_enabled == Some(false) {
            None
        } else {
            legacy
        };
        if config.api_keys.len() > limits.management_api_key_max_entries
            || config.mtls_identities.len() > limits.management_api_key_max_entries
        {
            return Err(Error::Configuration);
        }
        let mut api_keys = HashMap::new();
        let mut total_bytes = 0usize;
        for key in config.api_keys {
            if !valid_name(&key.key_id, 64)
                || key.secret_env.is_empty()
                || key.secret_env.len() > 128
                || !key
                    .secret_env
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                || !valid_name(&key.subject, limits.management_auth_max_subject_bytes)
            {
                return Err(Error::Configuration);
            }
            let secret = resolve(&key.secret_env).ok_or(Error::Configuration)?;
            if secret.len() != 64 || !secret.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Error::Configuration);
            }
            total_bytes = total_bytes
                .checked_add(key.key_id.len())
                .and_then(|n| n.checked_add(key.subject.len()))
                .and_then(|n| n.checked_add(secret.len()))
                .and_then(|n| n.checked_add(key.secret_env.len()))
                .and_then(|n| n.checked_add(64))
                .ok_or(Error::Configuration)?;
            total_bytes = key
                .scopes
                .iter()
                .try_fold(total_bytes, |n, scope| n.checked_add(scope.len()))
                .ok_or(Error::Configuration)?;
            total_bytes = total_bytes
                .checked_add(
                    serde_json::to_vec(&key.resources)
                        .map_err(|_| Error::Configuration)?
                        .len(),
                )
                .ok_or(Error::Configuration)?;
            if total_bytes > limits.management_api_key_max_bytes {
                return Err(Error::Configuration);
            }
            let principal = AdminPrincipal {
                subject: key.subject,
                auth_method: AdminAuthMethod::ApiKey,
                scopes: ScopeSet::parse(&key.scopes, &limits)?,
                resource_scope: AdminResourceScope::from_config(
                    &key.resources,
                    key.global,
                    &limits,
                )?,
                expires_at: key.expires_at,
                credential_id: Some(key.key_id.clone()),
                auth_generation: key.auth_generation,
            };
            if api_keys
                .insert(
                    key.key_id,
                    ApiKeyRecord {
                        hash: Sha256::digest(secret.as_bytes()).into(),
                        principal,
                        enabled: key.enabled,
                    },
                )
                .is_some()
            {
                return Err(Error::Configuration);
            }
        }
        let jwt = if let Some(jwt) = config.jwt {
            Some(JwtProvider::new(jwt, limits.clone())?)
        } else {
            None
        };
        let mut mtls = HashMap::new();
        let mut mtls_bytes = 0usize;
        for identity in config.mtls_identities {
            if !valid_name(&identity.subject, limits.management_auth_max_subject_bytes) {
                return Err(Error::Configuration);
            }
            let fingerprint =
                decode_hex_32(&identity.certificate_sha256).ok_or(Error::Configuration)?;
            mtls_bytes = mtls_bytes
                .checked_add(identity.subject.len())
                .and_then(|n| n.checked_add(64))
                .ok_or(Error::Configuration)?;
            mtls_bytes = identity
                .scopes
                .iter()
                .try_fold(mtls_bytes, |n, scope| n.checked_add(scope.len()))
                .ok_or(Error::Configuration)?;
            mtls_bytes = mtls_bytes
                .checked_add(
                    serde_json::to_vec(&identity.resources)
                        .map_err(|_| Error::Configuration)?
                        .len(),
                )
                .ok_or(Error::Configuration)?;
            if mtls_bytes > limits.management_api_key_max_bytes {
                return Err(Error::Configuration);
            }
            let principal = AdminPrincipal {
                subject: identity.subject,
                auth_method: AdminAuthMethod::Mtls,
                scopes: ScopeSet::parse(&identity.scopes, &limits)?,
                resource_scope: AdminResourceScope::from_config(
                    &identity.resources,
                    identity.global,
                    &limits,
                )?,
                expires_at: None,
                credential_id: None,
                auth_generation: 0,
            };
            if mtls.insert(fingerprint, principal).is_some() {
                return Err(Error::Configuration);
            }
        }
        Ok(Self {
            legacy,
            api_keys,
            jwt,
            mtls,
            jwt_max_bytes: limits.management_jwt_max_bytes,
        })
    }
    pub fn has_provider(&self) -> bool {
        self.legacy.is_some()
            || !self.api_keys.is_empty()
            || self.jwt.is_some()
            || !self.mtls.is_empty()
    }
    pub async fn authenticate(
        &self,
        authorization: Option<&str>,
        certificate: Option<&[u8]>,
    ) -> Result<AdminPrincipal> {
        // One and only one credential. A failed provider never falls through to another.
        if authorization.is_some() && certificate.is_some() {
            return Err(Error::Authentication);
        }
        if let Some(cert) = certificate {
            let hash: [u8; 32] = Sha256::digest(cert).into();
            return self.mtls.get(&hash).cloned().ok_or(Error::Authentication);
        }
        let header = authorization.ok_or(Error::Authentication)?;
        if let Some(value) = header.strip_prefix("ApiKey ") {
            if value.len() > 129 {
                return Err(Error::Authentication);
            }
            let (id, secret) = value.split_once('.').ok_or(Error::Authentication)?;
            if !valid_name(id, 64)
                || secret.len() != 64
                || !secret.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(Error::Authentication);
            }
            let record = self.api_keys.get(id).ok_or(Error::Authentication)?;
            if !record.enabled
                || !bool::from(record.hash.ct_eq(&Sha256::digest(secret.as_bytes())))
                || record
                    .principal
                    .expires_at
                    .is_some_and(|expiry| expiry <= crate::now_ms())
            {
                return Err(Error::Authentication);
            }
            return Ok(record.principal.clone());
        }
        let value = header
            .strip_prefix("Bearer ")
            .ok_or(Error::Authentication)?;
        if value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
            let legacy = self.legacy.as_ref().ok_or(Error::Authentication)?;
            return legacy.authenticate(value.as_bytes());
        }
        if value.len() <= self.jwt_max_bytes && value.split('.').count() == 3 {
            return self
                .jwt
                .as_ref()
                .ok_or(Error::Authentication)?
                .authenticate(value)
                .await;
        }
        Err(Error::Authentication)
    }
}

fn decode_hex_32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        out[i] = (chunk[0] as char)
            .to_digit(16)?
            .checked_mul(16)?
            .checked_add((chunk[1] as char).to_digit(16)?)? as u8;
    }
    Some(out)
}

impl JwtProvider {
    fn new(config: JwtConfig, limits: Arc<Limits>) -> Result<Self> {
        let url = url::Url::parse(&config.jwks_url).map_err(|_| Error::Configuration)?;
        let issuer = url::Url::parse(&config.issuer).map_err(|_| Error::Configuration)?;
        if url.scheme() != "https"
            || config.jwks_url.len() > 2_048
            || url.host_str().is_none()
            || url.username() != ""
            || url.password().is_some()
            || url.fragment().is_some()
            || issuer.scheme() != "https"
            || issuer.host_str().is_none()
            || issuer.username() != ""
            || issuer.password().is_some()
            || issuer.query().is_some()
            || issuer.fragment().is_some()
            || config.issuer.len() > 512
            || config.audience.is_empty()
            || config.audience.len() > 256
            || config.role_scopes.len() > limits.management_auth_max_scopes
            || config.global_roles.len() > limits.management_auth_max_scopes
            || [
                &config.subject_claim,
                &config.scope_claim,
                &config.roles_claim,
                &config.tenant_claim,
            ]
            .iter()
            .any(|name| !valid_name(name, 64))
        {
            return Err(Error::Configuration);
        }
        let mut total = 0usize;
        for (role, scopes) in &config.role_scopes {
            if !valid_name(role, 64) {
                return Err(Error::Configuration);
            }
            total = total.checked_add(role.len()).ok_or(Error::Configuration)?;
            ScopeSet::parse(scopes, &limits)?;
            total = scopes
                .iter()
                .try_fold(total, |n, s| n.checked_add(s.len()))
                .ok_or(Error::Configuration)?;
        }
        for role in &config.global_roles {
            if !config.role_scopes.contains_key(role) {
                return Err(Error::Configuration);
            }
            total = total.checked_add(role.len()).ok_or(Error::Configuration)?;
        }
        if total
            > limits
                .management_auth_max_scope_bytes
                .checked_mul(4)
                .ok_or(Error::Configuration)?
        {
            return Err(Error::Configuration);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Configuration)?;
        Ok(Self {
            config,
            client,
            cache: Mutex::new(JwksState {
                keys: HashMap::new(),
                expires: None,
                last_refresh: None,
            }),
            refresh: Mutex::new(()),
            limits,
            metrics: None,
        })
    }

    async fn authenticate(&self, token: &str) -> Result<AdminPrincipal> {
        let header = decode_header(token).map_err(|_| Error::Authentication)?;
        if header.alg != Algorithm::RS256
            || header.jku.is_some()
            || header.jwk.is_some()
            || header.x5u.is_some()
        {
            return Err(Error::Authentication);
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|kid| valid_name(kid, 128))
            .ok_or(Error::Authentication)?;
        let key = self.key(kid).await?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.leeway = 0;
        validation.validate_nbf = true;
        validation.set_required_spec_claims(&["exp", "nbf", "iss", "aud"]);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        let claims = decode::<serde_json::Value>(token, &key, &validation)
            .map_err(|_| Error::Authentication)?
            .claims;
        self.map_claims(&claims)
    }

    async fn key(&self, kid: &str) -> Result<DecodingKey> {
        {
            let state = self.cache.lock().await;
            if state.expires.is_some_and(|expiry| Instant::now() < expiry)
                && let Some(key) = state.keys.get(kid)
            {
                if let Some(metrics) = &self.metrics {
                    metrics.management_jwks_cache(true);
                }
                return Ok(key.clone());
            }
            if state.last_refresh.is_some_and(|last| {
                last.elapsed()
                    < Duration::from_millis(self.limits.management_jwks_refresh_min_interval_ms)
            }) {
                return Err(Error::Authentication);
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.management_jwks_cache(false);
        }
        // No unbounded waiter queue. Concurrent misses fail closed while one refresh proceeds.
        let _refresh = self.refresh.try_lock().map_err(|_| Error::Authentication)?;
        {
            let state = self.cache.lock().await;
            if state.expires.is_some_and(|expiry| Instant::now() < expiry)
                && let Some(key) = state.keys.get(kid)
            {
                return Ok(key.clone());
            }
            if state.last_refresh.is_some_and(|last| {
                last.elapsed()
                    < Duration::from_millis(self.limits.management_jwks_refresh_min_interval_ms)
            }) {
                return Err(Error::Authentication);
            }
        }
        // Charge the attempt before network I/O so an outage cannot cause rapid retries.
        self.cache.lock().await.last_refresh = Some(Instant::now());
        let response = self
            .client
            .get(&self.config.jwks_url)
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
            let length = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(Error::Unavailable)?;
            if length > self.limits.management_jwks_max_bytes {
                return Err(Error::Unavailable);
            }
            bytes.extend_from_slice(&chunk);
        }
        let keys = parse_jwks(&bytes, &self.limits)?;
        let mut state = self.cache.lock().await;
        state.keys = keys;
        state.expires =
            Some(Instant::now() + Duration::from_millis(self.limits.management_jwks_ttl_ms));
        state.keys.get(kid).cloned().ok_or(Error::Authentication)
    }

    fn map_claims(&self, claims: &serde_json::Value) -> Result<AdminPrincipal> {
        let object = claims.as_object().ok_or(Error::Authentication)?;
        let subject = object
            .get(&self.config.subject_claim)
            .and_then(serde_json::Value::as_str)
            .filter(|s| valid_name(s, self.limits.management_auth_max_subject_bytes))
            .ok_or(Error::Authentication)?;
        let mut scopes: Vec<String> = Vec::new();
        if let Some(value) = object.get(&self.config.scope_claim) {
            if let Some(text) = value.as_str() {
                if text.len() > self.limits.management_auth_max_scope_bytes {
                    return Err(Error::Authentication);
                }
                scopes.extend(text.split_ascii_whitespace().map(str::to_owned));
            } else if let Some(array) = value.as_array() {
                if array.len() > self.limits.management_auth_max_scopes {
                    return Err(Error::Authentication);
                }
                for item in array {
                    scopes.push(item.as_str().ok_or(Error::Authentication)?.to_owned());
                }
            } else {
                return Err(Error::Authentication);
            }
        }
        let roles = object
            .get(&self.config.roles_claim)
            .and_then(serde_json::Value::as_array);
        if object.contains_key(&self.config.roles_claim) && roles.is_none() {
            return Err(Error::Authentication);
        }
        let roles = roles.map(Vec::as_slice).unwrap_or_default();
        if roles.len() > self.limits.management_auth_max_scopes {
            return Err(Error::Authentication);
        }
        let mut global = false;
        let mut role_bytes = 0usize;
        for role in roles {
            let role = role
                .as_str()
                .filter(|s| valid_name(s, 64))
                .ok_or(Error::Authentication)?;
            role_bytes = role_bytes
                .checked_add(role.len())
                .ok_or(Error::Authentication)?;
            if role_bytes > self.limits.management_auth_max_scope_bytes {
                return Err(Error::Authentication);
            }
            if self.config.global_roles.iter().any(|name| name == role) {
                global = true;
            }
            if let Some(mapped) = self.config.role_scopes.get(role) {
                scopes.extend(mapped.iter().cloned());
            }
        }
        scopes.sort_unstable();
        scopes.dedup();
        let scopes = ScopeSet::parse(&scopes, &self.limits).map_err(|_| Error::Authentication)?;
        let tenant_values = object
            .get(&self.config.tenant_claim)
            .and_then(serde_json::Value::as_array);
        if object.contains_key(&self.config.tenant_claim) && tenant_values.is_none() {
            return Err(Error::Authentication);
        }
        let tenants = tenant_values.map(Vec::as_slice).unwrap_or_default();
        if tenants.len() > self.limits.management_auth_max_resource_entries {
            return Err(Error::Authentication);
        }
        let mut resources = AdminResourceConfig::default();
        for tenant in tenants {
            resources.tenants.push(
                TenantId::new(tenant.as_str().ok_or(Error::Authentication)?)
                    .map_err(|_| Error::Authentication)?,
            );
        }
        let resource_scope = AdminResourceScope::from_config(&resources, global, &self.limits)
            .map_err(|_| Error::Authentication)?;
        let exp_seconds = object
            .get("exp")
            .and_then(serde_json::Value::as_i64)
            .ok_or(Error::Authentication)?;
        Ok(AdminPrincipal {
            subject: subject.to_owned(),
            auth_method: AdminAuthMethod::Jwt,
            scopes,
            resource_scope,
            expires_at: Some(
                exp_seconds
                    .checked_mul(1_000)
                    .ok_or(Error::Authentication)?,
            ),
            credential_id: None,
            auth_generation: 0,
        })
    }
}

fn parse_jwks(bytes: &[u8], limits: &Limits) -> Result<HashMap<String, DecodingKey>> {
    if bytes.len() > limits.management_jwks_max_bytes {
        return Err(Error::Unavailable);
    }
    let set: JwkSet = serde_json::from_slice(bytes).map_err(|_| Error::Unavailable)?;
    if set.keys.len() > limits.management_jwks_max_keys {
        return Err(Error::Unavailable);
    }
    let mut keys = HashMap::new();
    for jwk in set.keys {
        if !matches!(
            jwk.algorithm,
            jsonwebtoken::jwk::AlgorithmParameters::RSA(_)
        ) {
            continue;
        }
        if jwk.common.public_key_use.is_some() && jwk.common.key_operations.is_some() {
            continue;
        }
        if jwk
            .common
            .key_algorithm
            .is_some_and(|alg| alg != jsonwebtoken::jwk::KeyAlgorithm::RS256)
        {
            continue;
        }
        if jwk
            .common
            .public_key_use
            .as_ref()
            .is_some_and(|use_| *use_ != jsonwebtoken::jwk::PublicKeyUse::Signature)
        {
            continue;
        }
        if jwk
            .common
            .key_operations
            .as_ref()
            .is_some_and(|ops| !ops.contains(&jsonwebtoken::jwk::KeyOperations::Verify))
        {
            continue;
        }
        let Some(id) = jwk
            .common
            .key_id
            .as_deref()
            .filter(|id| valid_name(id, 128))
        else {
            continue;
        };
        let decoded = DecodingKey::from_jwk(&jwk).map_err(|_| Error::Unavailable)?;
        if keys.insert(id.to_owned(), decoded).is_some() {
            return Err(Error::Unavailable);
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use netbaiot_core::DeviceId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::{TlsAcceptor, rustls};

    fn limits() -> Arc<Limits> {
        Arc::new(Limits::default())
    }
    fn device(tenant: &str, product: &str, name: &str) -> DeviceKey {
        DeviceKey {
            tenant_id: TenantId::new(tenant).unwrap(),
            product_id: ProductId::new(product).unwrap(),
            device_id: DeviceId::new(name).unwrap(),
        }
    }
    #[test]
    fn scope_and_resource_authorization() {
        let scopes = ScopeSet::parse(&["device.command".into()], &limits()).unwrap();
        let resource = AdminResourceScope::from_config(
            &AdminResourceConfig {
                tenants: vec![TenantId::new("a").unwrap()],
                products: vec![AdminProduct {
                    tenant_id: TenantId::new("b").unwrap(),
                    product_id: ProductId::new("p").unwrap(),
                }],
                devices: vec![device("c", "p", "d")],
            },
            false,
            &limits(),
        )
        .unwrap();
        let principal = AdminPrincipal {
            subject: "service:test".into(),
            auth_method: AdminAuthMethod::ApiKey,
            scopes,
            resource_scope: resource,
            expires_at: None,
            credential_id: None,
            auth_generation: 0,
        };
        assert!(principal.require_scope(AdminScope::DeviceCommand).is_ok());
        assert!(principal.require_scope(AdminScope::RuntimeDrain).is_err());
        assert!(
            principal
                .require_device(&device("a", "anything", "d"))
                .is_ok()
        );
        assert!(principal.require_device(&device("b", "p", "any")).is_ok());
        assert!(principal.require_device(&device("c", "p", "d")).is_ok());
        assert!(
            principal
                .require_device(&device("c", "p", "other"))
                .is_err()
        );
        assert!(
            principal
                .require_device(&device("other", "p", "d"))
                .is_err()
        );
        assert!(principal.require_global().is_err());
        assert!(
            AdminPrincipal::bootstrap()
                .require_scope(AdminScope::RuntimeDrain)
                .is_ok()
        );
    }
    #[tokio::test]
    async fn legacy_token_uses_bootstrap_principal_and_strict_routing() {
        let secret = "a".repeat(64);
        let admin = Arc::new(AdminAccess::new(&secret, HashMap::new(), &limits()).unwrap());
        let service =
            ManagementAuthService::new(ManagementAuthConfig::default(), Some(admin), limits())
                .unwrap();
        let principal = service
            .authenticate(Some(&format!("Bearer {secret}")), None)
            .await
            .unwrap();
        assert_eq!(principal.subject, "bootstrap-admin");
        assert!(principal.require_scope(AdminScope::RuntimeDrain).is_ok());
        assert!(principal.require_global().is_ok());
        for bad in [
            "Bearer short",
            "Bearer bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "Bearer gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
            "ApiKey a.b",
        ] {
            assert!(service.authenticate(Some(bad), None).await.is_err());
        }
        assert!(
            service
                .authenticate(Some(&format!("Bearer {secret}")), Some(b"certificate"))
                .await
                .is_err()
        );
    }

    fn api_config() -> ManagementAuthConfig {
        ManagementAuthConfig {
            api_keys: vec![ApiKeyConfig {
                key_id: "backend".into(),
                secret_env: "TEST_SECRET".into(),
                subject: "service:backend".into(),
                scopes: vec!["device.command".into()],
                resources: AdminResourceConfig {
                    tenants: vec![TenantId::new("a").unwrap()],
                    ..Default::default()
                },
                global: false,
                expires_at: None,
                auth_generation: 1,
                enabled: true,
            }],
            ..Default::default()
        }
    }
    #[tokio::test]
    async fn api_key_is_bounded_constant_time_and_restricted() {
        let secret = "a".repeat(64);
        let service =
            ManagementAuthService::new_with_resolver(api_config(), None, limits(), |_| {
                Some(secret.clone())
            })
            .unwrap();
        let header = format!("ApiKey backend.{secret}");
        let principal = service.authenticate(Some(&header), None).await.unwrap();
        assert_eq!(principal.auth_method, AdminAuthMethod::ApiKey);
        assert!(principal.require_device(&device("a", "p", "d")).is_ok());
        assert!(principal.require_device(&device("b", "p", "d")).is_err());
        for bad in [
            "ApiKey backend.bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "ApiKey unknown.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "ApiKey backend.short",
            "ApiKey backend.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaag",
        ] {
            assert!(service.authenticate(Some(bad), None).await.is_err());
        }
        let mut duplicate = api_config();
        duplicate.api_keys.push(duplicate.api_keys[0].clone());
        assert!(
            ManagementAuthService::new_with_resolver(duplicate, None, limits(), |_| Some(
                secret.clone()
            ))
            .is_err()
        );
        let mut disabled = api_config();
        disabled.api_keys[0].enabled = false;
        let service = ManagementAuthService::new_with_resolver(disabled, None, limits(), |_| {
            Some(secret.clone())
        })
        .unwrap();
        assert!(service.authenticate(Some(&header), None).await.is_err());
        let mut expired = api_config();
        expired.api_keys[0].expires_at = Some(1);
        let service = ManagementAuthService::new_with_resolver(expired, None, limits(), |_| {
            Some(secret.clone())
        })
        .unwrap();
        assert!(service.authenticate(Some(&header), None).await.is_err());
    }

    #[tokio::test]
    async fn mtls_identity_requires_an_exact_configured_fingerprint() {
        let certificate = b"trusted verified leaf certificate";
        let fingerprint = Sha256::digest(certificate)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let config = ManagementAuthConfig {
            mtls_identities: vec![MtlsIdentityConfig {
                certificate_sha256: fingerprint,
                subject: "service:test".into(),
                scopes: vec!["runtime.read".into()],
                resources: AdminResourceConfig::default(),
                global: true,
            }],
            ..Default::default()
        };
        let service = ManagementAuthService::new(config, None, limits()).unwrap();
        assert_eq!(
            service
                .authenticate(None, Some(certificate))
                .await
                .unwrap()
                .auth_method,
            AdminAuthMethod::Mtls
        );
        assert!(service.authenticate(None, Some(b"unmapped")).await.is_err());
        assert!(
            service
                .authenticate(Some("Bearer malformed"), Some(certificate))
                .await
                .is_err()
        );
    }

    #[test]
    fn jwks_parser_has_byte_and_key_count_ceilings() {
        let mut limits = Limits::default();
        assert!(parse_jwks(br#"{"keys":[]}"#, &limits).unwrap().is_empty());
        limits.management_jwks_max_bytes = 4;
        assert!(parse_jwks(br#"{"keys":[]}"#, &limits).is_err());
        limits.management_jwks_max_bytes = 65_536;
        limits.management_jwks_max_keys = 1;
        assert!(parse_jwks(br#"{"keys":[{"kty":"oct","k":"YWJj","kid":"a"},{"kty":"oct","k":"YWJj","kid":"b"}]}"#, &limits).is_err());
        assert!(parse_jwks(b"not-json", &limits).is_err());
    }

    #[test]
    fn duplicate_jwt_role_and_insecure_jwks_configuration_fail_startup() {
        let duplicate = r#"{"jwt":{"issuer":"https://issuer.example","audience":"iot","jwks_url":"https://issuer.example/jwks","role_scopes":{"operator":[],"operator":[]}}}"#;
        assert!(serde_json::from_str::<ManagementAuthConfig>(duplicate).is_err());
        let mut config = jwt_config();
        config.jwks_url = "http://issuer.example/jwks".into();
        assert!(JwtProvider::new(config, limits()).is_err());
        let service =
            ManagementAuthService::new(ManagementAuthConfig::default(), None, limits()).unwrap();
        assert!(!service.has_provider());
    }

    fn jwt_config() -> JwtConfig {
        JwtConfig {
            issuer: "https://issuer.example".into(),
            audience: "netbaiot".into(),
            jwks_url: "https://issuer.example/jwks".into(),
            subject_claim: default_sub(),
            scope_claim: default_scope(),
            roles_claim: default_roles(),
            tenant_claim: default_tenants(),
            role_scopes: HashMap::from([("operator".into(), vec!["device.command".into()])]),
            global_roles: vec![],
        }
    }

    async fn jwks_https_server() -> (
        std::net::SocketAddr,
        Arc<Mutex<Option<Vec<u8>>>>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let cert = include_bytes!("../../../tests/fixtures/localhost-cert.pem");
        let key = include_bytes!("../../../tests/fixtures/localhost-key.pem");
        let certificates = rustls_pemfile::certs(&mut cert.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let private = rustls_pemfile::private_key(&mut key.as_slice())
            .unwrap()
            .unwrap();
        let server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certificates, private)
        .unwrap();
        let tls = TlsAcceptor::from(Arc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = Arc::new(Mutex::new(Some(
            include_bytes!("../../../tests/fixtures/localhost-jwks.json").to_vec(),
        )));
        let requests = Arc::new(AtomicUsize::new(0));
        let body_for_task = body.clone();
        let requests_for_task = requests.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let Ok(mut stream) = tls.accept(socket).await else {
                    continue;
                };
                let mut bytes = [0u8; 4_096];
                let mut len = 0usize;
                while let Ok(read) = stream.read(&mut bytes[len..]).await {
                    if read == 0 {
                        break;
                    }
                    len += read;
                    if bytes[..len].windows(4).any(|part| part == b"\r\n\r\n") || len == bytes.len()
                    {
                        break;
                    }
                }
                requests_for_task.fetch_add(1, Ordering::SeqCst);
                let response_body = body_for_task.lock().await.clone();
                if let Some(response_body) = response_body {
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response_body.len()
                    );
                    if stream.write_all(header.as_bytes()).await.is_ok() {
                        let _ = stream.write_all(&response_body).await;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        });
        (address, body, requests, task)
    }

    #[tokio::test]
    async fn jwks_https_cache_outage_oversize_timeout_and_refresh_rate_limit() {
        let (address, body, requests, server) = jwks_https_server().await;
        let mut config = jwt_config();
        config.jwks_url = format!("https://localhost:{}/jwks", address.port());
        let mut provider = JwtProvider::new(config, limits()).unwrap();
        provider.client = reqwest::Client::builder()
            .no_proxy()
            .add_root_certificate(
                reqwest::Certificate::from_pem(include_bytes!(
                    "../../../tests/fixtures/localhost-cert.pem"
                ))
                .unwrap(),
            )
            .connect_timeout(Duration::from_millis(200))
            .timeout(Duration::from_millis(200))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let now = crate::now_ms() / 1_000;
        let claims = serde_json::json!({"iss":"https://issuer.example","aud":"netbaiot","sub":"user-1","exp":now+60,"nbf":now-1,"scope":"runtime.read","tenants":["a"]});
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test".into());
        let private =
            EncodingKey::from_rsa_pem(include_bytes!("../../../tests/fixtures/localhost-key.pem"))
                .unwrap();
        let token = encode(&header, &claims, &private).unwrap();
        assert!(provider.authenticate(&token).await.is_ok());
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        *body.lock().await = Some(vec![b'x'; limits().management_jwks_max_bytes + 1]);
        assert!(provider.authenticate(&token).await.is_ok());
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        {
            let mut cache = provider.cache.lock().await;
            cache.expires = Some(Instant::now() - Duration::from_millis(1));
            cache.last_refresh = Some(Instant::now() - Duration::from_secs(31));
        }
        assert!(provider.authenticate(&token).await.is_err());
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        *body.lock().await = None;
        provider.cache.lock().await.last_refresh = Some(Instant::now() - Duration::from_secs(31));
        assert!(provider.authenticate(&token).await.is_err());
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        for index in 0..100 {
            assert!(provider.key(&format!("unknown-{index}")).await.is_err());
        }
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn jwks_refresh_is_single_flight_and_cancellation_releases_lock() {
        let (address, body, requests, server) = jwks_https_server().await;
        *body.lock().await = None;
        let mut config = jwt_config();
        config.jwks_url = format!("https://localhost:{}/jwks", address.port());
        let mut provider = JwtProvider::new(config, limits()).unwrap();
        provider.client = reqwest::Client::builder()
            .no_proxy()
            .add_root_certificate(
                reqwest::Certificate::from_pem(include_bytes!(
                    "../../../tests/fixtures/localhost-cert.pem"
                ))
                .unwrap(),
            )
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let provider = Arc::new(provider);
        let now = crate::now_ms() / 1_000;
        let claims = serde_json::json!({"iss":"https://issuer.example","aud":"netbaiot","sub":"user-1","exp":now+60,"nbf":now-1,"scope":"runtime.read","tenants":["a"]});
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test".into());
        let private =
            EncodingKey::from_rsa_pem(include_bytes!("../../../tests/fixtures/localhost-key.pem"))
                .unwrap();
        let token = encode(&header, &claims, &private).unwrap();
        let leader = {
            let provider = provider.clone();
            let token = token.clone();
            tokio::spawn(async move { provider.authenticate(&token).await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while requests.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        leader.abort();
        let _ = leader.await;
        assert!(provider.refresh.try_lock().is_ok());
        *body.lock().await =
            Some(include_bytes!("../../../tests/fixtures/localhost-jwks.json").to_vec());
        provider.cache.lock().await.last_refresh = Some(Instant::now() - Duration::from_secs(31));
        let barrier = Arc::new(tokio::sync::Barrier::new(17));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let provider = provider.clone();
            let token = token.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                provider.authenticate(&token).await
            }));
        }
        barrier.wait().await;
        let mut successes = 0usize;
        for task in tasks {
            if task.await.unwrap().is_ok() {
                successes += 1;
            }
        }
        assert!(successes >= 1);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        server.abort();
    }
    #[tokio::test]
    async fn jwt_requires_signature_algorithm_issuer_audience_expiry_and_nbf() {
        let provider = JwtProvider::new(jwt_config(), limits()).unwrap();
        provider.cache.lock().await.keys = parse_jwks(
            include_bytes!("../../../tests/fixtures/localhost-jwks.json"),
            &limits(),
        )
        .unwrap();
        provider.cache.lock().await.expires = Some(Instant::now() + Duration::from_secs(60));
        let now = crate::now_ms() / 1000;
        let base = serde_json::json!({"iss":"https://issuer.example","aud":"netbaiot","sub":"user-1","exp":now+60,"nbf":now-1,"scope":"runtime.read","roles":["operator"],"tenants":["a"]});
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test".into());
        let private =
            EncodingKey::from_rsa_pem(include_bytes!("../../../tests/fixtures/localhost-key.pem"))
                .unwrap();
        let token = encode(&header, &base, &private).unwrap();
        let principal = provider.authenticate(&token).await.unwrap();
        assert!(principal.require_scope(AdminScope::DeviceCommand).is_ok());
        assert!(principal.require_device(&device("a", "p", "d")).is_ok());
        assert!(principal.require_device(&device("b", "p", "d")).is_err());
        let mut over_scopes = base.clone();
        over_scopes["scope"] = serde_json::json!("runtime.read ".repeat(80));
        assert!(
            provider
                .authenticate(&encode(&header, &over_scopes, &private).unwrap())
                .await
                .is_err()
        );
        let mut over_tenants = base.clone();
        over_tenants["tenants"] = serde_json::json!(vec!["a"; 129]);
        assert!(
            provider
                .authenticate(&encode(&header, &over_tenants, &private).unwrap())
                .await
                .is_err()
        );
        for (field, value) in [
            ("iss", serde_json::json!("wrong")),
            ("aud", serde_json::json!("wrong")),
            ("exp", serde_json::json!(now - 1)),
            ("nbf", serde_json::json!(now + 60)),
        ] {
            let mut claims = base.clone();
            claims[field] = value;
            assert!(
                provider
                    .authenticate(&encode(&header, &claims, &private).unwrap())
                    .await
                    .is_err()
            );
        }
        let mut unknown = header.clone();
        unknown.kid = Some("unknown".into());
        provider.cache.lock().await.last_refresh = Some(Instant::now());
        assert!(
            provider
                .authenticate(&encode(&unknown, &base, &private).unwrap())
                .await
                .is_err()
        );
        for index in 0..1_000 {
            assert!(provider.key(&format!("random-{index}")).await.is_err());
        }
        let mut none = Header::new(Algorithm::HS256);
        none.kid = Some("test".into());
        assert!(
            provider
                .authenticate(
                    &encode(
                        &none,
                        &base,
                        &jsonwebtoken::EncodingKey::from_secret(b"secret")
                    )
                    .unwrap()
                )
                .await
                .is_err()
        );
        let mut tampered = token.into_bytes();
        if let Some(last) = tampered.last_mut() {
            *last = if *last == b'A' { b'B' } else { b'A' };
        }
        assert!(
            provider
                .authenticate(&String::from_utf8(tampered).unwrap())
                .await
                .is_err()
        );
    }
}
