use crate::*;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_core::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;

/// Provisioned high-entropy 256-bit key. Deliberately no Debug implementation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub credential_id: String,
    pub secret_hex: String,
    pub identity: AuthenticatedDevice,
}
pub enum AuthenticationRequest<'a> {
    Secret {
        credential_id: &'a str,
        secret: &'a [u8],
    },
    Signed {
        credential_id: &'a str,
        message: &'a [u8],
        tag: &'a [u8],
    },
}
#[async_trait]
pub trait DeviceAuthenticator: Send + Sync {
    async fn authenticate(&self, request: AuthenticationRequest<'_>)
    -> Result<AuthenticatedDevice>;
}

#[derive(Clone, Eq)]
struct AuthCacheKey {
    credential_id: Arc<str>,
    fingerprint: [u8; 32],
    signed: bool,
}

impl PartialEq for AuthCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.credential_id == other.credential_id
            && self.fingerprint == other.fingerprint
            && self.signed == other.signed
    }
}
impl Hash for AuthCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.credential_id.hash(state);
        self.fingerprint.hash(state);
        self.signed.hash(state);
    }
}

#[derive(Clone)]
enum CachedAuth {
    Positive(AuthenticatedDevice),
    Negative,
}

struct CacheEntry {
    value: CachedAuth,
    expires: Instant,
    bytes: usize,
}

struct AuthCacheState {
    entries: HashMap<AuthCacheKey, CacheEntry>,
    order: VecDeque<AuthCacheKey>,
    bytes: usize,
    inflight: HashMap<AuthCacheKey, tokio::sync::watch::Sender<bool>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum AuthInvalidation {
    Device {
        device: DeviceKey,
    },
    Product {
        tenant_id: TenantId,
        product_id: ProductId,
    },
    Tenant {
        tenant_id: TenantId,
    },
    CredentialVersion {
        version: u32,
    },
    AuthGeneration {
        generation: u64,
    },
    All,
}

/// Bounded positive/negative cache with coalesced identical misses.
pub struct AuthCache {
    provider: Arc<dyn DeviceAuthenticator>,
    limits: Arc<Limits>,
    metrics: Arc<Metrics>,
    state: Mutex<AuthCacheState>,
    waiters: Arc<tokio::sync::Semaphore>,
}

impl AuthCache {
    pub fn new(
        provider: Arc<dyn DeviceAuthenticator>,
        limits: Arc<Limits>,
        metrics: Arc<Metrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            provider,
            waiters: Arc::new(tokio::sync::Semaphore::new(limits.auth_cache_max_waiters)),
            limits,
            metrics,
            state: Mutex::new(AuthCacheState {
                entries: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                inflight: HashMap::new(),
            }),
        })
    }

    pub async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        let key = cache_key(&request)?;
        loop {
            let follower = {
                let mut state = lock(&self.state)?;
                prune_expired(&mut state);
                if let Some(entry) = state.entries.get(&key) {
                    match &entry.value {
                        CachedAuth::Positive(auth) => {
                            self.metrics.inc(Metric::AuthCacheHits);
                            return Ok(auth.clone());
                        }
                        CachedAuth::Negative => {
                            self.metrics.inc(Metric::AuthNegativeHits);
                            return Err(Error::Authentication);
                        }
                    }
                }
                if let Some(completed) = state.inflight.get(&key) {
                    Some(completed.subscribe())
                } else {
                    let (completed, _) = tokio::sync::watch::channel(false);
                    state.inflight.insert(key.clone(), completed);
                    None
                }
            };
            if let Some(mut completed) = follower {
                let _waiter = self
                    .waiters
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Overloaded)?;
                tokio::time::timeout(
                    Duration::from_millis(self.limits.authentication_timeout_ms),
                    async {
                        if !*completed.borrow() {
                            completed.changed().await.map_err(|_| Error::Internal)?;
                        }
                        Ok::<(), Error>(())
                    },
                )
                .await
                .map_err(|_| Error::Timeout)??;
                continue;
            }
            self.metrics.inc(Metric::AuthCacheMisses);
            let result = deadline(
                self.limits.authentication_timeout_ms,
                self.provider.authenticate(request),
            )
            .await;
            let mut state = lock(&self.state)?;
            let completed = state.inflight.remove(&key).ok_or(Error::Internal)?;
            let value = match &result {
                Ok(auth) => CachedAuth::Positive(auth.clone()),
                Err(Error::Authentication | Error::Forbidden) => CachedAuth::Negative,
                Err(_) => {
                    let _ = completed.send(true);
                    return result;
                }
            };
            let ttl = match value {
                CachedAuth::Positive(_) => self.limits.auth_positive_ttl_ms,
                CachedAuth::Negative => self.limits.auth_negative_ttl_ms,
            };
            let bytes = key.credential_id.len()
                + std::mem::size_of::<AuthCacheKey>()
                + match &value {
                    CachedAuth::Positive(auth) => {
                        auth.device_key.tenant_id.as_str().len()
                            + auth.device_key.product_id.as_str().len()
                            + auth.device_key.device_id.as_str().len()
                            + auth.codec_id.as_str().len()
                            + 64
                    }
                    CachedAuth::Negative => 1,
                };
            while (state.entries.len() >= self.limits.auth_cache_max_entries
                || state.bytes.saturating_add(bytes) > self.limits.auth_cache_max_bytes)
                && !state.entries.is_empty()
            {
                if let Some(old) = state.order.pop_front()
                    && let Some(entry) = state.entries.remove(&old)
                {
                    state.bytes = state.bytes.saturating_sub(entry.bytes);
                    self.metrics.inc(Metric::AuthEvictions);
                }
            }
            if bytes <= self.limits.auth_cache_max_bytes {
                state.bytes += bytes;
                state.order.push_back(key.clone());
                state.entries.insert(
                    key,
                    CacheEntry {
                        value,
                        expires: Instant::now() + Duration::from_millis(ttl),
                        bytes,
                    },
                );
            }
            let _ = completed.send(true);
            return result;
        }
    }

    pub fn invalidate(&self, invalidation: &AuthInvalidation) -> Result<Vec<DeviceKey>> {
        let mut state = lock(&self.state)?;
        let mut devices = Vec::new();
        state.entries.retain(|_, entry| {
            let CachedAuth::Positive(auth) = &entry.value else {
                return !matches!(invalidation, AuthInvalidation::All);
            };
            let remove = match invalidation {
                AuthInvalidation::Device { device } => &auth.device_key == device,
                AuthInvalidation::Product {
                    tenant_id,
                    product_id,
                } => {
                    &auth.device_key.tenant_id == tenant_id
                        && &auth.device_key.product_id == product_id
                }
                AuthInvalidation::Tenant { tenant_id } => &auth.device_key.tenant_id == tenant_id,
                AuthInvalidation::CredentialVersion { version } => {
                    auth.credential_version == *version
                }
                AuthInvalidation::AuthGeneration { generation } => {
                    auth.auth_generation == *generation
                }
                AuthInvalidation::All => true,
            };
            if remove {
                devices.push(auth.device_key.clone());
            }
            !remove
        });
        state.bytes = state.entries.values().map(|entry| entry.bytes).sum();
        let live = state
            .entries
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        state.order.retain(|key| live.contains(key));
        self.metrics.inc(Metric::AuthInvalidations);
        Ok(devices)
    }

    pub fn usage(&self) -> Result<(usize, usize)> {
        let state = lock(&self.state)?;
        Ok((state.entries.len(), state.bytes))
    }
}

fn cache_key(request: &AuthenticationRequest<'_>) -> Result<AuthCacheKey> {
    let (credential_id, fingerprint, signed) = match request {
        AuthenticationRequest::Secret {
            credential_id,
            secret,
        } => (*credential_id, Sha256::digest(secret).into(), false),
        AuthenticationRequest::Signed {
            credential_id,
            message,
            tag,
        } => {
            let mut digest = Sha256::new();
            digest.update(message);
            digest.update(tag);
            (*credential_id, digest.finalize().into(), true)
        }
    };
    if credential_id.is_empty() || credential_id.len() > 64 {
        return Err(Error::Authentication);
    }
    Ok(AuthCacheKey {
        credential_id: Arc::from(credential_id),
        fingerprint,
        signed,
    })
}

fn prune_expired(state: &mut AuthCacheState) {
    let now = Instant::now();
    state.entries.retain(|_, entry| entry.expires > now);
    state.bytes = state.entries.values().map(|entry| entry.bytes).sum();
    let live = state
        .entries
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    state.order.retain(|key| live.contains(key));
}
struct Entry {
    key: [u8; 32],
    secret_hash: [u8; 32],
    identity: AuthenticatedDevice,
}
pub struct StaticAuthenticator {
    credentials: HashMap<String, Entry>,
}
pub fn decode_hex(s: &str) -> Result<Vec<u8>> {
    if s.len() > 256 || !s.len().is_multiple_of(2) {
        return Err(Error::Invalid);
    }
    s.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let a = (pair[0] as char).to_digit(16).ok_or(Error::Invalid)?;
            let b = (pair[1] as char).to_digit(16).ok_or(Error::Invalid)?;
            Ok((a * 16 + b) as u8)
        })
        .collect()
}
pub fn encode_hex(s: &[u8]) -> String {
    s.iter().map(|b| format!("{b:02x}")).collect()
}
impl StaticAuthenticator {
    pub fn new(credentials: Vec<Credential>, limits: &Limits) -> Result<Arc<Self>> {
        if credentials.is_empty() || credentials.len() > limits.max_devices {
            return Err(Error::Configuration);
        }
        let mut entries = HashMap::new();
        let mut devices = std::collections::HashSet::new();
        let mut tenants = HashMap::<TenantId, usize>::new();
        for credential in credentials {
            if DeviceId::new(&credential.credential_id).is_err()
                || credential.secret_hex.len() != 64
                || credential.identity.credential_version == 0
                || !devices.insert(credential.identity.device_key.clone())
            {
                return Err(Error::Configuration);
            }
            let count = tenants
                .entry(credential.identity.device_key.tenant_id.clone())
                .or_default();
            *count += 1;
            if *count > limits.max_devices_per_tenant {
                return Err(Error::Configuration);
            }
            let key: [u8; 32] = decode_hex(&credential.secret_hex)?
                .try_into()
                .map_err(|_| Error::Configuration)?;
            let secret_hash = Sha256::digest(credential.secret_hex.as_bytes()).into();
            if entries
                .insert(
                    credential.credential_id,
                    Entry {
                        key,
                        secret_hash,
                        identity: credential.identity,
                    },
                )
                .is_some()
            {
                return Err(Error::Configuration);
            }
        }
        Ok(Arc::new(Self {
            credentials: entries,
        }))
    }
}
#[async_trait]
impl DeviceAuthenticator for StaticAuthenticator {
    async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        let id = match request {
            AuthenticationRequest::Secret { credential_id, .. }
            | AuthenticationRequest::Signed { credential_id, .. } => credential_id,
        };
        let entry = self.credentials.get(id).ok_or(Error::Authentication)?;
        match request {
            AuthenticationRequest::Secret { secret, .. } => {
                if secret.len() != 64
                    || !bool::from(entry.secret_hash.ct_eq(&Sha256::digest(secret)))
                {
                    return Err(Error::Authentication);
                }
            }
            AuthenticationRequest::Signed { message, tag, .. } => {
                let mut mac = Hmac::<Sha256>::new_from_slice(&entry.key)
                    .map_err(|_| Error::Authentication)?;
                mac.update(message);
                mac.verify_slice(tag).map_err(|_| Error::Authentication)?;
            }
        }
        Ok(entry.identity.clone())
    }
}

/// Separate business API authorization; device secrets cannot enqueue commands.
pub struct AdminAccess {
    hash: [u8; 32],
    identities: HashMap<DeviceKey, AuthenticatedDevice>,
}
impl AdminAccess {
    pub fn new(
        secret: &str,
        identities: HashMap<DeviceKey, AuthenticatedDevice>,
        limits: &Limits,
    ) -> Result<Self> {
        if secret.len() != 64
            || decode_hex(secret)?.len() != 32
            || identities.len() > limits.max_devices
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            hash: Sha256::digest(secret.as_bytes()).into(),
            identities,
        })
    }
    pub fn verify(&self, secret: &[u8]) -> Result<()> {
        if secret.len() == 64 && bool::from(self.hash.ct_eq(&Sha256::digest(secret))) {
            Ok(())
        } else {
            Err(Error::Authentication)
        }
    }
    pub fn identity(&self, device: &DeviceKey) -> Option<&AuthenticatedDevice> {
        self.identities.get(device)
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    struct Provider {
        calls: AtomicUsize,
        mode: AtomicU8,
        identity: AuthenticatedDevice,
    }

    #[async_trait]
    impl DeviceAuthenticator for Provider {
        async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.mode.load(Ordering::Relaxed) {
                0 => Ok(self.identity.clone()),
                1 => Err(Error::Authentication),
                _ => Err(Error::Unavailable),
            }
        }
    }

    fn identity() -> AuthenticatedDevice {
        AuthenticatedDevice {
            device_key: DeviceKey {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new("d").unwrap(),
            },
            credential_version: 3,
            auth_generation: 7,
            codec_id: CodecId::new("json").unwrap(),
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        }
    }

    fn request<'a>(id: &'a str) -> AuthenticationRequest<'a> {
        AuthenticationRequest::Secret {
            credential_id: id,
            secret: b"secret",
        }
    }

    #[tokio::test]
    async fn positive_negative_ttl_capacity_invalidation_and_fail_closed() {
        let provider = Arc::new(Provider {
            calls: AtomicUsize::new(0),
            mode: AtomicU8::new(0),
            identity: identity(),
        });
        let limits = Arc::new(Limits {
            auth_cache_max_entries: 1,
            auth_cache_max_bytes: 4_096,
            auth_positive_ttl_ms: 5,
            auth_negative_ttl_ms: 2,
            ..Limits::default()
        });
        let cache = AuthCache::new(provider.clone(), limits, Arc::new(Metrics::default()));
        cache.authenticate(request("a")).await.unwrap();
        provider.mode.store(2, Ordering::Relaxed);
        cache.authenticate(request("a")).await.unwrap();
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            cache.authenticate(request("miss")).await,
            Err(Error::Unavailable)
        ));

        provider.mode.store(1, Ordering::Relaxed);
        assert!(matches!(
            cache.authenticate(request("bad")).await,
            Err(Error::Authentication)
        ));
        assert!(matches!(
            cache.authenticate(request("bad")).await,
            Err(Error::Authentication)
        ));
        let after_negative = provider.calls.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert!(matches!(
            cache.authenticate(request("bad")).await,
            Err(Error::Authentication)
        ));
        assert_eq!(provider.calls.load(Ordering::Relaxed), after_negative + 1);

        provider.mode.store(0, Ordering::Relaxed);
        cache.authenticate(request("one")).await.unwrap();
        cache.authenticate(request("two")).await.unwrap();
        let after_eviction = provider.calls.load(Ordering::Relaxed);
        cache.authenticate(request("one")).await.unwrap();
        assert_eq!(provider.calls.load(Ordering::Relaxed), after_eviction + 1);
        let invalidated = cache
            .invalidate(&AuthInvalidation::AuthGeneration { generation: 7 })
            .unwrap();
        assert_eq!(invalidated, vec![identity().device_key]);
        tokio::time::sleep(Duration::from_millis(6)).await;
        cache.authenticate(request("one")).await.unwrap();
    }
}
