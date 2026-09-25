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
}

#[derive(Clone)]
pub struct AuthenticatedSessionCandidate {
    pub auth: AuthenticatedDevice,
    epoch: u64,
}

/// A single verified request and its invalidation fence. Never exposes key material.
pub struct VerifiedDatagram {
    verifier: DeviceVerifier,
    epoch: u64,
}

impl VerifiedDatagram {
    pub fn identity(&self) -> &AuthenticatedDevice {
        self.verifier.identity()
    }
}

/// Identity plus HMAC verification material cached by the gateway. The key is
/// intentionally opaque and this type does not implement `Debug` or serialization.
#[derive(Clone)]
pub struct DeviceVerifier {
    identity: AuthenticatedDevice,
    key: [u8; 32],
}

impl DeviceVerifier {
    pub fn new(identity: AuthenticatedDevice, key: [u8; 32]) -> Self {
        Self { identity, key }
    }

    pub fn identity(&self) -> &AuthenticatedDevice {
        &self.identity
    }

    pub fn sign(&self, message: &[u8]) -> Result<[u8; 32]> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).map_err(|_| Error::Authentication)?;
        mac.update(message);
        Ok(mac.finalize().into_bytes().into())
    }

    pub fn verify(&self, message: &[u8], tag: &[u8]) -> Result<()> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).map_err(|_| Error::Authentication)?;
        mac.update(message);
        mac.verify_slice(tag).map_err(|_| Error::Authentication)
    }
}

#[async_trait]
pub trait DeviceAuthenticator: Send + Sync {
    async fn authenticate(&self, request: AuthenticationRequest<'_>)
    -> Result<AuthenticatedDevice>;

    /// Resolve stable verifier material once; UDP signatures are verified locally
    /// for every datagram after this bounded lookup.
    async fn resolve_verifier(&self, credential_id: &str) -> Result<DeviceVerifier>;
}

#[derive(Clone, Eq)]
struct AuthCacheKey {
    credential_id: Arc<str>,
    fingerprint: [u8; 32],
    verifier: bool,
}

impl PartialEq for AuthCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.credential_id == other.credential_id
            && self.fingerprint == other.fingerprint
            && self.verifier == other.verifier
    }
}
impl Hash for AuthCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.credential_id.hash(state);
        self.fingerprint.hash(state);
        self.verifier.hash(state);
    }
}

#[derive(Clone)]
enum CachedAuth {
    Positive(AuthenticatedDevice),
    Verifier(DeviceVerifier),
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
    inflight: HashMap<AuthCacheKey, Inflight>,
    epoch: u64,
    next_inflight_id: u64,
}

struct Inflight {
    completed: tokio::sync::watch::Sender<bool>,
    epoch: u64,
    id: u64,
}

struct InflightLeader<'a> {
    cache: &'a AuthCache,
    key: Option<AuthCacheKey>,
    id: u64,
}

impl Drop for InflightLeader<'_> {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else { return };
        let Ok(mut state) = self.cache.state.lock() else {
            return;
        };
        if state
            .inflight
            .get(&key)
            .is_some_and(|entry| entry.id == self.id)
            && let Some(entry) = state.inflight.remove(&key)
        {
            let _ = entry.completed.send(true);
        }
    }
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
                epoch: 1,
                next_inflight_id: 1,
            }),
        })
    }

    pub async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        Ok(self.authenticate_candidate(request).await?.auth)
    }

    pub async fn authenticate_candidate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedSessionCandidate> {
        let key = cache_key(&request)?;
        loop {
            let follower = {
                let mut state = lock(&self.state)?;
                prune_expired(&mut state);
                if let Some(entry) = state.entries.get(&key) {
                    match &entry.value {
                        CachedAuth::Positive(auth) => {
                            self.metrics.inc(Metric::AuthCacheHits);
                            return Ok(AuthenticatedSessionCandidate {
                                auth: auth.clone(),
                                epoch: state.epoch,
                            });
                        }
                        CachedAuth::Negative => {
                            self.metrics.inc(Metric::AuthNegativeHits);
                            return Err(Error::Authentication);
                        }
                        CachedAuth::Verifier(_) => return Err(Error::Internal),
                    }
                }
                if let Some(inflight) = state.inflight.get(&key) {
                    Some(inflight.completed.subscribe())
                } else {
                    let (completed, _) = tokio::sync::watch::channel(false);
                    let id = state.next_inflight_id;
                    state.next_inflight_id = state.next_inflight_id.wrapping_add(1).max(1);
                    let epoch = state.epoch;
                    state.inflight.insert(
                        key.clone(),
                        Inflight {
                            completed,
                            epoch,
                            id,
                        },
                    );
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
                            // A closed channel means the leader was cancelled or
                            // panicked. Loop and elect a replacement leader.
                            let _ = completed.changed().await;
                        }
                        Ok::<(), Error>(())
                    },
                )
                .await
                .map_err(|_| Error::Timeout)??;
                continue;
            }
            self.metrics.inc(Metric::AuthCacheMisses);
            let (leader_id, leader_epoch) = {
                let state = lock(&self.state)?;
                let inflight = state.inflight.get(&key).ok_or(Error::Internal)?;
                (inflight.id, inflight.epoch)
            };
            let mut leader = InflightLeader {
                cache: self,
                key: Some(key.clone()),
                id: leader_id,
            };
            let result = deadline(
                self.limits.authentication_timeout_ms,
                self.provider.authenticate(request),
            )
            .await;
            let mut state = lock(&self.state)?;
            let completed = state.inflight.remove(&key).ok_or(Error::Internal)?;
            leader.key = None;
            if state.epoch != leader_epoch {
                let _ = completed.completed.send(true);
                self.metrics.inc(Metric::AuthStaleResponses);
                return Err(Error::Unavailable);
            }
            let value = match &result {
                Ok(auth) => CachedAuth::Positive(auth.clone()),
                Err(Error::Authentication | Error::Forbidden) => CachedAuth::Negative,
                Err(_) => {
                    let _ = completed.completed.send(true);
                    return result.map(|auth| AuthenticatedSessionCandidate {
                        auth,
                        epoch: leader_epoch,
                    });
                }
            };
            let ttl = match value {
                CachedAuth::Positive(_) => self.limits.auth_positive_ttl_ms,
                CachedAuth::Negative => self.limits.auth_negative_ttl_ms,
                CachedAuth::Verifier(_) => return Err(Error::Internal),
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
                    CachedAuth::Verifier(verifier) => {
                        verifier.identity.device_key.tenant_id.as_str().len()
                            + verifier.identity.device_key.product_id.as_str().len()
                            + verifier.identity.device_key.device_id.as_str().len()
                            + verifier.identity.codec_id.as_str().len()
                            + 96
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
            let _ = completed.completed.send(true);
            return result.map(|auth| AuthenticatedSessionCandidate {
                auth,
                epoch: state.epoch,
            });
        }
    }

    pub fn candidate_is_current(&self, candidate: &AuthenticatedSessionCandidate) -> Result<bool> {
        Ok(lock(&self.state)?.epoch == candidate.epoch)
    }

    pub async fn verify_signed(
        &self,
        credential_id: &str,
        message: &[u8],
        tag: &[u8],
    ) -> Result<AuthenticatedDevice> {
        Ok(self
            .verify_signed_with_verifier(credential_id, message, tag)
            .await?
            .identity()
            .clone())
    }

    /// Finish a verified request synchronously, fenced against invalidation. The callback
    /// runs under the cache lock: it must not block, await, or reenter the auth cache.
    /// Unrelated invalidations conservatively fence outstanding datagrams too.
    pub fn with_current_verifier<T>(
        &self,
        verified: &VerifiedDatagram,
        finish: impl FnOnce(&DeviceVerifier) -> Result<T>,
    ) -> Result<T> {
        let state = lock(&self.state)?;
        if state.epoch != verified.epoch {
            self.metrics.inc(Metric::AuthStaleResponses);
            return Err(Error::Unavailable);
        }
        finish(&verified.verifier)
    }

    pub async fn verify_signed_with_verifier(
        &self,
        credential_id: &str,
        message: &[u8],
        tag: &[u8],
    ) -> Result<VerifiedDatagram> {
        let key = verifier_cache_key(credential_id)?;
        loop {
            let follower = {
                let mut state = lock(&self.state)?;
                prune_expired(&mut state);
                if let Some(entry) = state.entries.get(&key) {
                    match &entry.value {
                        CachedAuth::Verifier(verifier) => {
                            verifier.verify(message, tag)?;
                            self.metrics.inc(Metric::AuthCacheHits);
                            return Ok(VerifiedDatagram {
                                verifier: verifier.clone(),
                                epoch: state.epoch,
                            });
                        }
                        CachedAuth::Negative => {
                            self.metrics.inc(Metric::AuthNegativeHits);
                            return Err(Error::Authentication);
                        }
                        CachedAuth::Positive(_) => return Err(Error::Internal),
                    }
                }
                if let Some(inflight) = state.inflight.get(&key) {
                    Some(inflight.completed.subscribe())
                } else {
                    let (completed, _) = tokio::sync::watch::channel(false);
                    let id = state.next_inflight_id;
                    state.next_inflight_id = state.next_inflight_id.wrapping_add(1).max(1);
                    let epoch = state.epoch;
                    state.inflight.insert(
                        key.clone(),
                        Inflight {
                            completed,
                            epoch,
                            id,
                        },
                    );
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
                            let _ = completed.changed().await;
                        }
                    },
                )
                .await
                .map_err(|_| Error::Timeout)?;
                continue;
            }
            self.metrics.inc(Metric::AuthCacheMisses);
            let (leader_id, leader_epoch) = {
                let state = lock(&self.state)?;
                let inflight = state.inflight.get(&key).ok_or(Error::Internal)?;
                (inflight.id, inflight.epoch)
            };
            let mut leader = InflightLeader {
                cache: self,
                key: Some(key.clone()),
                id: leader_id,
            };
            let result = deadline(
                self.limits.authentication_timeout_ms,
                self.provider.resolve_verifier(credential_id),
            )
            .await;
            let mut state = lock(&self.state)?;
            let completed = state.inflight.remove(&key).ok_or(Error::Internal)?;
            leader.key = None;
            if state.epoch != leader_epoch {
                let _ = completed.completed.send(true);
                self.metrics.inc(Metric::AuthStaleResponses);
                return Err(Error::Unavailable);
            }
            let (value, verifier) = match result {
                Ok(verifier) => (CachedAuth::Verifier(verifier.clone()), Some(verifier)),
                Err(Error::Authentication | Error::Forbidden) => (CachedAuth::Negative, None),
                Err(error) => {
                    let _ = completed.completed.send(true);
                    return Err(error);
                }
            };
            let ttl = match value {
                CachedAuth::Verifier(_) => self.limits.auth_positive_ttl_ms,
                CachedAuth::Negative => self.limits.auth_negative_ttl_ms,
                CachedAuth::Positive(_) => return Err(Error::Internal),
            };
            let bytes = key.credential_id.len()
                + std::mem::size_of::<AuthCacheKey>()
                + match &value {
                    CachedAuth::Verifier(verifier) => {
                        verifier.identity.device_key.tenant_id.as_str().len()
                            + verifier.identity.device_key.product_id.as_str().len()
                            + verifier.identity.device_key.device_id.as_str().len()
                            + verifier.identity.codec_id.as_str().len()
                            + 96
                    }
                    CachedAuth::Negative => 1,
                    CachedAuth::Positive(_) => return Err(Error::Internal),
                };
            insert_cache_entry(
                &mut state,
                &self.limits,
                &self.metrics,
                key.clone(),
                value,
                ttl,
                bytes,
            );
            let _ = completed.completed.send(true);
            if let Some(verifier) = verifier {
                verifier.verify(message, tag)?;
                return Ok(VerifiedDatagram {
                    verifier,
                    epoch: state.epoch,
                });
            }
            return Err(Error::Authentication);
        }
    }

    pub fn invalidate(&self, invalidation: &AuthInvalidation) -> Result<Vec<DeviceKey>> {
        let mut state = lock(&self.state)?;
        state.epoch = state.epoch.wrapping_add(1).max(1);
        let mut devices = std::collections::HashSet::new();
        state.entries.retain(|_, entry| {
            let auth = match &entry.value {
                CachedAuth::Positive(auth) => auth,
                CachedAuth::Verifier(verifier) => verifier.identity(),
                // Negative entries have no trusted device/product/tenant association.
                // Remove them on any invalidation so a newly enabled credential can recover.
                CachedAuth::Negative => return false,
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
                devices.insert(auth.device_key.clone());
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
        Ok(devices.into_iter().collect())
    }

    pub fn usage(&self) -> Result<(usize, usize)> {
        let state = lock(&self.state)?;
        Ok((state.entries.len(), state.bytes))
    }

    pub fn inflight_usage(&self) -> Result<(usize, usize)> {
        Ok((
            lock(&self.state)?.inflight.len(),
            self.waiters.available_permits(),
        ))
    }
}

fn cache_key(request: &AuthenticationRequest<'_>) -> Result<AuthCacheKey> {
    let (credential_id, fingerprint) = match request {
        AuthenticationRequest::Secret {
            credential_id,
            secret,
        } => (*credential_id, Sha256::digest(secret).into()),
    };
    if credential_id.is_empty() || credential_id.len() > 64 {
        return Err(Error::Authentication);
    }
    Ok(AuthCacheKey {
        credential_id: Arc::from(credential_id),
        fingerprint,
        verifier: false,
    })
}

fn verifier_cache_key(credential_id: &str) -> Result<AuthCacheKey> {
    if credential_id.is_empty() || credential_id.len() > 64 {
        return Err(Error::Authentication);
    }
    Ok(AuthCacheKey {
        credential_id: Arc::from(credential_id),
        fingerprint: [0; 32],
        verifier: true,
    })
}

fn insert_cache_entry(
    state: &mut AuthCacheState,
    limits: &Limits,
    metrics: &Metrics,
    key: AuthCacheKey,
    value: CachedAuth,
    ttl: u64,
    bytes: usize,
) {
    while (state.entries.len() >= limits.auth_cache_max_entries
        || state.bytes.saturating_add(bytes) > limits.auth_cache_max_bytes)
        && !state.entries.is_empty()
    {
        if let Some(old) = state.order.pop_front()
            && let Some(entry) = state.entries.remove(&old)
        {
            state.bytes = state.bytes.saturating_sub(entry.bytes);
            metrics.inc(Metric::AuthEvictions);
        }
    }
    if bytes <= limits.auth_cache_max_bytes {
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
    let bytes = s.as_bytes();
    (0..bytes.len())
        .step_by(2)
        .map(|index| {
            let a = (bytes[index] as char).to_digit(16).ok_or(Error::Invalid)?;
            let b = (bytes[index + 1] as char)
                .to_digit(16)
                .ok_or(Error::Invalid)?;
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
            AuthenticationRequest::Secret { credential_id, .. } => credential_id,
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
        }
        Ok(entry.identity.clone())
    }

    async fn resolve_verifier(&self, credential_id: &str) -> Result<DeviceVerifier> {
        let entry = self
            .credentials
            .get(credential_id)
            .ok_or(Error::Authentication)?;
        Ok(DeviceVerifier::new(entry.identity.clone(), entry.key))
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
    pub fn authenticate(&self, secret: &[u8]) -> Result<crate::AdminPrincipal> {
        if secret.len() == 64 && bool::from(self.hash.ct_eq(&Sha256::digest(secret))) {
            Ok(crate::AdminPrincipal::bootstrap())
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

    struct BlockingProvider {
        calls: AtomicUsize,
        block: std::sync::atomic::AtomicBool,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
        identity: AuthenticatedDevice,
    }

    #[async_trait]
    impl DeviceAuthenticator for BlockingProvider {
        async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_waiters();
            if self.block.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
            Ok(self.identity.clone())
        }

        async fn resolve_verifier(&self, _: &str) -> Result<DeviceVerifier> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(DeviceVerifier::new(self.identity.clone(), [7; 32]))
        }
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

        async fn resolve_verifier(&self, _: &str) -> Result<DeviceVerifier> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.mode.load(Ordering::Relaxed) {
                0 => Ok(DeviceVerifier::new(self.identity.clone(), [7; 32])),
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

    #[tokio::test]
    async fn cancelled_leader_releases_followers_and_all_inflight_resources() {
        let provider = Arc::new(BlockingProvider {
            calls: AtomicUsize::new(0),
            block: std::sync::atomic::AtomicBool::new(true),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            identity: identity(),
        });
        let limits = Arc::new(Limits {
            authentication_timeout_ms: 1_000,
            auth_cache_max_waiters: 16,
            ..Limits::default()
        });
        let cache = AuthCache::new(
            provider.clone(),
            limits.clone(),
            Arc::new(Metrics::default()),
        );
        let leader_cache = cache.clone();
        let leader = tokio::spawn(async move { leader_cache.authenticate(request("same")).await });
        while provider.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut followers = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            followers.push(tokio::spawn(async move {
                cache.authenticate(request("same")).await
            }));
        }
        tokio::task::yield_now().await;
        provider.block.store(false, Ordering::SeqCst);
        leader.abort();
        let _ = leader.await;
        for follower in followers {
            tokio::time::timeout(Duration::from_secs(1), follower)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            cache.inflight_usage().unwrap(),
            (0, limits.auth_cache_max_waiters)
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidation_fences_delayed_success_and_retry_uses_new_generation() {
        let provider = Arc::new(BlockingProvider {
            calls: AtomicUsize::new(0),
            block: std::sync::atomic::AtomicBool::new(true),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            identity: identity(),
        });
        let limits = Arc::new(Limits::default());
        let cache = AuthCache::new(
            provider.clone(),
            limits.clone(),
            Arc::new(Metrics::default()),
        );
        let first_cache = cache.clone();
        let first = tokio::spawn(async move { first_cache.authenticate(request("same")).await });
        while provider.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cache.invalidate(&AuthInvalidation::All).unwrap();
        provider.block.store(false, Ordering::SeqCst);
        provider.release.notify_waiters();
        assert!(matches!(first.await.unwrap(), Err(Error::Unavailable)));
        cache.authenticate(request("same")).await.unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            cache.inflight_usage().unwrap(),
            (0, limits.auth_cache_max_waiters)
        );
    }

    #[tokio::test]
    async fn timed_out_leader_does_not_poison_retry() {
        let provider = Arc::new(BlockingProvider {
            calls: AtomicUsize::new(0),
            block: std::sync::atomic::AtomicBool::new(true),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            identity: identity(),
        });
        let limits = Arc::new(Limits {
            authentication_timeout_ms: 5,
            ..Limits::default()
        });
        let cache = AuthCache::new(
            provider.clone(),
            limits.clone(),
            Arc::new(Metrics::default()),
        );
        assert!(matches!(
            cache.authenticate(request("same")).await,
            Err(Error::Timeout)
        ));
        provider.block.store(false, Ordering::SeqCst);
        cache.authenticate(request("same")).await.unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            cache.inflight_usage().unwrap(),
            (0, limits.auth_cache_max_waiters)
        );
    }

    #[tokio::test]
    async fn ten_thousand_signed_packets_use_one_verifier_lookup() {
        let provider = Arc::new(Provider {
            calls: AtomicUsize::new(0),
            mode: AtomicU8::new(0),
            identity: identity(),
        });
        let cache = AuthCache::new(
            provider.clone(),
            Arc::new(Limits::default()),
            Arc::new(Metrics::default()),
        );
        for sequence in 0..10_000u64 {
            let message = sequence.to_be_bytes();
            let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
            mac.update(&message);
            let tag = mac.finalize().into_bytes();
            cache.verify_signed("signed", &message, &tag).await.unwrap();
        }
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            cache.verify_signed("signed", b"wrong", &[0; 32]).await,
            Err(Error::Authentication)
        ));
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        provider.mode.store(2, Ordering::Relaxed);
        let message = 10_001u64.to_be_bytes();
        let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
        mac.update(&message);
        let tag = mac.finalize().into_bytes();
        cache.verify_signed("signed", &message, &tag).await.unwrap();
        assert!(matches!(
            cache.verify_signed("uncached", &message, &tag).await,
            Err(Error::Unavailable)
        ));
        assert_eq!(provider.calls.load(Ordering::Relaxed), 2);
    }
}
