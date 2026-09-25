use crate::{
    AuthenticationRequest, DeviceAuthenticator, DeviceVerifier, Error, Result,
    metrics::{BusinessRpcCallResult, Histogram, Metric, Metrics},
};
use async_trait::async_trait;
use netbaiot_core::{
    AuthenticatedDevice, Permissions, TenantId,
    business_rpc::{
        AuthenticatedDeviceWire, BusinessRpcFrame, DeviceAuthenticateRequest,
        ResolveVerifierRequest, ResolveVerifierResponse, RpcErrorCode,
    },
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use uuid::Uuid;

/// One authority is active at a time. A lease generation fences late responses and old disconnects.
pub struct BusinessRpcRegistry {
    state: Mutex<State>,
    next_epoch: AtomicU64,
    pending_slots: Arc<Semaphore>,
    pending_bytes: Arc<Semaphore>,
    timeout: Duration,
    metrics: Arc<Metrics>,
    max_bytes: usize,
}
struct State {
    provider: Option<Provider>,
    pending: HashMap<(u64, Uuid), Pending>,
}
#[derive(Clone)]
pub struct BusinessProviderScope {
    pub global: bool,
    pub tenants: Vec<TenantId>,
}
impl BusinessProviderScope {
    fn allows(&self, tenant: &TenantId) -> bool {
        self.global || self.tenants.contains(tenant)
    }
}
struct Provider {
    scope: BusinessProviderScope,
    epoch: u64,
    serving: bool,
    revision: u64,
    sender: mpsc::Sender<BusinessRpcOutbound>,
}
struct Pending {
    method: &'static str,
    deadline: Instant,
    min_revision: u64,
    result: oneshot::Sender<Result<Value>>,
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
/// The writer owns the outbound frame; its byte permit remains held until the frame is written.
pub struct BusinessRpcOutbound {
    pub frame: BusinessRpcFrame,
    pub _bytes: OwnedSemaphorePermit,
}
pub struct ProviderLease {
    registry: Arc<BusinessRpcRegistry>,
    epoch: u64,
}
impl ProviderLease {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn mark_serving(&self, revision: u64) -> Result<()> {
        self.registry.mark_serving(self.epoch, revision)
    }
    pub fn mark_syncing(&self) -> Result<()> {
        self.registry.mark_syncing(self.epoch)
    }
    pub fn advance_revision(&self, revision: u64) -> Result<()> {
        self.registry.advance_revision(self.epoch, revision)
    }
}
impl Drop for ProviderLease {
    fn drop(&mut self) {
        self.registry.release(self.epoch);
    }
}
struct PendingGuard<'a> {
    registry: &'a BusinessRpcRegistry,
    epoch: u64,
    id: Uuid,
}
struct CallLatency<'a> {
    metrics: &'a Metrics,
    histogram: Histogram,
    started: Instant,
}
impl Drop for CallLatency<'_> {
    fn drop(&mut self) {
        self.metrics.observe(
            self.histogram,
            self.started.elapsed().as_micros().min(u64::MAX as u128) as u64,
        );
    }
}
impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        let sender = if let Ok(mut state) = self.registry.state.lock() {
            if state.pending.remove(&(self.epoch, self.id)).is_some() {
                state
                    .provider
                    .as_ref()
                    .filter(|p| p.epoch == self.epoch)
                    .map(|p| p.sender.clone())
            } else {
                None
            }
        } else {
            None
        };
        if let Some(sender) = sender {
            let frame = BusinessRpcFrame::Cancel {
                request_id: self.id,
            };
            let size = serde_json::to_vec(&frame)
                .ok()
                .and_then(|bytes| u32::try_from(bytes.len()).ok());
            if let Some(size) = size
                && let Ok(bytes) = self
                    .registry
                    .pending_bytes
                    .clone()
                    .try_acquire_many_owned(size)
            {
                let _ = sender.try_send(BusinessRpcOutbound {
                    frame,
                    _bytes: bytes,
                });
            }
        }
    }
}
impl BusinessRpcRegistry {
    pub fn new(max_pending: usize, max_bytes: usize, timeout: Duration) -> Result<Arc<Self>> {
        Self::new_with_metrics(
            max_pending,
            max_bytes,
            timeout,
            Arc::new(Metrics::default()),
        )
    }
    pub fn new_with_metrics(
        max_pending: usize,
        max_bytes: usize,
        timeout: Duration,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        if max_pending == 0 || max_bytes == 0 || timeout.is_zero() || max_bytes > u32::MAX as usize
        {
            return Err(Error::Configuration);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                provider: None,
                pending: HashMap::new(),
            }),
            next_epoch: AtomicU64::new(1),
            pending_slots: Arc::new(Semaphore::new(max_pending)),
            pending_bytes: Arc::new(Semaphore::new(max_bytes)),
            timeout,
            metrics,
            max_bytes,
        }))
    }
    pub fn register(
        self: &Arc<Self>,
        sender: mpsc::Sender<BusinessRpcOutbound>,
        scope: BusinessProviderScope,
    ) -> Result<ProviderLease> {
        let mut state = self.state.lock().map_err(|_| Error::Internal)?;
        if state.provider.is_some() {
            return Err(Error::Conflict);
        }
        let epoch = self.next_epoch.fetch_add(1, Ordering::Relaxed);
        if epoch > 1 {
            self.metrics.inc(Metric::BusinessRpcReconnects);
        }
        state.provider = Some(Provider {
            scope,
            epoch,
            serving: false,
            revision: 0,
            sender,
        });
        Ok(ProviderLease {
            registry: self.clone(),
            epoch,
        })
    }
    fn mark_serving(&self, epoch: u64, revision: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::Internal)?;
        let provider = state.provider.as_mut().ok_or(Error::Unavailable)?;
        if provider.epoch != epoch {
            return Err(Error::Unavailable);
        }
        if revision == 0 {
            return Err(Error::Invalid);
        }
        provider.revision = revision;
        provider.serving = true;
        Ok(())
    }
    fn mark_syncing(&self, epoch: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::Internal)?;
        let provider = state
            .provider
            .as_mut()
            .filter(|p| p.epoch == epoch)
            .ok_or(Error::Unavailable)?;
        provider.serving = false;
        Ok(())
    }
    fn advance_revision(&self, epoch: u64, revision: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::Internal)?;
        let provider = state
            .provider
            .as_mut()
            .filter(|p| p.epoch == epoch)
            .ok_or(Error::Unavailable)?;
        if !provider.serving || revision <= provider.revision {
            return Err(Error::Conflict);
        }
        provider.revision = revision;
        Ok(())
    }
    pub fn auth_revision(&self) -> Result<u64> {
        let state = self.state.lock().map_err(|_| Error::Internal)?;
        state
            .provider
            .as_ref()
            .filter(|p| p.serving)
            .map(|p| p.revision)
            .ok_or(Error::Unavailable)
    }
    fn release(&self, epoch: u64) {
        if let Ok(mut state) = self.state.lock()
            && state.provider.as_ref().is_some_and(|p| p.epoch == epoch)
        {
            state.provider = None;
            state.pending.retain(|(owner, _), _| *owner != epoch);
        }
    }
    pub fn is_serving(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|s| s.provider.as_ref().is_some_and(|p| p.serving))
    }
    pub fn pending_usage(&self) -> usize {
        self.state.lock().map_or(0, |s| s.pending.len())
    }
    pub fn render_metrics(&self) -> String {
        let (serving, pending, queue) = self.state.lock().map_or((false, 0, 0), |state| {
            (
                state.provider.as_ref().is_some_and(|p| p.serving),
                state.pending.len(),
                state
                    .provider
                    .as_ref()
                    .map_or(0, |p| p.sender.max_capacity() - p.sender.capacity()),
            )
        });
        format!(
            "netbaiot_business_rpc_provider_serving {}\nnetbaiot_business_rpc_pending {}\nnetbaiot_business_rpc_pending_bytes {}\nnetbaiot_business_rpc_auth_queue {}\n",
            u8::from(serving),
            pending,
            self.max_bytes - self.pending_bytes.available_permits(),
            queue
        )
    }
    /// Returns false for unknown, duplicate, stale, wrong-method, or expired responses.
    pub fn complete(&self, epoch: u64, id: Uuid, method: &str, result: Result<Value>) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let Some(pending) = state.pending.get(&(epoch, id)) else {
            self.metrics.inc(Metric::BusinessRpcLateResponses);
            return false;
        };
        if pending.method != method || pending.deadline <= Instant::now() {
            self.metrics.inc(Metric::BusinessRpcLateResponses);
            return false;
        }
        let scope = match state.provider.as_ref().filter(|p| p.epoch == epoch) {
            Some(provider) => provider.scope.clone(),
            None => return false,
        };
        let Some(pending) = state.pending.remove(&(epoch, id)) else {
            return false;
        };
        let checked = result.and_then(|body| {
            let identity = match method {
                "device.authenticate" => {
                    serde_json::from_value::<AuthenticatedDeviceWire>(body.clone())
                }
                "device.resolve_verifier" => {
                    serde_json::from_value::<ResolveVerifierResponse>(body.clone())
                        .map(|value| value.identity)
                }
                _ => return Err(Error::Invalid),
            }
            .map_err(|_| Error::Invalid)?;
            if identity.auth_revision < pending.min_revision
                || !scope.allows(&identity.device_key.tenant_id)
            {
                return Err(Error::Invalid);
            }
            Ok(body)
        });
        pending.result.send(checked).is_ok()
    }
    pub async fn call<T: Serialize, R: DeserializeOwned>(
        &self,
        method: &'static str,
        body: &T,
    ) -> Result<R> {
        let result = self.call_inner(method, body).await;
        let outcome = match &result {
            Ok(_) => BusinessRpcCallResult::Success,
            Err(Error::Authentication) => BusinessRpcCallResult::DeviceRejected,
            Err(Error::Timeout) => BusinessRpcCallResult::Timeout,
            Err(Error::Overloaded) => BusinessRpcCallResult::Overloaded,
            Err(Error::Unavailable) => BusinessRpcCallResult::Unavailable,
            Err(_) => BusinessRpcCallResult::Invalid,
        };
        self.metrics.business_rpc_method_result(method, outcome);
        result
    }
    async fn call_inner<T: Serialize, R: DeserializeOwned>(
        &self,
        method: &'static str,
        body: &T,
    ) -> Result<R> {
        let histogram = match method {
            "device.authenticate" => Histogram::BusinessRpcAuthLatency,
            "device.resolve_verifier" => Histogram::BusinessRpcVerifierLatency,
            _ => return Err(Error::Invalid),
        };
        let _latency = CallLatency {
            metrics: &self.metrics,
            histogram,
            started: Instant::now(),
        };
        let value = serde_json::to_value(body).map_err(|_| Error::Invalid)?;
        let size = serde_json::to_vec(&value)
            .map_err(|_| Error::Invalid)?
            .len();
        let byte_count = u32::try_from(size).map_err(|_| Error::Overloaded)?;
        if byte_count == 0 || byte_count as usize > 16 * 1024 {
            return Err(Error::Overloaded);
        }
        let deadline = Instant::now() + self.timeout;
        let slot = self
            .pending_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                self.metrics.inc(Metric::BusinessRpcOverloads);
                Error::Overloaded
            })?;
        let bytes = self
            .pending_bytes
            .clone()
            .try_acquire_many_owned(byte_count)
            .map_err(|_| {
                self.metrics.inc(Metric::BusinessRpcOverloads);
                Error::Overloaded
            })?;
        let (id, epoch, sender, receive) = {
            let mut state = self.state.lock().map_err(|_| Error::Internal)?;
            let provider = state
                .provider
                .as_ref()
                .filter(|p| p.serving)
                .ok_or(Error::Unavailable)?;
            let epoch = provider.epoch;
            let sender = provider.sender.clone();
            let id = Uuid::new_v4();
            let min_revision = provider.revision;
            let (send, receive) = oneshot::channel();
            state.pending.insert(
                (epoch, id),
                Pending {
                    method,
                    deadline,
                    min_revision,
                    result: send,
                    _slot: slot,
                    _bytes: bytes,
                },
            );
            (id, epoch, sender, receive)
        };
        let _guard = PendingGuard {
            registry: self,
            epoch,
            id,
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        let frame = BusinessRpcFrame::Request {
            request_id: id,
            method: method.into(),
            deadline_ms: remaining.as_millis().min(u32::MAX as u128) as u32,
            body: value,
        };
        let wire_size = serde_json::to_vec(&frame)
            .map_err(|_| Error::Internal)?
            .len();
        let wire_count = u32::try_from(wire_size).map_err(|_| Error::Overloaded)?;
        let wire_bytes = self
            .pending_bytes
            .clone()
            .try_acquire_many_owned(wire_count)
            .map_err(|_| {
                self.metrics.inc(Metric::BusinessRpcOverloads);
                Error::Overloaded
            })?;
        sender
            .try_send(BusinessRpcOutbound {
                frame,
                _bytes: wire_bytes,
            })
            .map_err(|_| {
                self.metrics.inc(Metric::BusinessRpcOverloads);
                Error::Overloaded
            })?;
        let value = tokio::time::timeout_at(deadline.into(), receive)
            .await
            .map_err(|_| {
                self.metrics.inc(Metric::BusinessRpcTimeouts);
                Error::Timeout
            })?
            .map_err(|_| Error::Unavailable)??;
        serde_json::from_value(value).map_err(|_| Error::Invalid)
    }
}

pub struct BusinessRpcAuthProvider {
    registry: Arc<BusinessRpcRegistry>,
}
impl BusinessRpcAuthProvider {
    pub fn new(registry: Arc<BusinessRpcRegistry>) -> Arc<Self> {
        Arc::new(Self { registry })
    }
}
fn checked_identity(
    wire: AuthenticatedDeviceWire,
    minimum_revision: u64,
) -> Result<AuthenticatedDevice> {
    if wire.credential_version == 0
        || wire.auth_generation == 0
        || wire.codec_version == 0
        || wire.auth_revision < minimum_revision
    {
        return Err(Error::Invalid);
    }
    Ok(AuthenticatedDevice {
        device_key: wire.device_key,
        credential_version: wire.credential_version,
        auth_generation: wire.auth_generation,
        codec_id: wire.codec_id,
        codec_version: wire.codec_version,
        permissions: Permissions {
            publish: wire.publish,
            commands: wire.commands,
        },
    })
}
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 15) as usize] as char);
    }
    out
}
fn decode_hex_32(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        return Err(Error::Invalid);
    }
    let mut key = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16).ok_or(Error::Invalid)? as u8;
        let lo = (pair[1] as char).to_digit(16).ok_or(Error::Invalid)? as u8;
        key[index] = (hi << 4) | lo;
    }
    Ok(key)
}
#[async_trait]
impl DeviceAuthenticator for BusinessRpcAuthProvider {
    async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        let AuthenticationRequest::Secret {
            credential_id,
            secret,
        } = request;
        if credential_id.len() > 64 || secret.len() > 256 {
            return Err(Error::Invalid);
        }
        let minimum_revision = self.registry.auth_revision()?;
        let body = DeviceAuthenticateRequest {
            credential_id: credential_id.into(),
            secret_hex: encode_hex(secret),
            min_auth_revision: minimum_revision,
        };
        let wire: AuthenticatedDeviceWire =
            self.registry.call("device.authenticate", &body).await?;
        checked_identity(wire, minimum_revision)
    }
    async fn resolve_verifier(&self, credential_id: &str) -> Result<DeviceVerifier> {
        if credential_id.len() > 64 {
            return Err(Error::Invalid);
        }
        let minimum_revision = self.registry.auth_revision()?;
        let response: ResolveVerifierResponse = self
            .registry
            .call(
                "device.resolve_verifier",
                &ResolveVerifierRequest {
                    credential_id: credential_id.into(),
                    min_auth_revision: minimum_revision,
                },
            )
            .await?;
        Ok(DeviceVerifier::new(
            checked_identity(response.identity, minimum_revision)?,
            decode_hex_32(&response.verifier_key_hex)?,
        ))
    }
}

pub fn rpc_error_to_runtime(code: RpcErrorCode) -> Error {
    match code {
        RpcErrorCode::DeviceRejected => Error::Authentication,
        RpcErrorCode::Unauthenticated => Error::Unavailable,
        RpcErrorCode::Unavailable => Error::Unavailable,
        RpcErrorCode::Overloaded => Error::Overloaded,
        RpcErrorCode::Timeout => Error::Timeout,
        RpcErrorCode::Conflict | RpcErrorCode::StaleRevision => Error::Conflict,
        // AuthCache negative-caches Forbidden. A business-principal permission
        // failure is not evidence that the device credential is invalid.
        RpcErrorCode::Forbidden => Error::Invalid,
        RpcErrorCode::InvalidRequest | RpcErrorCode::UnknownMethod => Error::Invalid,
        RpcErrorCode::Internal => Error::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthCache, Limits};
    use hmac::{Hmac, Mac};
    use serde_json::json;
    use sha2::Sha256;

    #[test]
    fn provider_permission_failure_cannot_negative_cache_device() {
        assert!(matches!(
            rpc_error_to_runtime(RpcErrorCode::Forbidden),
            Error::Invalid
        ));
        assert!(matches!(
            rpc_error_to_runtime(RpcErrorCode::DeviceRejected),
            Error::Authentication
        ));
    }

    fn test_identity(device: &str) -> Value {
        json!({
            "device_key": {"tenant_id":"demo","product_id":"sensor","device_id":device},
            "credential_version":1,"auth_generation":1,"codec_id":"netbaiot-json",
            "codec_version":1,"publish":true,"commands":true,"auth_revision":1
        })
    }

    #[tokio::test]
    async fn concurrent_out_of_order_responses_keep_their_request_ids() {
        let registry = BusinessRpcRegistry::new(2, 65_536, Duration::from_secs(1)).unwrap();
        let (send, mut recv) = mpsc::channel(2);
        let lease = registry
            .register(
                send,
                BusinessProviderScope {
                    global: true,
                    tenants: Vec::new(),
                },
            )
            .unwrap();
        assert!(!registry.is_serving());
        lease.mark_serving(1).unwrap();
        let a = tokio::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .call::<_, Value>("device.authenticate", &json!({"n":1}))
                    .await
            }
        });
        let b = tokio::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .call::<_, Value>("device.authenticate", &json!({"n":2}))
                    .await
            }
        });
        let first = recv.recv().await.unwrap();
        let second = recv.recv().await.unwrap();
        let BusinessRpcFrame::Request {
            request_id: id1,
            body: _body1,
            ..
        } = first.frame
        else {
            panic!("request expected")
        };
        let BusinessRpcFrame::Request {
            request_id: id2,
            body: _body2,
            ..
        } = second.frame
        else {
            panic!("request expected")
        };
        assert_ne!(id1, id2);
        assert!(!registry.complete(lease.epoch(), id1, "wrong.method", Ok(json!({"n":99}))));
        assert!(registry.complete(
            lease.epoch(),
            id2,
            "device.authenticate",
            Ok(test_identity("two"))
        ));
        assert!(registry.complete(
            lease.epoch(),
            id1,
            "device.authenticate",
            Ok(test_identity("one"))
        ));
        assert!(!registry.complete(
            lease.epoch(),
            id1,
            "device.authenticate",
            Ok(json!({"n":99}))
        ));
        let mut values = vec![a.await.unwrap().unwrap(), b.await.unwrap().unwrap()];
        values.sort_by_key(|value| {
            value["device_key"]["device_id"]
                .as_str()
                .unwrap()
                .to_owned()
        });
        assert_eq!(values, vec![test_identity("one"), test_identity("two")]);
        assert_eq!(registry.pending_usage(), 0);
    }

    #[tokio::test]
    async fn cancellation_and_lease_release_reclaim_pending() {
        let registry = BusinessRpcRegistry::new(1, 65_536, Duration::from_secs(60)).unwrap();
        let (send, mut recv) = mpsc::channel(1);
        let lease = registry
            .register(
                send,
                BusinessProviderScope {
                    global: true,
                    tenants: Vec::new(),
                },
            )
            .unwrap();
        lease.mark_serving(1).unwrap();
        let task = tokio::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .call::<_, Value>("device.authenticate", &json!({"n":1}))
                    .await
            }
        });
        let request = recv.recv().await.unwrap();
        assert_eq!(registry.pending_usage(), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(registry.pending_usage(), 0);
        drop(request);
        drop(lease);
        assert!(!registry.is_serving());
        let (send, _recv) = mpsc::channel(1);
        let replacement = registry
            .register(
                send,
                BusinessProviderScope {
                    global: true,
                    tenants: Vec::new(),
                },
            )
            .unwrap();
        replacement.mark_serving(1).unwrap();
        assert_eq!(registry.pending_usage(), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_reclaims_pending_and_late_response_is_ignored() {
        let metrics = Arc::new(Metrics::default());
        let registry = BusinessRpcRegistry::new_with_metrics(
            1,
            65_536,
            Duration::from_millis(5),
            metrics.clone(),
        )
        .unwrap();
        let (send, mut receive) = mpsc::channel(1);
        let lease = registry
            .register(
                send,
                BusinessProviderScope {
                    global: true,
                    tenants: Vec::new(),
                },
            )
            .unwrap();
        lease.mark_serving(1).unwrap();
        let task = tokio::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .call::<_, Value>("device.authenticate", &json!({"credential_id":"x"}))
                    .await
            }
        });
        let outbound = receive.recv().await.unwrap();
        let BusinessRpcFrame::Request { request_id, .. } = outbound.frame else {
            panic!("request expected")
        };
        drop(outbound._bytes);
        assert!(matches!(task.await.unwrap(), Err(Error::Timeout)));
        assert_eq!(registry.pending_usage(), 0);
        // The bounded cancellation frame owns its wire permit until written or dropped.
        drop(receive.try_recv().unwrap());
        assert_eq!(registry.pending_bytes.available_permits(), 65_536);
        assert!(!registry.complete(
            lease.epoch(),
            request_id,
            "device.authenticate",
            Ok(test_identity("one"))
        ));
        assert_eq!(metrics.get(Metric::BusinessRpcTimeouts), 1);
        assert_eq!(metrics.get(Metric::BusinessRpcLateResponses), 1);
    }
    #[tokio::test]
    async fn provider_scope_rejects_cross_tenant_identity_without_negative_cache_error() {
        let registry = BusinessRpcRegistry::new(1, 65_536, Duration::from_secs(1)).unwrap();
        let (send, mut receive) = mpsc::channel(1);
        let lease = registry
            .register(
                send,
                BusinessProviderScope {
                    global: false,
                    tenants: vec![TenantId::new("allowed").unwrap()],
                },
            )
            .unwrap();
        lease.mark_serving(1).unwrap();
        let call = tokio::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .call::<_, Value>("device.authenticate", &json!({"credential_id":"x"}))
                    .await
            }
        });
        let outbound = receive.recv().await.unwrap();
        let BusinessRpcFrame::Request { request_id, .. } = outbound.frame else {
            panic!("request expected")
        };
        assert!(registry.complete(
            lease.epoch(),
            request_id,
            "device.authenticate",
            Ok(test_identity("one"))
        ));
        assert!(matches!(call.await.unwrap(), Err(Error::Invalid)));
        assert_eq!(registry.pending_usage(), 0);
    }

    #[tokio::test]
    async fn udp_verifier_rpc_miss_is_singleflight_then_hmac_stays_local() {
        let registry = BusinessRpcRegistry::new(4, 65_536, Duration::from_secs(1)).unwrap();
        let (send, mut receive) = mpsc::channel(4);
        let lease = registry
            .register(
                send,
                BusinessProviderScope {
                    global: true,
                    tenants: Vec::new(),
                },
            )
            .unwrap();
        lease.mark_serving(1).unwrap();
        let cache = AuthCache::new(
            BusinessRpcAuthProvider::new(registry.clone()),
            Arc::new(Limits::default()),
            Arc::new(Metrics::default()),
        );
        let message = b"signed-udp-message";
        let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
        mac.update(message);
        let tag = mac.finalize().into_bytes().to_vec();
        let first = tokio::spawn({
            let cache = cache.clone();
            let tag = tag.clone();
            async move { cache.verify_signed("cred-udp", message, &tag).await }
        });
        let second = tokio::spawn({
            let cache = cache.clone();
            let tag = tag.clone();
            async move { cache.verify_signed("cred-udp", message, &tag).await }
        });
        let outbound = receive.recv().await.unwrap();
        let BusinessRpcFrame::Request {
            request_id, method, ..
        } = outbound.frame
        else {
            panic!("verifier request expected")
        };
        assert_eq!(method, "device.resolve_verifier");
        assert!(registry.complete(
            lease.epoch(),
            request_id,
            &method,
            Ok(json!({"identity": test_identity("udp"), "verifier_key_hex": "07".repeat(32)}))
        ));
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        for _ in 0..100 {
            cache
                .verify_signed("cred-udp", message, &tag)
                .await
                .unwrap();
        }
        assert!(
            receive.try_recv().is_err(),
            "cache hits must not call the provider"
        );
        assert_eq!(registry.pending_usage(), 0);
        drop(lease);
        cache
            .verify_signed("cred-udp", message, &tag)
            .await
            .unwrap();
        assert!(matches!(
            cache.verify_signed("cred-new", message, &tag).await,
            Err(Error::Unavailable)
        ));
        let (replacement, mut replacement_requests) = mpsc::channel(4);
        let new_lease = registry
            .register(
                replacement,
                BusinessProviderScope {
                    global: true,
                    tenants: Vec::new(),
                },
            )
            .unwrap();
        new_lease.mark_serving(2).unwrap();
        let recovered = tokio::spawn({
            let cache = cache.clone();
            let tag = tag.clone();
            async move { cache.verify_signed("cred-new", message, &tag).await }
        });
        let outbound = replacement_requests.recv().await.unwrap();
        let BusinessRpcFrame::Request {
            request_id, method, ..
        } = outbound.frame
        else {
            panic!("verifier request expected")
        };
        let mut identity = test_identity("new");
        identity["auth_revision"] = json!(2);
        assert!(registry.complete(
            new_lease.epoch(),
            request_id,
            &method,
            Ok(json!({"identity": identity, "verifier_key_hex": "07".repeat(32)}))
        ));
        recovered.await.unwrap().unwrap();
    }
}
