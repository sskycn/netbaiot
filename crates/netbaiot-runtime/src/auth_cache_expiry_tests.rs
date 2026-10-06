use super::*;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

pub(super) fn assert_cache_consistent(state: &AuthCacheState, limits: &Limits) {
    assert_eq!(
        state.bytes,
        state.entries.values().map(|e| e.bytes).sum::<usize>()
    );
    assert!(state.entries.len() <= limits.auth_cache_max_entries);
    assert!(state.bytes <= limits.auth_cache_max_bytes);
    assert_eq!(state.expirations.len(), state.entries.len());
    assert_eq!(state.order.len(), state.entries.len());
    let mut owners = std::collections::HashSet::new();
    for key in &state.order {
        assert!(state.entries.contains_key(key));
        assert!(owners.insert(key));
    }
    for (key, entry) in &state.entries {
        assert!(state.expirations.contains(&(entry.expires, key.clone())));
    }
    for (expires, key) in &state.expirations {
        assert!(
            state
                .entries
                .get(key)
                .is_some_and(|e| e.expires == *expires)
        );
    }
}

fn consistent(cache: &AuthCache) {
    assert_cache_consistent(&cache.state.lock().unwrap(), &cache.limits);
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

fn scopes() -> [AuthInvalidation; 6] {
    let auth = identity();
    [
        AuthInvalidation::Device {
            device: auth.device_key.clone(),
        },
        AuthInvalidation::Product {
            tenant_id: auth.device_key.tenant_id.clone(),
            product_id: auth.device_key.product_id.clone(),
        },
        AuthInvalidation::Tenant {
            tenant_id: auth.device_key.tenant_id.clone(),
        },
        AuthInvalidation::CredentialVersion {
            version: auth.credential_version,
        },
        AuthInvalidation::AuthGeneration {
            generation: auth.auth_generation,
        },
        AuthInvalidation::All,
    ]
}

struct Provider {
    calls: AtomicUsize,
    mode: AtomicU8,
    blocked: AtomicBool,
    release: tokio::sync::Semaphore,
}

impl Provider {
    async fn resolve(&self) -> Result<AuthenticatedDevice> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.blocked.load(Ordering::SeqCst) {
            self.release.acquire().await.unwrap().forget();
        }
        if self
            .mode
            .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            panic!("test provider leader panic");
        }
        match self.mode.load(Ordering::SeqCst) {
            0 => Ok(identity()),
            1 => Err(Error::Authentication),
            _ => Err(Error::Unavailable),
        }
    }
}

#[async_trait]
impl DeviceAuthenticator for Provider {
    async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
        self.resolve().await
    }

    async fn resolve_verifier(&self, _: &str) -> Result<DeviceVerifier> {
        Ok(DeviceVerifier::new(self.resolve().await?, [7; 32]))
    }
}

fn fixture(limits: Limits) -> (Arc<AuthCache>, Arc<Provider>) {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        mode: AtomicU8::new(0),
        blocked: AtomicBool::new(false),
        release: tokio::sync::Semaphore::new(0),
    });
    (
        AuthCache::new(
            provider.clone(),
            Arc::new(limits),
            Arc::new(Metrics::default()),
        ),
        provider,
    )
}

fn secret(id: &str) -> AuthenticationRequest<'_> {
    AuthenticationRequest::Secret {
        credential_id: id,
        secret: b"secret",
    }
}

fn tag() -> [u8; 32] {
    DeviceVerifier::new(identity(), [7; 32])
        .sign(b"datagram")
        .unwrap()
}

async fn attempt(cache: &AuthCache, verifier: bool) -> Result<AuthenticatedDevice> {
    if verifier {
        Ok(cache
            .verify_signed_with_verifier("same", b"datagram", &tag())
            .await?
            .identity()
            .clone())
    } else {
        cache.authenticate(secret("same")).await
    }
}

fn set_expiry(state: &mut AuthCacheState, key: &AuthCacheKey, expires: Instant) {
    let entry = state.entries.get_mut(key).unwrap();
    assert!(state.expirations.remove(&(entry.expires, key.clone())));
    entry.expires = expires;
    assert!(state.expirations.insert((expires, key.clone())));
}

fn insert(cache: &AuthCache, key: AuthCacheKey, value: CachedAuth, bytes: usize) {
    let mut state = cache.state.lock().unwrap();
    insert_cache_entry(
        &mut state,
        &cache.limits,
        &cache.metrics,
        key,
        value,
        600_000,
        bytes,
    );
    assert_cache_consistent(&state, &cache.limits);
}

async fn wait_for_waiters(cache: &AuthCache, followers: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while cache.inflight_usage().unwrap()
            != (1, cache.limits.auth_cache_max_waiters - followers)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn positive_negative_and_verifier_ttl_release_all_ownership_and_refresh() {
    for verifier in [false, true] {
        for negative in [false, true] {
            let (cache, provider) = fixture(Limits {
                auth_positive_ttl_ms: 60_000,
                auth_negative_ttl_ms: 30_000,
                ..Limits::default()
            });
            provider.mode.store(u8::from(negative), Ordering::SeqCst);
            let before = Instant::now();
            assert_eq!(attempt(&cache, verifier).await.is_err(), negative);
            let after = Instant::now();
            let key = if verifier {
                verifier_cache_key("same").unwrap()
            } else {
                cache_key(&secret("same")).unwrap()
            };
            let ttl = Duration::from_millis(if negative { 30_000 } else { 60_000 });
            {
                let state = cache.state.lock().unwrap();
                let entry = state.entries.get(&key).unwrap();
                assert!(entry.expires >= before + ttl && entry.expires <= after + ttl);
                assert!(state.bytes > 0);
                assert_cache_consistent(&state, &cache.limits);
            }
            // An unexpired positive/verifier remains usable during outage; negatives still reject.
            provider.mode.store(2, Ordering::SeqCst);
            assert_eq!(attempt(&cache, verifier).await.is_err(), negative);
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            {
                let mut state = cache.state.lock().unwrap();
                set_expiry(&mut state, &key, Instant::now());
                prune_expired(&mut state);
                assert_cache_consistent(&state, &cache.limits);
                assert_eq!(state.bytes, 0);
                assert!(state.entries.is_empty());
            }
            // Outage misses are not cached, including an expired formerly positive entry.
            assert!(matches!(
                attempt(&cache, verifier).await,
                Err(Error::Unavailable)
            ));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
            assert_eq!(cache.usage().unwrap(), (0, 0));
            provider.mode.store(0, Ordering::SeqCst);
            attempt(&cache, verifier).await.unwrap();
            attempt(&cache, verifier).await.unwrap();
            assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
            consistent(&cache);
        }
    }
}

#[tokio::test]
async fn expiry_lookup_removes_only_due_entries_and_equal_deadlines() {
    let (cache, _) = fixture(Limits::default());
    let keys: Vec<_> = (0..64)
        .map(|i| verifier_cache_key(&format!("expiry-{i}")).unwrap())
        .collect();
    for key in &keys {
        insert(&cache, key.clone(), CachedAuth::Negative, 100);
    }
    let mut state = cache.state.lock().unwrap();
    let now = Instant::now();
    // Mixed deadlines and many identical deadlines exercise the key tie breaker.
    for (i, key) in keys.iter().enumerate() {
        let expires = if i < 32 {
            now - Duration::from_secs((i % 3 + 1) as u64)
        } else {
            now + Duration::from_secs((i % 3 + 60) as u64)
        };
        set_expiry(&mut state, key, expires);
    }
    prune_expired(&mut state);
    assert_eq!(state.entries.len(), 32);
    assert_eq!(state.bytes, 3200);
    assert!(keys[..32].iter().all(|k| !state.entries.contains_key(k)));
    assert!(state.order.iter().eq(keys[32..].iter()));
    assert_cache_consistent(&state, &cache.limits);
    let expirations = state.expirations.clone();
    for _ in 0..100 {
        prune_expired(&mut state);
    }
    assert!(state.expirations == expirations);
    assert_cache_consistent(&state, &cache.limits);
}

#[tokio::test]
async fn expire_all_and_reinsert_never_grow_indices_or_bytes() {
    let (cache, _) = fixture(Limits {
        auth_cache_max_entries: 512,
        auth_cache_max_bytes: 51_200,
        ..Limits::default()
    });
    for _ in 0..32 {
        for i in 0..512 {
            let value = match i % 3 {
                0 => CachedAuth::Positive(identity()),
                1 => CachedAuth::Negative,
                _ => CachedAuth::Verifier(DeviceVerifier::new(identity(), [7; 32])),
            };
            insert(
                &cache,
                if i % 3 == 2 {
                    verifier_cache_key(&format!("burst-{i}")).unwrap()
                } else {
                    cache_key(&secret(&format!("burst-{i}"))).unwrap()
                },
                value,
                100,
            );
        }
        let mut state = cache.state.lock().unwrap();
        assert_eq!(state.entries.len(), 512);
        let keys: Vec<_> = state.entries.keys().cloned().collect();
        let due = Instant::now() - Duration::from_secs(1);
        for key in &keys {
            set_expiry(&mut state, key, due);
        }
        prune_expired(&mut state);
        assert_cache_consistent(&state, &cache.limits);
        assert_eq!(state.bytes, 0);
        assert!(state.entries.is_empty());
    }
}

#[tokio::test]
async fn count_and_byte_eviction_preserve_fifo_and_replacement_indices() {
    for (max_entries, max_bytes, entry_bytes) in [(3, 10_000, 100), (100, 300, 100)] {
        let (cache, _) = fixture(Limits {
            auth_cache_max_entries: max_entries,
            auth_cache_max_bytes: max_bytes,
            ..Limits::default()
        });
        for id in ["a", "b", "c"] {
            cache.authenticate(secret(id)).await.unwrap();
        }
        // Use controlled charges to exercise both ceilings with the same FIFO expectation.
        cache.invalidate(&AuthInvalidation::All).unwrap();
        for id in ["a", "b", "c"] {
            insert(
                &cache,
                cache_key(&secret(id)).unwrap(),
                CachedAuth::Positive(identity()),
                entry_bytes,
            );
        }
        cache.authenticate(secret("a")).await.unwrap(); // A hit must not promote a to LRU tail.
        insert(
            &cache,
            verifier_cache_key("d").unwrap(),
            CachedAuth::Verifier(DeviceVerifier::new(identity(), [7; 32])),
            entry_bytes,
        );
        {
            let state = cache.state.lock().unwrap();
            assert!(
                !state
                    .entries
                    .contains_key(&cache_key(&secret("a")).unwrap())
            );
            assert!(
                state
                    .order
                    .iter()
                    .map(|k| k.credential_id.as_ref())
                    .eq(["b", "c", "d"])
            );
            assert_cache_consistent(&state, &cache.limits);
        }
        let key = cache_key(&secret("b")).unwrap();
        let old_expiry = cache.state.lock().unwrap().entries[&key].expires;
        insert(&cache, key.clone(), CachedAuth::Negative, 50);
        {
            let state = cache.state.lock().unwrap();
            assert!(!state.expirations.contains(&(old_expiry, key.clone())));
            assert_eq!(state.bytes, 250);
            assert!(
                state
                    .order
                    .iter()
                    .map(|k| k.credential_id.as_ref())
                    .eq(["c", "d", "b"])
            );
        }
        // Oversize replacement releases old ownership and cannot install a partial index node.
        insert(&cache, key, CachedAuth::Negative, max_bytes + 1);
        assert_eq!(cache.usage().unwrap(), (0, 0));
        consistent(&cache);
    }
}

#[tokio::test]
async fn every_invalidation_scope_releases_matching_and_negative_indices() {
    for scope in scopes() {
        let (cache, _) = fixture(Limits::default());
        let mut unrelated = identity();
        unrelated.device_key.tenant_id = TenantId::new("other").unwrap();
        unrelated.device_key.product_id = ProductId::new("other").unwrap();
        unrelated.device_key.device_id = DeviceId::new("other").unwrap();
        unrelated.credential_version = 99;
        unrelated.auth_generation = 99;
        for _ in 0..64 {
            for (id, value) in [
                ("positive", CachedAuth::Positive(identity())),
                (
                    "verifier",
                    CachedAuth::Verifier(DeviceVerifier::new(identity(), [7; 32])),
                ),
                ("negative", CachedAuth::Negative),
                ("other", CachedAuth::Positive(unrelated.clone())),
            ] {
                let key = if matches!(value, CachedAuth::Verifier(_)) {
                    verifier_cache_key(id).unwrap()
                } else {
                    cache_key(&secret(id)).unwrap()
                };
                insert(&cache, key, value, 100);
            }
            let old_epoch = cache.state.lock().unwrap().epoch;
            let removed = cache.invalidate(&scope).unwrap();
            assert!(removed.contains(&identity().device_key));
            let state = cache.state.lock().unwrap();
            assert_ne!(state.epoch, old_epoch);
            let all = matches!(scope, AuthInvalidation::All);
            assert_eq!(state.entries.len(), usize::from(!all));
            assert_eq!(state.bytes, usize::from(!all) * 100);
            assert_cache_consistent(&state, &cache.limits);
        }
    }
}

#[tokio::test]
async fn public_mixed_cache_admission_obeys_both_budgets_and_recovers_capacity() {
    for (count, bytes) in [(3, 10_000), (100, 500)] {
        let (cache, provider) = fixture(Limits {
            auth_cache_max_entries: count,
            auth_cache_max_bytes: bytes,
            ..Limits::default()
        });
        for round in 0..16 {
            for i in 0..64 {
                let id = format!("mixed-{i}");
                provider.mode.store(u8::from(i % 3 == 1), Ordering::SeqCst);
                if i % 3 == 2 {
                    cache
                        .verify_signed_with_verifier(&id, b"datagram", &tag())
                        .await
                        .unwrap();
                } else {
                    assert_eq!(cache.authenticate(secret(&id)).await.is_err(), i % 3 == 1);
                }
                consistent(&cache);
            }
            assert!(cache.metrics.get(Metric::AuthEvictions) > 0);
            if round % 2 == 0 {
                cache.invalidate(&AuthInvalidation::All).unwrap();
            } else {
                let mut state = cache.state.lock().unwrap();
                let keys: Vec<_> = state.entries.keys().cloned().collect();
                let due = Instant::now();
                for key in keys {
                    set_expiry(&mut state, &key, due);
                }
                prune_expired(&mut state);
            }
            assert_eq!(cache.usage().unwrap(), (0, 0));
            consistent(&cache);
        }
    }
}

#[tokio::test]
async fn epoch_fences_verified_ack_and_session_candidates_for_every_scope() {
    let (cache, provider) = fixture(Limits::default());
    for scope in scopes() {
        let candidate = cache.authenticate_candidate(secret("same")).await.unwrap();
        let verified = cache
            .verify_signed_with_verifier("same", b"datagram", &tag())
            .await
            .unwrap();
        let hit = cache
            .verify_signed_with_verifier("same", b"datagram", &tag())
            .await
            .unwrap();
        assert_eq!(
            cache
                .with_current_verifier(&hit, |v| v.sign(b"ACK"))
                .unwrap()
                .len(),
            32
        );
        let calls = provider.calls.load(Ordering::SeqCst);
        cache.invalidate(&scope).unwrap();
        assert!(!cache.candidate_is_current(&candidate).unwrap());
        for verified in [verified, hit] {
            assert!(matches!(
                cache.with_current_verifier::<()>(&verified, |_| panic!("stale signer called")),
                Err(Error::Unavailable)
            ));
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
        attempt(&cache, true).await.unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), calls + 1);
        consistent(&cache);
    }
    // An unrelated invalidation still conservatively fences an already verified request.
    let verified = cache
        .verify_signed_with_verifier("same", b"datagram", &tag())
        .await
        .unwrap();
    cache
        .invalidate(&AuthInvalidation::CredentialVersion { version: 99 })
        .unwrap();
    assert!(matches!(
        cache.with_current_verifier::<()>(&verified, |_| panic!("weak key fence")),
        Err(Error::Unavailable)
    ));
    assert_eq!(cache.usage().unwrap().0, 1);
    consistent(&cache);
}

#[tokio::test]
async fn one_hundred_twenty_eight_identical_misses_share_one_result_and_provider_call() {
    for (verifier, mode) in [(false, 0), (false, 1), (true, 0), (true, 1)] {
        let (cache, provider) = fixture(Limits::default());
        provider.blocked.store(true, Ordering::SeqCst);
        provider.mode.store(mode, Ordering::SeqCst);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..128 {
            let cache = cache.clone();
            tasks.spawn(async move { attempt(&cache, verifier).await });
        }
        wait_for_waiters(&cache, 127).await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        provider.blocked.store(false, Ordering::SeqCst);
        provider.release.add_permits(1);
        while let Some(result) = tasks.join_next().await {
            let result = result.unwrap();
            if mode == 0 {
                assert_eq!(result.unwrap().device_key, identity().device_key);
            } else {
                assert!(matches!(result, Err(Error::Authentication)));
            }
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            cache.inflight_usage().unwrap(),
            (0, cache.limits.auth_cache_max_waiters)
        );
        consistent(&cache);
    }
}

#[tokio::test]
async fn cancelled_or_panicked_leader_wakes_bounded_followers_on_both_paths() {
    for verifier in [false, true] {
        for panic_leader in [false, true] {
            let (cache, provider) = fixture(Limits::default());
            provider.blocked.store(true, Ordering::SeqCst);
            let first_cache = cache.clone();
            let leader = tokio::spawn(async move { attempt(&first_cache, verifier).await });
            wait_for_waiters(&cache, 0).await;
            let mut followers = tokio::task::JoinSet::new();
            for _ in 0..8 {
                let cache = cache.clone();
                followers.spawn(async move { attempt(&cache, verifier).await });
            }
            wait_for_waiters(&cache, 8).await;
            provider.blocked.store(false, Ordering::SeqCst);
            if panic_leader {
                provider.mode.store(3, Ordering::SeqCst);
                provider.release.add_permits(1);
            } else {
                leader.abort();
            }
            let error = leader.await.unwrap_err();
            assert_eq!(error.is_panic(), panic_leader);
            while let Some(result) = followers.join_next().await {
                result.unwrap().unwrap();
            }
            assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                cache.inflight_usage().unwrap(),
                (0, cache.limits.auth_cache_max_waiters)
            );
            consistent(&cache);
        }
    }
}

#[tokio::test]
async fn waiter_limit_timeout_and_unavailable_do_not_leave_cache_or_inflight_ownership() {
    for verifier in [false, true] {
        let (cache, provider) = fixture(Limits {
            auth_cache_max_waiters: 2,
            ..Limits::default()
        });
        provider.blocked.store(true, Ordering::SeqCst);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..3 {
            let cache = cache.clone();
            tasks.spawn(async move { attempt(&cache, verifier).await });
        }
        wait_for_waiters(&cache, 2).await;
        assert!(matches!(
            attempt(&cache, verifier).await,
            Err(Error::Overloaded)
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        provider.blocked.store(false, Ordering::SeqCst);
        provider.release.add_permits(1);
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        assert_eq!(cache.inflight_usage().unwrap(), (0, 2));
        consistent(&cache);

        let (cache, provider) = fixture(Limits {
            authentication_timeout_ms: 5,
            ..Limits::default()
        });
        provider.blocked.store(true, Ordering::SeqCst);
        assert!(matches!(
            attempt(&cache, verifier).await,
            Err(Error::Timeout)
        ));
        assert_eq!(cache.usage().unwrap(), (0, 0));
        assert_eq!(
            cache.inflight_usage().unwrap(),
            (0, cache.limits.auth_cache_max_waiters)
        );
        consistent(&cache);
        provider.blocked.store(false, Ordering::SeqCst);
        provider.mode.store(2, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(matches!(
                attempt(&cache, verifier).await,
                Err(Error::Unavailable)
            ));
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
        assert_eq!(cache.usage().unwrap(), (0, 0));
        provider.mode.store(0, Ordering::SeqCst);
        attempt(&cache, verifier).await.unwrap();
        consistent(&cache);
    }
}

#[tokio::test]
async fn every_invalidation_scope_fences_stale_provider_responses_on_both_paths() {
    for verifier in [false, true] {
        for scope in scopes() {
            let (cache, provider) = fixture(Limits::default());
            provider.blocked.store(true, Ordering::SeqCst);
            let first_cache = cache.clone();
            let first = tokio::spawn(async move { attempt(&first_cache, verifier).await });
            wait_for_waiters(&cache, 0).await;
            cache.invalidate(&scope).unwrap();
            provider.blocked.store(false, Ordering::SeqCst);
            provider.release.add_permits(1);
            assert!(matches!(first.await.unwrap(), Err(Error::Unavailable)));
            assert_eq!(cache.usage().unwrap(), (0, 0));
            assert_eq!(
                cache.inflight_usage().unwrap(),
                (0, cache.limits.auth_cache_max_waiters)
            );
            consistent(&cache);
            attempt(&cache, verifier).await.unwrap();
            assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
            consistent(&cache);
        }
    }
}

#[test]
#[ignore = "manual isolated expiry-index allocation measurement"]
fn expiry_index_memory_audit() {
    for count in [1, 64, 512, 4096] {
        let keys: Vec<_> = (0..count)
            .map(|i| verifier_cache_key(&format!("memory-{i}")).unwrap())
            .collect();
        let expiry = Instant::now() + Duration::from_secs(600);
        let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
        let mut tree = BTreeSet::new();
        for key in &keys {
            tree.insert((expiry, key.clone()));
        }
        let usage = region.change();
        let retained = usage.bytes_allocated - usage.bytes_deallocated;
        let struct_bytes = std::mem::size_of_val(&tree);
        drop(tree);
        let end = region.change();
        assert_eq!(end.bytes_allocated, end.bytes_deallocated);
        // Float formatting can initialize std storage: print outside measurement.
        eprintln!(
            "{{\"count\":{count},\"index_tuple_bytes\":{},\"index_struct_bytes\":{},\"index_retained_bytes\":{retained},\"index_bytes_per_entry\":{},\"index_allocations\":{}}}",
            std::mem::size_of::<(Instant, AuthCacheKey)>(),
            struct_bytes,
            retained as f64 / count as f64,
            usage.allocations
        );
    }
}
