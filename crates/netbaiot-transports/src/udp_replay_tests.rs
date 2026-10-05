// Frozen ef8d54c implementation used only for differential tests and paired microbenchmarks.
use super::*;
struct LegacyReplayWindow {
    entries: HashMap<(DeviceKey, [u8; 16], u32), Replay>,
    limits: Arc<Limits>,
}
impl LegacyReplayWindow {
    fn new(limits: Arc<Limits>) -> Self {
        Self {
            entries: HashMap::new(),
            limits,
        }
    }
    fn check(
        &mut self,
        device: &DeviceKey,
        credential_version: u32,
        boot: [u8; 16],
        seq: u64,
        timestamp: i64,
        now: i64,
    ) -> Result<ReplayDecision> {
        if now.abs_diff(timestamp) > self.limits.udp_clock_skew_ms {
            return Err(Error::Authentication);
        }
        self.entries.retain(|_, v| v.expires > now);
        let key = (device.clone(), boot, credential_version);
        if let Some(entry) = self.entries.get(&key) {
            if seq <= entry.max {
                let delta = entry.max - seq;
                if delta >= 64 {
                    return Err(Error::Conflict);
                }
                if entry.bits & (1 << delta) != 0 {
                    return Ok(ReplayDecision::AcceptedDuplicate);
                }
            }
        } else if self.entries.len() >= self.limits.max_replay_entries
            || self.entries.keys().filter(|(d, _, _)| d == device).count()
                >= self.limits.max_replay_entries_per_device
            || self
                .entries
                .keys()
                .filter(|(d, _, _)| d.tenant_id == device.tenant_id)
                .count()
                >= self.limits.max_replay_entries_per_tenant
        {
            return Err(Error::Overloaded);
        }
        Ok(ReplayDecision::New)
    }
    /// Called only after a successful New check and EventAccepted, with no intervening
    /// replay mutation. The single UDP receive-loop owner guarantees this ordering.
    fn commit(
        &mut self,
        device: DeviceKey,
        credential_version: u32,
        boot: [u8; 16],
        seq: u64,
        now: i64,
    ) {
        let e = self
            .entries
            .entry((device, boot, credential_version))
            .or_insert(Replay {
                max: seq,
                bits: 0,
                expires: 0,
            });
        if seq > e.max {
            let delta = seq - e.max;
            e.bits = if delta >= 64 { 0 } else { e.bits << delta };
            e.max = seq;
        }
        let delta = e.max.saturating_sub(seq);
        if delta < 64 {
            e.bits |= 1 << delta;
        }
        e.expires = now.saturating_add(self.limits.replay_ttl_ms as i64);
    }
}

fn device(tenant: usize, id: usize) -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new(format!("tenant-{tenant}")).unwrap(),
        product_id: ProductId::new("product").unwrap(),
        device_id: DeviceId::new(format!("device-{id}")).unwrap(),
    }
}

fn assert_indexes(window: &ReplayWindow) {
    assert_eq!(window.expirations.len(), window.entries.len());
    assert_eq!(window.devices.values().sum::<usize>(), window.entries.len());
    assert_eq!(window.tenants.values().sum::<usize>(), window.entries.len());
    for (key, value) in &window.entries {
        assert!(window.expirations.contains(&expiry_key(value.expires, key)));
        assert_eq!(
            window.devices[&key.0],
            window
                .entries
                .keys()
                .filter(|candidate| candidate.0 == key.0)
                .count()
        );
        assert_eq!(
            window.tenants[&key.0.tenant_id],
            window
                .entries
                .keys()
                .filter(|candidate| candidate.0.tenant_id == key.0.tenant_id)
                .count()
        );
    }
    assert!(window.entries.len() <= window.limits.max_replay_entries);
    assert!(
        window
            .devices
            .values()
            .all(|count| *count <= window.limits.max_replay_entries_per_device)
    );
    assert!(
        window
            .tenants
            .values()
            .all(|count| *count <= window.limits.max_replay_entries_per_tenant)
    );
}

#[test]
fn replay_indexes_match_legacy_under_expiry_rotation_duplicates_and_clock_reversal() {
    let limits = Arc::new(Limits {
        max_replay_entries: 64,
        max_replay_entries_per_tenant: 16,
        max_replay_entries_per_device: 3,
        replay_ttl_ms: 25,
        ..Limits::default()
    });
    let mut window = ReplayWindow::new(limits.clone());
    let mut old = LegacyReplayWindow::new(limits);
    let mut random = 12345u64;
    for step in 0..10_000 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let key = device((random >> 12) as usize % 4, (random >> 4) as usize % 8);
        let version = (random >> 20) as u32 % 3;
        let boot = [(random >> 25) as u8 % 3; 16];
        let seq = random % 100;
        let now = step / 50 + (random % 10) as i64; // Includes backwards clock steps.
        let result = window.check(&key, version, boot, seq, now, now);
        let expected = old.check(&key, version, boot, seq, now, now);
        assert_eq!(format!("{result:?}"), format!("{expected:?}"));
        if matches!(result, Ok(ReplayDecision::New)) {
            window.commit(key.clone(), version, boot, seq, now);
            old.commit(key, version, boot, seq, now);
        }
        assert_indexes(&window);
        assert_eq!(window.entries.len(), old.entries.len());
        for (key, entry) in &window.entries {
            let expected = &old.entries[key];
            assert_eq!(
                (entry.max, entry.bits, entry.expires),
                (expected.max, expected.bits, expected.expires)
            );
        }
    }
    window.prune(i64::MAX);
    assert_indexes(&window);
    assert!(window.devices.is_empty());
    assert!(window.tenants.is_empty());
}

#[test]
fn replay_refresh_never_accumulates_expiry_nodes() {
    let mut window = ReplayWindow::new(Arc::new(Limits::default()));
    let key = device(0, 0);
    for seq in 0..20_000 {
        let now = if seq % 2 == 0 { 100 } else { 99 };
        assert_eq!(
            window.check(&key, 1, [0; 16], seq, now, now).unwrap(),
            ReplayDecision::New
        );
        window.commit(key.clone(), 1, [0; 16], seq, now);
        assert_indexes(&window);
        assert_eq!(window.entries.len(), 1);
    }
}

#[test]
#[ignore = "release-only paired replay-window microbenchmark"]
fn replay_hot_path_probe() {
    use std::{hint::black_box, time::Instant};
    let iterations = 20_000;
    for depth in [0, 64, 256, 1024] {
        for path in ["duplicate", "new-key", "check-commit"] {
            let mut old_samples = Vec::new();
            let mut new_samples = Vec::new();
            for pair in 0..7 {
                let limits = Arc::new(Limits {
                    max_replay_entries: 2048,
                    max_replay_entries_per_tenant: 2048,
                    replay_ttl_ms: 1_000_000,
                    ..Limits::default()
                });
                let mut old = LegacyReplayWindow::new(limits.clone());
                let mut new = ReplayWindow::new(limits);
                for index in 0..depth {
                    old.commit(device(0, index), 1, [0; 16], 1, 10_000);
                    new.commit(device(0, index), 1, [0; 16], 1, 10_000);
                }
                let key = if path == "new-key" || depth == 0 {
                    device(0, 2048)
                } else {
                    device(0, depth - 1)
                };
                // Alternate order; equal work, same baseline source, no network/crypto.
                for optimized in if pair % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let start = Instant::now();
                    for index in 0..iterations {
                        let now = 10_000 + if path == "check-commit" { index } else { 0 };
                        let seq = if path == "check-commit" {
                            index as u64 + 2
                        } else {
                            1
                        };
                        if optimized {
                            let result = new.check(black_box(&key), 1, [0; 16], seq, now, now);
                            if path == "check-commit" {
                                assert_eq!(result.unwrap(), ReplayDecision::New);
                                new.commit(key.clone(), 1, [0; 16], seq, now);
                            } else {
                                black_box(result).unwrap();
                            }
                        } else {
                            let result = old.check(black_box(&key), 1, [0; 16], seq, now, now);
                            if path == "check-commit" {
                                assert_eq!(result.unwrap(), ReplayDecision::New);
                                old.commit(key.clone(), 1, [0; 16], seq, now);
                            } else {
                                black_box(result).unwrap();
                            }
                        }
                    }
                    let ns = start.elapsed().as_nanos() / iterations as u128;
                    if optimized {
                        new_samples.push(ns);
                    } else {
                        old_samples.push(ns);
                    }
                }
            }
            old_samples.sort_unstable();
            new_samples.sort_unstable();
            println!(
                "entries={depth} path={path} baseline_ns={} indexed_ns={}",
                old_samples[3], new_samples[3]
            );
        }
    }
}
