use super::*;

fn device(tenant: &str, id: &str) -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new(tenant).unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new(id).unwrap(),
    }
}
fn setup(global: usize, tenant: usize, local: usize) -> Arc<Connections> {
    Connections::new(
        Arc::new(Limits {
            max_connections: 16,
            max_connections_per_ip: 16,
            max_connections_per_tenant: 16,
            connection_memory_reservation: 16,
            global_connection_logical_bytes: global,
            max_connection_bytes_per_tenant: tenant,
            max_read_buffer_per_connection: local,
            ..Limits::default()
        }),
        Arc::new(Metrics::default()),
    )
}
fn lease(owner: &Arc<Connections>, tenant: &str, id: &str) -> ConnectionLease {
    let mut lease = owner
        .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
        .unwrap();
    lease.authenticate(&device(tenant, id)).unwrap();
    lease
}
fn clean(owner: &Connections, global: usize) {
    assert_eq!(owner.memory.available(), global);
    let counts = owner
        .counts
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert!(counts.ips.is_empty());
    assert!(counts.devices.is_empty());
    assert!(counts.tenants.is_empty());
    assert_eq!(counts.transports, [0; 3]);
}

#[test]
fn replacement_accounts_full_peak_then_stable_capacity() {
    let owner = setup(89, 89, 64);
    let mut lease = lease(&owner, "t", "d");
    let mut budget = lease.read_budget();
    let mut old = budget.reserve_allocation(9).unwrap();
    let replacement = budget.reserve_replacement(64, &mut old).unwrap();
    assert_eq!(budget.capacity().unwrap(), 73);
    assert_eq!(owner.memory.available(), 0);
    assert_eq!(
        lock(&owner.counts).unwrap().tenants[&device("t", "d").tenant_id].read_bytes,
        73
    );
    assert!(matches!(
        budget.reserve_allocation(1),
        Err(Error::Overloaded)
    ));
    // Early commit is an error and preserves the pending owner for rollback.
    let (error, replacement) = match replacement.commit() {
        Err(error) => error,
        Ok(_) => panic!("cannot commit while stable capacity exceeds the limit"),
    };
    assert!(matches!(error, Error::Invalid));
    assert_eq!(budget.capacity().unwrap(), 73);
    drop(old);
    let new = match replacement.commit() {
        Ok(new) => new,
        Err(_) => panic!("old allocation has been released"),
    };
    assert_eq!(budget.capacity().unwrap(), 64);
    assert_eq!(owner.memory.available(), 9);
    drop((new, budget, lease));
    clean(&owner, 89);
}

#[test]
fn replacement_abort_releases_new_and_allows_retry() {
    let owner = setup(144, 144, 64);
    let mut lease = lease(&owner, "t", "d");
    let mut budget = lease.read_budget();
    let mut old = budget.reserve_allocation(64).unwrap();
    let replacement = budget.reserve_replacement(64, &mut old).unwrap();
    assert_eq!(budget.capacity().unwrap(), 128);
    drop(replacement);
    assert_eq!(budget.capacity().unwrap(), 64);
    assert_eq!(owner.memory.available(), 64);
    let retry = budget.reserve_replacement(64, &mut old).unwrap();
    drop((retry, old, budget, lease));
    clean(&owner, 144);
}

#[test]
fn replacement_failure_preserves_global_tenant_and_local_accounts() {
    for (global, tenant) in [(88, 144), (144, 88)] {
        let owner = setup(global, tenant, 64);
        let mut lease = lease(&owner, "t", "d");
        let mut budget = lease.read_budget();
        let mut old = budget.reserve_allocation(9).unwrap();
        assert!(matches!(
            budget.reserve_replacement(64, &mut old),
            Err(Error::Overloaded)
        ));
        assert_eq!(budget.capacity().unwrap(), 9);
        assert_eq!(owner.memory.available(), global - 25);
        assert_eq!(
            lock(&owner.counts).unwrap().tenants[&device("t", "d").tenant_id].read_bytes,
            9
        );
        let one = budget.reserve_allocation(1).unwrap();
        drop((one, old, budget, lease));
        clean(&owner, global);
    }
}

#[test]
fn replacement_and_expansion_reject_foreign_owners_and_overflow() {
    let owner = setup(144, 144, 64);
    let mut a = lease(&owner, "t", "a");
    let mut b = lease(&owner, "t", "b");
    let mut a_budget = a.read_budget();
    let mut b_budget = b.read_budget();
    let mut old = a_budget.reserve_allocation(9).unwrap();
    assert!(matches!(
        b_budget.reserve_replacement(64, &mut old),
        Err(Error::Invalid)
    ));
    assert!(matches!(
        b_budget.expand_allocation(&mut old, 64),
        Err(Error::Invalid)
    ));
    assert!(matches!(
        a_budget.reserve_replacement(usize::MAX, &mut old),
        Err(Error::Overloaded)
    ));
    assert!(matches!(
        a_budget.expand_allocation(&mut old, usize::MAX),
        Err(Error::Overloaded)
    ));
    assert_eq!(a_budget.capacity().unwrap(), 9);
    assert_eq!(b_budget.capacity().unwrap(), 0);
    drop((old, a_budget, b_budget, a, b));
    clean(&owner, 144);
}

#[test]
fn inline_expansion_fits_exact_aggregate_limits_and_releases_merged_permits() {
    let owner = setup(80, 80, 64);
    let mut lease = lease(&owner, "t", "d");
    let mut budget = lease.read_budget();
    let mut allocation = budget.reserve_allocation(8).unwrap();
    budget.expand_allocation(&mut allocation, 64).unwrap();
    assert_eq!(budget.capacity().unwrap(), 64);
    assert_eq!(owner.memory.available(), 0);
    assert!(matches!(
        budget.expand_allocation(&mut allocation, 65),
        Err(Error::Overloaded)
    ));
    assert_eq!(budget.capacity().unwrap(), 64);
    drop((allocation, budget, lease));
    clean(&owner, 80);
}

#[test]
fn poisoned_reader_cleanup_releases_tenant_bytes_and_keeps_admission_closed() {
    let owner = setup(128, 128, 64);
    let mut lease = lease(&owner, "t", "d");
    let mut budget = lease.read_budget();
    let allocation = budget.reserve_allocation(16).unwrap();
    let state = budget.state.clone();
    assert!(
        std::panic::catch_unwind(move || {
            let _held = state.lock().unwrap();
            panic!("intentional read-state poison");
        })
        .is_err()
    );
    drop(allocation);
    assert!(matches!(budget.reserve_allocation(1), Err(Error::Internal)));
    drop((budget, lease));
    clean(&owner, 128);
}

#[test]
fn poisoned_connection_cleanup_releases_all_known_ownership() {
    let owner = setup(128, 128, 64);
    let mut lease = lease(&owner, "t", "d");
    let mut budget = lease.read_budget();
    let allocation = budget.reserve_allocation(16).unwrap();
    let poison = owner.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _held = poison.counts.lock().unwrap();
            panic!("intentional counts poison");
        }))
        .is_err()
    );
    drop((allocation, budget, lease));
    clean(&owner, 128);
    assert!(owner.counts.is_poisoned());
    assert!(matches!(
        owner.acquire_pending("127.0.0.1".parse().unwrap()),
        Err(Error::Internal)
    ));
    clean(&owner, 128);
}

#[test]
fn concurrent_global_reservations_remain_exact_for_shared_and_distinct_tenants() {
    for shared in [false, true] {
        let owner = setup(384, 384, 32);
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let mut workers = Vec::new();
        for n in 0..16 {
            let tenant = if shared {
                "t".to_owned()
            } else {
                format!("t{n}")
            };
            let mut lease = lease(&owner, &tenant, &format!("d{n}"));
            let mut budget = lease.read_budget();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                let allocation = budget.reserve_allocation(32);
                (lease, budget, allocation)
            }));
        }
        let owners: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(
            owners
                .iter()
                .filter(|(_, _, allocation)| allocation.is_ok())
                .count(),
            4
        );
        assert_eq!(owner.memory.available(), 0);
        assert_eq!(
            lock(&owner.counts)
                .unwrap()
                .tenants
                .values()
                .map(|t| t.read_bytes)
                .sum::<usize>(),
            128
        );
        drop(owners);
        clean(&owner, 384);
    }
}

#[test]
fn concurrent_replacement_peaks_remain_bounded_and_return_every_permit() {
    for shared_tenant in [false, true] {
        let owner = setup(528, 528, 64);
        let start = Arc::new(std::sync::Barrier::new(16));
        let held = Arc::new(std::sync::Barrier::new(17));
        let release = Arc::new(std::sync::Barrier::new(17));
        let mut workers = Vec::new();
        for n in 0..16 {
            let tenant = if shared_tenant {
                "t".to_owned()
            } else {
                format!("t{n}")
            };
            let mut lease = lease(&owner, &tenant, &format!("d{n}"));
            let mut budget = lease.read_budget();
            let mut old = budget.reserve_allocation(9).unwrap();
            let (start, held, release) = (start.clone(), held.clone(), release.clone());
            workers.push(std::thread::spawn(move || {
                start.wait();
                let replacement = budget.reserve_replacement(64, &mut old);
                held.wait();
                release.wait();
                let admitted = replacement.is_ok();
                if let Ok(replacement) = replacement {
                    drop(old);
                    let new = match replacement.commit() {
                        Ok(new) => new,
                        Err(_) => panic!("exclusive replacement commits after old release"),
                    };
                    assert_eq!(budget.capacity().unwrap(), 64);
                    drop(new);
                } else {
                    drop(old);
                }
                drop((budget, lease));
                admitted
            }));
        }
        held.wait();
        assert_eq!(owner.memory.available(), 0);
        let counts = owner.counts.lock().unwrap();
        assert_eq!(
            counts
                .tenants
                .values()
                .map(|tenant| tenant.read_bytes)
                .sum::<usize>(),
            272
        );
        drop(counts);
        release.wait();
        let admitted = workers
            .into_iter()
            .map(|worker| usize::from(worker.join().unwrap()))
            .sum::<usize>();
        assert_eq!(admitted, 2);
        clean(&owner, 528);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_connection_releases_authentication_and_allocations() {
    let owner = setup(128, 128, 64);
    let task_owner = owner.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut lease = lease(&task_owner, "t", "d");
        let mut budget = lease.read_budget();
        let _allocation = budget.reserve_allocation(16).unwrap();
        ready_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready_rx.await.unwrap();
    assert_eq!(owner.memory.available(), 96);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    clean(&owner, 128);
}
