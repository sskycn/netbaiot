//! Isolated allocation and future-layout measurement; these are not RSS or
//! kernel socket measurements. Run this binary alone to avoid allocator noise.
use bytes::BytesMut;
use netbaiot_core::{DeviceId, DeviceKey, ProductId, TenantId, Transport};
use netbaiot_runtime::{Connections, Error, Limits, Metrics};
use netbaiot_transports::Reader;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{alloc::System, mem::size_of, sync::Arc, time::Duration};
use tokio::time::Instant;

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[test]
#[ignore = "allocation experiment; run alone with --ignored --nocapture"]
fn reader_memory_profile() {
    const COUNT: usize = 10_000;
    let mut readers = Vec::with_capacity(COUNT);
    let region = Region::new(ALLOCATOR);
    for _ in 0..COUNT {
        readers.push(Reader::new(1_048_576, 30_000));
    }
    let allocated = region.change();
    let capacity: usize = readers.iter().map(|r| r.buffer.capacity()).sum();
    println!(
        "reader_idle samples={COUNT} reader_struct={} allocator_bytes={} allocations={} reserved_buffer_capacity={} per_reader_heap={} per_reader_capacity={}",
        size_of::<Reader>(),
        allocated.bytes_allocated,
        allocated.allocations,
        capacity,
        allocated.bytes_allocated / COUNT,
        capacity / COUNT,
    );
    let mut reader = Reader::new(1_048_576, 30_000);
    let mut stream = tokio::io::empty();
    let future = reader.read_more(&mut stream, Instant::now() + Duration::from_secs(60));
    println!("read_more_future_bytes={}", std::mem::size_of_val(&future));
    drop(future);
    readers.clear();
    println!("reader_cleanup_excluding_vector={:?}", region.change());

    let limits = Arc::new(Limits {
        max_connections: COUNT,
        max_connections_per_ip: COUNT,
        connection_memory_reservation: 1_024,
        ..Limits::default()
    });
    let connections = Connections::new(limits, Arc::new(Metrics::default()));
    let mut leases: Vec<_> = (0..COUNT)
        .map(|_| {
            connections
                .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
                .unwrap()
        })
        .collect();
    let metered_region = Region::new(ALLOCATOR);
    for lease in &mut leases {
        readers.push(Reader::with_budget(65_540, 30_000, lease.read_budget()));
    }
    let metered = metered_region.change();
    println!(
        "reader_metered_idle samples={COUNT} allocator_bytes={} allocations={} per_reader_read_account_heap={} buffer_capacity={}",
        metered.bytes_allocated,
        metered.allocations,
        metered.bytes_allocated / COUNT,
        readers
            .iter()
            .map(|reader| reader.buffer.capacity())
            .sum::<usize>(),
    );
    drop(readers);
    drop(leases);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        for length in [2, 65_540] {
            let source = vec![7; length];
            let mut input = source.as_slice();
            let mut reader = Reader::new(65_540, 30_000);
            let region = Region::new(ALLOCATOR);
            while reader.buffer.len() != length {
                reader.read_more_to(&mut input, Instant::now() + Duration::from_secs(60), length).await.unwrap();
            }
            let allocated = region.change();
            println!(
                "reader_frame length={length} buffer_capacity={} allocator_bytes={} allocations={} reallocations={} realloc_bytes={}",
                reader.buffer.capacity(),
                allocated.bytes_allocated,
                allocated.allocations,
                allocated.reallocations,
                allocated.bytes_reallocated,
            );
            let frame = reader.take_frame(length).unwrap();
            reader.consumed();
            drop(frame);
            let reclaimed = region.change();
            println!(
                "reader_frame_reclaimed length={length} buffer_capacity={} net_requested_heap={}",
                reader.buffer.capacity(),
                reclaimed.bytes_allocated as i128 - reclaimed.bytes_deallocated as i128,
            );
            assert_eq!(reader.buffer.capacity(), 0);
        }
    });
}

#[test]
#[ignore = "isolated requested heap peak experiment; run alone with --ignored --nocapture"]
fn reader_replacement_requested_heap_peak() {
    const MAXIMUM: usize = 65_540;
    const PREFIXES: [usize; 5] = [9, 64, 4096, 32_768, 65_539];
    let mut measurements = Vec::with_capacity(PREFIXES.len());
    for prefix in PREFIXES {
        let fixed = Limits::default().connection_memory_reservation;
        let aggregate = 2 * fixed + prefix + MAXIMUM;
        let limits = Arc::new(Limits {
            max_connections: 3,
            max_connections_per_ip: 3,
            max_connections_per_tenant: 2,
            max_connections_per_device: 1,
            max_ingress_waiters: 3,
            global_connection_logical_bytes: aggregate,
            max_connection_bytes_per_tenant: aggregate,
            max_read_buffer_per_connection: MAXIMUM,
            ..Limits::default()
        });
        limits.validate().unwrap();
        let owner = Connections::new(limits, Arc::new(Metrics::default()));
        let peer = "127.0.0.1".parse().unwrap();
        let first_key = DeviceKey {
            tenant_id: TenantId::new("peak").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("first").unwrap(),
        };
        let second_key = DeviceKey {
            device_id: DeviceId::new("second").unwrap(),
            ..first_key.clone()
        };
        let mut first = owner.acquire(peer, Transport::Tcp).unwrap();
        let mut second = owner.acquire(peer, Transport::Tcp).unwrap();
        first.authenticate(&first_key).unwrap();
        second.authenticate(&second_key).unwrap();
        let mut budget = first.read_budget();
        let mut other_budget = second.read_budget();
        let mut old_permit = budget.reserve_allocation(prefix).unwrap();
        // Warm the allocator thread's semaphore-release bookkeeping before
        // measuring payload storage, including the otherwise spare slot.
        let warm_bytes = other_budget.reserve_allocation(1).unwrap();
        drop(warm_bytes);
        let warm_connection = owner.acquire_pending(peer).unwrap();
        drop(warm_connection);

        // All account, index, and output storage exists before this region.
        // Only the two byte allocations contribute to the requested heap peak.
        let region = Region::new(ALLOCATOR);
        let mut old = BytesMut::with_capacity(prefix);
        assert_eq!(old.capacity(), prefix);
        old.resize(prefix, 7);
        let replacement = budget
            .reserve_replacement(MAXIMUM, &mut old_permit)
            .unwrap();
        let mut new = BytesMut::with_capacity(MAXIMUM);
        assert_eq!(new.capacity(), MAXIMUM);
        new.extend_from_slice(&old);
        new.resize(MAXIMUM, 8);
        let peak = region.change();
        let peak_live = peak.bytes_allocated - peak.bytes_deallocated;
        let peak_charged = budget.capacity().unwrap();
        assert_eq!(peak_live, old.capacity() + new.capacity());
        assert_eq!(peak_charged, peak_live);
        assert_eq!(peak.allocations, 2);
        assert_eq!(peak.reallocations, 0);
        // The other reader has no local transition. Its next byte fails at the
        // tenant ceiling; the spare connection slot fails at global bytes.
        assert!(matches!(
            other_budget.reserve_allocation(1),
            Err(Error::Overloaded)
        ));
        assert!(matches!(
            owner.acquire_pending(peer),
            Err(Error::Overloaded)
        ));
        assert_eq!(budget.capacity().unwrap(), peak_charged);
        assert_eq!(other_budget.capacity().unwrap(), 0);

        drop(old);
        drop(old_permit);
        let new_permit = match replacement.commit() {
            Ok(permit) => permit,
            Err(_) => panic!("the old allocation and permit have been released"),
        };
        let stable = region.change();
        let stable_live = stable.bytes_allocated - stable.bytes_deallocated;
        let stable_charged = budget.capacity().unwrap();
        assert_eq!(
            stable_live, MAXIMUM,
            "prefix={prefix} peak={peak:?} stable={stable:?}"
        );
        assert_eq!(stable_charged, MAXIMUM);
        // Exactly the old allocation's bytes are reusable by another reader.
        let reused = other_budget.reserve_allocation(prefix).unwrap();
        assert!(matches!(
            other_budget.reserve_allocation(1),
            Err(Error::Overloaded)
        ));
        drop(reused);

        drop(new);
        drop(new_permit);
        let cleanup = region.change();
        let cleanup_live = cleanup.bytes_allocated - cleanup.bytes_deallocated;
        assert_eq!(cleanup_live, 0);
        assert_eq!(budget.capacity().unwrap(), 0);
        assert_eq!(other_budget.capacity().unwrap(), 0);
        assert_eq!(cleanup.allocations, 2);
        assert_eq!(cleanup.deallocations, 2);
        assert_eq!(cleanup.reallocations, 0);

        // These auxiliary allocations predate the measurement region. Reuse
        // the same IP, tenant, and devices after disconnect, then fill both
        // aggregate budgets exactly to detect stale count or byte ownership.
        drop((budget, other_budget, first, second));
        assert_eq!(owner.active().unwrap(), [0; 3]);
        let mut first = owner.acquire(peer, Transport::Tcp).unwrap();
        let mut second = owner.acquire(peer, Transport::Tcp).unwrap();
        first.authenticate(&first_key).unwrap();
        second.authenticate(&second_key).unwrap();
        let mut budget = first.read_budget();
        let mut other_budget = second.read_budget();
        let full = budget.reserve_allocation(MAXIMUM).unwrap();
        let rest = other_budget.reserve_allocation(prefix).unwrap();
        assert!(matches!(
            other_budget.reserve_allocation(1),
            Err(Error::Overloaded)
        ));
        drop((full, rest, budget, other_budget, first, second));
        assert_eq!(owner.active().unwrap(), [0; 3]);
        measurements.push((
            prefix,
            peak_live,
            peak_charged,
            stable_live,
            stable_charged,
            cleanup_live,
        ));
    }
    for (prefix, peak_live, peak_charged, stable_live, stable_charged, cleanup_live) in measurements
    {
        println!(
            "reader_replacement_heap prefix={prefix} maximum={MAXIMUM} peak_requested_live={peak_live} peak_charged={peak_charged} stable_requested_live={stable_live} stable_charged={stable_charged} cleanup_requested_live={cleanup_live} cleanup_charged=0 allocation_count=2 reallocation_count=0 aggregate_limits_unchanged=true aggregate_peak_full=true index_reuse_verified=true"
        );
    }
}
