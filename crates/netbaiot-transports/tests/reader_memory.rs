//! Isolated allocation and future-layout measurement; these are not RSS or
//! kernel socket measurements. Run this binary alone to avoid allocator noise.
use netbaiot_core::Transport;
use netbaiot_runtime::{Connections, Limits, Metrics};
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
