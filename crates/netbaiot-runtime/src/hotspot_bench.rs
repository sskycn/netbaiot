//! Isolated, serial release measurements. Setup, cleanup and sample storage are
//! outside the timed/allocation region. This is subsystem evidence, not capacity.
use std::{cell::Cell, time::Instant};

thread_local! {
    static TIMING_ENABLED: Cell<bool> = const { Cell::new(false) };
    static LAST_LOCK_NS: Cell<(u128, u128)> = const { Cell::new((0, 0)) };
}

/// Test binary only. Declare before the mutex guard so hold time includes its
/// release. No clock reads or probe state are added to the production runtime.
pub(crate) struct LockClock {
    started: Option<Instant>,
    acquired: Option<Instant>,
    wait_ns: u128,
}

impl LockClock {
    pub(crate) fn start() -> Self {
        Self {
            started: TIMING_ENABLED.get().then(Instant::now),
            acquired: None,
            wait_ns: 0,
        }
    }

    pub(crate) fn acquired(&mut self) {
        if let Some(started) = self.started {
            self.wait_ns = started.elapsed().as_nanos();
            self.acquired = Some(Instant::now());
        }
    }
}

impl Drop for LockClock {
    fn drop(&mut self) {
        if let Some(acquired) = self.acquired {
            LAST_LOCK_NS.set((self.wait_ns, acquired.elapsed().as_nanos()));
        }
    }
}

pub(crate) fn measure<T, R>(
    name: &str,
    size: usize,
    iterations: usize,
    mut setup: impl FnMut() -> T,
    mut action: impl FnMut(T) -> R,
    mut cleanup: impl FnMut(R),
) {
    if cfg!(debug_assertions) {
        panic!("run hotspot benchmarks in release mode");
    }
    TIMING_ENABLED.set(true);
    for _ in 0..32 {
        cleanup(action(setup()));
    }
    for run in 1..=3 {
        let mut samples = Vec::with_capacity(iterations);
        let mut allocations = 0;
        let mut allocated_bytes = 0;
        let mut wait_ns = 0;
        let mut hold_ns = 0;
        for _ in 0..iterations {
            let input = setup();
            let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
            LAST_LOCK_NS.set((0, 0));
            let started = Instant::now();
            let result = std::hint::black_box(action(input));
            let elapsed = started.elapsed().as_nanos();
            let stats = region.change();
            let (wait, hold) = LAST_LOCK_NS.get();
            wait_ns += wait;
            hold_ns += hold;
            allocations += stats.allocations;
            allocated_bytes += stats.bytes_allocated;
            cleanup(result);
            samples.push(elapsed);
        }
        let total = samples.iter().sum::<u128>();
        samples.sort_unstable();
        println!(
            "RUNTIME_HOTSPOT,{name},{size},{run},{:.0},{},{},{},{:.3},{:.3},{:.3},{:.3}",
            iterations as f64 * 1e9 / total as f64,
            samples[iterations / 2],
            samples[iterations * 95 / 100],
            samples[iterations * 99 / 100],
            allocations as f64 / iterations as f64,
            allocated_bytes as f64 / iterations as f64,
            wait_ns as f64 / iterations as f64,
            hold_ns as f64 / iterations as f64,
        );
    }
    TIMING_ENABLED.set(false);
}

pub(crate) fn enable_clock(enabled: bool) {
    TIMING_ENABLED.set(enabled);
    LAST_LOCK_NS.set((0, 0));
}
pub(crate) fn last_lock() -> (u128, u128) {
    LAST_LOCK_NS.get()
}

#[test]
#[ignore = "isolated serial release TCP ACK encoding/allocation comparison"]
fn tcp_ack_encoding() {
    use netbaiot_core::{EventAccepted, EventId, tcp_ack};
    let receipt = EventAccepted {
        event_id: EventId::generate(),
        accepted_at: 1_792_108_800_000,
        required_deliveries: 1,
        best_effort_deliveries: 0,
    };
    // Exact previous TCP hot path: serde JSON followed by LengthPrefixFramer::encode.
    // This test-only reference is not a supported wire codec or production fallback.
    measure(
        "tcp_ack_json_before",
        1,
        100_000,
        || (),
        |_| {
            let bytes = serde_json::to_vec(std::hint::black_box(&receipt)).unwrap();
            let mut frame = Vec::with_capacity(bytes.len() + 4);
            frame.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_be_bytes());
            frame.extend_from_slice(&bytes);
            frame
        },
        drop,
    );
    measure(
        "tcp_ack_binary_after",
        1,
        100_000,
        || (),
        |_| tcp_ack::encode(std::hint::black_box(&receipt)).unwrap(),
        |_| {},
    );
}

#[test]
#[ignore = "isolated serial release TCP ACK decoding/allocation comparison"]
fn tcp_ack_decoding() {
    use netbaiot_core::{EventAccepted, EventId, tcp_ack};
    let frame = tcp_ack::encode(&EventAccepted {
        event_id: EventId::generate(),
        accepted_at: 1_792_108_800_000,
        required_deliveries: 1,
        best_effort_deliveries: 0,
    })
    .unwrap();
    measure(
        "tcp_ack_decode_body",
        1,
        100_000,
        || (),
        |_| tcp_ack::decode(std::hint::black_box(&frame[4..])).unwrap(),
        |_| {},
    );
    measure(
        "tcp_ack_decode_frame",
        1,
        100_000,
        || (),
        |_| tcp_ack::decode_frame(std::hint::black_box(&frame)).unwrap(),
        |_| {},
    );
}
