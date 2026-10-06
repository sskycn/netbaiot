//! Isolated, serial release measurements. Setup, cleanup and sample storage are
//! outside the timed/allocation region. This is subsystem evidence, not capacity.
use std::time::Instant;

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
    for _ in 0..32 {
        cleanup(action(setup()));
    }
    for run in 1..=3 {
        let mut samples = Vec::with_capacity(iterations);
        let mut allocations = 0;
        let mut allocated_bytes = 0;
        for _ in 0..iterations {
            let input = setup();
            let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
            let started = Instant::now();
            let result = std::hint::black_box(action(input));
            let elapsed = started.elapsed().as_nanos();
            let stats = region.change();
            allocations += stats.allocations;
            allocated_bytes += stats.bytes_allocated;
            cleanup(result);
            samples.push(elapsed);
        }
        let total = samples.iter().sum::<u128>();
        samples.sort_unstable();
        println!(
            "RUNTIME_HOTSPOT,{name},{size},{run},{:.0},{},{},{},{:.3},{:.3}",
            iterations as f64 * 1e9 / total as f64,
            samples[iterations / 2],
            samples[iterations * 95 / 100],
            samples[iterations * 99 / 100],
            allocations as f64 / iterations as f64,
            allocated_bytes as f64 / iterations as f64,
        );
    }
}
