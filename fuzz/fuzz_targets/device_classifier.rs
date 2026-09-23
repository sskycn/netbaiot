#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    for length in 0..=data.len().min(12) {
        let prefix = &data[..length];
        for maximum in [1, 65_536, 1_048_576, usize::MAX] {
            let _ = netbaiot_transports::classifier::classify_prefix(prefix, maximum, 65_536);
        }
    }
});
