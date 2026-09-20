#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_runtime::{Limits, decode_spool_records};

fuzz_target!(|data: &[u8]| {
    let limits = Limits {
        spool_segment_max_bytes: 1_048_576,
        spool_max_bytes: 1_048_576,
        ..Limits::default()
    };
    if data.len() <= limits.spool_segment_max_bytes {
        let _ = decode_spool_records(data, &limits);
    }
});
