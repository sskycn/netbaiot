#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    if data.len()>65540 { return; }
    let _ = netbaiot_transports::udp::decode(data, 1200);
});
