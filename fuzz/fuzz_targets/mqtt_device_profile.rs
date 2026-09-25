#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use netbaiot_mqtt_wire::client::{decode, Limits, Version};

fuzz_target!(|data: &[u8]| {
    if data.len() > 131_072 { return; }
    for version in [Version::V311, Version::V5] {
        let mut input = BytesMut::new();
        for chunk in data.chunks(7) {
            input.extend_from_slice(chunk);
            loop {
                let before = input.len();
                match decode(&mut input, version, Limits::default()) {
                    Ok(Some(_)) => assert!(input.len() < before),
                    Ok(None) => { assert_eq!(input.len(), before); break; }
                    Err(_) => return,
                }
            }
        }
    }
});
