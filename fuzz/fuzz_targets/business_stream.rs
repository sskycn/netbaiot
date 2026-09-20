#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};

fuzz_target!(|data: &[u8]| {
    let framer = LengthPrefixFramer { maximum: 65_536 };
    let mut input = BytesMut::from(data);
    for _ in 0..128 {
        match framer.decode(&mut input) {
            Ok(Some(frame)) => {
                let _: Result<serde_json::Value, _> = serde_json::from_slice(&frame);
            }
            Ok(None) | Err(_) => break,
        }
    }
});
