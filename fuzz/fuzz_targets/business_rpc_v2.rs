#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use netbaiot_core::BusinessRpcFrame;
use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};

fuzz_target!(|data: &[u8]| {
    let framer = LengthPrefixFramer { maximum: 8 * 1024 * 1024 };
    let mut input = BytesMut::from(data);
    for _ in 0..128 {
        match framer.decode(&mut input) {
            Ok(Some(frame)) => {
                if let Ok(message) = serde_json::from_slice::<BusinessRpcFrame>(&frame) {
                    let _ = message.validate();
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
});
