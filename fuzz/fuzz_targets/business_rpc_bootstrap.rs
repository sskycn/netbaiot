#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_core::business_rpc::wire::{RpcBootstrap, RPC_MAX_METADATA_BYTES};
fuzz_target!(|data: &[u8]| {
    if data.len() > RPC_MAX_METADATA_BYTES || netbaiot_codecs::check_json_depth(data, 8).is_err() { return; }
    if let Ok(frame) = serde_json::from_slice::<RpcBootstrap>(data) { let _ = frame.validate(); }
});
