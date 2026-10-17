#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_core::business_rpc::wire::{DeviceCommandSendRequest, RpcFrameHeader, RpcFrameType, RpcGoAway, RpcOpen, RpcReset, RpcResponse};
use netbaiot_rpc_mux::{Initiator, MuxScheduler, ReassemblyBudget, StreamTable};

fuzz_target!(|data: &[u8]| {
    let mut table = match StreamTable::new(Initiator::Server, Default::default(), 64 * 1024, ReassemblyBudget::new(64 * 1024)) {
        Ok(table) => table,
        Err(_) => return,
    };
    let mut scheduler = match MuxScheduler::new(&Default::default(), 64 * 1024) {
        Ok(scheduler) => scheduler,
        Err(_) => return,
    };
    let mut offset: usize = 0;
    for _ in 0..32 {
        let Some(raw) = data.get(offset..offset.saturating_add(12)) else { break; };
        let Ok(header) = RpcFrameHeader::parse(raw, 8192) else { offset += 1; continue; };
        offset += 12;
        let Some(payload) = data.get(offset..offset.saturating_add(header.payload_len as usize)) else { break; };
        offset += payload.len();
        match header.frame_type {
            RpcFrameType::Open => {
                if let Ok(open) = serde_json::from_slice::<RpcOpen>(payload) {
                    let _ = open.validate();
                    let _ = table.open_peer(header.stream_id, &open);
                }
            }
            RpcFrameType::Response => {
                if let Ok(response) = serde_json::from_slice::<RpcResponse>(payload) {
                    let _ = response.validate();
                    let _ = table.mark_response(header.stream_id, response.content_length as usize);
                }
            }
            RpcFrameType::Data => {
                if let Ok(received) = table.receive_data(header.stream_id, payload, header.flags != 0)
                    && let Some(body) = received.complete
                {
                    let _ = serde_json::from_slice::<DeviceCommandSendRequest>(&body);
                }
            }
            RpcFrameType::WindowUpdate => {
                if let Ok(bytes) = <[u8; 4]>::try_from(payload) {
                    let increment = u32::from_be_bytes(bytes);
                    let _ = scheduler.window_update(header.stream_id, increment);
                    let _ = table.grant_credit(header.stream_id, increment);
                }
            }
            RpcFrameType::ResetStream | RpcFrameType::CloseStream => {
                if header.frame_type == RpcFrameType::ResetStream {
                    if let Ok(reason) = serde_json::from_slice::<RpcReset>(payload) { let _ = reason.validate(); }
                }
                let _ = table.reset(header.stream_id);
                scheduler.reset(header.stream_id);
            }
            RpcFrameType::GoAway => {
                if let Ok(away) = serde_json::from_slice::<RpcGoAway>(payload) { let _ = away.validate(); }
                let _ = table.goaway();
            }
            _ => {}
        }
    }
});
