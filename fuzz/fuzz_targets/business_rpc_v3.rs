#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_core::business_rpc_v3::{DeviceCommandSendRequest, V3FrameHeader, V3FrameType, V3GoAway, V3Open, V3Reset, V3Response};
use netbaiot_v3_mux::{Initiator, MuxScheduler, ReassemblyBudget, StreamTable};

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
        let Ok(header) = V3FrameHeader::parse(raw, 8192) else { offset += 1; continue; };
        offset += 12;
        let Some(payload) = data.get(offset..offset.saturating_add(header.payload_len as usize)) else { break; };
        offset += payload.len();
        match header.frame_type {
            V3FrameType::Open => {
                if let Ok(open) = serde_json::from_slice::<V3Open>(payload) {
                    let _ = open.validate();
                    let _ = table.open_peer(header.stream_id, &open);
                }
            }
            V3FrameType::Response => {
                if let Ok(response) = serde_json::from_slice::<V3Response>(payload) {
                    let _ = response.validate();
                    let _ = table.mark_response(header.stream_id, response.content_length as usize);
                }
            }
            V3FrameType::Data => {
                if let Ok(received) = table.receive_data(header.stream_id, payload, header.flags != 0)
                    && let Some(body) = received.complete
                {
                    let _ = serde_json::from_slice::<DeviceCommandSendRequest>(&body);
                }
            }
            V3FrameType::WindowUpdate => {
                if let Ok(bytes) = <[u8; 4]>::try_from(payload) {
                    let increment = u32::from_be_bytes(bytes);
                    let _ = scheduler.window_update(header.stream_id, increment);
                    let _ = table.grant_credit(header.stream_id, increment);
                }
            }
            V3FrameType::ResetStream | V3FrameType::CloseStream => {
                if header.frame_type == V3FrameType::ResetStream {
                    if let Ok(reason) = serde_json::from_slice::<V3Reset>(payload) { let _ = reason.validate(); }
                }
                let _ = table.reset(header.stream_id);
                scheduler.reset(header.stream_id);
            }
            V3FrameType::GoAway => {
                if let Ok(away) = serde_json::from_slice::<V3GoAway>(payload) { let _ = away.validate(); }
                let _ = table.goaway();
            }
            _ => {}
        }
    }
});
