#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_core::*;
use netbaiot_runtime::{DeviceVerifier, Limits};
use netbaiot_transports::udp::{self, ReplayDecision, ReplayWindow};
use std::sync::Arc;
fuzz_target!(|data: &[u8]| {
    if data.len() > 65_540 {
        return;
    }
    let _ = udp::decode(data, 1200);
    let identity = AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: false,
        },
    };
    let verifier = DeviceVerifier::new(identity.clone(), [7; 32]);
    let mut replay = ReplayWindow::new(Arc::new(Limits {
        max_replay_entries: 4,
        max_replay_entries_per_device: 4,
        ..Limits::default()
    }));
    for chunk in data.chunks_exact(32).take(64) {
        let seq = u64::from_be_bytes(chunk[..8].try_into().unwrap());
        let timestamp = i64::from_be_bytes(chunk[8..16].try_into().unwrap());
        let now = i64::from_be_bytes(chunk[16..24].try_into().unwrap());
        let version = u32::from_be_bytes(chunk[24..28].try_into().unwrap());
        let boot = [chunk[28]; 16];
        let check = replay.check(&identity.device_key, version, boot, seq, timestamp, now);
        if let Ok(decision) = check {
            if decision == ReplayDecision::New {
                replay.commit(identity.device_key.clone(), version, boot, seq, now);
            }
            let ack = udp::encode_ack(&verifier, version, boot, seq).unwrap();
            assert_eq!(ack.len(), 64);
            verifier.verify(&ack[..32], &ack[32..]).unwrap();
            let mut corrupt = ack;
            corrupt[usize::from(chunk[29]) % 64] ^= 1;
            assert!(verifier.verify(&corrupt[..32], &corrupt[32..]).is_err());
        }
    }
});
