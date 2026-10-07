#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_runtime::{Limits, decode_spool_records};
use sha2::{Digest, Sha256};

fuzz_target!(|data: &[u8]| {
    let limits = Limits {
        spool_segment_max_bytes: 1_048_576,
        spool_max_bytes: 1_048_576,
        ..Limits::default()
    };
    if data.len() <= limits.spool_segment_max_bytes {
        let _ = decode_spool_records(data, &limits);
    }
    if data.len() <= limits.spool_record_max_bytes {
        let mut current = b"NBSP".to_vec();
        current.extend_from_slice(&3u32.to_be_bytes());
        current.extend_from_slice(&1u64.to_be_bytes());
        current.extend_from_slice(&(data.len() as u32).to_be_bytes());
        current.extend_from_slice(data);
        current.extend_from_slice(&Sha256::digest(data));
        let record_bytes = current.len() as u64;
        current.extend_from_slice(b"SEND");
        current.extend_from_slice(&1u64.to_be_bytes());
        current.extend_from_slice(&record_bytes.to_be_bytes());
        let digest = Sha256::digest(&current);
        current.extend_from_slice(&digest);
        let _ = decode_spool_records(&current, &limits);
        let trailer = current.len() - 52;
        current[trailer + 11] ^= 1;
        let _ = decode_spool_records(&current, &limits);
    }
    for version in [0u32, 1, 2, 4, u32::MAX] {
        let mut header = b"NBSP".to_vec();
        header.extend_from_slice(&version.to_be_bytes());
        assert!(decode_spool_records(&header, &limits).is_err());
    }
});
