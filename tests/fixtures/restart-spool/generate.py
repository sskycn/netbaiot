#!/usr/bin/env python3
"""Regenerate with the actual pre-removal Rust serializers; no legacy runtime code is linked today."""
from pathlib import Path
import os
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
REVISION = '8ec59f37530659d65fe4ea398aba831b156d1d6b'


def historical(path):
    return subprocess.check_output(['git', 'show', f'{REVISION}:{path}'], cwd=ROOT, text=True)


with tempfile.TemporaryDirectory(prefix='netbaiot-legacy-fixture-') as directory:
    project = Path(directory)
    (project / 'src').mkdir()
    (project / 'Cargo.toml').write_text('''[package]
name = "netbaiot-legacy-fixture"
version = "0.0.0"
edition = "2024"
[dependencies]
serde = { version = "1", features = ["derive", "rc"] }
serde_json = { version = "1", features = ["raw_value"] }
thiserror = "2"
uuid = { version = "1", features = ["v4", "serde"] }
sha2 = "0.10"
''')
    (project / 'src/lib.rs').write_text(historical('crates/netbaiot-protocol/src/lib.rs'))
    event_source = historical('crates/netbaiot-runtime/src/event.rs')
    begin = event_source.index('#[derive(Clone, Debug, Serialize, Deserialize)]\npub struct SpoolRecord')
    end = event_source.index('\n}\n', begin) + 3
    record = event_source[begin:end]
    (project / 'src/main.rs').write_text('''use netbaiot_legacy_fixture::*;
use serde::{Serialize, Deserialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf};
use uuid::Uuid;
''' + record + '''
fn record(kind: DeviceEventKind, index: u128) -> SpoolRecord {
    SpoolRecord {
        event: DeviceEvent {
            event_id: EventId(Uuid::from_u128(index)),
            source_message_id: SourceMessageId::new(format!("legacy:{index}")).unwrap(),
            device: DeviceKey {
                tenant_id: TenantId::new("demo").unwrap(),
                product_id: ProductId::new("sensor").unwrap(),
                device_id: DeviceId::new("device-1").unwrap(),
            },
            received_at: 1, occurred_at: None, kind,
        },
        pending_sinks: vec![SinkId::new("development-audit").unwrap()],
        routing_revision: 1, accepted_at: 1,
        attempts: [(SinkId::new("development-audit").unwrap(), 2)].into(),
    }
}
fn main() {
    let output = PathBuf::from(std::env::args().nth(1).unwrap());
    let cases = [
        ("config-ack", vec![record(DeviceEventKind::ConfigAck(ConfigAck {
            revision: ConfigRevision::new(42).unwrap(),
            status: ConfigApplyStatus::Applied, error: None,
        }), 1)]),
        ("supported", vec![
            record(DeviceEventKind::Heartbeat(Heartbeat { sequence: 7 }), 2),
            record(DeviceEventKind::Telemetry([("temperature".into(), Scalar::Number(21.5))].into()), 3),
            record(DeviceEventKind::CommandAck(CommandAck {
                command_id: CommandId(Uuid::from_u128(5)), execution: ExecutionState::Succeeded,
            }), 4),
        ]),
    ];
    for (name, records) in cases {
        for version in [1u32, 2] {
            // NBSP framing from this revision's spool.rs commit_sync/decode_segment.
            let mut bytes = b"NBSP".to_vec();
            bytes.extend_from_slice(&version.to_be_bytes());
            if version == 2 { bytes.extend_from_slice(&7u64.to_be_bytes()); }
            for record in &records {
                let payload = serde_json::to_vec(record).unwrap();
                bytes.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
                bytes.extend_from_slice(&payload);
                bytes.extend_from_slice(&Sha256::digest(&payload));
            }
            std::fs::write(output.join(format!("{name}-v{version}.spool")), bytes).unwrap();
        }
    }
}
''')
    subprocess.run(['cargo', '+1.88.0', 'run', '--offline', '--manifest-path', str(project / 'Cargo.toml'),
                    '--', str(Path(__file__).resolve().parent)], check=True,
                   env={**os.environ, 'CARGO_TARGET_DIR': str(ROOT / 'target/legacy-fixture-generator')})
