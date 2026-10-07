#!/usr/bin/env python3
"""Overlay diagnostic output only, preserving each historical queue/presence implementation."""

from pathlib import Path
import sys


probe, source = map(Path, sys.argv[1:])
runtime = Path("crates/netbaiot-runtime/src")
# These implementations did not change in the optimization range; the overlay
# adds credential-free provider/sink status logging only.
for relative in ("business_rpc.rs", "business_event.rs"):
    (source / runtime / relative).write_text((probe / runtime / relative).read_text())
test = Path("apps/netbaiot-server/tests/business_rpc_v2.rs")
(source / test).write_text((probe / test).read_text())


def insert(path, needle, extra):
    text = path.read_text()
    if text.count(needle) != 1:
        raise ValueError(f"unexpected historical diagnostic insertion site: {path}")
    path.write_text(text.replace(needle, needle + extra))


sessions = source / runtime / "sessions.rs"
current = (probe / runtime / "sessions.rs").read_text()
diagnostic = current.split("        let live = state.sessions.get(device);", 1)[1].split("        Ok(DeviceConnectionInfo", 1)[0]
insert(sessions, "        let presence = state.presence.get(device);\n", "        let live = state.sessions.get(device);" + diagnostic)
insert(sessions, "        state.generation = generation;\n", '        tracing::debug!(?device, generation, replacing, "registered session diagnostic");\n')
insert(sessions, "            state.sessions.remove(&self.device);\n", '            tracing::debug!(device=?self.device, generation=self.generation, cancelled=self.cancel.is_cancelled(), "current session lease dropped");\n')

event = source / runtime / "event.rs"
insert(event, "            let event = Arc::new(record.event);\n", '            tracing::debug!(event_id=%event.event_id, attempts=?record.attempts, routing_revision=record.routing_revision, "restored required event diagnostic");\n')
insert(event, "        record.attempt = record.attempt.saturating_add(1);\n", '        tracing::debug!(event_id=%record.event.event_id, attempt=record.attempt, ?result, "delivery completion diagnostic");\n')
