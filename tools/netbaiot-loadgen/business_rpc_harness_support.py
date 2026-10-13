"""Owned process shutdown and opaque committed-spool evidence for RPC probes."""

import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time
from scrub_paths import scrub_text


COMMITTED_FILES = ("eventbus-recovery.spool", "mqtt-runtime.state")
TEXT_EVIDENCE_SUFFIXES = {".json", ".jsonl", ".txt", ".log"}


def scrub_evidence(directory, repo_root, write=False):
    """Scrub only the harness's top-level text artifacts, never recovery dirs."""
    changed = []
    for path in sorted(Path(directory).iterdir()):
        if path.is_symlink() or not path.is_file() or path.name in COMMITTED_FILES or path.suffix not in TEXT_EVIDENCE_SUFFIXES:
            continue
        try:
            original = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        scrubbed = scrub_text(original, repo_root=repo_root)
        if original != scrubbed:
            changed.append(path)
            if write:
                path.write_text(scrubbed, encoding="utf-8")
    return changed


def gateway_stop_timeout(config):
    return config.get("limits", {}).get("shutdown_timeout_ms", 30_000) / 1000 + 5


def stop(process, timeout=3):
    started = time.monotonic()
    exit_code = process.poll()
    terminated = exit_code is None
    forced_kill = False
    if terminated:
        process.terminate()
        try:
            exit_code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            forced_kill = True
            process.kill()
            exit_code = process.wait(timeout=5)
    return {
        "exit_code": exit_code,
        "terminated": terminated,
        "forced_kill": forced_kill,
        "wait_timeout_seconds": timeout,
        "elapsed_seconds": time.monotonic() - started,
    }


def preserve_committed_spool(source, destination):
    source, destination = Path(source), Path(destination)
    manifest = {"source_directory_mode": None, "copy_directory_mode": None, "files": [], "errors": []}
    if not source.exists():
        return manifest
    metadata = source.lstat()
    if not stat.S_ISDIR(metadata.st_mode):
        manifest["errors"].append("spool source is not a regular directory")
        return manifest
    manifest["source_directory_mode"] = oct(stat.S_IMODE(metadata.st_mode))
    destination.mkdir(parents=True, mode=0o700, exist_ok=True)
    if not stat.S_ISDIR(destination.lstat().st_mode):
        raise OSError("spool evidence destination is not a regular directory")
    destination.chmod(0o700)
    manifest["copy_directory_mode"] = oct(stat.S_IMODE(destination.stat().st_mode))
    for name in COMMITTED_FILES:
        file = source / name
        try:
            metadata = file.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(metadata.st_mode):
            manifest["errors"].append(f"committed file {name} is not regular")
            continue
        copied = destination / name
        digest = hashlib.sha256()
        size = 0
        # Never decode or print recovery payloads. The original files are bounded
        # by gateway limits; copy with a fixed buffer after the owner has exited.
        with file.open("rb") as reader, copied.open("xb") as writer:
            copied.chmod(0o600)
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                writer.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        manifest["files"].append({
            "name": name,
            "bytes": size,
            "sha256": digest.hexdigest(),
            "source_file_mode": oct(stat.S_IMODE(metadata.st_mode)),
            "copy_file_mode": oct(stat.S_IMODE(copied.stat().st_mode)),
        })
    if os.name == "posix":
        descriptor = os.open(destination, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return manifest


def shutdown_gateway(process, config, evidence, snapshot=None):
    result = stop(process, gateway_stop_timeout(config))
    try:
        result["spool"] = preserve_committed_spool(config["spool_directory"], evidence)
    except OSError as error:
        # Metadata only: never include file contents in errors or summaries.
        result["spool"] = {"files": [], "errors": [f"spool evidence copy failed: {type(error).__name__}"]}
    result["pre_shutdown_status"] = snapshot
    result["source_spool_directory"] = str(Path(config["spool_directory"]).resolve())
    # A successful gateway exit always commits the MQTT snapshot, even when
    # required EventBus deliveries drained. An empty/missing evidence path must
    # never turn an otherwise successful process exit into a durability claim.
    committed_mqtt = any(file["name"] == "mqtt-runtime.state" for file in result["spool"]["files"])
    result["durable_shutdown"] = result["exit_code"] == 0 and not result["forced_kill"] and not result["spool"]["errors"] and committed_mqtt
    return result


def write_shutdown(path, result):
    Path(path).write_text(json.dumps(result, indent=2) + "\n")
