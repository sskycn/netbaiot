import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "business_rpc_harness_support", ROOT / "tools/netbaiot-loadgen/business_rpc_harness_support.py"
)
shutdown = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shutdown)


class ShutdownEvidenceTests(unittest.TestCase):
    def test_gateway_grace_uses_configured_timeout_and_default(self):
        self.assertEqual(shutdown.gateway_stop_timeout({"limits": {}}), 35)
        self.assertEqual(shutdown.gateway_stop_timeout({"limits": {"shutdown_timeout_ms": 60_000}}), 65)

    def test_normal_exit_waits_for_gateway_grace_without_kill(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        result = shutdown.stop(process, 35)
        process.wait.assert_called_once_with(timeout=35)
        process.terminate.assert_called_once()
        process.kill.assert_not_called()
        self.assertEqual(result["exit_code"], 0)
        self.assertFalse(result["forced_kill"])

    def test_proxy_default_grace_remains_three_seconds(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        shutdown.stop(process)
        process.wait.assert_called_once_with(timeout=3)

    def test_expired_grace_is_recorded_as_forced_failure(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("gateway", 35), -9]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = shutdown.shutdown_gateway(process, {"limits": {}, "spool_directory": str(root / "absent")}, root / "copy")
        process.kill.assert_called_once()
        self.assertTrue(result["forced_kill"])
        self.assertEqual(result["exit_code"], -9)
        self.assertFalse(result["durable_shutdown"])

    def test_already_failed_owner_cannot_claim_durable_shutdown(self):
        process = Mock()
        process.poll.return_value = 1
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = shutdown.shutdown_gateway(process, {"spool_directory": str(root / "absent")}, root / "copy")
        process.terminate.assert_not_called()
        self.assertFalse(result["durable_shutdown"])

    def test_successful_exit_without_committed_mqtt_evidence_is_not_durable(self):
        for missing in (True, False):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "source"
                if not missing:
                    source.mkdir()
                process = Mock()
                process.poll.return_value = 0
                result = shutdown.shutdown_gateway(process, {"spool_directory": str(source)}, root / "copy")
                self.assertFalse(result["durable_shutdown"])

    def test_successful_drained_exit_with_committed_mqtt_evidence_is_durable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir(mode=0o700)
            (source / "mqtt-runtime.state").write_bytes(b"opaque fixture")
            process = Mock()
            process.poll.return_value = 0
            result = shutdown.shutdown_gateway(process, {"spool_directory": str(source)}, root / "copy")
            self.assertTrue(result["durable_shutdown"])

    def test_committed_opaque_files_are_preserved_with_hashes_and_permissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir(mode=0o700)
            for name in shutdown.COMMITTED_FILES:
                (source / name).write_bytes(b"opaque fixture, never decoded")
                (source / name).chmod(0o600)
            (source / ".incomplete.tmp").write_bytes(b"not authoritative")
            result = shutdown.preserve_committed_spool(source, root / "copy")
            self.assertFalse(result["errors"])
            self.assertEqual(result["source_directory_mode"], "0o700")
            self.assertEqual(result["copy_directory_mode"], "0o700")
            self.assertEqual({file["name"] for file in result["files"]}, set(shutdown.COMMITTED_FILES))
            for file in result["files"]:
                self.assertEqual(file["bytes"], len(b"opaque fixture, never decoded"))
                self.assertEqual(file["sha256"], hashlib.sha256(b"opaque fixture, never decoded").hexdigest())
                self.assertEqual(file["copy_file_mode"], "0o600")
            shutdown.write_shutdown(root / "shutdown.json", result)
            self.assertNotIn("opaque fixture", (root / "shutdown.json").read_text())
            self.assertEqual(json.loads((root / "shutdown.json").read_text()), result)

    @unittest.skipUnless(os.name == "posix", "symlink metadata behavior requires POSIX")
    def test_symlinked_committed_file_is_rejected_without_reading_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            (root / "outside").write_bytes(b"not a committed spool")
            (source / shutdown.COMMITTED_FILES[0]).symlink_to(root / "outside")
            result = shutdown.preserve_committed_spool(source, root / "copy")
            self.assertTrue(result["errors"])
            self.assertFalse(result["files"])

    def test_path_scrubbing_preserves_ascii_recovery_bytes_and_manifest_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "v3-8192-spool"
            source.mkdir()
            opaque = f"opaque recovery fixture {ROOT}/path".encode()
            for name in shutdown.COMMITTED_FILES:
                (source / name).write_bytes(opaque)
            # Directory exclusion applies even to JSON files inside spool state.
            (source / "metadata.json").write_text(json.dumps({"path": str(ROOT / "path")}))
            manifest = shutdown.preserve_committed_spool(source, root / "v3-8192-committed-spool")
            (root / "experiment.json").write_text(json.dumps({"path": str(ROOT / "path")}))
            (root / "gateway.log").write_text(f"fixture at {ROOT}/path\n")
            changed = shutdown.scrub_evidence(root, repo_root=ROOT, write=True)
            self.assertEqual({path.name for path in changed}, {"experiment.json", "gateway.log"})
            self.assertIn("<repo>/path", (root / "experiment.json").read_text())
            self.assertIn("<repo>/path", (root / "gateway.log").read_text())
            for file in manifest["files"]:
                for directory in [source, root / "v3-8192-committed-spool"]:
                    actual = (directory / file["name"]).read_bytes()
                    self.assertEqual(actual, opaque)
                    self.assertEqual(hashlib.sha256(actual).hexdigest(), file["sha256"])
            self.assertIn(str(ROOT / "path"), (source / "metadata.json").read_text())

    def test_failed_evidence_copy_preserves_source_and_cannot_claim_durability(self):
        process = Mock()
        process.poll.return_value = 0
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, copy = root / "spool", root / "copy"
            source.mkdir()
            copy.mkdir()
            name = shutdown.COMMITTED_FILES[0]
            (source / name).write_bytes(b"authoritative source")
            (copy / name).write_bytes(b"previous evidence")
            result = shutdown.shutdown_gateway(process, {"spool_directory": str(source)}, copy)
            self.assertFalse(result["durable_shutdown"])
            self.assertTrue(result["spool"]["errors"])
            self.assertEqual((source / name).read_bytes(), b"authoritative source")
            self.assertEqual((copy / name).read_bytes(), b"previous evidence")


if __name__ == "__main__":
    unittest.main()
