"""Focused release failures: version fencing, package completeness and hostile tar."""

import contextlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from release_preflight import REQUIRED, TARGETS, check_tag, check_versions, package_files
from release_package import binary_names, checksums, package, validate_archive
from scrub_paths import scrub_text, scrub_tree


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for name in REQUIRED + ("docs/releases/v0.2.3.md",):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("public package input\n")
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.2.3"\n')

    def build(self, target):
        build = self.root / "build" / target
        build.mkdir(parents=True)
        magic = b"MZ00" if "windows" in target else b"\x7fELF" if "linux" in target else b"\xcf\xfa\xed\xfe"
        for name in binary_names(target):
            (build / name).write_bytes(magic + b"test-only format stub")
        return build

    def bundle(self, target=TARGETS[0]):
        with contextlib.redirect_stdout(io.StringIO()):
            return package(self.root, self.build(target), self.root / "dist", "v0.2.3", target)

    def test_exact_tag_and_workspace_lock_versions(self):
        for tag in ("v0.2.2", "v0.2.3-rc.1", "0.2.3", "v0.2.3/evil", "v0.2.3\n"):
            with self.assertRaises(ValueError):
                check_tag(tag, "0.2.3")
        metadata = json.dumps({"workspace_members": ["local"], "packages": [
            {"id": "local", "name": "netbaiot-test", "version": "0.2.3"}]})
        with patch("release_preflight.subprocess.check_output", return_value=metadata):
            for locked in ("0.2.2", "0.2.3"):
                (self.root / "Cargo.lock").write_text(f'[[package]]\nname = "netbaiot-test"\nversion = "{locked}"\n')
                if locked == "0.2.2":
                    with self.assertRaises(ValueError):
                        check_versions(self.root, "v0.2.3")
                else:
                    with contextlib.redirect_stdout(io.StringIO()):
                        check_versions(self.root, "v0.2.3")

    def test_missing_notes_and_demo_dependency_block_package(self):
        notes = self.root / "docs/releases/v0.2.3.md"
        notes.write_text("")
        with self.assertRaises(ValueError):
            package_files(self.root, "v0.2.3")
        notes.write_text("release notes")
        (self.root / "examples/device_udp.py").unlink()
        with self.assertRaises(ValueError):
            package_files(self.root, "v0.2.3")

    def test_five_targets_and_checksum_set(self):
        for target in TARGETS:
            self.bundle(target)
        dist = self.root / "dist"
        with contextlib.redirect_stdout(io.StringIO()):
            checksums(self.root, dist, "v0.2.3")
        rows = (dist / "SHA256SUMS").read_text().splitlines()
        self.assertEqual(len(rows), 5)
        self.assertEqual(len({row.split()[1] for row in rows}), 5)
        (dist / "unrelated.tar.gz").write_bytes(b"extra archive")
        with self.assertRaises(ValueError):
            checksums(self.root, dist, "v0.2.3")

    def test_source_links_are_pinned_but_packaged_links_stay_local(self):
        (self.root / "source.rs").write_text("source")
        (self.root / "README.md").write_text("[guide](docs/quick-start.md) [source](source.rs)\n")
        archive = self.bundle()
        with tarfile.open(archive) as bundle:
            prefix = f"netbaiot-v0.2.3-{TARGETS[0]}"
            text = bundle.extractfile(f"{prefix}/README.md").read().decode()
            self.assertIn("(docs/quick-start.md)", text)
            self.assertIn("https://github.com/sskycn/netbaiot/blob/v0.2.3/source.rs", text)
        (self.root / "README.md").write_text("[missing](missing.md)")
        with self.assertRaises(ValueError):
            package(self.root, self.root / "build" / TARGETS[0], self.root / "dist", "v0.2.3", TARGETS[0])

    def test_hostile_or_incomplete_archives_fail(self):
        archive = self.root / f"netbaiot-v0.2.3-{TARGETS[0]}.tar.gz"
        prefix = archive.name[:-7]
        for names, link in [([f"{prefix}/../escape"], False),
                            ([f"{prefix}/README.md"] * 2, False),
                            ([f"{prefix}/README.md"], True),
                            ([f"{prefix}/README.md"], False)]:
            with tarfile.open(archive, "w:gz") as bundle:
                for name in names:
                    member = tarfile.TarInfo(name)
                    if link:
                        member.type = tarfile.SYMTYPE
                        member.linkname = "/etc/passwd"
                        bundle.addfile(member)
                    else:
                        member.size = 1
                        bundle.addfile(member, io.BytesIO(b"x"))
            with self.assertRaises(ValueError):
                validate_archive(archive, "v0.2.3", TARGETS[0])

    def test_local_path_and_wrong_binary_format_block_archive(self):
        (self.root / "README.md").write_text('/Users/example/private-output\n')
        with self.assertRaises(ValueError):
            self.bundle()
        (self.root / "README.md").write_text("public documentation")
        (self.root / "build" / TARGETS[0] / "netbaiot").write_bytes(b"MZ00wrong platform")
        with self.assertRaises(ValueError):
            package(self.root, self.root / "build" / TARGETS[0], self.root / "dist", "v0.2.3", TARGETS[0])


class ScrubTests(unittest.TestCase):
    def test_dynamic_and_historical_paths_preserve_json_and_values(self):
        source = json.dumps({"count": 42, "repo": "/home/researcher/work/netbaiot/tests/fixtures/localhost-key.pem",
            "temporary": "/private/tmp/netbaiot-run/spool", "other_home": "/Users/different/private/log",
            "mac_temp": "/private/var/folders/ab/long-random/T/netbaiot-run/spool"})
        result = scrub_text(source, repo_root="/home/researcher/work/netbaiot",
            home="/home/researcher", temp_root="/tmp")
        parsed = json.loads(result)
        self.assertEqual(parsed["count"], 42)
        self.assertEqual(parsed["repo"], "<repo>/tests/fixtures/localhost-key.pem")
        self.assertEqual(parsed["temporary"], "<tmp>/netbaiot-run/spool")
        self.assertEqual(parsed["other_home"], "<home>/private/log")
        self.assertEqual(parsed["mac_temp"], "<tmp>/netbaiot-run/spool")
        self.assertEqual(scrub_text(result), result)
        self.assertEqual(scrub_text("/Users/other/work/netbaiot/crates/test"), "<repo>/crates/test")

    def test_tree_check_is_read_only_and_write_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            path.write_text('{"spool":"/private/tmp/netbaiot-run/spool"}')
            self.assertEqual(scrub_tree(directory), [path])
            self.assertIn("/private/tmp/", path.read_text())
            self.assertEqual(scrub_tree(directory, write=True), [path])
            self.assertEqual(scrub_tree(directory), [])


if __name__ == "__main__":
    unittest.main()
