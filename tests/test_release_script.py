"""Exercise automatic release notes and rollback against local Git remotes."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from release_notes import generate
from release_preflight import REQUIRED


class ReleaseScriptTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        self.remote = Path(self.temporary.name) / "origin.git"
        self.environment = os.environ.copy()
        self.environment["PYTHONDONTWRITEBYTECODE"] = "1"
        self.environment["GIT_CONFIG_NOSYSTEM"] = "1"
        self.environment["GIT_CONFIG_GLOBAL"] = os.devnull
        for name in REQUIRED:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("public package input\n", encoding="utf-8")
        (self.root / "docs/releases").mkdir()
        for name in ("release.sh", "release_notes.py", "release_preflight.py",
                     "release_targets.py", "release_targets.json", "scrub_paths.py"):
            path = self.root / "scripts" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / "scripts" / name, path)
        self.set_version("0.2.4")
        self.command("git", "init", "-b", "main")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.commit("Previous release")
        self.git("tag", "-a", "v0.2.4", "-m", "Previous release")
        self.previous = self.git("rev-parse", "HEAD")
        (self.root / "change.txt").write_text("change\n", encoding="utf-8")
        self.commit("feat: add [codec] <support> `v1`")
        self.candidate = self.git("rev-parse", "HEAD")
        self.command("git", "init", "--bare", str(self.remote))
        self.git("remote", "add", "origin", str(self.remote))
        self.sync()

        binaries = Path(self.temporary.name) / "bin"
        binaries.mkdir()
        cargo = binaries / "cargo"
        cargo.write_text("""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import sys
version = re.search(r'version = "([^"]+)"', Path("Cargo.toml").read_text())[1]
if sys.argv[1:3] == ["update", "--workspace"]:
    if os.environ.get("FAIL_CARGO_UPDATE"):
        raise SystemExit(1)
    Path("Cargo.lock").write_text(f'[[package]]\\nname = "fixture"\\nversion = "{version}"\\n')
elif sys.argv[1] == "metadata":
    print(json.dumps({"workspace_members": ["fixture"], "packages": [
        {"id": "fixture", "name": "fixture", "version": version}]}))
else:
    raise SystemExit("unexpected cargo command")
""", encoding="utf-8")
        cargo.chmod(0o755)
        self.environment["PATH"] = str(binaries) + os.pathsep + self.environment["PATH"]

    def command(self, *arguments, check=True):
        return subprocess.run(arguments, cwd=self.root, env=self.environment,
                              text=True, capture_output=True, check=check, timeout=30)

    def git(self, *arguments):
        return self.command("git", *arguments).stdout.strip()

    def set_version(self, version):
        (self.root / "Cargo.toml").write_text(f'[workspace.package]\nversion = "{version}"\n')
        (self.root / "Cargo.lock").write_text(f'[[package]]\nname = "fixture"\nversion = "{version}"\n')

    def commit(self, subject):
        self.git("add", ".")
        self.git("commit", "-m", subject)

    def sync(self):
        self.git("push", "origin", "main", "--tags")

    def release(self):
        return self.command("bash", "scripts/release.sh", "v0.3.0", check=False)

    @property
    def notes(self):
        return self.root / "docs/releases/v0.3.0.md"

    def assert_released(self):
        self.assertEqual(self.git("status", "--porcelain"), "")
        head = self.git("rev-parse", "HEAD")
        self.assertEqual(self.git("rev-parse", "v0.3.0^{}"), head)
        self.assertIn(head, self.git("ls-remote", "origin", "refs/heads/main"))
        self.assertIn(head, self.git("ls-remote", "origin", "refs/tags/v0.3.0^{}"))

    def assert_no_preparation(self, head):
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.assertEqual(self.git("tag", "--list", "v0.3.0"), "")
        self.assertEqual(self.git("ls-remote", "origin", "refs/tags/v0.3.0"), "")

    def test_missing_notes_are_generated_committed_and_pushed_with_version(self):
        result = self.release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        notes = self.notes.read_text()
        self.assertIn("# NetbaIoT v0.3.0", notes)
        self.assertIn("Changes since v0.2.4", notes)
        self.assertIn("feat: add \\[codec\\] &lt;support&gt; \\`v1\\`", notes)
        self.assertIn(self.candidate, notes)
        self.assertNotIn(self.previous, notes)
        self.assertIn('version = "0.3.0"', (self.root / "Cargo.lock").read_text())
        self.assertEqual(set(self.git("diff", "--name-only", "HEAD^").splitlines()),
                         {"Cargo.toml", "Cargo.lock", "docs/releases/v0.3.0.md"})
        self.assert_released()

    def test_curated_notes_are_preserved(self):
        self.notes.write_text("# Curated v0.3.0\n\nExact reviewed content.\n")
        self.commit("docs: curate release notes")
        self.sync()
        original = self.notes.read_bytes()
        result = self.release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.notes.read_bytes(), original)
        self.assert_released()

    def test_matching_version_still_commits_generated_notes(self):
        self.set_version("0.3.0")
        self.commit("chore: prepare version")
        self.sync()
        result = self.release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.git("diff", "--name-only", "HEAD^"), "docs/releases/v0.3.0.md")
        self.assert_released()

    def test_matching_version_and_existing_notes_tag_without_new_commit(self):
        self.set_version("0.3.0")
        self.notes.write_text("# Reviewed release\n")
        self.commit("chore: prepare release")
        self.sync()
        head = self.git("rev-parse", "HEAD")
        result = self.release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assert_released()

    def test_empty_tracked_notes_are_generated(self):
        self.notes.touch()
        self.commit("docs: reserve release notes")
        self.sync()
        result = self.release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("## Changes", self.notes.read_text())
        self.assert_released()

    def test_preflight_failure_restores_versions_and_removes_generated_notes(self):
        (self.root / "examples/device_udp.py").unlink()
        self.commit("Remove required package input")
        self.sync()
        head = self.git("rev-parse", "HEAD")
        result = self.release()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Release preflight", result.stderr)
        self.assertFalse(self.notes.exists())
        self.assert_no_preparation(head)

    def test_update_failure_restores_empty_tracked_notes(self):
        self.notes.touch()
        self.commit("docs: reserve release notes")
        self.sync()
        head = self.git("rev-parse", "HEAD")
        self.environment["FAIL_CARGO_UPDATE"] = "1"
        result = self.release()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.notes.read_bytes(), b"")
        self.assert_no_preparation(head)

    def test_commit_failure_rolls_back_preparation_without_tag(self):
        hook = self.root / ".git/hooks/pre-commit"
        hook.write_text("#!/bin/sh\nexit 1\n")
        hook.chmod(0o755)
        result = self.release()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.notes.exists())
        self.assert_no_preparation(self.candidate)

    def test_remote_existing_tag_does_not_generate_notes(self):
        self.git("tag", "v0.3.0")
        self.sync()
        self.git("tag", "-d", "v0.3.0")
        result = self.release()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("origin 上的标签已存在", result.stderr)
        self.assertFalse(self.notes.exists())
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.assertEqual(self.git("rev-parse", "HEAD"), self.candidate)

    def test_dirty_or_unsynced_branch_does_not_generate_notes(self):
        (self.root / "untracked.txt").touch()
        result = self.release()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.notes.exists())
        self.commit("Local-only commit")
        head = self.git("rev-parse", "HEAD")
        result = self.release()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.notes.exists())
        self.assert_no_preparation(head)

    def test_generator_handles_first_release_and_no_new_commits(self):
        self.git("tag", "-d", "v0.2.4")
        self.assertTrue(generate(self.root, "v0.3.0"))
        self.assertIn(self.previous, self.notes.read_text())
        self.notes.unlink()
        self.git("tag", "v0.2.4")
        self.assertTrue(generate(self.root, "v0.3.0"))
        self.assertIn("No additional commits since v0.2.4", self.notes.read_text())

    def test_unreachable_release_tag_is_not_a_baseline(self):
        self.git("checkout", "--orphan", "unrelated")
        self.commit("Unrelated history")
        self.git("tag", "v9.9.9")
        self.git("checkout", "main")
        self.assertTrue(generate(self.root, "v0.3.0"))
        self.assertIn("Changes since v0.2.4", self.notes.read_text())
        self.assertNotIn("Unrelated history", self.notes.read_text())

    def test_generator_rejects_invalid_tag_and_symlink(self):
        with self.assertRaises(ValueError):
            generate(self.root, "v0.3.0/escape")
        self.notes.symlink_to(self.root / "README.md")
        with self.assertRaises(ValueError):
            generate(self.root, "v0.3.0")
        self.assertEqual((self.root / "README.md").read_text(), "public package input\n")


if __name__ == "__main__":
    unittest.main()
