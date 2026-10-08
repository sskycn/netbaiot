"""Check the real workflow dependency graph and Linux release target policy."""
from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from release_targets import REQUIRED_TARGETS, OPTIONAL_TARGETS, matrix

def jobs(name):
    # These workflows deliberately use literal job IDs/needs and a single jobs
    # block. Reject unsupported forms instead of pretending to parse all YAML.
    text = (ROOT / ".github/workflows" / name).read_text()
    body = text.split("\njobs:\n", 1)[1]
    matches = list(re.finditer(r"(?m)^  ([a-z][a-z0-9-]*):\n", body))
    result = {}
    for index, match in enumerate(matches):
        block = body[match.end():matches[index+1].start() if index+1 < len(matches) else len(body)]
        need = re.search(r"(?m)^    needs: (.+)$", block)
        dependencies = [] if need is None else [part.strip() for part in need[1].strip("[]").split(",")]
        if any(re.fullmatch(r"[a-z][a-z0-9-]*", part) is None for part in dependencies):
            raise ValueError("policy test requires literal needs")
        result[match[1]] = (block, dependencies)
    return text, result

class PlatformPolicyTests(unittest.TestCase):
    def test_single_manifest_has_two_required_linux_and_three_optional(self):
        self.assertEqual(set(REQUIRED_TARGETS), {"x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"})
        self.assertEqual(len(OPTIONAL_TARGETS), 3)
        self.assertTrue(set(REQUIRED_TARGETS).isdisjoint(OPTIONAL_TARGETS))
        self.assertEqual({item["target"] for item in matrix("required")["include"]}, set(REQUIRED_TARGETS))
        self.assertEqual({item["runner"] for item in matrix("required")["include"]}, {"ubuntu-24.04", "ubuntu-24.04-arm"})

    def test_linux_failure_blocks_publish_optional_failures_do_not(self):
        text, graph = jobs("release.yml")
        self.assertIn("needs: [verify, linux]", text)
        self.assertIn("needs: verify", text)
        self.assertIn("uses: ./.github/workflows/linux-packages.yml", text)
        self.assertNotIn("optional-platforms", text)
        self.assertNotIn("windows", text)
        self.assertNotIn("macos", text)
        self.assertIn("if: github.event_name == 'push'", text)
        # Evaluate the publication prerequisites, including unrelated failures.
        for statuses, allowed in [({'verify':True,'linux':False,'windows':True},False),
                                  ({'verify':False,'linux':True,'windows':True},False),
                                  ({'verify':True,'linux':True,'windows':False,'macos':False},True)]:
            self.assertEqual(all(statuses[name] for name in graph["publish"][1]), allowed)
        linux, linux_graph = jobs("linux-packages.yml")
        self.assertEqual(linux_graph["checksums"][1], ["plan", "build"])
        self.assertFalse(all({'plan':True,'build':False}[name] for name in linux_graph["checksums"][1]))
        self.assertIn("needs: [plan, build]", linux)
        self.assertIn("--group required", linux)
        for required in ("cargo xtask check", "cargo xtask package", "tests/release_archive_smoke.py", "verify-checksums", "sha256sum -c"):
            self.assertIn(required, linux)
        for critical in (text, linux, (ROOT / ".github/workflows/release-verify.yml").read_text()):
            self.assertNotIn("continue-on-error", critical)
            self.assertNotIn("|| true", critical)
            self.assertNotIn("always()", critical)

    def test_windows_is_manual_and_truthfully_fails_independently(self):
        optional = (ROOT / ".github/workflows/optional-platforms.yml").read_text()
        self.assertIn("workflow_dispatch:", optional)
        self.assertNotRegex(optional, r"(?m)^  (push|pull_request|workflow_call):")
        self.assertIn("--group optional", optional)
        self.assertIn("cargo xtask check", optional)
        self.assertNotIn("continue-on-error", optional)
        self.assertNotIn("|| true", optional)
        for name in ("dx-platform.yml", "recovery-platform.yml"):
            text = (ROOT / ".github/workflows" / name).read_text()
            self.assertNotIn("windows-latest", text)
            self.assertIn("macos-latest", text)
        for name in ("windows-regression-bisect.yml", "windows-regression-diagnostics.yml"):
            text = (ROOT / ".github/workflows" / name).read_text()
            self.assertIn("workflow_dispatch:", text)
            self.assertNotRegex(text, r"(?m)^  (push|pull_request|workflow_call):")

if __name__ == "__main__":
    unittest.main()
