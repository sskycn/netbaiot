#!/usr/bin/env python3
"""Dependency-free release checks, runnable with Python 3.9+ and Cargo."""

import argparse
import json
from pathlib import Path
import re
import subprocess

from scrub_paths import LOCAL_PATH, scrub_tree


ROOT = Path(__file__).resolve().parents[1]
TARGETS = (
    "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin", "aarch64-apple-darwin", "x86_64-pc-windows-msvc",
)
REQUIRED = (
    "README.md", "README.zh-CN.md", "LICENSE", "NOTICE", "SECURITY.md",
    "CONTRIBUTING.md", "AGENTS.md", "docs/schema/netbaiot-config.schema.json", "configs/tutorial.json", "configs/development.json", "scripts/demo/start.sh",
    "examples/business_http_sink.py", "examples/device_tcp.py", "examples/device_udp.py",
    "docs/quick-start.md", "docs/protocol-support.md", "docs/delivery-semantics.md",
    "docs/security.md", "docs/operations-guide.md",
)
TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?")
LINK = re.compile(r"(!?\[[^\]\n]*\]\()([^\s)]+)(\))")


def workspace_version(root):
    source = (root / "Cargo.toml").read_text(encoding="utf-8")
    section = re.search(r"(?ms)^\[workspace\.package\]\s*\n(.*?)(?=^\[|\Z)", source)
    match = re.search(r'^version\s*=\s*"([^"]+)"', section[1] if section else "", re.M)
    if not match:
        raise ValueError("missing workspace.package.version")
    return match[1]


def check_tag(tag, version):
    if TAG.fullmatch(tag) is None or tag != f"v{version}":
        raise ValueError(f"tag {tag!r} must exactly match workspace version v{version}")


def package_files(root, tag):
    # Only public Markdown guides, selected examples and tutorial inputs. No raw
    # evidence, source tree, build products, fixtures or fuzz corpus is copied.
    paths = set(REQUIRED)
    # Package manifests use tar/Markdown paths, independent of the host OS.
    paths.update(path.relative_to(root).as_posix() for path in (root / "docs").glob("*.md"))
    paths.add(f"docs/releases/{tag}.md")
    for name in sorted(paths):
        path = root / name
        if not path.is_file() or path.is_symlink() or path.stat().st_size == 0:
            raise ValueError(f"missing, empty or symlinked package input: {name}")
    return sorted(paths)


def check_versions(root, tag):
    version = workspace_version(root)
    check_tag(tag, version)
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--offline", "--no-deps", "--format-version", "1"],
        cwd=root, text=True, timeout=60,
    ))
    members = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] in members]
    lock = (root / "Cargo.lock").read_text(encoding="utf-8")
    lock_versions = {}
    for section in lock.split("[[package]]")[1:]:
        name = re.search(r'^name = "([^"]+)"$', section, re.M)
        locked = re.search(r'^version = "([^"]+)"$', section, re.M)
        if name and locked and not re.search(r"^source =", section, re.M):
            lock_versions[name[1]] = locked[1]
    for package in packages:
        if package["version"] != version or lock_versions.get(package["name"]) != version:
            raise ValueError(f"workspace/lock version mismatch: {package['name']}")
    print(f"Version PASS: {tag}, {len(packages)} workspace packages and Cargo.lock")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--tag", default="")
    args = parser.parse_args()
    root = args.repo.resolve()
    tag = args.tag or f"v{workspace_version(root)}"
    check_versions(root, tag)
    paths = package_files(root, tag)
    for name in paths:
        if LOCAL_PATH.search((root / name).read_text(encoding="utf-8")):
            raise ValueError(f"local absolute path in package input: {name}")
    if scrub_tree(root / "docs/performance", repo_root=root):
        raise ValueError("performance evidence contains local absolute paths; run scripts/scrub_paths.py")
    print(f"Preflight PASS: notes, {len(paths)} package inputs, sanitized evidence")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f"Release preflight FAIL: {error}")
