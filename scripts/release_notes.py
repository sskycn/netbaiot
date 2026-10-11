#!/usr/bin/env python3
"""Generate missing release notes from reachable release tags and Git history."""

import argparse
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

from release_preflight import TAG


ROOT = Path(__file__).resolve().parents[1]
GITHUB = "https://github.com/sskycn/netbaiot"


def git(root, *arguments):
    return subprocess.check_output(
        ["git", *arguments], cwd=root, text=True, encoding="utf-8", timeout=60,
    ).strip()


def previous_release(root, tag):
    tags = [candidate for candidate in git(root, "tag", "--merged", "HEAD", "--list", "v*").splitlines()
            if candidate != tag and TAG.fullmatch(candidate)]
    if not tags:
        return None
    matches = [argument for candidate in tags for argument in ("--match", candidate)]
    return git(root, "describe", "--tags", "--abbrev=0", *matches, "HEAD")


def markdown_subject(subject):
    subject = subject.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return re.sub(r"([\\`*_\[\]])", r"\\\1", subject)


def generate(root, tag):
    if TAG.fullmatch(tag) is None:
        raise ValueError(f"invalid release tag: {tag!r}")
    path = root / "docs" / "releases" / f"{tag}.md"
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError(f"release notes must be a regular file: {path.relative_to(root)}")
    if path.exists() and path.stat().st_size:
        print(f"Release notes preserved: {path.relative_to(root)}")
        return False

    previous = previous_release(root, tag)
    head = git(root, "rev-parse", "HEAD")
    revision_range = f"{previous}..HEAD" if previous else "HEAD"
    commits = git(root, "log", "--reverse", "--format=%H%x00%s", revision_range)
    lines = [f"# NetbaIoT {tag}", ""]
    if previous:
        comparison = f"{GITHUB}/compare/{quote(previous, safe='')}...{quote(tag, safe='')}"
        lines.extend([f"Changes since {previous}. [Full comparison]({comparison}).", ""])
    else:
        lines.extend(["This release includes the repository history through the source commit below.", ""])
    lines.extend([
        f"Generated from Git commit subjects at [{head[:7]}]({GITHUB}/commit/{head}).",
        "", "## Changes", "",
    ])
    for commit in commits.splitlines():
        identity, separator, subject = commit.partition("\0")
        if not separator or re.fullmatch(r"[0-9a-f]{40,64}", identity) is None:
            raise ValueError("invalid Git log record")
        lines.append(f"- {markdown_subject(subject)} ([{identity[:7]}]({GITHUB}/commit/{identity}))")
    if not commits:
        lines.append(f"- No additional commits since {previous}; this release uses the same source revision.")
    lines.extend([
        "", "## Upgrade and compatibility", "",
        "Review the changes above and the [protocol support](../protocol-support.md),",
        "[upgrade requirements](../migration/current-protocol-only.md) and",
        "[operations guide](../operations-guide.md) before deploying.",
        "Quiesce the previous process and wait for successful graceful shutdown; preserve",
        "supported recovery files containing unacknowledged required work. Consumers must",
        "deduplicate replay using stable `event_id`. Planned restart recovery does not",
        "provide crash durability.",
        "", "See [platform support](../platform-support.md) for release assets and",
        "[benchmarks](../benchmarks.md) for measurement scope and limitations.", "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Release notes generated: {path.relative_to(root)} ({revision_range})")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    generate(args.repo.resolve(), args.tag)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f"Release notes FAIL: {error}")
