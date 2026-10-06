#!/usr/bin/env python3
"""Remove machine-local paths from publishable evidence; never alter live inputs."""

import argparse
from pathlib import Path
import re
import tempfile


ROOT = Path(__file__).resolve().parents[1]
LOCAL_PATH = re.compile(r"/(?:Users/|home/|private/tmp(?:/|\b)|(?:private/)?var/folders/)")


def scrub_text(text, repo_root=ROOT, home=None, temp_root=None):
    # Longest roots first: repo/temp may themselves live under the user's home.
    roots = [(Path(repo_root), "<repo>"),
             (Path(temp_root or tempfile.gettempdir()), "<tmp>"),
             (Path(home or Path.home()), "<home>")]
    replacements = set()
    for path, label in roots:
        if str(path) != "/":
            replacements.add((str(path), label))
            replacements.add((str(path.resolve()), label))
    for path, label in sorted(replacements, key=lambda item: (-len(item[0]), item)):
        # An absolute root must start a path, not match inside /private/tmp or
        # another longer path. macOS resolves /tmp to /private/tmp; Linux does
        # not, so correctness must not depend on that platform alias.
        text = re.sub(r"(?<![\w/<>])" + re.escape(path) + r"(?=$|[/\s\"'():,])", lambda _: label, text)
    # Historical evidence may have been produced on another user's checkout.
    # Match the repository directory, never a particular username.
    text = re.sub(r"/(?:Users|home)/[^/\s\"'<>]+/(?:[^/\s\"'<>]+/)*?netbaiot(?=/|[\s\"'():,]|$)",
                  "<repo>", text)
    text = re.sub(r"/(?:private/)?var/folders/[^/\s\"'<>]+/[^/\s\"'<>]+/T(?=/|[\s\"']|$)",
                  "<tmp>", text)
    text = re.sub(r"/(?:private/)?tmp(?=/|[\s\"'():,]|$)", "<tmp>", text)
    text = re.sub(r"/(?:Users|home)/[^/\s\"'<>:]+", "<home>", text)
    return text


def scrub_tree(directory, repo_root=ROOT, write=False):
    changed = []
    for path in sorted(Path(directory).rglob("*")):
        if not path.is_file() or path.is_symlink():
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    changed = scrub_tree(args.directory, write=args.write)
    for path in changed:
        print(path.relative_to(args.directory))
    print(f"{len(changed)} files {'sanitized' if args.write else 'need sanitization'}")
    return int(bool(changed) and not args.write)


if __name__ == "__main__":
    raise SystemExit(main())
