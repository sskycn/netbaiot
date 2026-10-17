#!/usr/bin/env python3
"""Check repository Markdown links/anchors using only the Python standard library."""
import argparse
import html
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]


def prose(text):
    """Exclude fenced examples, which may intentionally contain placeholder links."""
    fence = None
    for line in text.splitlines():
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            token = marker.group(1)
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            yield ""
        else:
            yield line if fence is None else ""


def anchors(text):
    result, counts = set(), {}
    for line in prose(text):
        result.update(re.findall(r'\b(?:id|name)=["\']([^"\']+)["\']', line))
        match = re.match(r"^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        if not match:
            continue
        title = html.unescape(match.group(1))
        title = re.sub(r"\[([^]]+)\]\([^)]*\)", r"\1", title)
        title = re.sub(r"<[^>]+>", "", title)
        slug = re.sub(r"[^\w\- ]", "", title.lower()).replace(" ", "-")
        ordinal = counts.get(slug, 0)
        counts[slug] = ordinal + 1
        result.add(slug if not ordinal else f"{slug}-{ordinal}")
    return result


def links(text):
    for line_number, line in enumerate(prose(text), 1):
        # Inline links/images and reference definitions; titles are optional.
        for match in re.finditer(r"\[[^\]\n]*\]\(\s*(<[^>]+>|[^\s)]+)", line):
            yield line_number, match.group(1).strip("<>")
        match = re.match(r"^\s*\[[^]]+\]:\s*(<[^>]+>|\S+)", line)
        if match:
            yield line_number, match.group(1).strip("<>")


def documents(root):
    paths = set(root.glob("*.md"))
    for directory in ["docs", "configs", "scripts", ".github"]:
        paths.update((root / directory).rglob("*.md"))
    return sorted(paths)


def check(root, paths):
    failures, checked, cache = [], 0, {}
    root = root.resolve()
    for source in paths:
        source = source.resolve()
        for line, href in links(source.read_text(encoding="utf-8")):
            url = urlsplit(href)
            if url.scheme or url.netloc:
                continue
            checked += 1
            target = (source.parent / unquote(url.path)).resolve() if url.path else source
            label = f"{source.relative_to(root)}:{line}: {href}"
            if not target.is_relative_to(root) or not target.exists():
                failures.append(f"{label}: missing local target")
            elif url.fragment and target.suffix.lower() == ".md":
                if target not in cache:
                    cache[target] = anchors(target.read_text(encoding="utf-8"))
                if unquote(url.fragment) not in cache[target]:
                    failures.append(f"{label}: missing anchor")
    return checked, failures


def inventory(root, paths):
    entries = []
    for path in paths:
        relative = path.relative_to(root).as_posix()
        status = "current"
        if relative.startswith(("docs/releases/", "docs/performance/")):
            status = "historical evidence / index"
        elif relative.startswith("docs/migration/"):
            status = "compatibility guidance"
        elif path.name == "configuration-fields.md":
            status = "generated reference"
        entries.append({"path": relative, "status": status})
    return {"markdown": entries, "configs": [p.relative_to(root).as_posix() for p in sorted((root / "configs").rglob("*.json"))]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, help="write the reviewed file inventory as JSON")
    args = parser.parse_args()
    paths = documents(ROOT)
    if args.inventory:
        args.inventory.parent.mkdir(parents=True, exist_ok=True)
        args.inventory.write_text(json.dumps(inventory(ROOT, paths), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    checked, failures = check(ROOT, paths)
    for failure in failures:
        print(f"FAIL {failure}")
    print(f"Markdown: {len(paths)} files, {checked} local links, {len(failures)} failures")
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
