#!/usr/bin/env python3
"""Build safe archives; require Linux and validate every present optional target."""

import argparse
import gzip
import hashlib
from pathlib import Path, PurePosixPath
import posixpath
import shutil
import tarfile
import tempfile
from urllib.parse import quote, unquote, urlsplit

from release_preflight import LINK, REQUIRED, ROOT, TARGETS, check_tag, package_files, workspace_version
from scrub_paths import LOCAL_PATH
from release_targets import DETAILS, REQUIRED_TARGETS


MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_MEMBERS = 256


def binary_names(target):
    suffix = ".exe" if "windows" in target else ""
    return (f"netbaiot-server{suffix}", f"netbaiot{suffix}")


def validate_binary(header, target):
    spec = DETAILS[target]
    if spec["format"] == "elf":
        valid = (len(header) >= 20 and header[:7] == b"\x7fELF\x02\x01\x01"
                 and int.from_bytes(header[18:20], "little") == spec["machine"])
    elif spec["format"] == "pe":
        offset = int.from_bytes(header[60:64], "little") if len(header) >= 64 else len(header)
        valid = (header[:2] == b"MZ" and offset >= 64 and offset + 6 <= len(header)
                 and header[offset:offset+4] == b"PE\0\0"
                 and int.from_bytes(header[offset+4:offset+6], "little") == spec["machine"])
    else:
        magic = header[:4]
        endian = "little" if magic == b"\xcf\xfa\xed\xfe" else "big"
        valid = (magic in (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf") and len(header) >= 8
                 and int.from_bytes(header[4:8], endian) == spec["machine"])
        if magic in (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf") and len(header) >= 8:
            count = int.from_bytes(header[4:8], "big")
            stride = 20 if magic == b"\xca\xfe\xba\xbe" else 32
            valid = (0 < count <= 32 and 8 + count * stride <= len(header)
                     and any(int.from_bytes(header[8+i*stride:12+i*stride], "big") == spec["machine"]
                             for i in range(count)))
    if not valid:
        raise ValueError(f"invalid binary format/architecture for {target}")


def render_markdown(text, name, included, root, tag):
    root = root.resolve()
    def replace(match):
        url = urlsplit(match[2])
        if url.scheme or url.netloc or not url.path:
            return match[0]
        destination = (root / name).parent.joinpath(unquote(url.path)).resolve()
        try:
            relative = destination.relative_to(root).as_posix()
        except ValueError as error:
            raise ValueError(f"link escapes repository in {name}") from error
        if relative in included:
            return match[0]
        if not destination.exists():
            raise ValueError(f"broken source link in {name}: {url.path}")
        kind = "tree" if destination.is_dir() else "blob"
        external = f"https://github.com/sskycn/netbaiot/{kind}/{quote(tag, safe='')}/{quote(relative)}"
        if url.fragment:
            external += f"#{url.fragment}"
        return f"{match[1]}{external}{match[3]}"
    return LINK.sub(replace, text)


def validate_archive(archive, tag, target, expected_files=None):
    check_tag(tag, tag[1:])
    if target not in TARGETS:
        raise ValueError(f"unsupported target: {target}")
    prefix = f"netbaiot-{tag}-{target}"
    if archive.name != f"{prefix}.tar.gz":
        raise ValueError(f"incorrect archive name: {archive.name}")
    required = set(expected_files or REQUIRED) | {f"docs/releases/{tag}.md"} | set(binary_names(target))
    files, seen, total = {}, set(), 0
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle:
            path = PurePosixPath(member.name)
            if (member.name in seen or len(seen) >= MAX_MEMBERS or path.is_absolute()
                    or "\\" in member.name
                    or member.name != path.as_posix()
                    or ".." in path.parts or not path.parts or path.parts[0] != prefix
                    or not (member.isfile() or member.isdir())):
                raise ValueError(f"unsafe or duplicate archive member: {member.name}")
            seen.add(member.name)
            total += member.size
            if member.size < 0 or total > MAX_ARCHIVE_BYTES:
                raise ValueError("archive exceeds uncompressed byte limit")
            if member.isdir():
                continue
            name = PurePosixPath(*path.parts[1:]).as_posix()
            if name in ("netbaiot-server", "netbaiot", "netbaiot-server.exe", "netbaiot.exe"):
                if name not in binary_names(target):
                    raise ValueError("unexpected duplicate/platform binary")
                source = bundle.extractfile(member)
                validate_binary(source.read(4096), target)
                if "windows" not in target and member.mode & 0o111 != 0o111:
                    raise ValueError(f"invalid binary or executable mode: {name}")
            elif name.endswith((".md", ".json", ".py", ".sh")):
                if member.size > 2 * 1024 * 1024:
                    raise ValueError(f"oversized documentation/demo file: {name}")
                files[name] = bundle.extractfile(member).read().decode("utf-8")
                if LOCAL_PATH.search(files[name]):
                    raise ValueError(f"local absolute path in archive: {name}")
            if member.size == 0:
                raise ValueError(f"empty archive file: {name}")
            required.discard(name)
        if required:
            raise ValueError(f"missing archive files: {sorted(required)}")
        file_names = {PurePosixPath(item).relative_to(prefix).as_posix() for item in seen
                      if item != prefix and bundle.getmember(item).isfile()}
        if expected_files is not None and file_names != set(expected_files) | set(binary_names(target)):
            raise ValueError("unexpected archive files")
        for name, text in files.items():
            if not name.endswith(".md"):
                continue
            for match in LINK.finditer(text):
                url = urlsplit(match[2])
                if url.scheme or url.netloc or not url.path:
                    continue
                resolved = posixpath.normpath((PurePosixPath(name).parent / unquote(url.path)).as_posix())
                if resolved not in file_names:
                    raise ValueError(f"broken archive link in {name}: {url.path}")
    return prefix


def package(root, build_dir, dist, tag, target):
    check_tag(tag, workspace_version(root))
    selected = package_files(root, tag)
    dist.mkdir(parents=True, exist_ok=True)
    prefix = f"netbaiot-{tag}-{target}"
    archive = dist / f"{prefix}.tar.gz"
    with tempfile.TemporaryDirectory(prefix="package-", dir=dist) as temporary:
        stage = Path(temporary) / prefix
        stage.mkdir()
        for name in selected:
            destination = stage / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / name, destination)
            if name.endswith(".md"):
                destination.write_text(render_markdown(destination.read_text(encoding="utf-8"), name,
                    set(selected), root, tag), encoding="utf-8")
        for name in binary_names(target):
            shutil.copyfile(build_dir / name, stage / name)
        # Stable ordering/metadata avoids packaging host usernames and timestamps.
        with archive.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as zipped:
            with tarfile.open(fileobj=zipped, mode="w") as bundle:
                for path in [stage] + sorted(stage.rglob("*")):
                    member = bundle.gettarinfo(str(path), arcname=path.relative_to(stage.parent).as_posix())
                    member.uid = member.gid = member.mtime = 0
                    member.uname = member.gname = ""
                    member.mode = 0o755 if path.is_dir() or path.name in binary_names(target) or path.suffix == ".sh" else 0o644
                    if path.is_file():
                        with path.open("rb") as source:
                            bundle.addfile(member, source)
                    else:
                        bundle.addfile(member)
    validate_archive(archive, tag, target, selected)
    print(f"Package PASS: {archive.name}")
    return archive


def archive_set(root, dist, tag, require_linux=True):
    check_tag(tag, workspace_version(root))
    expected = {f"netbaiot-{tag}-{target}.tar.gz": target for target in TARGETS}
    actual = {path.name for path in dist.glob("*.tar.gz")}
    required = {f"netbaiot-{tag}-{target}.tar.gz" for target in REQUIRED_TARGETS} if require_linux else set()
    if not actual or required - actual or actual - set(expected):
        raise ValueError(f"invalid archive set; missing required Linux={sorted(required-actual)}, extra={sorted(actual-set(expected))}")
    if not require_linux and any(expected[name] in REQUIRED_TARGETS for name in actual):
        raise ValueError("optional-only set cannot contain official Linux targets")
    return {name: expected[name] for name in sorted(actual)}


def checksum_lines(root, dist, tag, require_linux=True):
    expected = archive_set(root, dist, tag, require_linux)
    lines = []
    for name, target in sorted(expected.items()):
        path = dist / name
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"not a regular archive asset: {name}")
        validate_archive(path, tag, target, package_files(root, tag))
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        lines.append(f"{digest.hexdigest()}  {name}\n")
    return lines


def checksums(root, dist, tag, require_linux=True):
    lines = checksum_lines(root, dist, tag, require_linux)
    manifest = dist / "SHA256SUMS"
    if manifest.is_symlink() or (manifest.exists() and not manifest.is_file()):
        raise ValueError("unsafe checksum manifest")
    (dist / "SHA256SUMS").write_text("".join(lines), encoding="ascii")
    print(f"Archives/checksums PASS: {len(lines)} valid targets, SHA256SUMS")


def verify_checksums(root, dist, tag, require_linux=True, verbose=True):
    expected = "".join(checksum_lines(root, dist, tag, require_linux))
    if (dist / "SHA256SUMS").is_symlink():
        raise ValueError("unsafe checksum manifest")
    if (dist / "SHA256SUMS").stat().st_size != len(expected.encode("ascii")):
        raise ValueError("SHA256SUMS length does not match the exact asset set")
    if (dist / "SHA256SUMS").read_text(encoding="ascii") != expected:
        raise ValueError("SHA256SUMS does not exactly match present, validated archives")
    if verbose:
        print("SHA256SUMS verification PASS")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("package", "checksums", "verify-checksums", "assets"))
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--target", choices=TARGETS)
    parser.add_argument("--build-dir", type=Path)
    parser.add_argument("--dist", type=Path, required=True)
    parser.add_argument("--optional-only", action="store_true", help="independent development artifacts, not an official release set")
    args = parser.parse_args()
    if args.action == "package":
        if not args.target or not args.build_dir:
            parser.error("package requires --target and --build-dir")
        package(args.repo.resolve(), args.build_dir, args.dist, args.tag, args.target)
    elif args.action == "checksums":
        checksums(args.repo.resolve(), args.dist, args.tag, not args.optional_only)
    else:
        verify_checksums(args.repo.resolve(), args.dist, args.tag, not args.optional_only,
                         verbose=args.action != "assets")
        if args.action == "assets":
            for name in archive_set(args.repo.resolve(), args.dist, args.tag, not args.optional_only):
                print(name)
            print("SHA256SUMS")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, tarfile.TarError) as error:
        raise SystemExit(f"Release package FAIL: {error}")
