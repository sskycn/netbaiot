#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
用法：scripts/release.sh vX.Y.Z

自动生成缺失的 docs/releases/vX.Y.Z.md（保留已有非空说明），更新 Cargo
workspace 版本和 Cargo.lock，提交发布变更，推送当前分支及发布标签。
标签推送后，GitHub Actions 会构建各平台发布包并创建 Release。

示例：
  scripts/release.sh v0.1.1
  scripts/release.sh v0.2.0-rc.1
EOF
}

fail() {
  printf '错误：%s\n' "$*" >&2
  exit 1
}

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || fail '请从 Git 仓库中运行此脚本。'
cd "$repo_root"

if (($# == 1)) && [[ "$1" == '-h' || "$1" == '--help' ]]; then
  usage
  exit 0
fi
(($# == 1)) || { usage >&2; fail '请提供目标版本，例如 v0.1.1。'; }

for command in git awk python3 cargo; do
  command -v "$command" >/dev/null 2>&1 || fail "缺少命令：$command"
done

tag="$1"
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]] || \
  fail "标签格式无效：$tag（需要 vX.Y.Z，可带 prerelease/build metadata）。"
new_version="${tag#v}"
notes_file="docs/releases/$tag.md"

if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
  fail '工作区有未提交或未跟踪的文件。请先提交或清理，再发布。'
fi

branch="$(git symbolic-ref --quiet --short HEAD)" || \
  fail '当前处于 detached HEAD；请切换到要发布的分支后重试。'
git remote get-url origin >/dev/null 2>&1 || fail '找不到 origin 远端。'
git check-ref-format "refs/tags/$tag" >/dev/null || fail "Git 标签无效：$tag"
if git show-ref --verify --quiet "refs/tags/$tag"; then
  fail "本地标签已存在：$tag"
fi
remote_tag="$(git ls-remote origin "refs/tags/$tag" "refs/tags/$tag^{}")" || \
  fail '无法读取 origin 标签；请检查网络和 Git 权限。'
[[ -z "$remote_tag" ]] || fail "origin 上的标签已存在：$tag"
remote_branch="$(git ls-remote --heads origin "refs/heads/$branch")" || \
  fail '无法读取 origin 分支；请检查网络和 Git 权限。'
[[ -n "$remote_branch" ]] || fail "origin 上不存在分支：$branch"
remote_commit="${remote_branch%%$'\t'*}"
local_commit="$(git rev-parse HEAD)"
[[ "$local_commit" == "$remote_commit" ]] || \
  fail "当前分支与 origin/$branch 不一致。请先同步分支，再发布。"

old_version="$(awk '
  /^\[workspace\.package\]$/ { in_workspace_package = 1; next }
  /^\[/ { in_workspace_package = 0 }
  in_workspace_package && $1 == "version" {
    gsub(/"/, "", $3)
    print $3
    exit
  }
' Cargo.toml)"
[[ -n "$old_version" ]] || fail '无法从 Cargo.toml 读取 workspace.package.version。'
[[ "$old_version" != "$new_version" ]] || \
  printf 'Workspace 版本已是 %s；无需更新版本文件。\n' "$new_version"

version_changed=0
notes_generated=0
restore_preparation() {
  if ((version_changed)); then
    git restore --source=HEAD --staged --worktree -- Cargo.toml Cargo.lock
  fi
  if ((notes_generated)); then
    if git ls-files --error-unmatch -- "$notes_file" >/dev/null 2>&1; then
      git restore --source=HEAD --staged --worktree -- "$notes_file"
    else
      rm -f -- "$notes_file"
    fi
  fi
}

if [[ ! -s "$notes_file" ]]; then
  notes_generated=1
  if ! python3 scripts/release_notes.py --tag "$tag"; then
    restore_preparation
    fail '自动生成 release notes 失败；没有创建或推送标签。'
  fi
fi

if [[ "$old_version" != "$new_version" ]]; then
  version_changed=1
  if ! python3 - "$new_version" <<'PY'
from pathlib import Path
import re
import sys

version = sys.argv[1]
path = Path("Cargo.toml")
text = path.read_text()
section_pattern = re.compile(
    r"(?ms)(^\[workspace\.package\][ \t]*\n)(.*?)(?=^\[|\Z)"
)
match = section_pattern.search(text)
if match is None:
    raise SystemExit("找不到 [workspace.package]。")

body, changed = re.subn(
    r'(?m)^([ \t]*version[ \t]*=[ \t]*)"[^"\n]+"([ \t]*(?:#.*)?)$',
    lambda item: f'{item.group(1)}"{version}"{item.group(2)}',
    match.group(2),
    count=1,
)
if changed != 1:
    raise SystemExit("无法唯一更新 workspace.package.version。")

updated = text[: match.start(2)] + body + text[match.end(2) :]
path.write_text(updated)
PY
  then
    restore_preparation
    fail '更新 Cargo.toml 版本失败；已还原发布准备文件。'
  fi

  if ! cargo update --workspace; then
    restore_preparation
    fail '更新 Cargo.lock 失败；已还原发布准备文件。'
  fi
fi

if ! python3 scripts/release_preflight.py --tag "$tag"; then
  restore_preparation
  fail 'Release preflight 未通过；已还原发布准备文件，没有创建或推送标签。'
fi

if ((version_changed || notes_generated)); then
  if ! git add Cargo.toml Cargo.lock "$notes_file"; then
    restore_preparation
    fail '暂存发布变更失败；已还原发布准备文件。'
  fi
  if ! git commit -m "chore(release): $tag"; then
    restore_preparation
    fail '提交发布变更失败；已还原发布准备文件。'
  fi
  printf '已提交发布准备：%s -> %s，%s\n' "$old_version" "$new_version" "$notes_file"
fi

git tag -a "$tag" -m "Release $tag"
if ! git push --atomic origin "HEAD:refs/heads/$branch" "refs/tags/$tag"; then
  fail "推送失败；本地提交和标签 $tag 已保留，可修复远端问题后手动推送。"
fi
printf '已推送分支和标签 %s。GitHub Actions 将构建并发布 Release。\n' "$tag"
