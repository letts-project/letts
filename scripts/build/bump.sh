#!/usr/bin/env bash
# bump.sh — increment the build number in VERSION and commit just that file.
# Run via `make bump`. Commits ONLY VERSION (other working-tree changes are
# left untouched / uncommitted).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "bump: not a git repository" >&2
  exit 1
fi

cur="$(tr -d '[:space:]' < VERSION 2>/dev/null || true)"
[ -n "${cur:-}" ] || cur=0
case "$cur" in
  *[!0-9]*) echo "bump: VERSION is not a non-negative integer: '$cur'" >&2; exit 1 ;;
esac

next=$((cur + 1))
printf '%s\n' "$next" > VERSION

ver="$(scripts/build/version.sh)"
git commit -q -m "chore: bump build to ${next} (${ver})" -- VERSION
echo "bumped build ${cur} -> ${next}   version=${ver}"
echo "commit: $(git log --oneline -1)"
