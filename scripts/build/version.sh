#!/usr/bin/env sh
# version.sh — print the build version string derived from the VERSION file.
# Single source of truth for `letts version`, the deb version, and the deb
# filename. Format: 0.0.<N>, where N is the incrementing build number.
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
n="$(tr -d '[:space:]' < "$ROOT/VERSION" 2>/dev/null || true)"
[ -n "${n:-}" ] || n=0
printf '0.0.%s\n' "$n"
