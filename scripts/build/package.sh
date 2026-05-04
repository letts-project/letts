#!/usr/bin/env bash
# package.sh — build linux/amd64 binaries then produce dist/letts_<ver>_amd64.deb.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! command -v nfpm >/dev/null 2>&1; then
  echo "nfpm not found. Install one of:" >&2
  echo "  brew install nfpm" >&2
  echo "  go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest   (then add \$(go env GOPATH)/bin to PATH)" >&2
  exit 1
fi

GOOS=linux GOARCH=amd64 ./scripts/build/build.sh

# Version from the VERSION file (single source of truth — the same 0.0.<N>
# string the binaries are stamped with). Always valid and apt-sortable.
VERSION="$(./scripts/build/version.sh)"

mkdir -p dist
VERSION="$VERSION" nfpm pkg -f packaging/nfpm.yaml --packager deb --target dist/

echo "deb written to dist/ (version=${VERSION})"
ls -1 dist/*.deb
