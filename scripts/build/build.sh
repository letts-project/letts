#!/usr/bin/env bash
# build.sh — cross-compile dugdale and letts into dist/ with version stamping.
# Defaults to linux/amd64; override via GOOS/GOARCH env. Pure-Go (CGO off).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

GOOS="${GOOS:-linux}"
GOARCH="${GOARCH:-amd64}"
export CGO_ENABLED=0 GOOS GOARCH

VERSION="$("$ROOT/scripts/build/version.sh")"   # build number (0.0.<N>) from VERSION file
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
BUILT_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

LDFLAGS="-s -w \
  -X letts/internal/version.Version=${VERSION} \
  -X letts/internal/version.Commit=${COMMIT} \
  -X letts/internal/version.BuiltAt=${BUILT_AT}"

mkdir -p dist
go build -trimpath -ldflags "$LDFLAGS" -o "dist/dugdale-${GOOS}-${GOARCH}" ./cmd/dugdale
go build -trimpath -ldflags "$LDFLAGS" -o "dist/letts-${GOOS}-${GOARCH}"   ./cmd/letts

echo "built: dist/dugdale-${GOOS}-${GOARCH}, dist/letts-${GOOS}-${GOARCH} (version=${VERSION} commit=${COMMIT})"
