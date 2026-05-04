#!/bin/bash
# run — build dugdale and start it in the foreground with the dev config.
# Ctrl-C to stop (graceful drain). Re-run to pick up rebuilds.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

mkdir -p /tmp/letts-dev/data

cd "$ROOT"
go build -o /tmp/letts-dev/dugdale ./cmd/dugdale

exec /tmp/letts-dev/dugdale \
    --config "$HERE/dugdale.yaml" \
    --insecure-config-permissions
