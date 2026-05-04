#!/usr/bin/env bash
# apply — reconcile the local dugdale with scripts/dev/letts.yaml via the
# `letts` CLI. Run once after starting dugdale (before any dispatch).
# Idempotent — safe to re-run.
#
# The dev letts.yaml has mission_dir as a placeholder; this script sed-
# substitutes it to an absolute path (MISSION_DIR env, default
# scripts/dev/missions) into a temp file before invoking the CLI.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MISSION_DIR="${MISSION_DIR:-$HERE/missions}"

WORK_YAML="$(mktemp -t letts-dev.XXXXXX.yaml)"
trap 'rm -f "$WORK_YAML"' EXIT
sed "s|__MISSION_DIR_PLACEHOLDER__|$MISSION_DIR|" "$HERE/letts.yaml" > "$WORK_YAML"
chmod 600 "$WORK_YAML"

cd "$HERE"
exec go run ../../cmd/letts apply -f "$WORK_YAML" "$@"
