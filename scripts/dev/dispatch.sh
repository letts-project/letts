#!/usr/bin/env bash
# dispatch — dispatch a mission and stream events to completion via
# `letts run`.
#
# Usage:  ./dispatch.sh <mission> [input-json] [lane]
#   mission     — basename of a script in MISSION_DIR (no .sh extension)
#   input-json  — JSON object passed in `input` (default '{}')
#   lane        — lane name (default 'default')
#
# Examples:
#   ./dispatch.sh hello
#   ./dispatch.sh progress '{"items":3}'
#   ./dispatch.sh with_output '{}' default
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

MISSION="${1:?mission name required}"
INPUT="${2:-{}}"
LANE="${3:-default}"
MISSION_DIR="${MISSION_DIR:-$HERE/missions}"

WORK_YAML="$(mktemp -t letts-dev.XXXXXX.yaml)"
trap 'rm -f "$WORK_YAML"' EXIT
sed "s|__MISSION_DIR_PLACEHOLDER__|$MISSION_DIR|" "$HERE/letts.yaml" > "$WORK_YAML"
chmod 600 "$WORK_YAML"

cd "$HERE"
exec go run ../../cmd/letts --config "$WORK_YAML" run \
    --host=local \
    --lane="$LANE" \
    --mission="$MISSION" \
    --input="$INPUT"
