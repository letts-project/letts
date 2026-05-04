#!/usr/bin/env bash
# 05_admin_operations — exercise restart, delete, and ctl listings.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 05: admin operations"

step "ctl dugdales list shows local"
DG="$("$LETTS_BIN" ctl dugdales list -o json | jq -r '.[] | .id' | head -n1)"
assert_equal "local" "$DG" "dugdale id"

step "ctl lanes list returns all four"
# Field is .name (not .lane) — verified via `ctl lanes list -o json | jq`.
LANES="$("$LETTS_BIN" ctl lanes list --host=local -o json | jq -r '.[].name' | sort | tr '\n' ' ' | sed 's/ $//')"
assert_equal "fast manual normal slow" "$LANES" "lane names"

step "creating a failed mission via intentional_fail"
# `letts run -o json` emits {outcome,return,exit_code,signal,fail_*,duration_ms}
# but NOT mission_id (printRunResult in cmd/letts/run.go:583). Use dispatch and
# wait_for_status instead so we keep the id we'll restart from.
ORIG="$("$LETTS_BIN" dispatch --host=local --lane=normal --mission=intentional_fail \
    --input='{"message":"to-restart"}' -o json | jq -r .mission_id)"
wait_for_status "$ORIG" "done" 10

step "restart yields new mission_id with restarted_from set"
NEW="$("$LETTS_BIN" ctl missions restart "$ORIG" --host=local -o json | jq -r .mission_id)"
wait_for_status "$NEW" "done" 10
RESTARTED_FROM="$("$LETTS_BIN" ctl missions show "$NEW" --host=local -o json | jq -r .restarted_from)"
assert_equal "$ORIG" "$RESTARTED_FROM" "restarted_from points at original"

step "delete removes the mission from list"
"$LETTS_BIN" ctl missions delete "$NEW" --host=local --force >/dev/null
GONE="$("$LETTS_BIN" ctl missions show "$NEW" --host=local -o json 2>&1 || true)"
assert_contains "not_found" "$GONE" "deleted mission GET surfaces not_found"

ok "scenario 05 done"
