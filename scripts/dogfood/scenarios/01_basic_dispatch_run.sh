#!/usr/bin/env bash
# 01_basic_dispatch_run — fire-and-forget dispatch and event tail to done,
# then a synchronous `letts run` with input and return-value verification.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 01: basic dispatch and run"

step "dispatching batch_iterator (n=5)"
MID="$("$LETTS_BIN" dispatch --host=local --lane=normal --mission=batch_iterator \
    --input='{"n":5,"delay_ms":20}' -o json | jq -r .mission_id)"
ok "mission_id=$MID"

step "tailing events to done"
EVENTS="$("$LETTS_BIN" events "$MID" --host=local --follow)"
LAST="$(printf '%s\n' "$EVENTS" | tail -n 1)"
EVENT="$(printf '%s' "$LAST" | jq -r .event)"
OUTCOME="$(printf '%s' "$LAST" | jq -r .outcome)"
assert_equal "done"    "$EVENT"   "last event"
assert_equal "success" "$OUTCOME" "outcome"

step "synchronous letts run complex_return"
OUT="$("$LETTS_BIN" run --host=local --lane=normal --mission=complex_return -o json)"
RET_OK="$(printf '%s' "$OUT" | jq -r .return.totals.ok)"
RET_VERSION="$(printf '%s' "$OUT" | jq -r .return.meta.version)"
assert_equal "3"     "$RET_OK"      "return.totals.ok"
assert_equal "1.2.3" "$RET_VERSION" "return.meta.version"

ok "scenario 01 done"
