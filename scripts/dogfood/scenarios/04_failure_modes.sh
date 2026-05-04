#!/usr/bin/env bash
# 04_failure_modes — exercise three terminal failure paths:
#   (a) intentional_fail — fd 3 fail event with details
#   (b) timeout — dispatch slow_interruptible with a 2s timeout
#   (c) killed — dispatch slow_interruptible, then `ctl missions kill`
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 04: failure modes"

step "(a) intentional fail"
set +e
"$LETTS_BIN" run --host=local --lane=normal --mission=intentional_fail \
    --input='{"message":"dogfood-boom","exit_code":2}' -o json > /tmp/dogfood-fail.json
RC=$?
set -e
[ "$RC" -ne 0 ] || fail "expected non-zero exit from intentional_fail, got 0"
OUTCOME="$(jq -r .outcome /tmp/dogfood-fail.json)"
REASON="$(jq -r .fail_reason /tmp/dogfood-fail.json)"
MSG="$(jq -r .fail_message /tmp/dogfood-fail.json)"
assert_equal "failed"        "$OUTCOME" "outcome"
assert_equal "explicit"      "$REASON"  "fail_reason"
assert_equal "dogfood-boom"  "$MSG"     "fail_message"
rm /tmp/dogfood-fail.json

step "(b) timeout"
MID="$("$LETTS_BIN" dispatch --host=local --lane=normal --mission=slow_interruptible \
    --input='{"seconds":30}' --timeout=2s -o json | jq -r .mission_id)"
wait_for_status "$MID" "done" 10
OUTCOME="$("$LETTS_BIN" ctl missions show "$MID" --host=local -o json | jq -r .outcome)"
assert_equal "timeout" "$OUTCOME" "timed-out outcome"

step "(c) killed"
MID="$("$LETTS_BIN" dispatch --host=local --lane=normal --mission=slow_interruptible \
    --input='{"seconds":30}' -o json | jq -r .mission_id)"
# Give it a moment to be picked up.
wait_for_status "$MID" "running" 10
"$LETTS_BIN" ctl missions kill "$MID" --host=local --signal=TERM >/dev/null
wait_for_status "$MID" "done" 10
OUTCOME="$("$LETTS_BIN" ctl missions show "$MID" --host=local -o json | jq -r .outcome)"
assert_equal "killed" "$OUTCOME" "killed outcome"

ok "scenario 04 done"
