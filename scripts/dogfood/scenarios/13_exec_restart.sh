#!/usr/bin/env bash
# 13_exec_restart — `letts ctl exec restart <exec_id>` (A2 and F3 path).
# Dispatch an exec with a pinned mission-id, then restart it. Verify the
# restart yields a *new* exec_id and `ctl exec show` finds it.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 13: ctl exec restart kind=exec"

EXEC_ID="$(uuidv7)"
step "dispatching exec with --mission-id=$EXEC_ID"
"$LETTS_BIN" exec --host=local --lane=normal --mission-id="$EXEC_ID" -- true >/dev/null
ok "first exec finished"

step "restarting via ctl exec restart"
NEW_ID="$("$LETTS_BIN" ctl exec restart "$EXEC_ID" --host=local -o json | jq -r .mission_id)"
[ -n "$NEW_ID" ] || fail "restart did not return a new mission_id"
[ "$NEW_ID" != "$EXEC_ID" ] || fail "restart returned the same id ($NEW_ID); expected a new exec_id"
ok "restart produced new exec_id=$NEW_ID"

step "ctl exec show <new_id> returns kind=exec"
KIND="$("$LETTS_BIN" ctl exec show "$NEW_ID" --host=local -o json | jq -r .kind)"
assert_equal "exec" "$KIND" "show kind"

ok "scenario 13 done"
