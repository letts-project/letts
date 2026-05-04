#!/usr/bin/env bash
# 09_exec_argv — basic `letts exec` argv path. Runs `uptime` on local
# dugdale, verifies stdout looks like real uptime output, then checks
# `letts ctl exec list` shows the invocation in its tabular header.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 09: letts exec argv basic"

step "letts exec --host=local --lane=normal -- uptime"
OUT="$("$LETTS_BIN" exec --host=local --lane=normal -- uptime)"
[ -n "$OUT" ] || fail "stdout empty"
# `uptime` output always contains " up " on macOS and Linux.
if ! printf '%s' "$OUT" | grep -q " up "; then
    fail "stdout does not look like uptime: $OUT"
fi
ok "got uptime line: $(printf '%s' "$OUT" | head -n1)"

step "letts ctl exec list --host=local --limit=5 shows the invocation"
LIST_OUT="$("$LETTS_BIN" ctl exec list --host=local --limit=5)"
assert_contains "MISSION_ID" "$LIST_OUT" "list header"

step "the same listing carries kind=exec rows in -o json"
ROW_KIND="$("$LETTS_BIN" ctl exec list --host=local --limit=1 -o json \
    | jq -r '.missions[0].kind // empty')"
assert_equal "exec" "$ROW_KIND" "first row kind"

ok "scenario 09 done"
