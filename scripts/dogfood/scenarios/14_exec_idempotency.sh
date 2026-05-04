#!/usr/bin/env bash
# 14_exec_idempotency — verify exec-dispatch idempotency semantics:
#
#  (a) Same UUID and same payload → 200 replay (same exec_id, no new row).
#  (b) Same UUID but different command → 409 idempotency_conflict.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 14: exec idempotency match and conflict"

UUID="$(uuidv7)"

step "first dispatch with --mission-id=$UUID"
"$LETTS_BIN" exec --host=local --lane=normal --mission-id="$UUID" -- true >/dev/null
ok "first dispatch succeeded"

step "(a) second dispatch — same UUID and same payload → replay 200"
"$LETTS_BIN" exec --host=local --lane=normal --mission-id="$UUID" -- true >/dev/null
ok "replay succeeded (no idempotency_conflict)"

step "(b) third dispatch — same UUID but different command → 409 conflict"
ERR_FILE="$(mktemp -t letts-dogfood-14.XXXXXX)"
set +e
"$LETTS_BIN" exec --host=local --lane=normal --mission-id="$UUID" -- false >"$ERR_FILE" 2>&1
RC=$?
set -e
if [ "$RC" -eq 0 ]; then
    cat "$ERR_FILE"
    rm -f "$ERR_FILE"
    fail "expected non-zero exit on fingerprint conflict, got 0"
fi
# The 409 body has "idempotency_conflict" code in the JSON error payload.
# wrapExecTransport flows through ExecTransportError (rc=255).
assert_contains "idempotency_conflict" "$(cat "$ERR_FILE")" "error body code"
rm -f "$ERR_FILE"

ok "scenario 14 done"
