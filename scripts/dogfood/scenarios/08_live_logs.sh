#!/usr/bin/env bash
# 08_live_logs — dispatch chatty (10 lines @ 200ms), tail with letts logs
# --follow for 1.5s, assert we received >=3 lines spanning >=600ms (proof
# of live streaming, not buffered-to-end). Then re-fetch full log without
# follow and assert all 10 lines present.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 08: live logs"

step "dispatching chatty mission"
MID="$("$LETTS_BIN" dispatch --host=local --lane=normal --mission=chatty \
    --input='{"lines":10,"delay_ms":200}' -o json | jq -r .mission_id)"

step "tailing logs --follow for 1.5s"
PARTIAL="$(mktemp)"
"$LETTS_BIN" logs "$MID" --host=local --stream=stdout --follow > "$PARTIAL" &
TAILPID=$!
sleep 1.5
kill -TERM "$TAILPID" 2>/dev/null || true
wait "$TAILPID" 2>/dev/null || true

LINES="$(grep -c '^\[chatty ' "$PARTIAL" || true)"
[ "$LINES" -ge 3 ] || fail "expected >=3 lines in partial tail, got $LINES (cat $PARTIAL)"
ok "received $LINES lines during 1.5s window"

# Timestamps in chatty are microtime(true); parse first and last lines
# to confirm they span >=600ms (proves daemon is flushing live, not at end).
FIRST_TS="$(grep '^\[chatty ' "$PARTIAL" | head -n1 | awk '{print $NF}')"
LAST_TS="$(grep  '^\[chatty ' "$PARTIAL" | tail -n1 | awk '{print $NF}')"
SPAN_MS="$(awk -v a="$FIRST_TS" -v b="$LAST_TS" 'BEGIN{printf "%d\n", (b-a)*1000}')"
[ "$SPAN_MS" -ge 600 ] || fail "expected ts span >=600ms, got ${SPAN_MS}ms (live streaming broken?)"
ok "ts span = ${SPAN_MS}ms (live streaming confirmed)"
rm -f "$PARTIAL"

step "waiting for mission to finish"
wait_for_status "$MID" "done" 10

step "re-fetching full log without follow"
FULL="$("$LETTS_BIN" logs "$MID" --host=local --stream=stdout)"
TOTAL="$(printf '%s\n' "$FULL" | grep -c '^\[chatty ' || true)"
assert_equal "10" "$TOTAL" "total chatty lines after done"

ok "scenario 08 done"
