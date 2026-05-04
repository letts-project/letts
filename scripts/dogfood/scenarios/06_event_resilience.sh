#!/usr/bin/env bash
# 06_event_resilience — dispatch batch_iterator, tail events with --follow,
# kill the follower mid-stream, then resume with --from <last_seq>.
# Verify resumed stream picks up at last_seq+1 without losing events.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 06: event resilience"

step "dispatching long batch_iterator"
MID="$("$LETTS_BIN" dispatch --host=local --lane=normal --mission=batch_iterator \
    --input='{"n":20,"delay_ms":200}' -o json | jq -r .mission_id)"

step "tailing events for ~1.5s then killing follower"
PARTIAL="$(mktemp)"
"$LETTS_BIN" events "$MID" --host=local --follow > "$PARTIAL" &
TAILPID=$!
sleep 1.5
kill -TERM "$TAILPID" 2>/dev/null || true
wait "$TAILPID" 2>/dev/null || true
LAST_SEQ="$(tail -n 1 "$PARTIAL" | jq -r .seq)"
[ -n "$LAST_SEQ" ] && [ "$LAST_SEQ" -gt 0 ] || fail "expected non-zero last_seq, got '$LAST_SEQ'"
ok "last_seq before resume = $LAST_SEQ"

step "resuming with --from $LAST_SEQ"
RESUMED="$("$LETTS_BIN" events "$MID" --host=local --follow --from "$LAST_SEQ")"
FIRST_RESUMED_SEQ="$(printf '%s\n' "$RESUMED" | head -n 1 | jq -r .seq)"
LAST_RESUMED_EVENT="$(printf '%s\n' "$RESUMED" | tail -n 1 | jq -r .event)"
# --from <N> means "events with seq > N". First resumed should
# be exactly LAST_SEQ + 1 with no gap.
assert_equal "$((LAST_SEQ + 1))" "$FIRST_RESUMED_SEQ" "first resumed seq"
assert_equal "done" "$LAST_RESUMED_EVENT" "last event after resume"

rm -f "$PARTIAL"
ok "scenario 06 done"
