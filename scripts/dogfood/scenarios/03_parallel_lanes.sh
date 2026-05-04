#!/usr/bin/env bash
# 03_parallel_lanes — dispatch 8 batch_iterator missions across fast/normal/slow,
# observe lane concurrency via ctl missions list, wait for all to finish,
# verify each succeeded.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 03: parallel lanes"

declare -a MIDS=()
dispatch_one() {
    local lane="$1"
    local mid
    # n=10, delay_ms=500 → ~5s per mission. We need a wide enough window to
    # snapshot after dispatch and pickup (~0.3s) but before any finish (~5s).
    mid="$("$LETTS_BIN" dispatch --host=local --lane="$lane" --mission=batch_iterator \
        --input='{"n":10,"delay_ms":500}' -o json | jq -r .mission_id)"
    MIDS+=("$mid")
}

step "dispatching 8 missions (4 fast, 2 normal, 2 slow)"
for _ in 1 2 3 4; do dispatch_one fast; done
for _ in 1 2;       do dispatch_one normal; done
for _ in 1 2;       do dispatch_one slow; done
ok "dispatched ${#MIDS[@]} missions"

# Give them a moment to pick up. PHP cold-start + dispatcher tick. Verified
# from local probing: ~0.3s is enough for all 4 fast slots to be filled.
sleep 1

step "snapshot of running missions per lane"
SNAP="$("$LETTS_BIN" ctl missions list --host=local --status=running -o json)"
FAST_RUNNING="$(printf '%s' "$SNAP" | jq -r '.missions | map(select(.lane=="fast")) | length')"
SLOW_RUNNING="$(printf '%s' "$SNAP" | jq -r '.missions | map(select(.lane=="slow")) | length')"
# fast concurrency is 4 — every dispatched mission should be running.
# slow concurrency is 1 — only one of the two should be running.
assert_equal "4" "$FAST_RUNNING" "fast lane running count"
assert_equal "1" "$SLOW_RUNNING" "slow lane running count"

step "waiting for all missions to finish"
for mid in "${MIDS[@]}"; do
    wait_for_status "$mid" "done" 60
done

step "verifying all outcomes are success"
for mid in "${MIDS[@]}"; do
    OUTCOME="$("$LETTS_BIN" ctl missions show "$mid" --host=local -o json | jq -r .outcome)"
    assert_equal "success" "$OUTCOME" "$mid outcome"
done

ok "scenario 03 done"
