#!/usr/bin/env bash
# 07_metrics — drive a few missions across outcomes, then GET /v1/metrics
# and assert the phantom counters/gauges are now live.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 07: metrics"

step "driving success and failure outcomes"
"$LETTS_BIN" run --host=local --lane=normal --mission=complex_return -o json > /dev/null
set +e
"$LETTS_BIN" run --host=local --lane=normal --mission=intentional_fail \
    --input='{"message":"metrics-test"}' -o json > /dev/null 2>&1
set -e

step "scraping /v1/metrics"
METRICS="$(curl -sS "http://127.0.0.1:$PORT/v1/metrics")"

step "letts_dugdale_info has non-empty version and commit labels"
INFO_LINE="$(printf '%s' "$METRICS" | grep '^letts_dugdale_info{' | head -n1)"
[ -n "$INFO_LINE" ] || fail "no letts_dugdale_info line in /v1/metrics"
printf '%s' "$INFO_LINE" | grep -qE 'version="[^"]+"' || fail "version label empty: $INFO_LINE"
printf '%s' "$INFO_LINE" | grep -qE 'commit="[^"]+"'  || fail "commit label empty: $INFO_LINE"
ok "letts_dugdale_info ok: $INFO_LINE"

step "letts_missions_total has success and failed entries"
SUCCESS_COUNT="$(printf '%s' "$METRICS" \
    | awk '/^letts_missions_total\{.*outcome="success"/ {print $2}' | head -n1)"
FAILED_COUNT="$(printf '%s' "$METRICS" \
    | awk '/^letts_missions_total\{.*outcome="failed"/  {print $2}' | head -n1)"
[ -n "$SUCCESS_COUNT" ] && [ "$(printf '%.0f' "$SUCCESS_COUNT")" -gt 0 ] \
    || fail "expected letts_missions_total outcome=success > 0, got '$SUCCESS_COUNT'"
[ -n "$FAILED_COUNT" ]  && [ "$(printf '%.0f' "$FAILED_COUNT")" -gt 0 ] \
    || fail "expected letts_missions_total outcome=failed > 0, got '$FAILED_COUNT'"
ok "missions_total success=$SUCCESS_COUNT failed=$FAILED_COUNT"

step "letts_http_requests_total has /v1/dispatch entries"
DISPATCH_REQS="$(printf '%s' "$METRICS" \
    | awk '/^letts_http_requests_total\{.*route="\/v1\/dispatch"/ {print $2}' | head -n1)"
[ -n "$DISPATCH_REQS" ] && [ "$(printf '%.0f' "$DISPATCH_REQS")" -gt 0 ] \
    || fail "expected letts_http_requests_total /v1/dispatch > 0, got '$DISPATCH_REQS'"
ok "http_requests_total /v1/dispatch=$DISPATCH_REQS"

step "letts_lane_concurrency reports fast=4"
# The poller runs every 15s with an initial RefreshOnce on startup. If the
# scrape happens before the first post-apply tick, the gauge is unset. Poll
# the metrics endpoint for up to 20s waiting for the value to appear.
LANE_FAST=""
for i in $(seq 1 20); do
    METRICS="$(curl -sS "http://127.0.0.1:$PORT/v1/metrics")"
    LANE_FAST="$(printf '%s' "$METRICS" \
        | awk '/^letts_lane_concurrency\{lane="fast"/ {print $2}' | head -n1)"
    if [ -n "$LANE_FAST" ] && [ "$(printf '%.0f' "$LANE_FAST")" = "4" ]; then
        break
    fi
    sleep 1
done
[ -n "$LANE_FAST" ] && [ "$(printf '%.0f' "$LANE_FAST")" = "4" ] \
    || fail "expected letts_lane_concurrency fast=4 within 20s, got '$LANE_FAST'"
ok "lane_concurrency fast=4"

ok "scenario 07 done"
