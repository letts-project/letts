#!/usr/bin/env bash
# 12_exec_kind_isolation — verify scope/kind isolation at the daemon edge.
#
#  (a) POST /v1/dispatch with an exec-scope token must 403 (recognised
#      token but wrong scope).
#  (b) POST /v1/exec/dispatch with a dispatch-scope token must 403.
#
# Both probes go through raw curl to bypass the CLI's auto-scope picker.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 12: kind isolation (raw HTTP probes)"

step "(a) /v1/dispatch with exec-token → 403"
HTTP_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $EXEC_TOKEN" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: 0192aaaa-0000-7000-8000-000000000001" \
    -d '{"mission":"noop","input":{}}' \
    "http://127.0.0.1:$PORT/v1/dispatch")"
assert_equal "403" "$HTTP_CODE" "/v1/dispatch with exec token"

step "(b) /v1/exec/dispatch with dispatch-token → 403"
HTTP_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $DISP_TOKEN" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: 0192aaaa-0000-7000-8000-000000000002" \
    -d '{"lane":"normal","command":["uptime"]}' \
    "http://127.0.0.1:$PORT/v1/exec/dispatch")"
assert_equal "403" "$HTTP_CODE" "/v1/exec/dispatch with dispatch token"

ok "scenario 12 done"
