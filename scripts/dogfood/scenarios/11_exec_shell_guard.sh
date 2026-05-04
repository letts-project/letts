#!/usr/bin/env bash
# 11_exec_shell_guard — verify the two-layer shell-form guard.
#
#  (a) CLI side: `bash -c …` without --allow-shell must be rejected
#      client-side (BadUsage exit 2) before any HTTP roundtrip.
#  (b) Server side: with --allow-shell, the CLI lets the payload through;
#      dogfood dugdale runs exec.allow_shell=false, so the daemon must
#      400 the request and the CLI must exit non-zero.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 11: exec shell-form guard (CLI and server)"

step "(a) CLI blocks shell-form without --allow-shell"
set +e
"$LETTS_BIN" exec --host=local --lane=normal -- bash -c "echo blocked" >/tmp/letts-dogfood-11a.out 2>&1
RC=$?
set -e
[ "$RC" -ne 0 ] || { cat /tmp/letts-dogfood-11a.out; fail "shell-form passed without --allow-shell (rc=$RC)"; }
ok "CLI rejected shell-form (rc=$RC)"

step "(b) server rejects shell-form even with --allow-shell when exec.allow_shell=false"
set +e
"$LETTS_BIN" exec --host=local --lane=normal --allow-shell -- bash -c "echo ok" >/tmp/letts-dogfood-11b.out 2>&1
RC=$?
set -e
[ "$RC" -ne 0 ] || { cat /tmp/letts-dogfood-11b.out; fail "server allowed shell-form despite exec.allow_shell=false (rc=$RC)"; }
ok "server rejected shell-form (rc=$RC)"

rm -f /tmp/letts-dogfood-11a.out /tmp/letts-dogfood-11b.out
ok "scenario 11 done"
