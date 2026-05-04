#!/usr/bin/env bash
# _teardown.sh — stop dugdale (graceful, then forceful) and wipe .runtime/.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"

if [ -f "$RUNTIME_DIR/dugdale.pid" ]; then
    PID="$(cat "$RUNTIME_DIR/dugdale.pid")"
    if kill -0 "$PID" 2>/dev/null; then
        step "stopping dugdale (pid=$PID)"
        kill -TERM "$PID" 2>/dev/null || true
        for i in 1 2 3 4 5; do
            kill -0 "$PID" 2>/dev/null || break
            sleep 1
        done
        kill -KILL "$PID" 2>/dev/null || true
    fi
fi
rm -rf "$RUNTIME_DIR"
ok "torn down"
