#!/usr/bin/env bash
# _setup.sh — build binaries, install PHP deps, start dugdale, apply lanes.
# Idempotent across re-runs: kills any old dugdale.pid first.
#
# Exports LETTS_CONFIG for the calling shell when sourced.

set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"

LETTS_REPO="$(cd "$DOGFOOD_DIR/../.." && pwd)"
LETTS_PHP_REPO="$(cd "$LETTS_REPO/.." && pwd)/letts-php"

step "preparing runtime"
mkdir -p "$RUNTIME_DIR"

# Kill stale daemon from a previous run.
if [ -f "$RUNTIME_DIR/dugdale.pid" ]; then
    OLD_PID="$(cat "$RUNTIME_DIR/dugdale.pid")"
    if kill -0 "$OLD_PID" 2>/dev/null; then
        kill -TERM "$OLD_PID" 2>/dev/null || true
        for i in 1 2 3 4 5; do kill -0 "$OLD_PID" 2>/dev/null || break; sleep 1; done
        kill -KILL "$OLD_PID" 2>/dev/null || true
    fi
    rm -f "$RUNTIME_DIR/dugdale.pid"
fi
rm -rf "$RUNTIME_DIR/data" "$RUNTIME_DIR"/*.yaml "$RUNTIME_DIR"/*.log

step "building Go binaries"
(cd "$LETTS_REPO" && go build -o "$DUGDALE_BIN" ./cmd/dugdale)
(cd "$LETTS_REPO" && go build -o "$LETTS_BIN"   ./cmd/letts)

step "ensuring PHP deps"
if [ ! -d "$LETTS_PHP_REPO" ]; then
    die "expected $LETTS_PHP_REPO to exist (clone letts-php as a sibling of letts)"
fi
if [ ! -d "$DOGFOOD_DIR/vendor" ]; then
    (cd "$DOGFOOD_DIR" && composer install --quiet)
fi

step "generating configs"
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
DATA_DIR="$RUNTIME_DIR/data"
MISSION_DIR="$DOGFOOD_DIR/missions"
mkdir -p "$DATA_DIR"

sed -e "s|\${PORT}|$PORT|g" -e "s|\${DATA_DIR}|$DATA_DIR|g" \
    "$DOGFOOD_DIR/dugdale.yaml.tpl" > "$RUNTIME_DIR/dugdale.yaml"
chmod 600 "$RUNTIME_DIR/dugdale.yaml"

sed -e "s|\${PORT}|$PORT|g" -e "s|\${MISSION_DIR}|$MISSION_DIR|g" \
    "$DOGFOOD_DIR/letts.yaml.tpl" > "$RUNTIME_DIR/letts.yaml"
chmod 600 "$RUNTIME_DIR/letts.yaml"

step "starting dugdale on 127.0.0.1:$PORT"
"$DUGDALE_BIN" --config "$RUNTIME_DIR/dugdale.yaml" \
    >"$RUNTIME_DIR/dugdale.log" 2>&1 &
echo $! > "$RUNTIME_DIR/dugdale.pid"

# Health poll.
for i in $(seq 1 50); do
    if curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/v1/healthz" \
       | grep -q '^200$'; then
        ok "dugdale healthy after ${i}00ms"
        break
    fi
    if [ "$i" = "50" ]; then
        cat "$RUNTIME_DIR/dugdale.log" >&2
        die "dugdale did not become healthy within 5s"
    fi
    sleep 0.1
done

step "applying lanes and runtime"
export LETTS_CONFIG="$RUNTIME_DIR/letts.yaml"
"$LETTS_BIN" apply -f "$LETTS_CONFIG" >/dev/null
ok "applied; ready"

# Persist exported var so subsequent scripts get it via run-all.sh.
# Also export tokens/port — exec scenarios (09-14) hit raw curl and need to
# distinguish scopes for the kind-isolation guard (scenario 12).
echo "export LETTS_CONFIG='$LETTS_CONFIG'" > "$RUNTIME_DIR/env.sh"
echo "export PORT='$PORT'" >> "$RUNTIME_DIR/env.sh"
echo "export DISP_TOKEN='dogfood-dispatch'" >> "$RUNTIME_DIR/env.sh"
echo "export ADMIN_TOKEN='dogfood-admin'" >> "$RUNTIME_DIR/env.sh"
echo "export EXEC_TOKEN='dogfood-exec'" >> "$RUNTIME_DIR/env.sh"
