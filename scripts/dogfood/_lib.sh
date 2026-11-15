# _lib.sh — sourced by _setup.sh, _teardown.sh, and every scenarios/NN_*.sh.
# Provides:
#   DOGFOOD_DIR       — absolute path to scripts/dogfood
#   RUNTIME_DIR       — $DOGFOOD_DIR/.runtime (created by _setup.sh)
#   LETTS_BIN         — $RUNTIME_DIR/letts
#   DUGDALE_BIN       — $RUNTIME_DIR/dugdale
#   step "<label>"    — print a step banner
#   ok "<msg>"        — green check and msg
#   fail "<msg>"      — red cross and msg, then exit 1
#   die "<msg>"       — fatal exit (used by setup/teardown)
#   wait_for_status MISSION_ID EXPECTED_STATUS TIMEOUT_SEC
#   assert_equal EXPECTED ACTUAL LABEL
#   assert_contains NEEDLE HAYSTACK LABEL
#
# Sources nothing; safe to source multiple times.

set -euo pipefail

DOGFOOD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$DOGFOOD_DIR/.runtime"
LETTS_BIN="$RUNTIME_DIR/letts"
DUGDALE_BIN="$RUNTIME_DIR/dugdale"

# Colors — auto-disable when NO_COLOR is set or stdout is not a TTY.
if [ -z "${NO_COLOR:-}" ] && [ -t 1 ]; then
    _C_RED=$'\033[31m'; _C_GREEN=$'\033[32m'; _C_YEL=$'\033[33m'
    _C_BOLD=$'\033[1m'; _C_RESET=$'\033[0m'
else
    _C_RED=""; _C_GREEN=""; _C_YEL=""; _C_BOLD=""; _C_RESET=""
fi

step() { printf '\n%s==> %s%s\n' "$_C_BOLD" "$*" "$_C_RESET"; }
ok()   { printf '  %s✓%s %s\n' "$_C_GREEN" "$_C_RESET" "$*"; }
fail() { printf '  %s✗%s %s\n' "$_C_RED" "$_C_RESET" "$*" >&2; exit 1; }
die()  { printf '%sFATAL%s %s\n' "$_C_RED" "$_C_RESET" "$*" >&2; exit 1; }

assert_equal() {
    local expected="$1" actual="$2" label="$3"
    if [ "$expected" = "$actual" ]; then
        ok "$label: $actual"
    else
        fail "$label: expected '$expected', got '$actual'"
    fi
}

assert_contains() {
    local needle="$1" haystack="$2" label="$3"
    if printf '%s' "$haystack" | grep -qF "$needle"; then
        ok "$label: contains '$needle'"
    else
        fail "$label: '$needle' not found in: $haystack"
    fi
}

wait_for_status() {
    local mid="$1" want="$2" timeout="${3:-30}" elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local status
        status="$("$LETTS_BIN" ctl missions show "$mid" --host=local -o json 2>/dev/null | jq -r '.status // empty')"
        if [ "$status" = "$want" ]; then
            ok "mission $mid reached status=$want after ${elapsed}s"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    fail "mission $mid did not reach status=$want within ${timeout}s (last=$status)"
}

