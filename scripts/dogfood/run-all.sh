#!/usr/bin/env bash
# run-all — full dogfood pass. Sets up dugdale, runs every scenarios/NN_*.sh
# in order, tears down even on failure. Exits 0 only if every scenario passed.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/_lib.sh"

teardown() { "$HERE/_teardown.sh" || true; }
trap teardown EXIT

"$HERE/_setup.sh"

PASSED=0
FAILED=0
FAILED_NAMES=()
for s in "$HERE"/scenarios/[0-9]*.sh; do
    name="$(basename "$s" .sh)"
    if "$s"; then
        PASSED=$((PASSED + 1))
    else
        FAILED=$((FAILED + 1))
        FAILED_NAMES+=("$name")
    fi
done

printf '\n%s========%s\n' "$_C_BOLD" "$_C_RESET"
if [ "$FAILED" -eq 0 ]; then
    printf '%sall %d scenarios passed%s\n' "$_C_GREEN" "$PASSED" "$_C_RESET"
    exit 0
else
    printf '%s%d passed, %d failed:%s %s\n' "$_C_RED" "$PASSED" "$FAILED" "$_C_RESET" "${FAILED_NAMES[*]}"
    exit 1
fi
