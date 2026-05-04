#!/usr/bin/env bash
# 10_exec_in_out — `letts exec` with --script, --in, --out. Uses fixtures/
# upper.sh as the script, fixtures/lower.txt as the input. Verifies the
# downloaded output equals the uppercased input.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 10: letts exec with --script, --in, and --out"

FIXTURES="$(cd "$(dirname "$0")/fixtures" && pwd)"
TMP="$(mktemp -d -t letts-dogfood-10.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT
DEST="$TMP/RESULT.txt"

step "exec --script=upper.sh --in text=lower.txt --out result=$DEST"
# NOTE: argv elements are NOT shell-expanded by the daemon's exec runtime
# (see internal/mission/exec_runtime.go:272 — `exec.Command(argv[0], argv[1:]...)`
# is verbatim). An argv like `bash $LETTS_SCRIPT` would pass the literal
# string `$LETTS_SCRIPT` to bash and fail with exit 127 (file not found).
# Use the deterministic workdir-relative path instead: dugdale always places
# the staged script at $LETTS_WORKDIR/script/script and chmod 755s it, and
# cmd.Dir = workdir, so `bash script/script` resolves correctly.
"$LETTS_BIN" exec --host=local --lane=normal \
    --script="$FIXTURES/upper.sh" \
    --in text="$FIXTURES/lower.txt" \
    --out result="$DEST" \
    -- bash script/script

[ -s "$DEST" ] || fail "expected output file $DEST to be non-empty"
GOT="$(cat "$DEST")"
WANT="HELLO WORLD"
assert_equal "$WANT" "$GOT" "uppercased output (trim newline)"

ok "scenario 10 done"
