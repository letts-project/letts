#!/usr/bin/env bash
# 02_input_output_files — dispatch file_processor with an input file (inline
# uploaded via --file=role=path), download the output via --output-file=role=path,
# verify uppercased contents. Then a second `big_io` invocation with three
# input roles and two output roles.
#
# NOTE: `letts run --file=role=path` uploads the file inline as a staging blob
# in the same call and references it in the dispatch — no separate
# `ctl staging upload` step needed (CLI has no flag to reference a
# pre-uploaded staging_id; see cmd/letts/dispatch.go:182-198 stageFiles).
# `--output-file=role=path` downloads that specific output after done.
set -euo pipefail
. "$(cd "$(dirname "$0")/.." && pwd)/_lib.sh"
. "$RUNTIME_DIR/env.sh"

step "scenario 02: input / output files"

TMP="$(mktemp -d -t letts-dogfood.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

printf 'hello dogfood\n' > "$TMP/in.txt"

step "running file_processor synchronously"
RES="$("$LETTS_BIN" run --host=local --lane=normal --mission=file_processor \
    --file="in=$TMP/in.txt" --output-file="out=$TMP/out" -o json)"
OUTCOME="$(printf '%s' "$RES" | jq -r .outcome)"
assert_equal "success" "$OUTCOME" "outcome"

step "verifying downloaded output content"
[ -f "$TMP/out" ] || fail "expected $TMP/out to exist"
GOT="$(cat "$TMP/out")"
assert_equal "HELLO DOGFOOD" "$GOT" "downloaded out (trim newline)"
# The trailing newline is preserved by strtoupper; assertion above strips it
# via shell since `$( )` trims trailing newlines. That's intentional.

step "creating three sized blobs for big_io"
printf 'aaa\n'                        > "$TMP/small.txt"
head -c 4096   /dev/urandom | base64  > "$TMP/medium.txt"
head -c 65536  /dev/urandom | base64  > "$TMP/large.txt"

step "running big_io with three input roles and two output roles"
RES="$("$LETTS_BIN" run --host=local --lane=normal --mission=big_io \
    --file="small=$TMP/small.txt" --file="medium=$TMP/medium.txt" --file="large=$TMP/large.txt" \
    --output-file="primary=$TMP/primary" --output-file="aux=$TMP/aux" -o json)"
OUTCOME="$(printf '%s' "$RES" | jq -r .outcome)"
assert_equal "success" "$OUTCOME" "big_io outcome"

step "verifying big_io output shapes"
[ -f "$TMP/primary" ] || fail "expected $TMP/primary to exist"
[ -f "$TMP/aux" ]     || fail "expected $TMP/aux to exist"
# wc -c on macOS pads with leading spaces; the $(( ... )) arithmetic expansion
# absorbs that, but the ACTUAL value needs a $(( )) too to normalize.
EXPECTED_PRIMARY=$(( $(wc -c < "$TMP/small.txt") + $(wc -c < "$TMP/medium.txt") + $(wc -c < "$TMP/large.txt") ))
ACTUAL_PRIMARY=$(( $(wc -c < "$TMP/primary") ))
assert_equal "$EXPECTED_PRIMARY" "$ACTUAL_PRIMARY" "primary size = sum of inputs"
AUX_SMALL_SIZE="$(jq -r .sizes.small "$TMP/aux")"
SMALL_BYTES=$(( $(wc -c < "$TMP/small.txt") ))
assert_equal "$SMALL_BYTES" "$AUX_SMALL_SIZE" "aux reports small size"

ok "scenario 02 done"
