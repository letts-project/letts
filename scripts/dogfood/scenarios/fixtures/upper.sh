#!/usr/bin/env bash
# Fixture for scenario 10-exec-in-out: reads $LETTS_IN_text, uppercases,
# writes to $LETTS_OUT_result. Both env vars are populated by dugdale's
# exec runtime (see internal/mission/exec_runtime.go).
set -euo pipefail
in="${LETTS_IN_text:?need input}"
out="${LETTS_OUT_result:?need output}"
tr '[:lower:]' '[:upper:]' < "$in" > "$out"
