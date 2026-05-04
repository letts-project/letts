#!/bin/sh
# hello — print to stdout/stderr, send fd3 success with a return value.
echo "hello from $LETTS_MISSION_ID"
echo "warning to stderr" >&2
printf '{"event":"success","return":{"greeting":"hi","mission_id":"%s"}}\n' "$LETTS_MISSION_ID" >&3
exit 0
