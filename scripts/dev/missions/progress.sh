#!/bin/sh
# progress — emit a few fd3 progress events, then succeed.
for i in 1 2 3 4 5; do
    val=$(awk "BEGIN { printf \"%.2f\", $i/5 }")
    printf '{"event":"progress","value":%s,"message":"step %s"}\n' "$val" "$i" >&3
    sleep 0.2
done
printf '{"event":"success","return":{"steps":5}}\n' >&3
exit 0
