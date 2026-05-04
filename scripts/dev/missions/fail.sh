#!/bin/sh
# fail — explicit fail event with a structured reason and a non-zero exit.
echo "about to fail" >&2
printf '{"event":"fail","message":"deliberate failure","reason":"demo","details":{"hint":"this mission always fails"},"exit_code":7}\n' >&3
exit 7
