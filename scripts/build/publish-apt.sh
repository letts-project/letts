#!/usr/bin/env bash
# publish-apt.sh — upload a .deb to the eswyft Gitea Debian package registry.
# Idempotent: a duplicate version (HTTP 409) is treated as already-published.
# Only curl is required, so it also runs by hand. Usage: publish-apt.sh <deb>
#   env: APT_UPLOAD_URL  APT_PACKAGE_TOKEN
set -euo pipefail

deb="${1:?usage: publish-apt.sh <deb-file>}"
[ -f "$deb" ] || { echo "publish-apt: deb not found: $deb" >&2; exit 1; }
: "${APT_UPLOAD_URL:?}" "${APT_PACKAGE_TOKEN:?}"

code="$(curl -sS -o /dev/null -w '%{http_code}' \
  -H "Authorization: token ${APT_PACKAGE_TOKEN}" \
  --upload-file "$deb" "$APT_UPLOAD_URL")"

case "$code" in
  201|202) echo "publish-apt: uploaded $(basename "$deb")  OK" ;;
  409)     echo "publish-apt: $(basename "$deb") already in repo (409); skipping" ;;
  *)       echo "publish-apt: upload failed (HTTP $code)" >&2; exit 1 ;;
esac
