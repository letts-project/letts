#!/usr/bin/env bash
# publish-release.sh — create (or reuse) a release and upload the .deb asset.
# Used by CI for both Gitea and GitHub; also runnable by hand from a dev box
# (only curl and jq required — no gh, no docker).
#
# Usage: publish-release.sh <gitea|github> <deb-file> <version> <notes-file>
#
# Env (gitea):  GITEA_API   GITEA_REPO   GITEA_TOKEN
# Env (github): GITHUB_REPO GH_PAT       GIT_SHA
set -euo pipefail

flavor="${1:?usage: publish-release.sh <gitea|github> <deb> <version> <notes>}"
deb="${2:?missing deb file}"
version="${3:?missing version}"
notes_file="${4:?missing notes file}"

[ -f "$deb" ] || {
    echo "publish: deb not found: $deb" >&2
    exit 1
}
[ -f "$notes_file" ] || {
    echo "publish: notes not found: $notes_file" >&2
    exit 1
}

tag="v${version}"
asset="$(basename "$deb")"
body="$(cat "$notes_file")"

case "$flavor" in
gitea)
    : "${GITEA_API:?}" "${GITEA_REPO:?}" "${GITEA_TOKEN:?}"
    api="${GITEA_API}/repos/${GITEA_REPO}"
    auth=(-H "Authorization: token ${GITEA_TOKEN}")
    conflict=409
    create_payload="$(jq -n --arg t "$tag" --arg b "$body" \
        '{tag_name:$t, name:$t, body:$b}')"
    ;;
github)
    : "${GITHUB_REPO:?}" "${GH_PAT:?}" "${GIT_SHA:?}"
    api="https://api.github.com/repos/${GITHUB_REPO}"
    auth=(-H "Authorization: Bearer ${GH_PAT}" -H "Accept: application/vnd.github+json")
    conflict=422
    create_payload="$(jq -n --arg t "$tag" --arg c "$GIT_SHA" --arg b "$body" \
        '{tag_name:$t, target_commitish:$c, name:$t, body:$b}')"
    ;;
*)
    echo "publish: unknown flavor '$flavor' (gitea|github)" >&2
    exit 1
    ;;
esac

# Create the release; capture body and HTTP status separately. On a conflict the
# release already exists (idempotent re-run) — fetch it by tag instead.
resp="$(curl -sS -w $'\n%{http_code}' "${auth[@]}" \
    -H 'Content-Type: application/json' \
    -X POST "${api}/releases" -d "$create_payload")"
code="${resp##*$'\n'}"
json="${resp%$'\n'*}"

if [ "$code" = "$conflict" ]; then
    # A conflict usually means the release already exists — but GitHub also
    # returns 422 when target_commitish points at a commit it doesn't have yet.
    # Verify by fetching the release by tag before assuming it is there.
    rel="$(curl -sS -w $'\n%{http_code}' "${auth[@]}" "${api}/releases/tags/${tag}")"
    rcode="${rel##*$'\n'}"
    if [ "${rcode:0:1}" = "2" ]; then
        echo "publish[$flavor]: release ${tag} exists; reusing"
        json="${rel%$'\n'*}"
    else
        echo "publish[$flavor]: create returned $code but ${tag} not found (HTTP $rcode);" >&2
        echo "  is the tag/commit on the remote? create body: $json" >&2
        exit 1
    fi
elif [ "${code:0:1}" != "2" ]; then
    echo "publish[$flavor]: create failed (HTTP $code): $json" >&2
    exit 1
fi

rel_id="$(printf '%s' "$json" | jq -r '.id')"
[ -n "$rel_id" ] && [ "$rel_id" != "null" ] || {
    echo "publish[$flavor]: could not determine release id" >&2
    exit 1
}

# If an asset with this name already exists (re-run), delete it first. The
# delete path differs between the two APIs.
existing="$(curl -fsS "${auth[@]}" "${api}/releases/${rel_id}/assets" |
    jq -r --arg n "$asset" '.[] | select(.name==$n) | .id' | head -n1)"
if [ -n "$existing" ]; then
    echo "publish[$flavor]: replacing existing asset ${asset} (id ${existing})"
    if [ "$flavor" = gitea ]; then
        curl -fsS "${auth[@]}" -X DELETE "${api}/releases/${rel_id}/assets/${existing}" >/dev/null
    else
        curl -fsS "${auth[@]}" -X DELETE "${api}/releases/assets/${existing}" >/dev/null
    fi
fi

# Upload. Gitea takes a multipart 'attachment' field; GitHub takes the raw
# bytes on its uploads host.
if [ "$flavor" = gitea ]; then
    curl -fsS "${auth[@]}" -X POST \
        "${api}/releases/${rel_id}/assets?name=${asset}" \
        -F "attachment=@${deb}" >/dev/null
else
    curl -fsS "${auth[@]}" -X POST \
        "https://uploads.github.com/repos/${GITHUB_REPO}/releases/${rel_id}/assets?name=${asset}" \
        -H 'Content-Type: application/vnd.debian.binary-package' \
        --data-binary "@${deb}" >/dev/null
fi

echo "publish[$flavor]: ${asset} -> ${tag}  OK"
