#!/usr/bin/env bash
# release.sh — push the current branch and its version tag to origin (Gitea),
# which triggers the release CI. Run via `make release`, AFTER `make bump`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "release: not a git repository" >&2
  exit 1
fi

branch="$(git rev-parse --abbrev-ref HEAD)"
ver="$(scripts/build/version.sh)"
tag="v${ver}"

# Preflight 1: clean tree — CI builds from the pushed commit, so uncommitted
# changes would silently not be in the released artifact.
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
  echo "release: working tree is dirty; commit or stash before releasing" >&2
  exit 1
fi

# Preflight 2: the version tag must exist (i.e. 'make bump' was run).
if ! git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "release: tag ${tag} not found — run 'make bump' first" >&2
  exit 1
fi

# Preflight 3: the tag must point at HEAD (release the tip, not an old commit).
if [ "$(git rev-parse "${tag}^{commit}")" != "$(git rev-parse HEAD)" ]; then
  echo "release: ${tag} does not point at HEAD; bump+release must be the tip" >&2
  exit 1
fi

echo "releasing ${tag} from ${branch} -> origin"
git push origin "${branch}" --follow-tags
echo "pushed; Gitea Actions will build and publish to Gitea + GitHub"
