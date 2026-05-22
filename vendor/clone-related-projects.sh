#!/usr/bin/env bash
# clone-related-projects.sh
#
# Bootstraps (or refreshes) the vendored upstream repos this project tracks
# for research and audit. None of these repos are built or shipped — they
# are read-only working copies used by the upstream-tracking workflow
# described in `jvm/trino-ducklake/dev-docs/RESEARCH-HOWTO.md`.
#
# Usage:
#   ./vendor/clone-related-projects.sh         # clone any missing, fetch the rest
#   ./vendor/clone-related-projects.sh --pull  # additionally fast-forward each repo
#
# Idempotent: missing repos are cloned; existing repos get `git fetch
# --all --prune` (and optionally `git pull --ff-only` on the current
# branch when --pull is passed).
#
# This script never modifies working-tree state of an existing repo
# beyond fetching, unless --pull is explicitly passed. The
# RESEARCH-HOWTO procedure relies on local HEAD staying at the previous
# baseline until the user advances it explicitly.

set -euo pipefail

PULL_AFTER_FETCH=0
for arg in "$@"; do
  case "$arg" in
    --pull) PULL_AFTER_FETCH=1 ;;
    -h|--help)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

VENDOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$VENDOR_DIR"

# repo_dir <TAB> clone URL. All HTTPS so the script works without SSH
# keys configured. pg_ducklake is hosted by Rely Cloud; the others are
# under github.com/duckdb or github.com/hotdata-dev.
REPOS=(
  "datafusion-ducklake	https://github.com/hotdata-dev/datafusion-ducklake.git"
  "duckdb-web	https://github.com/duckdb/duckdb-web.git"
  "ducklake	https://github.com/duckdb/ducklake.git"
  "ducklake-web	https://github.com/duckdb/ducklake-web.git"
  "pg_ducklake	https://github.com/relytcloud/pg_ducklake.git"
  "duckdb-quack https://github.com/duckdb/duckdb-quack.git"
)

clone_or_fetch() {
  local dir="$1" url="$2"

  if [[ -d "$dir/.git" ]]; then
    echo "==> $dir: fetching"
    git -C "$dir" fetch --all --prune --tags
    if [[ "$PULL_AFTER_FETCH" -eq 1 ]]; then
      local branch
      branch="$(git -C "$dir" symbolic-ref --short HEAD 2>/dev/null || echo "")"
      if [[ -n "$branch" ]]; then
        echo "    fast-forwarding $branch"
        git -C "$dir" pull --ff-only
      else
        echo "    detached HEAD — skipping pull"
      fi
    fi
  elif [[ -e "$dir" ]]; then
    echo "==> $dir: exists but is not a git checkout — skipping" >&2
  else
    echo "==> $dir: cloning from $url"
    git clone "$url" "$dir"
  fi
}

for entry in "${REPOS[@]}"; do
  dir="${entry%%	*}"
  url="${entry##*	}"
  clone_or_fetch "$dir" "$url"
done

echo
echo "Done. Next: run the survey procedure in"
echo "jvm/trino-ducklake/dev-docs/RESEARCH-HOWTO.md"
