#!/usr/bin/env bash
# End-to-end validation runner. Resolves a checkout of the module source,
# builds and starts the server from this repository's docker compose,
# prepares the Python environment, then executes every suite in order.
# Any suite failure fails the run.
#
# Usage:  bash run_all.sh              # standard run (~10 minutes; suite 09 dominates)
#         FULL=1 bash run_all.sh       # adds the large datasets to suite 01
#         bash run_all.sh 03 09        # run only the given suite numbers
#
# Module source, in precedence order:
#   VR_SOURCE=/path/to/valkey-roaring   explicit checkout (built as-is)
#   ../valkey-roaring                   sibling checkout, if present
#   .module-src                         cloned/updated from VR_REPO at VR_REF
#
#   VR_REPO   git remote to clone   (default: the public valkey-roaring repo)
#   VR_REF    branch, tag, or full 40-char SHA   (default: main)
#   VR_KEEP=1 leave the server running after a passing run

set -uo pipefail
cd "$(dirname "$0")"

VR_REPO="${VR_REPO:-https://github.com/fjmaco/valkey-roaring.git}"
VR_REF="${VR_REF:-main}"

# --- module source ---------------------------------------------------------
if [ -n "${VR_SOURCE:-}" ]; then
  if [ ! -f "$VR_SOURCE/Dockerfile" ]; then
    echo "VR_SOURCE=$VR_SOURCE has no Dockerfile — not a valkey-roaring checkout." >&2
    exit 1
  fi
elif [ -f ../valkey-roaring/Dockerfile ]; then
  VR_SOURCE=../valkey-roaring
else
  # init + fetch rather than clone: `git clone --branch` takes a branch or a
  # tag but not a commit, and fetching a ref works the same whether the
  # checkout is new or already there.
  [ -d .module-src/.git ] || git init --quiet .module-src
  git -C .module-src remote add origin "$VR_REPO" 2>/dev/null \
    || git -C .module-src remote set-url origin "$VR_REPO"
  git -C .module-src fetch --quiet --depth 1 origin "$VR_REF" \
    || { echo "Could not fetch $VR_REF from $VR_REPO" >&2; exit 1; }
  git -C .module-src checkout --quiet --force FETCH_HEAD
  VR_SOURCE=.module-src
fi
export VR_SOURCE
echo "module source: $VR_SOURCE ($(git -C "$VR_SOURCE" rev-parse --short HEAD 2>/dev/null || echo 'not a git checkout'))"

# --- environment -----------------------------------------------------------
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
  .venv/bin/pip install --quiet -r requirements.txt
fi

# --- server ----------------------------------------------------------------
docker compose up -d --build || { echo "compose up failed" >&2; exit 1; }
for _ in $(seq 1 60); do
  [ "$(docker compose exec -T valkey valkey-cli PING 2>/dev/null)" = "PONG" ] && break
  sleep 1
done
if [ "$(docker compose exec -T valkey valkey-cli PING 2>/dev/null)" != "PONG" ]; then
  echo "server did not come up:" >&2
  docker compose logs >&2
  exit 1
fi

# --- suites ----------------------------------------------------------------
filter=("$@")
overall=0
declare -a summary
for suite in suites/test_*.py; do
  num=$(basename "$suite" | cut -d_ -f2)
  if [ ${#filter[@]} -gt 0 ] && [[ ! " ${filter[*]} " == *" $num "* ]]; then
    continue
  fi
  echo
  echo "================ $(basename "$suite") ================"
  if .venv/bin/python "$suite"; then
    summary+=("PASS  $(basename "$suite")")
  else
    summary+=("FAIL  $(basename "$suite")")
    overall=1
  fi
done

echo
echo "======================= SUMMARY ======================="
printf '%s\n' "${summary[@]}"

# Clean up on success so the next run starts from a fresh volume; leave a
# failed run's server up so it can be inspected.
if [ "$overall" = "0" ] && [ "${VR_KEEP:-0}" = "0" ]; then
  docker compose down --volumes >/dev/null 2>&1
else
  echo
  echo "server left running for inspection (docker compose down --volumes to clear)"
fi
exit $overall
