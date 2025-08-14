#!/usr/bin/env bash
set -euo pipefail

######################
# PATHS (edit if needed)
######################
REPO_ROOT="/home/mahady/cascade"
ANSIBLE_DIR="$REPO_ROOT/cascade-ansible"
PLAYBOOKS="$ANSIBLE_DIR/playbooks"
COMPOSE_DIR="$REPO_ROOT/src/applications/tests/cascade_chain/docker_test_cfg"
PROJECT_NAME="cascade"

######################
# CLASSIFICATION RULES
######################
# BACKEND CODE (C/C++ + CMake + Dockerfile)
CODE_DIRS_REGEX='^(src/|include/|cmake/|CMakeLists\.txt$|.*\.cmake$|Dockerfile(|\..*)$)'
CODE_EXTS_REGEX='(\.c|\.cc|\.cpp|\.cxx|\.h|\.hh|\.hpp|\.hxx)$'
CODE_REGEX="(${CODE_DIRS_REGEX}|${CODE_EXTS_REGEX})"

# CONFIG (your test settings/templates only — NOT backend code)
CFG_REGEX='^(cascade-ansible/templates/|cascade-ansible/host_vars/all\.yml$)'

# IGNORE (noise / generated)
IGNORE_REGEX='(^\.git/|^\.github/|^cascade-ansible/output/|^cascade-ansible/cfg/|^cfg/|^build-|^cmake-build|/\.#|~$|^\.idea/|^\.vscode/)'

echo "[watch] starting in $REPO_ROOT (Ctrl+C to stop)"
cd "$REPO_ROOT"

# Ensure inotifywait exists
if ! command -v inotifywait >/dev/null 2>&1; then
  echo "[watch] ERROR: install inotify-tools"
  exit 1
fi

# Watch only relevant roots
WATCH_PATHS=( src include cmake cascade-ansible/templates cascade-ansible/host_vars )
[[ -f CMakeLists.txt ]] && WATCH_PATHS+=( CMakeLists.txt )
for f in Dockerfile Dockerfile.dev Dockerfile.ci; do [[ -f "$f" ]] && WATCH_PATHS+=( "$f" ); done
echo "[watch] paths: ${WATCH_PATHS[*]}"

# Debounce storms
last_ts=0
debounce_secs=1

inotifywait -m -r -e modify,move,create,delete --format '%T %w%f' --timefmt '%s' "${WATCH_PATHS[@]}" \
| while read -r ts changed; do
  rel="${changed#$REPO_ROOT/}"

  # debounce
  if (( ts - last_ts < debounce_secs )); then continue; fi
  last_ts=$ts

  # ignore junk
  if [[ "$rel" =~ $IGNORE_REGEX ]]; then continue; fi

  if [[ "$rel" =~ $CFG_REGEX ]]; then
    echo "[watch][config] $rel -> render + sync"
    ( cd "$PLAYBOOKS" && \
      ansible-playbook render.yml && \
      ansible-playbook sync_configs.yml ) \
      && echo "[watch][config] done" || echo "[watch][config] FAILED"
    continue
  fi

  if [[ "$rel" =~ $CODE_REGEX ]]; then
    echo "[watch][code] $rel -> build + up -> restart app"
    ( cd "$PLAYBOOKS" && \
      ansible-playbook -e "compose_dir=$COMPOSE_DIR project_name=$PROJECT_NAME" build_up.yml && \
      ansible-playbook -e "project_name=$PROJECT_NAME" restart_processes.yml ) \
      && echo "[watch][code] done" || echo "[watch][code] FAILED"
    continue
  fi
done

