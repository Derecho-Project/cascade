#!/bin/bash

set -euo pipefail

SCRIPT_NAME=$(basename "${BASH_SOURCE[0]}")
HELP_STRING="A helper script to set up a local cascade instance
Usage: $SCRIPT_NAME [options]
  -h            Print this message and exit.
  -d    D       CFG directory location (defaults to cfg).
  -n    N       Sets a range from 0..N-1 for range commands.

  -u    K       update key in range.
  -v    V       update value in range.
                (ex. -n 7 -u default_log_level -v info)

  -k            Kill running cascade nodes.
  -r            Remove old logs in range.
  -s            Start server nodes in range.
  -f    D       directory to mount (requires -c flag to start and -k to end).
  -F    D       directory to mount (runs in foreground).
  -c    I       Start client node at folder cfg/nI."

CFG_DIR="$PWD/cfg"
FUSE_FOREGROUND=false
FUSE_DIR=""
HELP=false
KILL=false
CLEAN=false
SERVERS=false
NODES=0
CLIENT=-1

UPDATE_KEY=""
UPDATE_VALUE=""

# TODO get from env? or as arg
BASE_PATH="/root/workspace/cascade/build-Release/src"

export PATH="$PATH:$BASE_PATH/service"
export PATH="$PATH:$BASE_PATH/service/fuse"

FUSE_COMMAND="cascade_fuse_client_hl" # "cascade_fuse_client"
CLIENT_COMMAND="cascade_client"
SERVER_COMMAND="cascade_server"
LOG_FILE="node_info.log"

if ! command -v $SERVER_COMMAND &>/dev/null; then
  echo "$SERVER_COMMAND could not be found"
  exit_abnormal
fi

usage() {
  echo "$HELP_STRING" 1>&2
}

exit_abnormal() {
  usage
  exit 1
}

check_num() {
  # $1: string to check
  RE_ISNUM='^[0-9]+$'
  if ! [[ $1 =~ $RE_ISNUM ]]; then
    exit_abnormal
  fi
}

foreach() {
  # $1: command to do (accepts count as arg)
  COUNT=0
  while [ $COUNT -lt "$NODES" ]; do
    DIR="$CFG_DIR/n$COUNT"
    cd "$DIR"

    $1 $COUNT

    cd - &>/dev/null
    ((COUNT += 1))
  done
}

main() {
  while getopts 'hd:n:f:F:krsc:u:v:' opt; do
    case "$opt" in
    h) HELP=true ;;
    d) CFG_DIR="$OPTARG" ;;
    n)
      NODES="$OPTARG"
      check_num "$NODES"
      ;;
    f) FUSE_DIR="$OPTARG" ;;
    F)
      FUSE_DIR="$OPTARG"
      FUSE_FOREGROUND=true
      ;;
    k) KILL=true ;;
    r) CLEAN=true ;;
    s) SERVERS=true ;;
    c)
      CLIENT="$OPTARG"
      check_num "$CLIENT"
      ;;
    u) UPDATE_KEY="$OPTARG" ;;
    v) UPDATE_VALUE="$OPTARG" ;;
    *) exit_abnormal ;;
    esac
  done

  if [[ "$HELP" = true ]]; then
    usage
    exit
  fi

  if [[ -n "$FUSE_DIR" ]]; then
    MOUNT="$(realpath "$FUSE_DIR")"

    if [[ ! "$CLIENT" -eq "-1" ]]; then
      DIR="$CFG_DIR/n$CLIENT"
      cd "$DIR"

      echo "fuse client mounting at $MOUNT"
      mkdir -p "$MOUNT"

      if [[ "$FUSE_FOREGROUND" = true ]]; then
        "$FUSE_COMMAND" -s -f "$MOUNT"
      else
        "$FUSE_COMMAND" -s -f "$MOUNT" &>"$LOG_FILE" &
      fi

      cd - &>/dev/null
      exit
    fi

    if [[ "$KILL" = true ]]; then
      echo "unmounting $MOUNT and ending fuses client"
      umount "$MOUNT"

      pkill -SIGINT "$FUSE_COMMAND" || true

      exit
    fi
  fi

  if [[ "$KILL" = true ]]; then
    # TODO better idea: save PIDs?

    pkill -SIGINT "$CLIENT_COMMAND" || true
    pkill -SIGINT "$SERVER_COMMAND" || true # kill cascade_server

    # kill old cascade_runner and subprocesses (excluding itself)
    # pgrep "${SCRIPT_NAME%.*}" | grep -v "$$" | while read -r LINE; do
    #   SUB_SLEEP=$(pgrep -P "$LINE")
    #   kill "$LINE"      # kill cascade_runner while loop
    #   kill "$SUB_SLEEP" # kill sleep
    # done
  fi

  if [[ "$CLEAN" = true ]]; then
    x() {
      echo "$1 > removing .plog and logs"
      rm -rf .plog derecho_debug.log "$LOG_FILE"
    }
    foreach x
  fi

  if [[ -n "$UPDATE_KEY" ]]; then
    x() {
      CONFIG_FILE="derecho.cfg"
      echo "$1 > updating $UPDATE_KEY with $UPDATE_VALUE in $CONFIG_FILE"
      sed -i "s/\($UPDATE_KEY *= *\)\(.*\)/\1$UPDATE_VALUE/" "$CONFIG_FILE"
    }
    foreach x
  fi

  if [[ ! "$CLIENT" -eq "-1" ]]; then
    DIR="$CFG_DIR/n$CLIENT"

    if [[ -z "$FUSE_DIR" ]]; then
      cd "$DIR"
      "$CLIENT_COMMAND"
    fi

    cd - &>/dev/null
    exit
  fi
  set +e

  if [[ "$SERVERS" = true ]]; then
    x() {
      echo "$1 > starting server node"
      "$SERVER_COMMAND" --signal &>"$LOG_FILE" &
      # (while true; do sleep 10000; done) |
      #   "$SERVER_COMMAND" &>"$LOG_FILE" &
    }
    foreach x

  fi
}

main "$@"