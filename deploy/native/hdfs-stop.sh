#!/usr/bin/env bash
set -euo pipefail

# Stops only the HDFS processes recorded by hdfs-start.sh.
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
pid_dir="$HARBORTRACE_DATA_DIR/run"

stop_owned() {
  local label="$1" expected="$2"
  local pid_file="$pid_dir/$label.pid" pid
  if [[ ! -f "$pid_file" ]]; then
    printf '%s has no PID file\n' "$label"
    return
  fi
  pid="$(cat "$pid_file")"
  if [[ ! "$pid" =~ ^[0-9]+$ ]] || ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    printf '%s was not running; removed stale PID file\n' "$label"
    return
  fi
  if ! ps -p "$pid" -o args= | grep -Fq "$expected"; then
    echo "PID file $pid_file points to another process; refusing to stop it" >&2
    exit 1
  fi
  kill "$pid"
  for _ in {1..30}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      printf 'Stopped %s (PID %s)\n' "$label" "$pid"
      return
    fi
    sleep 1
  done
  echo "$label (PID $pid) did not stop within 30 seconds" >&2
  exit 1
}

stop_owned datanode 'org.apache.hadoop.hdfs.server.datanode.DataNode'
stop_owned namenode 'org.apache.hadoop.hdfs.server.namenode.NameNode'
