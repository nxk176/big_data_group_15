#!/usr/bin/env bash
set -euo pipefail

project_root="/mnt/e/big_data"
data_dir="$project_root/data"
runtime_dir="${HARBORTRACE_RUNTIME_DIR:-$HOME/.local/harbortrace}"
export HADOOP_HOME="$runtime_dir/hadoop-3.5.0"
export HADOOP_CONF_DIR="$project_root/deploy/native"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_HEAPSIZE_MAX=512m
pid_dir="$data_dir/run"
log_dir="$data_dir/logs/native"
name_dir="$data_dir/hdfs/name"
mkdir -p "$pid_dir" "$log_dir" "$name_dir" "$data_dir/hdfs/datanode"
test -x "$HADOOP_HOME/bin/hdfs"

if [[ ! -f "$name_dir/current/VERSION" ]]; then
  if [[ "${1:-}" != "--init-empty" ]]; then
    echo "No HarborTrace NameNode namespace; pass --init-empty only for a new, empty project store" >&2
    exit 1
  fi
  if [[ -n "$(find "$name_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Existing unrecognized NameNode data; refusing to format" >&2
    exit 1
  fi
  if [[ -n "$(find "$data_dir/hdfs/datanode" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Existing DataNode data without a recognized NameNode; refusing to format" >&2
    exit 1
  fi
  "$HADOOP_HOME/bin/hdfs" namenode -format -nonInteractive -clusterId harbortrace-local-1 >"$log_dir/namenode-format.log" 2>&1
fi

start_owned() {
  local label="$1" expected="$2" ports="$3" service="$4"
  local pid_file="$pid_dir/$label.pid" pid port
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      if ps -p "$pid" -o args= | grep -Fq "$expected"; then
        printf '%s already running as PID %s\n' "$label" "$pid"
        return
      fi
      echo "PID file $pid_file points to another process; refusing to replace it" >&2
      exit 1
    fi
  fi
  for port in $ports; do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      echo "Port $port is occupied; refusing to start $label" >&2
      exit 1
    fi
  done
  nohup "$HADOOP_HOME/bin/hdfs" "$service" >>"$log_dir/$label.log" 2>&1 </dev/null &
  pid=$!
  printf '%s\n' "$pid" >"$pid_file"
  sleep 3
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "$label exited; inspect $log_dir/$label.log" >&2
    exit 1
  fi
  printf 'Started %s as PID %s\n' "$label" "$pid"
}

start_owned namenode 'org.apache.hadoop.hdfs.server.namenode.NameNode' '9000 9870' namenode
start_owned datanode 'org.apache.hadoop.hdfs.server.datanode.DataNode' '9866 9864 9867' datanode
