#!/usr/bin/env bash
set -euo pipefail

project_root="/mnt/e/big_data"
runtime_dir="${HARBORTRACE_RUNTIME_DIR:-$HOME/.local/harbortrace}"
export HADOOP_HOME="$runtime_dir/hadoop-3.5.0"
export HADOOP_CONF_DIR="$project_root/deploy/native"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
hdfs="$HADOOP_HOME/bin/hdfs"
fixture="$project_root/tests/fixtures/hdfs_week02.txt"
destination="hdfs://127.0.0.1:9000/harbortrace/week02/hdfs_week02.txt"
folder="hdfs://127.0.0.1:9000/harbortrace/week02"
test -x "$hdfs"
test -f "$fixture"

report="$($hdfs dfsadmin -report)"
live_nodes="$(printf '%s\n' "$report" | sed -n 's/^Live datanodes (\([0-9][0-9]*\)):.*/\1/p' | head -1)"
if [[ -z "$live_nodes" || "$live_nodes" -lt 1 ]]; then
  echo "No live DataNode reported by HDFS" >&2
  exit 1
fi

safe_mode_off=false
for _ in {1..40}; do
  if [[ "$("$hdfs" dfsadmin -safemode get 2>/dev/null)" == *"OFF"* ]]; then
    safe_mode_off=true
    break
  fi
  sleep 1
done
if [[ "$safe_mode_off" != true ]]; then
  echo "NameNode did not leave Safe Mode within 40 seconds" >&2
  exit 1
fi

"$hdfs" dfs -mkdir -p "$folder"
if ! "$hdfs" dfs -test -e "$destination"; then
  temporary="$destination.upload-$$"
  if "$hdfs" dfs -test -e "$temporary"; then
    echo "Unfinished upload exists: $temporary" >&2
    exit 1
  fi
  "$hdfs" dfs -put "$fixture" "$temporary"
  "$hdfs" dfs -mv "$temporary" "$destination"
fi

local_sha="$(sha256sum "$fixture" | cut -d ' ' -f 1)"
remote_sha="$("$hdfs" dfs -cat "$destination" | sha256sum | cut -d ' ' -f 1)"
remote_bytes="$("$hdfs" dfs -stat '%b' "$destination")"
if [[ "$local_sha" != "$remote_sha" || "$remote_bytes" -ne "$(wc -c <"$fixture")" ]]; then
  echo "HDFS round-trip checksum or byte count differs from the fixture" >&2
  exit 1
fi
printf 'HDFS smoke passed: live_datanodes=%s bytes=%s sha256=%s path=%s\n' \
  "$live_nodes" "$remote_bytes" "$remote_sha" "$destination"
