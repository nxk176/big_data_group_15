#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
hdfs="$HADOOP_HOME/bin/hdfs"
fixture="$project_root/tests/fixtures/hdfs_week02.txt"
destination="$HDFS_URI/harbortrace/week02/hdfs_week02.txt"
folder="$HDFS_URI/harbortrace/week02"
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

local_sha="$(sha_of 256 <"$fixture")"
remote_sha="$("$hdfs" dfs -cat "$destination" | sha_of 256)"
remote_bytes="$("$hdfs" dfs -stat '%b' "$destination")"
if [[ "$local_sha" != "$remote_sha" || "$remote_bytes" -ne "$(wc -c <"$fixture")" ]]; then
  echo "HDFS round-trip checksum or byte count differs from the fixture" >&2
  exit 1
fi
printf 'HDFS smoke passed: live_datanodes=%s bytes=%s sha256=%s path=%s\n' \
  "$live_nodes" "$remote_bytes" "$remote_sha" "$destination"
