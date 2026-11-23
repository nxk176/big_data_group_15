#!/usr/bin/env bash
set -euo pipefail

# Stops only the broker recorded by kafka-start.sh; a clean shutdown flushes its logs.
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
pid_file="$HARBORTRACE_DATA_DIR/run/kafka.pid"
if [[ ! -f "$pid_file" ]]; then
  echo "kafka has no PID file"
  exit 0
fi
pid="$(cat "$pid_file")"
if [[ ! "$pid" =~ ^[0-9]+$ ]] || ! kill -0 "$pid" 2>/dev/null; then
  rm -f "$pid_file"
  echo "kafka was not running; removed stale PID file"
  exit 0
fi
if ! ps -p "$pid" -o args= | grep -Fq 'kafka.Kafka'; then
  echo "PID file $pid_file points to another process; refusing to stop it" >&2
  exit 1
fi
kill "$pid"
for _ in {1..60}; do
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    printf 'Stopped kafka (PID %s)\n' "$pid"
    exit 0
  fi
  sleep 1
done
echo "kafka (PID $pid) did not stop within 60 seconds" >&2
exit 1
