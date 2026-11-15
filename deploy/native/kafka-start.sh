#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
export KAFKA_HOME="${KAFKA_HOME:-$runtime_dir/kafka_2.13-4.1.2}"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx512m -Xms512m}"
data_dir="$HARBORTRACE_DATA_DIR"
kafka_dir="$data_dir/kafka"
log_dir="$kafka_dir/logs"
properties="$kafka_dir/server.properties"
pid_file="$data_dir/run/kafka.pid"
export LOG_DIR="$data_dir/logs/kafka"
mkdir -p "$data_dir/run" "$LOG_DIR" "$log_dir"
test -x "$KAFKA_HOME/bin/kafka-server-start.sh"
sed "s#@KAFKA_LOG_DIR@#$log_dir#" "$deploy_dir/kafka-server.properties" >"$properties"

if [[ ! -f "$log_dir/meta.properties" ]]; then
  if [[ "${1:-}" != "--init-empty" ]]; then
    echo "No HarborTrace Kafka metadata; pass --init-empty only for a new, empty broker store" >&2
    exit 1
  fi
  if [[ -n "$(find "$log_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Existing unrecognized Kafka log directory; refusing to format" >&2
    exit 1
  fi
  cluster_id="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$cluster_id" -c "$properties" >"$LOG_DIR/format.log" 2>&1
fi

if [[ -f "$pid_file" ]]; then
  pid="$(cat "$pid_file")"
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    if ps -p "$pid" -o args= | grep -Fq 'kafka.Kafka'; then
      printf 'kafka already running as PID %s\n' "$pid"
      exit 0
    fi
    echo "PID file $pid_file points to another process; refusing to replace it" >&2
    exit 1
  fi
fi
for port in 9092 9093; do
  if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
    echo "Port $port is occupied; refusing to start kafka" >&2
    exit 1
  fi
done
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$properties" >>"$LOG_DIR/kafka.out" 2>&1 </dev/null &
pid=$!
printf '%s\n' "$pid" >"$pid_file"
for _ in {1..60}; do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "kafka exited; inspect $LOG_DIR/kafka.out" >&2
    exit 1
  fi
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; then
    printf 'Started kafka as PID %s; bootstrap 127.0.0.1:9092\n' "$pid"
    exit 0
  fi
  sleep 1
done
echo "kafka did not answer on 127.0.0.1:9092 within 60 seconds; inspect $LOG_DIR/kafka.out" >&2
exit 1
