#!/usr/bin/env bash
# HarborTrace end to end on one day of real AIS, one machine:
#
#   NOAA day file -> Spark preparation (canonical, day-partitioned) -> batch engine -> artifact
#                                                                  -> replay files -> Kafka topic
#                                                                     -> streaming engine -> updates
#   artifact + updates -> MongoDB serving store -> dashboard
#
#   bash scripts/demo_end_to_end.sh <step>     steps, in order (or 'all' for prepare..status):
#     check     tools, inputs and services; changes nothing
#     prepare   Spark preparation of the day                     (data component)
#     batch     batch engine -> completed-batch artifact         (processing engine)
#     replay    canonical -> half-hour replay files              (processing engine)
#     kafka     start the local broker and publish the replay    (data component)
#     stream    streaming engine reading the Kafka topic         (processing engine)
#     mongo     start a local MongoDB for this run               (serving)
#     serve     import the artifact, apply the stream updates    (serving)
#     status    serving generations and counts
#     dashboard serve the dashboard on http://127.0.0.1:8050/    (serving; Ctrl-C to stop)
#     stop      stop the MongoDB and Kafka started by this script
#
# Settings (environment): DAY=2024-01-01, PORTS="4120 4110" (or all), RUN=e2e-$DAY,
# MASTER=local[4], CANONICAL_ROOT (default data/e2e/$RUN/canonical; may be hdfs://...).
# Every step writes under data/e2e/$RUN and refuses to overwrite a finished output, so a
# failed step can be rerun after the cause is fixed. Nothing is deleted.
set -euo pipefail
cd "$(dirname "$0")/.."

DAY="${DAY:-2024-01-01}"
PORTS="${PORTS:-4120 4110}"
RUN="${RUN:-e2e-$DAY}"
MASTER="${MASTER:-local[4]}"
PY="${PY:-$HOME/.venvs/harbortrace/bin/python}"
SOURCE="${SOURCE:-/mnt/e/big_data/data/source/ais-$DAY.parquet}"
BOUNDARIES="${BOUNDARIES:-/mnt/e/big_data/data/boundaries/usace_ports.geojson}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-harbortrace.$RUN}"
MONGO_PORT="${MONGO_PORT:-27018}"
MONGO_URI="mongodb://127.0.0.1:$MONGO_PORT"
DATABASE="${DATABASE:-harbortrace_e2e}"

W="data/e2e/$RUN"
CANONICAL_ROOT="${CANONICAL_ROOT:-$PWD/$W/canonical}"
CANONICAL_DAY="$CANONICAL_ROOT/day=$DAY"
ARTIFACT="$W/artifact"
REPLAY="$W/replay"
STREAM="$W/stream"
MONGO_DIR="$W/mongo"
LOG="$W/logs"

export PYTHONPATH=src
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")}"
mkdir -p "$LOG"
read -r -a PORT_ARGS <<<"$PORTS"

say() { printf '\n[%s] %s\n' "$(date +%T)" "$*"; }
done_already() { if [ -e "$1" ]; then echo "Already done: $1 exists"; return 0; fi; return 1; }
run() { local name=$1; shift; "$@" 2>&1 | tee "$LOG/$name.log"; }
serving() { "$PY" scripts/serving_import.py --mongo-uri "$MONGO_URI" --database "$DATABASE" "$@"; }
need() { [ -f "$1" ] || { echo "Missing $1 ($2)" >&2; exit 1; }; }

step_check() {
  "$PY" -c 'import pyspark; print("pyspark", pyspark.__version__)'
  java -version 2>&1 | head -1
  for f in "$SOURCE" "$BOUNDARIES"; do [ -f "$f" ] && echo "found $f" || echo "MISSING $f"; done
  [ -f src/harbortrace/spark_prepare.py ] && echo "spark preparation: present" || echo "spark preparation: MISSING (PR #4)"
  [ -f deploy/native/kafka-start.sh ] && echo "Kafka scripts: present" || echo "Kafka scripts: MISSING (PR #3)"
  command -v mongod >/dev/null && echo "mongod: $(command -v mongod)" || echo "mongod: MISSING"
  echo "run directory: $W"; free -g | sed -n 2p
}

step_prepare() {
  need src/harbortrace/spark_prepare.py "merge PR #4"
  done_already "$CANONICAL_DAY" && return
  say "Spark preparation of $DAY for ports: $PORTS"
  run prepare "$PY" scripts/data_storage.py spark-prepare --source "$SOURCE" --boundaries "$BOUNDARIES" \
    --ports "${PORT_ARGS[@]}" --output "$CANONICAL_ROOT" --master "$MASTER" --driver-memory 3g
}

step_batch() {
  done_already "$ARTIFACT/manifest.json" && return
  say "Batch engine -> $ARTIFACT"
  run batch "$PY" scripts/engine_batch.py --real --run-id "$RUN" --ports "${PORT_ARGS[@]}" \
    --input "$CANONICAL_DAY" --output "$ARTIFACT" --master "$MASTER" --partitions 8 --driver-memory 3g
}

step_replay() {
  done_already "$REPLAY" && return
  say "Replay files -> $REPLAY"
  run replay "$PY" scripts/engine_experiments.py replay-spark --canonical "$CANONICAL_DAY" \
    --run-id "$RUN" --out "$REPLAY" --master "$MASTER"
}

step_kafka() {
  need deploy/native/kafka-start.sh "merge PR #3"
  done_already "$W/kafka-published" && return
  say "Kafka broker"
  bash deploy/native/kafka-start.sh || bash deploy/native/kafka-start.sh --init-empty
  say "Publish $REPLAY to topic $TOPIC"
  run kafka "$PY" scripts/data_storage.py kafka-publish --input "$REPLAY" --topic "$TOPIC" \
    --bootstrap "$KAFKA_BOOTSTRAP"
  date -u +%FT%TZ >"$W/kafka-published"
}

step_stream() {
  # Rerunning continues from the checkpoint in $STREAM.
  need "$W/kafka-published" "run the kafka step"
  say "Streaming engine reading kafka:$TOPIC -> $STREAM"
  run stream "$PY" scripts/engine_stream.py --source "kafka:$TOPIC" --kafka-bootstrap "$KAFKA_BOOTSTRAP" \
    --output "$STREAM" --stream-id "$RUN" --ports "${PORT_ARGS[@]}" --max-per-trigger 20000 \
    --master "$MASTER" --partitions 8 --driver-memory 3g --compare-artifact "$ARTIFACT"
}

step_mongo() {
  if mongod_running; then echo "MongoDB already running on port $MONGO_PORT"; return; fi
  mkdir -p "$MONGO_DIR"
  say "MongoDB on port $MONGO_PORT, data in $MONGO_DIR"
  mongod --dbpath "$MONGO_DIR" --port "$MONGO_PORT" --bind_ip 127.0.0.1 --fork --logpath "$LOG/mongod.log"
}
mongod_running() { "$PY" -c "import pymongo,sys; pymongo.MongoClient('$MONGO_URI', serverSelectionTimeoutMS=1500).admin.command('ping')" 2>/dev/null; }

step_serve() {
  need "$ARTIFACT/manifest.json" "run the batch step"
  [ -d "$STREAM/updates" ] || { echo "Missing $STREAM/updates (run the stream step)" >&2; exit 1; }
  mongod_running || { echo "MongoDB is not running (run the mongo step)" >&2; exit 1; }
  say "Import the batch artifact (completed generation)"
  run serve-import serving import "$ARTIFACT"
  say "Apply the streaming updates (provisional view)"
  run serve-updates serving apply-directory "$STREAM" --source-id "$RUN"
}

step_status() {
  # Summary only: the full status lists every provisional cohort.
  serving status | "$PY" -c '
import json, sys
s = json.load(sys.stdin)
cohorts = s.get("provisional", [])
print(json.dumps({"active": s.get("active"), "generations": len(s.get("generations", [])),
                  "health": s.get("health"), "provisional_cohorts": len(cohorts),
                  "provisional_visits": sum(c["visit_count"] for c in cohorts)}, indent=2))'
}

step_dashboard() {
  say "Dashboard on http://127.0.0.1:8050/ (Ctrl-C to stop)"
  PYTHONPATH=src:. "$PY" scripts/serve_dashboard.py --mongo-uri "$MONGO_URI" --database "$DATABASE"
}

step_stop() {
  if mongod_running; then mongod --shutdown --dbpath "$MONGO_DIR"; fi
  [ -f deploy/native/kafka-stop.sh ] && bash deploy/native/kafka-stop.sh || true
}

step_all() {
  for s in check prepare batch replay kafka stream mongo serve status; do "step_$s"; done
  say "Done. Next: bash $0 dashboard"
}

step=${1:-}
declare -F "step_$step" >/dev/null || { sed -n 2,24p "$0"; exit 2; }
"step_$step"
