#!/usr/bin/env bash
set -euo pipefail

# Deploys MongoDB (with users), Kafka, the dashboard and the Spark service account to the
# harbortrace namespace of the local k3s. Rerunning applies changes and keeps the data.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$here/../native/env.sh"
state="$HARBORTRACE_DATA_DIR/k3s"
credentials="$state/credentials.env"
mkdir -p "$state"
chmod 700 "$state"

# Passwords are generated once and kept outside Git. MongoDB creates its users only on an empty
# volume, so a later change here would not reach an existing database.
if [[ ! -f "$credentials" ]]; then
  secret() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
  database="${HARBORTRACE_DATABASE:-harbortrace}"
  admin_password="$(secret)" writer_password="$(secret)" reader_password="$(secret)"
  umask 077
  cat >"$credentials" <<CREDENTIALS
HARBORTRACE_DATABASE=$database
MONGO_ADMIN_USER=admin
MONGO_ADMIN_PASSWORD=$admin_password
HARBORTRACE_WRITER_USER=harbortrace_writer
HARBORTRACE_WRITER_PASSWORD=$writer_password
HARBORTRACE_READER_USER=harbortrace_reader
HARBORTRACE_READER_PASSWORD=$reader_password
# From the node (WSL / VM): import and publish with the writer, read with the reader.
HARBORTRACE_MONGO_URI=mongodb://harbortrace_writer:$writer_password@127.0.0.1:30017/$database?authSource=$database
HARBORTRACE_MONGO_READER_URI=mongodb://harbortrace_reader:$reader_password@127.0.0.1:30017/$database?authSource=$database
HARBORTRACE_KAFKA_BOOTSTRAP=127.0.0.1:30092
HARBORTRACE_DASHBOARD_URL=http://127.0.0.1:30050/
CREDENTIALS
  umask 022
fi
set -a
source "$credentials"
set +a

kubectl apply -f "$here/manifests/namespace.yaml"
kubectl -n harbortrace create secret generic mongodb-credentials \
  --from-literal=MONGO_INITDB_ROOT_USERNAME="$MONGO_ADMIN_USER" \
  --from-literal=MONGO_INITDB_ROOT_PASSWORD="$MONGO_ADMIN_PASSWORD" \
  --from-literal=HARBORTRACE_DATABASE="$HARBORTRACE_DATABASE" \
  --from-literal=HARBORTRACE_WRITER_USER="$HARBORTRACE_WRITER_USER" \
  --from-literal=HARBORTRACE_WRITER_PASSWORD="$HARBORTRACE_WRITER_PASSWORD" \
  --from-literal=HARBORTRACE_READER_USER="$HARBORTRACE_READER_USER" \
  --from-literal=HARBORTRACE_READER_PASSWORD="$HARBORTRACE_READER_PASSWORD" \
  --from-literal=HARBORTRACE_READER_URI_CLUSTER="mongodb://$HARBORTRACE_READER_USER:$HARBORTRACE_READER_PASSWORD@mongodb.harbortrace.svc.cluster.local:27017/$HARBORTRACE_DATABASE?authSource=$HARBORTRACE_DATABASE" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$here/manifests/mongodb.yaml" -f "$here/manifests/kafka.yaml" -f "$here/manifests/spark-rbac.yaml"
sed "s#@REPO@#$project_root#g" "$here/manifests/dashboard.yaml" | kubectl apply -f -

kubectl -n harbortrace rollout status statefulset/mongodb --timeout=300s
kubectl -n harbortrace rollout status statefulset/kafka --timeout=300s
kubectl -n harbortrace rollout status deployment/dashboard --timeout=300s
kubectl -n harbortrace get pods,services -o wide
cat <<SUMMARY

HarborTrace services on k3s (namespace harbortrace)
  MongoDB    127.0.0.1:30017 (node)  mongodb.harbortrace.svc.cluster.local:27017 (pods)  database $HARBORTRACE_DATABASE
  Kafka      127.0.0.1:30092 (node)  kafka.harbortrace.svc.cluster.local:9092 (pods)
  Dashboard  $HARBORTRACE_DASHBOARD_URL  (read-only MongoDB user)
  Users and connection strings: $credentials  (set -a; source it; set +a)
SUMMARY
