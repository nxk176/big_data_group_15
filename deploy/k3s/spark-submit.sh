#!/usr/bin/env bash
set -euo pipefail

# Runs a project script as a Spark application on the local k3s:
#   deploy/k3s/spark-submit.sh scripts/engine_batch.py --real --run-id ... --input hdfs://127.0.0.1:9000/... --output ...
#   deploy/k3s/spark-submit.sh scripts/data_storage.py spark-prepare --source data/source/... --output hdfs://...
# The driver and executors are pods (cluster deploy mode) built from the official Spark image;
# "--master submit" is appended so the script leaves the master to spark-submit.
# With hostNetwork every pod shares the node's ports: the driver keeps 7078/7079 (executors connect
# to it there) and executors take free ports (blockManager.port=0), so several fit on one node.
# They also share the node's address, so Spark would read another executor's shuffle files from
# "local" disk, which is a different pod's private directory: host-local reads are switched off.
# Settings: SPARK_EXECUTORS (2), SPARK_EXECUTOR_CORES (1), SPARK_EXECUTOR_MEMORY (2g), SPARK_DRIVER_MEMORY (2g),
# SPARK_PACKAGES (Maven coordinates resolved when the driver starts; a Kafka source needs
# org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7, because a session option is too late in cluster mode).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$here/../native/env.sh"
if [[ $# -lt 1 || ! -f "$project_root/$1" ]]; then
  echo "usage: $0 <script under the checkout, e.g. scripts/engine_batch.py> [script arguments]" >&2
  exit 1
fi
script="$1"
shift
image="${SPARK_IMAGE:-apache/spark:3.5.7-scala2.12-java17-python3-ubuntu}"
python_deps="${HARBORTRACE_SPARK_PYTHON_DEPS:-/var/lib/harbortrace/spark-python}"
spark_submit="${SPARK_SUBMIT:-$project_root/.venv/bin/spark-submit}"
test -x "$spark_submit"
# pip's spark-submit locates Spark with whatever python3 is first on PATH; point it at the checkout's venv.
export SPARK_HOME="${SPARK_HOME:-$("$project_root/.venv/bin/python" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')}"
test -d "$python_deps" || { echo "Python packages for the Spark image are missing: run deploy/k3s/spark-python-deps.sh" >&2; exit 1; }

state="$HARBORTRACE_DATA_DIR/k3s/spark"
mkdir -p "$state"
driver_template="$state/driver-template.yaml"
executor_template="$state/executor-template.yaml"
sed -e "s#@REPO@#$project_root#g" -e "s#@PYTHON_DEPS@#$python_deps#g" -e "s#@UID@#$(id -u)#g" \
    -e "s#@GID@#$(id -g)#g" -e "s#@USER@#$(id -un)#g" \
    -e "s#@REVISION@#$(git -C "$project_root" rev-parse HEAD 2>/dev/null || true)#g" \
    "$here/spark/pod-template.yaml" >"$driver_template"
# Executors download the files the driver distributes (the zipped package) into their work
# directory: not the checkout, and not the image's /opt/spark/work-dir, which this user cannot write.
sed "s#workingDir: .*#workingDir: /tmp#" "$driver_template" >"$executor_template"
name="harbortrace-$(basename "$script" .py | tr '_' '-')"
api_server="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"

# Submit with a two-hour token of the namespace's spark account instead of the cluster-admin
# certificate: least privilege, and Spark 3.5's Kubernetes client cannot read k3s's EC client
# keys without BouncyCastle on the classpath.
kubeconfig="$state/kubeconfig"
ca_data="$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
token="$(kubectl -n harbortrace create token spark --duration=2h)"
(umask 077; cat >"$kubeconfig" <<KUBECONFIG
apiVersion: v1
kind: Config
clusters: [{name: k3s, cluster: {server: "$api_server", certificate-authority-data: "$ca_data"}}]
users: [{name: spark, user: {token: "$token"}}]
contexts: [{name: spark, context: {cluster: k3s, user: spark, namespace: harbortrace}}]
current-context: spark
KUBECONFIG
)
export KUBECONFIG="$kubeconfig"
# spark-submit would ship HADOOP_CONF_DIR (fs.defaultFS = HDFS) to the pods, turning every path
# without a scheme into an HDFS path. Paths stay local, as on the node; HDFS needs hdfs:// URIs.
unset HADOOP_CONF_DIR

packages=()
if [[ -n "${SPARK_PACKAGES:-}" ]]; then
  packages=(--packages "$SPARK_PACKAGES")
fi

cd "$project_root"
exec "$spark_submit" "${packages[@]}" \
  --master "k8s://$api_server" \
  --deploy-mode cluster \
  --name "$name" \
  --conf spark.kubernetes.namespace=harbortrace \
  --conf spark.kubernetes.container.image="$image" \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.driver.podTemplateFile="$driver_template" \
  --conf spark.kubernetes.executor.podTemplateFile="$executor_template" \
  --conf spark.kubernetes.driver.podTemplateContainerName=spark \
  --conf spark.kubernetes.executor.podTemplateContainerName=spark \
  --conf spark.executor.instances="${SPARK_EXECUTORS:-2}" \
  --conf spark.executor.cores="${SPARK_EXECUTOR_CORES:-1}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-2g}" \
  --conf spark.kubernetes.driver.label.app=harbortrace-spark \
  --conf spark.kubernetes.executor.label.app=harbortrace-spark \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  --conf spark.driver.port=7078 \
  --conf spark.driver.blockManager.port=7079 \
  --conf spark.blockManager.port=0 \
  --conf spark.shuffle.readHostLocalDisk=false \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  "local://$project_root/$script" "$@" --master submit
