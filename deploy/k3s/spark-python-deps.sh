#!/usr/bin/env bash
set -euo pipefail

# Prepares the Python packages for Spark pods (see spark/python-deps.yaml). Needs sudo once, to
# create the node directory owned by the current user. Rerun after changing the package list.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
image="${SPARK_IMAGE:-apache/spark:3.5.7-scala2.12-java17-python3-ubuntu}"
python_deps="${HARBORTRACE_SPARK_PYTHON_DEPS:-/var/lib/harbortrace/spark-python}"
# Versions of requirements-week05/engine/serving.txt, which import cleanly on the image's Python 3.10.
packages=(pyarrow==17.0.0 shapely==2.0.6 numpy==2.1.3 pandas==2.2.3 requests==2.32.3 pymongo==4.10.1)

sudo mkdir -p "$python_deps"
sudo chown "$(id -u):$(id -g)" "$python_deps"
quoted="$(printf '"%s", ' "${packages[@]}")"
kubectl -n harbortrace delete job spark-python-deps --ignore-not-found >/dev/null
sed -e "s#@IMAGE@#$image#g" -e "s#@PYTHON_DEPS@#$python_deps#g" -e "s#@UID@#$(id -u)#g" -e "s#@GID@#$(id -g)#g" \
    -e "s#@PACKAGES@#${quoted%, }#" "$here/spark/python-deps.yaml" | kubectl apply -f -
kubectl -n harbortrace wait --for=condition=complete job/spark-python-deps --timeout=900s
kubectl -n harbortrace logs job/spark-python-deps | tail -2
printf 'Spark Python packages in %s: %s\n' "$python_deps" "${packages[*]}"
