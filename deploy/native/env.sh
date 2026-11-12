#!/usr/bin/env bash
# Shared environment for the native HDFS scripts. Source it; do not execute it.
# Paths derive from this checkout unless overridden, so no laptop path is assumed.

deploy_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="${HARBORTRACE_PROJECT_ROOT:-$(cd "$deploy_dir/../.." && pwd)}"
export HARBORTRACE_DATA_DIR="${HARBORTRACE_DATA_DIR:-$project_root/data}"
runtime_dir="${HARBORTRACE_RUNTIME_DIR:-$HOME/.local/harbortrace}"
export HADOOP_HOME="${HADOOP_HOME:-$runtime_dir/hadoop-3.5.0}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$deploy_dir}"
export HDFS_URI="${HDFS_URI:-hdfs://127.0.0.1:9000}"

if [[ -z "${JAVA_HOME:-}" ]]; then
  if [[ -x /usr/libexec/java_home ]] && /usr/libexec/java_home -v 17 >/dev/null 2>&1; then
    JAVA_HOME="$(/usr/libexec/java_home -v 17)"
  elif [[ -x /opt/homebrew/opt/openjdk@17/bin/java ]]; then
    JAVA_HOME=/opt/homebrew/opt/openjdk@17
  elif [[ -x /usr/local/opt/openjdk@17/bin/java ]]; then
    JAVA_HOME=/usr/local/opt/openjdk@17
  else
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
  fi
fi
export JAVA_HOME

# GNU coreutils on Linux, shasum on macOS.
sha_of() {
  local bits="$1"
  if command -v "sha${bits}sum" >/dev/null 2>&1; then
    "sha${bits}sum" | cut -d ' ' -f 1
  else
    shasum -a "$bits" | cut -d ' ' -f 1
  fi
}
