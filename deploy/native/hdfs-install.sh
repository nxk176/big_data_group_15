#!/usr/bin/env bash
set -euo pipefail

# Installs only the Hadoop runtime. Project data always stays in E:\big_data.
runtime_dir="${HARBORTRACE_RUNTIME_DIR:-$HOME/.local/harbortrace}"
archive="$runtime_dir/downloads/hadoop-3.5.0.tar.gz"
target="$runtime_dir/hadoop-3.5.0"
url="https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz"
sha512="04ab94496cc00c8b7a28d03f6308eff8d2a4e7f37a9da5e8e086e4d6fc990e7a94d661908f6a6136039536efb362614b8aecdef185b5fb8ed588f0b152c7aa16"

mkdir -p "$runtime_dir/downloads"
if [[ ! -f "$archive" ]]; then
  curl --fail --location --retry 5 --retry-delay 2 --continue-at - --output "$archive" "$url"
fi
printf '%s  %s\n' "$sha512" "$archive" | sha512sum --check --status
if [[ -f "$target/.harbortrace-complete" && -x "$target/bin/hdfs" ]]; then
  printf 'Verified Hadoop 3.5.0 runtime: %s\n' "$target"
  exit 0
fi
if [[ -e "$target" ]]; then
  echo "Partial or unrelated runtime directory exists: $target" >&2
  exit 1
fi
tar -xzf "$archive" -C "$runtime_dir"
test -x "$target/bin/hdfs"
touch "$target/.harbortrace-complete"
printf 'Installed Hadoop 3.5.0 runtime: %s\n' "$target"
