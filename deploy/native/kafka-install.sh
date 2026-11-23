#!/usr/bin/env bash
set -euo pipefail

# Installs only the Kafka runtime (KRaft mode, no ZooKeeper). Data stays under HARBORTRACE_DATA_DIR.
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
version="4.1.2"
name="kafka_2.13-$version"
archive="$runtime_dir/downloads/$name.tgz"
target="$runtime_dir/$name"
sha512="78ac6e488b1071122f9608dfdb363f6fe50e1dbbc492347002c0398dfbf77e0d8caa5bf794c5937379721004dfe92025937d238b0faf4eb715529417fd43b491"

mkdir -p "$runtime_dir/downloads"
if [[ ! -f "$archive" ]]; then
  # dlcdn serves current releases; archive.apache.org keeps every release.
  for url in "https://dlcdn.apache.org/kafka/$version/$name.tgz" "https://archive.apache.org/dist/kafka/$version/$name.tgz"; do
    if curl --fail --location --retry 3 --retry-delay 2 --continue-at - --output "$archive.part" "$url"; then
      mv "$archive.part" "$archive"
      break
    fi
  done
fi
test -f "$archive"
if [[ "$(sha_of 512 <"$archive")" != "$sha512" ]]; then
  echo "Kafka archive checksum differs from the pinned SHA-512: $archive" >&2
  exit 1
fi
if [[ -f "$target/.harbortrace-complete" && -x "$target/bin/kafka-server-start.sh" ]]; then
  printf 'Verified Kafka %s runtime: %s\n' "$version" "$target"
  exit 0
fi
if [[ -e "$target" ]]; then
  echo "Partial or unrelated runtime directory exists: $target" >&2
  exit 1
fi
tar -xzf "$archive" -C "$runtime_dir"
test -x "$target/bin/kafka-server-start.sh"
touch "$target/.harbortrace-complete"
printf 'Installed Kafka %s runtime: %s\n' "$version" "$target"
