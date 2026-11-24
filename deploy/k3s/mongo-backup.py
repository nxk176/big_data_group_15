"""Dump the quiet HarborTrace database, or verify a restore in a new database on the selected k3s cluster."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid


POD = ["kubectl", "-n", "harbortrace", "exec", "-i", "mongodb-0", "-c", "mongodb", "--"]
SNAPSHOT = Path(__file__).with_name("mongo-snapshot.js")
ARCHIVE = "mongodb.archive.gz"

# MongoDB Database Tools accept sensitive options in a configuration file. The only
# credential passed to a child process here is in its environment, inside the MongoDB pod.
TOOL = """
set -eu
umask 077
config=$(mktemp)
trap 'rm -f "$config"' EXIT
if [ "$1" = dump ]; then
    export HARBORTRACE_TOOL_PASSWORD="$HARBORTRACE_READER_PASSWORD"
else
    export HARBORTRACE_TOOL_PASSWORD="$MONGO_INITDB_ROOT_PASSWORD"
fi
mongosh --nodb --quiet --norc --eval 'print(JSON.stringify({password: process.env.HARBORTRACE_TOOL_PASSWORD}))' </dev/null > "$config"
if [ "$1" = dump ]; then
    mongodump --config "$config" --archive --gzip --username "$HARBORTRACE_READER_USER" \
        --authenticationDatabase "$HARBORTRACE_DATABASE" --db "$HARBORTRACE_DATABASE"
else
    mongorestore --config "$config" --archive --gzip --stopOnError --username "$MONGO_INITDB_ROOT_USERNAME" \
        --authenticationDatabase admin --nsInclude "$2.*" --nsFrom "$2.*" --nsTo "$3.*"
fi
"""


def snapshot(target=None):
    command = POD.copy()
    if target:
        command += ["env", f"HARBORTRACE_RESTORE_TARGET={target}"]
    command += ["mongosh", "--nodb", "--quiet", "--norc", "--file", "/dev/stdin"]
    output = subprocess.run(command, input=SNAPSHOT.read_bytes(), stdout=subprocess.PIPE, check=True)
    return json.loads(output.stdout)


def archive_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def dump(directory):
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        raise SystemExit(f"Choose a new backup directory. {directory} already exists.") from None
    before = snapshot()
    if not before["collections"]:
        raise SystemExit("Source has no application collections to back up.")
    archive = directory / ARCHIVE
    with archive.open("xb") as output:
        subprocess.run(POD + ["sh", "-c", TOOL, "backup", "dump"], stdin=subprocess.DEVNULL, stdout=output, check=True)
    if snapshot() != before:
        raise SystemExit("Source changed during the dump. Pause publishers and repeat in a new directory.")
    manifest = {"archive_sha256": archive_hash(archive), "snapshot": before}
    pending = directory / "manifest.partial"
    with pending.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write("\n")
    pending.replace(directory / "manifest.json")
    return {"backup": str(directory), "archive_bytes": archive.stat().st_size, **before}


def restore(directory):
    with (directory / "manifest.json").open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    expected = manifest["snapshot"]
    source = expected["database"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,63}", source) or source in {"admin", "config", "local"}:
        raise SystemExit("Invalid application database name in the manifest.")
    archive = directory / ARCHIVE
    if archive_hash(archive) != manifest["archive_sha256"]:
        raise SystemExit("Archive SHA-256 differs from the manifest. Restore stopped before writing.")
    target = "harbortrace_restore_" + uuid.uuid4().hex
    empty = snapshot(target)
    if target == source or empty["collections"]:
        raise SystemExit("Restore target is already in use.")
    print(f"Restoring {source} into empty database {target}", file=sys.stderr, flush=True)
    with archive.open("rb") as stream:
        subprocess.run(POD + ["sh", "-c", TOOL, "backup", "restore", source, target], stdin=stream, check=True)
    restored = snapshot(target)
    if restored != {**expected, "database": target}:
        raise SystemExit(f"Restored counts, content, indexes, collection options or active generation differ in {target}.")
    return {"source": source, "collections_before_restore": 0, "verified": True, **restored}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["dump", "restore"])
    parser.add_argument("directory", type=Path, help="Backup directory on this machine")
    args = parser.parse_args()
    os.umask(0o077)
    report = dump(args.directory) if args.action == "dump" else restore(args.directory)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
