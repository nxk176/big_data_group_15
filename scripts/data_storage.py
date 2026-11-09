"""Data and storage component: canonical/replay fixtures and verified HDFS readback.

  fixtures       regenerate (or --check) the committed canonical and replay fixtures
  hdfs-readback  publish canonical Parquet + manifest to HDFS and verify bytes, hash and rows
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from harbortrace.contracts import CONTRACT_VERSION, Observation
from harbortrace.hdfs import Hdfs, publish_file
from harbortrace.ingest import atomic_json, prepare, sha256_file
from harbortrace.replay import account, replay_messages
from harbortrace.spark_core import spark_session
from week04_checkpoint import print_table
from week05_ingestion import FIXTURE, configuration, write_fixture

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests/fixtures/canonical"
CANONICAL = FIXTURE_DIR / "week06_canonical.jsonl"
REPLAY = FIXTURE_DIR / "week06_replay.jsonl"
MANIFEST = FIXTURE_DIR / "week06_manifest.json"
REPLAY_RUN_ID = "fixture-replay-week06"
REPLAY_STARTED_AT = "2024-01-02T00:00:00+00:00"
REPLAY_SEED = 20261102


def new_run(kind: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run = ROOT / "data/storage" / f"{kind}-{stamp}"
    run.mkdir(parents=True, exist_ok=False)
    return run


def prepare_fixture(run: Path) -> dict:
    """Run the synthetic week-five source through the real preparation path."""
    spec = json.loads(FIXTURE.read_text(encoding="utf-8"))
    source, boundaries = write_fixture(run, spec)
    return prepare(configuration(run, [9001]), source=source, boundaries=boundaries)


def jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def build_fixtures() -> dict[Path, str]:
    manifest = prepare_fixture(new_run("fixtures"))
    canonical = pq.read_table(manifest["canonical"]).to_pylist()
    for row in canonical:
        Observation.from_json(row)
    messages = replay_messages(canonical, REPLAY_RUN_ID, REPLAY_STARTED_AT, seed=REPLAY_SEED, duplicate_copies=1)
    canonical_text, replay_text = jsonl(canonical), jsonl(messages)
    summary = {
        "contract_version": CONTRACT_VERSION, "kind": "synthetic",
        "description": "Canonical observations and replay messages prepared from the synthetic week-five source; "
                       "they establish contract behavior, not real-world accuracy.",
        "generated_by": "PYTHONPATH=src python scripts/data_storage.py fixtures",
        "origin": {"fixture": FIXTURE.relative_to(ROOT).as_posix(), "sha256": sha256_file(FIXTURE)},
        "canonical": {"path": CANONICAL.name, "format": "JSONL, one canonical observation per line, sorted keys",
                      "order": ["mmsi", "event_time"], "rows": len(canonical), "sha256": sha(canonical_text),
                      "row_accounting": manifest["row_accounting"]},
        "replay": {"path": REPLAY.name, "replay_run_id": REPLAY_RUN_ID, "started_at": REPLAY_STARTED_AT,
                   "seed": REPLAY_SEED, "duplicate_copies": 1, "key": "mmsi", "order": ["event_time", "mmsi"],
                   "sha256": sha(replay_text), "accounting": account(messages)},
    }
    return {CANONICAL: canonical_text, REPLAY: replay_text,
            MANIFEST: json.dumps(summary, indent=2, sort_keys=True) + "\n"}


def command_fixtures(args) -> None:
    built = build_fixtures()
    if args.check:
        stale = [path.name for path, text in built.items()
                 if not path.is_file() or path.read_text(encoding="utf-8") != text]
        if stale:
            raise SystemExit(f"FAIL: committed fixtures differ from a fresh preparation: {stale}")
        print(f"PASS: {len(built)} committed fixtures match a fresh, deterministic preparation.")
        return
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for path, text in built.items():
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")


def command_hdfs_readback(args) -> None:
    run = new_run("hdfs")
    if args.canonical:
        canonical = args.canonical.resolve()
        manifest_path = canonical.with_suffix(".manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["sha256"] != sha256_file(canonical):
            raise SystemExit("FAIL: canonical Parquet differs from its preparation manifest")
    else:
        manifest = prepare_fixture(run)
        canonical = Path(manifest["canonical"])
        manifest_path = canonical.with_suffix(".manifest.json")
    hdfs = Hdfs.from_environment(args.uri)
    # Content-addressed: identical Parquet reuses one dataset; each preparation's
    # manifest (run times, local paths) is kept beside it as a separate provenance record.
    dataset = f"{args.remote_root.rstrip('/')}/{canonical.stem}-{manifest['sha256'][:16]}"
    uploads = [publish_file(hdfs, canonical, f"{dataset}/{canonical.name}"),
               publish_file(hdfs, manifest_path, f"{dataset}/manifests/{sha256_file(manifest_path)[:16]}.json")]
    expected_rows = manifest["row_accounting"]["canonical_rows"]
    config = configuration(run, [9001])
    spark = spark_session(config, "harbortrace-hdfs-readback")
    try:
        local_frame = spark.read.parquet(str(canonical))
        remote_frame = spark.read.parquet(uploads[0]["remote"])
        remote_rows = remote_frame.count()
        missing = local_frame.select("logical_id", "source_record_id").exceptAll(
            remote_frame.select("logical_id", "source_record_id")).count()
        spark_version = spark.version
    finally:
        spark.stop()
    if remote_rows != expected_rows or missing:
        raise SystemExit(f"FAIL: HDFS rows {remote_rows} vs manifest {expected_rows}; {missing} local rows absent remotely")
    report = {"result": "PASS", "checked_at_utc": datetime.now(timezone.utc).isoformat(),
              "mode": "real-canonical" if args.canonical else "synthetic-fixture", "hdfs_uri": hdfs.uri,
              "dataset": hdfs.url(dataset), "uploads": uploads, "manifest_canonical_rows": expected_rows,
              "spark_hdfs_rows": remote_rows, "rows_missing_remotely": missing, "spark_version": spark_version}
    atomic_json(run / "hdfs-readback.json", report)
    print("\nHARBORTRACE - CANONICAL HDFS READBACK")
    print_table("UPLOADS", ["File", "Bytes", "SHA-256 match", "Replication", "Reused"],
                [[Path(item["local"]).name, item["bytes"], item["sha256"] == item["remote_sha256"],
                  item["replication"], item["reused"]] for item in uploads])
    print_table("ROW CHECK", ["Check", "Value"],
                [["Manifest canonical rows", expected_rows], ["Spark rows read from HDFS", remote_rows],
                 ["Local rows absent in HDFS", missing], ["HDFS dataset", hdfs.url(dataset)]])
    print(f"Report: {run / 'hdfs-readback.json'}")
    print("RESULT: PASS - bytes, SHA-256 and Spark row counts agree between local and HDFS copies.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    fixtures = commands.add_parser("fixtures", help="Regenerate canonical and replay fixtures")
    fixtures.add_argument("--check", action="store_true", help="Fail if committed fixtures differ; write nothing")
    fixtures.set_defaults(handler=command_fixtures)
    readback = commands.add_parser("hdfs-readback", help="Publish canonical Parquet to HDFS and verify it")
    readback.add_argument("--uri", help="HDFS URI (default: $HDFS_URI or hdfs://127.0.0.1:9000)")
    readback.add_argument("--remote-root", default="/harbortrace/canonical")
    readback.add_argument("--canonical", type=Path,
                          help="Existing prepared canonical Parquet with its .manifest.json (default: synthetic fixture)")
    readback.set_defaults(handler=command_hdfs_readback)
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
