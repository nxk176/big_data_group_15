"""Data and storage component: fixtures, verified HDFS readback and Kafka replay.

  fixtures       regenerate (or --check) the committed canonical and replay fixtures
  hdfs-readback  publish canonical Parquet + manifest to HDFS and verify bytes, hash and rows
  replay-files   turn prepared canonical Parquet into event-time-windowed replay JSONL files
  kafka-publish  send replay JSONL to a Kafka topic, then read it back with the diagnostic consumer
  kafka-verify   re-run the diagnostic consumer against an earlier publish report
  spark-prepare  prepare source days with Spark into day-partitioned canonical data (local or HDFS)
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
from harbortrace.hdfs import Hdfs, block_report, publish_file
from harbortrace.ingest import atomic_json, prepare, sha256_file
from harbortrace.kafka_replay import ensure_topic, kafka_clients, publish, replay_files, retained_messages, verify
from harbortrace.replay import account, replay_messages
from harbortrace.spark_core import spark_session
from harbortrace.spark_prepare import compare_with_reference, prepare_spark
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


def command_replay_files(args) -> None:
    canonical = args.canonical.resolve()
    manifest = json.loads(canonical.with_suffix(".manifest.json").read_text(encoding="utf-8"))
    if manifest["sha256"] != sha256_file(canonical):
        raise SystemExit("FAIL: canonical Parquet differs from its preparation manifest")
    output = (args.output or ROOT / "data/replay" / args.run_id).resolve()
    if output.exists():
        raise SystemExit(f"FAIL: {output} exists; replay files are immutable, choose a new --output or --run-id")
    # Holds one prepared file in memory (about 240,000 messages for the two-port day).
    rows = pq.read_table(canonical).to_pylist()
    messages = replay_messages(rows, args.run_id, args.started_at, seed=args.seed, duplicate_copies=args.duplicates)
    windows: dict[str, list[dict]] = {}
    for message in messages:
        event = datetime.fromisoformat(message["event_time"])
        start = event.replace(minute=event.minute - event.minute % args.window_minutes, second=0, microsecond=0)
        windows.setdefault(start.strftime("%Y%m%dT%H%M"), []).append(message)
    working = output.with_name(output.name + ".tmp")
    working.mkdir(parents=True)
    files = []
    for name, rows in sorted(windows.items()):
        path = working / f"replay-{name}.jsonl"
        path.write_text(jsonl(rows), encoding="utf-8")
        files.append({"path": path.name, "messages": len(rows), "sha256": sha256_file(path)})
    summary = {"contract_version": CONTRACT_VERSION, "canonical": str(canonical), "canonical_sha256": manifest["sha256"],
               "canonical_rows": manifest["row_accounting"]["canonical_rows"], "replay_run_id": args.run_id,
               "started_at": args.started_at, "seed": args.seed, "duplicate_copies": args.duplicates,
               "window_minutes": args.window_minutes, "files": files, "accounting": account(messages)}
    atomic_json(working / "replay-manifest.json", summary)
    working.rename(output)
    print_table("REPLAY FILES", ["Item", "Value"],
                [["Canonical rows", summary["canonical_rows"]], ["Messages", len(messages)],
                 ["Seeded retry copies", args.duplicates], ["Files", len(files)], ["Output", output]])


def print_kafka(publication: dict, check: dict) -> None:
    print_table("KAFKA PUBLISH", ["Partition", "First offset", "Last offset", "Messages"],
                [[p, span["first"], span["last"], span["messages"]] for p, span in publication["offsets"].items()])
    sent, received = publication["tally"], check["received"]
    print_table("SENT VERSUS RECEIVED", ["Measure", "Sent", "Received"],
                [[key, sent[key], received[key]] for key in
                 ("messages", "distinct_replay_ids", "distinct_source_records", "replay_copies",
                  "distinct_logical_observations", "vessels")]
                + [["delivery failures", publication["failed"], "-"],
                   ["foreign / invalid / wrong key", "-", f"{check['counts']['foreign']} / {check['counts']['invalid']} / "
                                                          f"{check['counts']['wrong_key']}"],
                   ["key on two partitions / out of order", "-", f"{check['counts']['key_moved']} / "
                                                                f"{check['counts']['out_of_order']}"],
                   ["replay-ID digest", publication["replay_id_digest"][:16], check["replay_id_digest"][:16]]])
    for problem in check["problems"]:
        print(f"PROBLEM: {problem}")
    print(f"RESULT: {check['result']}")


def command_kafka_publish(args) -> None:
    files = replay_files(args.input or [REPLAY])
    run = new_run("kafka")
    producer, admin, consumer = kafka_clients(args.bootstrap)
    try:
        topic = ensure_topic(admin, args.topic, args.partitions)
        existing = retained_messages(consumer, args.topic, args.partitions)
        if existing and not args.append:
            raise SystemExit(f"FAIL: topic {args.topic} already holds {existing} messages; use a new topic or --append")
        publication = {**publish(files, args.topic, producer, progress_every=args.progress_every),
                       "bootstrap": args.bootstrap, "topic_setup": topic, "messages_before": existing}
        atomic_json(run / "publish.json", publication)
        check = verify(publication, consumer, timeout=args.timeout)
    finally:
        consumer.close()
    atomic_json(run / "verify.json", check)
    print(f"\nHARBORTRACE - KAFKA REPLAY ({len(files)} files -> {args.topic} at {args.bootstrap})")
    print(f"Sent {publication['sent']:,} messages in {publication['seconds']} s "
          f"({publication['messages_per_second']:,} messages/s)")
    print_kafka(publication, check)
    print(f"Reports: {run / 'publish.json'}, {run / 'verify.json'}")
    if check["result"] != "PASS":
        raise SystemExit(1)


def command_kafka_verify(args) -> None:
    publication = json.loads(args.report.read_text(encoding="utf-8"))
    _, _, consumer = kafka_clients(args.bootstrap or publication["bootstrap"])
    try:
        check = verify(publication, consumer, timeout=args.timeout)
    finally:
        consumer.close()
    atomic_json(args.report.with_name(f"verify-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"), check)
    print_kafka(publication, check)
    if check["result"] != "PASS":
        raise SystemExit(1)


def wait_for_replication(hdfs: Hdfs, path: str, timeout: float = 120) -> dict:
    """The NameNode copies blocks after a replication change in the background; wait for it."""
    import time
    deadline = time.monotonic() + timeout
    while True:
        report = block_report(hdfs, path)
        if not report["under_replicated"] or time.monotonic() > deadline:
            return report
        time.sleep(3)


def command_spark_prepare(args) -> None:
    from harbortrace.engine import engine_session
    run = new_run("spark-prepare")
    fixture = not args.source
    if fixture:
        spec = json.loads(FIXTURE.read_text(encoding="utf-8"))
        sources, boundaries = write_fixture(run, spec)
        sources, ports = [sources], ["9001"]
        reference = prepare(configuration(run, ports), source=sources[0], boundaries=boundaries)
    else:
        sources, boundaries = args.source, args.boundaries
        ports = None if args.ports == ["all"] else args.ports
        reference = json.loads(args.compare.read_text(encoding="utf-8")) if args.compare else None
    output = args.output or str(run / "spark-canonical")
    if output.startswith("hdfs://") and args.replication is None:
        args.replication = int(Hdfs.from_environment().configured("dfs.replication"))
    spark = engine_session("harbortrace-spark-prepare", master=args.master, driver_memory=args.driver_memory,
                           shuffle_partitions=args.partitions)
    try:
        manifest = prepare_spark(spark, sources, boundaries, output, ports=ports,
                                 replication=args.replication, kind="synthetic" if fixture else "real")
        readback = spark.read.parquet(output).count()
        comparison = compare_with_reference(spark, output, reference) if reference else None
    finally:
        spark.stop()
    blocks = wait_for_replication(Hdfs.from_environment(), output) if output.startswith("hdfs://") else None
    report = {"manifest": manifest, "spark_readback_rows": readback, "comparison": comparison, "blocks": blocks}
    atomic_json(run / "spark-prepare.json", report)
    accounting = manifest["row_accounting"]
    print("\nHARBORTRACE - SPARK PREPARATION")
    print_table("ROW ACCOUNTING", ["Category", "Rows"],
                [[key, accounting[key]] for key in ("source_rows", "canonical_rows", "invalid", "duplicate_rows",
                                                    "conflict_rows", "outside_region", "accounted_rows")]
                + [["read back from output", readback]])
    print_table("OUTPUT", ["Item", "Value"],
                [["Ports used / rejected", f"{manifest['ports_used']} / {len(manifest['ports_rejected'])}"],
                 ["Days", ", ".join(f"{day}: {rows}" for day, rows in manifest["days"].items())],
                 ["Spark seconds", manifest["seconds"]], ["Output", output]])
    if comparison:
        print_table("COMPARED WITH THE SINGLE-PROCESS REFERENCE", ["Check", "Value"],
                    [[key, value] for key, value in comparison.items()])
    if blocks:
        print_table("HDFS BLOCKS", ["Check", "Value"], [[key, value] for key, value in blocks.items()])
    print(f"Report: {run / 'spark-prepare.json'}")
    failed = readback != accounting["canonical_rows"] or (comparison and comparison["result"] != "EQUAL")
    failed = failed or (blocks and (blocks["status"] != "HEALTHY" or blocks["under_replicated"]))
    print("RESULT:", "FAIL" if failed else "PASS")
    if failed:
        raise SystemExit(1)


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
    files = commands.add_parser("replay-files", help="Write replay JSONL files from prepared canonical Parquet")
    files.add_argument("--canonical", type=Path, required=True, help="Prepared Parquet with its .manifest.json")
    files.add_argument("--run-id", required=True)
    files.add_argument("--started-at", default="2024-01-02T00:00:00+00:00", help="UTC ingestion time of message 1")
    files.add_argument("--seed", type=int, default=REPLAY_SEED)
    files.add_argument("--duplicates", type=int, default=0, help="Seeded producer-retry copies")
    files.add_argument("--window-minutes", type=int, default=30, choices=(5, 10, 15, 30, 60))
    files.add_argument("--output", type=Path, help="New directory (default: data/replay/<run-id>)")
    files.set_defaults(handler=command_replay_files)
    kafka = commands.add_parser("kafka-publish", help="Publish replay JSONL to Kafka and verify the topic")
    kafka.add_argument("--input", type=Path, nargs="+", help="JSONL files or directories (default: committed fixture)")
    kafka.add_argument("--topic", default="harbortrace.replay")
    kafka.add_argument("--bootstrap", default="127.0.0.1:9092")
    kafka.add_argument("--partitions", type=int, default=4)
    kafka.add_argument("--append", action="store_true", help="Allow a topic that already holds messages")
    kafka.add_argument("--timeout", type=float, default=60, help="Seconds without progress before giving up")
    kafka.add_argument("--progress-every", type=int, default=0)
    kafka.set_defaults(handler=command_kafka_publish)
    check = commands.add_parser("kafka-verify", help="Read back the offsets an earlier publish report recorded")
    check.add_argument("--report", type=Path, required=True, help="publish.json from kafka-publish")
    check.add_argument("--bootstrap")
    check.add_argument("--timeout", type=float, default=60)
    check.set_defaults(handler=command_kafka_verify)
    spark_prepare = commands.add_parser("spark-prepare", help="Spark preparation into day-partitioned canonical data")
    spark_prepare.add_argument("--source", type=Path, nargs="+", help="AIS day files (default: synthetic fixture, "
                               "compared with the single-process preparation)")
    spark_prepare.add_argument("--boundaries", type=Path, default=ROOT / "data/boundaries/usace_ports.geojson")
    spark_prepare.add_argument("--ports", nargs="+", default=["4120", "4110"], help="Port IDs, or 'all' usable polygons")
    spark_prepare.add_argument("--output", help="New local path or hdfs:// URI (default: under data/storage/)")
    spark_prepare.add_argument("--replication", type=int, help="Replication of the written HDFS files (default: the cluster's dfs.replication)")
    spark_prepare.add_argument("--compare", type=Path, help="Manifest of an ingest.prepare run of the same source to compare with")
    spark_prepare.add_argument("--master", default="local[*]")
    spark_prepare.add_argument("--driver-memory", default="4g")
    spark_prepare.add_argument("--partitions", type=int, default=16)
    spark_prepare.set_defaults(handler=command_spark_prepare)
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
