"""Run the streaming engine: replay messages -> provisional updates for serving.

    PYTHONPATH=src python scripts/engine_stream.py --fixture
    PYTHONPATH=src python scripts/engine_stream.py --source data/replay/pilot --output data/stream/pilot \
        --stream-id pilot --ports 4120 4110 --compare-artifact data/engine/pilot-2024-01-01
    PYTHONPATH=src python scripts/engine_stream.py --source kafka:harbortrace.replay \
        --kafka-bootstrap 127.0.0.1:9092 --output data/stream/live --stream-id live --ports 4120 4110 --continuous

A file source is a directory of JSONL replay messages. Rerunning with the same --output
continues from its checkpoint.
"""
from __future__ import annotations

import argparse
import json
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from harbortrace.contracts import Observation
from harbortrace.engine import engine_session
from harbortrace.stream_state import visit_json
from harbortrace.streaming import KAFKA_PACKAGE, read_metrics, read_updates, start_stream, stream_report
from harbortrace.visits import Rules, reconstruct

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/canonical/week06_replay.jsonl"
END_FIELDS = {"quality_flags", "maximum_gap_seconds"}


def provisional_view(output: Path) -> dict[str, dict]:
    view: dict[str, dict] = {}
    for update in read_updates(output):
        view.update({visit["visit_id"]: visit for visit in update["visits"]})
    return view


def compare(view: dict[str, dict], batch: dict[str, dict], watermark: datetime) -> dict:
    """Classify every difference between the provisional view and a batch result."""
    result = Counter()
    for key, visit in batch.items():
        if key not in view:
            started_late = datetime.fromisoformat(visit["first_inside"]) > watermark
            result["batch_only_after_watermark" if started_late else "batch_only_UNEXPLAINED"] += 1
            continue
        fields = {name for name in visit if visit[name] != view[key][name]}
        exit_by = datetime.fromisoformat(visit["possible_exit_by"]) if visit["possible_exit_by"] else None
        if not fields:
            result["identical"] += 1
        elif visit["right_censored"] and fields <= END_FIELDS:
            result["end_flag_or_gap_only"] += 1  # open when the vessel's stream state expired
        elif datetime.fromisoformat(visit["last_inside"]) > watermark:
            result["batch_has_observations_after_watermark"] += 1
        elif exit_by and view[key]["right_censored"] and view[key]["last_inside"] == visit["last_inside"]:
            # Same inside observations; the outside observation confirming the exit is not released yet.
            result["exit_confirmation_not_released"] += 1
        else:
            result["differs_UNEXPLAINED"] += 1
    result["stream_only_UNEXPLAINED"] = len(set(view) - set(batch))
    return dict(sorted(result.items()))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fixture", action="store_true", help="Run and check the committed synthetic replay fixture")
    parser.add_argument("--source", help="Directory of JSONL replay messages, or kafka:<topic>")
    parser.add_argument("--kafka-bootstrap")
    parser.add_argument("--output", type=Path, help="Output directory (updates, metrics, checkpoint)")
    parser.add_argument("--stream-id")
    parser.add_argument("--ports", nargs="+", help="Port IDs, or 'all' for every assigned port")
    parser.add_argument("--lateness-minutes", type=int, default=10, help="Event-time watermark delay")
    parser.add_argument("--max-buffer", type=int, default=5000, help="Buffered observations per vessel before early release")
    parser.add_argument("--max-per-trigger", type=int, help="Files (or Kafka offsets) per microbatch")
    parser.add_argument("--continuous", action="store_true", help="Keep running instead of stopping when input is drained")
    parser.add_argument("--compare-artifact", type=Path, help="Batch artifact to compare the provisional view with")
    parser.add_argument("--master", default="local[2]", help="Spark master, or 'submit' to defer to spark-submit")
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument("--driver-memory", default="2g")
    parser.add_argument("--spark-ui", action="store_true")
    args = parser.parse_args()
    if args.fixture:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        output = ROOT / "data/stream" / f"fixture-{stamp}"
        source = output / "input"
        source.mkdir(parents=True)
        shutil.copy(FIXTURE, source / FIXTURE.name)
        source, stream_id, ports = str(source), "fixture-week06", ["9001"]
    else:
        if not (args.source and args.output and args.stream_id and args.ports):
            parser.error("--source, --output, --stream-id and --ports are required")
        source, output, stream_id = args.source, args.output, args.stream_id
        ports = None if args.ports == ["all"] else args.ports
    spark = engine_session("harbortrace-engine-stream", master=None if args.master == "submit" else args.master,
                           driver_memory=args.driver_memory, shuffle_partitions=args.partitions, ui=args.spark_ui,
                           packages=KAFKA_PACKAGE if source.startswith("kafka:") else None)
    try:
        query = start_stream(spark, source, output, ports=ports, stream_id=stream_id,
                             lateness_minutes=args.lateness_minutes, max_buffer=args.max_buffer,
                             kafka_bootstrap=args.kafka_bootstrap, max_per_trigger=args.max_per_trigger,
                             available_now=not args.continuous)
        query.awaitTermination()
        if query.exception():
            raise RuntimeError(str(query.exception()))
        report = stream_report(query)
    finally:
        spark.stop()
    metrics = read_metrics(output)
    totals = {name: sum(row[name] for row in metrics)
              for name in ("accepted", "duplicates", "late", "forced", "invalid", "updates", "expired")}
    view = provisional_view(output)
    report.update(totals=totals, provisional_visits=len(view), stream_id=stream_id, source=source,
                  lateness_minutes=args.lateness_minutes,
                  peak_buffered=max((row["buffered"] for row in metrics), default=0))
    watermark = datetime.fromisoformat(report["watermark"].replace("Z", "+00:00")) if report.get("watermark") else None
    if args.fixture:
        messages = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
        released = [Observation.from_json(row) for row in messages
                    if datetime.fromisoformat(row["event_time"]) <= watermark]
        expected = {visit.visit_id: visit_json(visit) for visit in reconstruct(released, ports, Rules())}
        if view != expected or not view:
            raise AssertionError("Provisional view differs from the reference on the released observations")
    if args.compare_artifact and watermark:
        batch = {row["visit_id"]: row for row in
                 map(json.loads, (args.compare_artifact / "visits.jsonl").read_text().splitlines())}
        report["comparison_with_batch"] = compare(view, batch, watermark)
    (Path(output) / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("\nHARBORTRACE - STREAMING ENGINE")
    print(f"Source: {source}; microbatches: {report.get('batches')}; input rows: {report.get('input_rows')}")
    print(f"Watermark: {report.get('watermark')} (newest event minus {args.lateness_minutes} min)")
    print(f"Messages: {totals}")
    print(f"State: {report.get('state_rows')} vessels kept, {report.get('state_memory_bytes')} bytes; "
          f"peak buffered observations in one microbatch: {report['peak_buffered']}; "
          f"dropped by watermark: {report.get('rows_dropped_by_watermark')}")
    print(f"Provisional visits: {len(view)}")
    if "comparison_with_batch" in report:
        print(f"Compared with batch artifact: {report['comparison_with_batch']}")
    print(f"Output: {output}")
    if args.fixture:
        print("RESULT: PASS - the provisional view equals the reference on the released fixture observations.")


if __name__ == "__main__":
    main()
