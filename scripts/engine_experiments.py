"""Engine experiments on replayed data: late arrivals and a killed streaming job.

    # canonical Parquet -> ordered JSONL replay files (uses replay.replay_messages; loads the day in memory).
    # "replay-spark" does the same with Spark for inputs that do not fit in memory (no duplicate copies).
    PYTHONPATH=src python scripts/engine_experiments.py replay --canonical data/canonical/ais-2024-01-01-regional.parquet \
        --run-id pilot-2024-01-01 --out data/replay/pilot-2024-01-01
    # delay a share of the messages and measure what each watermark loses and batch restores
    PYTHONPATH=src python scripts/engine_experiments.py late --replay data/replay/pilot-2024-01-01 \
        --artifact data/engine/pilot-2024-01-01 --ports 4120 4110 --out data/experiments/late
    # kill the streaming job mid-run, restart it, compare with an uninterrupted run
    PYTHONPATH=src python scripts/engine_experiments.py crash --replay data/replay/pilot-2024-01-01 \
        --ports 4120 4110 --out data/experiments/crash
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from harbortrace.streaming import read_metrics, read_updates

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def write_ordered(directory: Path, messages: list[dict], arrival, minutes: int = 30) -> int:
    """Write messages into files of `minutes` of arrival time each; file times fix the read order."""
    directory.mkdir(parents=True, exist_ok=False)
    buckets: dict[str, list[dict]] = {}
    for message in sorted(messages, key=arrival):
        when = arrival(message)
        buckets.setdefault(f"{when:%Y%m%dT%H}{when.minute // minutes:02d}", []).append(message)
    for index, (_, rows) in enumerate(sorted(buckets.items())):
        path = directory / f"part-{index:04d}.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        os.utime(path, (1_700_000_000 + index,) * 2)
    return len(buckets)


def read_replay(directory: Path) -> list[dict]:
    return [json.loads(line) for path in sorted(directory.glob("part-*.jsonl")) for line in path.read_text().splitlines()]


def event_time(message: dict) -> datetime:
    return datetime.fromisoformat(message["event_time"])


def command_replay(args) -> None:
    import pyarrow.parquet as pq
    from harbortrace.replay import account, replay_messages
    rows = pq.read_table(args.canonical).to_pylist()
    messages = replay_messages(rows, args.run_id, "2026-01-01T00:00:00+00:00", seed=args.seed,
                               duplicate_copies=args.duplicates, step_ms=1)
    files = write_ordered(args.out, messages, event_time)
    print(json.dumps({"files": files, **account(messages)}, indent=2))


def _replay_lines(rows, run_id: str, minutes: int):
    """Canonical rows -> (arrival bucket, replay message JSON), following the replay contract."""
    from harbortrace.contracts import CONTRACT_VERSION
    from harbortrace.replay import CANONICAL_FIELDS, replay_id
    for row in rows:
        data = row.asDict(recursive=True)
        message = {key: data[key] for key in CANONICAL_FIELDS}
        message.update({"contract_version": CONTRACT_VERSION, "replay_run_id": run_id, "copy_index": 0,
                        "replay_id": replay_id(run_id, data["source_record_id"], 0),
                        "ingested_at": data["event_time"]})
        when = datetime.fromisoformat(data["event_time"])
        yield f"{when:%Y%m%dT%H}{when.minute // minutes:02d}", json.dumps(message)


def command_replay_spark(args) -> None:
    """Replay files for inputs too large for memory: Spark writes one file per arrival bucket."""
    from harbortrace.engine import engine_session, read_observations
    spark = engine_session("harbortrace-replay-files", master=args.master, driver_memory="3g",
                           shuffle_partitions=args.partitions)
    try:
        run_id, minutes = args.run_id, args.bucket_minutes
        lines = read_observations(spark, [args.canonical]).rdd.mapPartitions(
            lambda rows: _replay_lines(rows, run_id, minutes))
        frame = spark.createDataFrame(lines, "bucket string, value string")
        frame.repartition("bucket").sortWithinPartitions("bucket").write.partitionBy("bucket").text(str(args.out))
    finally:
        spark.stop()
    files = sorted(args.out.glob("bucket=*/part-*"))
    for index, path in enumerate(files):  # the file source reads in modification-time order
        os.utime(path, (1_700_000_000 + index,) * 2)
    print(json.dumps({"files": len(files), "bytes": sum(path.stat().st_size for path in files)}))


def stream_command(source: Path, output: Path, ports: list[str], lateness: int) -> list[str]:
    return [sys.executable, str(ROOT / "scripts/engine_stream.py"), "--source", str(source), "--output", str(output),
            "--stream-id", "experiment", "--ports", *ports, "--lateness-minutes", str(lateness),
            "--max-per-trigger", "1"]


def run_stream(source: Path, output: Path, ports: list[str], lateness: int, artifact: Path | None = None) -> dict:
    command = stream_command(source, output, ports, lateness)
    if artifact:
        command += ["--compare-artifact", str(artifact)]
    subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads((output / "report.json").read_text())


def against_released_prefix(output: Path, messages: list[dict], ports: list[str], watermark: datetime) -> dict:
    """Compare the provisional view with the batch reconstruction of every observation up to the watermark.

    Without lost observations the two are equal except for the end flag and gap of a visit that
    was open when a vessel's state expired. Anything else is damage caused by dropped arrivals.
    """
    from engine_stream import END_FIELDS, provisional_view
    from harbortrace.contracts import Observation
    from harbortrace.stream_state import visit_json
    from harbortrace.visits import Rules, reconstruct
    released = [Observation.from_json(row) for row in messages if event_time(row) <= watermark]
    expected = {visit.visit_id: visit_json(visit) for visit in reconstruct(released, ports, Rules())}
    view, result = provisional_view(output), {"identical": 0, "end_flag_or_gap_only": 0, "different": 0}
    for key, visit in expected.items():
        if key not in view:
            continue
        fields = {name for name in visit if visit[name] != view[key][name]}
        result["identical" if not fields else "end_flag_or_gap_only" if fields <= END_FIELDS else "different"] += 1
    result["missing"] = len(set(expected) - set(view))
    result["spurious"] = len(set(view) - set(expected))
    result["expected_visits"] = len(expected)
    return result


def command_late(args) -> None:
    """Delay a seeded share of messages, then run the stream once per watermark setting."""
    messages = read_replay(args.replay)
    rng = random.Random(args.seed)
    delayed = set(rng.sample(range(len(messages)), int(len(messages) * args.delay_fraction)))
    delay = timedelta(minutes=args.delay_minutes)
    arrivals = {id(message): event_time(message) + (delay if index in delayed else timedelta(0))
                for index, message in enumerate(messages)}
    write_ordered(args.out / "input", messages, lambda message: arrivals[id(message)], args.bucket_minutes)
    rows = []
    for lateness in args.lateness:
        report = run_stream(args.out / "input", args.out / f"lateness-{lateness:03d}", args.ports, lateness, args.artifact)
        watermark = datetime.fromisoformat(report["watermark"].replace("Z", "+00:00"))
        comparison = against_released_prefix(args.out / f"lateness-{lateness:03d}", messages, args.ports, watermark)
        wrong = comparison["different"] + comparison["missing"] + comparison["spurious"]
        rows.append({"lateness_minutes": lateness, "delayed_messages": len(delayed),
                     "dropped_by_watermark": report["rows_dropped_by_watermark"],
                     "dropped_behind_vessel_frontier": report["totals"]["late"],
                     "accepted": report["totals"]["accepted"], "provisional_visits": report["provisional_visits"],
                     "visits_expected_at_watermark": comparison["expected_visits"],
                     "visits_wrong_until_batch": wrong, "state_memory_bytes": report["state_memory_bytes"],
                     "peak_buffered": report["peak_buffered"], "microbatches": report["batches"],
                     "comparison_with_released_prefix": comparison,
                     "comparison_with_full_day_artifact": report["comparison_with_batch"]})
        print(json.dumps(rows[-1]))
    result = {"replay": str(args.replay), "messages": len(messages), "delay_fraction": args.delay_fraction,
              "delay_minutes": args.delay_minutes, "bucket_minutes": args.bucket_minutes, "seed": args.seed,
              "runs": rows}
    (args.out / "late.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("\nLATE-ARRIVAL EXPERIMENT")
    print(f"{len(delayed)} of {len(messages)} messages arrive {args.delay_minutes} minutes after their event time; "
          f"one microbatch holds {args.bucket_minutes} minutes of arrivals")
    print("lateness | dropped | visits expected at watermark | wrong until batch corrects | peak buffered")
    for row in rows:
        dropped = row["dropped_by_watermark"] + row["dropped_behind_vessel_frontier"]
        print(f"{row['lateness_minutes']:>5} min | {dropped:>7} | {row['visits_expected_at_watermark']:>28} | "
              f"{row['visits_wrong_until_batch']:>26} | {row['peak_buffered']:>13}")


def canonical_updates(output: Path) -> list[str]:
    """Updates without the wall-clock field, as comparable strings."""
    return sorted(json.dumps({key: value for key, value in update.items() if key != "emitted_at"}, sort_keys=True)
                  for update in read_updates(output))


def command_crash(args) -> None:
    """SIGKILL the job (driver, JVM and workers) mid-run, restart on the same checkpoint."""
    args.out.mkdir(parents=True, exist_ok=False)
    environment = dict(os.environ)
    run_stream(args.replay, args.out / "uninterrupted", args.ports, args.lateness)
    crashed = args.out / "killed-and-restarted"
    process = subprocess.Popen(stream_command(args.replay, crashed, args.ports, args.lateness), env=environment,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    deadline = time.time() + 600
    while len(list((crashed / "updates").glob("batch=*"))) < args.kill_after_batches:
        if process.poll() is not None or time.time() > deadline:
            raise RuntimeError("The job ended before the kill point; lower --kill-after-batches")
        time.sleep(0.2)
    os.killpg(process.pid, signal.SIGKILL)
    process.wait()
    batches_at_kill = len(list((crashed / "updates").glob("batch=*")))
    updates_at_kill = len(read_updates(crashed))
    restarted = run_stream(args.replay, crashed, args.ports, args.lateness)
    reference, recovered = canonical_updates(args.out / "uninterrupted"), canonical_updates(crashed)
    identities = [(update["cohort_id"], update["seq"]) for update in read_updates(crashed)]
    accepted = sum(row["accepted"] for row in read_metrics(crashed))
    expected = sum(row["accepted"] for row in read_metrics(args.out / "uninterrupted"))
    result = {"batches_written_at_kill": batches_at_kill, "updates_written_at_kill": updates_at_kill,
              "batches_after_restart": restarted["batches"], "updates_uninterrupted": len(reference),
              "updates_after_restart": len(recovered), "updates_equal": reference == recovered,
              "repeated_update_identities": len(identities) - len(set(identities)),
              "accepted_messages": accepted, "accepted_messages_uninterrupted": expected}
    (args.out / "crash.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    ok = result["updates_equal"] and not result["repeated_update_identities"] and accepted == expected
    print("RESULT: PASS - the restarted job produced exactly the updates of an uninterrupted run." if ok
          else "RESULT: FAIL - the restarted job differs from an uninterrupted run.")
    if not ok:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay")
    replay.add_argument("--canonical", required=True)
    replay.add_argument("--run-id", required=True)
    replay.add_argument("--out", required=True, type=Path)
    replay.add_argument("--duplicates", type=int, default=500)
    replay.add_argument("--seed", type=int, default=20261112)
    replay.set_defaults(handler=command_replay)
    replay_spark = commands.add_parser("replay-spark")
    replay_spark.add_argument("--canonical", required=True)
    replay_spark.add_argument("--run-id", required=True)
    replay_spark.add_argument("--out", required=True, type=Path)
    replay_spark.add_argument("--bucket-minutes", type=int, default=30, choices=[5, 10, 15, 30])
    replay_spark.add_argument("--master", default="local[4]")
    replay_spark.add_argument("--partitions", type=int, default=8)
    replay_spark.set_defaults(handler=command_replay_spark)
    late = commands.add_parser("late")
    late.add_argument("--replay", required=True, type=Path)
    late.add_argument("--artifact", required=True, type=Path)
    late.add_argument("--ports", nargs="+", required=True)
    late.add_argument("--out", required=True, type=Path)
    late.add_argument("--delay-fraction", type=float, default=0.02)
    late.add_argument("--delay-minutes", type=int, default=60)
    late.add_argument("--bucket-minutes", type=int, default=30, choices=[5, 10, 15, 30],
                      help="Arrival time covered by one input file, hence one microbatch")
    late.add_argument("--lateness", type=int, nargs="+", default=[5, 10, 30, 60])
    late.add_argument("--seed", type=int, default=20261112)
    late.set_defaults(handler=command_late)
    crash = commands.add_parser("crash")
    crash.add_argument("--replay", required=True, type=Path)
    crash.add_argument("--ports", nargs="+", required=True)
    crash.add_argument("--out", required=True, type=Path)
    crash.add_argument("--lateness", type=int, default=10)
    crash.add_argument("--kill-after-batches", type=int, default=20)
    crash.set_defaults(handler=command_crash)
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
