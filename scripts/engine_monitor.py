"""Watch a streaming engine output directory: progress, threshold alerts and the run's logs.

    PYTHONPATH=src python scripts/engine_monitor.py data/stream/<run>                 # one report
    PYTHONPATH=src python scripts/engine_monitor.py data/stream/<run> --follow       # every 10 s
        [--logs data/e2e/<run>/logs] [--kafka-bootstrap 127.0.0.1:9092] [--threshold batch_seconds=60]

Reads only files the query writes, so it can run beside the query, after it, or after a crash.
Every newly committed batch and every new alert is appended as one JSON line to --events
(default <output>/../<name>-monitor.jsonl), which gathers the run's history in one file.
Exit status: 2 when a critical alert is active (single report), else 0.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from harbortrace.monitoring import (DEFAULT_THRESHOLDS, alerts, batch_progress, kafka_lag,  # noqa: E402
                                    latest_kafka_position, log_summary, summary)


def high_offsets(bootstrap: str, planned: dict) -> dict:
    from harbortrace.kafka_replay import kafka_clients, watermarks
    _, _, consumer = kafka_clients(bootstrap)
    try:
        return {topic: {p: watermarks(consumer, topic, p, timeout=10)[1] for p in parts} for topic, parts in planned.items()}
    finally:
        consumer.close()


def threshold(text: str) -> tuple[str, float]:
    name, _, value = text.partition("=")
    if name not in DEFAULT_THRESHOLDS or not value:
        raise argparse.ArgumentTypeError(f"use NAME=VALUE with NAME one of {', '.join(DEFAULT_THRESHOLDS)}")
    return name, float(value)


def snapshot(args) -> dict:
    progress = batch_progress(args.output)
    lag = None
    if args.kafka_bootstrap:
        planned = latest_kafka_position(args.output)
        lag = kafka_lag(planned, high_offsets(args.kafka_bootstrap, planned)) if planned else None
    found = alerts(progress, dict(args.threshold), now=time.time(), lag=lag)
    return {"checked_at": datetime.now(timezone.utc).isoformat(), "summary": summary(progress), "kafka_lag": lag,
            "alerts": found, "logs": log_summary(args.logs) if args.logs else None, "progress": progress}


def print_report(state: dict, last: int) -> None:
    s = state["summary"]
    print(f"\n[{state['checked_at'][:19]}Z] batches committed {s['batches_committed']}/{s['batches_planned']}"
          + (f", running {s['running_batch']}" if s["running_batch"] is not None else "")
          + f"; watermark {s['watermark']}")
    t = s["totals"]
    print(f"accepted {t['accepted']:,}  duplicates {t['duplicates']:,}  late {t['late']:,}  invalid {t['invalid']:,}"
          f"  updates {t['updates']:,}  peak buffered {s['peak_buffered']:,}"
          + (f"  batch s mean {s['batch_seconds']['mean']} max {s['batch_seconds']['max']}" if s["batch_seconds"] else ""))
    if state["kafka_lag"]:
        print(f"Kafka lag: {state['kafka_lag']['total']:,} messages")
    print(f"{'batch':>6} {'seconds':>8} {'accepted':>9} {'msg/s':>9} {'late':>5} {'buffered':>9} {'updates':>8}  watermark")
    for r in state["progress"][-last:]:
        print(f"{r['batch_id']:>6} {r.get('seconds', '-'):>8} {r.get('accepted', '-'):>9} {r.get('messages_per_second', '-'):>9}"
              f" {r.get('late', '-'):>5} {r.get('buffered', '-'):>9} {r.get('updates', '-'):>8}  "
              f"{r['watermark'] or '-'}{'' if r['committed'] else '  (running)'}")
    for alert in state["alerts"]:
        print(f"ALERT {alert['level'].upper()} {alert['rule']}: {alert['message']}")
    if not state["alerts"]:
        print("No alerts.")
    for name, entry in (state["logs"] or {}).items():
        print(f"log {name}: {entry['counts']}" + (f"; last error: {entry['last_errors'][-1]}" if entry["last_errors"] else ""))


def append_events(path: Path, state: dict, seen_batches: set, seen_alerts: set) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for r in state["progress"]:
            if r["committed"] and r["batch_id"] not in seen_batches:
                seen_batches.add(r["batch_id"])
                handle.write(json.dumps({"event": "batch", "logged_at": state["checked_at"], **r}) + "\n")
        for alert in state["alerts"]:
            key = (alert["rule"], alert["batch_id"])
            if key not in seen_alerts:
                seen_alerts.add(key)
                handle.write(json.dumps({"event": "alert", "logged_at": state["checked_at"], **alert}) + "\n")


def seen_from(path: Path) -> tuple[set, set]:
    batches, found = set(), set()
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if row["event"] == "batch":
                batches.add(row["batch_id"])
            else:
                found.add((row["rule"], row["batch_id"]))
    return batches, found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("output", type=Path, help="Streaming engine output directory (holds checkpoint/ and metrics/)")
    parser.add_argument("--follow", action="store_true", help="Report again every --interval seconds until Ctrl-C")
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--last", type=int, default=10, help="Batches shown in the table")
    parser.add_argument("--logs", type=Path, help="Directory of *.log files to scan for warnings and errors")
    parser.add_argument("--kafka-bootstrap", help="Also report how many topic messages are not yet read")
    parser.add_argument("--threshold", type=threshold, action="append", default=[], metavar="NAME=VALUE")
    parser.add_argument("--events", type=Path, help="JSON lines event log (default <output>/../<name>-monitor.jsonl)")
    parser.add_argument("--json", action="store_true", help="Print one JSON report instead of the table")
    args = parser.parse_args()
    if not (args.output / "checkpoint").is_dir():
        parser.error(f"{args.output} has no checkpoint/ directory")
    events = args.events or args.output.parent / f"{args.output.name}-monitor.jsonl"
    seen_batches, seen_alerts = seen_from(events)
    while True:
        state = snapshot(args)
        append_events(events, state, seen_batches, seen_alerts)
        if args.json:
            print(json.dumps({k: v for k, v in state.items() if k != "progress"} | {"progress": state["progress"][-args.last:]},
                             indent=2))
        else:
            print_report(state, args.last)
        if not args.follow:
            return 2 if any(a["level"] == "critical" for a in state["alerts"]) else 0
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    sys.exit(main())
