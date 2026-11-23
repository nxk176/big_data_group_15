"""Monitoring of a streaming engine output directory, read from files only.

Everything comes from what the query already writes, so it works while the query runs, after it
finished, and after its driver died (the 14-day run lost Spark's in-memory progress that way):

- ``checkpoint/offsets/<n>``: the planned batch, its watermark and source offsets; written when the
  batch starts.
- ``checkpoint/commits/<n>``: written when the batch has finished; the gap between the two file
  times is the batch duration.
- ``metrics/batch=<n>``: the engine's own counters for the batch (accepted, late, buffered, ...).

``alerts`` compares the progress with thresholds; ``log_summary`` gathers warnings and errors from
the run's log files into one place.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from .streaming import METRICS

DEFAULT_THRESHOLDS = {
    "batch_seconds": 120.0,        # one microbatch slower than this
    "stall_seconds": 300.0,        # no batch committed for this long while one is planned
    "late_ratio": 0.01,            # late messages over accepted, per batch
    "invalid_ratio": 0.001,        # invalid messages over accepted, per batch
    "buffered": 500_000,           # observations held in state after one batch
    "watermark_stuck_batches": 5,  # consecutive batches with input but no watermark progress
    "kafka_lag": 1_000_000,        # messages in the topic not yet planned into a batch
}
LOG_LEVEL = re.compile(r"\b(ERROR|WARN(?:ING)?|FATAL|Traceback)\b")


def _iso(ms: int | None) -> str | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _numbered(directory: Path) -> dict[int, Path]:
    return {int(p.name): p for p in directory.glob("[0-9]*") if p.name.isdigit()} if directory.is_dir() else {}


def _offset_entry(path: Path) -> tuple[dict, list]:
    lines = path.read_text(encoding="utf-8").splitlines()
    meta = json.loads(lines[1])
    sources = [json.loads(line) if line not in ("", "-") else None for line in lines[2:]]
    return meta, sources


def source_position(sources: list) -> dict:
    """Kafka: messages per partition summed per topic; file source: the file log offset."""
    position = {"kafka": {}, "files": None}
    for source in sources:
        if not isinstance(source, dict):
            continue
        if "logOffset" in source:
            position["files"] = source["logOffset"]
            continue
        for topic, partitions in source.items():
            if isinstance(partitions, dict):
                position["kafka"][topic] = {int(k): int(v) for k, v in partitions.items()}
    return position


def _kafka_total(position: dict) -> int | None:
    if not position["kafka"]:
        return None
    return sum(sum(parts.values()) for parts in position["kafka"].values())


def _metrics_by_batch(output: Path) -> dict[int, dict]:
    rows: dict[int, dict] = {}
    for part in sorted(Path(output, "metrics").glob("batch=*/part-*")):
        for line in part.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                rows[row["batch_id"]] = row
    return rows


def batch_progress(output: Path | str) -> list[dict]:
    """One record per planned microbatch, oldest first; the last one may still be running."""
    output = Path(output)
    offsets = _numbered(output / "checkpoint" / "offsets")
    commits = _numbered(output / "checkpoint" / "commits")
    metrics = _metrics_by_batch(output)
    records, previous_total = [], None
    for batch_id in sorted(offsets):
        meta, sources = _offset_entry(offsets[batch_id])
        position = source_position(sources)
        total = _kafka_total(position)
        started = offsets[batch_id].stat().st_mtime
        record = {
            "batch_id": batch_id,
            "started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
            "watermark": _iso(meta.get("batchWatermarkMs")),
            "watermark_ms": meta.get("batchWatermarkMs"),
            "committed": batch_id in commits,
            "kafka_offset_total": total,
            "kafka_messages": None if total is None or previous_total is None else total - previous_total,
            "file_log_offset": position["files"],
        }
        if batch_id in commits:
            finished = commits[batch_id].stat().st_mtime
            record["committed_at"] = datetime.fromtimestamp(finished, tz=timezone.utc).isoformat()
            record["seconds"] = round(max(finished - started, 0.0), 3)
        row = metrics.get(batch_id)
        if row:
            record.update({name: row.get(name, 0) for name in (*METRICS, "updates", "vessels")})
            if record.get("seconds"):
                record["messages_per_second"] = round(row.get("accepted", 0) / record["seconds"], 1)
        records.append(record)
        previous_total = total if total is not None else previous_total
    return records


def latest_kafka_position(output: Path | str) -> dict[str, dict[int, int]]:
    offsets = _numbered(Path(output) / "checkpoint" / "offsets")
    if not offsets:
        return {}
    _, sources = _offset_entry(offsets[max(offsets)])
    return source_position(sources)["kafka"]


def kafka_lag(planned: dict[str, dict[int, int]], high_offsets: dict[str, dict[int, int]]) -> dict:
    """Messages in each topic beyond what the query has planned so far."""
    lag = {}
    for topic, partitions in high_offsets.items():
        done = planned.get(topic, {})
        lag[topic] = sum(max(high - done.get(partition, 0), 0) for partition, high in partitions.items())
    return {"per_topic": lag, "total": sum(lag.values())}


def alerts(progress: list[dict], thresholds: dict | None = None, *, now: float | None = None,
           lag: dict | None = None) -> list[dict]:
    """Threshold breaches, each with a level, a rule name, the batch and a readable message."""
    limits = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    found: list[dict] = []

    def add(level, rule, message, batch_id=None, value=None):
        found.append({"level": level, "rule": rule, "batch_id": batch_id, "value": value, "message": message})

    for record in progress:
        batch_id, accepted = record["batch_id"], record.get("accepted") or 0
        if record.get("seconds", 0) > limits["batch_seconds"]:
            add("warning", "batch_seconds", f"batch {batch_id} took {record['seconds']:.0f} s "
                f"(limit {limits['batch_seconds']:.0f} s)", batch_id, record["seconds"])
        for name in ("late", "invalid"):
            count = record.get(name) or 0
            if count and accepted and count / accepted > limits[f"{name}_ratio"]:
                add("warning", f"{name}_ratio", f"batch {batch_id}: {count} {name} of {accepted} accepted",
                    batch_id, round(count / accepted, 6))
            elif count and not accepted:
                add("critical", f"{name}_ratio", f"batch {batch_id}: {count} {name}, none accepted", batch_id, count)
        if (record.get("buffered") or 0) > limits["buffered"]:
            add("warning", "buffered", f"batch {batch_id} holds {record['buffered']} observations in state",
                batch_id, record["buffered"])

    stuck, run = int(limits["watermark_stuck_batches"]), 0
    for before, record in zip(progress, progress[1:]):
        moved = (record.get("watermark_ms") or 0) > (before.get("watermark_ms") or 0)
        run = 0 if moved or not record.get("accepted") else run + 1
        if run == stuck:
            add("warning", "watermark_stuck", f"watermark has not advanced for {stuck} batches with input "
                f"(at {record['watermark']})", record["batch_id"], stuck)

    pending = [r for r in progress if not r["committed"]]
    if pending and now is not None:
        started = datetime.fromisoformat(pending[-1]["started_at"]).timestamp()
        if now - started > limits["stall_seconds"]:
            add("critical", "stall", f"batch {pending[-1]['batch_id']} started {now - started:.0f} s ago and "
                "has not committed", pending[-1]["batch_id"], round(now - started))
    if lag and lag["total"] > limits["kafka_lag"]:
        add("warning", "kafka_lag", f"{lag['total']} messages in Kafka not yet read", None, lag["total"])
    return found


def summary(progress: list[dict]) -> dict:
    done = [r for r in progress if r["committed"]]
    seconds = [r["seconds"] for r in done if "seconds" in r]
    totals = {name: sum(r.get(name) or 0 for r in done) for name in ("accepted", "duplicates", "late", "invalid", "updates")}
    return {
        "batches_planned": len(progress), "batches_committed": len(done),
        "running_batch": next((r["batch_id"] for r in progress if not r["committed"]), None),
        "watermark": done[-1]["watermark"] if done else None,
        "totals": totals,
        "batch_seconds": {"mean": round(sum(seconds) / len(seconds), 2), "max": max(seconds)} if seconds else None,
        "peak_buffered": max((r.get("buffered") or 0 for r in done), default=0),
    }


def log_summary(directory: Path | str, *, keep: int = 5) -> dict:
    """Warning and error lines of every *.log file under ``directory``, counted per file and level."""
    report = {}
    for path in sorted(Path(directory).glob("*.log")):
        counts: dict[str, int] = {}
        last: list[str] = []
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = LOG_LEVEL.search(line)
                if not match:
                    continue
                level = {"WARNING": "WARN", "Traceback": "ERROR", "FATAL": "ERROR"}.get(match.group(1), match.group(1))
                counts[level] = counts.get(level, 0) + 1
                if level == "ERROR":
                    last = (last + [line.rstrip()[:300]])[-keep:]
        if counts:
            report[path.name] = {"counts": counts, "last_errors": last}
    return report
