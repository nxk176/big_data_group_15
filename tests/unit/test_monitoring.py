"""Monitoring reads progress from the files a streaming query writes."""
import json
import os
from pathlib import Path

from harbortrace import monitoring

T0 = 1_800_000_000  # file times of the fake run (seconds)
WM0 = 1_704_067_200_000  # 2024-01-01T00:00:00Z in ms


def write_batch(output: Path, batch_id: int, *, offsets: dict, watermark_ms: int, seconds: float | None,
                metrics: dict | None = None) -> None:
    checkpoint = output / "checkpoint"
    (checkpoint / "offsets").mkdir(parents=True, exist_ok=True)
    (checkpoint / "commits").mkdir(parents=True, exist_ok=True)
    meta = {"batchWatermarkMs": watermark_ms, "batchTimestampMs": 0, "conf": {}}
    path = checkpoint / "offsets" / str(batch_id)
    path.write_text("v1\n" + json.dumps(meta) + "\n" + json.dumps({"topic": offsets}) + "\n")
    start = T0 + batch_id * 100
    os.utime(path, (start, start))
    if seconds is not None:
        commit = checkpoint / "commits" / str(batch_id)
        commit.write_text("v1\n{}\n")
        os.utime(commit, (start + seconds, start + seconds))
        row = {"batch_id": batch_id, "vessels": 1, "updates": 2, "accepted": 1000, "late": 0, "duplicates": 0,
               "forced": 0, "invalid": 0, "buffered": 10, "expired": 0, "visits": 1, **(metrics or {})}
        directory = output / "metrics" / f"batch={batch_id:08d}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "part-00000.txt").write_text(json.dumps(row) + "\n")


def test_progress_comes_from_checkpoint_and_metrics(tmp_path):
    write_batch(tmp_path, 0, offsets={"0": 600, "1": 400}, watermark_ms=WM0, seconds=4)
    write_batch(tmp_path, 1, offsets={"0": 1100, "1": 900}, watermark_ms=WM0 + 60_000, seconds=5)
    write_batch(tmp_path, 2, offsets={"0": 1500, "1": 1500}, watermark_ms=WM0 + 120_000, seconds=None)
    progress = monitoring.batch_progress(tmp_path)
    assert [r["batch_id"] for r in progress] == [0, 1, 2]
    assert [r["committed"] for r in progress] == [True, True, False]
    assert progress[0]["seconds"] == 4 and progress[0]["messages_per_second"] == 250
    assert progress[1]["kafka_messages"] == 1000 and progress[2]["kafka_messages"] == 1000
    assert progress[1]["watermark"] == "2024-01-01T00:01:00+00:00"
    s = monitoring.summary(progress)
    assert s["batches_committed"] == 2 and s["running_batch"] == 2 and s["totals"]["accepted"] == 2000
    assert monitoring.latest_kafka_position(tmp_path) == {"topic": {0: 1500, 1: 1500}}


def test_alerts_fire_only_above_their_thresholds(tmp_path):
    write_batch(tmp_path, 0, offsets={"0": 1}, watermark_ms=WM0, seconds=5)
    write_batch(tmp_path, 1, offsets={"0": 2}, watermark_ms=WM0 + 1, seconds=200, metrics={"late": 50, "buffered": 900_000})
    write_batch(tmp_path, 2, offsets={"0": 3}, watermark_ms=WM0 + 2, seconds=None)
    progress = monitoring.batch_progress(tmp_path)
    rules = {a["rule"]: a for a in monitoring.alerts(progress, now=T0 + 200 + 1000)}
    assert set(rules) == {"batch_seconds", "late_ratio", "buffered", "stall"}
    assert rules["stall"]["level"] == "critical" and rules["batch_seconds"]["batch_id"] == 1
    assert monitoring.alerts(progress, {"batch_seconds": 500, "late_ratio": 0.1, "buffered": 10**6, "stall_seconds": 10**6},
                             now=T0 + 1200) == []


def test_stuck_watermark_and_kafka_lag(tmp_path):
    for batch_id in range(7):
        write_batch(tmp_path, batch_id, offsets={"0": batch_id}, watermark_ms=WM0, seconds=1)
    progress = monitoring.batch_progress(tmp_path)
    found = monitoring.alerts(progress, {"watermark_stuck_batches": 5})
    assert [a["rule"] for a in found] == ["watermark_stuck"]
    lag = monitoring.kafka_lag({"t": {0: 10, 1: 20}}, {"t": {0: 15, 1: 20, 2: 7}})
    assert lag == {"per_topic": {"t": 12}, "total": 12}
    assert [a["rule"] for a in monitoring.alerts([], {"kafka_lag": 5}, lag=lag)] == ["kafka_lag"]


def test_log_summary_counts_levels_and_keeps_last_errors(tmp_path):
    (tmp_path / "stream.log").write_text("INFO ok\n26/11/23 WARN TaskSetManager: slow\nERROR boom 1\nTraceback (most recent call last):\n")
    (tmp_path / "quiet.log").write_text("INFO fine\n")
    report = monitoring.log_summary(tmp_path)
    assert list(report) == ["stream.log"]
    assert report["stream.log"]["counts"] == {"WARN": 1, "ERROR": 2}
    assert report["stream.log"]["last_errors"][-1].startswith("Traceback")
