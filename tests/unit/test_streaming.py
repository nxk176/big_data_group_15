"""Structured Streaming checks with a file source (synthetic input, no Kafka needed)."""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("pandas")
pytestmark = pytest.mark.spark

from harbortrace import engine, streaming  # noqa: E402
from harbortrace.replay import replay_messages  # noqa: E402
from test_stream_state import PORTS, START, reference, vessel_observations  # noqa: E402

LATENESS_MINUTES = 10
VESSELS = [f"99000010{index}" for index in range(6)]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = engine.engine_session("harbortrace-streaming-tests", shuffle_partitions=2,
                                    warehouse=str(tmp_path_factory.mktemp("wh")))
    yield session
    session.stop()


@pytest.fixture(scope="module")
def messages() -> list[dict]:
    observations = [row for index, mmsi in enumerate(VESSELS) for row in vessel_observations(mmsi, seed=20 + index)]
    return replay_messages(observations, "stream-test", START, seed=5, duplicate_copies=30)


def write_files(directory: Path, rows: list[dict], files: int, first: int = 0) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    size = -(-len(rows) // files)
    for index in range(files):
        chunk = rows[index * size:(index + 1) * size]
        path = directory / f"part-{first + index:04d}.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in chunk))
        # The file source reads in modification-time order; make that order explicit.
        os.utime(path, (1_700_000_000 + first + index,) * 2)


def run(spark, source: Path, output: Path) -> dict:
    query = streaming.start_stream(spark, str(source), output, ports=list(PORTS), stream_id="t",
                                   lateness_minutes=LATENESS_MINUTES, max_per_trigger=1)
    query.awaitTermination()
    assert query.exception() is None
    return streaming.stream_report(query)


def view_of(output: Path) -> dict[str, dict]:
    view = {}
    for update in streaming.read_updates(output):
        view.update({visit["visit_id"]: visit for visit in update["visits"]})
    return view


def assert_matches_batch(view: dict, released: list[dict]) -> None:
    """Equal to the batch result, except the end flag and gap of a visit open at state expiry."""
    expected = reference(released)
    assert set(view) == set(expected)
    for key, visit in expected.items():
        differing = {name for name in visit if visit[name] != view[key][name]}
        assert differing <= {"quality_flags", "maximum_gap_seconds"}, (key, differing)
        if differing:
            assert set(view[key]["quality_flags"]) ^ set(visit["quality_flags"]) == {
                "input_range_ended", "long_gap_unknown_continuity"}


def released_before(messages: list[dict], watermark: str) -> list[dict]:
    limit = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    return [row for row in messages if datetime.fromisoformat(row["event_time"]) <= limit]


def test_stream_equals_batch_on_everything_the_watermark_released(spark, tmp_path, messages):
    write_files(tmp_path / "in", messages, files=8)
    report = run(spark, tmp_path / "in", tmp_path / "out")
    newest = max(datetime.fromisoformat(row["event_time"]) for row in messages)
    assert datetime.fromisoformat(report["watermark"].replace("Z", "+00:00")) == newest - timedelta(
        minutes=LATENESS_MINUTES)
    view = view_of(tmp_path / "out")
    assert_matches_batch(view, released_before(messages, report["watermark"]))
    assert len(view) > 20 and report["batches"] >= 8
    metrics = streaming.read_metrics(tmp_path / "out")
    assert sum(row["duplicates"] for row in metrics) == 30
    assert sum(row["accepted"] for row in metrics) == len(messages) - 30
    assert sum(row["late"] + row["forced"] + row["invalid"] for row in metrics) == 0
    assert report["state_rows"] <= len(VESSELS) and report["input_rows"] == len(messages)
    for update in streaming.read_updates(tmp_path / "out"):  # serving contract
        from harbortrace.serving import validate_update
        validate_update(update)


def test_restart_from_checkpoint_continues_without_loss_or_repeats(spark, tmp_path, messages):
    half = len(messages) // 2
    write_files(tmp_path / "in", messages[:half], files=4)
    run(spark, tmp_path / "in", tmp_path / "out")
    first = streaming.read_updates(tmp_path / "out")
    write_files(tmp_path / "in", messages[half:], files=4, first=4)
    report = run(spark, tmp_path / "in", tmp_path / "out")  # same checkpoint: a restart
    updates = streaming.read_updates(tmp_path / "out")
    assert updates[:len(first)] == first
    identities = [(update["cohort_id"], update["seq"]) for update in updates]
    assert len(identities) == len(set(identities))
    assert_matches_batch(view_of(tmp_path / "out"), released_before(messages, report["watermark"]))
    assert sum(row["accepted"] for row in streaming.read_metrics(tmp_path / "out")) == len(messages) - 30


def test_messages_that_break_the_replay_contract_are_counted_not_processed(spark, tmp_path, messages):
    forged = dict(messages[5], replay_id="0" * 64)
    unreadable = {"mmsi": "990000100", "event_time": "not a time"}
    write_files(tmp_path / "in", messages[:200] + [forged, unreadable], files=2)
    run(spark, tmp_path / "in", tmp_path / "out")
    metrics = streaming.read_metrics(tmp_path / "out")
    assert sum(row["invalid"] for row in metrics) == 1  # the unreadable row never reaches a vessel group
    assert sum(row["accepted"] + row["duplicates"] for row in metrics) == 200
