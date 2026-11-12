import hashlib
import json
from pathlib import Path

import pytest

from harbortrace.contracts import Observation
from harbortrace.replay import account, message_key, replay_id, replay_messages, validate_message

FIXTURES = Path(__file__).parents[1] / "fixtures"
CANONICAL_DIR = FIXTURES / "canonical"


def canonical_rows():
    return [json.loads(line) for line in (CANONICAL_DIR / "week06_canonical.jsonl").read_text().splitlines()]


def test_committed_fixtures_match_manifest():
    manifest = json.loads((CANONICAL_DIR / "week06_manifest.json").read_text())
    assert manifest["kind"] == "synthetic"
    for part in ("canonical", "replay"):
        data = (CANONICAL_DIR / manifest[part]["path"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == manifest[part]["sha256"]
    rows = canonical_rows()
    accounting = manifest["canonical"]["row_accounting"]
    assert len(rows) == manifest["canonical"]["rows"] == accounting["canonical_rows"]
    assert accounting["accounted_rows"] == accounting["source_rows"]
    observations = [Observation.from_json(row) for row in rows]
    assert [(o.mmsi, o.event_time) for o in observations] == sorted((o.mmsi, o.event_time) for o in observations)
    assert len({o.logical_id for o in observations}) == len(observations)


def test_committed_replay_messages_validate_and_reconcile():
    manifest = json.loads((CANONICAL_DIR / "week06_manifest.json").read_text())
    messages = [json.loads(line) for line in (CANONICAL_DIR / "week06_replay.jsonl").read_text().splitlines()]
    assert account(messages) == manifest["replay"]["accounting"]
    assert {m["source_record_id"] for m in messages} == {r["source_record_id"] for r in canonical_rows()}


def test_replay_is_deterministic_and_event_time_ordered():
    rows = canonical_rows()
    first = replay_messages(rows, "run-a", "2024-01-02T00:00:00Z", seed=7, duplicate_copies=2)
    again = replay_messages(list(reversed(rows)), "run-a", "2024-01-02T00:00:00Z", seed=7, duplicate_copies=2)
    assert first == again
    assert [m["event_time"] for m in first] == sorted(m["event_time"] for m in first)
    assert [m["ingested_at"] for m in first] == sorted(m["ingested_at"] for m in first)
    counts = account(first)
    assert counts["messages"] == len(rows) + 2
    assert counts["replay_copies"] == 2 and counts["repeated_replay_ids"] == 0
    assert counts["distinct_logical_observations"] == len(rows)
    assert message_key(first[0]) == first[0]["mmsi"]


def test_replay_identity_depends_on_run_and_copy():
    record = canonical_rows()[0]["source_record_id"]
    assert replay_id("run-a", record, 0) != replay_id("run-b", record, 0)
    assert replay_id("run-a", record, 0) != replay_id("run-a", record, 1)
    [message] = replay_messages(canonical_rows()[:1], "run-a", "2024-01-02T00:00:00Z", seed=1)
    assert message["replay_id"] == replay_id("run-a", record, 0)


def test_invalid_replay_messages_are_rejected():
    [message] = replay_messages(canonical_rows()[:1], "run-a", "2024-01-02T00:00:00Z", seed=1)
    with pytest.raises(ValueError, match="replay_id"):
        validate_message({**message, "copy_index": 1})
    with pytest.raises(ValueError, match="missing"):
        validate_message({k: v for k, v in message.items() if k != "ingested_at"})
    with pytest.raises(ValueError, match="contract version"):
        validate_message({**message, "contract_version": "0.9"})
    with pytest.raises(ValueError, match="duplicate_copies"):
        replay_messages(canonical_rows(), "run-a", "2024-01-02T00:00:00Z", seed=1, duplicate_copies=99)
    with pytest.raises(ValueError, match="Naive"):
        replay_messages(canonical_rows(), "run-a", "2024-01-02T00:00:00", seed=1)
