"""Opt-in check against a real MongoDB server.

    HARBORTRACE_TEST_MONGO_URI=mongodb://127.0.0.1:27017 python -m pytest tests/integration -q

Uses a throwaway database whose name starts with ``harbortrace_it_`` and drops it afterwards.
"""
import copy
import json
import os
import shutil
import uuid
from collections import Counter
from pathlib import Path

import pytest

from harbortrace import serving

URI = os.environ.get("HARBORTRACE_TEST_MONGO_URI")
FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures/serving"
pytestmark = pytest.mark.skipif(not URI, reason="set HARBORTRACE_TEST_MONGO_URI to run against MongoDB")


@pytest.fixture
def db():
    name = f"harbortrace_it_{uuid.uuid4().hex}"
    database = serving.connect(URI, name)
    try:
        database.client.admin.command("ping")
        serving.ensure_indexes(database)
        yield database
    finally:
        database.client.drop_database(name)
        database.client.close()


def test_complete_visit_import_and_query_on_real_server(db):
    artifact = serving.load_artifact(FIXTURES / "generation_a")
    serving.publish(db, artifact, owner="it")
    reader = serving.Reader(db)
    result = reader.visits(serving.VisitQuery(limit=1000))
    assert result["total"] == 12
    assert Counter(v["status"] for v in result["visits"]) == {"candidate": 2, "reconciled": 10}
    expected = {v["visit_id"]: v for v in artifact.visits}
    for stored in result["visits"]:
        record = {key: value for key, value in stored.items() if key != "generation_id"}
        assert record == expected[record["visit_id"]]
        assert set(record) == set(serving.VISIT_FIELDS)
        assert serving.visit_errors(record) == []
    assert reader.ports() == ["4110", "4120"]
    assert reader.visits(serving.VisitQuery(port_id="4110"))["total"] == 6
    assert reader.visits(serving.VisitQuery(status="candidate"))["total"] == 2
    assert reader.visits(serving.VisitQuery(start="2024-01-01T23:00:00Z",
                                           end="2024-01-02T00:00:00Z"))["total"] == 2
    assert reader.visits(serving.VisitQuery(port_id="0000"))["total"] == 0
    assert sum(row["confirmed_count"] for row in reader.hourly()) == 10
    retry = serving.publish(db, artifact, owner="it")
    assert retry["reused"] is True
    assert db[serving.VISITS].count_documents({}) == 12
    assert db[serving.GENERATIONS].count_documents({}) == 1


def test_publish_switch_and_read_on_real_server(db):
    a = serving.load_artifact(FIXTURES / "generation_a")
    b = serving.load_artifact(FIXTURES / "generation_b")
    serving.ensure_indexes(db)
    serving.publish(db, a, owner="it")
    with pytest.raises(RuntimeError):
        serving.publish(db, b, owner="it", fail_after_visits=2)
    reader = serving.Reader(db)
    assert reader.visits(serving.VisitQuery(limit=1000))["total"] == len(a.visits)
    serving.publish(db, b, owner="it")
    assert reader.active_generation()["_id"] == b.generation_id
    assert sum(r["visits"] for r in reader.summary(serving.VisitQuery())["by_port_status"]) == len(b.visits)
    plan = db[serving.VISITS].find({"generation_id": b.generation_id, "port_id": "4120"}).explain()
    assert "IXSCAN" in str(plan)  # the serving index is used
    assert serving.health(db)["status"] == "ok"


@pytest.mark.parametrize("damage, message", [
    ("missing_field", "missing fields"),
    ("invalid_span", "observed_span_seconds"),
    ("checksum", "SHA-256"),
    ("incomplete", "not marked complete"),
    ("count", "visit_count"),
    ("status_counts", "status_counts"),
])
def test_invalid_artifact_leaves_real_database_unchanged(db, tmp_path, damage, message):
    serving.publish(db, serving.load_artifact(FIXTURES / "generation_a"), owner="it")
    before = {name: list(db[name].find().sort("_id", 1)) for name in db.list_collection_names()}
    directory = tmp_path / "invalid"
    shutil.copytree(FIXTURES / "generation_b", directory)
    manifest_path = directory / "manifest.json"
    visits_path = directory / "visits.jsonl"
    manifest = json.loads(manifest_path.read_text())
    if damage in ("missing_field", "invalid_span"):
        rows = [json.loads(line) for line in visits_path.read_text().splitlines()]
        if damage == "missing_field":
            rows[0].pop("source_digest")
        else:
            rows[0]["observed_span_seconds"] += 1
        payload = ("\n".join(json.dumps(row) for row in rows) + "\n").encode()
        visits_path.write_bytes(payload)
        # Keep the hash valid so that validation reaches the damaged Visit field.
        manifest["visits_sha256"] = serving.sha256_bytes(payload)
    elif damage == "checksum":
        visits_path.write_bytes(visits_path.read_bytes() + b"{}\n")
    elif damage == "incomplete":
        manifest["complete"] = False
    elif damage == "count":
        manifest["visit_count"] += 1
    else:
        manifest["status_counts"] = {"reconciled": 11}
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(serving.ValidationError, match=message):
        serving.publish(db, serving.load_artifact(directory), owner="it")
    after = {name: list(db[name].find().sort("_id", 1)) for name in db.list_collection_names()}
    assert after == before


def test_provisional_fixture_sequence_on_real_server(db):
    fixture = json.loads((FIXTURES / "provisional_updates.json").read_text())
    assert fixture["synthetic"] is True
    reader = serving.Reader(db)
    outcomes = []
    for step in fixture["steps"]:
        try:
            outcome = serving.apply_update(db, step["update"])["result"]
        except serving.StaleUpdateError:
            outcome = "stale"
        except serving.ConflictError:
            outcome = "conflict"
        outcomes.append(outcome)
        assert outcome == step["expect"]
        visible = reader.provisional(serving.VisitQuery())
        assert sorted(v["visit_id"] for v in visible["visits"]) == step["visit_ids"]
        for stored in visible["visits"]:
            assert serving.visit_errors({key: stored[key] for key in serving.VISIT_FIELDS}) == []
        if step["update"]["seq"] == 2:
            revised = step["update"]["visits"][0]
            stored = next(v for v in visible["visits"] if v["visit_id"] == revised["visit_id"])
            assert stored["quality_flags"] == revised["quality_flags"]
    assert outcomes == ["applied", "duplicate_ignored", "applied", "conflict", "applied", "stale", "applied"]
    assert visible["total"] == 1
    assert visible["cohorts"][0]["seq"] == 5
    assert db[serving.PROVISIONAL].count_documents({}) == 1


@pytest.mark.parametrize("damage, message", [
    ("missing_field", "missing fields"),
    ("identity", "visit_id differs"),
    ("time", "UTC offset"),
])
def test_invalid_update_leaves_real_database_unchanged(db, damage, message):
    steps = json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"]
    serving.apply_update(db, steps[0]["update"])
    before = {name: list(db[name].find().sort("_id", 1)) for name in db.list_collection_names()}
    update = copy.deepcopy(steps[2]["update"])
    if damage == "missing_field":
        update["visits"][0].pop("source_digest")
    elif damage == "identity":
        update["visits"][0]["visit_id"] = "0" * 64
    else:
        update["visits"][0]["first_inside"] = "2024-01-01T00:05:00"
    with pytest.raises(serving.ValidationError, match=message):
        serving.apply_update(db, update)
    after = {name: list(db[name].find().sort("_id", 1)) for name in db.list_collection_names()}
    assert after == before
