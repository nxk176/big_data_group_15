"""Serving component checks against an in-process MongoDB API (mongomock), on SYNTHETIC fixtures."""
import copy
import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import mongomock
import pytest

from harbortrace import serving
from harbortrace.contracts import Observation, digest, utc_time
from harbortrace.visits import Rules, reconstruct

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/serving"


@pytest.fixture
def db():
    serving.METRICS.counters.clear()
    database = mongomock.MongoClient()["harbortrace_test"]
    serving.ensure_indexes(database)
    return database


@pytest.fixture
def gen_a():
    return serving.load_artifact(FIXTURES / "generation_a")


@pytest.fixture
def gen_b():
    return serving.load_artifact(FIXTURES / "generation_b")


def copy_artifact(tmp_path, name="generation_a"):
    target = tmp_path / name
    shutil.copytree(FIXTURES / name, target)
    return target


# ----------------------------------------------------------------- fixtures and validation

def test_committed_fixtures_are_reproducible(tmp_path):
    sys.path.insert(0, str(ROOT / "scripts"))
    try:
        import make_serving_fixtures
    finally:
        sys.path.pop(0)
    make_serving_fixtures.build(tmp_path)
    for path in sorted(p for p in tmp_path.rglob("*") if p.is_file()):
        assert (FIXTURES / path.relative_to(tmp_path)).read_bytes() == path.read_bytes(), path


def test_fixtures_are_complete_labelled_and_valid(gen_a, gen_b):
    for artifact in (gen_a, gen_b):
        assert artifact.manifest["synthetic"] is True
        assert all(set(v) == set(serving.VISIT_FIELDS) for v in artifact.visits)
        assert all(not serving.visit_errors(v) for v in artifact.visits)
    assert {v["status"] for v in gen_a.visits} == {"reconciled", "candidate"}
    assert any(v["left_censored"] for v in gen_a.visits) and any(v["right_censored"] for v in gen_a.visits)
    assert gen_a.generation_id != gen_b.generation_id
    assert len(gen_b.visits) == len(gen_a.visits) - 1  # late observation bridged one split


@pytest.mark.parametrize("mutation, message", [
    (lambda v: v.pop("source_digest"), "missing fields"),
    (lambda v: v.update(extra=1), "unexpected fields"),
    (lambda v: v.update(visit_id="0" * 64), "visit_id differs"),
    (lambda v: v.update(first_inside=v["first_inside"].replace("+00:00", "")), "UTC offset"),
    (lambda v: v.update(first_inside=v["first_inside"].replace("+00:00", "+07:00")), "UTC offset"),
    (lambda v: v.update(observed_span_seconds=v["observed_span_seconds"] + 1), "observed_span_seconds"),
    (lambda v: v.update(left_censored=not v["left_censored"]), "left_censored"),
    (lambda v: v.update(status="confirmed"), "status"),
    (lambda v: v.update(quality_flags=["z", "a"]), "sorted"),
    (lambda v: v.update(source_record_ids=[]), "bounded sample"),
    (lambda v: v.update(observation_count=True), "non-negative integer"),
    (lambda v: v.update(mmsi="12345"), "nine digits"),
])
def test_visit_validation_rejects_contract_violations(gen_a, mutation, message):
    visit = copy.deepcopy(gen_a.visits[0])
    mutation(visit)
    assert any(message in error for error in serving.visit_errors(visit)), serving.visit_errors(visit)


def _rewrite(path: Path, transform):
    manifest = json.loads((path / "manifest.json").read_text())
    transform(manifest)
    (path / "manifest.json").write_text(json.dumps(manifest))


def _edit(**updates):
    return lambda path: _rewrite(path, lambda manifest: manifest.update(updates))


def _append_line(path):
    (path / "visits.jsonl").write_text((path / "visits.jsonl").read_text() + "{}\n")


@pytest.mark.parametrize("damage, message", [
    (lambda path: (path / "manifest.json").unlink(), "incomplete"),
    (_edit(complete=False), "not marked complete"),
    (_edit(contract_version="2.0"), "contract"),
    (_edit(visit_count=13), "visit_count"),
    (_edit(rule_version="visits-v9"), "rule_version"),
    (_edit(visits_file="../generation_b/visits.jsonl"), "inside the artifact"),
    (_append_line, "SHA-256"),
])
def test_artifact_import_rejects_incomplete_or_inconsistent_input(tmp_path, damage, message):
    path = copy_artifact(tmp_path)
    damage(path)
    with pytest.raises(serving.ValidationError, match=message):
        serving.load_artifact(path)


def test_duplicate_visit_ids_are_rejected(tmp_path, gen_a):
    visits = list(gen_a.visits) + [gen_a.visits[0]]
    serving.write_artifact(tmp_path / "dup", visits[:-1], run_id="x", rule_version="visits-v1",
                           synthetic=True, created_at="2026-11-02T00:00:00+00:00")
    payload = serving.visits_jsonl(visits)
    (tmp_path / "dup/visits.jsonl").write_bytes(payload)
    _rewrite(tmp_path / "dup", lambda m: m.update(visits_sha256=serving.sha256_bytes(payload),
                                                       visit_count=len(visits),
                                                       status_counts={"candidate": 2, "reconciled": 11}))
    with pytest.raises(serving.ValidationError, match="duplicate visit_id"):
        serving.load_artifact(tmp_path / "dup")


def test_package_engine_output_round_trip(tmp_path):
    """Engine visits (as written by the week-5 runner) package into a valid artifact."""
    events = [("2024-01-01T00:00:00Z", None), ("2024-01-01T00:05:00Z", "4120"),
              ("2024-01-01T00:20:00Z", "4120"), ("2024-01-01T00:30:00Z", None), ("2024-01-01T00:45:00Z", None)]
    observations = [Observation("990000123", utc_time(t), -118.2, 33.75, "f", i, digest("f", i),
                                digest("990000123", t, p), assignment=p) for i, (t, p) in enumerate(events)]
    rows = [json.loads(json.dumps(v.as_json())) for v in reconstruct(observations, ["4120"], Rules())]
    serving.write_artifact(tmp_path / "a", rows, run_id="engine-run", rule_version="visits-v1",
                           synthetic=True, created_at="2026-11-02T00:00:00+00:00")
    artifact = serving.load_artifact(tmp_path / "a")
    assert artifact.manifest["status_counts"] == {"reconciled": 1}
    with pytest.raises(FileExistsError):
        serving.write_artifact(tmp_path / "a", rows, run_id="engine-run", rule_version="visits-v1",
                               synthetic=True, created_at="2026-11-02T00:00:00+00:00")


# ----------------------------------------------------------------- publication

def test_publish_activates_and_counts_match(db, gen_a):
    result = serving.publish(db, gen_a, owner="p1")
    assert result["activated"] and result["visits"] == len(gen_a.visits)
    reader = serving.Reader(db)
    assert reader.active_generation()["_id"] == gen_a.generation_id
    assert reader.visits(serving.VisitQuery(limit=1000))["total"] == len(gen_a.visits)
    expected = serving.hourly_aggregates(gen_a.visits)
    assert len(reader.hourly()) == len(expected)
    assert sum(a["confirmed_count"] for a in expected) == gen_a.manifest["status_counts"]["reconciled"]


def test_publish_retry_is_idempotent(db, gen_a):
    serving.publish(db, gen_a, owner="p1")
    again = serving.publish(db, gen_a, owner="p1")
    assert again["reused"] is True and again["activated"] is False
    assert db[serving.VISITS].count_documents({}) == len(gen_a.visits)
    assert db[serving.GENERATIONS].count_documents({}) == 1


def test_crash_during_publication_is_invisible_and_recoverable(db, gen_a, gen_b):
    serving.publish(db, gen_a, owner="p1")
    with pytest.raises(RuntimeError, match="simulated"):
        serving.publish(db, gen_b, owner="p1", fail_after_visits=3)
    reader = serving.Reader(db)
    assert reader.active_generation()["_id"] == gen_a.generation_id
    assert reader.visits(serving.VisitQuery(limit=1000))["total"] == len(gen_a.visits)
    health = serving.health(db)
    assert health["status"] == "ok"
    later = datetime.now(timezone.utc) + timedelta(hours=1)
    assert any(a["code"] == "publication_stuck" for a in serving.health(db, now=later)["alerts"])
    assert serving.recover_incomplete(db, older_than_seconds=0) == [gen_b.generation_id]
    with pytest.raises(serving.ValidationError, match="not complete"):
        serving.activate_generation(db, gen_b.generation_id, serving.acquire_lease(db, "p1"))
    resumed = serving.publish(db, gen_b, owner="p1")
    assert resumed["activated"] and reader.active_generation()["_id"] == gen_b.generation_id
    assert db[serving.VISITS].count_documents({"generation_id": gen_b.generation_id}) == len(gen_b.visits)


def test_fencing_rejects_a_stale_publisher(db, gen_a, gen_b):
    serving.publish(db, gen_a, owner="p1", activate=False)
    serving.publish(db, gen_b, owner="p1", activate=False)
    now = datetime.now(timezone.utc)
    old = serving.acquire_lease(db, "slow", ttl_seconds=10, now=now)
    with pytest.raises(serving.FencedError, match="held by slow"):
        serving.acquire_lease(db, "fast", now=now)
    new = serving.acquire_lease(db, "fast", now=now + timedelta(seconds=11))  # slow's lease expired
    assert new > old
    serving.activate_generation(db, gen_b.generation_id, new)
    with pytest.raises(serving.FencedError):
        serving.activate_generation(db, gen_a.generation_id, old)
    assert serving.Reader(db).active_generation()["_id"] == gen_b.generation_id
    assert serving.METRICS.counters["fenced_writes"] == 1


def test_rollback_and_drop_rules(db, gen_a, gen_b):
    serving.publish(db, gen_a, owner="p1")
    serving.publish(db, gen_b, owner="p1")
    assert serving.rollback(db, owner="ops") == gen_a.generation_id
    assert serving.Reader(db).active_generation()["_id"] == gen_a.generation_id
    with pytest.raises(serving.ValidationError):
        serving.drop_generation(db, gen_a.generation_id)
    actions = [r["action"] for r in db[serving.AUDIT].find()]
    assert actions.count("generation_activated") == 3


def test_reader_pins_the_requested_generation(db, gen_a, gen_b):
    serving.publish(db, gen_a, owner="p1")
    reader = serving.Reader(db)
    pinned = reader.active_generation()["_id"]
    serving.publish(db, gen_b, owner="p1")  # pointer moves between two reads of one request
    first = reader.visits(serving.VisitQuery(limit=1000), pinned)
    second = reader.summary(serving.VisitQuery(), pinned)
    assert first["generation"]["generation_id"] == second["generation"]["generation_id"] == gen_a.generation_id
    assert sum(r["visits"] for r in second["by_port_status"]) == first["total"] == len(gen_a.visits)


def test_reader_filters(db, gen_a):
    serving.publish(db, gen_a, owner="p1")
    reader = serving.Reader(db)
    by_port = reader.visits(serving.VisitQuery(port_id="4110", limit=1000))
    assert by_port["total"] == sum(v["port_id"] == "4110" for v in gen_a.visits)
    late_evening = reader.visits(serving.VisitQuery(start="2024-01-01T23:00:00Z", end="2024-01-02T00:00:00Z"))
    assert [v["first_inside"] for v in late_evening["visits"]] == ["2024-01-01T23:55:00+00:00"] * 2
    assert reader.visits(serving.VisitQuery(start="2024-01-02T00:00:00Z"))["total"] == 0
    candidates = reader.visits(serving.VisitQuery(status="candidate"))
    assert candidates["total"] == gen_a.manifest["status_counts"]["candidate"]
    limited = reader.visits(serving.VisitQuery(limit=2))
    assert limited["truncated"] and len(limited["visits"]) == 2
    with pytest.raises(serving.ValidationError):
        serving.VisitQuery(status="confirmed").mongo_filter()
    with pytest.raises(serving.ValidationError):
        serving.VisitQuery(start="2024-01-02T00:00:00Z", end="2024-01-01T00:00:00Z").mongo_filter()
    assert reader.visits(serving.VisitQuery(port_id="0000"))["visits"] == []


def test_empty_database_reads(db):
    reader = serving.Reader(db)
    assert reader.active_generation() is None
    assert reader.visits(serving.VisitQuery())["visits"] == []
    assert reader.ports() == []
    assert serving.health(db)["alerts"][0]["code"] == "no_active_generation"


def test_health_detects_count_mismatch(db, gen_a):
    serving.publish(db, gen_a, owner="p1")
    db[serving.VISITS].delete_one({"generation_id": gen_a.generation_id})
    report = serving.health(db)
    assert report["status"] == "critical"
    assert "active_count_mismatch" in [a["code"] for a in report["alerts"]]
    stale = serving.health(db, now=datetime.now(timezone.utc) + timedelta(days=3))
    assert "generation_stale" in [a["code"] for a in stale["alerts"]]


# ----------------------------------------------------------------- provisional updates

def test_provisional_fixture_sequence(db):
    steps = json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"]
    reader = serving.Reader(db)
    for step in steps:
        try:
            outcome = serving.apply_update(db, step["update"])["result"]
        except serving.StaleUpdateError:
            outcome = "stale"
        except serving.ConflictError:
            outcome = "conflict"
        assert outcome == step["expect"], step["update"]["seq"]
        visible = reader.provisional(serving.VisitQuery())
        assert sorted(v["visit_id"] for v in visible["visits"]) == step["visit_ids"]
        # Only the pointed-to sequence is ever stored after an update completes.
        assert len(db[serving.PROVISIONAL].distinct("seq")) == 1
    assert serving.METRICS.counters["provisional_stale_rejected"] == 1


def test_provisional_reader_ignores_uncommitted_sequence(db):
    steps = json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"]
    serving.apply_update(db, steps[0]["update"])
    # Simulate a writer that stored seq 9 documents but crashed before moving the pointer.
    ghost = {**steps[0]["update"]["visits"][0], "_id": "ghost", "cohort_id": steps[0]["update"]["cohort_id"],
             "seq": 9, "emitted_at": "2026-11-02T00:00:00+00:00", "visit_id": "f" * 64}
    db[serving.PROVISIONAL].insert_one(ghost)
    ids = [v["visit_id"] for v in serving.Reader(db).provisional(serving.VisitQuery())["visits"]]
    assert "f" * 64 not in ids and len(ids) == 2


@pytest.mark.parametrize("change, message", [
    (lambda u: u.update(update_id="0" * 64), "update_id"),
    (lambda u: u.update(mode="append"), "mode"),
    (lambda u: u.update(seq=0), "positive"),
    (lambda u: u.update(deleted_visit_ids=["x"]), "delete mode"),
    (lambda u: u.update(emitted_at="2026-11-02T00:00:00"), "UTC offset"),
    (lambda u: u.update(visits=u["visits"] + u["visits"][:1]), "duplicate visit_id"),
])
def test_update_validation(db, change, message):
    update = copy.deepcopy(json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"][0]["update"])
    change(update)
    with pytest.raises(serving.ValidationError, match=message):
        serving.apply_update(db, update)


def test_metrics_export():
    metrics = serving.Metrics()
    metrics.incr("reads", 2)
    metrics.gauge("lag", 1.5)
    with metrics.timer("op"):
        pass
    text = metrics.prometheus()
    assert "harbortrace_reads_total 2" in text and "harbortrace_lag 1.5" in text
    assert metrics.snapshot()["timings"]["op"]["count"] == 1
