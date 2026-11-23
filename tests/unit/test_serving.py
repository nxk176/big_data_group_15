"""Serving component checks against an in-process MongoDB API (mongomock), on SYNTHETIC fixtures."""
import copy
import json
import shutil
import sys
import tracemalloc
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


def test_expired_lease_cannot_activate_without_a_successor(db, gen_a, monkeypatch):
    serving.publish(db, gen_a, owner="p1", activate=False)
    now = datetime.now(timezone.utc)
    token = serving.acquire_lease(db, "slow", ttl_seconds=10, now=now)
    monkeypatch.setattr(serving, "utc_now", lambda: now + timedelta(seconds=10))
    with pytest.raises(serving.FencedError):
        serving.activate_generation(db, gen_a.generation_id, token)
    assert serving.Reader(db).active_generation() is None
    assert serving.METRICS.counters["fenced_writes"] == 1


@pytest.mark.parametrize("stage", ["initialization", "completion"])
def test_stale_publisher_cannot_mutate_newer_generation_metadata(db, gen_a, monkeypatch, stage):
    collection = db[serving.GENERATIONS]
    method = "replace_one" if stage == "initialization" else "update_one"
    original = getattr(collection, method)
    raced = False
    winner = None

    def write_after_takeover(*args, **kwargs):
        nonlocal raced, winner
        if not raced:
            raced = True
            db[serving.LEASES].update_one({"_id": "publisher"},
                                         {"$set": {"expires_at": "2000-01-01T00:00:00+00:00"}})
            if stage == "initialization":
                serving.publish(db, gen_a, owner="fast")
            else:
                with pytest.raises(RuntimeError, match="simulated"):
                    serving.publish(db, gen_a, owner="fast", fail_after_visits=2)
            winner = collection.find_one({"_id": gen_a.generation_id})
        return original(*args, **kwargs)

    monkeypatch.setattr(collection, method, write_after_takeover)
    with pytest.raises(serving.FencedError):
        serving.publish(db, gen_a, owner="slow")
    assert collection.find_one({"_id": gen_a.generation_id}) == winner
    if stage == "initialization":
        assert serving.Reader(db).visits(serving.VisitQuery())["total"] == len(gen_a.visits)
    else:
        assert winner["status"] == "loading" and winner["fence_token"] == 2
        assert "completed_at" not in winner


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
        # Old snapshots remain available for readers that pinned before this update.
        cohort = db[serving.COHORTS].find_one({"_id": step["update"]["cohort_id"]})
        assert db[serving.PROVISIONAL].count_documents({"snapshot_id": cohort["snapshot_id"]}) == visible["total"]
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
    (lambda u: u.update(deleted_visit_ids=["a" * 64]), "delete mode"),
    (lambda u: u.update(emitted_at="2026-11-02T00:00:00"), "UTC offset"),
    (lambda u: u.update(visits=u["visits"] + u["visits"][:1]), "duplicate visit_id"),
])
def test_update_validation(db, change, message):
    update = copy.deepcopy(json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"][0]["update"])
    change(update)
    with pytest.raises(serving.ValidationError, match=message):
        serving.apply_update(db, update)


def provisional_update(seq=1, *, visits=None, mode="snapshot", deleted=None):
    update = copy.deepcopy(json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"][0]["update"])
    update.update(seq=seq, update_id=digest(update["cohort_id"], seq), mode=mode)
    if visits is not None:
        update["visits"] = visits
    if deleted is not None:
        update["deleted_visit_ids"] = deleted
    return update


def test_provisional_coverage_follows_activation_and_rollback(db, gen_a, gen_b):
    common = gen_a.visits[0]
    only_in_a = next(v for v in gen_a.visits if v["mmsi"] == "990000002" and v["status"] == "candidate")
    serving.apply_update(db, provisional_update(visits=[common, only_in_a]))
    reader, query = serving.Reader(db), serving.VisitQuery()
    serving.publish(db, gen_b, owner="test", activate=False)
    assert reader.provisional(query)["total"] == 2

    serving.publish(db, gen_b, owner="test")
    visible = reader.provisional(query)
    assert visible["total"] == 1
    assert [v["visit_id"] for v in visible["visits"]] == [only_in_a["visit_id"]]
    assert visible["cohorts"][0]["visit_count"] == 2

    serving.publish(db, gen_a, owner="test")
    assert reader.provisional(query)["total"] == 0
    assert serving.rollback(db, owner="test") == gen_b.generation_id
    assert [v["visit_id"] for v in reader.provisional(query)["visits"]] == [only_in_a["visit_id"]]


def test_provisional_filters_and_page_limit_apply_to_uncovered_visits(db, gen_a):
    serving.publish(db, gen_a, owner="test")
    # This provisional candidate became reconciled under the same identity.
    covered = dict(next(v for v in gen_a.visits if v["mmsi"] == "990000001"), status="candidate")
    next_day = dict(covered)
    for key in serving.REQUIRED_TIMES + serving.NULLABLE_TIMES:
        if next_day[key] is not None:
            next_day[key] = (datetime.fromisoformat(next_day[key]) + timedelta(days=1)).isoformat()
    next_day["visit_id"] = digest(next_day["mmsi"], next_day["port_id"], next_day["first_inside"], next_day["rule_version"])
    unmatched = []
    for visit in (v for v in gen_a.visits if v["mmsi"] == "990000002"):
        # Same vessel, port and time, but another rule makes a different visit ID.
        row = dict(visit, rule_version="visits-v2", status="candidate")
        row["visit_id"] = digest(row["mmsi"], row["port_id"], row["first_inside"], row["rule_version"])
        unmatched.append(row)
    serving.apply_update(db, provisional_update(visits=[covered, next_day, *reversed(unmatched)]))
    reader = serving.Reader(db)
    page = reader.provisional(serving.VisitQuery(port_id="4120", start="2024-01-01T00:00:00Z",
                                                end="2024-01-02T00:00:00Z", status="candidate", limit=1))
    assert page["total"] == 2
    assert [v["visit_id"] for v in page["visits"]] == [unmatched[0]["visit_id"]]
    vessel = reader.provisional(serving.VisitQuery(mmsi=covered["mmsi"], status="candidate"))
    assert [v["visit_id"] for v in vessel["visits"]] == [next_day["visit_id"]]
    assert reader.provisional(serving.VisitQuery(port_id="4110"))["total"] == 0


@pytest.mark.parametrize("value", [None, [], 1, "update"])
def test_update_container_validation(db, value):
    with pytest.raises(serving.ValidationError, match="object"):
        serving.apply_update(db, value)
    assert db[serving.COHORTS].count_documents({}) == 0


@pytest.mark.parametrize("field, value, message", [
    ("cohort_id", "", "cohort_id"), ("cohort_id", "  ", "cohort_id"),
    ("cohort_id", {"$ne": "x"}, "cohort_id"), ("cohort_id", 42, "cohort_id"),
    ("visits", None, "visits"), ("visits", {}, "visits"),
    ("visits", "visits", "visits"), ("visits", [None], "object"),
    ("deleted_visit_ids", None, "deleted_visit_ids"),
    ("deleted_visit_ids", "a" * 64, "deleted_visit_ids"),
    ("deleted_visit_ids", ["bad"], "SHA-256"),
    ("deleted_visit_ids", [{"$ne": "x"}], "SHA-256"),
    ("deleted_visit_ids", ["a" * 64, "a" * 64], "duplicate"),
])
def test_malformed_update_fields_leave_database_unchanged(db, field, value, message):
    update = provisional_update()
    update[field] = value
    with pytest.raises(serving.ValidationError, match=message):
        serving.apply_update(db, update)
    assert db[serving.COHORTS].count_documents({}) == 0
    assert db[serving.PROVISIONAL].count_documents({}) == 0


def test_retry_timestamp_is_attempt_metadata_and_preserves_winner(db):
    update = provisional_update()
    serving.apply_update(db, update)
    winning_timestamp = update["emitted_at"]
    update["emitted_at"] = "2026-11-02T00:00:01+00:00"
    assert serving.apply_update(db, update)["result"] == "duplicate_ignored"
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert visible["cohorts"][0]["emitted_at"] == winning_timestamp
    assert {v["emitted_at"] for v in visible["visits"]} == {winning_timestamp}


@pytest.mark.parametrize("initial", [False, True])
@pytest.mark.parametrize("identical", [False, True])
@pytest.mark.parametrize("different_timestamp", [False, True])
def test_racing_updates_preserve_winner_and_retry_semantics(db, monkeypatch, initial, identical, different_timestamp):
    base = provisional_update()
    if not initial:
        serving.apply_update(db, base)
    seq = 1 if initial else 2
    winner = provisional_update(seq, visits=base["visits"][:1])
    loser = copy.deepcopy(winner) if identical else provisional_update(seq, visits=base["visits"][1:])
    if different_timestamp:
        loser["emitted_at"] = "2026-11-02T00:00:01+00:00"
    collection = db[serving.PROVISIONAL]
    original = collection.bulk_write
    raced = False

    def write_after_winner(*args, **kwargs):
        nonlocal raced
        if not raced:
            raced = True
            serving.apply_update(db, winner)
        return original(*args, **kwargs)

    monkeypatch.setattr(collection, "bulk_write", write_after_winner)
    if identical:
        assert serving.apply_update(db, loser)["result"] == "duplicate_ignored"
    else:
        with pytest.raises(serving.ConflictError):
            serving.apply_update(db, loser)
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert [v["visit_id"] for v in visible["visits"]] == [winner["visits"][0]["visit_id"]]
    assert visible["cohorts"][0]["visit_count"] == visible["total"] == 1
    assert visible["cohorts"][0]["emitted_at"] == winner["emitted_at"]
    assert {v["emitted_at"] for v in visible["visits"]} == {winner["emitted_at"]}


def test_provisional_reader_pins_snapshot_while_update_commits(db, monkeypatch):
    first = provisional_update()
    serving.apply_update(db, first)
    collection = db[serving.COHORTS]
    original = collection.find

    def update_after_pinning(*args, **kwargs):
        if args[0] != {}:
            return original(*args, **kwargs)
        cohorts = list(original(*args, **kwargs))
        serving.apply_update(db, provisional_update(2, visits=first["visits"][:1]))
        return cohorts

    monkeypatch.setattr(collection, "find", update_after_pinning)
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert visible["cohorts"][0]["seq"] == 1
    assert {v["visit_id"] for v in visible["visits"]} == {v["visit_id"] for v in first["visits"]}


def test_initialization_race_handles_duplicate_key(db, monkeypatch):
    winner = provisional_update(visits=provisional_update()["visits"][:1])
    loser = provisional_update(visits=provisional_update()["visits"][1:])
    collection = db[serving.COHORTS]
    original = collection.insert_one
    raced = False

    def insert_after_winner(*args, **kwargs):
        nonlocal raced
        if not raced:
            raced = True
            serving.apply_update(db, winner)
        return original(*args, **kwargs)

    monkeypatch.setattr(collection, "insert_one", insert_after_winner)
    with pytest.raises(serving.ConflictError):
        serving.apply_update(db, loser)
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert [v["visit_id"] for v in visible["visits"]] == [winner["visits"][0]["visit_id"]]


def test_same_update_based_on_different_snapshots_cannot_mix_rows(db, monkeypatch):
    first = provisional_update()
    serving.apply_update(db, first)
    update = provisional_update(3, visits=first["visits"][:1], mode="upsert")
    collection = db[serving.PROVISIONAL]
    original = collection.bulk_write
    raced = False

    def write_after_newer_base(*args, **kwargs):
        nonlocal raced
        if not raced:
            raced = True
            serving.apply_update(db, provisional_update(2, visits=[], mode="delete",
                                                        deleted=[first["visits"][1]["visit_id"]]))
            serving.apply_update(db, update)
        return original(*args, **kwargs)

    monkeypatch.setattr(collection, "bulk_write", write_after_newer_base)
    assert serving.apply_update(db, update)["result"] == "duplicate_ignored"
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert [v["visit_id"] for v in visible["visits"]] == [first["visits"][0]["visit_id"]]
    assert visible["total"] == visible["cohorts"][0]["visit_count"] == 1


def test_newer_update_losing_cas_is_retryable_and_rebuilds_from_winner(db, monkeypatch):
    first = provisional_update()
    serving.apply_update(db, first)
    later = provisional_update(3, visits=first["visits"][:1], mode="upsert")
    collection = db[serving.PROVISIONAL]
    original = collection.bulk_write
    raced = False

    def write_after_earlier_winner(*args, **kwargs):
        nonlocal raced
        if not raced:
            raced = True
            serving.apply_update(db, provisional_update(2, visits=[], mode="delete",
                                                        deleted=[first["visits"][1]["visit_id"]]))
        return original(*args, **kwargs)

    monkeypatch.setattr(collection, "bulk_write", write_after_earlier_winner)
    with pytest.raises(serving.ConcurrentUpdateError, match="retry"):
        serving.apply_update(db, later)
    assert serving.Reader(db).provisional(serving.VisitQuery())["cohorts"][0]["seq"] == 2
    assert serving.apply_update(db, later)["result"] == "applied"
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert visible["cohorts"][0]["seq"] == 3
    assert [v["visit_id"] for v in visible["visits"]] == [first["visits"][0]["visit_id"]]


def test_legacy_cohort_can_be_read_and_extended(db):
    update = provisional_update()
    cohort_id = update["cohort_id"]
    db[serving.COHORTS].insert_one({"_id": cohort_id, "seq": 1, "applied": {},
                                  "visit_count": 2, "emitted_at": update["emitted_at"]})
    for visit in update["visits"]:
        db[serving.PROVISIONAL].insert_one({"_id": f"legacy:{visit['visit_id']}",
                                          "cohort_id": cohort_id, "seq": 1, **visit})
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 2
    serving.apply_update(db, provisional_update(2, visits=[], mode="delete",
                                                deleted=[update["visits"][0]["visit_id"]]))
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert [v["visit_id"] for v in visible["visits"]] == [update["visits"][1]["visit_id"]]
    assert db[serving.PROVISIONAL].count_documents({"seq": 1}) == 2


def test_metrics_export():
    metrics = serving.Metrics()
    metrics.incr("reads", 2)
    metrics.gauge("lag", 1.5)
    with metrics.timer("op"):
        pass
    text = metrics.prometheus()
    assert "harbortrace_reads_total 2" in text and "harbortrace_lag 1.5" in text
    assert metrics.snapshot()["timings"]["op"]["count"] == 1


def test_timing_metrics_keep_cumulative_statistics_with_bounded_memory():
    metrics = serving.Metrics()
    tracing = tracemalloc.is_tracing()
    if not tracing:
        tracemalloc.start()
    try:
        for _ in range(250):
            for seconds in (0.125, 0.25, 0.5, 1.0):
                metrics.observe("request", seconds)
        retained_before = tracemalloc.get_traced_memory()[0]
        for _ in range(24_750):
            for seconds in (0.125, 0.25, 0.5, 1.0):
                metrics.observe("request", seconds)
        retained_after = tracemalloc.get_traced_memory()[0]
    finally:
        if not tracing:
            tracemalloc.stop()
    # Additional observations of one metric must not retain samples. A generous
    # allocation allowance avoids tying this check to a particular accumulator.
    assert retained_after - retained_before < 16_384
    metrics.observe("publish", 3.0)
    metrics.observe("publish", 3.0)
    assert metrics.snapshot()["timings"] == {
        "request": {"count": 100_000, "max_seconds": 1.0, "total_seconds": 46_875.0},
        "publish": {"count": 2, "max_seconds": 3.0, "total_seconds": 6.0},
    }
    exported = metrics.prometheus().splitlines()
    assert "harbortrace_request_seconds_count 100000" in exported
    assert "harbortrace_request_seconds_sum 46875.000000" in exported
    assert "harbortrace_publish_seconds_count 2" in exported
    assert "harbortrace_publish_seconds_sum 6.000000" in exported
    metrics.timings.clear()
    assert metrics.snapshot()["timings"] == {}
    assert "harbortrace_request_seconds_count" not in metrics.prometheus()
    metrics.observe("request", 0.5)
    assert metrics.snapshot()["timings"]["request"] == {
        "count": 1, "max_seconds": 0.5, "total_seconds": 0.5,
    }
