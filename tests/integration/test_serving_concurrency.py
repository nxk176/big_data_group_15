"""Opt-in real MongoDB regressions for races at serving publication boundaries.

Set HARBORTRACE_TEST_MONGO_URI; each test drops its random harbortrace_it_* database.
Fixtures are synthetic. Events force interleavings without timing-dependent sleeps.
"""
import copy
import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from pymongo.collection import Collection

from harbortrace import serving
from harbortrace.contracts import digest

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


def update(seq=1, *, visits=None, mode="snapshot", deleted=None):
    value = json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"][0]["update"]
    value.update(seq=seq, update_id=digest(value["cohort_id"], seq), mode=mode)
    if visits is not None:
        value["visits"] = visits
    if deleted is not None:
        value["deleted_visit_ids"] = deleted
    return value


def race_writer(db, monkeypatch, paused_update, advance, *, method="bulk_write", collection=serving.PROVISIONAL):
    """Pause a real writer before its DB operation, let another writer commit, resume."""
    entered, resume = threading.Event(), threading.Event()
    original = getattr(Collection, method)

    def pause(self, *args, **kwargs):
        if (self.database.name == db.name and self.name == collection
                and threading.current_thread().name.startswith("paused_writer")):
            entered.set()
            if not resume.wait(timeout=15):
                raise AssertionError("concurrent test did not release its paused writer")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Collection, method, pause)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="paused_writer") as pool:
        future = pool.submit(serving.apply_update, db, paused_update)
        try:
            assert entered.wait(timeout=10), "writer did not reach the expected publication boundary"
            advance()
        finally:
            resume.set()
        return future.result(timeout=10)


@pytest.mark.parametrize("initial", [False, True])
@pytest.mark.parametrize("identical", [False, True])
@pytest.mark.parametrize("different_timestamp", [False, True])
def test_same_sequence_writers_keep_winner_complete(db, monkeypatch, initial, identical, different_timestamp):
    first = update()
    if not initial:
        serving.apply_update(db, first)
    seq = 1 if initial else 2
    winner = update(seq, visits=first["visits"][:1])
    loser = copy.deepcopy(winner) if identical else update(seq, visits=first["visits"][1:])
    if different_timestamp:
        loser["emitted_at"] = "2026-11-02T00:00:01+00:00"
    if identical:
        result = race_writer(db, monkeypatch, loser, lambda: serving.apply_update(db, winner))
        assert result["result"] == "duplicate_ignored"
    else:
        with pytest.raises(serving.ConflictError):
            race_writer(db, monkeypatch, loser, lambda: serving.apply_update(db, winner))
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert [v["visit_id"] for v in visible["visits"]] == [winner["visits"][0]["visit_id"]]
    assert visible["total"] == visible["cohorts"][0]["visit_count"] == 1
    assert visible["cohorts"][0]["emitted_at"] == winner["emitted_at"]
    assert {v["emitted_at"] for v in visible["visits"]} == {winner["emitted_at"]}
    assert serving.apply_update(db, winner)["result"] == "duplicate_ignored"


def test_initial_insert_race_becomes_content_conflict(db, monkeypatch):
    first = update()
    winner = update(visits=first["visits"][:1])
    loser = update(visits=first["visits"][1:])
    with pytest.raises(serving.ConflictError):
        race_writer(db, monkeypatch, loser, lambda: serving.apply_update(db, winner),
                    method="insert_one", collection=serving.COHORTS)
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert [v["visit_id"] for v in visible["visits"]] == [first["visits"][0]["visit_id"]]


def test_equal_payloads_from_different_bases_do_not_share_staged_rows(db, monkeypatch):
    first = update()
    serving.apply_update(db, first)
    late = update(3, visits=first["visits"][:1], mode="upsert")

    def advance():
        serving.apply_update(db, update(2, visits=[], mode="delete",
                                        deleted=[first["visits"][1]["visit_id"]]))
        serving.apply_update(db, late)

    assert race_writer(db, monkeypatch, late, advance)["result"] == "duplicate_ignored"
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert [v["visit_id"] for v in visible["visits"]] == [first["visits"][0]["visit_id"]]
    assert visible["total"] == visible["cohorts"][0]["visit_count"] == 1


def test_newer_cas_loser_requires_retry_instead_of_becoming_stale(db, monkeypatch):
    first = update()
    serving.apply_update(db, first)
    later = update(3, visits=first["visits"][:1], mode="upsert")
    winner = update(2, visits=[], mode="delete", deleted=[first["visits"][1]["visit_id"]])
    with pytest.raises(serving.ConcurrentUpdateError, match="retry"):
        race_writer(db, monkeypatch, later, lambda: serving.apply_update(db, winner))
    assert serving.Reader(db).provisional(serving.VisitQuery())["cohorts"][0]["seq"] == 2
    assert serving.apply_update(db, later)["result"] == "applied"
    visible = serving.Reader(db).provisional(serving.VisitQuery())
    assert visible["cohorts"][0]["seq"] == 3
    assert [v["visit_id"] for v in visible["visits"]] == [first["visits"][0]["visit_id"]]


def test_reader_keeps_rows_pinned_before_pointer_change(db, monkeypatch):
    first = update()
    serving.apply_update(db, first)
    entered, resume = threading.Event(), threading.Event()
    original = Collection.find

    def pause_rows(self, *args, **kwargs):
        if (self.database.name == db.name and self.name == serving.PROVISIONAL
                and threading.current_thread().name.startswith("pinned_reader")):
            entered.set()
            if not resume.wait(timeout=15):
                raise AssertionError("concurrent test did not release its pinned reader")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Collection, "find", pause_rows)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="pinned_reader") as pool:
        future = pool.submit(serving.Reader(db).provisional, serving.VisitQuery())
        try:
            assert entered.wait(timeout=10)
            serving.apply_update(db, update(2, visits=first["visits"][:1]))
        finally:
            resume.set()
        pinned = future.result(timeout=10)
    assert pinned["cohorts"][0]["seq"] == 1
    assert {v["visit_id"] for v in pinned["visits"]} == {v["visit_id"] for v in first["visits"]}
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 1


@pytest.mark.parametrize("stage", ["initialization", "completion"])
def test_stale_publisher_cannot_overwrite_newer_generation_on_real_server(db, monkeypatch, stage):
    artifact = serving.load_artifact(FIXTURES / "generation_a")
    method = "replace_one" if stage == "initialization" else "update_one"
    original = getattr(Collection, method)
    entered, resume = threading.Event(), threading.Event()

    def pause(self, *args, **kwargs):
        if (self.database.name == db.name and self.name == serving.GENERATIONS
                and threading.current_thread().name.startswith("paused_publisher")):
            entered.set()
            if not resume.wait(timeout=15):
                raise AssertionError("concurrent test did not release its stale publisher")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Collection, method, pause)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="paused_publisher") as pool:
        future = pool.submit(serving.publish, db, artifact, owner="slow")
        try:
            assert entered.wait(timeout=10)
            db[serving.LEASES].update_one({"_id": "publisher"},
                                         {"$set": {"expires_at": "2000-01-01T00:00:00+00:00"}})
            if stage == "initialization":
                serving.publish(db, artifact, owner="fast")
            else:
                with pytest.raises(RuntimeError, match="simulated"):
                    serving.publish(db, artifact, owner="fast", fail_after_visits=2)
            winner = db[serving.GENERATIONS].find_one({"_id": artifact.generation_id})
        finally:
            resume.set()
        with pytest.raises(serving.FencedError):
            future.result(timeout=10)
    assert db[serving.GENERATIONS].find_one({"_id": artifact.generation_id}) == winner
    if stage == "initialization":
        assert serving.Reader(db).visits(serving.VisitQuery())["total"] == len(artifact.visits)
    else:
        assert winner["status"] == "loading" and winner["fence_token"] == 2
        assert "completed_at" not in winner
