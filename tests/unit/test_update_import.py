"""Engine update directories enter serving without importing Spark."""
import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import mongomock
import pytest

from harbortrace import serving, update_import

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/serving"


@pytest.fixture
def updates():
    steps = json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"]
    return [copy.deepcopy(steps[index]["update"]) for index in (0, 2, 6)]


@pytest.fixture
def db():
    return mongomock.MongoClient()["update_import_test"]


def batch(root, number, updates, complete=True):
    directory = root / f"batch={number}"
    directory.mkdir(parents=True)
    (directory / "part-00000").write_text("".join(json.dumps(row) + "\n" for row in updates))
    if complete:
        (directory / "_SUCCESS").touch()
    return directory


def test_numeric_batch_order_and_restart_receipts(db, tmp_path, updates):
    batch(tmp_path / "updates", "10", [updates[1]])
    batch(tmp_path / "updates", "2", [updates[0]])
    first = update_import.apply_directory(db, tmp_path, source_id="engine-test")
    assert first["batches_applied"] == 2
    assert first["updates"] == {"applied": 2}
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 3
    before = list(db[serving.COHORTS].find())
    retry = update_import.apply_directory(db, tmp_path / "updates", source_id="engine-test")
    assert retry["batches_reused"] == 2
    assert retry["batches_applied"] == 0
    assert list(db[serving.COHORTS].find()) == before


def test_incomplete_batch_blocks_later_batches(db, tmp_path, updates):
    directory = batch(tmp_path, "0", [updates[0]], complete=False)
    batch(tmp_path, "1", [updates[1]])
    result = update_import.apply_directory(db, tmp_path, source_id="incomplete")
    assert result["batches_applied"] == 0
    assert result["pending_batches"] == [0, 1]
    assert db[serving.COHORTS].count_documents({}) == 0
    (directory / "_SUCCESS").touch()
    assert update_import.apply_directory(db, tmp_path, source_id="incomplete")["batches_applied"] == 2


def test_changed_completed_batch_is_rejected(db, tmp_path, updates):
    directory = batch(tmp_path, "0", [updates[0]])
    update_import.apply_directory(db, tmp_path, source_id="immutable")
    (directory / "part-00000").write_text(json.dumps(updates[2]) + "\n")
    with pytest.raises(serving.ConflictError, match="changed"):
        update_import.apply_directory(db, tmp_path, source_id="immutable")
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 2


def test_whole_batch_is_validated_before_first_update(db, tmp_path, updates):
    broken = copy.deepcopy(updates[1])
    broken["visits"][0].pop("source_digest")
    batch(tmp_path, "0", [updates[0], broken])
    with pytest.raises(serving.ValidationError, match="missing fields"):
        update_import.apply_directory(db, tmp_path, source_id="invalid")
    assert db[serving.COHORTS].count_documents({}) == 0
    assert db[update_import.RECEIPTS].count_documents({}) == 0


@pytest.mark.parametrize("failure", [RuntimeError, serving.ConcurrentUpdateError])
def test_restart_after_partial_batch_retries_unreceipted_updates(db, tmp_path, updates, monkeypatch, failure):
    batch(tmp_path, "0", updates[:2])
    original = serving.apply_update

    def fail_on_second(database, update):
        if update["seq"] == 2:
            raise failure("interrupted import")
        return original(database, update)

    with monkeypatch.context() as patch:
        patch.setattr(serving, "apply_update", fail_on_second)
        with pytest.raises(RuntimeError, match="interrupted"):
            update_import.apply_directory(db, tmp_path, source_id="restart")
    assert db[update_import.RECEIPTS].count_documents({}) == 0
    result = update_import.apply_directory(db, tmp_path, source_id="restart")
    assert result["updates"] == {"duplicate_ignored": 1, "applied": 1}
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 3


def test_conflicting_sequence_does_not_receive_a_receipt(db, tmp_path, updates):
    serving.apply_update(db, updates[0])
    conflict = copy.deepcopy(updates[0])
    conflict["visits"] = conflict["visits"][:1]
    batch(tmp_path, "0", [conflict])
    with pytest.raises(serving.ConflictError):
        update_import.apply_directory(db, tmp_path, source_id="conflict")
    assert db[update_import.RECEIPTS].count_documents({}) == 0
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 2


def test_stale_update_is_reported_without_reverting_cohort(db, tmp_path, updates):
    serving.apply_update(db, updates[2])
    batch(tmp_path, "0", [updates[0]])
    result = update_import.apply_directory(db, tmp_path, source_id="old")
    assert result["updates"] == {"stale": 1}
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 1


@pytest.mark.parametrize("damage", ["json", "no_parts", "duplicate_batch"])
def test_damaged_directory_fails_closed(db, tmp_path, updates, damage):
    directory = batch(tmp_path, "0", [updates[0]])
    if damage == "json":
        (directory / "part-00000").write_text('{"broken":\n')
    elif damage == "no_parts":
        (directory / "part-00000").unlink()
    else:
        batch(tmp_path, "0000", [updates[0]])
    with pytest.raises(serving.ValidationError):
        update_import.apply_directory(db, tmp_path, source_id="damaged")
    assert db[serving.COHORTS].count_documents({}) == 0


def test_directory_command_runs_without_spark(tmp_path, updates):
    batch(tmp_path, "0", [updates[0]])
    root = FIXTURES.parents[2]
    environment = {**os.environ, "PYTHONPATH": str(root / "src")}
    result = subprocess.run([sys.executable, str(root / "scripts/serving_import.py"),
                             "--memory", "apply-directory", str(tmp_path), "--source-id", "cli-test"],
                            env=environment, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["updates"] == {"applied": 1}
    assert report["batches_applied"] == 1


@pytest.mark.parametrize("change", ["truncated", "marker_replaced", "part_added"])
def test_rewriting_completed_batch_remains_pending(db, tmp_path, updates, monkeypatch, change):
    directory = batch(tmp_path, "0", updates[:2])
    part = directory / "part-00000"
    payload = part.read_bytes()
    original = Path.read_bytes
    changed = False

    def rewrite_during_read(path):
        nonlocal changed
        if path == part and not changed:
            changed = True
            if change == "truncated":
                (directory / "_SUCCESS").unlink()
                part.write_text(json.dumps(updates[0]) + "\n")
            elif change == "marker_replaced":
                (directory / "_SUCCESS").unlink()
                (directory / "_SUCCESS").touch()
            else:
                (directory / "part-00001").write_text(json.dumps(updates[2]) + "\n")
        return original(path)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", rewrite_during_read)
        result = update_import.apply_directory(db, tmp_path, source_id="rewritten")
    assert result["batches_applied"] == 0
    assert result["pending_batches"] == [0]
    assert db[serving.COHORTS].count_documents({}) == 0
    assert db[update_import.RECEIPTS].count_documents({}) == 0
    part.write_bytes(payload)
    (directory / "part-00001").unlink(missing_ok=True)
    (directory / "_SUCCESS").touch()
    assert update_import.apply_directory(db, tmp_path, source_id="rewritten")["batches_applied"] == 1
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 3


def test_same_size_part_rewrite_with_unchanged_metadata_remains_pending(db, tmp_path, updates, monkeypatch):
    directory = batch(tmp_path, "0", [updates[0]])
    batch(tmp_path, "1", [updates[1]])
    part = directory / "part-00000"
    original_stat = Path.stat
    original_read = Path.read_bytes
    initial_stat = part.stat()
    payload = part.read_bytes()
    replacement = payload.replace(b"2026-11-02", b"2026-11-03", 1)
    assert replacement != payload and len(replacement) == len(payload)

    def coarse_stat(path, *args, **kwargs):
        if path == part:
            return initial_stat
        return original_stat(path, *args, **kwargs)

    def read_then_overwrite(path):
        data = original_read(path)
        if path == part and data == payload:
            part.write_bytes(replacement)
        return data

    with monkeypatch.context() as patch:
        patch.setattr(Path, "stat", coarse_stat)
        patch.setattr(Path, "read_bytes", read_then_overwrite)
        result = update_import.apply_directory(db, tmp_path, source_id="coarse-clock")

    assert result["pending_batches"] == [0, 1]
    assert db[serving.COHORTS].count_documents({}) == 0
    assert db[update_import.RECEIPTS].count_documents({}) == 0
    assert update_import.apply_directory(db, tmp_path, source_id="coarse-clock")["batches_applied"] == 2


def test_spark_retry_can_rename_and_reorder_identical_update_parts(db, tmp_path, updates):
    directory = batch(tmp_path, "0", updates[:2])
    update_import.apply_directory(db, tmp_path, source_id="spark-retry")
    (directory / "part-00000").unlink()
    renamed = directory / "part-00000-new-job-uuid-c000.txt"
    renamed.write_text("\n".join(json.dumps(row, indent=None) for row in reversed(updates[:2])) + "\n")
    retry = update_import.apply_directory(db, tmp_path, source_id="spark-retry")
    assert retry["batches_reused"] == 1 and retry["batches_applied"] == 0
    assert serving.Reader(db).provisional(serving.VisitQuery())["total"] == 3


def test_spark_retry_emission_time_is_not_a_new_payload(db, tmp_path, updates):
    directory = batch(tmp_path, "0", [updates[0]])
    update_import.apply_directory(db, tmp_path, source_id="new-attempt")
    retry = copy.deepcopy(updates[0])
    retry["emitted_at"] = "2026-11-20T00:00:00+00:00"
    (directory / "part-00000").write_text(json.dumps(retry) + "\n")
    result = update_import.apply_directory(db, tmp_path, source_id="new-attempt")
    assert result["batches_reused"] == 1
    cohort = db[serving.COHORTS].find_one({"_id": retry["cohort_id"]})
    assert cohort["emitted_at"] == updates[0]["emitted_at"]
