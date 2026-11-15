"""Batch engine checks on small committed fixtures (synthetic unless stated)."""
import json
from dataclasses import fields
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from harbortrace import engine  # noqa: E402
from harbortrace.contracts import Observation, Visit  # noqa: E402
from harbortrace.serving import ValidationError, load_artifact  # noqa: E402
from harbortrace.visits import Rules, reconstruct  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures"
CANONICAL = FIXTURES / "canonical/week06_canonical.jsonl"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = engine.engine_session("harbortrace-engine-tests", shuffle_partitions=2,
                                    warehouse=str(tmp_path_factory.mktemp("wh")))
    yield session
    session.stop()


def rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def reference(observations: list[dict], ports: list[str]) -> list[dict]:
    visits = reconstruct([Observation.from_json(row) for row in observations], ports, Rules())
    return [json.loads(json.dumps(visit.as_json())) for visit in visits]


def moving_vessels(vessels: int = 9) -> list[dict]:
    """Synthetic vessels covering confirmation, exit, a long gap, censoring and a candidate."""
    out, row = [], 0
    for vessel in range(vessels):
        mmsi = f"99{vessel:07d}"
        pattern = [None, "P1", "P1", "P1", None, None, "P2", "P2", None, "P1"][: 4 + vessel]
        minute = vessel
        for step, port in enumerate(pattern):
            minute += 45 if (vessel % 3 == 1 and step == 3) else 6
            row += 1
            time = f"2024-01-01T{minute // 60:02d}:{minute % 60:02d}:00+00:00"
            out.append({"mmsi": mmsi, "event_time": time, "longitude": 0.0, "latitude": 0.0,
                        "source_file": "synthetic", "source_row": row, "source_record_id": f"{row:064x}",
                        "logical_id": f"{row + 10_000:064x}", "replay_id": None, "sog": 1.0,
                        "quality_flags": [], "assignment": port, "assignment_flags": []})
    return out


def test_visit_schema_matches_the_contract():
    from pyspark.sql import types as T  # noqa: F401
    declared = [part.strip().split(" ")[0] for part in engine.VISIT_DDL.split(",")]
    assert declared == [field.name for field in fields(Visit)]


def test_fixture_artifact_equals_reference_and_is_accepted_by_serving(spark, tmp_path):
    output = tmp_path / "artifact"
    manifest = engine.run_batch(spark, [str(CANONICAL)], output, run_id="fixture", ports=["9001"], synthetic=True)
    assert rows(output / "visits.jsonl") == reference(rows(CANONICAL), ["9001"])
    assert manifest["visit_count"] == 2 and manifest["status_counts"] == {"reconciled": 2}
    assert manifest["source"]["input_rows"] == 7 and manifest["synthetic"] is True
    artifact = load_artifact(output)  # the serving component's own validation
    assert len(artifact.visits) == 2 and artifact.manifest["run_id"] == "fixture"
    assert not (tmp_path / ".artifact.partial").exists()


def test_output_is_independent_of_partitioning_and_input_order(spark, tmp_path):
    observations = moving_vessels()
    forward, backward = tmp_path / "forward.jsonl", tmp_path / "backward.jsonl"
    forward.write_text("".join(json.dumps(row) + "\n" for row in observations))
    backward.write_text("".join(json.dumps(row) + "\n" for row in reversed(observations)))
    one = engine.run_batch(spark, [str(forward)], tmp_path / "one", run_id="r", ports=["P1", "P2"],
                           synthetic=True, partitions=1)
    many = engine.run_batch(spark, [str(backward)], tmp_path / "many", run_id="r", ports=["P1", "P2"],
                            synthetic=True, partitions=5)
    assert one["visits_sha256"] == many["visits_sha256"]
    produced = rows(tmp_path / "many/visits.jsonl")
    assert produced == reference(observations, ["P1", "P2"])
    assert {"reconciled", "candidate"} <= {row["status"] for row in produced}
    assert any("long_gap_unknown_continuity" in row["quality_flags"] for row in produced)
    load_artifact(tmp_path / "many")


def test_real_excerpt_is_reconstructed(spark, tmp_path):
    excerpt = FIXTURES / "real_ais_excerpt.jsonl"
    manifest = engine.run_batch(spark, [str(excerpt)], tmp_path / "real", run_id="excerpt",
                                ports=["4120", "4110"], synthetic=False)
    assert rows(tmp_path / "real/visits.jsonl") == reference(rows(excerpt), ["4120", "4110"])
    assert manifest["synthetic"] is False


def test_existing_artifact_is_never_overwritten(spark, tmp_path):
    output = tmp_path / "artifact"
    engine.run_batch(spark, [str(CANONICAL)], output, run_id="first", ports=["9001"], synthetic=True)
    before = (output / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        engine.run_batch(spark, [str(CANONICAL)], output, run_id="second", ports=["9001"], synthetic=True)
    assert (output / "manifest.json").read_bytes() == before


@pytest.mark.parametrize("mutate, message", [
    (lambda row: row.update(mmsi=None), "not canonical"),
    (lambda row: row.update(event_time="yesterday"), "not canonical"),
    (lambda row: row.pop("logical_id"), "not canonical"),
])
def test_non_canonical_input_publishes_nothing(spark, tmp_path, mutate, message):
    observations = rows(CANONICAL)
    mutate(observations[2])
    source = tmp_path / "broken.jsonl"
    source.write_text("".join(json.dumps(row) + "\n" for row in observations))
    with pytest.raises(engine.EngineError, match=message):
        engine.run_batch(spark, [str(source)], tmp_path / "artifact", run_id="broken", ports=["9001"], synthetic=True)
    assert list(tmp_path.glob("*artifact*")) == []


def test_no_visits_publishes_nothing(spark, tmp_path):
    with pytest.raises(engine.EngineError, match="No visits"):
        engine.run_batch(spark, [str(CANONICAL)], tmp_path / "artifact", run_id="empty", ports=["0000"], synthetic=True)
    assert list(tmp_path.glob("*artifact*")) == []


def test_interrupted_write_leaves_no_artifact(spark, tmp_path, monkeypatch):
    def fail(row):
        raise OSError("disk full")
    monkeypatch.setattr(engine, "_visit_line", fail)
    with pytest.raises(OSError):
        engine.run_batch(spark, [str(CANONICAL)], tmp_path / "artifact", run_id="crash", ports=["9001"], synthetic=True)
    assert list(tmp_path.glob("*artifact*")) == []
    with pytest.raises(ValidationError):
        load_artifact(tmp_path / "artifact")
