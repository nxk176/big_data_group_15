import json
import random
import struct
from pathlib import Path

import pytest

from harbortrace.contracts import Observation, digest, utc_time
from harbortrace.quality import canonicalize, classify_same_time, point_from_wkb
from harbortrace.spatial import Port, assign
from harbortrace.visits import Rules, reconstruct


def obs(time, port, row, *, mmsi="123456789", flags=(), source=None):
    timestamp = utc_time(time)
    logical = digest(mmsi, timestamp.isoformat(), port, source or row)
    return Observation(mmsi, timestamp, -118.2, 33.75, "fixture", row,
                       digest("fixture", row), logical, assignment=port,
                       assignment_flags=tuple(flags))


@pytest.mark.parametrize("case", json.loads((Path(__file__).parents[1] / "fixtures" / "reference_cases.json").read_text()).keys())
def test_expected_cases(case):
    spec = json.loads((Path(__file__).parents[1] / "fixtures" / "reference_cases.json").read_text())[case]
    events = [obs(time, port, index) for index, (time, port) in enumerate(spec["events"])]
    actual = reconstruct(events, ["4120"], Rules())
    assert len(actual) == len(spec["expected"])
    for visit, expected in zip(actual, spec["expected"]):
        for key, value in expected.items():
            if key == "flag":
                assert value in visit.quality_flags
            else:
                assert getattr(visit, key) == value


def test_order_and_replay_invariance():
    events = [obs(t, p, i) for i, (t, p) in enumerate([
        ("2024-01-01T00:00:00Z", None), ("2024-01-01T00:05:00Z", "4120"),
        ("2024-01-01T00:15:00Z", "4120"), ("2024-01-01T00:20:00Z", None),
        ("2024-01-01T00:30:00Z", None)])]
    expected = reconstruct(events, ["4120"], Rules())
    duplicate = Observation.from_json({**events[2].as_json(), "replay_id": "replayed"})
    for seed in range(10):
        shuffled = events + [duplicate]
        random.Random(seed).shuffle(shuffled)
        assert reconstruct(shuffled, ["4120"], Rules()) == expected


def test_late_bridge_merges_two_censored_visits():
    base = [obs(t, "4120", i) for i, t in enumerate([
        "2024-01-01T00:05:00Z", "2024-01-01T00:15:00Z",
        "2024-01-01T00:50:00Z", "2024-01-01T01:00:00Z"])]
    assert len(reconstruct(base, ["4120"], Rules())) == 2
    repaired = reconstruct(base + [obs("2024-01-01T00:30:00Z", "4120", 9)], ["4120"], Rules())
    assert len(repaired) == 1
    assert repaired[0].observed_span_seconds == 3300


def test_late_outside_pair_splits_visit():
    base = [obs(t, "4120", i) for i, t in enumerate([
        "2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z",
        "2024-01-01T00:30:00Z", "2024-01-01T00:40:00Z"])]
    assert len(reconstruct(base, ["4120"], Rules())) == 1
    repaired = reconstruct(base + [obs("2024-01-01T00:15:00Z", None, 8), obs("2024-01-01T00:25:00Z", None, 9)], ["4120"], Rules())
    assert len(repaired) == 2


def test_same_time_conflict_is_not_duplicate():
    first = obs("2024-01-01T00:00:00Z", "4120", 0)
    other = obs("2024-01-01T00:00:00Z", None, 1)
    accepted, quarantine = classify_same_time([first, other])
    assert not accepted
    assert {row["reason"] for row in quarantine} == {"same_time_position_conflict"}


def test_wkb_and_quality():
    wkb = struct.pack("<BIdd", 1, 1, -118.2, 33.75)
    assert point_from_wkb(wkb) == (-118.2, 33.75)
    row = {"mmsi": 123456789, "base_date_time": "2024-01-01T00:00:00Z", "geometry": wkb, "sog": 2.5}
    assert canonicalize(row, "day.parquet", 0).longitude == -118.2
    with pytest.raises(ValueError, match="not_point_geometry"):
        point_from_wkb(struct.pack("<BI", 1, 2) + bytes(16))


def test_spatial_boundary_and_overlap():
    from shapely.geometry import box
    one = Port("4120", "Los Angeles", 2023, box(0, 0, 1, 1))
    two = Port("4110", "Long Beach", 2023, box(0.5, 0.5, 1.5, 1.5))
    assert assign(0, 0.2, [one], 100)[0] == "4120"
    assert "near_boundary" in assign(0, 0.2, [one], 100)[1]
    assert assign(0.75, 0.75, [one, two], 100) == (None, ("overlapping_port_polygons",))
