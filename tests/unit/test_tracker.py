"""The sparse per-vessel tracker must equal one reference machine per port (synthetic input)."""
import hashlib
import random
from datetime import datetime, timedelta, timezone

import pytest

from harbortrace.contracts import Observation
from harbortrace.visits import Rules, VesselTracker, reconstruct

PORTS = [f"P{index}" for index in range(1, 8)]
RULES = Rules()


def track(seed: int, steps: int = 400) -> list[Observation]:
    """Random movement between seven ports with gaps, ties, repeats and ambiguous points."""
    rng = random.Random(seed)
    mmsi, clock, port, rows = f"99{seed:07d}", datetime(2024, 1, 1, tzinfo=timezone.utc), None, []
    for step in range(steps):
        clock += timedelta(minutes=rng.choice([0, 1, 2, 4, 6, 9, 11, 45]), seconds=rng.randint(0, 50))
        if rng.random() < 0.25:
            port = rng.choice([None, None] + PORTS + ["UNLISTED"])
        roll = rng.random()
        rows.append(Observation(
            mmsi=mmsi, event_time=clock, longitude=0.0, latitude=0.0, source_file="synthetic", source_row=step,
            source_record_id=hashlib.sha256(f"s{seed}-{step}".encode()).hexdigest(),
            logical_id=hashlib.sha256(f"l{seed}-{step}".encode()).hexdigest(),
            quality_flags=("same_time_position_conflict",) if roll < 0.03 else (),
            assignment=None if 0.03 <= roll < 0.07 else port,
            assignment_flags=("overlapping_port_polygons",) if 0.03 <= roll < 0.07 else
            (("near_boundary",) if roll > 0.9 else ())))
        if rng.random() < 0.05:
            rows.append(rows[-1])  # an adjacent repeat of the same logical observation
    return rows


def tracked(observations: list[Observation], ports) -> list:
    tracker, visits, largest = VesselTracker(observations[0].mmsi, RULES, ports), [], 0
    for observation in sorted(observations, key=lambda x: (x.event_time, x.logical_id, x.source_record_id)):
        visits.extend(tracker.feed(observation))
        largest = max(largest, len(tracker.active))
    visits.extend(tracker.close())
    return sorted(visits, key=lambda x: (x.mmsi, x.port_id, x.first_inside)), largest


@pytest.mark.parametrize("seed", range(40))
def test_tracker_equals_one_machine_per_port(seed):
    observations = track(seed)
    expected = reconstruct(observations, PORTS, RULES)
    actual, largest = tracked(observations, PORTS)
    assert actual == expected and len(expected) > 5
    assert largest < len(PORTS)  # machines exist only for ports with an open visit


def test_tracker_without_a_port_list_accepts_every_assigned_port():
    observations = track(99)
    actual, _ = tracked(observations, None)
    assert actual == reconstruct(observations, PORTS + ["UNLISTED"], RULES)
    assert any(visit.port_id == "UNLISTED" for visit in actual)
