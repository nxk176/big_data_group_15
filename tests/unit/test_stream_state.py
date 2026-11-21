"""Bounded streaming state checks (pure Python, synthetic input)."""
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone

import pytest

from harbortrace.contracts import Observation
from harbortrace.replay import replay_messages
from harbortrace.stream_state import (OpenSslSha256, PythonSha256, ResumableSha256, StreamMachine,
                                      VesselStream, visit_json)
from harbortrace.visits import Rules, reconstruct

PORTS = ("P1", "P2")
RULES = Rules()
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
LATENESS = timedelta(minutes=10)
NOW = "2026-11-12T00:00:00+00:00"


def vessel_observations(mmsi: str, seed: int, steps: int = 220) -> list[dict]:
    """A synthetic track: port stays, short excursions, exits and occasional long gaps."""
    rng = random.Random(seed)
    rows, clock, port = [], START + timedelta(minutes=rng.randint(0, 30)), None
    for step in range(steps):
        clock += timedelta(minutes=45 if rng.random() < 0.03 else rng.randint(1, 7), seconds=rng.randint(0, 59))
        if rng.random() < 0.12:
            port = rng.choice([None, None, "P1", "P2"])
        row = len(rows)
        rows.append({"mmsi": mmsi, "event_time": clock.isoformat(), "longitude": 0.0, "latitude": 0.0,
                     "source_file": "synthetic", "source_row": row,
                     "source_record_id": hashlib.sha256(f"{mmsi}-{row}".encode()).hexdigest(),
                     "logical_id": hashlib.sha256(f"L-{mmsi}-{row}".encode()).hexdigest(), "replay_id": None,
                     "sog": 1.0, "quality_flags": [], "assignment": port,
                     "assignment_flags": ["near_boundary"] if rng.random() < 0.05 else []})
    return rows


def reference(messages: list[dict]) -> dict[str, dict]:
    visits = reconstruct([Observation.from_json(row) for row in messages], list(PORTS), RULES)
    return {visit.visit_id: visit_json(visit) for visit in visits}


def roundtrip(vessel: VesselStream, **kwargs) -> VesselStream:
    """Save and restore through JSON, as the Spark state store does between microbatches."""
    state = json.loads(json.dumps(vessel.export()))
    return VesselStream(vessel.mmsi, PORTS, RULES, stream_id="test", state=state, **kwargs)


def apply(view: dict, update: dict | None) -> None:
    if update:
        assert update["mode"] == "upsert" and not update["deleted_visit_ids"]
        view.update({visit["visit_id"]: visit for visit in update["visits"]})


def sha256_implementations():
    return [PythonSha256] + ([OpenSslSha256] if ResumableSha256 is OpenSslSha256 else [])


@pytest.mark.parametrize("implementation", sha256_implementations())
def test_resumable_sha256_matches_hashlib_across_saved_states(implementation):
    rng = random.Random(7)
    for _ in range(40):
        parts = [rng.randbytes(rng.choice([0, 1, 55, 56, 63, 64, 65, 200])) for _ in range(rng.randint(0, 9))]
        resumable = implementation()
        for part in parts:
            resumable.update(part)
            assert resumable.hexdigest() == resumable.hexdigest()  # reading the digest changes nothing
            resumable = implementation.restore(json.loads(json.dumps(resumable.export())))
        assert resumable.hexdigest() == hashlib.sha256(b"".join(parts)).hexdigest()


def test_sha256_state_moves_between_implementations():
    """A state saved on a machine with OpenSSL must restore on one without it, and back."""
    implementations = sha256_implementations()
    rng = random.Random(11)
    for _ in range(40):
        parts = [rng.randbytes(rng.choice([0, 1, 55, 56, 63, 64, 65, 200])) for _ in range(rng.randint(1, 9))]
        resumable = implementations[0]()
        for index, part in enumerate(parts):
            resumable.update(part)
            resumable = implementations[(index + 1) % len(implementations)].restore(resumable.export())
        assert resumable.hexdigest() == hashlib.sha256(b"".join(parts)).hexdigest()


def test_preview_leaves_the_open_visit_untouched():
    machine = StreamMachine("990000001", "P1", RULES)
    for observation in vessel_observations("990000001", seed=5)[:40]:
        machine.feed(Observation.from_json(observation))
        before = json.dumps(machine.export(), sort_keys=True)
        machine.preview()
        assert json.dumps(machine.export(), sort_keys=True) == before


def test_every_released_prefix_equals_batch_reconstruction():
    messages = replay_messages(vessel_observations("990000001", seed=1), "run", START, seed=3, duplicate_copies=12)
    vessel, view, seen, high = VesselStream("990000001", PORTS, RULES, stream_id="test"), {}, [], None
    sequences = []
    for start in range(0, len(messages), 9):
        chunk = messages[start:start + 9]
        seen += chunk
        high = max(datetime.fromisoformat(row["event_time"]) for row in seen)
        update = vessel.process(chunk, high - LATENESS, NOW)
        apply(view, update)
        if update:
            sequences.append(update["seq"])
        vessel = roundtrip(vessel)
        released = [row for row in seen if vessel.frontier and datetime.fromisoformat(row["event_time"]) <= vessel.frontier]
        assert view == reference(released)
    assert sequences == list(range(1, len(sequences) + 1)) and len(sequences) > 5
    apply(view, vessel.process([], None, NOW, flush=True))
    assert view == reference(messages)
    assert vessel.counters["duplicates"] == 12 and vessel.counters["late"] == 0
    assert {"reconciled", "candidate"} <= {visit["status"] for visit in view.values()}
    assert any("long_gap_unknown_continuity" in visit["quality_flags"] for visit in view.values())


def test_arrival_disorder_within_lateness_does_not_change_the_result():
    observations = vessel_observations("990000002", seed=2)
    arrivals = list(observations)
    rng = random.Random(11)
    for index in range(0, len(arrivals) - 3, 3):  # local shuffles spanning less than the lateness
        window = arrivals[index:index + 3]
        rng.shuffle(window)
        arrivals[index:index + 3] = window
    vessel, view, high = VesselStream("990000002", PORTS, RULES, stream_id="test"), {}, START
    for row in arrivals:
        high = max(high, datetime.fromisoformat(row["event_time"]))
        apply(view, vessel.process([row], high - timedelta(minutes=60), NOW))
        vessel = roundtrip(vessel)
    apply(view, vessel.process([], None, NOW, flush=True))
    assert view == reference(observations)
    assert vessel.counters["late"] == 0


def test_observation_older_than_the_released_frontier_is_counted_and_dropped():
    observations = vessel_observations("990000003", seed=3, steps=60)
    late, rest = observations[10], observations[:10] + observations[11:]
    vessel, view = VesselStream("990000003", PORTS, RULES, stream_id="test"), {}
    apply(view, vessel.process(rest, datetime.fromisoformat(rest[-1]["event_time"]), NOW))
    assert vessel.process([late], datetime.fromisoformat(rest[-1]["event_time"]), NOW) is None
    assert vessel.counters["late"] == 1
    assert view == reference(rest)


def test_state_stays_bounded_for_a_vessel_that_never_leaves():
    """The earlier prototype kept every message and failed above 5,000 per vessel."""
    vessel, sizes, view = VesselStream("990000004", PORTS, RULES, stream_id="test"), [], {}
    rows = []
    for minute in range(20_000):
        when = START + timedelta(minutes=minute)
        rows.append({"mmsi": "990000004", "event_time": when.isoformat(), "assignment": "P1",
                     "logical_id": f"{minute:064x}", "source_record_id": f"{minute + 10 ** 9:064x}",
                     "quality_flags": [], "assignment_flags": []})
    for start in range(0, len(rows), 50):
        chunk = rows[start:start + 50]
        apply(view, vessel.process(chunk, datetime.fromisoformat(chunk[-1]["event_time"]) - LATENESS, NOW))
        vessel = roundtrip(vessel)
        sizes.append(len(json.dumps(vessel.export())))
    assert len(vessel.buffer) <= 10 and max(sizes) < 8_000
    assert max(sizes[-50:]) <= max(sizes[:50]) + 64  # counters gain digits; nothing accumulates
    (visit,) = view.values()
    assert visit["observation_count"] == 20_000 - len(vessel.buffer)
    digest = hashlib.sha256("".join(row["source_record_id"] for row in rows[:visit["observation_count"]]).encode())
    assert visit["source_digest"] == digest.hexdigest()


def test_buffer_cap_releases_early_instead_of_failing():
    observations = vessel_observations("990000005", seed=5, steps=120)
    vessel = VesselStream("990000005", PORTS, RULES, stream_id="test", max_buffer=20)
    update = vessel.process(observations, None, NOW)  # no watermark yet: nothing is releasable
    assert len(vessel.buffer) == 20 and vessel.counters["forced"] == 100
    assert update is None or update["visits"]


def test_idle_state_expires_and_a_returning_vessel_starts_a_new_cohort():
    first = vessel_observations("990000006", seed=6, steps=40)
    last = datetime.fromisoformat(first[-1]["event_time"])
    vessel, view = VesselStream("990000006", PORTS, RULES, stream_id="test"), {}
    apply(view, vessel.process(first, last, NOW))
    assert not vessel.expired(last + timedelta(minutes=RULES.max_gap_minutes))
    assert vessel.expired(last + timedelta(minutes=RULES.max_gap_minutes, seconds=1))
    assert vessel.wake_at() == last + timedelta(minutes=RULES.max_gap_minutes)
    later = [dict(row, event_time=(datetime.fromisoformat(row["event_time"]) + timedelta(days=1)).isoformat(),
                  logical_id=row["logical_id"][::-1], source_record_id=row["source_record_id"][::-1])
             for row in first]
    returned = VesselStream("990000006", PORTS, RULES, stream_id="test")  # state was dropped
    update = returned.process(later, datetime.fromisoformat(later[-1]["event_time"]), NOW)
    apply(view, update)
    assert update["cohort_id"] != vessel.cohort_id and update["seq"] == 1

    # A visit still open when the state expired was reported as if input ended there. Batch,
    # which sees the later data, closes it at the long gap instead: it carries another end
    # flag and counts that gap in maximum_gap_seconds. Nothing else may differ.
    expected, differing = reference(first + later), 0
    assert set(view) == set(expected)
    for key, visit in expected.items():
        fields = {name for name in visit if visit[name] != view[key][name]}
        assert fields <= {"quality_flags", "maximum_gap_seconds"}
        if fields:
            differing += 1
            assert set(view[key]["quality_flags"]) ^ set(visit["quality_flags"]) == {
                "input_range_ended", "long_gap_unknown_continuity"}
    assert differing <= len(PORTS)


def test_machine_state_roundtrip_preserves_behaviour():
    observations = [Observation.from_json(row) for row in vessel_observations("990000007", seed=8)]
    plain, saved = StreamMachine("990000007", "P1", RULES), StreamMachine("990000007", "P1", RULES)
    for observation in observations:
        expected = plain.feed(observation)
        saved = StreamMachine.restore("990000007", "P1", RULES, json.loads(json.dumps(saved.export())))
        assert saved.feed(observation) == expected
    assert saved.close() == plain.close()


def test_updates_are_accepted_by_the_serving_contract():
    mongomock = pytest.importorskip("mongomock")
    from harbortrace import serving
    messages = vessel_observations("990000008", seed=9)
    vessel, db, applied = VesselStream("990000008", PORTS, RULES, stream_id="test"), mongomock.MongoClient().db, 0
    for start in range(0, len(messages), 25):
        chunk = messages[start:start + 25]
        update = vessel.process(chunk, datetime.fromisoformat(chunk[-1]["event_time"]) - LATENESS, NOW)
        if update:
            assert serving.apply_update(db, update)["result"] == "applied"
            assert serving.apply_update(db, update)["result"] == "duplicate_ignored"  # sink retry
            applied += 1
    stored = {row["visit_id"] for row in db[serving.PROVISIONAL].find({"cohort_id": vessel.cohort_id})}
    released = [row for row in messages if datetime.fromisoformat(row["event_time"]) <= vessel.frontier]
    assert applied > 3 and stored == set(reference(released))


def test_stream_without_a_port_list_follows_every_assigned_port():
    messages = vessel_observations("990000009", seed=10)
    vessel, view = VesselStream("990000009", None, RULES, stream_id="test"), {}
    for start in range(0, len(messages), 20):
        chunk = messages[start:start + 20]
        apply(view, vessel.process(chunk, datetime.fromisoformat(chunk[-1]["event_time"]) - LATENESS, NOW))
        vessel = VesselStream("990000009", None, RULES, stream_id="test", state=json.loads(json.dumps(vessel.export())))
    apply(view, vessel.process([], None, NOW, flush=True))
    assert view == reference(messages) and {visit["port_id"] for visit in view.values()} == set(PORTS)
