"""Pure, bounded-memory reference visit state machine."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timedelta

from .contracts import Observation, Visit, digest


@dataclass(frozen=True)
class Rules:
    confirm_minutes: int = 10
    exit_minutes: int = 10
    max_gap_minutes: int = 30
    version: str = "visits-v1"


class VisitMachine:
    def __init__(self, mmsi: str, port_id: str, rules: Rules):
        self.mmsi, self.port_id, self.rules = mmsi, port_id, rules
        self.last_outside = None
        self.previous_time = None
        self.previous_logical = None
        self._reset()

    def _reset(self):
        self.first_inside = None
        self.last_inside = None
        self.entry_after = None
        self.left_censored = False
        self.confirmed = False
        self.pending_exit = None
        self.count = 0
        self.max_gap = 0
        self.flags: set[str] = set()
        self.source_ids: list[str] = []
        self.source_count = 0
        self.source_hash = hashlib.sha256()

    def _record(self, obs: Observation):
        self.count += 1
        self.source_count += 1
        self.source_hash.update(obs.source_record_id.encode())
        if len(self.source_ids) < 16:
            self.source_ids.append(obs.source_record_id)
        elif self.source_count == 17:
            self.flags.add("source_ids_sampled")
        self.flags.update(obs.quality_flags)
        self.flags.update(obs.assignment_flags)

    def _finish(self, exit_by=None, extra_flags=()) -> Visit | None:
        if self.first_inside is None:
            return None
        flags = self.flags | set(extra_flags)
        if not self.confirmed:
            flags.add("insufficient_confirmation")
        right_censored = exit_by is None
        if self.left_censored:
            flags.add("left_censored")
        if right_censored:
            flags.add("right_censored")
        result = Visit(
            visit_id=digest(self.mmsi, self.port_id, self.first_inside.isoformat(), self.rules.version),
            mmsi=self.mmsi, port_id=self.port_id,
            first_inside=self.first_inside.isoformat(), last_inside=self.last_inside.isoformat(),
            possible_entry_after=self.entry_after.isoformat() if self.entry_after else None,
            possible_entry_by=self.first_inside.isoformat(),
            possible_exit_after=self.last_inside.isoformat(),
            possible_exit_by=exit_by.isoformat() if exit_by else None,
            observed_span_seconds=int((self.last_inside - self.first_inside).total_seconds()),
            observation_count=self.count, maximum_gap_seconds=self.max_gap,
            left_censored=self.left_censored, right_censored=right_censored,
            quality_flags=tuple(sorted(flags)), source_record_ids=tuple(self.source_ids),
            source_record_count=self.source_count, source_digest=self.source_hash.hexdigest(),
            rule_version=self.rules.version,
            status="reconciled" if self.confirmed else "candidate",
        )
        self._reset()
        return result

    def feed(self, obs: Observation) -> list[Visit]:
        if obs.mmsi != self.mmsi:
            raise ValueError("Machine received another MMSI")
        if self.previous_time and obs.event_time < self.previous_time:
            raise ValueError("Observations must be sorted by event time and stable ID")
        if obs.logical_id == self.previous_logical:
            return []
        emitted: list[Visit] = []
        if self.previous_time:
            gap = int((obs.event_time - self.previous_time).total_seconds())
            if self.first_inside is not None:
                self.max_gap = max(self.max_gap, gap)
            if gap > self.rules.max_gap_minutes * 60:
                ended = self._finish(extra_flags=("long_gap_unknown_continuity",))
                if ended:
                    emitted.append(ended)
                self.last_outside = None
        self.previous_time, self.previous_logical = obs.event_time, obs.logical_id
        if "same_time_position_conflict" in obs.quality_flags or "overlapping_port_polygons" in obs.assignment_flags:
            if self.first_inside is not None:
                self.flags.add("ambiguous_observation")
            return emitted
        inside = obs.assignment == self.port_id
        if inside:
            if self.first_inside is None:
                self.first_inside = self.last_inside = obs.event_time
                self.entry_after = self.last_outside
                self.left_censored = self.last_outside is None
            else:
                if self.pending_exit is not None:
                    self.flags.add("short_excursion_or_boundary_jitter")
                    self.pending_exit = None
                self.last_inside = obs.event_time
            self._record(obs)
            if (obs.event_time - self.first_inside) >= timedelta(minutes=self.rules.confirm_minutes):
                self.confirmed = True
            return emitted
        if self.first_inside is None:
            self.last_outside = obs.event_time
            return emitted
        if self.pending_exit is None:
            self.pending_exit = obs.event_time
        elif (obs.event_time - self.pending_exit) >= timedelta(minutes=self.rules.exit_minutes):
            exit_first = self.pending_exit
            ended = self._finish(exit_by=exit_first)
            if ended:
                emitted.append(ended)
            self.last_outside = obs.event_time
        return emitted

    def close(self) -> list[Visit]:
        ended = self._finish(extra_flags=("input_range_ended",))
        return [ended] if ended else []


def reconstruct(observations: list[Observation], ports: list[str], rules: Rules) -> list[Visit]:
    machines: dict[tuple[str, str], VisitMachine] = {}
    visits: list[Visit] = []
    for obs in sorted(observations, key=lambda x: (x.mmsi, x.event_time, x.logical_id, x.source_record_id)):
        for port in ports:
            key = obs.mmsi, port
            machine = machines.setdefault(key, VisitMachine(obs.mmsi, port, rules))
            visits.extend(machine.feed(obs))
    for key in sorted(machines):
        visits.extend(machines[key].close())
    return sorted(visits, key=lambda x: (x.mmsi, x.port_id, x.first_inside))
