"""Versioned identities and compact data contracts shared by adapters."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from typing import Any

CONTRACT_VERSION = "1.0"


def digest(*values: Any) -> str:
    return hashlib.sha256(json.dumps(values, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def utc_time(value: str | datetime) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if dt.tzinfo is None:
        raise ValueError("Naive event time is not accepted")
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class Observation:
    mmsi: str
    event_time: datetime
    longitude: float
    latitude: float
    source_file: str
    source_row: int
    source_record_id: str
    logical_id: str
    replay_id: str | None = None
    sog: float | None = None
    quality_flags: tuple[str, ...] = ()
    assignment: str | None = None
    assignment_flags: tuple[str, ...] = ()

    def as_json(self) -> dict:
        data = asdict(self)
        data["event_time"] = self.event_time.isoformat()
        return data

    @classmethod
    def from_json(cls, data: dict) -> "Observation":
        allowed = {field.name for field in fields(cls)}
        item = {key: value for key, value in data.items() if key in allowed}
        item["event_time"] = utc_time(item["event_time"])
        item["quality_flags"] = tuple(item.get("quality_flags") or ())
        item["assignment_flags"] = tuple(item.get("assignment_flags") or ())
        return cls(**item)


@dataclass(frozen=True)
class Visit:
    visit_id: str
    mmsi: str
    port_id: str
    first_inside: str
    last_inside: str
    possible_entry_after: str | None
    possible_entry_by: str
    possible_exit_after: str
    possible_exit_by: str | None
    observed_span_seconds: int
    observation_count: int
    maximum_gap_seconds: int
    left_censored: bool
    right_censored: bool
    quality_flags: tuple[str, ...]
    source_record_ids: tuple[str, ...]
    source_record_count: int
    source_digest: str
    rule_version: str
    status: str = "reconciled"

    def as_json(self) -> dict:
        return asdict(self)
