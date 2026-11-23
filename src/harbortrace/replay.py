"""Deterministic historical replay messages built from canonical observations.

A replay copy is transport evidence, not new vessel activity: every message keeps
the canonical fields and adds replay identity plus ingestion time.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Iterable

from .contracts import CONTRACT_VERSION, Observation, digest, utc_time

REPLAY_FIELDS = ("replay_run_id", "replay_id", "copy_index", "ingested_at")
CANONICAL_FIELDS = ("mmsi", "event_time", "longitude", "latitude", "source_file", "source_row",
                    "source_record_id", "logical_id", "sog", "quality_flags", "assignment", "assignment_flags")


def replay_id(run_id: str, source_record_id: str, copy_index: int) -> str:
    return digest(run_id, source_record_id, copy_index)


def message_key(message: dict) -> str:
    """Kafka key: one vessel's messages share a partition and keep their order."""
    return message["mmsi"]


def replay_messages(observations: Iterable[dict], run_id: str, started_at: str | datetime, *,
                    seed: int, duplicate_copies: int = 0, step_ms: int = 100) -> list[dict]:
    """Order observations by event time and attach replay identity.

    `duplicate_copies` re-sends that many seeded, distinct records right after
    their originals with copy_index 1, imitating a producer retry.
    """
    if not run_id:
        raise ValueError("replay run ID is required")
    if step_ms <= 0:
        raise ValueError("step_ms must be positive")
    start = utc_time(started_at)
    rows = sorted((Observation.from_json(row).as_json() for row in observations),
                  key=lambda row: (row["event_time"], row["mmsi"], row["source_record_id"]))
    if not 0 <= duplicate_copies <= len(rows):
        raise ValueError(f"duplicate_copies must be within 0..{len(rows)}")
    resent = set(random.Random(seed).sample(range(len(rows)), duplicate_copies))
    messages: list[dict] = []
    for index, row in enumerate(rows):
        for copy_index in ((0, 1) if index in resent else (0,)):
            ingested = start + timedelta(milliseconds=step_ms * len(messages))
            message = {key: list(row[key]) if isinstance(row[key], tuple) else row[key]
                       for key in CANONICAL_FIELDS}
            message.update({"contract_version": CONTRACT_VERSION, "replay_run_id": run_id,
                            "replay_id": replay_id(run_id, row["source_record_id"], copy_index),
                            "copy_index": copy_index, "ingested_at": ingested.isoformat()})
            messages.append(message)
    return messages


def validate_message(message: dict) -> None:
    """Reject messages missing contract fields or carrying a forged replay identity."""
    absent = [key for key in CANONICAL_FIELDS + REPLAY_FIELDS if key not in message]
    if absent:
        raise ValueError(f"Replay message missing fields: {absent}")
    if message.get("contract_version") != CONTRACT_VERSION:
        raise ValueError(f"Unsupported contract version: {message.get('contract_version')}")
    expected = replay_id(message["replay_run_id"], message["source_record_id"], message["copy_index"])
    if message["replay_id"] != expected:
        raise ValueError(f"replay_id does not match its run/source/copy identity: {message['replay_id']}")
    utc_time(message["event_time"])
    utc_time(message["ingested_at"])


def _fingerprint(identifier: str) -> int:
    # First 64 bits of a SHA-256 identity; a collision among 10^8 messages has probability ~3e-4.
    return int(identifier[:16], 16)


class ReplayTally:
    """Incremental `account`: eight bytes per identity, so a whole day of messages fits in memory."""

    def __init__(self):
        from array import array
        self.messages = 0
        self._ids = {name: array("Q") for name in ("replay_id", "source_record_id", "logical_id")}
        self._vessels: set[str] = set()
        self._run_ids: set[str] = set()

    def add(self, message: dict, *, validate: bool = True) -> None:
        if validate:
            validate_message(message)
        self.messages += 1
        for name, values in self._ids.items():
            values.append(_fingerprint(message[name]))
        self._vessels.add(message["mmsi"])
        self._run_ids.add(message["replay_run_id"])

    def _sorted(self, name: str):
        import numpy as np
        return np.sort(np.frombuffer(self._ids[name], dtype=np.uint64))

    @staticmethod
    def _distinct(values) -> int:
        return int(len(values) and 1 + (values[1:] != values[:-1]).sum())

    def summary(self) -> dict:
        replay, source, logical = (self._sorted(name) for name in self._ids)
        distinct_replay, distinct_source = self._distinct(replay), self._distinct(source)
        return {"messages": self.messages, "distinct_replay_ids": distinct_replay,
                "repeated_replay_ids": self.messages - distinct_replay,
                "distinct_source_records": distinct_source, "replay_copies": self.messages - distinct_source,
                "distinct_logical_observations": self._distinct(logical),
                "vessels": len(self._vessels), "run_ids": sorted(self._run_ids)}

    def digest(self) -> str:
        """Order-independent identity of the multiset of replay IDs, compared across transport."""
        import hashlib
        return hashlib.sha256(self._sorted("replay_id").astype(">u8").tobytes()).hexdigest()


def account(messages: Iterable[dict]) -> dict:
    """Sent-versus-distinct counts a diagnostic consumer compares after transport."""
    tally = ReplayTally()
    for message in messages:
        tally.add(message)
    return tally.summary()
