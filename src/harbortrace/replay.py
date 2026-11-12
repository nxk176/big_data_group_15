"""Deterministic historical replay messages built from canonical observations.

A replay copy is transport evidence, not new vessel activity: every message keeps
the canonical fields and adds replay identity plus ingestion time.
"""
from __future__ import annotations

import random
from collections import Counter
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


def account(messages: Iterable[dict]) -> dict:
    """Sent-versus-distinct counts a diagnostic consumer compares after transport."""
    rows = list(messages)
    for row in rows:
        validate_message(row)
    replay_ids = Counter(row["replay_id"] for row in rows)
    sources = {row["source_record_id"] for row in rows}
    return {"messages": len(rows), "distinct_replay_ids": len(replay_ids),
            "repeated_replay_ids": sum(count - 1 for count in replay_ids.values()),
            "distinct_source_records": len(sources), "replay_copies": len(rows) - len(sources),
            "distinct_logical_observations": len({row["logical_id"] for row in rows}),
            "vessels": len({row["mmsi"] for row in rows}),
            "run_ids": sorted({row["replay_run_id"] for row in rows})}
