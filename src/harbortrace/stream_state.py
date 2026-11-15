"""Bounded per-vessel state for provisional (streaming) visit reconstruction.

Pure Python, no Spark. One ``VesselStream`` holds what Spark keeps between microbatches
for one vessel:

* a buffer of observations newer than the watermark (they may still be joined by late
  ones, so their order is not final);
* one constant-size visit state machine per port with an open visit, fed only with
  observations at or before the watermark, in the same order the batch engine uses.

Observations are never kept after they pass the watermark, so state does not grow with
a vessel's history. The provisional view after any call equals the batch reconstruction
of the observations released so far; visits are only added or replaced, never retracted.
"""
from __future__ import annotations

import copy
import math
import struct
from datetime import datetime, timedelta

from .contracts import CONTRACT_VERSION, Observation, Visit, digest, utc_time
from .visits import Rules, VesselTracker, VisitMachine

# --------------------------------------------------------------------------- resumable SHA-256
# ``source_digest`` is a SHA-256 over every source record ID of a visit. A hashlib object
# cannot be saved into a state store, so the streaming path uses this implementation, whose
# state is eight integers and a short tail. It yields the same digests as hashlib.

_MASK = 0xFFFFFFFF


def _cube_root(value: int) -> int:
    root = 1 << ((value.bit_length() + 2) // 3)
    while True:
        better = (2 * root + value // (root * root)) // 3
        if better >= root:
            return root
        root = better


_PRIMES = [n for n in range(2, 312) if all(n % d for d in range(2, math.isqrt(n) + 1))]
_INITIAL = tuple(math.isqrt(p << 64) & _MASK for p in _PRIMES[:8])
_ROUND = tuple(_cube_root(p << 96) & _MASK for p in _PRIMES[:64])


def _rotate(value: int, bits: int) -> int:
    return ((value >> bits) | (value << (32 - bits))) & _MASK


def _compress(state: tuple, block: bytes) -> tuple:
    w = list(struct.unpack(">16I", block))
    for i in range(16, 64):
        s0 = _rotate(w[i - 15], 7) ^ _rotate(w[i - 15], 18) ^ (w[i - 15] >> 3)
        s1 = _rotate(w[i - 2], 17) ^ _rotate(w[i - 2], 19) ^ (w[i - 2] >> 10)
        w.append((w[i - 16] + s0 + w[i - 7] + s1) & _MASK)
    a, b, c, d, e, f, g, h = state
    for i in range(64):
        t1 = (h + (_rotate(e, 6) ^ _rotate(e, 11) ^ _rotate(e, 25)) + ((e & f) ^ (~e & _MASK & g))
              + _ROUND[i] + w[i]) & _MASK
        t2 = ((_rotate(a, 2) ^ _rotate(a, 13) ^ _rotate(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) & _MASK
        a, b, c, d, e, f, g, h = (t1 + t2) & _MASK, a, b, c, (d + t1) & _MASK, e, f, g
    return tuple((x + y) & _MASK for x, y in zip(state, (a, b, c, d, e, f, g, h)))


class ResumableSha256:
    """SHA-256 with an exportable state (same ``update``/``hexdigest`` as hashlib)."""

    def __init__(self, words: tuple = _INITIAL, length: int = 0, tail: bytes = b""):
        self.words, self.length, self.tail = tuple(words), length, tail

    def update(self, data: bytes) -> None:
        self.length += len(data)
        data = self.tail + data
        cut = len(data) - len(data) % 64
        for start in range(0, cut, 64):
            self.words = _compress(self.words, data[start:start + 64])
        self.tail = data[cut:]

    def hexdigest(self) -> str:
        padded = self.tail + b"\x80" + b"\x00" * ((55 - len(self.tail)) % 64) + struct.pack(">Q", self.length * 8)
        words = self.words
        for start in range(0, len(padded), 64):
            words = _compress(words, padded[start:start + 64])
        return "".join(f"{word:08x}" for word in words)

    def export(self) -> list:
        return [list(self.words), self.length, self.tail.hex()]

    @classmethod
    def restore(cls, data: list) -> "ResumableSha256":
        return cls(tuple(data[0]), data[1], bytes.fromhex(data[2]))


# --------------------------------------------------------------------------- saveable machine

_TIMES = ("last_outside", "previous_time", "first_inside", "last_inside", "entry_after", "pending_exit")
_PLAIN = ("previous_logical", "left_censored", "confirmed", "count", "max_gap", "source_count")


class StreamMachine(VisitMachine):
    """The reference visit machine with state that can be saved and restored."""

    def _reset(self):
        super()._reset()
        self.source_hash = ResumableSha256()

    def export(self) -> dict:
        data = {name: getattr(self, name).isoformat() if getattr(self, name) else None for name in _TIMES}
        data.update({name: getattr(self, name) for name in _PLAIN})
        data.update(flags=sorted(self.flags), source_ids=list(self.source_ids),
                    source_hash=self.source_hash.export())
        return data

    @classmethod
    def restore(cls, mmsi: str, port_id: str, rules: Rules, data: dict) -> "StreamMachine":
        machine = cls(mmsi, port_id, rules)
        for name in _TIMES:
            setattr(machine, name, utc_time(data[name]) if data[name] else None)
        for name in _PLAIN:
            setattr(machine, name, data[name])
        machine.flags = set(data["flags"])
        machine.source_ids = list(data["source_ids"])
        machine.source_hash = ResumableSha256.restore(data["source_hash"])
        return machine

    def preview(self) -> Visit | None:
        """The open visit as the batch engine would report it if input ended here."""
        return copy.deepcopy(self).close()[0] if self.first_inside is not None else None


# --------------------------------------------------------------------------- vessel state

def visit_json(visit: Visit) -> dict:
    data = visit.as_json()
    data["quality_flags"] = list(data["quality_flags"])
    data["source_record_ids"] = list(data["source_record_ids"])
    return data


class VesselStream:
    """Incremental provisional reconstruction for one vessel.

    ``process`` accepts new replay messages and the current watermark and returns a
    provisional update (serving contract) when the vessel's visits changed, else None.
    """

    def __init__(self, mmsi: str, ports: tuple[str, ...] | None, rules: Rules, *, stream_id: str,
                 max_buffer: int = 5000, state: dict | None = None):
        self.mmsi, self.rules = mmsi, rules
        self.stream_id, self.max_buffer = stream_id, max_buffer
        state = state or {}
        self.cohort_id: str | None = state.get("cohort_id")
        self.seq: int = state.get("seq", 0)
        self.frontier = utc_time(state["frontier"]) if state.get("frontier") else None
        self.buffer = [[utc_time(row[0]), *row[1:]] for row in state.get("buffer", [])]
        self.tracker = VesselTracker(mmsi, rules, ports, machine=StreamMachine)
        saved = state.get("tracker")
        if saved:
            for name in ("last_outside", "previous_time"):
                setattr(self.tracker, name, utc_time(saved[name]) if saved[name] else None)
            self.tracker.previous_logical = saved["previous_logical"]
            self.tracker.active = {port: StreamMachine.restore(mmsi, port, rules, data)
                                   for port, data in saved["active"].items()}
        self.sent: dict[str, str] = dict(state.get("sent", {}))
        self.counters = {name: state.get(name, 0) for name in ("accepted", "late", "duplicates", "forced")}

    def export(self) -> dict:
        tracker = self.tracker
        return {"cohort_id": self.cohort_id, "seq": self.seq,
                "frontier": self.frontier.isoformat() if self.frontier else None,
                "buffer": [[row[0].isoformat(), *row[1:]] for row in self.buffer],
                "tracker": {
                    "last_outside": tracker.last_outside.isoformat() if tracker.last_outside else None,
                    "previous_time": tracker.previous_time.isoformat() if tracker.previous_time else None,
                    "previous_logical": tracker.previous_logical,
                    "active": {port: machine.export() for port, machine in tracker.active.items()}},
                "sent": self.sent, **self.counters}

    def process(self, messages: list[dict], watermark: datetime | None, emitted_at: str,
                *, flush: bool = False) -> dict | None:
        """Buffer new messages, release those the watermark has passed, report changes.

        A message at or before the vessel's released frontier is counted as late and
        dropped: the machines have already moved past it. The batch engine reads the
        retained messages again and corrects the view. ``flush`` releases everything
        (end of a bounded input).
        """
        buffered = {row[1] for row in self.buffer}
        for message in messages:
            when = utc_time(message["event_time"])
            if self.frontier is not None and when <= self.frontier:
                self.counters["late"] += 1
            elif message["logical_id"] in buffered:
                self.counters["duplicates"] += 1  # replay copy or redelivery
            else:
                buffered.add(message["logical_id"])
                self.counters["accepted"] += 1
                self.buffer.append([when, message["logical_id"], message["source_record_id"],
                                    message.get("assignment"), list(message.get("quality_flags") or ()),
                                    list(message.get("assignment_flags") or ())])
        if not self.buffer and not messages:
            return None
        self.buffer.sort(key=lambda row: (row[0], row[1], row[2]))
        if self.cohort_id is None and self.buffer:
            self.cohort_id = f"{self.stream_id}:{self.mmsi}:{self.buffer[0][0].isoformat()}"
        if flush:
            ready = len(self.buffer)
        else:
            ready = sum(1 for row in self.buffer if watermark is not None and row[0] <= watermark)
            overflow = len(self.buffer) - ready - self.max_buffer
            if overflow > 0:
                # Degrade instead of failing: release the oldest early and count it.
                ready += overflow
                self.counters["forced"] += overflow
        released, self.buffer = self.buffer[:ready], self.buffer[ready:]
        changed: list[Visit] = []
        for when, logical_id, source_record_id, assignment, quality_flags, assignment_flags in released:
            observation = Observation(
                mmsi=self.mmsi, event_time=when, longitude=0.0, latitude=0.0, source_file="", source_row=0,
                source_record_id=source_record_id, logical_id=logical_id, quality_flags=tuple(quality_flags),
                assignment=assignment, assignment_flags=tuple(assignment_flags))
            changed.extend(self.tracker.feed(observation))
        if released:
            self.frontier = released[-1][0]
        for port in sorted(set(self.sent) | set(self.tracker.active)):
            machine = self.tracker.active.get(port)
            preview = machine.preview() if machine else None
            mark = digest(visit_json(preview)) if preview else None
            if mark != self.sent.get(port):
                if preview:
                    changed.append(preview)
                    self.sent[port] = mark
                else:
                    self.sent.pop(port, None)
        if not changed:
            return None
        self.seq += 1
        visits = sorted((visit_json(visit) for visit in changed), key=lambda row: (row["port_id"], row["first_inside"]))
        return {"contract_version": CONTRACT_VERSION, "cohort_id": self.cohort_id, "seq": self.seq,
                "update_id": digest(self.cohort_id, self.seq), "mode": "upsert", "emitted_at": emitted_at,
                "visits": visits, "deleted_visit_ids": []}

    def wake_at(self) -> datetime:
        """When this vessel must be revisited without new data: the earliest buffered
        observation becoming releasable, else the moment its state may be dropped."""
        if self.buffer:
            return self.buffer[0][0]
        return self.frontier + timedelta(minutes=self.rules.max_gap_minutes)

    def expired(self, watermark: datetime | None) -> bool:
        """True once no observation within ``max_gap`` of the last one can still arrive.

        Any later observation follows a long gap, after which the batch machine also
        starts from a blank state, so later visits are unaffected. One difference
        remains for a visit that was open at expiry: it was reported with
        ``input_range_ended``, while batch, seeing the later observation, closes it with
        ``long_gap_unknown_continuity`` and counts that gap in ``maximum_gap_seconds``.
        """
        if self.buffer or self.frontier is None or watermark is None:
            return False
        return watermark > self.frontier + timedelta(minutes=self.rules.max_gap_minutes)
