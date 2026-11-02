"""Serving layer: validated batch import, immutable MongoDB generations and provisional updates.

The processing engine supplies *artifacts* (files); this module owns everything that
happens after that boundary:

* full-record validation of completed batch visits against the published ``Visit`` contract;
* immutable, deterministic serving generations with a single active pointer;
* a lease with a monotonically increasing fencing token so that a stale publisher cannot
  move the pointer after a newer publisher took over;
* provisional (streaming) cohort updates with explicit snapshot/upsert/delete semantics,
  retry identity, stale-update rejection and atomic per-cohort pointer flips;
* readers that pin one complete generation per request;
* application metrics and health/alert evaluation.

Only the ``pymongo`` collection API is used, so tests run against ``mongomock`` and the
same code runs against a real MongoDB server.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .contracts import CONTRACT_VERSION, Visit, digest

LOG = logging.getLogger("harbortrace.serving")

VISIT_FIELDS = tuple(item.name for item in fields(Visit))
NULLABLE_TIMES = ("possible_entry_after", "possible_exit_by")
REQUIRED_TIMES = ("first_inside", "last_inside", "possible_entry_by", "possible_exit_after")
INTEGER_FIELDS = ("observed_span_seconds", "observation_count", "maximum_gap_seconds", "source_record_count")
STATUSES = ("reconciled", "candidate")
MAX_SOURCE_SAMPLE = 16
HEX64 = re.compile(r"^[0-9a-f]{64}$")
MMSI = re.compile(r"^[0-9]{9}$")
ARTIFACT_TYPE = "completed_batch"
UPDATE_MODES = ("snapshot", "upsert", "delete")

GENERATIONS, VISITS, AGGREGATES, STATE, LEASES = "generations", "visits", "aggregates", "serving_state", "leases"
PROVISIONAL, COHORTS, AUDIT = "provisional_visits", "provisional_cohorts", "audit_log"


class ValidationError(ValueError):
    """An artifact or update violates the serving contract; nothing is published."""


class StaleUpdateError(ValueError):
    """A provisional update is older than the cohort's applied sequence."""


class ConflictError(ValueError):
    """The same retry identity was reused for different content."""


class FencedError(RuntimeError):
    """A publisher holding an outdated lease token attempted a write."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_utc(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValidationError(f"{name}: expected an ISO-8601 string, received {type(value).__name__}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValidationError(f"{name}: not ISO-8601 ({value!r})") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValidationError(f"{name}: time must carry a UTC offset ({value!r})")
    return parsed


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------- validation

def visit_errors(doc: dict) -> list[str]:
    """Return every contract violation of one complete visit record (empty when valid)."""
    errors: list[str] = []
    missing = [name for name in VISIT_FIELDS if name not in doc]
    extra = sorted(set(doc) - set(VISIT_FIELDS))
    if missing:
        errors.append(f"missing fields: {missing}")
    if extra:
        errors.append(f"unexpected fields: {extra}")
    if missing:
        return errors
    for name in ("visit_id", "mmsi", "port_id", "source_digest", "rule_version", "status"):
        if not isinstance(doc[name], str) or not doc[name]:
            errors.append(f"{name}: expected a non-empty string")
    for name in INTEGER_FIELDS:
        if not isinstance(doc[name], int) or isinstance(doc[name], bool) or doc[name] < 0:
            errors.append(f"{name}: expected a non-negative integer")
    for name in ("left_censored", "right_censored"):
        if not isinstance(doc[name], bool):
            errors.append(f"{name}: expected a boolean")
    for name in ("quality_flags", "source_record_ids"):
        if not isinstance(doc[name], (list, tuple)) or not all(isinstance(x, str) for x in doc[name]):
            errors.append(f"{name}: expected a list of strings")
    if errors:
        return errors
    if not MMSI.match(doc["mmsi"]):
        errors.append("mmsi: expected nine digits")
    if doc["status"] not in STATUSES:
        errors.append(f"status: expected one of {STATUSES}")
    for name in ("visit_id", "source_digest"):
        if not HEX64.match(doc[name]):
            errors.append(f"{name}: expected a lowercase SHA-256 hex digest")
    times: dict[str, datetime | None] = {}
    for name in REQUIRED_TIMES + NULLABLE_TIMES:
        if doc[name] is None and name in NULLABLE_TIMES:
            times[name] = None
            continue
        try:
            times[name] = parse_utc(doc[name], name)
        except ValidationError as error:
            errors.append(str(error))
    if errors:
        return errors
    first, last = times["first_inside"], times["last_inside"]
    if last < first:
        errors.append("last_inside precedes first_inside")
    if times["possible_entry_by"] != first:
        errors.append("possible_entry_by must equal first_inside")
    if times["possible_exit_after"] != last:
        errors.append("possible_exit_after must equal last_inside")
    if times["possible_entry_after"] is not None and times["possible_entry_after"] > first:
        errors.append("possible_entry_after must not follow first_inside")
    if times["possible_exit_by"] is not None and times["possible_exit_by"] < last:
        errors.append("possible_exit_by must not precede last_inside")
    if doc["left_censored"] != (times["possible_entry_after"] is None):
        errors.append("left_censored must be true exactly when possible_entry_after is null")
    if doc["right_censored"] != (times["possible_exit_by"] is None):
        errors.append("right_censored must be true exactly when possible_exit_by is null")
    if doc["observed_span_seconds"] != int((last - first).total_seconds()):
        errors.append("observed_span_seconds differs from last_inside - first_inside")
    if doc["observation_count"] < 1:
        errors.append("observation_count must be at least 1")
    if doc["source_record_count"] < doc["observation_count"]:
        errors.append("source_record_count is smaller than observation_count")
    sample = len(doc["source_record_ids"])
    if sample > MAX_SOURCE_SAMPLE or sample > doc["source_record_count"] or sample == 0:
        errors.append("source_record_ids must be a non-empty bounded sample of the source records")
    if list(doc["quality_flags"]) != sorted(set(doc["quality_flags"])):
        errors.append("quality_flags must be sorted and unique")
    expected_id = digest(doc["mmsi"], doc["port_id"], doc["first_inside"], doc["rule_version"])
    if doc["visit_id"] != expected_id:
        errors.append("visit_id differs from SHA256(mmsi, port, first_inside, rule_version)")
    return errors


def normalize_visit(doc: dict) -> dict:
    """Validate a visit and return a canonical JSON-compatible copy (lists, not tuples)."""
    errors = visit_errors(doc)
    if errors:
        raise ValidationError(f"visit {doc.get('visit_id', '?')}: " + "; ".join(errors))
    item = {name: doc[name] for name in VISIT_FIELDS}
    item["quality_flags"] = list(item["quality_flags"])
    item["source_record_ids"] = list(item["source_record_ids"])
    return item


# --------------------------------------------------------------------------- batch artifacts

@dataclass(frozen=True)
class BatchArtifact:
    """A completed, validated batch output ready for publication."""
    path: Path
    manifest: dict
    visits: tuple[dict, ...]

    @property
    def generation_id(self) -> str:
        # Deterministic: re-importing the same artifact is a retry, not a new generation.
        return digest("generation", self.manifest["run_id"], self.manifest["visits_sha256"])[:24]


def visits_jsonl(visits: Iterable[dict]) -> bytes:
    rows = sorted(visits, key=lambda row: (row["mmsi"], row["port_id"], row["first_inside"], row["visit_id"]))
    return "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows).encode()


def build_manifest(visits: list[dict], *, run_id: str, rule_version: str, synthetic: bool,
                   created_at: str, source: dict | None = None) -> dict:
    payload = visits_jsonl(visits)
    return {
        "artifact_type": ARTIFACT_TYPE, "contract_version": CONTRACT_VERSION, "complete": True,
        "run_id": run_id, "rule_version": rule_version, "synthetic": synthetic,
        "created_at": created_at, "visits_file": "visits.jsonl", "visits_sha256": sha256_bytes(payload),
        "visit_count": len(visits), "status_counts": dict(sorted(Counter(v["status"] for v in visits).items())),
        "source": source or {},
    }


def write_artifact(directory: Path, visits: list[dict], **manifest_args) -> Path:
    """Write an immutable artifact (used by fixtures and to package published engine output)."""
    directory = Path(directory)
    if directory.exists() and any(directory.iterdir()):
        raise FileExistsError(f"Artifact directory is not empty: {directory}")
    directory.mkdir(parents=True, exist_ok=True)
    normalized = [normalize_visit(v) for v in visits]
    (directory / "visits.jsonl").write_bytes(visits_jsonl(normalized))
    manifest = build_manifest(normalized, **manifest_args)
    # The manifest is written last: its presence marks the artifact as complete.
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return directory


def load_artifact(directory: Path | str) -> BatchArtifact:
    """Read and fully validate a completed batch artifact; raise before any database write."""
    directory = Path(directory)
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise ValidationError(f"{directory}: manifest.json is absent; the artifact is incomplete")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for key in ("artifact_type", "contract_version", "complete", "run_id", "rule_version",
                "visits_file", "visits_sha256", "visit_count", "status_counts", "synthetic"):
        if key not in manifest:
            raise ValidationError(f"manifest: missing {key}")
    if manifest["artifact_type"] != ARTIFACT_TYPE:
        raise ValidationError(f"manifest: unsupported artifact_type {manifest['artifact_type']!r}")
    if manifest["contract_version"] != CONTRACT_VERSION:
        raise ValidationError(f"manifest: contract {manifest['contract_version']} != supported {CONTRACT_VERSION}")
    if manifest["complete"] is not True:
        raise ValidationError("manifest: artifact is not marked complete")
    visit_path = directory / manifest["visits_file"]
    if visit_path.parent.resolve() != directory.resolve() or not visit_path.is_file():
        raise ValidationError("manifest: visits_file must be a file inside the artifact directory")
    payload = visit_path.read_bytes()
    if sha256_bytes(payload) != manifest["visits_sha256"]:
        raise ValidationError("visits.jsonl SHA-256 differs from the manifest")
    visits: list[dict] = []
    for number, line in enumerate(payload.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValidationError(f"visits.jsonl line {number}: invalid JSON") from error
        visit = normalize_visit(row)
        if visit["rule_version"] != manifest["rule_version"]:
            raise ValidationError(f"visits.jsonl line {number}: rule_version differs from the manifest")
        visits.append(visit)
    if len(visits) != manifest["visit_count"]:
        raise ValidationError(f"visit_count {manifest['visit_count']} != {len(visits)} records")
    ids = Counter(v["visit_id"] for v in visits)
    duplicates = [key for key, count in ids.items() if count > 1]
    if duplicates:
        raise ValidationError(f"duplicate visit_id values: {duplicates[:3]}")
    status_counts = dict(sorted(Counter(v["status"] for v in visits).items()))
    if status_counts != dict(sorted(manifest["status_counts"].items())):
        raise ValidationError(f"status_counts {manifest['status_counts']} != {status_counts}")
    return BatchArtifact(directory, manifest, tuple(visits))


def hourly_aggregates(visits: Iterable[dict]) -> list[dict]:
    """Contract hourly aggregate: reconciled visits only, keyed by port and UTC hour of first_inside."""
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for visit in visits:
        if visit["status"] != "reconciled":
            continue
        hour = parse_utc(visit["first_inside"], "first_inside").replace(minute=0, second=0, microsecond=0)
        groups[(visit["port_id"], _iso(hour))].append(visit)
    rows = []
    for (port, hour), items in sorted(groups.items()):
        spans = [v["observed_span_seconds"] for v in items]
        rows.append({
            "port_id": port, "hour": hour, "confirmed_count": len(items),
            "median_observed_span_seconds": statistics.median(spans),
            "left_censored_count": sum(v["left_censored"] for v in items),
            "right_censored_count": sum(v["right_censored"] for v in items),
        })
    return rows


# --------------------------------------------------------------------------- metrics

@dataclass
class Metrics:
    """In-process application metrics, exported as JSON lines and in Prometheus text format."""
    counters: Counter = field(default_factory=Counter)
    timings: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    gauges: dict[str, float] = field(default_factory=dict)

    def incr(self, name: str, value: int = 1):
        self.counters[name] += value

    def observe(self, name: str, seconds: float):
        self.timings[name].append(seconds)

    def gauge(self, name: str, value: float):
        self.gauges[name] = value

    def timer(self, name: str):
        metrics = self

        class _Timer:
            def __enter__(self):
                self.start = time.perf_counter()
                return self

            def __exit__(self, *exc):
                metrics.observe(name, time.perf_counter() - self.start)
                return False
        return _Timer()

    def snapshot(self) -> dict:
        return {
            "counters": dict(sorted(self.counters.items())),
            "gauges": dict(sorted(self.gauges.items())),
            "timings": {name: {"count": len(v), "max_seconds": max(v), "total_seconds": sum(v)}
                        for name, v in sorted(self.timings.items()) if v},
        }

    def prometheus(self) -> str:
        lines = []
        for name, value in sorted(self.counters.items()):
            lines.append(f"harbortrace_{name}_total {value}")
        for name, value in sorted(self.gauges.items()):
            lines.append(f"harbortrace_{name} {value}")
        for name, values in sorted(self.timings.items()):
            lines.append(f"harbortrace_{name}_seconds_count {len(values)}")
            lines.append(f"harbortrace_{name}_seconds_sum {sum(values):.6f}")
        return "\n".join(lines) + "\n"


METRICS = Metrics()


def audit(db, action: str, **details):
    """Append-only audit record of publication decisions (governance evidence)."""
    record = {"at": _iso(utc_now()), "action": action, **details}
    db[AUDIT].insert_one(dict(record))
    LOG.info(json.dumps(record, default=str, sort_keys=True))


# --------------------------------------------------------------------------- leases / fencing

def acquire_lease(db, owner: str, *, name: str = "publisher", ttl_seconds: int = 300,
                  now: datetime | None = None) -> int:
    """Acquire (or renew) the named lease and return its fencing token.

    The token increases on every acquisition, so a publisher that paused past its lease
    and resumes later holds a smaller token and is fenced off from the active pointer.
    """
    now = now or utc_now()
    expiry = _iso(now + timedelta(seconds=ttl_seconds))
    current = db[LEASES].find_one({"_id": name})
    if current is None:
        try:
            db[LEASES].insert_one({"_id": name, "owner": owner, "token": 1, "expires_at": expiry})
            return 1
        except Exception:  # lost a creation race; fall through to a normal acquisition
            current = db[LEASES].find_one({"_id": name})
    if current["owner"] != owner and current["expires_at"] > _iso(now):
        raise FencedError(f"lease {name!r} is held by {current['owner']} until {current['expires_at']}")
    updated = db[LEASES].find_one_and_update(
        {"_id": name, "token": current["token"]},
        {"$set": {"owner": owner, "expires_at": expiry}, "$inc": {"token": 1}},
        return_document=True,
    )
    if updated is None:
        raise FencedError(f"lease {name!r} changed concurrently; retry")
    return updated["token"]


def release_lease(db, owner: str, token: int, *, name: str = "publisher"):
    """Release the lease early (the token is kept, so later holders still get a larger one)."""
    db[LEASES].update_one({"_id": name, "owner": owner, "token": token},
                          {"$set": {"expires_at": _iso(utc_now() - timedelta(seconds=1))}})


def check_fence(db, token: int, *, name: str = "publisher"):
    current = db[LEASES].find_one({"_id": name})
    if current is None or current["token"] != token:
        METRICS.incr("fenced_writes")
        raise FencedError(f"fencing token {token} is not the current lease token "
                          f"{None if current is None else current['token']}")


# --------------------------------------------------------------------------- publication

def ensure_indexes(db):
    db[VISITS].create_index([("generation_id", 1), ("port_id", 1), ("first_inside", 1)])
    db[VISITS].create_index([("generation_id", 1), ("mmsi", 1)])
    db[AGGREGATES].create_index([("generation_id", 1), ("port_id", 1), ("hour", 1)])
    db[PROVISIONAL].create_index([("cohort_id", 1), ("seq", 1)])


def _bulk_upsert(collection, documents: list[dict], batch_size: int = 1000):
    from pymongo import ReplaceOne
    for start in range(0, len(documents), batch_size):
        chunk = documents[start:start + batch_size]
        if chunk:
            collection.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in chunk], ordered=False)


def publish(db, artifact: BatchArtifact, *, owner: str, activate: bool = True,
            fail_after_visits: int | None = None, now: datetime | None = None) -> dict:
    """Load an artifact into an immutable generation, verify counts, then flip the pointer.

    Retrying the same artifact is idempotent (deterministic generation and document IDs).
    ``fail_after_visits`` simulates a publisher crash for failure tests.
    """
    started = time.perf_counter()
    token = acquire_lease(db, owner, now=now)
    generation_id = artifact.generation_id
    existing = db[GENERATIONS].find_one({"_id": generation_id})
    if existing and existing["status"] == "complete":
        METRICS.incr("publish_retries_noop")
        result = {"generation_id": generation_id, "status": "complete", "reused": True}
        if activate:
            result["activated"] = activate_generation(db, generation_id, token)
        release_lease(db, owner, token)
        return result
    aggregates = hourly_aggregates(artifact.visits)
    db[GENERATIONS].replace_one({"_id": generation_id}, {
        "_id": generation_id, "status": "loading", "run_id": artifact.manifest["run_id"],
        "rule_version": artifact.manifest["rule_version"], "contract_version": CONTRACT_VERSION,
        "synthetic": artifact.manifest["synthetic"], "visits_sha256": artifact.manifest["visits_sha256"],
        "expected_visit_count": len(artifact.visits), "expected_aggregate_count": len(aggregates),
        "status_counts": artifact.manifest["status_counts"], "source": artifact.manifest.get("source", {}),
        "started_at": _iso(now or utc_now()), "fence_token": token,
    }, upsert=True)
    audit(db, "generation_loading", generation_id=generation_id, owner=owner, token=token)
    documents = [{"_id": f"{generation_id}:{v['visit_id']}", "generation_id": generation_id, **v}
                 for v in artifact.visits]
    if fail_after_visits is not None:
        _bulk_upsert(db[VISITS], documents[:fail_after_visits])
        METRICS.incr("publish_failures")
        raise RuntimeError(f"simulated publisher crash after {fail_after_visits} visits")
    _bulk_upsert(db[VISITS], documents)
    _bulk_upsert(db[AGGREGATES], [{"_id": f"{generation_id}:{a['port_id']}:{a['hour']}",
                                   "generation_id": generation_id, **a} for a in aggregates])
    stored_visits = db[VISITS].count_documents({"generation_id": generation_id})
    stored_aggregates = db[AGGREGATES].count_documents({"generation_id": generation_id})
    if stored_visits != len(artifact.visits) or stored_aggregates != len(aggregates):
        METRICS.incr("publish_failures")
        audit(db, "generation_count_mismatch", generation_id=generation_id,
              visits=stored_visits, aggregates=stored_aggregates)
        raise ValidationError(f"stored counts {stored_visits}/{stored_aggregates} differ from "
                              f"expected {len(artifact.visits)}/{len(aggregates)}")
    check_fence(db, token)
    db[GENERATIONS].update_one({"_id": generation_id, "status": "loading"},
                               {"$set": {"status": "complete", "completed_at": _iso(utc_now()),
                                         "visit_count": stored_visits, "aggregate_count": stored_aggregates}})
    audit(db, "generation_complete", generation_id=generation_id, visits=stored_visits)
    METRICS.incr("generations_published")
    METRICS.incr("visits_imported", stored_visits)
    METRICS.observe("publish", time.perf_counter() - started)
    result = {"generation_id": generation_id, "status": "complete", "reused": False,
              "visits": stored_visits, "aggregates": stored_aggregates}
    if activate:
        result["activated"] = activate_generation(db, generation_id, token)
    release_lease(db, owner, token)
    return result


def activate_generation(db, generation_id: str, token: int, *, reason: str = "publish") -> bool:
    """Atomically point readers at a complete generation, guarded by the fencing token."""
    generation = db[GENERATIONS].find_one({"_id": generation_id})
    if generation is None or generation["status"] != "complete":
        raise ValidationError(f"generation {generation_id} is not complete; it cannot be activated")
    check_fence(db, token)
    state = db[STATE].find_one({"_id": "active"})
    if state and state.get("fence_token", 0) > token:
        METRICS.incr("fenced_writes")
        raise FencedError(f"pointer was set with a newer token {state['fence_token']} > {token}")
    previous = state["generation_id"] if state else None
    if previous == generation_id:
        return False
    filter_ = {"_id": "active", "generation_id": previous} if state else {"_id": "active"}
    update = {"$set": {"generation_id": generation_id, "previous_generation_id": previous,
                       "fence_token": token, "activated_at": _iso(utc_now()), "reason": reason}}
    outcome = db[STATE].update_one(filter_, update, upsert=state is None)
    if outcome.matched_count == 0 and outcome.upserted_id is None:
        raise FencedError("active pointer changed concurrently; re-read and retry")
    audit(db, "generation_activated", generation_id=generation_id, previous=previous, token=token, reason=reason)
    METRICS.incr("activations")
    return True


def rollback(db, *, owner: str) -> str:
    """Re-activate the previously active complete generation (operator recovery action)."""
    state = db[STATE].find_one({"_id": "active"})
    if not state or not state.get("previous_generation_id"):
        raise ValidationError("no previous generation to roll back to")
    token = acquire_lease(db, owner)
    target = state["previous_generation_id"]
    activate_generation(db, target, token, reason="rollback")
    release_lease(db, owner, token)
    return target


def recover_incomplete(db, *, older_than_seconds: int = 0, now: datetime | None = None) -> list[str]:
    """Mark abandoned 'loading' generations as failed; their documents are never readable."""
    now = now or utc_now()
    failed = []
    for generation in db[GENERATIONS].find({"status": "loading"}):
        started = parse_utc(generation["started_at"], "started_at")
        if (now - started).total_seconds() >= older_than_seconds:
            db[GENERATIONS].update_one({"_id": generation["_id"], "status": "loading"},
                                       {"$set": {"status": "failed", "failed_at": _iso(now)}})
            audit(db, "generation_failed", generation_id=generation["_id"])
            failed.append(generation["_id"])
    return failed


def drop_generation(db, generation_id: str):
    """Delete an inactive generation's documents (explicit cleanup decision only)."""
    state = db[STATE].find_one({"_id": "active"}) or {}
    if generation_id in (state.get("generation_id"), state.get("previous_generation_id")):
        raise ValidationError("the active or rollback generation cannot be dropped")
    db[VISITS].delete_many({"generation_id": generation_id})
    db[AGGREGATES].delete_many({"generation_id": generation_id})
    db[GENERATIONS].delete_one({"_id": generation_id})
    audit(db, "generation_dropped", generation_id=generation_id)


# --------------------------------------------------------------------------- provisional updates

def update_payload_digest(update: dict) -> str:
    return digest(update["mode"], sorted((v["visit_id"] for v in update.get("visits", []))),
                  [visits_jsonl(update.get("visits", [])).decode()], sorted(update.get("deleted_visit_ids", [])))


def validate_update(update: dict) -> dict:
    """Validate a provisional cohort update.

    Contract: ``cohort_id`` names one streaming result set (e.g. query + port); ``seq`` is a
    strictly increasing integer assigned by the producer; ``update_id`` is
    ``SHA256(cohort_id, seq)`` and is the retry identity. ``snapshot`` replaces the cohort,
    ``upsert`` replaces/adds the listed visits, ``delete`` removes the listed visit IDs.
    """
    for key in ("contract_version", "cohort_id", "seq", "update_id", "mode", "emitted_at"):
        if key not in update:
            raise ValidationError(f"update: missing {key}")
    if update["contract_version"] != CONTRACT_VERSION:
        raise ValidationError("update: unsupported contract_version")
    if update["mode"] not in UPDATE_MODES:
        raise ValidationError(f"update: mode must be one of {UPDATE_MODES}")
    if not isinstance(update["seq"], int) or isinstance(update["seq"], bool) or update["seq"] < 1:
        raise ValidationError("update: seq must be a positive integer")
    if update["update_id"] != digest(update["cohort_id"], update["seq"]):
        raise ValidationError("update: update_id must equal SHA256(cohort_id, seq)")
    parse_utc(update["emitted_at"], "emitted_at")
    visits = [normalize_visit(v) for v in update.get("visits", [])]
    deleted = list(update.get("deleted_visit_ids", []))
    if update["mode"] == "delete" and (visits or not deleted):
        raise ValidationError("update: delete mode lists deleted_visit_ids only")
    if update["mode"] != "delete" and deleted:
        raise ValidationError("update: deleted_visit_ids are only accepted in delete mode")
    if len({v["visit_id"] for v in visits}) != len(visits):
        raise ValidationError("update: duplicate visit_id within one update")
    return {**update, "visits": visits, "deleted_visit_ids": deleted}


def apply_update(db, update: dict) -> dict:
    """Apply one provisional update; retries are no-ops, stale or conflicting updates are rejected.

    Each applied sequence materializes the full cohort under ``seq`` and then moves the
    cohort pointer, so a reader sees either the old or the new cohort, never a mixture.
    """
    update = validate_update(update)
    cohort_id, seq = update["cohort_id"], update["seq"]
    payload = update_payload_digest(update)
    cohort = db[COHORTS].find_one({"_id": cohort_id}) or {"_id": cohort_id, "seq": 0, "applied": {}}
    applied = cohort.get("applied", {})
    if str(seq) in applied:
        if applied[str(seq)] != payload:
            METRICS.incr("provisional_conflicts")
            raise ConflictError(f"{cohort_id} seq {seq} was already applied with different content")
        METRICS.incr("provisional_retries_noop")
        return {"cohort_id": cohort_id, "seq": seq, "result": "duplicate_ignored"}
    if seq <= cohort["seq"]:
        METRICS.incr("provisional_stale_rejected")
        raise StaleUpdateError(f"{cohort_id} seq {seq} is not newer than applied seq {cohort['seq']}")
    current = {doc["visit_id"]: doc for doc in
               db[PROVISIONAL].find({"cohort_id": cohort_id, "seq": cohort["seq"]}, {"_id": 0})}
    if update["mode"] == "snapshot":
        state = {v["visit_id"]: v for v in update["visits"]}
    elif update["mode"] == "upsert":
        state = {key: {k: doc[k] for k in VISIT_FIELDS} for key, doc in current.items()}
        state.update({v["visit_id"]: v for v in update["visits"]})
    else:
        state = {key: {k: doc[k] for k in VISIT_FIELDS} for key, doc in current.items()
                 if key not in set(update["deleted_visit_ids"])}
    documents = [{"_id": f"{cohort_id}:{seq}:{key}", "cohort_id": cohort_id, "seq": seq,
                  "emitted_at": update["emitted_at"], **visit} for key, visit in sorted(state.items())]
    _bulk_upsert(db[PROVISIONAL], documents)
    # Compare-and-set on the previous sequence: a concurrent writer cannot interleave.
    applied = dict(list(applied.items())[-50:])  # bounded retry memory
    applied[str(seq)] = payload
    new_state = {"_id": cohort_id, "seq": seq, "applied": applied, "updated_at": _iso(utc_now()),
                 "emitted_at": update["emitted_at"], "visit_count": len(documents)}
    if cohort["seq"] == 0 and db[COHORTS].find_one({"_id": cohort_id}) is None:
        db[COHORTS].insert_one(new_state)
    else:
        outcome = db[COHORTS].replace_one({"_id": cohort_id, "seq": cohort["seq"]}, new_state)
        if outcome.matched_count == 0:
            db[PROVISIONAL].delete_many({"cohort_id": cohort_id, "seq": seq})
            raise StaleUpdateError(f"{cohort_id} advanced concurrently; seq {seq} discarded")
    db[PROVISIONAL].delete_many({"cohort_id": cohort_id, "seq": {"$lt": seq}})
    METRICS.incr("provisional_updates_applied")
    lag = (utc_now() - parse_utc(update["emitted_at"], "emitted_at")).total_seconds()
    METRICS.gauge("provisional_last_lag_seconds", max(lag, 0.0))
    return {"cohort_id": cohort_id, "seq": seq, "result": "applied", "visits": len(documents)}


# --------------------------------------------------------------------------- readers

@dataclass(frozen=True)
class VisitQuery:
    port_id: str | None = None
    start: str | None = None      # inclusive lower bound on first_inside (UTC ISO)
    end: str | None = None        # exclusive upper bound on first_inside (UTC ISO)
    status: str | None = None
    mmsi: str | None = None
    limit: int = 200

    def mongo_filter(self) -> dict:
        query: dict[str, Any] = {}
        if self.port_id:
            query["port_id"] = str(self.port_id)
        if self.status:
            if self.status not in STATUSES:
                raise ValidationError(f"status must be one of {STATUSES}")
            query["status"] = self.status
        if self.mmsi:
            query["mmsi"] = str(self.mmsi)
        bounds = {}
        if self.start:
            bounds["$gte"] = _iso(parse_utc(self.start, "start"))
        if self.end:
            bounds["$lt"] = _iso(parse_utc(self.end, "end"))
        if self.start and self.end and bounds["$gte"] >= bounds["$lt"]:
            raise ValidationError("start must precede end")
        if bounds:
            query["first_inside"] = bounds
        return query


class Reader:
    """Read API used by the dashboard. Each call pins one complete generation."""

    def __init__(self, db):
        self.db = db

    def active_generation(self) -> dict | None:
        state = self.db[STATE].find_one({"_id": "active"})
        if not state:
            return None
        generation = self.db[GENERATIONS].find_one({"_id": state["generation_id"]})
        if not generation or generation["status"] != "complete":
            METRICS.incr("reader_inconsistent_pointer")
            raise ValidationError("active pointer references an incomplete generation")
        return {**generation, "activated_at": state.get("activated_at")}

    def visits(self, query: VisitQuery, generation_id: str | None = None) -> dict:
        with METRICS.timer("read_visits"):
            generation = (self.db[GENERATIONS].find_one({"_id": generation_id, "status": "complete"})
                          if generation_id else self.active_generation())
            if generation is None:
                return {"generation": None, "visits": [], "total": 0}
            filter_ = {"generation_id": generation["_id"], **query.mongo_filter()}
            total = self.db[VISITS].count_documents(filter_)
            limit = max(1, min(int(query.limit), 1000))
            rows = list(self.db[VISITS].find(filter_, {"_id": 0}).sort(
                [("first_inside", 1), ("mmsi", 1), ("port_id", 1)]).limit(limit))
            METRICS.incr("reads")
            return {"generation": _public_generation(generation), "visits": rows, "total": total,
                    "truncated": total > len(rows)}

    def summary(self, query: VisitQuery, generation_id: str | None = None) -> dict:
        generation = (self.db[GENERATIONS].find_one({"_id": generation_id, "status": "complete"})
                      if generation_id else self.active_generation())
        if generation is None:
            return {"generation": None, "by_port_status": [], "censoring": {}}
        filter_ = {"generation_id": generation["_id"], **query.mongo_filter()}
        pipeline = [{"$match": filter_},
                    {"$group": {"_id": {"port_id": "$port_id", "status": "$status"},
                                "visits": {"$sum": 1}, "observations": {"$sum": "$observation_count"},
                                "left_censored": {"$sum": {"$cond": ["$left_censored", 1, 0]}},
                                "right_censored": {"$sum": {"$cond": ["$right_censored", 1, 0]}},
                                "span_seconds": {"$sum": "$observed_span_seconds"}}}]
        rows = []
        for item in self.db[VISITS].aggregate(pipeline):
            rows.append({**item["_id"], **{k: v for k, v in item.items() if k != "_id"}})
        rows.sort(key=lambda row: (row["port_id"], row["status"]))
        return {"generation": _public_generation(generation), "by_port_status": rows}

    def hourly(self, port_id: str | None = None, generation_id: str | None = None) -> list[dict]:
        generation = (self.db[GENERATIONS].find_one({"_id": generation_id, "status": "complete"})
                      if generation_id else self.active_generation())
        if generation is None:
            return []
        filter_ = {"generation_id": generation["_id"]}
        if port_id:
            filter_["port_id"] = str(port_id)
        return list(self.db[AGGREGATES].find(filter_, {"_id": 0}).sort([("port_id", 1), ("hour", 1)]))

    def provisional(self, query: VisitQuery) -> dict:
        cohorts = list(self.db[COHORTS].find({}, {"applied": 0}))
        rows: list[dict] = []
        for cohort in cohorts:
            filter_ = {"cohort_id": cohort["_id"], "seq": cohort["seq"], **query.mongo_filter()}
            rows.extend(self.db[PROVISIONAL].find(filter_, {"_id": 0}))
        rows.sort(key=lambda row: (row["first_inside"], row["mmsi"], row["port_id"]))
        return {"cohorts": [{"cohort_id": c["_id"], "seq": c["seq"], "emitted_at": c.get("emitted_at"),
                             "visit_count": c.get("visit_count", 0)} for c in cohorts],
                "visits": rows[:max(1, min(int(query.limit), 1000))], "total": len(rows)}

    def ports(self) -> list[str]:
        generation = self.active_generation()
        if generation is None:
            return []
        return sorted(self.db[VISITS].distinct("port_id", {"generation_id": generation["_id"]}))


def _public_generation(generation: dict) -> dict:
    keys = ("_id", "run_id", "rule_version", "contract_version", "synthetic", "visit_count",
            "status_counts", "completed_at", "activated_at", "visits_sha256", "source")
    item = {key: generation.get(key) for key in keys}
    item["generation_id"] = item.pop("_id")
    return item


# --------------------------------------------------------------------------- health / alerts

DEFAULT_THRESHOLDS = {
    "max_generation_age_hours": 36.0,
    "max_provisional_lag_seconds": 900.0,
    "max_failed_generations": 0,
    "max_loading_minutes": 30.0,
}


def health(db, *, now: datetime | None = None, thresholds: dict | None = None) -> dict:
    """Evaluate serving health and return alert records for an operator or a probe."""
    now = now or utc_now()
    limits = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    alerts: list[dict] = []
    try:
        db.command("ping")
    except Exception as error:  # pragma: no cover - depends on a broken server
        return {"status": "down", "alerts": [{"severity": "critical", "code": "database_unreachable",
                                              "detail": str(error)}]}
    state = db[STATE].find_one({"_id": "active"})
    generation = db[GENERATIONS].find_one({"_id": state["generation_id"]}) if state else None
    if generation is None:
        alerts.append({"severity": "warning", "code": "no_active_generation", "detail": "nothing published"})
    else:
        if generation["status"] != "complete":
            alerts.append({"severity": "critical", "code": "active_generation_incomplete",
                           "detail": generation["_id"]})
        stored = db[VISITS].count_documents({"generation_id": generation["_id"]})
        if stored != generation.get("visit_count"):
            alerts.append({"severity": "critical", "code": "active_count_mismatch",
                           "detail": f"{stored} != {generation.get('visit_count')}"})
        age = (now - parse_utc(generation["completed_at"], "completed_at")).total_seconds() / 3600
        if age > limits["max_generation_age_hours"]:
            alerts.append({"severity": "warning", "code": "generation_stale", "detail": f"{age:.1f} h"})
    failed = db[GENERATIONS].count_documents({"status": "failed"})
    if failed > limits["max_failed_generations"]:
        alerts.append({"severity": "warning", "code": "failed_generations", "detail": str(failed)})
    for loading in db[GENERATIONS].find({"status": "loading"}):
        minutes = (now - parse_utc(loading["started_at"], "started_at")).total_seconds() / 60
        if minutes > limits["max_loading_minutes"]:
            alerts.append({"severity": "warning", "code": "publication_stuck",
                           "detail": f"{loading['_id']} loading for {minutes:.0f} min"})
    for cohort in db[COHORTS].find({}, {"applied": 0}):
        lag = (now - parse_utc(cohort["emitted_at"], "emitted_at")).total_seconds()
        if lag > limits["max_provisional_lag_seconds"]:
            alerts.append({"severity": "warning", "code": "provisional_lag",
                           "detail": f"{cohort['_id']} last update {lag:.0f} s ago"})
    status = "critical" if any(a["severity"] == "critical" for a in alerts) else (
        "degraded" if alerts else "ok")
    return {"status": status, "checked_at": _iso(now),
            "active_generation": generation["_id"] if generation else None, "alerts": alerts}


def connect(uri: str, database: str, *, read_only: bool = False):
    """Open a MongoDB database with majority write/read concerns and short timeouts."""
    from pymongo import MongoClient
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
                         retryWrites=not read_only, appname="harbortrace-serving")
    return client.get_database(database, write_concern=WriteConcern(w="majority"),
                               read_concern=ReadConcern("majority"))


def visit_from_dataclass(visit: Visit) -> dict:
    return normalize_visit(visit.as_json())
