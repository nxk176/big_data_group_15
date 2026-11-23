"""Import completed Spark update directories with durable batch receipts."""
from __future__ import annotations

import json
import os
import re
import uuid
from collections import Counter
from pathlib import Path

from . import serving
from .contracts import digest

RECEIPTS = "update_imports"
BATCH_NAME = re.compile(r"batch=([0-9]+)\Z")


class _BatchChanged(serving.ValidationError):
    """The engine rewrote a batch while the importer read its files."""


def _batches(directory: Path) -> list[tuple[int, Path]]:
    if not directory.is_dir():
        raise serving.ValidationError(f"update directory does not exist: {directory}")
    if (directory / "updates").is_dir():
        directory = directory / "updates"
    batches = {}
    for path in directory.iterdir():
        if not path.name.startswith("batch="):
            continue
        match = BATCH_NAME.fullmatch(path.name)
        if match is None or not path.is_dir():
            raise serving.ValidationError(f"invalid update batch directory: {path.name}")
        number = int(match.group(1))
        if number in batches:
            raise serving.ValidationError(f"duplicate update batch number: {number}")
        batches[number] = path
    return sorted(batches.items())


def _signature(state: os.stat_result) -> tuple:
    return state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns, state.st_ctime_ns


def _part_snapshot(path: Path) -> list[tuple[Path, tuple]]:
    return [(part, _signature(part.stat())) for part in sorted(path.glob("part-*")) if part.is_file()]


def _load_batch(path: Path) -> tuple[list[dict], str]:
    marker_path = path / "_SUCCESS"
    try:
        # Keep the original inode alive so an empty replacement marker cannot reuse it.
        with marker_path.open("rb") as marker:
            marker_before = _signature(os.fstat(marker.fileno()))
            before = _part_snapshot(path)
            parts = [(part, part.read_bytes()) for part, _ in before]
            # Coarse timestamps can hide an in-place rewrite with the same byte count.
            if (any(part.read_bytes() != payload for part, payload in parts)
                    or _part_snapshot(path) != before
                    or _signature(marker_path.stat()) != marker_before):
                raise _BatchChanged(f"{path.name}: batch changed during read")
    except (FileNotFoundError, NotADirectoryError, IsADirectoryError) as error:
        raise _BatchChanged(f"{path.name}: batch is being rewritten") from error
    if not parts:
        raise serving.ValidationError(f"{path.name}: completed batch has no part files")
    updates = []
    for part, payload in parts:
        try:
            lines = payload.decode("utf-8").splitlines()
        except UnicodeDecodeError as error:
            raise serving.ValidationError(f"{path.name}/{part.name}: invalid UTF-8") from error
        for line_number, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                update = json.loads(line)
            except json.JSONDecodeError as error:
                raise serving.ValidationError(f"{path.name}/{part.name}:{line_number}: invalid JSON") from error
            if not isinstance(update, dict):
                raise serving.ValidationError(f"{path.name}/{part.name}:{line_number}: expected an update object")
            updates.append(serving.validate_update(update))
    canonical = []
    for update in updates:
        content = {key: value for key, value in update.items() if key != "emitted_at"}
        content["visits"] = sorted(update["visits"], key=lambda row: row["visit_id"])
        content["deleted_visit_ids"] = sorted(update["deleted_visit_ids"])
        canonical.append(json.dumps(content, sort_keys=True, separators=(",", ":")))
    # Spark retries can change filenames, row order and wall-clock emission metadata.
    checksum = digest("engine-update-batch", sorted(canonical))
    return sorted(updates, key=lambda row: (row["cohort_id"], row["seq"])), checksum


def apply_directory(db, directory: Path | str, *, source_id: str | None = None) -> dict:
    """Apply complete batches in numeric order, stopping at the first unfinished batch.

    Each batch is validated before its first cohort update. A receipt follows successful update
    application, so restarting an interrupted batch uses the serving retry identities.
    One source lease prevents two directory importers from racing their receipts.
    """
    directory = Path(directory)
    batches = _batches(directory)
    updates_root = directory / "updates" if (directory / "updates").is_dir() else directory
    source_id = source_id if source_id is not None else str(updates_root.resolve())
    if not isinstance(source_id, str) or not source_id.strip():
        raise serving.ValidationError("source_id must be a non-empty string")
    report = {"source_id": source_id, "batches_applied": 0, "batches_reused": 0,
              "pending_batches": [], "updates": {}}
    outcomes = Counter()
    lease_name, owner = f"update-import:{digest(source_id)}", uuid.uuid4().hex
    token = serving.acquire_lease(db, owner, name=lease_name)
    try:
        for index, (number, path) in enumerate(batches):
            if not (path / "_SUCCESS").is_file():
                report["pending_batches"] = [item[0] for item in batches[index:]]
                break
            token = serving.acquire_lease(db, owner, name=lease_name)
            try:
                updates, checksum = _load_batch(path)
            except _BatchChanged:
                report["pending_batches"] = [item[0] for item in batches[index:]]
                break
            receipt_id = digest(source_id, number)
            receipt = db[RECEIPTS].find_one({"_id": receipt_id})
            if receipt:
                if receipt["sha256"] != checksum:
                    raise serving.ConflictError(f"completed batch {number} changed after import")
                report["batches_reused"] += 1
                continue
            batch_outcomes = Counter()
            for update in updates:
                serving.check_fence(db, token, name=lease_name)
                try:
                    result = serving.apply_update(db, update)["result"]
                except serving.StaleUpdateError:
                    result = "stale"
                batch_outcomes[result] += 1
            serving.check_fence(db, token, name=lease_name)
            db[RECEIPTS].insert_one({"_id": receipt_id, "source_id": source_id, "batch": number,
                                     "sha256": checksum, "update_count": len(updates),
                                     "outcomes": dict(batch_outcomes), "completed_at": serving.utc_now().isoformat()})
            report["batches_applied"] += 1
            outcomes.update(batch_outcomes)
    finally:
        serving.release_lease(db, owner, token, name=lease_name)
    report["updates"] = dict(outcomes)
    return report
