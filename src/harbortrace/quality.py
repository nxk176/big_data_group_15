"""Validation and identity rules. Rejected rows remain in quarantine."""
from __future__ import annotations

import math
import struct
from datetime import datetime, timezone

from .contracts import Observation, digest, utc_time


def implied_knots(previous: Observation, current: Observation) -> float | None:
    """Great-circle speed for a same-MMSI pair; a review flag, not truth."""
    if previous.mmsi != current.mmsi:
        raise ValueError("Movement pair has different MMSIs")
    seconds = (current.event_time - previous.event_time).total_seconds()
    if seconds <= 0:
        return None
    lat1, lat2 = map(math.radians, (previous.latitude, current.latitude))
    dlat = lat2 - lat1
    dlon = math.radians(current.longitude - previous.longitude)
    hav = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    km = 6371.0088 * 2 * math.asin(min(1.0, math.sqrt(hav)))
    return km / 1.852 / (seconds / 3600)


def point_from_wkb(value: bytes) -> tuple[float, float]:
    if not isinstance(value, bytes) or len(value) < 21:
        raise ValueError("missing_or_short_wkb")
    byte_order = "<" if value[0] == 1 else ">" if value[0] == 0 else None
    if byte_order is None:
        raise ValueError("invalid_wkb_byte_order")
    geometry_type = struct.unpack_from(byte_order + "I", value, 1)[0]
    if geometry_type != 1:
        raise ValueError("not_point_geometry")
    lon, lat = struct.unpack_from(byte_order + "dd", value, 5)
    if not all(map(math.isfinite, (lon, lat))):
        raise ValueError("nonfinite_geometry")
    return lon, lat


def canonicalize(row: dict, source_file: str, source_row: int) -> Observation:
    mmsi = str(row.get("mmsi") or "").strip()
    if not (mmsi.isdigit() and len(mmsi) == 9):
        raise ValueError("invalid_mmsi")
    raw_time = row.get("base_date_time")
    if isinstance(raw_time, datetime):
        # Provider documents UTC; Parquet timestamps can be stored without tz.
        event_time = raw_time.replace(tzinfo=timezone.utc) if raw_time.tzinfo is None else utc_time(raw_time)
    else:
        if not raw_time:
            raise ValueError("missing_event_time")
        event_time = utc_time(str(raw_time) + ("+00:00" if "+" not in str(raw_time) and "Z" not in str(raw_time) else ""))
    longitude, latitude = point_from_wkb(row.get("geometry"))
    if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
        raise ValueError("invalid_coordinates")
    sog = row.get("sog")
    if sog is not None:
        sog = float(sog)
        if not math.isfinite(sog) or sog < 0:
            raise ValueError("invalid_sog")
    source_record_id = digest(source_file, source_row)
    logical_id = digest(mmsi, event_time.isoformat(), longitude, latitude)
    flags = ("suspicious_sog",) if sog is not None and sog > 60 else ()
    return Observation(mmsi, event_time, longitude, latitude, source_file, source_row,
                       source_record_id, logical_id, sog=sog, quality_flags=flags)


def classify_same_time(observations: list[Observation]) -> tuple[list[Observation], list[dict]]:
    """Keep one identical logical observation; quarantine all conflicting positions."""
    from dataclasses import replace
    grouped: dict[tuple[str, datetime], list[Observation]] = {}
    for item in observations:
        grouped.setdefault((item.mmsi, item.event_time), []).append(item)
    accepted: list[Observation] = []
    records: list[dict] = []
    for key, group in grouped.items():
        ids = {item.logical_id for item in group}
        if len(ids) > 1:
            for item in group:
                records.append({"reason": "same_time_position_conflict", "source_record_id": item.source_record_id,
                                "mmsi": item.mmsi, "event_time": item.event_time.isoformat()})
            continue
        accepted.append(group[0])
        for item in group[1:]:
            records.append({"reason": "duplicate", "source_record_id": item.source_record_id,
                            "kept_source_record_id": group[0].source_record_id})
    return accepted, records
