"""Exact port polygon assignment with explicit ambiguity."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from shapely.geometry import Point, shape
from shapely.validation import explain_validity, make_valid


@dataclass(frozen=True)
class Port:
    port_id: str
    name: str
    year: int | None
    geometry: object
    repair_reason: str | None = None
    area_change_fraction: float = 0.0


def load_ports(path: str | Path, requested: list[int | str]) -> list[Port]:
    feature_collection = json.loads(Path(path).read_text(encoding="utf-8"))
    if feature_collection.get("type") != "FeatureCollection":
        raise ValueError("Expected GeoJSON FeatureCollection")
    selected: list[Port] = []
    wanted = {str(value) for value in requested}
    for feature in feature_collection["features"]:
        props = feature["properties"]
        port_id = str(props["PORTIDPK"])
        if port_id in wanted:
            geom = shape(feature["geometry"])
            if geom.is_empty:
                raise ValueError(f"Empty port polygon: {port_id}")
            repair_reason = None
            change = 0.0
            if not geom.is_valid:
                repair_reason = explain_validity(geom)
                repaired = make_valid(geom)
                if repaired.geom_type not in {"Polygon", "MultiPolygon"} or not repaired.is_valid:
                    raise ValueError(f"Port polygon cannot be repaired unambiguously: {port_id}")
                change = abs(repaired.area - geom.area) / geom.area
                if change > 0.15:
                    raise ValueError(f"Port polygon repair changes area more than 15%: {port_id}")
                geom = repaired
            selected.append(Port(port_id, props["FEATURENAME"], props.get("DATA_YEAR"), geom,
                                 repair_reason, change))
    if {p.port_id for p in selected} != wanted:
        raise ValueError(f"Missing requested ports: {wanted - {p.port_id for p in selected}}")
    return selected


def region_bounds(ports: list[Port], margin: float) -> tuple[float, float, float, float]:
    if margin < 0:
        raise ValueError("margin must be nonnegative")
    bounds = [p.geometry.bounds for p in ports]
    return min(x[0] for x in bounds) - margin, min(x[1] for x in bounds) - margin, max(x[2] for x in bounds) + margin, max(x[3] for x in bounds) + margin


def assign(longitude: float, latitude: float, ports: list[Port], jitter_meters: float) -> tuple[str | None, tuple[str, ...]]:
    point = Point(longitude, latitude)
    matches = [port for port in ports if port.geometry.covers(point)]
    if len(matches) > 1:
        return None, ("overlapping_port_polygons",)
    flags: list[str] = []
    if matches:
        # Distance in degrees times the shorter local axis is a conservative
        # lower bound, so this may over-flag but will not miss a nearby edge.
        port = matches[0]
        import math
        if port.repair_reason:
            flags.append("boundary_topology_repaired")
        lower_meters = port.geometry.boundary.distance(point) * min(110540, 111320 * math.cos(math.radians(latitude)))
        if lower_meters <= jitter_meters:
            flags.append("near_boundary")
        return port.port_id, tuple(flags)
    return None, ()
