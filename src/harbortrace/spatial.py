"""Exact port polygon assignment with explicit ambiguity."""
from __future__ import annotations

import json
import math
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


def _features(path: str | Path) -> list[dict]:
    feature_collection = json.loads(Path(path).read_text(encoding="utf-8"))
    if feature_collection.get("type") != "FeatureCollection":
        raise ValueError("Expected GeoJSON FeatureCollection")
    return feature_collection["features"]


def _port(feature: dict) -> Port:
    """One port polygon, repaired only when the repair is a valid areal geometry within 15% of the area."""
    props = feature["properties"]
    port_id = str(props["PORTIDPK"])
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
        if geom.area == 0:  # e.g. a symmetric bow-tie, whose lobes cancel; the change is unbounded
            raise ValueError(f"Invalid port polygon has zero area, so its repair cannot be bounded: {port_id}")
        change = abs(repaired.area - geom.area) / geom.area
        if change > 0.15:
            raise ValueError(f"Port polygon repair changes area more than 15%: {port_id}")
        geom = repaired
    return Port(port_id, props["FEATURENAME"], props.get("DATA_YEAR"), geom, repair_reason, change)


def load_ports(path: str | Path, requested: list[int | str]) -> list[Port]:
    wanted = {str(value) for value in requested}
    selected = [_port(feature) for feature in _features(path)
                if str(feature["properties"]["PORTIDPK"]) in wanted]
    if {p.port_id for p in selected} != wanted:
        raise ValueError(f"Missing requested ports: {wanted - {p.port_id for p in selected}}")
    return selected


def load_usable_ports(path: str | Path) -> tuple[list[Port], dict[str, str]]:
    """Every polygon of the layer that passes the repair rule, and the rejected ones with their reason."""
    usable: list[Port] = []
    rejected: dict[str, str] = {}
    for feature in _features(path):
        try:
            usable.append(_port(feature))
        except ValueError as error:
            rejected[str(feature["properties"]["PORTIDPK"])] = str(error)
    if not usable:
        raise ValueError("No usable port polygon in the layer")
    return usable, rejected


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
        if port.repair_reason:
            flags.append("boundary_topology_repaired")
        lower_meters = port.geometry.boundary.distance(point) * min(110540, 111320 * math.cos(math.radians(latitude)))
        if lower_meters <= jitter_meters:
            flags.append("near_boundary")
        return port.port_id, tuple(flags)
    return None, ()


class PortIndex:
    """Vectorized `assign` over many points: an STRtree replaces testing every point against every port.

    Results equal `assign` point for point: the same GEOS `covers` predicate and
    boundary distance, and the same scalar latitude scaling.
    """

    def __init__(self, ports: list[Port], jitter_meters: float):
        import numpy as np
        import shapely
        self.ports = ports
        self.jitter_meters = jitter_meters
        self.geometries = np.array([port.geometry for port in ports], dtype=object)
        shapely.prepare(self.geometries)
        self.tree = shapely.STRtree(self.geometries)
        self.boundaries = shapely.boundary(self.geometries)

    def assign(self, longitudes, latitudes) -> tuple[list[str | None], list[tuple[str, ...]]]:
        import numpy as np
        import shapely
        longitudes, latitudes = np.asarray(longitudes, dtype=float), np.asarray(latitudes, dtype=float)
        points = shapely.points(longitudes, latitudes)
        # Bounding-box candidates, then covers(prepared polygon, point): the predicate `assign` uses.
        # (A predicate inside tree.query would prepare the points instead of the polygons.)
        point_index, port_index = self.tree.query(points)
        inside = shapely.covers(self.geometries[port_index], points[point_index])
        point_index, port_index = point_index[inside], port_index[inside]
        matches = np.bincount(point_index, minlength=len(points))
        chosen = np.full(len(points), -1)
        chosen[point_index] = port_index
        single = np.flatnonzero(matches == 1)
        degrees = shapely.distance(self.boundaries[chosen[single]], points[single])
        port_ids: list[str | None] = [None] * len(points)
        flags: list[tuple[str, ...]] = [()] * len(points)
        for position in np.flatnonzero(matches > 1):
            flags[position] = ("overlapping_port_polygons",)
        for position, distance in zip(single.tolist(), degrees.tolist()):
            port = self.ports[chosen[position]]
            latitude = float(latitudes[position])
            items = ["boundary_topology_repaired"] if port.repair_reason else []
            if distance * min(110540, 111320 * math.cos(math.radians(latitude))) <= self.jitter_meters:
                items.append("near_boundary")
            port_ids[position], flags[position] = port.port_id, tuple(items)
        return port_ids, flags
