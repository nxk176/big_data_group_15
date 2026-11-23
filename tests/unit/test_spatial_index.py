import json
import random

import pytest
import shapely
from shapely.geometry import Polygon, box, mapping

from harbortrace.spatial import Port, PortIndex, assign, load_ports, load_usable_ports


def synthetic_ports():
    """Two overlapping boxes, a far box and a repaired self-intersecting polygon (all synthetic)."""
    bow_tie = Polygon([(3, 0), (4, 1), (4, 0), (3, 1), (3, 0)])
    repaired = shapely.make_valid(bow_tie)
    return [Port("1", "A", 2024, box(0, 0, 1, 1)), Port("2", "B", 2024, box(0.5, 0.5, 1.5, 1.5)),
            Port("3", "C", 2024, box(10, 10, 11, 11)),
            Port("4", "D", 2024, repaired, "Self-intersection[3.5 0.5]", 0.0)]


def sample_points(ports, count=5000, seed=7):
    rng = random.Random(seed)
    points = [(rng.uniform(-0.5, 4.5), rng.uniform(-0.5, 1.6)) for _ in range(count)]
    points += [(rng.uniform(9.9, 11.1), rng.uniform(9.9, 11.1)) for _ in range(500)]
    for port in ports:  # exact vertices and edge points, where `covers` and the distance flag matter most
        points += [tuple(map(float, xy)) for xy in shapely.get_coordinates(port.geometry.boundary)]
        points += [(float(port.geometry.bounds[0]), (port.geometry.bounds[1] + port.geometry.bounds[3]) / 2)]
    return points


@pytest.mark.parametrize("jitter", [0, 100, 5000])
def test_vectorized_assignment_equals_scalar_assign(jitter):
    ports = synthetic_ports()
    points = sample_points(ports)
    ids, flags = PortIndex(ports, jitter).assign([x for x, _ in points], [y for _, y in points])
    expected = [assign(x, y, ports, jitter) for x, y in points]
    assert list(zip(ids, flags)) == expected
    seen = {flag for _, item in expected for flag in item}
    assert {"overlapping_port_polygons", "boundary_topology_repaired"} <= seen
    if jitter:
        assert "near_boundary" in seen


def test_empty_input():
    assert PortIndex(synthetic_ports(), 100).assign([], []) == ([], [])


def write_layer(path, polygons):
    features = [{"type": "Feature", "properties": {"PORTIDPK": port_id, "FEATURENAME": f"P{port_id}", "DATA_YEAR": 2024},
                 "geometry": mapping(geometry)} for port_id, geometry in polygons]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))


def test_usable_ports_keep_valid_and_report_rejected(tmp_path):
    lopsided = Polygon([(0, 0), (4, 4), (4, 0), (0, 3), (0, 0)])  # repair changes the area by 257%
    symmetric = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])  # lobes cancel: zero signed area
    layer = tmp_path / "ports.geojson"
    write_layer(layer, [(1, box(0, 0, 1, 1)), (2, box(2, 2, 3, 3)), (3, lopsided), (4, symmetric)])
    usable, rejected = load_usable_ports(layer)
    assert [port.port_id for port in usable] == ["1", "2"]
    assert "more than 15%" in rejected["3"] and "zero area" in rejected["4"]
    assert [port.port_id for port in load_ports(layer, [2, 1])] == ["1", "2"]
    with pytest.raises(ValueError):
        load_ports(layer, [3])
