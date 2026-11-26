"""Check the measured multi-join on a small, hand-counted input."""
import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from harbortrace import analytics
from scripts import join_comparison


def test_every_strategy_keeps_values_and_duplicates(tmp_path):
    spark = analytics.analytics_session("local[2]", shuffle_partitions=2, warehouse=str(tmp_path))
    try:
        visits = spark.createDataFrame(
            [("a", "v1", "p1"), ("a", "v1", "p1"), ("b", "v1", "p2"), ("c", "v2", "p1")],
            ["visit_id", "mmsi", "port_id"],
        )
        ports = analytics.port_reference(spark, ["p1", "p2"])
        vessels = analytics.vessel_totals(visits)
        expected = [("a", "v1", "Port p1", 3, 2), ("a", "v1", "Port p1", 3, 2),
                    ("b", "v1", "Port p2", 3, 2), ("c", "v2", "Port p1", 1, 1)]
        for strategy in join_comparison.STRATEGIES:
            actual = join_comparison.join_visits(visits, ports, vessels, strategy)
            assert sorted(map(tuple, actual.collect())) == expected
        assert join_comparison.differences(actual, actual.distinct()) == {
            "before_only": 1, "after_only": 0}
        assert join_comparison.differences(actual.distinct(), actual) == {
            "before_only": 0, "after_only": 1}
    finally:
        spark.stop()
