"""Spark analytics checks. Inputs are SYNTHETIC; they verify computations, not findings."""
import math
import statistics
from collections import Counter
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from harbortrace import analytics  # noqa: E402
from harbortrace.serving import load_artifact  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures/serving"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = analytics.analytics_session("local[2]", warehouse=str(tmp_path_factory.mktemp("wh")),
                                          shuffle_partitions=2, name="harbortrace-analytics-tests")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def synthetic():
    return analytics.synthetic_visits()


@pytest.fixture(scope="module")
def enriched(spark, synthetic):
    frame = analytics.enrich(analytics.visits_frame(spark, synthetic)).cache()
    yield frame
    frame.unpersist()


def test_synthetic_generator_is_deterministic_and_labelled(synthetic):
    assert synthetic == analytics.synthetic_visits()
    assert all(v["port_id"].startswith("SYN") and v["mmsi"].startswith("99") for v in synthetic)


def test_window_features_match_python(enriched, synthetic):
    vessel = sorted((v for v in synthetic if v["mmsi"] == synthetic[0]["mmsi"]),
                    key=lambda v: (v["first_inside"], v["port_id"]))
    rows = (enriched.where(f"mmsi = '{vessel[0]['mmsi']}'")
            .orderBy("vessel_visit_number").collect())
    assert [r["vessel_visit_number"] for r in rows] == list(range(1, len(vessel) + 1))
    assert rows[0]["previous_port"] is None and rows[0]["hours_since_previous_visit"] is None
    assert [r["previous_port"] for r in rows[1:]] == [v["port_id"] for v in vessel[:-1]]
    spans = [v["observed_span_seconds"] / 60 for v in vessel]
    assert math.isclose(rows[2]["rolling_3_span_minutes"], sum(spans[:3]) / 3)
    assert {r["span_bucket"] for r in enriched.select("span_bucket").distinct().collect()} <= {
        "lt_30m", "30m_2h", "2h_6h", "ge_6h"}


def test_custom_aggregates_match_python(enriched, synthetic):
    profiles = {r["port_id"]: r for r in analytics.port_profiles(enriched).collect()}
    port = "SYN01"
    reconciled = [v for v in synthetic if v["port_id"] == port and v["status"] == "reconciled"]
    spans = [v["observed_span_seconds"] / 60 for v in reconciled]
    median = statistics.median(spans)
    mad = statistics.median(abs(s - median) for s in spans)
    share = sum(v["left_censored"] or v["right_censored"] for v in reconciled) / len(reconciled)
    assert math.isclose(profiles[port]["span_mad_minutes"], mad)
    assert math.isclose(profiles[port]["censored_share"], share)
    assert profiles[port]["visits"] == len(reconciled)
    assert profiles[port]["candidate_visits"] == sum(
        v["port_id"] == port and v["status"] == "candidate" for v in synthetic)


def test_pivot_unpivot_round_trip(enriched, synthetic):
    pivoted = analytics.pivot_span_buckets(enriched)
    long = analytics.unpivot_span_buckets(pivoted).collect()
    assert sum(r["visits"] for r in long) == sum(v["status"] == "reconciled" for v in synthetic)
    assert len(long) == pivoted.count() * 4


def test_join_strategies_and_row_equality(spark, enriched):
    ports = analytics.port_reference(spark, [f"SYN{i:02d}" for i in range(1, 13)])
    result = analytics.join_experiment(enriched, ports)
    assert result["rows_equal"] and result["broadcast_strategy"] and result["sort_merge_strategy"]
    assert result["rows"] == result["multi_join_rows"] == enriched.count()
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"  # restored


def test_statistics(enriched, synthetic):
    stats = analytics.descriptive_statistics(enriched)
    reconciled = [v["observed_span_seconds"] / 60 for v in synthetic if v["status"] == "reconciled"]
    assert stats["reconciled_visits"] == len(reconciled)
    assert math.isclose(stats["mean"], statistics.fmean(reconciled))
    assert stats["span_minutes_quantiles"]["p10"] <= stats["span_minutes_quantiles"]["p90"]


def test_time_series_fills_missing_days_and_uses_trailing_baseline(spark):
    rows = []
    for day, count in [(1, 2), (2, 2), (3, 2), (5, 2), (6, 2), (7, 2), (8, 2), (9, 2), (10, 9)]:
        for n in range(count):
            rows.append(_visit(f"99{day:03d}{n:04d}", "SYN01", f"2024-01-{day:02d}T0{n % 10}:00:00+00:00"))
    frame = analytics.enrich(analytics.visits_frame(spark, rows))
    series = {r["visit_date"].day: r for r in analytics.daily_time_series(frame).collect()}
    assert series[4]["arrivals"] == 0  # filled calendar day
    assert series[10]["baseline_days"] == 7 and series[10]["anomaly"] is True
    assert math.isclose(series[10]["baseline_mean"], (0 + 2 * 6) / 7)
    assert series[3]["z_score"] is None  # not enough history


def test_clustering_gate_refuses_small_fixture(spark):
    artifact = load_artifact(FIXTURES / "generation_a")
    frame = analytics.enrich(analytics.visits_frame(spark, list(artifact.visits)))
    result = analytics.cluster_ports(analytics.port_profiles(frame), synthetic=True)
    assert result["status"] == "insufficient_data" and result["gate"]["usable"] is False


def test_clustering_runs_on_synthetic_and_is_labelled(enriched):
    result = analytics.cluster_ports(analytics.port_profiles(enriched), k_values=(2, 3), seeds=(1, 2),
                                     synthetic=True)
    assert result["status"] == "evaluated" and result["label"] == "synthetic_plumbing_only"
    assert result["gate"]["qualifying_ports"] >= analytics.MIN_QUALIFYING_PORTS
    assert -1 <= result["selected"]["silhouette_mean"] <= 1
    assert 0 <= result["selected"]["seed_agreement"] <= 1


def test_graph_transitions_match_python(enriched, synthetic):
    result = analytics.graph_metrics(enriched, use_graphframes=False)
    expected = Counter()
    by_vessel = {}
    for v in sorted(synthetic, key=lambda v: (v["mmsi"], v["first_inside"], v["port_id"])):
        previous = by_vessel.get(v["mmsi"])
        if previous:
            expected[(previous, v["port_id"])] += 1
        by_vessel[v["mmsi"]] = v["port_id"]
    assert result["edges"] == len(expected)
    assert sum(d["in_transitions"] for d in result["degrees"]) == sum(expected.values())
    top = result["top_edges"][0]
    assert top["transitions"] == max(expected.values())


def _visit(mmsi, port, first):
    from harbortrace.contracts import digest
    return {"visit_id": digest(mmsi, port, first, "visits-v1"), "mmsi": mmsi, "port_id": port,
            "first_inside": first, "last_inside": first, "possible_entry_after": first,
            "possible_entry_by": first, "possible_exit_after": first, "possible_exit_by": first,
            "observed_span_seconds": 0, "observation_count": 1, "maximum_gap_seconds": 0,
            "left_censored": False, "right_censored": False, "quality_flags": [],
            "source_record_ids": ["a" * 64], "source_record_count": 1, "source_digest": "b" * 64,
            "rule_version": "visits-v1", "status": "reconciled"}
