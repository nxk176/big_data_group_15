"""Run the Spark analytics suite on a completed batch artifact or on SYNTHETIC visits.

    PYTHONPATH=src python scripts/analytics_run.py --synthetic
    PYTHONPATH=src python scripts/analytics_run.py --artifact tests/fixtures/serving/generation_a
    PYTHONPATH=src python scripts/analytics_run.py --artifact data/serving/artifacts/<real-run>

Outputs (ignored by Git) go to data/analytics/<mode>-<UTC stamp>/: results.json, CSV tables and
physical plans. Synthetic results verify code paths only; they are not findings.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from harbortrace import analytics
from harbortrace.serving import load_artifact

ROOT = Path(__file__).resolve().parents[1]


def save_csv(frame, path: Path):
    rows = frame.collect()
    with path.open("w", encoding="utf-8") as handle:
        handle.write(",".join(frame.columns) + "\n")
        for row in rows:
            handle.write(",".join("" if v is None else str(v).replace(",", ";") for v in row) + "\n")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--artifact", type=Path, help="Completed batch artifact directory (validated first)")
    source.add_argument("--synthetic", action="store_true", help="Generate labelled synthetic visits")
    parser.add_argument("--seed", type=int, default=20261102)
    parser.add_argument("--spark-master", default="local[2]")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--packages", help="Extra Spark packages, e.g. graphframes:graphframes:0.8.4-spark3.5-s_2.12")
    parser.add_argument("--repositories", help="Extra package repositories, e.g. https://repos.spark-packages.org")
    args = parser.parse_args()

    if args.synthetic:
        visits, synthetic, mode = analytics.synthetic_visits(seed=args.seed), True, "synthetic"
    else:
        artifact = load_artifact(args.artifact)
        visits, synthetic, mode = list(artifact.visits), artifact.manifest["synthetic"], "artifact"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = (args.output_dir or ROOT / "data/analytics" / f"{mode}-{stamp}").resolve()
    out.mkdir(parents=True, exist_ok=False)
    spark = analytics.analytics_session(args.spark_master, warehouse=str(out / "warehouse"),
                                        packages=args.packages, repositories=args.repositories)
    results: dict = {"mode": mode, "synthetic": synthetic, "input_visits": len(visits), "spark_version": spark.version}
    timings: dict = {}
    try:
        started = time.perf_counter()
        frame = analytics.visits_frame(spark, visits)
        enriched = analytics.enrich(frame).cache()
        results["enriched_rows"] = enriched.count()
        timings["enrich"] = time.perf_counter() - started

        started = time.perf_counter()
        profiles = analytics.port_profiles(enriched).cache()
        results["port_profiles"] = save_csv(profiles, out / "port_profiles.csv")
        pivoted = analytics.pivot_span_buckets(enriched)
        results["pivot_rows"] = save_csv(pivoted, out / "span_bucket_pivot.csv")
        long = analytics.unpivot_span_buckets(pivoted)
        results["unpivot_rows"] = save_csv(long, out / "span_bucket_long.csv")
        results["pivot_unpivot_total_preserved"] = (
            sum(r["visits"] for r in long.collect())
            == enriched.where("status = 'reconciled'").count())
        timings["aggregate"] = time.perf_counter() - started

        started = time.perf_counter()
        ports = analytics.port_reference(spark, [r["port_id"] for r in enriched.select("port_id").distinct().collect()])
        joins = analytics.join_experiment(enriched, ports)
        for name, plan in joins.pop("plans").items():
            (out / f"plan_{name}.txt").write_text(plan + "\n", encoding="utf-8")
        results["joins"] = joins
        timings["joins"] = time.perf_counter() - started

        started = time.perf_counter()
        results["statistics"] = analytics.descriptive_statistics(enriched)
        series = analytics.daily_time_series(enriched)
        results["time_series_rows"] = save_csv(series, out / "daily_series.csv")
        results["time_series_anomalies"] = series.where("anomaly").count()
        timings["statistics"] = time.perf_counter() - started

        started = time.perf_counter()
        results["clustering"] = analytics.cluster_ports(profiles, synthetic=synthetic)
        timings["mllib"] = time.perf_counter() - started

        started = time.perf_counter()
        results["graph"] = analytics.graph_metrics(enriched)
        timings["graph"] = time.perf_counter() - started
        results["timings_seconds"] = {k: round(v, 3) for k, v in timings.items()}
    finally:
        spark.stop()
    (out / "results.json").write_text(json.dumps(results, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    print("\nHARBORTRACE ANALYTICS" + (" (SYNTHETIC INPUT - code-path evidence only)" if synthetic else ""))
    print(f"Input visits: {len(visits)}; ports profiled: {results['port_profiles']}")
    print(f"Pivot/unpivot totals preserved: {results['pivot_unpivot_total_preserved']}")
    j = results["joins"]
    print(f"Joins: rows={j['rows']} equal={j['rows_equal']} broadcast={j['broadcast_strategy']} "
          f"sort-merge={j['sort_merge_strategy']} multi-join rows={j['multi_join_rows']}")
    s = results["statistics"]
    print(f"Statistics: reconciled={s.get('reconciled_visits')} median span min={s.get('span_minutes_quantiles', {}).get('p50')} "
          f"corr(span, observations)={s.get('corr_span_observations')}")
    print(f"Time series: {results['time_series_rows']} port-days, {results['time_series_anomalies']} flagged")
    c = results["clustering"]
    if c["status"] == "evaluated":
        best = c["selected"]
        print(f"MLlib KMeans [{c['label']}]: {c['gate']['qualifying_ports']} qualifying ports; selected k={best['k']} "
              f"silhouette={best['silhouette_mean']:.3f} seed agreement={best['seed_agreement']:.2f}")
    else:
        print(f"MLlib KMeans: not fitted ({c['status']}): {c['gate']['qualifying_ports']} qualifying ports "
              f"< {c['gate']['min_ports']} required with >= {c['gate']['min_complete_visits_per_port']} complete visits each")
    g = results["graph"]
    print(f"Graph ({g['engine']}): {g['vertices']} ports, {g['edges']} transition edges"
          + (f"; GraphFrames unavailable: {g['graphframes_unavailable']}" if "graphframes_unavailable" in g else ""))
    print(f"Timings (s): {results['timings_seconds']}")
    print(f"Artifacts: {out}")


if __name__ == "__main__":
    main()
