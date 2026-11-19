"""Run the Spark analytics suite on a completed batch artifact or on SYNTHETIC visits.

    PYTHONPATH=src python scripts/analytics_run.py --synthetic
    PYTHONPATH=src python scripts/analytics_run.py --artifact tests/fixtures/serving/generation_a
    PYTHONPATH=src python scripts/analytics_run.py --artifact data/serving/artifacts/<real-run>

Outputs (ignored by Git) go to data/analytics/<mode>-<UTC stamp>/: results.json, CSV tables and
physical plans. Synthetic results verify code paths only; they are not findings.
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from harbortrace import analytics
from harbortrace.serving import BatchArtifact, build_manifest, load_artifact
from harbortrace.contracts import digest
from harbortrace.visits import Rules

ROOT = Path(__file__).resolve().parents[1]


def save_csv(frame, path: Path, *, sum_columns=()) -> dict:
    """Export a small result table once and reuse those rows for reported totals."""
    rows = frame.collect()
    indices = {column: frame.columns.index(column) for column in sum_columns}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(frame.columns)
        writer.writerows(rows)
    return {"row_count": len(rows),
            "sums": {column: sum(row[index] or 0 for row in rows) for column, index in indices.items()}}


def input_provenance(visits, *, artifact: BatchArtifact | None = None,
                     seed: int | None = None, created_at: str | None = None) -> dict:
    """Preserve validated artifact identity or describe the canonical synthetic input."""
    if artifact is not None:
        return {**artifact.manifest, "generation_id": artifact.generation_id,
                "input_type": "completed_batch", "artifact_path": str(artifact.path.resolve())}
    if seed is None:
        raise ValueError("Synthetic input provenance requires a seed")
    manifest = build_manifest(
        visits, run_id=f"synthetic-seed-{seed}", rule_version=Rules().version, synthetic=True,
        created_at=created_at or datetime.now(timezone.utc).isoformat(),
        source={"generator": "harbortrace.analytics.synthetic_visits",
                "generator_parameters": {"seed": seed, "ports": 12, "vessels": 60, "days": 14},
                "hash_encoding": "canonical serving visits_jsonl (sorted compact JSON, UTF-8)"})
    return {**manifest, "generation_id": digest("generation", manifest["run_id"], manifest["visits_sha256"])[:24],
            "input_type": "generated_synthetic"}


def main():
    run_started = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--artifact", type=Path, help="Completed batch artifact directory (validated first)")
    source.add_argument("--synthetic", action="store_true", help="Generate labelled synthetic visits")
    parser.add_argument("--seed", type=int, default=20261102)
    parser.add_argument("--spark-master", default="local[2]")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--packages", help="Extra Spark packages, e.g. io.graphframes:graphframes-spark3_2.12:0.12.3")
    parser.add_argument("--repositories", help="Extra package repositories, e.g. https://repos.spark-packages.org")
    args = parser.parse_args()

    started = time.perf_counter()
    artifact = None
    if args.synthetic:
        visits, synthetic, mode = analytics.synthetic_visits(seed=args.seed), True, "synthetic"
    else:
        artifact = load_artifact(args.artifact)
        visits, synthetic, mode = list(artifact.visits), artifact.manifest["synthetic"], "artifact"
    provenance = input_provenance(visits, artifact=artifact, seed=args.seed, created_at=started_at)
    timings: dict = {"input": time.perf_counter() - started}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = (args.output_dir or ROOT / "data/analytics" / f"{mode}-{stamp}").resolve()
    out.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    spark = analytics.analytics_session(args.spark_master, warehouse=str(out / "warehouse"),
                                        packages=args.packages, repositories=args.repositories)
    timings["spark_startup"] = time.perf_counter() - started
    results: dict = {"mode": mode, "synthetic": synthetic, "input_visits": len(visits),
                     "spark_version": spark.version, "input_provenance": provenance,
                     "analysis_run_id": f"analytics-{stamp}-{time.time_ns()}", "started_at": started_at,
                     "execution": {"spark_master": args.spark_master, "packages": args.packages,
                                   "repositories": args.repositories,
                                   "timing_scope": "wall-clock phases include Spark actions and CSV/plan exports; total ends after Spark shutdown before results.json export",
                                   "performance_claim": "No before/after speedup measurement performed"}}
    cached = []
    try:
        started = time.perf_counter()
        frame = analytics.visits_frame(spark, visits)
        enriched = analytics.enrich(frame).cache()
        cached.append(enriched)
        results["enriched_rows"] = enriched.count()
        timings["enrich"] = time.perf_counter() - started

        started = time.perf_counter()
        profiles = analytics.port_profiles(enriched).cache()
        cached.append(profiles)
        profile_export = save_csv(profiles, out / "port_profiles.csv", sum_columns=("visits",))
        results["port_profiles"] = profile_export["row_count"]
        # Export materializes the cache once; unpivot reads it rather than rerunning its aggregate.
        pivoted = analytics.pivot_span_buckets(enriched).cache()
        cached.append(pivoted)
        results["pivot_rows"] = save_csv(pivoted, out / "span_bucket_pivot.csv")["row_count"]
        long = analytics.unpivot_span_buckets(pivoted)
        long_export = save_csv(long, out / "span_bucket_long.csv", sum_columns=("visits",))
        results["unpivot_rows"] = long_export["row_count"]
        results["pivot_unpivot_total_preserved"] = long_export["sums"]["visits"] == profile_export["sums"]["visits"]
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
        series_export = save_csv(series, out / "daily_series.csv", sum_columns=("anomaly",))
        results["time_series_rows"] = series_export["row_count"]
        results["time_series_anomalies"] = series_export["sums"]["anomaly"]
        timings["statistics"] = time.perf_counter() - started

        started = time.perf_counter()
        results["clustering"] = analytics.cluster_ports(profiles, synthetic=synthetic)
        timings["mllib"] = time.perf_counter() - started

        started = time.perf_counter()
        results["graph"] = analytics.graph_metrics(enriched)
        timings["graph"] = time.perf_counter() - started
    finally:
        started = time.perf_counter()
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()
        timings["spark_shutdown"] = time.perf_counter() - started
    timings["total"] = time.perf_counter() - run_started
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    results["timings_seconds"] = {k: round(v, 6) for k, v in timings.items()}
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
