r"""Time three Spark strategies for the same visits × ports × vessel totals query.

From the repository root with Java 17 and requirements-serving.txt installed:

    PYTHONPATH=src SPARK_LOCAL_IP=127.0.0.1 \
        PYSPARK_SUBMIT_ARGS='--driver-memory 1g --conf spark.ui.showConsoleProgress=false pyspark-shell' \
        python scripts/join_comparison.py --artifact ../inputs/engine-all-ports-2024-01-02_15 \
        --local-threads 2 --shuffle-partitions 8 --rounds 6 --output-dir data/analytics/joins-14-days

Use a fresh output directory. It receives the timings, executed plans, row checks
and one full Parquet copy of the verified result. The 14-day measurement is in
docs/evidence/joins-2024-01-02_15. Those plans abbreviate the local file location
to <batch-artifact>. The run used an Apple M1 Pro shared with other
services. Timings cover a local query over warmed inputs and exclude validation,
building dimensions, equality checks and writing the table.
"""
from __future__ import annotations

import argparse
import itertools
import json
import platform
import statistics
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from harbortrace import analytics
from harbortrace.serving import load_artifact, snapshot_visit_file


# The hints apply only to the two dimension sides, in join order.
STRATEGIES = {
    "merge_both": ("merge", "merge"),
    "broadcast_ports": ("broadcast", "merge"),
    "broadcast_both": ("broadcast", "broadcast"),
}


def join_visits(visits, ports, vessels, strategy):
    port_hint, vessel_hint = STRATEGIES[strategy]
    return analytics.join_visit_dimensions(visits, ports.hint(port_hint), vessels.hint(vessel_hint))


def differences(before, after):
    """Count full rows unique to each multiset, including excess copies."""
    return {"before_only": before.exceptAll(after).count(),
            "after_only": after.exceptAll(before).count()}


def measure(joined):
    # A bare count allows Catalyst to discard the result columns. This reduction
    # consumes every output field and returns a single bounded row to the driver.
    row_hash = F.xxhash64(*joined.columns).cast("decimal(38,0)")
    action = joined.agg(F.count("*").alias("rows"), F.sum(row_hash).alias("hash_sum"))
    start = time.perf_counter()
    row = action.collect()[0]
    seconds = time.perf_counter() - start
    return seconds, {"rows": row.rows, "hash_sum": str(row.hash_sum or 0)}, analytics.plan_text(action)


def compare(spark, path, expected_visits, out, rounds):
    visits = analytics.visits_frame(spark, path=path).select("visit_id", "mmsi", "port_id").cache()
    ports = vessels = None
    try:
        counts = {"visits": visits.count()}
        if counts["visits"] != expected_visits:
            raise ValueError(f"Expected {expected_visits} visits, read {counts['visits']}")
        port_ids = [r.port_id for r in visits.select("port_id").distinct().collect()]
        ports = analytics.port_reference(spark, port_ids).cache()
        vessels = analytics.vessel_totals(visits).cache()
        counts.update(ports=ports.count(), vessels=vessels.count())

        def query(name):
            return join_visits(visits, ports, vessels, name)

        measurements = {name: [] for name in STRATEGIES}
        warmups = {}
        plans = {}
        observations = []
        orders = itertools.cycle(itertools.permutations(STRATEGIES))
        for index in range(rounds + 1):
            order = tuple(STRATEGIES) if index == 0 else next(orders)
            for name in order:
                elapsed, signature, plan = measure(query(name))
                observations.append(signature)
                if index == 0:
                    warmups[name] = elapsed
                else:
                    measurements[name].append(elapsed)
                plans[name] = plan
        summary = {}
        for name, times in measurements.items():
            plan = plans[name]
            (out / f"plan_{name}.txt").write_text(plan.rstrip("\n") + "\n", encoding="utf-8")
            summary[name] = {
                "warmup_seconds": warmups[name], "trials_seconds": times,
                "median_seconds": statistics.median(times),
                "sort_merge_joins": plan.count("SortMergeJoin"),
                "broadcast_hash_joins": plan.count("BroadcastHashJoin"),
            }
        baseline = query("merge_both")
        comparisons = {name: differences(baseline, query(name)) for name in STRATEGIES if name != "merge_both"}
        plans_match = all(
            summary[name]["broadcast_hash_joins"] == hints.count("broadcast")
            and summary[name]["sort_merge_joins"] == hints.count("merge")
            for name, hints in STRATEGIES.items())
        signatures_match = all(entry == observations[0] for entry in observations)
        valid = (counts["visits"] == observations[0]["rows"] and signatures_match and plans_match
                 and all(diff == {"before_only": 0, "after_only": 0} for diff in comparisons.values()))
        if valid:
            # The three tables are multiset-equal. Keep one full Parquet copy outside Git.
            baseline.write.parquet(str(out / "joined_visits"))
        return {"input_rows": counts, "output_signature": observations[0], "strategies": summary,
                "signatures_match": signatures_match, "plans_match": plans_match,
                "differences_from_merge_both": comparisons, "valid_comparison": valid,
                "saved_output": "joined_visits" if valid else None}
    finally:
        for cached in (vessels, ports, visits):
            if cached is not None:
                cached.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--artifact", type=Path, required=True, help="Completed local batch artifact")
    parser.add_argument("--output-dir", type=Path, required=True, help="New directory for plans, results and Parquet")
    parser.add_argument("--local-threads", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--rounds", type=int, default=6, help="Measured rounds after one warm-up per strategy")
    args = parser.parse_args()
    if min(args.local_threads, args.shuffle_partitions, args.rounds) < 1:
        parser.error("--local-threads, --shuffle-partitions and --rounds must be positive")
    with tempfile.TemporaryDirectory(prefix="harbortrace-join-") as scratch:
        run(args, Path(scratch))


def run(args, scratch):
    out = args.output_dir.resolve()
    artifact = load_artifact(args.artifact)
    manifest = artifact.manifest
    path = snapshot_visit_file(artifact, scratch / "visits.jsonl")
    del artifact
    out.mkdir(parents=True, exist_ok=False)
    master = f"local[{args.local_threads}]"
    spark = analytics.analytics_session(master, shuffle_partitions=args.shuffle_partitions, name="harbortrace-joins")
    try:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        result = compare(spark, f"file:{path}", manifest["visit_count"], out, args.rounds)
        result["artifact"] = {key: manifest[key] for key in (
            "run_id", "visits_sha256", "status_counts", "synthetic", "rule_version")}
        result["execution"] = {
            "completed_at": datetime.now(timezone.utc).isoformat(), "spark_version": spark.version,
            "python_version": platform.python_version(), "machine": platform.machine(), "system": platform.system(),
            "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
            "master": master, "shuffle_partitions": args.shuffle_partitions,
            "adaptive_enabled": False, "auto_broadcast_threshold_bytes": -1, "rounds": args.rounds,
            "order": "one warm-up in declared order, then repeated permutations of the three strategies",
            "timed_action": "collect count and sum of xxhash64 over every output column; inputs materialized before timing",
        }
        (out / "results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
        if not result["valid_comparison"]:
            raise SystemExit("Join results or physical strategies differ from the expected comparison")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
