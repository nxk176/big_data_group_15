"""Spark analytics over reconciled visit outputs.

Inputs are complete visit records (the batch artifact or the active serving generation).
Every function returns DataFrames or plain dictionaries so the runner can save results,
print plans and attach evidence. Results computed on ``synthetic_visits`` exercise the
code path only; they are not findings about real vessel behaviour.

Analytical scope (see docs/serving_analytics_guide.md):

* multistage transformations, window functions, pivot/unpivot, a Python UDF and custom
  aggregates written as zero/merge/finish folds;
* broadcast vs sort-merge join plans with row-equality checks, plus a multiple join;
* descriptive statistics and a daily time series with rolling baseline and anomaly flags;
* MLlib port-profile clustering behind an explicit usable-sample gate;
* port-transition graph metrics with GraphFrames when available, DataFrame fallback otherwise.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from .contracts import Observation, digest
from .visits import Rules, reconstruct

# Usable-sample criteria for the port clustering experiment (project method choice).
MIN_QUALIFYING_PORTS = 10
MIN_COMPLETE_VISITS_PER_PORT = 20


# --------------------------------------------------------------------------- synthetic input

def synthetic_visits(*, ports: int = 12, vessels: int = 60, days: int = 14, seed: int = 20261102,
                     rules: Rules | None = None) -> list[dict]:
    """Generate SYNTHETIC complete visit records by running the reference state machine.

    Port IDs are prefixed ``SYN`` and MMSIs start with 99 so the output can never be
    mistaken for NOAA data. Port profiles differ deliberately so the plumbing has
    something to separate; a clustering result on this input proves nothing about ports.
    """
    rng = random.Random(seed)
    rules = rules or Rules()
    port_ids = [f"SYN{index:02d}" for index in range(1, ports + 1)]
    profile = {port: (rng.choice([30, 90, 240, 600]), rng.choice([0.0, 0.02, 0.06])) for port in port_ids}
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    observations: list[Observation] = []
    row = 0

    def emit(mmsi: str, at: datetime, port: str | None):
        nonlocal row
        row += 1
        lon, lat = (-118.2 + rng.uniform(-0.01, 0.01), 33.7 + rng.uniform(-0.01, 0.01))
        observations.append(Observation(
            mmsi=mmsi, event_time=at, longitude=lon, latitude=lat, source_file="synthetic",
            source_row=row, source_record_id=digest("synthetic", seed, row),
            logical_id=digest(mmsi, at.isoformat(), lon, lat), assignment=port))

    for vessel in range(vessels):
        mmsi = f"99{vessel:07d}"
        clock = start + timedelta(minutes=rng.randint(0, 600))
        end = start + timedelta(days=days)
        while clock < end:
            port = rng.choice(port_ids)
            mean_minutes, gap_rate = profile[port]
            duration = max(15, int(rng.gauss(mean_minutes, mean_minutes * 0.25)))
            emit(mmsi, clock, None)
            at = clock + timedelta(minutes=5)
            stop = at + timedelta(minutes=duration)
            while at <= stop:
                emit(mmsi, at, port)
                step = 5 if rng.random() > gap_rate else 40  # occasional long gap
                at += timedelta(minutes=step)
            if rng.random() > 0.1:  # sometimes the exit is never observed
                emit(mmsi, at + timedelta(minutes=5), None)
                emit(mmsi, at + timedelta(minutes=16), None)
            clock = at + timedelta(hours=rng.randint(2, 30))
    visits = reconstruct(observations, port_ids, rules)
    return [visit.as_json() | {"quality_flags": list(visit.quality_flags),
                               "source_record_ids": list(visit.source_record_ids)} for visit in visits]


# --------------------------------------------------------------------------- session

# JVM flags Spark 3.5 needs for off-heap buffers on Java 17+.
JAVA_OPTIONS = ("-Dio.netty.tryReflectionSetAccessible=true "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")


def analytics_session(master: str = "local[2]", *, warehouse: str | None = None, shuffle_partitions: int = 4,
                      packages: str | None = None, repositories: str | None = None,
                      name: str = "harbortrace-analytics"):
    import os
    import sys
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (SparkSession.builder.master(master).appName(name)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               .config("spark.driver.extraJavaOptions", JAVA_OPTIONS)
               .config("spark.executor.extraJavaOptions", JAVA_OPTIONS)
               .config("spark.ui.enabled", "false"))
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    if repositories:
        builder = builder.config("spark.jars.repositories", repositories)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --------------------------------------------------------------------------- frames

def visit_schema():
    from pyspark.sql import types as T
    return T.StructType([
        T.StructField("visit_id", T.StringType(), False), T.StructField("mmsi", T.StringType(), False),
        T.StructField("port_id", T.StringType(), False), T.StructField("first_inside", T.StringType(), False),
        T.StructField("last_inside", T.StringType(), False),
        T.StructField("possible_entry_after", T.StringType(), True),
        T.StructField("possible_entry_by", T.StringType(), False),
        T.StructField("possible_exit_after", T.StringType(), False),
        T.StructField("possible_exit_by", T.StringType(), True),
        T.StructField("observed_span_seconds", T.LongType(), False),
        T.StructField("observation_count", T.LongType(), False),
        T.StructField("maximum_gap_seconds", T.LongType(), False),
        T.StructField("left_censored", T.BooleanType(), False),
        T.StructField("right_censored", T.BooleanType(), False),
        T.StructField("quality_flags", T.ArrayType(T.StringType()), False),
        T.StructField("source_record_ids", T.ArrayType(T.StringType()), False),
        T.StructField("source_record_count", T.LongType(), False),
        T.StructField("source_digest", T.StringType(), False),
        T.StructField("rule_version", T.StringType(), False), T.StructField("status", T.StringType(), False),
    ])


def visits_frame(spark, visits: list[dict] | None = None, path: str | None = None):
    """Typed visit frame with parsed timestamps (from rows or a JSONL file/directory)."""
    from pyspark.sql import functions as F
    if (visits is None) == (path is None):
        raise ValueError("Pass exactly one of visits or path")
    frame = (spark.createDataFrame(visits, schema=visit_schema()) if visits is not None
             else spark.read.schema(visit_schema()).json(path))
    for name in ("first_inside", "last_inside", "possible_entry_after", "possible_exit_by"):
        frame = frame.withColumn(f"{name}_ts", F.to_timestamp(name))
    return frame


def port_reference(spark, port_ids: list[str]):
    """Small port dimension table (the broadcast side of the join experiments)."""
    rows = [(port, f"Port {port}", "synthetic" if port.startswith("SYN") else "usace") for port in sorted(port_ids)]
    return spark.createDataFrame(rows, "port_id string, port_name string, boundary_source string")


# --------------------------------------------------------------------------- multistage transformations

def enrich(frame):
    """Stage 1: derived columns, a Python UDF and window features per vessel and port-day."""
    from pyspark.sql import Window, functions as F, types as T

    @F.udf(T.StringType())
    def span_bucket(seconds):
        # Python UDF: readable duration classes for the dashboard and pivot tables.
        if seconds is None:
            return None
        minutes = seconds / 60
        if minutes < 30:
            return "lt_30m"
        if minutes < 120:
            return "30m_2h"
        if minutes < 360:
            return "2h_6h"
        return "ge_6h"

    vessel = Window.partitionBy("mmsi").orderBy("first_inside_ts", "port_id")
    port_day = Window.partitionBy("port_id", "visit_date").orderBy("first_inside_ts", "mmsi")
    rolling = vessel.rowsBetween(-2, 0)
    return (frame
            .withColumn("visit_date", F.to_date("first_inside_ts"))
            .withColumn("first_hour", F.date_trunc("hour", "first_inside_ts"))
            .withColumn("span_minutes", F.col("observed_span_seconds") / 60.0)
            .withColumn("span_bucket", span_bucket("observed_span_seconds"))
            .withColumn("censored", F.col("left_censored") | F.col("right_censored"))
            .withColumn("previous_port", F.lag("port_id").over(vessel))
            .withColumn("next_port", F.lead("port_id").over(vessel))
            .withColumn("hours_since_previous_visit",
                        (F.col("first_inside_ts").cast("long") - F.lag("last_inside_ts").over(vessel).cast("long")) / 3600.0)
            .withColumn("vessel_visit_number", F.row_number().over(vessel))
            .withColumn("rolling_3_span_minutes", F.avg("span_minutes").over(rolling))
            .withColumn("arrival_rank_in_port_day", F.dense_rank().over(port_day)))


def censored_share_agg(left: str = "left_censored", right: str = "right_censored"):
    """Custom aggregate: share of visits with any censored bound.

    Written as an explicit zero/merge/finish fold (Spark's ``aggregate`` higher-order
    function) so it runs inside the JVM without Python serialization or Arrow.
    """
    from pyspark.sql import functions as F
    items = F.collect_list(F.struct(F.col(left).alias("l"), F.col(right).alias("r")))
    zero = F.struct(F.lit(0).alias("n"), F.lit(0).alias("c"))
    return F.aggregate(
        items, zero,
        lambda acc, x: F.struct((acc["n"] + 1).alias("n"),
                                (acc["c"] + F.when(x["l"] | x["r"], 1).otherwise(0)).alias("c")),
        lambda acc: F.when(acc["n"] > 0, acc["c"] / acc["n"]))


def span_mad_agg(column: str = "span_minutes"):
    """Custom aggregate: exact median absolute deviation (robust spread) of a numeric column."""
    from pyspark.sql import functions as F

    def median(values):
        n = F.size(values)
        lower = F.element_at(values, ((n + 1) / 2).cast("int"))
        upper = F.element_at(values, (n / 2 + 1).cast("int"))
        return F.when(n == 0, F.lit(None)).when(n % 2 == 1, lower).otherwise((lower + upper) / 2)

    values = F.array_sort(F.collect_list(F.col(column).cast("double")))
    center = median(values)
    deviations = F.array_sort(F.transform(values, lambda x: F.abs(x - center)))
    return median(deviations)


def port_profiles(enriched):
    """Stage 2: per-port aggregate profile (reconciled visits only)."""
    from pyspark.sql import functions as F
    reconciled = enriched.where(F.col("status") == "reconciled")
    profiles = reconciled.groupBy("port_id").agg(
        F.count("*").alias("visits"),
        F.sum(F.when(~F.col("censored"), 1).otherwise(0)).alias("complete_visits"),
        F.countDistinct("mmsi").alias("vessels"),
        F.expr("percentile_approx(span_minutes, 0.5)").alias("median_span_minutes"),
        span_mad_agg("span_minutes").alias("span_mad_minutes"),
        censored_share_agg().alias("censored_share"),
        F.avg(F.when(F.array_contains("quality_flags", "long_gap_unknown_continuity"), 1.0).otherwise(0.0))
        .alias("long_gap_share"),
        F.avg(F.when(F.hour("first_inside_ts").between(0, 5), 1.0).otherwise(0.0)).alias("night_arrival_share"),
        F.avg("observation_count").alias("mean_observations"),
    )
    candidates = enriched.where(F.col("status") == "candidate").groupBy("port_id").agg(
        F.count("*").alias("candidate_visits"))
    return (profiles.join(candidates, "port_id", "left")
            .withColumn("candidate_visits", F.coalesce("candidate_visits", F.lit(0)))
            .withColumn("visits_per_vessel", F.col("visits") / F.col("vessels"))
            .orderBy("port_id"))


def pivot_span_buckets(enriched):
    """Pivot: reconciled visit counts per port (rows) by duration bucket (columns)."""
    from pyspark.sql import functions as F
    buckets = ["lt_30m", "30m_2h", "2h_6h", "ge_6h"]
    return (enriched.where(F.col("status") == "reconciled")
            .groupBy("port_id").pivot("span_bucket", buckets).count()
            .fillna(0, subset=buckets).orderBy("port_id"))


def unpivot_span_buckets(pivoted):
    """Unpivot (Spark 3.4+ DataFrame.unpivot) back to long format; inverse of the pivot."""
    columns = [c for c in pivoted.columns if c != "port_id"]
    return pivoted.unpivot("port_id", columns, "span_bucket", "visits").orderBy("port_id", "span_bucket")


# --------------------------------------------------------------------------- joins

def plan_text(frame) -> str:
    return frame._jdf.queryExecution().executedPlan().toString()


def join_experiment(enriched, ports):
    """Same logical join executed with broadcast and sort-merge hints; rows must agree.

    Adaptive execution is disabled for the duration so the chosen physical strategy is
    visible in the static plan; the caller's setting is restored afterwards.
    """
    from pyspark.sql import functions as F
    spark = enriched.sparkSession
    previous = spark.conf.get("spark.sql.adaptive.enabled")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    try:
        columns = ["visit_id", "port_id", "port_name", "boundary_source", "span_minutes"]
        broadcast = enriched.join(F.broadcast(ports), "port_id").select(*columns)
        merge = enriched.join(ports.hint("merge"), "port_id").select(*columns)
        broadcast_plan, merge_plan = plan_text(broadcast), plan_text(merge)
        left = sorted(tuple(r) for r in broadcast.collect())
        right = sorted(tuple(r) for r in merge.collect())
        # Multiple join: visits x ports x per-vessel statistics.
        vessel_stats = enriched.groupBy("mmsi").agg(F.count("*").alias("vessel_visits"),
                                                    F.countDistinct("port_id").alias("vessel_ports"))
        multi = (enriched.join(F.broadcast(ports), "port_id").join(vessel_stats, "mmsi")
                 .select("visit_id", "mmsi", "port_name", "vessel_visits", "vessel_ports"))
        multi_plan = plan_text(multi)
        multi_rows = multi.count()
    finally:
        spark.conf.set("spark.sql.adaptive.enabled", previous)
    return {
        "rows": len(left), "rows_equal": left == right,
        "broadcast_strategy": "BroadcastHashJoin" in broadcast_plan,
        "sort_merge_strategy": "SortMergeJoin" in merge_plan,
        "multi_join_rows": multi_rows,
        "plans": {"broadcast": broadcast_plan, "sort_merge": merge_plan, "multi_join": multi_plan},
    }


# --------------------------------------------------------------------------- statistics / time series

def descriptive_statistics(enriched) -> dict:
    from pyspark.sql import functions as F
    reconciled = enriched.where(F.col("status") == "reconciled").cache()
    try:
        total = reconciled.count()
        if total == 0:
            return {"reconciled_visits": 0}
        quantiles = reconciled.approxQuantile("span_minutes", [0.1, 0.25, 0.5, 0.75, 0.9], 0.001)
        row = reconciled.agg(F.mean("span_minutes").alias("mean"), F.stddev("span_minutes").alias("stddev"),
                             F.skewness("span_minutes").alias("skewness"),
                             F.corr("span_minutes", "observation_count").alias("corr_span_observations"),
                             F.avg(F.col("censored").cast("double")).alias("censored_share")).first()
        return {"reconciled_visits": total,
                "span_minutes_quantiles": dict(zip(["p10", "p25", "p50", "p75", "p90"], quantiles)),
                **{key: (None if row[key] is None else float(row[key])) for key in row.asDict()}}
    finally:
        reconciled.unpersist()


def daily_time_series(enriched, *, baseline_days: int = 7, z_threshold: float = 2.0):
    """Daily reconciled arrivals per port with a trailing baseline and z-score anomaly flag.

    Days without visits are filled with zero so the rolling window counts calendar days.
    The baseline excludes the current day.
    """
    from pyspark.sql import Window, functions as F
    reconciled = enriched.where(F.col("status") == "reconciled")
    daily = reconciled.groupBy("port_id", "visit_date").agg(F.count("*").alias("arrivals"),
                                                           F.sum("span_minutes").alias("observed_minutes"))
    bounds = reconciled.groupBy("port_id").agg(F.min("visit_date").alias("lo"), F.max("visit_date").alias("hi"))
    calendar = bounds.select("port_id", F.explode(F.sequence("lo", "hi")).alias("visit_date"))
    series = (calendar.join(daily, ["port_id", "visit_date"], "left")
              .fillna(0, subset=["arrivals", "observed_minutes"]))
    window = Window.partitionBy("port_id").orderBy("visit_date").rowsBetween(-baseline_days, -1)
    series = (series.withColumn("baseline_mean", F.avg("arrivals").over(window))
              .withColumn("baseline_std", F.stddev_samp("arrivals").over(window))
              .withColumn("baseline_days", F.count("arrivals").over(window))
              .withColumn("z_score", F.when((F.col("baseline_days") >= baseline_days) & (F.col("baseline_std") > 0),
                                            (F.col("arrivals") - F.col("baseline_mean")) / F.col("baseline_std")))
              .withColumn("anomaly", F.coalesce(F.abs("z_score") >= z_threshold, F.lit(False))))
    return series.orderBy("port_id", "visit_date")


# --------------------------------------------------------------------------- MLlib

FEATURES = ["median_span_minutes", "span_mad_minutes", "censored_share", "long_gap_share",
            "night_arrival_share", "visits_per_vessel", "mean_observations"]


def clustering_gate(profiles, *, min_ports: int = MIN_QUALIFYING_PORTS,
                    min_complete: int = MIN_COMPLETE_VISITS_PER_PORT) -> dict:
    rows = profiles.select("port_id", "complete_visits").collect()
    qualifying = sorted(r["port_id"] for r in rows if r["complete_visits"] >= min_complete)
    return {"ports": len(rows), "qualifying_ports": len(qualifying), "qualifying_port_ids": qualifying,
            "min_ports": min_ports, "min_complete_visits_per_port": min_complete,
            "usable": len(qualifying) >= min_ports}


def cluster_ports(profiles, *, k_values=(2, 3, 4), seeds=(1, 2, 3), synthetic: bool,
                  min_ports: int = MIN_QUALIFYING_PORTS,
                  min_complete: int = MIN_COMPLETE_VISITS_PER_PORT) -> dict:
    """KMeans port-profile clustering evaluated by silhouette and seed stability.

    Refuses to fit when the usable-sample gate fails; it never pads the input.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.sql import functions as F

    gate = clustering_gate(profiles, min_ports=min_ports, min_complete=min_complete)
    label = "synthetic_plumbing_only" if synthetic else "real_data"
    if not gate["usable"]:
        return {"status": "insufficient_data", "label": label, "gate": gate}
    data = profiles.where(F.col("port_id").isin(gate["qualifying_port_ids"])).fillna(0.0, subset=FEATURES).cache()
    try:
        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette")
        results = []
        for k in k_values:
            if k >= gate["qualifying_ports"]:
                continue
            scores, assignments = [], []
            for seed in seeds:
                pipeline = Pipeline(stages=[
                    VectorAssembler(inputCols=FEATURES, outputCol="raw_features"),
                    StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
                    KMeans(k=k, seed=seed, featuresCol="features", predictionCol="cluster"),
                ])
                predicted = pipeline.fit(data).transform(data)
                scores.append(evaluator.evaluate(predicted))
                assignments.append({r["port_id"]: r["cluster"] for r in predicted.select("port_id", "cluster").collect()})
            results.append({"k": k, "silhouette_mean": sum(scores) / len(scores),
                            "silhouette_min": min(scores), "silhouette_max": max(scores),
                            "seed_agreement": _pair_agreement(assignments), "assignment": assignments[0]})
        best = max(results, key=lambda r: (r["silhouette_mean"], -r["k"])) if results else None
        return {"status": "evaluated", "label": label, "gate": gate, "features": FEATURES,
                "candidates": results, "selected": best}
    finally:
        data.unpersist()


def _pair_agreement(assignments: list[dict]) -> float:
    """Rand-index style agreement of co-membership across seeds (1.0 = identical partitions)."""
    if len(assignments) < 2:
        return 1.0
    keys = sorted(assignments[0])
    agree = total = 0
    for a in range(len(assignments)):
        for b in range(a + 1, len(assignments)):
            for i in range(len(keys)):
                for j in range(i + 1, len(keys)):
                    left = assignments[a][keys[i]] == assignments[a][keys[j]]
                    right = assignments[b][keys[i]] == assignments[b][keys[j]]
                    agree += left == right
                    total += 1
    return agree / total if total else 1.0


# --------------------------------------------------------------------------- graph

def port_transitions(enriched):
    """Directed edges previous_port -> port_id from consecutive visits of the same vessel."""
    from pyspark.sql import functions as F
    return (enriched.where(F.col("previous_port").isNotNull())
            .groupBy(F.col("previous_port").alias("src"), F.col("port_id").alias("dst"))
            .agg(F.count("*").alias("transitions"), F.countDistinct("mmsi").alias("vessels"))
            .orderBy(F.desc("transitions"), "src", "dst"))


def graph_metrics(enriched, *, use_graphframes: bool = True) -> dict:
    """Port transition graph: degrees, PageRank and components.

    Uses GraphFrames when the package is installed and its JAR is on the Spark classpath;
    otherwise reports DataFrame-computed degrees and states that PageRank was not run.
    """
    from pyspark.sql import functions as F
    edges = port_transitions(enriched).cache()
    vertices = enriched.select(F.col("port_id").alias("id")).distinct()
    try:
        out_degree = edges.groupBy(F.col("src").alias("id")).agg(F.sum("transitions").alias("out_transitions"))
        in_degree = edges.groupBy(F.col("dst").alias("id")).agg(F.sum("transitions").alias("in_transitions"))
        degrees = (vertices.join(out_degree, "id", "left").join(in_degree, "id", "left")
                   .fillna(0).orderBy(F.desc("in_transitions"), "id"))
        result = {"engine": "dataframe", "vertices": vertices.count(), "edges": edges.count(),
                  "degrees": [r.asDict() for r in degrees.collect()],
                  "top_edges": [r.asDict() for r in edges.limit(10).collect()]}
        if use_graphframes:
            try:
                from graphframes import GraphFrame
                graph = GraphFrame(vertices, edges.withColumnRenamed("transitions", "weight"))
                ranks = graph.pageRank(resetProbability=0.15, maxIter=20).vertices
                result["engine"] = "graphframes"
                result["pagerank"] = [r.asDict() for r in ranks.orderBy(F.desc("pagerank")).collect()]
                components = graph.stronglyConnectedComponents(maxIter=10)
                result["strongly_connected_components"] = components.select("component").distinct().count()
            except Exception as error:  # package or JAR missing
                result["graphframes_unavailable"] = f"{type(error).__name__}: {str(error)[:200]}"
        return result
    finally:
        edges.unpersist()
