"""Batch processing engine: canonical observations in, a completed-batch artifact out.

The engine reads canonical observations (Parquet or JSONL, local or ``hdfs://``),
reconstructs visits with Spark and writes the artifact the serving component imports:
``visits.jsonl`` plus a ``manifest.json`` that is written last and marks completion.

It has no dependency on a laptop configuration file or on the serving code. The Spark
master may be left to ``spark-submit`` so the same job runs locally and on a cluster.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from .contracts import CONTRACT_VERSION, Observation
from .visits import Rules, VesselTracker

ARTIFACT_TYPE = "completed_batch"
OBSERVATION_DDL = ("mmsi string, event_time string, longitude double, latitude double, source_file string, "
                   "source_row long, source_record_id string, logical_id string, replay_id string, sog double, "
                   "quality_flags array<string>, assignment string, assignment_flags array<string>")
VISIT_DDL = ("visit_id string, mmsi string, port_id string, first_inside string, last_inside string, "
             "possible_entry_after string, possible_entry_by string, possible_exit_after string, "
             "possible_exit_by string, observed_span_seconds long, observation_count long, "
             "maximum_gap_seconds long, left_censored boolean, right_censored boolean, "
             "quality_flags array<string>, source_record_ids array<string>, source_record_count long, "
             "source_digest string, rule_version string, status string")
REQUIRED_KEYS = ("mmsi", "event_time", "logical_id", "source_record_id")
VISIT_ORDER = ("mmsi", "port_id", "first_inside", "visit_id")


class EngineError(RuntimeError):
    """The input or the reconstructed output failed a check; no artifact was published."""


def engine_session(name: str, *, master: str | None = "local[2]", driver_memory: str = "2g",
                   shuffle_partitions: int = 2, ui: bool = False, warehouse: str | None = None,
                   packages: str | None = None):
    """Create a Spark session. ``master=None`` leaves the master to ``spark-submit``."""
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.appName(name)
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", str(ui).lower()))
    if master:
        builder = builder.master(master)
        if master.startswith("local"):
            # Local workers must use this interpreter; a cluster keeps its own Python.
            os.environ["PYSPARK_PYTHON"] = sys.executable
            builder = (builder.config("spark.pyspark.python", sys.executable)
                       .config("spark.driver.memory", driver_memory))
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _ship_package(spark)
    return spark


def _ship_package(spark) -> None:
    """Send this package to the Python workers.

    The reconstruction runs inside worker processes, which do not inherit the driver's
    import path; on a cluster they are other machines without a checkout.
    """
    package = Path(__file__).resolve().parent
    archive = Path(tempfile.mkdtemp(prefix="harbortrace-")) / "harbortrace.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for path in sorted(package.glob("*.py")):
            bundle.write(path, f"{package.name}/{path.name}")
    spark.sparkContext.addPyFile(str(archive))


def read_observations(spark, inputs: list[str]):
    """Read canonical observations with the contract schema instead of inferring one."""
    paths = [str(path) for path in inputs]
    if not paths or len(paths) != len(set(paths)):
        raise EngineError("Input paths must be a non-empty list without repeats")
    is_json = [path.endswith((".jsonl", ".json")) for path in paths]
    if any(is_json) and not all(is_json):
        raise EngineError("Mixing JSONL and Parquet inputs in one run is not supported")
    if all(is_json):
        return spark.read.schema(OBSERVATION_DDL).json(paths)
    from pyspark.sql import types as T
    frame = spark.read.parquet(*paths)
    schema = T._parse_datatype_string(OBSERVATION_DDL)
    absent = [field.name for field in schema if field.name not in frame.columns]
    if absent:
        raise EngineError(f"Canonical columns missing: {absent}")
    return frame.select([frame[field.name].cast(field.dataType).alias(field.name) for field in schema])


def reconstruct_visits(spark, observations, ports: list[str] | None, rules: Rules, partitions: int):
    """Return (visits, ordered observations). One shuffle by vessel, one sort per partition.

    Every vessel's observations reach one partition in event-time order, so the pure
    state machines in ``visits.py`` run unchanged inside each partition. ``ports=None``
    reconstructs visits for every port that appears in the assignments. The visit schema
    is explicit: Spark does not scan the output a second time to infer it.
    """
    from pyspark.sql import functions as F
    ordered = (observations.withColumn("_event_ts", F.to_timestamp("event_time"))
               .repartition(partitions, "mmsi")
               .sortWithinPartitions("mmsi", "_event_ts", "logical_id", "source_record_id"))
    port_ids = None if ports is None else tuple(str(port) for port in ports)
    lines = ordered.rdd.mapPartitions(lambda rows: _reconstruct_vessels(rows, port_ids, rules))
    return spark.read.schema(VISIT_DDL).json(lines), ordered


def _reconstruct_vessels(rows, port_ids: tuple[str, ...] | None, rules: Rules):
    """Run one tracker per vessel over a partition sorted by vessel and time."""
    tracker = None
    for row in rows:
        item = Observation.from_json(row.asDict(recursive=True))
        if tracker is None or item.mmsi != tracker.mmsi:
            if tracker is not None:
                for visit in tracker.close():
                    yield json.dumps(visit.as_json())
            tracker = VesselTracker(item.mmsi, rules, port_ids)
        for visit in tracker.feed(item):
            yield json.dumps(visit.as_json())
    if tracker is not None:
        for visit in tracker.close():
            yield json.dumps(visit.as_json())


def _visit_line(row) -> bytes:
    return (json.dumps(row.asDict(recursive=True), sort_keys=True, separators=(",", ":")) + "\n").encode()


def run_batch(spark, inputs: list[str], output: Path | str, *, run_id: str, ports: list[str] | None,
              synthetic: bool, rules: Rules = Rules(), partitions: int | None = None,
              created_at: str | None = None) -> dict:
    """Reconstruct visits and publish a completed-batch artifact at ``output``.

    The directory appears under its final name only after ``manifest.json`` exists, so a
    crashed or rejected run never leaves something that looks like a complete artifact.
    """
    from pyspark.sql import functions as F
    output = Path(output)
    if not run_id:
        raise EngineError("run_id is required")
    if ports is not None and not ports:
        raise EngineError("Give at least one port, or None for every assigned port")
    if output.exists():
        raise FileExistsError(f"Artifact already exists and is immutable: {output}")
    partitions = partitions or int(spark.conf.get("spark.sql.shuffle.partitions"))
    started = time.perf_counter()
    observations = read_observations(spark, inputs)
    unusable = F.lit(False)
    for name in REQUIRED_KEYS:
        unusable = unusable | F.col(name).isNull()
    unusable = unusable | F.to_timestamp("event_time").isNull()
    counted = observations.agg(F.count(F.lit(1)).alias("rows"),
                               F.sum(unusable.cast("long")).alias("unusable")).first()
    if not counted["rows"]:
        raise EngineError("Input holds no observations")
    if counted["unusable"]:
        raise EngineError(f"{counted['unusable']} observations lack a vessel, time or identity; "
                          "the input is not canonical")
    visits, ordered = reconstruct_visits(spark, observations, ports, rules, partitions)
    visits = visits.persist()
    partial = output.with_name(f".{output.name}.partial")
    try:
        status_counts = {row["status"]: row["count"] for row in visits.groupBy("status").count().collect()}
        visit_count = sum(status_counts.values())
        if not visit_count:
            raise EngineError("No visits reconstructed; check the port selection and input before publishing")
        if None in status_counts:
            raise EngineError("Reconstruction produced an unreadable visit record")
        repeated = visits.groupBy("visit_id").count().where("count > 1").limit(3).collect()
        if repeated:
            raise EngineError(f"Repeated visit_id values: {[row['visit_id'] for row in repeated]}")
        if partial.exists():
            shutil.rmtree(partial)  # leftover of an earlier crashed run with the same name
        partial.mkdir(parents=True)
        digest, written = hashlib.sha256(), 0
        with open(partial / "visits.jsonl", "wb") as handle:
            for row in visits.orderBy(*VISIT_ORDER).toLocalIterator():
                line = _visit_line(row)
                handle.write(line)
                digest.update(line)
                written += 1
        if written != visit_count:
            raise EngineError(f"Wrote {written} visits but Spark counted {visit_count}")
        plan = ordered._jdf.queryExecution().executedPlan().toString()
        (partial / "spark-plan.txt").write_text(plan + "\n", encoding="utf-8")
        manifest = {
            "artifact_type": ARTIFACT_TYPE, "contract_version": CONTRACT_VERSION, "complete": True,
            "run_id": run_id, "rule_version": rules.version, "synthetic": bool(synthetic),
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
            "visits_file": "visits.jsonl", "visits_sha256": digest.hexdigest(),
            "visit_count": visit_count, "status_counts": dict(sorted(status_counts.items())),
            "source": {
                "inputs": [str(path) for path in inputs], "input_rows": counted["rows"],
                "ports": "all" if ports is None else [str(port) for port in ports],
                "rules": {"confirm_minutes": rules.confirm_minutes, "exit_minutes": rules.exit_minutes,
                          "max_gap_minutes": rules.max_gap_minutes},
                "engine": {"spark_version": spark.version, "master": spark.sparkContext.master,
                           "partitions": partitions,
                           "elapsed_seconds": round(time.perf_counter() - started, 3)},
            },
        }
        # The manifest is written last and the directory is renamed after it.
        (partial / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                                               encoding="utf-8")
        os.rename(partial, output)
        return manifest
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise
    finally:
        visits.unpersist()
