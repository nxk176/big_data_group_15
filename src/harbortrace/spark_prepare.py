"""Spark source preparation: validation, port assignment and duplicate handling, partitioned by day.

Applies the rules of `ingest.prepare`, the single-process reference, with the same
identities: every row passes through `quality.canonicalize` inside Arrow batches,
ports come from `spatial.PortIndex`, and same-vessel/same-time groups are resolved
with window functions. Output (local or ``hdfs://``):

    <output>/day=YYYY-MM-DD/part-*.parquet   canonical observations, sorted by mmsi, event_time
    <output>/_quarantine/part-*.json         rejected rows with their reason
    <output>/_manifest.json                  inputs, ports, row accounting, per-day counts

Spark readers skip the ``_`` paths, so the directory reads as canonical Parquet. The
directory appears under its final name only after the manifest is written.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .contracts import CONTRACT_VERSION
from .ingest import _code_revision, sha256_file
from .spatial import Port, load_ports, load_usable_ports, region_bounds

CANONICAL_COLUMNS = ("mmsi", "event_time", "longitude", "latitude", "source_file", "source_row",
                     "source_record_id", "logical_id", "replay_id", "sog", "quality_flags", "assignment",
                     "assignment_flags")
PREPARED_DDL = ("mmsi string, event_time string, longitude double, latitude double, source_file string, "
                "source_row long, source_record_id string, logical_id string, replay_id string, sog double, "
                "quality_flags array<string>, assignment string, assignment_flags array<string>, "
                "status string, reason string")
SOURCE_COLUMNS = ("mmsi", "base_date_time", "geometry", "sog")


class PrepareError(RuntimeError):
    pass


def _prepare_function(ports, bounds: tuple[float, float, float, float], jitter_meters: float):
    """Arrow batches of source rows -> prepared rows with status region / invalid / outside_region.

    ``ports`` is a broadcast of the selected Port list, sent to each executor once.
    """
    def run(batches):
        import pyarrow as pa
        from harbortrace.quality import canonicalize
        from harbortrace.spatial import PortIndex
        index = PortIndex(ports.value, jitter_meters)
        schema = None
        for batch in batches:
            out, region = [], []
            for row in batch.to_pylist():
                source_file, source_row = row.pop("source_file"), row.pop("source_row")
                try:
                    item = canonicalize(row, source_file, source_row)
                except (ValueError, TypeError, OverflowError) as error:
                    out.append({"source_file": source_file, "source_row": source_row,
                                "status": "invalid", "reason": str(error)})
                    continue
                if not (bounds[0] <= item.longitude <= bounds[2] and bounds[1] <= item.latitude <= bounds[3]):
                    out.append({"source_file": source_file, "source_row": source_row, "status": "outside_region"})
                    continue
                payload = item.as_json()
                payload["quality_flags"] = list(payload["quality_flags"])
                payload["status"] = "region"
                region.append(payload)
            ids, flags = index.assign([row["longitude"] for row in region], [row["latitude"] for row in region])
            for row, port_id, port_flags in zip(region, ids, flags):
                row["assignment"], row["assignment_flags"] = port_id, list(port_flags)
            if schema is None:
                schema = pa.schema([
                    ("mmsi", pa.string()), ("event_time", pa.string()), ("longitude", pa.float64()),
                    ("latitude", pa.float64()), ("source_file", pa.string()), ("source_row", pa.int64()),
                    ("source_record_id", pa.string()), ("logical_id", pa.string()), ("replay_id", pa.string()),
                    ("sog", pa.float64()), ("quality_flags", pa.list_(pa.string())), ("assignment", pa.string()),
                    ("assignment_flags", pa.list_(pa.string())), ("status", pa.string()), ("reason", pa.string())])
            yield pa.RecordBatch.from_pylist(out + region, schema=schema)
    return run


def _filesystem(spark, path: str):
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path


def _write_text(spark, path: str, text: str) -> None:
    filesystem, hadoop_path = _filesystem(spark, path)
    stream = filesystem.create(hadoop_path, False)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def read_text(spark, path: str) -> str:
    filesystem, hadoop_path = _filesystem(spark, path)
    jvm = spark.sparkContext._jvm
    stream = filesystem.open(hadoop_path)
    try:
        return jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def select_ports(boundaries: Path, ports: list[str] | None) -> tuple[list[Port], dict[str, str]]:
    """The requested ports, or every usable polygon of the layer when ``ports`` is None."""
    if ports is None:
        return load_usable_ports(boundaries)
    return load_ports(boundaries, ports), {}


def prepare_spark(spark, sources: list[Path], boundaries: Path, output: str, *, ports: list[str] | None,
                  region_margin_degrees: float = 0.35, boundary_jitter_meters: float = 100,
                  replication: int | None = None, kind: str = "real") -> dict:
    """Prepare source Parquet files into a day-partitioned canonical dataset at ``output``."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    started = time.perf_counter()
    sources = [Path(source).resolve() for source in sources]
    if not sources or len({source.name for source in sources}) != len(sources):
        raise PrepareError("Give at least one source file, without repeated file names")
    selected, rejected = select_ports(boundaries, ports)
    bounds = region_bounds(selected, region_margin_degrees)
    if replication:
        # Spark's Hadoop client defaults to 3 replicas whatever the cluster's own setting is.
        spark.sparkContext._jsc.hadoopConfiguration().set("dfs.replication", str(replication))
    filesystem, final = _filesystem(spark, output)
    if filesystem.exists(final):
        raise PrepareError(f"Output already exists and is immutable: {output}")
    parent, name = output.rstrip("/").rsplit("/", 1)
    partial, staging = f"{parent}/.{name}.partial", f"{parent}/.{name}.staging"
    _, partial_path = _filesystem(spark, partial)
    _, staging_path = _filesystem(spark, staging)
    for leftover in (partial_path, staging_path):
        if filesystem.exists(leftover):
            filesystem.delete(leftover, True)  # a previous attempt crashed before publishing

    raw = spark.read.parquet(*[str(source) for source in sources])
    absent = [name for name in ("mmsi", "base_date_time", "geometry") if name not in raw.columns]
    if absent:
        raise PrepareError(f"Missing AIS columns: {absent}")
    columns = [F.col(name) for name in SOURCE_COLUMNS if name in raw.columns]
    if "sog" not in raw.columns:
        columns.append(F.lit(None).cast("double").alias("sog"))
    source = raw.select(*columns, F.col("_metadata.file_name").alias("source_file"),
                        F.col("_metadata.row_index").alias("source_row"))
    broadcast = spark.sparkContext.broadcast(selected)
    # Intermediate results are staged as Parquet, not cached: persist() of a fifteen-day all-port
    # input (106 million rows) exhausted an 8 GB heap, while staged files are read back in batches.
    try:
        (source.mapInArrow(_prepare_function(broadcast, bounds, boundary_jitter_meters), PREPARED_DDL)
         .write.parquet(f"{staging}/prepared"))
    finally:
        broadcast.unpersist()
    prepared = spark.read.parquet(f"{staging}/prepared")
    group = Window.partitionBy("mmsi", "event_time")
    # One logical observation keeps its first source row; differing positions reject the whole group.
    (prepared.where("status = 'region'")
     .withColumn("_low", F.min("logical_id").over(group))
     .withColumn("_high", F.max("logical_id").over(group))
     .withColumn("_kept", F.min("source_row").over(group))
     .withColumn("_group", F.count(F.lit(1)).over(group))
     .withColumn("status", F.when(F.col("_low") != F.col("_high"), "conflict")
                 .when(F.col("source_row") != F.col("_kept"), "duplicate").otherwise("canonical"))
     .drop("_low", "_high").write.parquet(f"{staging}/resolved"))
    region = spark.read.parquet(f"{staging}/resolved")
    oversized = region.where("_group > 10000").limit(1).collect()
    if oversized:
        raise PrepareError(f"More than 10,000 records for one MMSI/time group: {oversized[0]['mmsi']}; inspect source")
    canonical = (region.where("status = 'canonical'").select(*CANONICAL_COLUMNS)
                 .withColumn("day", F.substring("event_time", 1, 10)))
    (canonical.repartition("day").sortWithinPartitions("mmsi", "event_time", "logical_id", "source_row")
     .write.partitionBy("day").parquet(partial))
    rejected_rows = (prepared.where("status = 'invalid'").select("source_file", "source_row", "reason")
                     .withColumn("kept_source_row", F.lit(None).cast("long"))
                     .unionByName(region.where("status = 'conflict'").select(
                         "source_file", "source_row", F.lit("same_time_position_conflict").alias("reason"),
                         F.lit(None).cast("long").alias("kept_source_row")))
                     .unionByName(region.where("status = 'duplicate'").select(
                         "source_file", "source_row", F.lit("duplicate").alias("reason"),
                         F.col("_kept").alias("kept_source_row"))))
    rejected_rows.orderBy("source_file", "source_row").coalesce(1).write.json(f"{partial}/_quarantine")
    status = {row["status"]: row["count"] for row in prepared.groupBy("status").count().collect()}
    resolved = {row["status"]: row["count"] for row in region.groupBy("status").count().collect()}
    per_source = {row["source_file"]: row["count"] for row in prepared.groupBy("source_file").count().collect()}
    per_day = {str(row["day"]): row["count"]
               for row in spark.read.parquet(partial).groupBy("day").count().collect()}
    filesystem.delete(staging_path, True)
    accounting = {"source_rows": sum(status.values()), "invalid": status.get("invalid", 0),
                  "outside_region": status.get("outside_region", 0), "region_rows": status.get("region", 0),
                  "canonical_rows": resolved.get("canonical", 0), "duplicate_rows": resolved.get("duplicate", 0),
                  "conflict_rows": resolved.get("conflict", 0)}
    accounting["accounted_rows"] = sum(accounting[key] for key in
                                       ("invalid", "outside_region", "canonical_rows", "duplicate_rows", "conflict_rows"))
    if accounting["accounted_rows"] != accounting["source_rows"] or sum(per_day.values()) != accounting["canonical_rows"]:
        raise PrepareError(f"Row accounting failure: {accounting}, days {per_day}")
    manifest = {"contract_version": CONTRACT_VERSION, "kind": kind, "engine": "spark",
                "sources": [{"path": str(source), "sha256": sha256_file(source), "rows": per_source.get(source.name, 0)}
                            for source in sources],
                "boundary": str(boundaries), "boundary_sha256": sha256_file(boundaries),
                "ports": "all usable" if ports is None else [port.port_id for port in selected],
                "ports_used": len(selected), "ports_rejected": rejected,
                "region_bounds": list(bounds), "region_margin_degrees": region_margin_degrees,
                "boundary_jitter_meters": boundary_jitter_meters, "replication": replication,
                "row_accounting": accounting, "days": dict(sorted(per_day.items())),
                "code_revision": _code_revision(), "harbortrace_version": __version__,
                "spark_version": spark.version, "spark_master": spark.sparkContext.master,
                "seconds": round(time.perf_counter() - started, 3),
                "prepared_at_utc": datetime.now(timezone.utc).isoformat(), "output": output}
    _write_text(spark, f"{partial}/_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if replication:
        # A FileSystem cached before the setting above keeps creating files with its old factor.
        files = filesystem.listFiles(partial_path, True)
        while files.hasNext():
            filesystem.setReplication(files.next().getPath(), replication)
    if not filesystem.rename(partial_path, final):
        raise PrepareError(f"Could not publish {partial} as {output}")
    return manifest


def compare_with_reference(spark, output: str, reference_manifest: dict) -> dict:
    """Row-for-row comparison with a canonical file and quarantine from `ingest.prepare`."""
    from collections import Counter
    spark_rows = spark.read.parquet(output).select(*CANONICAL_COLUMNS)
    reference = spark.read.parquet(reference_manifest["canonical"]).select(*CANONICAL_COLUMNS)
    only_spark, only_reference = spark_rows.exceptAll(reference).count(), reference.exceptAll(spark_rows).count()
    manifest = json.loads(read_text(spark, f"{output}/_manifest.json"))
    quarantine = Counter((row["source_row"], row["reason"], row["kept_source_row"])
                         for row in spark.read.json(f"{output}/_quarantine").collect())
    expected = Counter()
    for line in Path(reference_manifest["quarantine"]).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        expected[(record["source_row"], record["reason"], record.get("kept_source_row"))] += 1
    keys = ("source_rows", "invalid", "outside_region", "region_rows", "canonical_rows",
            "duplicate_rows", "conflict_rows", "accounted_rows")
    differences = {key: (manifest["row_accounting"].get(key, 0), reference_manifest["row_accounting"].get(key, 0))
                   for key in keys
                   if manifest["row_accounting"].get(key, 0) != reference_manifest["row_accounting"].get(key, 0)}
    return {"result": "EQUAL" if not (only_spark or only_reference or differences or quarantine != expected) else "DIFFERENT",
            "canonical_rows": manifest["row_accounting"]["canonical_rows"],
            "rows_only_in_spark": only_spark, "rows_only_in_reference": only_reference,
            "quarantine_rows": sum(quarantine.values()), "quarantine_equal": quarantine == expected,
            "accounting_differences": differences}
