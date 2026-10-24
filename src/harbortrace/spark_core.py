"""Spark adapter for deterministic visit reconstruction."""
from __future__ import annotations

import json
import os
import sys

from .config import Config
from .contracts import Observation
from .visits import Rules, VisitMachine


def spark_session(config: Config, name: str, packages: str | None = None):
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (SparkSession.builder.master(config["spark_master"]).appName(name)
             .config("spark.driver.memory", config["spark_driver_memory"])
             .config("spark.pyspark.python", sys.executable)
             .config("spark.sql.shuffle.partitions", str(config["spark_shuffle_partitions"]))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.warehouse.dir", str(config.data_dir / "spark" / "warehouse"))
             .config("spark.ui.enabled", "false"))
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _reconstruct_partition(rows, port_ids: tuple[str, ...], rules: Rules):
    current_mmsi = None
    machines = None
    for row in rows:
        payload = row.asDict(recursive=True) if hasattr(row, "asDict") else row
        item = Observation.from_json(payload)
        if item.mmsi != current_mmsi:
            if machines is not None:
                for port_id in port_ids:
                    for visit in machines[port_id].close():
                        yield json.dumps(visit.as_json())
            current_mmsi = item.mmsi
            machines = {port_id: VisitMachine(item.mmsi, port_id, rules) for port_id in port_ids}
        for port_id in port_ids:
            for visit in machines[port_id].feed(item):
                yield json.dumps(visit.as_json())
    if machines is not None:
        for port_id in port_ids:
            for visit in machines[port_id].close():
                yield json.dumps(visit.as_json())


def reconstruct_frame(spark, frame, config: Config):
    required = {"mmsi", "event_time", "logical_id", "source_record_id", "assignment"}
    if not required.issubset(frame.columns):
        raise ValueError(f"Canonical columns missing: {required - set(frame.columns)}")
    ordered = (frame.repartition(config["spark_shuffle_partitions"], "mmsi")
               .sortWithinPartitions("mmsi", "event_time", "logical_id", "source_record_id"))
    rules = Rules(config["confirm_minutes"], config["exit_minutes"],
                  config["max_gap_minutes"], config["rule_version"])
    port_ids = tuple(map(str, config["ports"]))
    visits_json = ordered.rdd.mapPartitions(lambda partition: _reconstruct_partition(partition, port_ids, rules))
    if visits_json.isEmpty():
        return None, ordered
    return spark.read.json(visits_json), ordered
