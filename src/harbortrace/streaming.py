"""Structured Streaming job: replay messages in, provisional updates out.

Messages (one JSON object per record, the replay contract) come from a Kafka topic or
from a directory of JSONL files. They are grouped by vessel and handed to
``stream_state.VesselStream``, whose saved state is bounded. Each microbatch writes the
provisional updates of the serving contract under ``<output>/updates/batch=<id>/`` and
its counters under ``<output>/metrics/batch=<id>/``.

Delivery: a microbatch that is retried after a failure reads the same input offsets and
the same saved state, produces the same updates and overwrites the same batch
directory. Updates carry ``(cohort_id, seq)``, so applying one twice is a no-op in the
serving component. An observation that arrives after its vessel's state has already
moved past its event time is dropped and counted; the batch engine reads the retained
input again and corrects the provisional view.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .visits import Rules

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
STATE_DDL = "state string"
OUTPUT_DDL = ("mmsi string, cohort_id string, seq long, visits long, update string, accepted long, "
              "late long, duplicates long, forced long, invalid long, buffered long, expired long")
COUNTERS = ("accepted", "late", "duplicates", "forced")


def _vessel_function(ports: tuple[str, ...] | None, rules: Rules, stream_id: str, max_buffer: int):
    def update(key, frames, state):
        import pandas as pd
        from harbortrace.replay import validate_message
        from harbortrace.stream_state import VesselStream

        mmsi = key[0]
        watermark_ms = state.getCurrentWatermarkMs()
        watermark = datetime.fromtimestamp(watermark_ms / 1000, tz=timezone.utc) if watermark_ms > 0 else None
        saved = json.loads(state.get[0]) if state.exists else None
        vessel = VesselStream(mmsi, ports, rules, stream_id=stream_id, max_buffer=max_buffer, state=saved)
        before = dict(vessel.counters)
        messages, invalid = [], 0
        if not state.hasTimedOut:
            for frame in frames:
                for value in frame["value"]:
                    message = json.loads(value)
                    try:
                        validate_message(message)
                        messages.append(message)
                    except ValueError:
                        invalid += 1
        result = vessel.process(messages, watermark, datetime.now(timezone.utc).isoformat())
        expired = vessel.expired(watermark)
        if expired or (vessel.frontier is None and not vessel.buffer):
            if state.exists:
                state.remove()
        else:
            state.update((json.dumps(vessel.export(), separators=(",", ":")),))
            wake_ms = int(vessel.wake_at().timestamp() * 1000) + 1
            state.setTimeoutTimestamp(max(wake_ms, watermark_ms + 1))
        row = {"mmsi": mmsi, "cohort_id": vessel.cohort_id, "seq": vessel.seq if result else None,
               "visits": len(result["visits"]) if result else 0,
               "update": json.dumps(result, sort_keys=True, separators=(",", ":")) if result else None,
               "invalid": invalid, "buffered": len(vessel.buffer), "expired": int(expired)}
        row.update({name: vessel.counters[name] - before[name] for name in COUNTERS})
        yield pd.DataFrame([row])
    return update


def message_lines(spark, source: str, *, kafka_bootstrap: str | None = None, max_per_trigger: int | None = None):
    """A streaming frame with one string column ``value`` holding a replay message.

    ``source`` is ``kafka:<topic>`` or a directory of JSONL files (local or ``hdfs://``).
    """
    if source.startswith("kafka:"):
        if not kafka_bootstrap:
            raise ValueError("kafka_bootstrap is required for a Kafka source")
        reader = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap)
                  .option("subscribe", source.split(":", 1)[1]).option("startingOffsets", "earliest")
                  .option("failOnDataLoss", "true"))
        if max_per_trigger:
            reader = reader.option("maxOffsetsPerTrigger", str(max_per_trigger))
        return reader.load().selectExpr("CAST(value AS STRING) AS value")
    reader = spark.readStream.format("text")
    if max_per_trigger:
        reader = reader.option("maxFilesPerTrigger", str(max_per_trigger))
    return reader.load(source)


def provisional_updates(lines, *, ports: list[str] | None, rules: Rules, stream_id: str,
                        lateness_minutes: int, max_buffer: int = 5000):
    """Group messages by vessel under an event-time watermark and emit update rows."""
    from pyspark.sql import functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout
    parsed = (lines.select("value", F.get_json_object("value", "$.mmsi").alias("mmsi"),
                           F.to_timestamp(F.get_json_object("value", "$.event_time")).alias("event_ts"))
              .where("mmsi IS NOT NULL AND event_ts IS NOT NULL")
              .withWatermark("event_ts", f"{lateness_minutes} minutes"))
    function = _vessel_function(None if ports is None else tuple(str(port) for port in ports),
                                rules, stream_id, max_buffer)
    return parsed.groupBy("mmsi").applyInPandasWithState(
        function, OUTPUT_DDL, STATE_DDL, "append", GroupStateTimeout.EventTimeTimeout)


def _sink(output: str):
    def write(frame, batch_id: int):
        from pyspark.sql import functions as F
        frame.persist()
        try:
            # Overwrite by batch ID: a retried microbatch replaces its own output.
            (frame.where("update IS NOT NULL").select(F.col("update").alias("value")).coalesce(1)
             .write.mode("overwrite").text(f"{output}/updates/batch={batch_id:08d}"))
            sums = [F.coalesce(F.sum(name), F.lit(0)).alias(name)
                    for name in COUNTERS + ("invalid", "buffered", "expired", "visits")]
            (frame.agg(F.count(F.lit(1)).alias("vessels"), F.count("update").alias("updates"), *sums)
             .withColumn("batch_id", F.lit(batch_id)).coalesce(1)
             .write.mode("overwrite").json(f"{output}/metrics/batch={batch_id:08d}"))
        finally:
            frame.unpersist()
    return write


def start_stream(spark, source: str, output: Path | str, *, ports: list[str] | None, stream_id: str,
                 rules: Rules = Rules(), lateness_minutes: int = 10, max_buffer: int = 5000,
                 kafka_bootstrap: str | None = None, max_per_trigger: int | None = None,
                 available_now: bool = True, interval_seconds: int = 10):
    """Start the query. ``<output>/checkpoint`` holds offsets and state for restarts."""
    lines = message_lines(spark, source, kafka_bootstrap=kafka_bootstrap, max_per_trigger=max_per_trigger)
    updates = provisional_updates(lines, ports=ports, rules=rules, stream_id=stream_id,
                                  lateness_minutes=lateness_minutes, max_buffer=max_buffer)
    writer = (updates.writeStream.outputMode("append").queryName(f"harbortrace-stream-{stream_id}")
              .foreachBatch(_sink(str(output))).option("checkpointLocation", f"{output}/checkpoint"))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(
        processingTime=f"{interval_seconds} seconds")
    return writer.start()


def stream_report(query) -> dict:
    """State size, watermark and drop counts from the last progress that carried them."""
    progress = [item for item in query.recentProgress if item.get("stateOperators")]
    if not progress:
        return {"batches": len(query.recentProgress)}
    last = progress[-1]
    operator = last["stateOperators"][0]
    return {"batches": last["batchId"] + 1, "watermark": last.get("eventTime", {}).get("watermark"),
            "input_rows": sum(item["numInputRows"] for item in query.recentProgress),
            "state_rows": operator["numRowsTotal"], "state_memory_bytes": operator["memoryUsedBytes"],
            "rows_dropped_by_watermark": sum(item["stateOperators"][0]["numRowsDroppedByWatermark"]
                                             for item in progress)}


def read_updates(output: Path | str) -> list[dict]:
    """Updates from a local output directory in the order they must be applied."""
    updates = []
    for batch in sorted(Path(output, "updates").glob("batch=*")):
        rows = [json.loads(line) for part in sorted(batch.glob("part-*")) for line in part.open(encoding="utf-8")]
        updates.extend(sorted(rows, key=lambda row: (row["cohort_id"], row["seq"])))
    return updates


def read_metrics(output: Path | str) -> list[dict]:
    rows = [json.loads(line) for part in sorted(Path(output, "metrics").glob("batch=*/part-*"))
            for line in part.read_text().splitlines()]
    return sorted(rows, key=lambda row: row["batch_id"])
