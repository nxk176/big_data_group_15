"""Run the published AIS visit fixture through the same Spark adapter as batch jobs."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from harbortrace.config import Config
from harbortrace.contracts import Observation, digest, utc_time
from harbortrace.spark_core import reconstruct_frame, spark_session


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "reference_cases.json"
REAL_EXCERPT = ROOT / "tests" / "fixtures" / "real_ais_excerpt.jsonl"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--details", action="store_true", help="Show case results, the Spark plan, and real visit fields")
    args = parser.parse_args()
    specs = json.loads(FIXTURE.read_text(encoding="utf-8"))
    observations = []
    for case_number, (case, spec) in enumerate(specs.items(), start=1):
        mmsi = f"900000{case_number:03d}"  # Synthetic fixture identities.
        for row, (time, assignment) in enumerate(spec["events"]):
            event_time = utc_time(time)
            observations.append(Observation(
                mmsi=mmsi, event_time=event_time, longitude=-118.2, latitude=33.75,
                source_file=f"synthetic:{case}", source_row=row,
                source_record_id=digest(case, row),
                logical_id=digest(case, row, time, assignment),
                assignment=assignment,
            ))
    config = Config(ROOT / "configs" / "week04.synthetic", {
        "data_dir": str(ROOT / "data"), "spark_master": "local[2]",
        "spark_driver_memory": "2g", "spark_shuffle_partitions": 2,
        "ports": [4120], "confirm_minutes": 10, "exit_minutes": 10,
        "max_gap_minutes": 30, "rule_version": "visits-v1",
    })
    spark = spark_session(config, "harbortrace-week04-checkpoint")
    try:
        frame = spark.read.json(spark.sparkContext.parallelize(
            [json.dumps(item.as_json()) for item in observations], 2))
        visits, ordered = reconstruct_frame(spark, frame, config)
        if visits is None:
            raise AssertionError("Fixture unexpectedly produced no visits")
        plan = ordered._jdf.queryExecution().executedPlan().toString()
        if "Sort" not in plan:
            raise AssertionError("Spark did not sort observations by vessel and event time")
        actual_by_mmsi = {}
        for row in visits.collect():
            actual_by_mmsi.setdefault(row.mmsi, []).append(row.asDict(recursive=True))
        for case_number, (case, spec) in enumerate(specs.items(), start=1):
            mmsi = f"900000{case_number:03d}"
            actual = sorted(actual_by_mmsi.get(mmsi, []), key=lambda row: row["first_inside"])
            if len(actual) != len(spec["expected"]):
                raise AssertionError(f"{case}: expected {len(spec['expected'])} visits, got {len(actual)}")
            for visit, expected in zip(actual, spec["expected"]):
                for field, value in expected.items():
                    if field == "flag":
                        if value not in visit["quality_flags"]:
                            raise AssertionError(f"{case}: missing quality flag {value}")
                    elif visit[field] != value:
                        raise AssertionError(f"{case}: {field} = {visit[field]!r}, expected {value!r}")
            if args.details:
                for visit in actual:
                    print(json.dumps({"case": case, **{field: visit[field] for field in (
                        "first_inside", "last_inside", "observed_span_seconds", "status",
                        "left_censored", "right_censored", "quality_flags",
                    )}}, sort_keys=True))
        counts = Counter(row["status"] for rows in actual_by_mmsi.values() for row in rows)
        real_frame = spark.read.json(str(REAL_EXCERPT))
        real_input_count = real_frame.count()
        if real_input_count != 2:
            raise AssertionError(f"Expected two real excerpt observations, got {real_input_count}")
        real_visits, _ = reconstruct_frame(spark, real_frame, config)
        if real_visits is None:
            raise AssertionError("Real AIS excerpt unexpectedly produced no visit")
        real_rows = [row.asDict(recursive=True) for row in real_visits.collect()]
        if len(real_rows) != 1:
            raise AssertionError(f"Expected one visit from the real AIS excerpt, got {len(real_rows)}")
        real_visit = real_rows[0]
        if (real_visit["status"], real_visit["port_id"], real_visit["observed_span_seconds"],
                real_visit["left_censored"], real_visit["right_censored"]) != (
                "reconciled", "4120", 1080, True, True):
            raise AssertionError(f"Unexpected real AIS excerpt result: {real_visit}")
        if args.details:
            print("Spark physical plan for the synthetic fixture:")
            print(plan)
            print(json.dumps({"real_excerpt_visit": real_visit}, indent=2, sort_keys=True))
        print(json.dumps({
            "result": "PASS", "spark_version": spark.version,
            "fixture_cases": len(specs), "input_observations": len(observations),
            "visits": sum(counts.values()), "status_counts": dict(counts),
            "sort_in_physical_plan": True, "real_excerpt_observations": real_input_count,
            "real_excerpt_visits": len(real_rows),
        }, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
