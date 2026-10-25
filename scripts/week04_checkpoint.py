"""Run the published AIS visit fixture through the same Spark adapter as batch jobs."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from harbortrace.config import Config
from harbortrace.contracts import Observation, digest, utc_time
from harbortrace.spark_core import reconstruct_frame, spark_session


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "reference_cases.json"
REAL_EXCERPT = ROOT / "tests" / "fixtures" / "real_ais_excerpt.jsonl"


def print_table(title: str, headers: list[str], rows: list[list]) -> None:
    values = [[str(value) for value in row] for row in rows]
    widths = [max([len(header)] + [len(row[index]) for row in values])
              for index, header in enumerate(headers)]
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def line(row):
        return "| " + " | ".join(value.ljust(width) for value, width in zip(row, widths)) + " |"

    print(f"\n{title}")
    print(border)
    print(line(headers))
    print(border)
    for row in values:
        print(line(row))
    print(border)


def short_time(value: str) -> str:
    return datetime.fromisoformat(value).strftime("%m-%d %H:%M:%S")


def missing_bounds(visit: dict) -> str:
    labels = []
    if visit["left_censored"]:
        labels.append("entry")
    if visit["right_censored"]:
        labels.append("exit")
    return "+".join(labels) or "none"


def visit_cells(visit: dict) -> list:
    return [short_time(visit["first_inside"]), short_time(visit["last_inside"]),
            f"{visit['observed_span_seconds'] / 60:.1f}", visit["status"], missing_bounds(visit)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--details", action="store_true", help="Add the Spark plan and real-visit provenance to the tables")
    output.add_argument("--json", action="store_true", help="Print only the machine-readable JSON summary")
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
        case_visits = []
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
            case_visits.extend((case, visit) for visit in actual)
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
        summary = {
            "result": "PASS", "spark_version": spark.version,
            "fixture_cases": len(specs), "input_observations": len(observations),
            "visits": sum(counts.values()), "status_counts": dict(counts),
            "sort_in_physical_plan": True, "real_excerpt_observations": real_input_count,
            "real_excerpt_visits": len(real_rows),
        }
        if args.json:
            print(json.dumps(summary, sort_keys=True))
        else:
            print("\nHARBORTRACE - WEEK 4 CHECKPOINT")
            print("Times are UTC; dates are MM-DD. Span is measured between observed inside points.")
            headers = ["First inside", "Last inside", "Span min", "Status", "Missing bounds"]
            print_table("SYNTHETIC VISITS", ["Case"] + headers,
                        [[case] + visit_cells(visit) for case, visit in case_visits])
            print_table("REAL NOAA EXCERPT", ["MMSI", "Port"] + headers,
                        [[real_visit["mmsi"], real_visit["port_id"] + " (LA)"] + visit_cells(real_visit)])
            print_table("CHECKPOINT SUMMARY", ["Metric", "Value"], [
                ["Synthetic scenarios checked", len(specs)],
                ["Synthetic observations", len(observations)],
                ["Synthetic visits", summary["visits"]],
                ["Reconciled synthetic visits", counts["reconciled"]],
                ["Candidate synthetic visits", counts["candidate"]],
                ["Real excerpt observations", real_input_count],
                ["Real excerpt visits", len(real_rows)],
                ["Sort in physical plan", "yes"],
                ["Spark version", spark.version],
                ["Result", summary["result"]],
            ])
            print("Missing entry/exit bounds mean that supporting outside observations are absent.")
            if args.details:
                fields = ["visit_id", "possible_entry_after", "possible_entry_by", "possible_exit_after",
                          "possible_exit_by", "observation_count", "maximum_gap_seconds", "rule_version",
                          "source_digest"]
                provenance = [[field, real_visit[field] if real_visit[field] is not None else "unknown"]
                              for field in fields]
                provenance.append(["quality_flags", ", ".join(real_visit["quality_flags"])])
                provenance.extend([f"source_record_{index}", value]
                                  for index, value in enumerate(real_visit["source_record_ids"], start=1))
                print_table("REAL VISIT PROVENANCE", ["Field", "Value"], provenance)
                print("\nSPARK PHYSICAL PLAN")
                print(plan)
            print("RESULT: PASS - all fixture and real-excerpt checks passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
