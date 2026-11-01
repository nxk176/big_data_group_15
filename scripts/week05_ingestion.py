"""Prepare unassigned position records, audit quality and reconstruct visits with Spark."""
from __future__ import annotations

import argparse
import json
import struct
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from harbortrace.config import Config
from harbortrace.ingest import atomic_json, download_ais, download_boundaries, prepare, sha256_file
from harbortrace.spark_core import reconstruct_frame, spark_session
from week04_checkpoint import print_table, visit_cells

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/week05_ingestion.json"
PILOT_SOURCE_SHA = "3124557f70bb216e3a4d5817cf019368a6ca47d86aec8d3d2aa8c8e6d1e92560"
PILOT_BOUNDARY_SHA = "3419b3badc0e5d5f31ccbe784e1476be60bdff4182edb06090f5401185bbc779"


def configuration(data_dir: Path, ports: list[int]) -> Config:
    return Config(ROOT / "configs/week05.runtime", {
        "project_root": str(ROOT), "data_dir": str(data_dir), "ports": ports,
        "spark_master": "local[2]", "spark_driver_memory": "2g", "spark_shuffle_partitions": 2,
        "region_margin_degrees": 0.35, "boundary_jitter_meters": 100,
        "confirm_minutes": 10, "exit_minutes": 10, "max_gap_minutes": 30,
        "rule_version": "visits-v1", "seed": 20261101,
        "minimum_free_bytes": 1_000_000_000, "project_data_cap_bytes": 60_000_000_000,
        "download_cap_bytes": 1_000_000_000,
    })


def write_fixture(run: Path, spec: dict) -> tuple[Path, Path]:
    source = run / "source/week05-fixture.parquet"
    boundaries = run / "boundaries/synthetic_ports.geojson"
    source.parent.mkdir(parents=True)
    boundaries.parent.mkdir(parents=True)
    records = []
    for row in spec["rows"]:
        records.append({
            "mmsi": row["mmsi"], "base_date_time": row["base_date_time"],
            "geometry": struct.pack("<BIdd", 1, 1, *row["point"]), "sog": row["sog"],
        })
    schema = pa.schema([
        ("mmsi", pa.string()), ("base_date_time", pa.string()),
        ("geometry", pa.binary()), ("sog", pa.float64()),
    ])
    pq.write_table(pa.Table.from_pylist(records, schema=schema), source, compression="snappy")
    boundaries.write_text(json.dumps(spec["ports"], indent=2) + "\n", encoding="utf-8")
    return source, boundaries


def check_fixture(spec: dict, manifest: dict, visits: list[dict]) -> None:
    accounting = manifest["row_accounting"]
    for key, expected in spec["expected_accounting"].items():
        if accounting.get(key, 0) != expected:
            raise AssertionError(f"{key}: expected {expected}, received {accounting.get(key, 0)}")
    rejects = Counter(json.loads(line)["reason"]
                      for line in Path(manifest["quarantine"]).read_text().splitlines())
    if rejects != Counter(spec["expected_quarantine"]):
        raise AssertionError(f"Unexpected quarantine accounting: {rejects}")
    by_vessel = {row["mmsi"]: row for row in visits}
    if len(visits) != len(spec["expected_visits"]) or len(by_vessel) != len(visits):
        raise AssertionError("Unexpected number of fixture visits")
    for expected in spec["expected_visits"]:
        actual = by_vessel[expected["mmsi"]]
        for key, value in expected.items():
            if key == "quality_flag":
                if value not in actual["quality_flags"]:
                    raise AssertionError(f"Missing fixture quality flag: {value}")
            elif actual[key] != value:
                raise AssertionError(f"{expected['mmsi']} {key}: {actual[key]!r} != {value!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--real-pilot", action="store_true", help="Read existing pinned NOAA/USACE pilot files")
    mode.add_argument("--download-pilot", action="store_true", help="Explicitly acquire missing pilot snapshots, then process them")
    parser.add_argument("--output-dir", type=Path, help="A new directory under the project's data directory")
    args = parser.parse_args()
    real = args.real_pilot or args.download_pilot
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run = (args.output_dir or ROOT / "data/week05" / f"{'pilot' if real else 'fixture'}-{stamp}").resolve()
    if not run.is_relative_to((ROOT / "data").resolve()):
        parser.error("--output-dir must be a new directory under the project's data directory")
    run.mkdir(parents=True, exist_ok=False)
    config = configuration(run, [4120, 4110] if real else [9001])
    spec = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if args.download_pilot:
        acquisition = configuration(ROOT / "data", [4120, 4110])
        download_ais(acquisition, "2024-01-01")
        download_boundaries(acquisition)
    if real:
        source = ROOT / "data/source/ais-2024-01-01.parquet"
        boundaries = ROOT / "data/boundaries/usace_ports.geojson"
        if not source.is_file() or not boundaries.is_file():
            parser.error("Pilot snapshots are absent; provide them or explicitly use --download-pilot")
        if sha256_file(source) != PILOT_SOURCE_SHA or sha256_file(boundaries) != PILOT_BOUNDARY_SHA:
            raise ValueError("Pilot snapshot differs from the reviewed hashes; inspect it before using the pinned expectations")
    else:
        source, boundaries = write_fixture(run, spec)
    manifest = prepare(config, source=source, boundaries=boundaries)
    if prepare(config, source=source, boundaries=boundaries) != manifest:
        raise AssertionError("Repeated preparation did not reuse the same immutable manifest")
    accounting = manifest["row_accounting"]
    if accounting["accounted_rows"] != accounting["source_rows"]:
        raise AssertionError("Input row accounting does not balance")
    spark = spark_session(config, "harbortrace-week05-ingestion")
    try:
        frame = spark.read.parquet(manifest["canonical"])
        if frame.count() != accounting["canonical_rows"]:
            raise AssertionError("Spark input count differs from the preparation manifest")
        assignments = {row["assignment"] or "outside/ambiguous": row["count"]
                       for row in frame.groupBy("assignment").count().collect()}
        visits_frame, ordered = reconstruct_frame(spark, frame, config)
        if visits_frame is None:
            raise AssertionError("Prepared input unexpectedly produced no visits")
        visits = sorted((row.asDict(recursive=True) for row in visits_frame.collect()),
                        key=lambda row: (row["mmsi"], row["port_id"], row["first_inside"]))
        plan = ordered._jdf.queryExecution().executedPlan().toString()
        if "Sort" not in plan:
            raise AssertionError("Spark's preparation plan has no Sort")
        if real:
            expected = {"source_rows": 7293408, "canonical_rows": 235713,
                        "duplicate_rows": 7, "outside_region": 7057688}
            for key, count in expected.items():
                if accounting.get(key, 0) != count:
                    raise AssertionError(f"Pinned pilot {key} does not match: {accounting}")
            if len(visits) != 370 or Counter(row["status"] for row in visits) != {"reconciled": 271, "candidate": 99}:
                raise AssertionError("Pinned pilot visit counts differ from the recorded reference")
        else:
            check_fixture(spec, manifest, visits)
    finally:
        spark.stop()
    visit_path = run / "visits.jsonl"
    visit_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in visits), encoding="utf-8")
    (run / "spark-plan.txt").write_text(plan + "\n", encoding="utf-8")
    summary = {"result": "PASS", "mode": "real-pilot" if real else "synthetic-fixture",
               "row_accounting": accounting, "assignments": assignments, "visits": len(visits),
               "status_counts": dict(Counter(row["status"] for row in visits)),
               "canonical_manifest": str(Path(manifest["canonical"]).with_suffix(".manifest.json")),
               "visit_output": str(visit_path), "output_dir": str(run), "spark_version": spark.version}
    atomic_json(run / "summary.json", summary)
    print("\nHARBORTRACE - WEEK 5 INGESTION AND RECONSTRUCTION")
    print("Input: reviewed official snapshots." if real else "Input: synthetic positions and a fictional square port; no network required.")
    keys = ("source_rows", "canonical_rows", "invalid", "duplicate_rows", "conflict_rows", "outside_region", "accounted_rows")
    print_table("INPUT ROW ACCOUNTING", ["Category", "Rows"], [[key, accounting.get(key, 0)] for key in keys])
    print_table("SPATIAL ASSIGNMENT", ["Port / outside", "Observations"],
                [[key, value] for key, value in sorted(assignments.items())])
    print_table("RECONSTRUCTED VISITS" + (" (FIRST 10)" if len(visits) > 10 else ""),
                ["MMSI", "Port", "First inside", "Last inside", "Span min", "Status", "Missing bounds"],
                [[row["mmsi"], row["port_id"]] + visit_cells(row) for row in visits[:10]])
    print(f"Visits: {len(visits)}; status counts: {summary['status_counts']}")
    print(f"Artifacts: {run}")
    print("RESULT: PASS - preparation, accounting, immutable reuse and Spark output checks passed.")


if __name__ == "__main__":
    main()
