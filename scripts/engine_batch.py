"""Run the batch engine: canonical observations -> completed-batch artifact for serving.

    PYTHONPATH=src python scripts/engine_batch.py --fixture
    PYTHONPATH=src python scripts/engine_batch.py --real --run-id pilot-2024-01-01 --ports 4120 4110 \
        --input data/canonical/ais-2024-01-01-regional.parquet --output data/engine/pilot-2024-01-01

``--master submit`` leaves the Spark master to spark-submit (cluster runs).
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from harbortrace.contracts import Observation
from harbortrace.engine import engine_session, run_batch
from harbortrace.visits import Rules, reconstruct

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/canonical/week06_canonical.jsonl"


def check_fixture(output: Path) -> None:
    """The Spark artifact must equal the pure reference on the committed canonical fixture."""
    observations = [Observation.from_json(json.loads(line)) for line in FIXTURE.read_text().splitlines()]
    expected = [json.loads(json.dumps(visit.as_json())) for visit in reconstruct(observations, ["9001"], Rules())]
    actual = [json.loads(line) for line in (output / "visits.jsonl").read_text().splitlines()]
    if actual != expected:
        raise AssertionError("Spark artifact differs from the pure reference reconstruction")
    if [row["status"] for row in actual] != ["reconciled", "reconciled"]:
        raise AssertionError(f"Expected two reconciled fixture visits, received {len(actual)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fixture", action="store_true", help="Run and check the committed synthetic fixture")
    parser.add_argument("--input", nargs="+", help="Canonical Parquet or JSONL paths (local or hdfs://)")
    parser.add_argument("--output", type=Path, help="New artifact directory")
    parser.add_argument("--run-id")
    parser.add_argument("--ports", nargs="+", help="Port IDs, or 'all' for every assigned port")
    label = parser.add_mutually_exclusive_group()
    label.add_argument("--real", action="store_true", help="Input is real source data")
    label.add_argument("--synthetic", action="store_true", help="Input is synthetic")
    parser.add_argument("--master", default="local[2]", help="Spark master, or 'submit' to defer to spark-submit")
    parser.add_argument("--partitions", type=int, default=2, help="Shuffle partitions for the per-vessel sort")
    parser.add_argument("--driver-memory", default="2g")
    parser.add_argument("--spark-ui", action="store_true", help="Keep the Spark UI on (port 4040) for inspection")
    args = parser.parse_args()
    if args.fixture:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        inputs, output = [str(FIXTURE)], ROOT / "data/engine" / f"fixture-{stamp}"
        run_id, ports, synthetic = "fixture-week06", ["9001"], True
    else:
        if not (args.input and args.output and args.run_id and args.ports and (args.real or args.synthetic)):
            parser.error("--input, --output, --run-id, --ports and one of --real/--synthetic are required")
        inputs, output, run_id, synthetic = args.input, args.output, args.run_id, args.synthetic
        ports = None if args.ports == ["all"] else args.ports
    spark = engine_session("harbortrace-engine-batch", master=None if args.master == "submit" else args.master,
                           driver_memory=args.driver_memory, shuffle_partitions=args.partitions, ui=args.spark_ui)
    try:
        manifest = run_batch(spark, inputs, output, run_id=run_id, ports=ports, synthetic=synthetic,
                             partitions=args.partitions)
    finally:
        spark.stop()
    if args.fixture:
        check_fixture(output)
    source = manifest["source"]
    print("\nHARBORTRACE - BATCH ENGINE")
    print(f"Input: {'SYNTHETIC' if synthetic else 'real source data'}; {source['input_rows']} canonical observations "
          f"from {len(source['inputs'])} path(s); ports "
          f"{source['ports'] if source['ports'] == 'all' else ', '.join(source['ports'])}")
    print(f"Visits: {manifest['visit_count']}; status counts: {manifest['status_counts']}")
    print(f"Spark {source['engine']['spark_version']} on {source['engine']['master']}, "
          f"{source['engine']['partitions']} partitions, {source['engine']['elapsed_seconds']} s")
    print(f"visits.jsonl SHA-256: {manifest['visits_sha256']}")
    print(f"Artifact: {output}")
    print(f"Import:   PYTHONPATH=src python scripts/serving_import.py --memory import {output}")
    if args.fixture:
        print("RESULT: PASS - the Spark artifact equals the pure reference on the committed fixture.")


if __name__ == "__main__":
    main()
