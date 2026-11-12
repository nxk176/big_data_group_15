"""Build the SYNTHETIC serving fixtures (complete visits, two generations, provisional updates).

Visits are produced by the published reference state machine from the published
reference-case event sequences, so every record carries all Visit contract fields.
The fixtures establish application/contract behaviour only, not real-world accuracy.

    PYTHONPATH=src python scripts/make_serving_fixtures.py          # rewrite fixtures
    PYTHONPATH=src python scripts/make_serving_fixtures.py --check  # verify committed copies
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

from harbortrace.contracts import CONTRACT_VERSION, Observation, digest, utc_time
from harbortrace.serving import visit_from_dataclass, write_artifact
from harbortrace.visits import Rules, reconstruct

ROOT = Path(__file__).resolve().parents[1]
CASES = ROOT / "tests/fixtures/reference_cases.json"
TARGET = ROOT / "tests/fixtures/serving"
CREATED_AT = "2026-11-02T00:00:00+00:00"
PORTS = ["4120", "4110"]


def observations(spec: dict, mmsi: str, port_override: str | None = None, offset: int = 0) -> list[Observation]:
    rows = []
    for index, (time, port) in enumerate(spec["events"]):
        port = port_override if (port and port_override) else port
        stamp = utc_time(time)
        rows.append(Observation(mmsi, stamp, -118.2, 33.75, "synthetic-serving-fixture", offset + index,
                                digest("synthetic-serving-fixture", mmsi, offset + index),
                                digest(mmsi, stamp.isoformat(), port), assignment=port))
    return rows


def build(directory: Path) -> None:
    cases = json.loads(CASES.read_text(encoding="utf-8"))
    rules = Rules()
    observed: list[Observation] = []
    for number, (name, spec) in enumerate(sorted(cases.items())):
        observed += observations(spec, f"99000000{number}", offset=number * 100)
        observed += observations(spec, f"99000001{number}", port_override="4110", offset=number * 100 + 50)
    base = [visit_from_dataclass(v) for v in reconstruct(observed, PORTS, rules)]
    write_artifact(directory / "generation_a", base, run_id="synthetic-fixture-run-a",
                   rule_version=rules.version, synthetic=True, created_at=CREATED_AT,
                   source={"description": "reference_cases.json events, two synthetic vessels per case"})
    # Generation B: a late observation bridges the long-gap split of one vessel (batch reconciliation).
    late_mmsi = "990000002"  # long_gap_splits is the third case in sorted order
    late = Observation(late_mmsi, utc_time("2024-01-01T00:35:00Z"), -118.2, 33.75, "synthetic-serving-fixture",
                       9999, digest("synthetic-serving-fixture", late_mmsi, 9999),
                       digest(late_mmsi, "late", "4120"), assignment="4120")
    revised = [visit_from_dataclass(v) for v in reconstruct(observed + [late], PORTS, rules)]
    write_artifact(directory / "generation_b", revised, run_id="synthetic-fixture-run-b",
                   rule_version=rules.version, synthetic=True, created_at=CREATED_AT,
                   source={"description": "generation_a input plus one late bridging observation"})
    cohort = "synthetic-stream:4120"
    first, second, third = base[0], base[1], base[2]
    changed = {**second, "quality_flags": sorted(set(second["quality_flags"]) | {"provisional_revised"})}

    def update(seq: int, mode: str, visits=(), deleted=(), emitted="2026-11-02T00:00:00+00:00"):
        return {"contract_version": CONTRACT_VERSION, "cohort_id": cohort, "seq": seq,
                "update_id": digest(cohort, seq), "mode": mode, "emitted_at": emitted,
                "visits": list(visits), "deleted_visit_ids": list(deleted)}

    sequence = {
        "synthetic": True,
        "description": "Provisional cohort updates in delivery order with expected outcomes and resulting cohort.",
        "steps": [
            {"update": update(1, "snapshot", [first, second]), "expect": "applied",
             "visit_ids": sorted([first["visit_id"], second["visit_id"]])},
            {"update": update(1, "snapshot", [first, second]), "expect": "duplicate_ignored",
             "visit_ids": sorted([first["visit_id"], second["visit_id"]])},
            {"update": update(2, "upsert", [changed, third], emitted="2026-11-02T00:05:00+00:00"), "expect": "applied",
             "visit_ids": sorted([first["visit_id"], second["visit_id"], third["visit_id"]])},
            {"update": update(1, "snapshot", [third]), "expect": "conflict",
             "visit_ids": sorted([first["visit_id"], second["visit_id"], third["visit_id"]])},
            {"update": update(4, "delete", deleted=[first["visit_id"]], emitted="2026-11-02T00:15:00+00:00"),
             "expect": "applied", "visit_ids": sorted([second["visit_id"], third["visit_id"]])},
            {"update": update(3, "upsert", [first], emitted="2026-11-02T00:10:00+00:00"), "expect": "stale",
             "visit_ids": sorted([second["visit_id"], third["visit_id"]])},
            {"update": update(5, "snapshot", [third], emitted="2026-11-02T00:20:00+00:00"), "expect": "applied",
             "visit_ids": [third["visit_id"]]},
        ],
    }
    (directory / "provisional_updates.json").write_text(json.dumps(sequence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (directory / "README.md").write_text(
        "# Serving fixtures (SYNTHETIC)\n\n"
        "Generated by `scripts/make_serving_fixtures.py` from `tests/fixtures/reference_cases.json`.\n"
        "Synthetic vessels (MMSI prefix `99`) on the two project port IDs. They verify import, publication,\n"
        "update and dashboard behaviour only; they are not evidence of real vessel activity.\n\n"
        "- `generation_a/` - complete batch artifact (manifest + visits.jsonl).\n"
        "- `generation_b/` - same input plus one late observation that bridges a long-gap split.\n"
        "- `provisional_updates.json` - ordered cohort updates with expected outcomes "
        "(applied, retry ignored, conflicting reuse of a sequence, delete, late stale update rejected, replacement snapshot).\n",
        encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="Compare a fresh build with the committed fixtures")
    args = parser.parse_args()
    if args.check:
        with tempfile.TemporaryDirectory() as temporary:
            fresh = Path(temporary)
            build(fresh)
            different = [str(p.relative_to(fresh)) for p in sorted(fresh.rglob("*")) if p.is_file()
                         and (not (TARGET / p.relative_to(fresh)).is_file()
                              or (TARGET / p.relative_to(fresh)).read_bytes() != p.read_bytes())]
        print("fixtures up to date" if not different else f"fixtures differ: {different}")
        return 1 if different else 0
    if TARGET.exists():
        shutil.rmtree(TARGET)
    TARGET.mkdir(parents=True)
    build(TARGET)
    print(f"wrote {TARGET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
