"""Serving runner: validate and publish batch artifacts, apply provisional updates, operate generations.

Database selection: ``--mongo-uri`` (or HARBORTRACE_MONGO_URI) and ``--database``; ``--memory``
uses an in-process mongomock database for demonstrations without a MongoDB server.

    PYTHONPATH=src python scripts/serving_import.py --memory demo
    PYTHONPATH=src python scripts/serving_import.py --mongo-uri mongodb://127.0.0.1:27017 \
        --database harbortrace_dev import tests/fixtures/serving/generation_a
    PYTHONPATH=src python scripts/serving_import.py package data/week05/fixture-.../visits.jsonl \
        --run-id week05-fixture --out data/serving/artifacts/week05-fixture
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from harbortrace import serving

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/serving"


def database(args):
    if args.memory:
        import mongomock
        return mongomock.MongoClient()[args.database]
    uri = args.mongo_uri or os.environ.get("HARBORTRACE_MONGO_URI")
    if not uri:
        raise SystemExit("Select --mongo-uri, HARBORTRACE_MONGO_URI or --memory explicitly")
    return serving.connect(uri, args.database)


def show(value):
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def cmd_import(args, db):
    artifact = serving.load_artifact(args.artifact)
    print(f"validated {len(artifact.visits)} visits from {args.artifact} "
          f"(synthetic={artifact.manifest['synthetic']}, run={artifact.manifest['run_id']})")
    serving.ensure_indexes(db)
    show(serving.publish(db, artifact, owner=args.owner, activate=not args.no_activate))


def cmd_package(args, _db):
    rows = [json.loads(line) for line in Path(args.visits).read_text(encoding="utf-8").splitlines() if line.strip()]
    versions = {row.get("rule_version") for row in rows}
    if len(versions) != 1:
        raise SystemExit(f"expected one rule_version, found {versions}")
    created = datetime.now(timezone.utc).isoformat()
    path = serving.write_artifact(Path(args.out), rows, run_id=args.run_id, rule_version=versions.pop(),
                                  synthetic=args.synthetic, created_at=created,
                                  source={"visits_file": str(Path(args.visits).resolve()),
                                          "visits_file_sha256": serving.sha256_bytes(Path(args.visits).read_bytes())})
    print(f"wrote artifact {path} with {len(rows)} visits")


def cmd_updates(args, db):
    data = json.loads(Path(args.file).read_text(encoding="utf-8"))
    updates = [step["update"] for step in data["steps"]] if "steps" in data else data
    for update in updates:
        try:
            outcome = serving.apply_update(db, update)
        except serving.StaleUpdateError as error:
            outcome = {"cohort_id": update.get("cohort_id"), "seq": update.get("seq"), "result": "stale", "detail": str(error)}
        except serving.ConflictError as error:
            outcome = {"cohort_id": update.get("cohort_id"), "seq": update.get("seq"), "result": "conflict", "detail": str(error)}
        print(json.dumps(outcome, sort_keys=True))


def cmd_status(_args, db):
    reader = serving.Reader(db)
    generation = reader.active_generation()
    show({"active": generation and {k: generation.get(k) for k in ("_id", "run_id", "synthetic", "visit_count",
                                                                   "status_counts", "activated_at")},
          "generations": [{k: g.get(k) for k in ("_id", "status", "run_id", "visit_count")}
                          for g in db[serving.GENERATIONS].find().sort("started_at", 1)],
          "provisional": reader.provisional(serving.VisitQuery())["cohorts"],
          "health": serving.health(db)})


def cmd_activate(args, db):
    token = serving.acquire_lease(db, args.owner)
    activated = serving.activate_generation(db, args.generation, token, reason="operator")
    serving.release_lease(db, args.owner, token)
    show({"activated": activated})


def cmd_rollback(args, db):
    show({"active": serving.rollback(db, owner=args.owner)})


def cmd_recover(args, db):
    show({"failed": serving.recover_incomplete(db, older_than_seconds=args.older_than)})


def cmd_demo(args, db):
    """End-to-end fixture walkthrough with explicit checks; exits non-zero on any mismatch."""
    checks: list[tuple[str, bool]] = []
    serving.ensure_indexes(db)
    reader = serving.Reader(db)
    a = serving.load_artifact(FIXTURES / "generation_a")
    b = serving.load_artifact(FIXTURES / "generation_b")
    first = serving.publish(db, a, owner="demo-publisher")
    checks.append(("generation A published and active", first["activated"] and reader.active_generation()["_id"] == a.generation_id))
    retry = serving.publish(db, a, owner="demo-publisher")
    checks.append(("retrying A is a no-op", retry["reused"] and db[serving.VISITS].count_documents({}) == len(a.visits)))
    try:
        serving.publish(db, b, owner="demo-publisher", fail_after_visits=4)
    except RuntimeError as error:
        print(f"simulated crash: {error}")
    checks.append(("crash leaves A active", reader.active_generation()["_id"] == a.generation_id))
    checks.append(("readers never see partial B", reader.visits(serving.VisitQuery())["total"] == len(a.visits)))
    resumed = serving.publish(db, b, owner="demo-publisher")
    checks.append(("retry completes and activates B", resumed["activated"] and reader.active_generation()["_id"] == b.generation_id))
    checks.append(("B count matches manifest", reader.visits(serving.VisitQuery())["total"] == b.manifest["visit_count"]))
    rolled = serving.rollback(db, owner="operator")
    checks.append(("rollback reactivates A", rolled == a.generation_id))
    stale_token = serving.acquire_lease(db, "operator")
    newer = serving.acquire_lease(db, "operator")
    try:
        serving.activate_generation(db, b.generation_id, stale_token)
        fenced = False
    except serving.FencedError:
        fenced = True
    checks.append(("stale fencing token cannot move the pointer", fenced))
    serving.activate_generation(db, b.generation_id, newer, reason="demo")
    serving.release_lease(db, "operator", newer)
    outcomes = []
    data = json.loads((FIXTURES / "provisional_updates.json").read_text(encoding="utf-8"))
    for step in data["steps"]:
        try:
            outcome = serving.apply_update(db, step["update"])["result"]
        except serving.StaleUpdateError:
            outcome = "stale"
        except serving.ConflictError:
            outcome = "conflict"
        ids = sorted(v["visit_id"] for v in reader.provisional(serving.VisitQuery())["visits"])
        outcomes.append(outcome == step["expect"] and ids == step["visit_ids"])
    checks.append((f"{len(outcomes)} provisional update steps behave as expected", all(outcomes)))
    summary = reader.summary(serving.VisitQuery(port_id="4120"))
    checks.append(("summary for port 4120 is non-empty", bool(summary["by_port_status"])))
    report = serving.health(db)
    checks.append(("health has no critical alerts", report["status"] in ("ok", "degraded")))
    print("\nHARBORTRACE SERVING DEMO (SYNTHETIC FIXTURES)")
    for name, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
    print("\nSummary by port/status:")
    for row in summary["by_port_status"]:
        print(f"  {row}")
    print(f"\nHealth: {report['status']} alerts={[a['code'] for a in report['alerts']]}")
    print(f"Metrics: {serving.METRICS.snapshot()['counters']}")
    audit = Counter(r["action"] for r in db[serving.AUDIT].find())
    print(f"Audit log: {dict(sorted(audit.items()))}")
    if not all(passed for _, passed in checks):
        raise SystemExit("RESULT: FAIL")
    print("RESULT: PASS - fixture-only publication, retry, crash, rollback, fencing and update checks passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mongo-uri")
    parser.add_argument("--database", default="harbortrace_dev")
    parser.add_argument("--memory", action="store_true", help="Use an in-process mongomock database")
    parser.add_argument("--owner", default=f"{os.environ.get('USER', 'publisher')}@{os.uname().nodename}")
    parser.add_argument("-v", "--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("import", help="Validate and publish a completed batch artifact")
    p.add_argument("artifact", type=Path)
    p.add_argument("--no-activate", action="store_true")
    p.set_defaults(func=cmd_import)
    p = commands.add_parser("package", help="Wrap an engine visits.jsonl into a validated artifact")
    p.add_argument("visits", type=Path)
    p.add_argument("--run-id", required=True)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--synthetic", action="store_true", help="Label the artifact as synthetic input")
    p.set_defaults(func=cmd_package, no_db=True)
    p = commands.add_parser("apply-updates", help="Apply provisional updates from a JSON file")
    p.add_argument("file", type=Path)
    p.set_defaults(func=cmd_updates)
    commands.add_parser("status").set_defaults(func=cmd_status)
    p = commands.add_parser("activate")
    p.add_argument("generation")
    p.set_defaults(func=cmd_activate)
    commands.add_parser("rollback").set_defaults(func=cmd_rollback)
    p = commands.add_parser("recover", help="Mark abandoned loading generations as failed")
    p.add_argument("--older-than", type=int, default=1800)
    p.set_defaults(func=cmd_recover)
    commands.add_parser("demo", help="Run the fixture walkthrough with checks").set_defaults(func=cmd_demo)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s")
    try:
        args.func(args, None if getattr(args, "no_db", False) else database(args))
    except serving.ValidationError as error:
        print(f"REJECTED: {error}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
