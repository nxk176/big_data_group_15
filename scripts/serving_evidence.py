"""Measure artifact-to-MongoDB-to-HTTP behavior in a disposable database.

Run from the repository root with Python dependencies installed and a MongoDB server:
    PYTHONPATH=src:. HARBORTRACE_MONGO_URI=mongodb://127.0.0.1:27027 \
      python scripts/serving_evidence.py --artifact tests/fixtures/serving/generation_a \
      --output data/serving/evidence.json

The command deletes only its randomly named test database. It never records the URI.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from wsgiref.simple_server import WSGIRequestHandler, make_server

from dashboard.app import make_app
from harbortrace import serving
from harbortrace.update_import import apply_directory


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        pass


def request(base: str, path: str, token: str | None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        response = urlopen(Request(base + path, headers=headers), timeout=10)
    except HTTPError as error:
        response = error
    with response:
        return response.code, json.loads(response.read())


def measure(operation, samples: int) -> dict:
    elapsed = []
    for _ in range(samples):
        started = time.perf_counter()
        operation()
        elapsed.append((time.perf_counter() - started) * 1000)
    return {"samples": samples, "min": min(elapsed), "median": statistics.median(elapsed), "max": max(elapsed)}


def stages(plan) -> list[str]:
    result = []
    if isinstance(plan, dict):
        if "stage" in plan:
            result.append(plan["stage"])
        for value in plan.values():
            result.extend(stages(value))
    elif isinstance(plan, list):
        for value in plan:
            result.extend(stages(value))
    return sorted(set(result))


def collect(db, artifact, *, samples: int, updates: Path | None) -> dict:
    serving.ensure_indexes(db)
    started = time.perf_counter()
    serving.publish(db, artifact, owner="serving-evidence")
    import_seconds = time.perf_counter() - started
    retry = serving.publish(db, artifact, owner="serving-evidence")
    reader, query = serving.Reader(db), serving.VisitQuery(limit=1000)
    totals = {"manifest": artifact.manifest["visit_count"],
              "stored": reader.visits(query)["total"],
              "summary": sum(row["visits"] for row in reader.summary(query)["by_port_status"])}
    report = {"recorded_at": datetime.now(timezone.utc).isoformat(),
              "input": artifact.manifest, "generation_id": artifact.generation_id,
              "mongo_version": db.client.server_info()["version"], "database": db.name,
              "counts": totals, "retry_reused": retry["reused"], "import_seconds": import_seconds,
              "query_timings_ms": {"visits": measure(lambda: reader.visits(query), samples),
                                   "summary": measure(lambda: reader.summary(query), samples),
                                   "hourly": measure(lambda: reader.hourly(), samples)}}
    if updates:
        report["updates"] = apply_directory(db, updates)
        report["updates_retry"] = apply_directory(db, updates)
        report["provisional_visits"] = reader.provisional(query)["total"]
    plan = db[serving.VISITS].find({"generation_id": artifact.generation_id,
                                    "port_id": artifact.visits[0]["port_id"] if artifact.visits else ""}).explain()
    report["winning_plan_stages"] = stages(plan["queryPlanner"]["winningPlan"])
    report["alerts"] = {"stale": [row["code"] for row in serving.health(
        db, now=datetime.now(timezone.utc) + timedelta(days=3))["alerts"]]}
    token = uuid.uuid4().hex
    with make_server("127.0.0.1", 0, make_app(db, token=token), handler_class=QuietHandler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            code, payload = request(base, "/api/visits", token)
            totals["http"] = payload["total"]
            report["http_statuses"] = {"visits": code,
                                       "unauthorized": request(base, "/api/visits", None)[0],
                                       "health": request(base, "/healthz", None)[0]}
            report["query_timings_ms"]["http_visits"] = measure(lambda: request(base, "/api/visits", token), samples)
            if artifact.visits:
                document = db[serving.VISITS].find_one({"generation_id": artifact.generation_id})
                db[serving.VISITS].delete_one({"_id": document["_id"]})
                try:
                    code, payload = request(base, "/healthz", None)
                    report["http_statuses"]["health_count_mismatch"] = code
                    report["alerts"]["missing_record"] = [row["code"] for row in payload["alerts"]]
                finally:
                    db[serving.VISITS].insert_one(document)
        finally:
            server.shutdown()
            thread.join(timeout=5)
    if len(set(totals.values())) != 1:
        raise serving.ValidationError(f"artifact, MongoDB, summary, and HTTP counts differ: {totals}")
    report["health_after_restore"] = serving.health(db)["status"]
    report["metrics"] = serving.METRICS.snapshot()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--updates", type=Path, help="Optional engine output or updates directory")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=5)
    args = parser.parse_args()
    uri = os.environ.get("HARBORTRACE_MONGO_URI")
    if not uri:
        parser.error("Set HARBORTRACE_MONGO_URI to the test MongoDB server")
    if args.samples < 1:
        parser.error("--samples must be positive")
    if args.output.exists():
        parser.error("--output must be a new file")
    artifact = serving.load_artifact(args.artifact)
    name = f"harbortrace_it_evidence_{uuid.uuid4().hex}"
    db = serving.connect(uri, name)
    try:
        report = collect(db, artifact, samples=args.samples, updates=args.updates)
    finally:
        try:
            db.client.drop_database(name)
        finally:
            db.client.close()
    report["database_removed"] = True
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(report, sort_keys=True, indent=2, default=str) + "\n")
    print(json.dumps({"counts": report["counts"], "synthetic": report["input"]["synthetic"],
                      "database_removed": True, "output": str(args.output)}, sort_keys=True))


if __name__ == "__main__":
    main()
