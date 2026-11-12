"""Serve the read-only HarborTrace dashboard.

    # Against MongoDB (use a read-only database user in shared environments):
    PYTHONPATH=src:. python scripts/serve_dashboard.py --mongo-uri mongodb://127.0.0.1:27017 --database harbortrace_dev

    # Without a server: in-memory database preloaded with the SYNTHETIC serving fixtures:
    PYTHONPATH=src:. python scripts/serve_dashboard.py --memory --load-fixtures

Then open http://127.0.0.1:8050/. Set HARBORTRACE_DASHBOARD_TOKEN to require a bearer token.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT)]

from dashboard.app import make_app  # noqa: E402
from harbortrace import serving  # noqa: E402


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):  # the app already logs one JSON line per request
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mongo-uri")
    parser.add_argument("--database", default="harbortrace_dev")
    parser.add_argument("--memory", action="store_true")
    parser.add_argument("--load-fixtures", action="store_true", help="With --memory: publish the synthetic fixtures")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8050)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.memory:
        import mongomock
        db = mongomock.MongoClient()[args.database]
        if args.load_fixtures:
            fixtures = ROOT / "tests/fixtures/serving"
            serving.publish(db, serving.load_artifact(fixtures / "generation_a"), owner="dashboard-demo")
            updates = json.loads((fixtures / "provisional_updates.json").read_text(encoding="utf-8"))
            for step in updates["steps"]:
                try:
                    serving.apply_update(db, step["update"])
                except (serving.StaleUpdateError, serving.ConflictError):
                    pass
    else:
        uri = args.mongo_uri or os.environ.get("HARBORTRACE_MONGO_URI")
        if not uri:
            parser.error("Select --mongo-uri, HARBORTRACE_MONGO_URI or --memory")
        db = serving.connect(uri, args.database, read_only=True)
    with make_server(args.host, args.port, make_app(db), handler_class=QuietHandler) as server:
        print(f"HarborTrace dashboard on http://{args.host}:{args.port}/ (Ctrl+C to stop)")
        server.serve_forever()


if __name__ == "__main__":
    main()
