"""Opt-in check against a real MongoDB server.

    HARBORTRACE_TEST_MONGO_URI=mongodb://127.0.0.1:27017 python -m pytest tests/integration -q

Uses a throwaway database whose name starts with ``harbortrace_it_`` and drops it afterwards.
"""
import os
import uuid
from pathlib import Path

import pytest

from harbortrace import serving

URI = os.environ.get("HARBORTRACE_TEST_MONGO_URI")
FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures/serving"
pytestmark = pytest.mark.skipif(not URI, reason="set HARBORTRACE_TEST_MONGO_URI to run against MongoDB")


@pytest.fixture
def db():
    name = f"harbortrace_it_{uuid.uuid4().hex[:8]}"
    database = serving.connect(URI, name)
    yield database
    database.client.drop_database(name)


def test_publish_switch_and_read_on_real_server(db):
    a = serving.load_artifact(FIXTURES / "generation_a")
    b = serving.load_artifact(FIXTURES / "generation_b")
    serving.ensure_indexes(db)
    serving.publish(db, a, owner="it")
    with pytest.raises(RuntimeError):
        serving.publish(db, b, owner="it", fail_after_visits=2)
    reader = serving.Reader(db)
    assert reader.visits(serving.VisitQuery(limit=1000))["total"] == len(a.visits)
    serving.publish(db, b, owner="it")
    assert reader.active_generation()["_id"] == b.generation_id
    assert sum(r["visits"] for r in reader.summary(serving.VisitQuery())["by_port_status"]) == len(b.visits)
    plan = db[serving.VISITS].find({"generation_id": b.generation_id, "port_id": "4120"}).explain()
    assert "IXSCAN" in str(plan)  # the serving index is used
    assert serving.health(db)["status"] == "ok"
