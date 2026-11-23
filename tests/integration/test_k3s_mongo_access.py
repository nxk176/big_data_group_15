"""Run after publishing a generation on k3s, with the deployment credential file sourced."""
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.request import urlopen

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from harbortrace import serving


pytestmark = pytest.mark.skipif(os.getenv("HARBORTRACE_TEST_K3S_ACCESS") != "1",
                                reason="set HARBORTRACE_TEST_K3S_ACCESS=1 for a deployed HarborTrace database")
ROOT = Path(__file__).resolve().parents[2]


def test_deployed_mongodb_roles():
    name = os.environ["HARBORTRACE_DATABASE"]
    with (MongoClient(os.environ["HARBORTRACE_MONGO_READER_URI"]) as reader,
          MongoClient(os.environ["HARBORTRACE_MONGO_URI"]) as writer):
        for client, expected_role in [(reader, "read"), (writer, "readWrite")]:
            info = client[name].command("connectionStatus")["authInfo"]
            assert info["authenticatedUserRoles"] == [{"role": expected_role, "db": name}]
        visits = reader[name][serving.VISITS]
        assert visits.find_one({}, {"_id": 1}) is not None
        existing_index = next(index for index in visits.list_indexes() if index["name"] != "_id_")
        # Each operation is harmless even if someone grants the reader an unexpected privilege.
        with pytest.raises(OperationFailure) as denied_update:
            visits.update_one({"$expr": {"$eq": [0, 1]}}, {"$set": {"smoke_test": True}}, upsert=False)
        with pytest.raises(OperationFailure) as denied_index:
            visits.create_index(list(existing_index["key"].items()), name=existing_index["name"])
        with pytest.raises(OperationFailure) as denied_collection:
            reader[name].command("create", serving.VISITS)
        with pytest.raises(OperationFailure) as denied_admin:
            writer.admin["system.users"].find_one({}, {"_id": 1})
        with MongoClient(*reader.address, serverSelectionTimeoutMS=5000) as anonymous:
            with pytest.raises(OperationFailure) as denied_read:
                anonymous[name][serving.VISITS].find_one({}, {"_id": 1})
        assert [error.value.code for error in (denied_update, denied_index, denied_collection,
                                                denied_admin, denied_read)] == [13] * 5


def test_deployed_dashboard_and_reader_status():
    # An unusable writer URI makes the read-only CLI's account selection observable.
    env = {**os.environ, "HARBORTRACE_MONGO_URI": "mongodb://127.0.0.1:1", "PYTHONPATH": str(ROOT / "src")}
    status = subprocess.check_output([sys.executable, str(ROOT / "scripts/serving_import.py"), "status"], env=env)
    active = json.loads(status)["active"]
    url = os.environ["HARBORTRACE_DASHBOARD_URL"].rstrip("/") + "/api/visits?limit=1"
    with urlopen(url, timeout=30) as response:
        page = json.load(response)
    assert page["generation"]["generation_id"] == active["_id"]
    assert page["total"] == active["visit_count"] > 0
