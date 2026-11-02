"""Dashboard behaviour through the WSGI interface (SYNTHETIC fixtures, mongomock)."""
import io
import json
from pathlib import Path

import mongomock
import pytest

from dashboard.app import make_app
from harbortrace import serving

FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures/serving"


def call(app, path="/", query="", method="GET", headers=None):
    environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query,
               "wsgi.input": io.BytesIO(), "SERVER_NAME": "test", "SERVER_PORT": "80"}
    environ.update(headers or {})
    captured = {}

    def start_response(status, response_headers):
        captured["status"], captured["headers"] = status, dict(response_headers)
    body = b"".join(app(environ, start_response)).decode()
    return int(captured["status"].split()[0]), captured["headers"], body


@pytest.fixture
def db():
    return mongomock.MongoClient()["dashboard_test"]


@pytest.fixture
def published(db):
    serving.publish(db, serving.load_artifact(FIXTURES / "generation_a"), owner="test")
    return db


def test_empty_state(db):
    app = make_app(db, token="")
    status, headers, body = call(app)
    assert status == 200 and "No published results yet" in body
    assert "No provisional updates received" in body
    assert headers["X-Content-Type-Options"] == "nosniff"
    status, _, body = call(app, "/api/visits")
    assert status == 200 and json.loads(body)["visits"] == []


def test_page_shows_uncertainty_provenance_and_synthetic_label(published):
    status, _, body = call(make_app(published, token=""))
    assert status == 200
    assert "SYNTHETIC fixture data" in body
    assert "Entry between (UTC)" in body and "unknown" in body  # censored bound displayed
    assert "left_censored" in body and "visits-v1" in body
    assert "Candidate visits did not meet the confirmation rule" in body
    assert "<svg" in body and "2024-01-01 23:55" in body


def test_time_buckets_fill_gaps_and_sum_ports():
    from dashboard.app import _time_buckets
    rows = [{"hour": "2024-01-01T00:00:00+00:00", "confirmed_count": 2, "port_id": "a"},
            {"hour": "2024-01-01T00:00:00+00:00", "confirmed_count": 1, "port_id": "b"},
            {"hour": "2024-01-01T03:00:00+00:00", "confirmed_count": 1, "port_id": "a"}]
    assert [count for _, count in _time_buckets(rows)] == [3, 0, 0, 1]
    rows.append({"hour": "2024-02-01T00:00:00+00:00", "confirmed_count": 5, "port_id": "a"})
    daily = _time_buckets(rows)
    assert len(daily) == 32 and daily[0][1] == 4 and daily[-1][1] == 5


def test_filters_and_empty_filter_result(published):
    app = make_app(published, token="")
    _, _, body = call(app, "/api/visits", "port=4110&start=2024-01-01&end=2024-01-01")
    data = json.loads(body)
    assert data["total"] >= 1
    assert all(v["port_id"] == "4110" and v["first_inside"].startswith("2024-01-01") for v in data["visits"])
    _, _, body = call(app, "/api/visits", "end=2023-12-31")
    assert json.loads(body)["total"] == 0  # the inclusive end date excludes later days
    _, _, body = call(app, "/", "start=2030-01-01")
    assert "No visits match these filters" in body


def test_invalid_filters_are_rejected_and_input_is_escaped(published):
    app = make_app(published, token="")
    status, _, body = call(app, "/api/visits", "start=not-a-date")
    assert status == 400 and "invalid filter" in body
    status, _, _ = call(app, "/api/visits", "status=confirmed")
    assert status == 400
    status, _, body = call(app, "/", "mmsi=%3Cscript%3Ealert(1)%3C/script%3E")
    assert status == 200 and "<script>" not in body and "&lt;script&gt;" in body


def test_summary_counts_match_reader(published):
    _, _, body = call(make_app(published, token=""), "/api/summary")
    rows = json.loads(body)["by_port_status"]
    manifest = json.loads((FIXTURES / "generation_a/manifest.json").read_text())
    assert sum(r["visits"] for r in rows) == manifest["visit_count"]


def test_provisional_section_is_separate(published):
    steps = json.loads((FIXTURES / "provisional_updates.json").read_text())["steps"]
    serving.apply_update(published, steps[0]["update"])
    _, _, body = call(make_app(published, token=""))
    assert "Provisional:" in body and "may be revised or removed" in body
    _, _, body = call(make_app(published, token=""), "/api/provisional")
    assert json.loads(body)["total"] == 2


def test_token_protects_everything_except_health(published):
    app = make_app(published, token="s3cret")
    assert call(app)[0] == 401
    assert call(app, headers={"HTTP_AUTHORIZATION": "Bearer wrong"})[0] == 401
    assert call(app, headers={"HTTP_AUTHORIZATION": "Bearer s3cret"})[0] == 200
    assert call(app, "/healthz")[0] == 200
    assert call(app, method="POST", headers={"HTTP_AUTHORIZATION": "Bearer s3cret"})[0] == 405


def test_health_and_metrics_endpoints(published):
    app = make_app(published, token="")
    status, _, body = call(app, "/healthz")
    assert status == 200 and json.loads(body)["status"] == "ok"
    published[serving.VISITS].delete_one({})
    status, _, body = call(app, "/healthz")
    assert status == 503 and json.loads(body)["status"] == "critical"
    status, headers, body = call(app, "/metrics")
    assert status == 200 and "harbortrace_http_requests_total" in body
    assert call(app, "/missing")[0] == 404


def test_one_request_uses_one_generation(published, monkeypatch):
    """Even if the pointer moves mid-request, every section reads the pinned generation."""
    app = make_app(published, token="")
    original = app.reader.summary
    gen_b = serving.load_artifact(FIXTURES / "generation_b")

    def summary_then_switch(query, generation_id=None):
        result = original(query, generation_id)
        serving.publish(published, gen_b, owner="test")  # pointer switches here
        return result
    monkeypatch.setattr(app.reader, "summary", summary_then_switch)
    _, _, body = call(app)
    gen_a = json.loads((FIXTURES / "generation_a/manifest.json").read_text())
    assert f"{gen_a['visit_count']} visits." in body
