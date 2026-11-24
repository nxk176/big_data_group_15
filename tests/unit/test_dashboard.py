"""Dashboard behaviour through the WSGI interface (SYNTHETIC fixtures, mongomock)."""
import io
import json
from pathlib import Path
from unittest.mock import MagicMock

import mongomock
import pytest

from dashboard.app import make_app
from harbortrace import serving
from scripts import serve_dashboard

FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures/serving"


def test_dashboard_refuses_writer_environment_without_reader(monkeypatch, capsys):
    monkeypatch.setenv("HARBORTRACE_MONGO_URI", "mongodb://writer.invalid")
    monkeypatch.delenv("HARBORTRACE_MONGO_READER_URI", raising=False)
    monkeypatch.setattr("sys.argv", ["serve_dashboard.py"])
    connect = MagicMock(side_effect=AssertionError("dashboard attempted the writer URI"))
    monkeypatch.setattr(serving, "connect", connect)
    with pytest.raises(SystemExit) as error:
        serve_dashboard.main()
    assert error.value.code == 2 and "HARBORTRACE_MONGO_READER_URI" in capsys.readouterr().err
    connect.assert_not_called()


@pytest.mark.parametrize(("flags", "expected"), [
    ([], "mongodb://reader.invalid"),
    (["--mongo-uri", "mongodb://explicit.invalid"], "mongodb://explicit.invalid"),
])
def test_dashboard_reader_and_explicit_uri_selection(monkeypatch, flags, expected):
    monkeypatch.setenv("HARBORTRACE_MONGO_URI", "mongodb://writer.invalid")
    monkeypatch.setenv("HARBORTRACE_MONGO_READER_URI", "mongodb://reader.invalid")
    monkeypatch.setenv("HARBORTRACE_DATABASE", "selected_database")
    monkeypatch.setattr("sys.argv", ["serve_dashboard.py", *flags])
    connect = MagicMock()
    monkeypatch.setattr(serving, "connect", connect)
    monkeypatch.setattr(serve_dashboard, "make_server", MagicMock())
    serve_dashboard.main()
    connect.assert_called_once_with(expected, "selected_database", read_only=True)


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


def test_hourly_api_uses_date_status_and_vessel_filters(published):
    app = make_app(published, token="")
    assert json.loads(call(app, "/api/hourly", "start=2030-01-01")[2]) == []
    assert json.loads(call(app, "/api/hourly", "status=candidate")[2]) == []
    assert json.loads(call(app, "/api/hourly", "mmsi=000000000")[2]) == []
    visit = next(v for v in serving.load_artifact(FIXTURES / "generation_a").visits
                 if v["status"] == "reconciled")
    expected = published[serving.VISITS].count_documents({"mmsi": visit["mmsi"], "status": "reconciled"})
    rows = json.loads(call(app, "/api/hourly", f"mmsi={visit['mmsi']}")[2])
    assert sum(row["confirmed_count"] for row in rows) == expected
    assert all(row["port_id"] == visit["port_id"] for row in rows)


def test_candidate_and_unknown_vessel_pages_have_no_unfiltered_chart(published):
    app = make_app(published, token="")
    for query in ("status=candidate", "mmsi=000000000"):
        status, _, body = call(app, query=query)
        assert status == 200
        assert "No reconciled hourly aggregates" in body
        assert "<svg" not in body


def test_port_choices_are_pinned_to_page_generation(published, monkeypatch):
    app = make_app(published, token="")
    original = app.reader.active_generation
    artifact = serving.load_artifact(FIXTURES / "generation_b")
    switched = False

    def generation_then_switch():
        nonlocal switched
        result = original()
        if not switched:
            switched = True
            serving.publish(published, artifact, owner="test")
            published[serving.VISITS].update_many({"generation_id": artifact.generation_id},
                                                {"$set": {"port_id": "new-port"}})
        return result

    monkeypatch.setattr(app.reader, "active_generation", generation_then_switch)
    _, _, body = call(app)
    assert "value='4110'" in body and "value='4120'" in body
    assert "new-port" not in body


def test_many_port_summary_is_not_limited_to_displayed_visits(db, tmp_path):
    from harbortrace.contracts import digest

    base = next(v for v in serving.load_artifact(FIXTURES / "generation_a").visits
                if v["status"] == "reconciled")
    rows = []
    for index in range(205):
        row = {**base, "port_id": f"SYN{index:04d}"}
        row["visit_id"] = digest(row["mmsi"], row["port_id"], row["first_inside"], row["rule_version"])
        rows.append(row)
    path = serving.write_artifact(tmp_path / "many-ports", rows, run_id="synthetic-205-ports",
                                  rule_version=base["rule_version"], synthetic=True,
                                  created_at="2026-11-20T00:00:00+00:00")
    serving.publish(db, serving.load_artifact(path), owner="test")
    app = make_app(db, token="")
    visits = json.loads(call(app, "/api/visits")[2])
    assert visits["total"] == 205 and len(visits["visits"]) == 200 and visits["truncated"] is True
    summary = json.loads(call(app, "/api/summary")[2])
    assert sum(row["visits"] for row in summary["by_port_status"]) == 205
    assert len(summary["by_port_status"]) == 205
    assert "value='SYN0204'" in call(app)[2]
    assert json.loads(call(app, "/api/visits", "port=SYN0204")[2])["total"] == 1
