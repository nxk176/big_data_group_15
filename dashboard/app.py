"""Read-only HarborTrace dashboard as a dependency-free WSGI application.

Pages and endpoints (all GET):

    /                  filters (port, UTC date range, status), summary, hourly chart,
                       reconciled visits with uncertainty/provenance, provisional section
    /api/generation    active generation metadata
    /api/visits        visits for the filter (JSON)
    /api/summary       counts by port and status
    /api/hourly        hourly aggregates
    /api/provisional   provisional cohorts and visits (clearly separate)
    /healthz           health and alerts (HTTP 503 when critical)
    /metrics           application metrics in Prometheus text format

Each request resolves the active generation once and passes its ID to every query, so one
page never mixes two generations even if a publisher switches the pointer mid-request.
When ``HARBORTRACE_DASHBOARD_TOKEN`` is set, requests need ``Authorization: Bearer <token>``
(``/healthz`` stays open for probes).
"""
from __future__ import annotations

import hmac
import html
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs, urlencode

from harbortrace.serving import METRICS, Reader, ValidationError, VisitQuery, health

LOG = logging.getLogger("harbortrace.dashboard")
STYLE = """
:root{--bg:#f7f8fa;--fg:#1d2330;--muted:#5b6475;--card:#fff;--line:#d9dde5;--accent:#1f6feb;--warn:#9a6700;--bad:#b42318}
@media (prefers-color-scheme:dark){:root{--bg:#11151c;--fg:#e6e9ef;--muted:#9aa3b2;--card:#181d26;--line:#2a313d;--accent:#58a6ff;--warn:#d29922;--bad:#f85149}}
body{margin:0;font:14px/1.45 system-ui,sans-serif;background:var(--bg);color:var(--fg)}
main{max-width:1200px;margin:0 auto;padding:16px}
h1{font-size:20px;margin:0 0 4px} h2{font-size:16px;margin:24px 0 8px}
.muted{color:var(--muted)} .card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px;margin:8px 0}
.banner{border-left:4px solid var(--warn)} .bad{color:var(--bad)}
form{display:flex;flex-wrap:wrap;gap:8px;align-items:end} label{display:flex;flex-direction:column;font-size:12px;color:var(--muted)}
input,select,button{font:inherit;padding:4px 6px;border:1px solid var(--line);border-radius:4px;background:var(--card);color:var(--fg)}
button{background:var(--accent);color:#fff;border:0;padding:6px 12px}
.scroll{overflow-x:auto} table{border-collapse:collapse;width:100%} th,td{border-bottom:1px solid var(--line);padding:4px 6px;text-align:left;white-space:nowrap;font-variant-numeric:tabular-nums}
th{font-size:12px;color:var(--muted)} .tag{display:inline-block;border:1px solid var(--line);border-radius:10px;padding:0 6px;margin:1px;font-size:11px}
.tag.c{border-color:var(--warn);color:var(--warn)} .bar{fill:var(--accent)} .axis{fill:var(--muted);font-size:10px}
"""


def _e(value) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _t(value) -> str:
    """Compact UTC timestamp (all times are UTC); 'unknown' for a censored (null) bound."""
    if not value:
        return "unknown"
    try:
        return datetime.fromisoformat(str(value)).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return _e(value)


def _time_buckets(hourly: list[dict], max_bars: int = 168) -> list[tuple[datetime, int]]:
    """Sum hourly rows over ports and fill empty periods; switch to days for long ranges."""
    totals: dict[datetime, int] = {}
    for row in hourly:
        hour = datetime.fromisoformat(row["hour"]).astimezone(timezone.utc)
        totals[hour] = totals.get(hour, 0) + row["confirmed_count"]
    first, last = min(totals), max(totals)
    step = timedelta(hours=1)
    if (last - first) / step + 1 > max_bars:
        step = timedelta(days=1)
        daily: dict[datetime, int] = {}
        for hour, count in totals.items():
            day = hour.replace(hour=0)
            daily[day] = daily.get(day, 0) + count
        totals, first, last = daily, min(daily), max(daily)
    series, cursor = [], first
    while cursor <= last:
        series.append((cursor, totals.get(cursor, 0)))
        cursor += step
    return series


def _query(params: dict) -> VisitQuery:
    def one(name):
        values = params.get(name)
        return values[0].strip() if values and values[0].strip() else None
    start, end = one("start"), one("end")
    try:
        start_iso = f"{date.fromisoformat(start).isoformat()}T00:00:00+00:00" if start else None
        # The end date is inclusive in the form; the query bound is exclusive.
        end_iso = f"{(date.fromisoformat(end) + timedelta(days=1)).isoformat()}T00:00:00+00:00" if end else None
        limit = int(one("limit") or 200)
    except ValueError as error:
        raise ValidationError(f"invalid filter value: {error}") from error
    query = VisitQuery(port_id=one("port"), start=start_iso, end=end_iso, status=one("status"),
                       mmsi=one("mmsi"), limit=limit)
    query.mongo_filter()  # validate early
    return query


class Dashboard:
    def __init__(self, db, *, token: str | None = None, thresholds: dict | None = None):
        self.db = db
        self.reader = Reader(db)
        self.token = token if token is not None else os.environ.get("HARBORTRACE_DASHBOARD_TOKEN")
        self.thresholds = thresholds

    # ------------------------------------------------------------------ WSGI
    def __call__(self, environ, start_response):
        started = time.perf_counter()
        path = environ.get("PATH_INFO", "/")
        status, headers, body = "500 Internal Server Error", [], b""
        try:
            if environ.get("REQUEST_METHOD", "GET") != "GET":
                status, headers, body = self._json("405 Method Not Allowed", {"error": "read-only dashboard"})
            elif path != "/healthz" and not self._authorized(environ):
                METRICS.incr("http_unauthorized")
                status, headers, body = self._json("401 Unauthorized", {"error": "bearer token required"})
            else:
                status, headers, body = self._route(path, parse_qs(environ.get("QUERY_STRING", "")))
        except ValidationError as error:
            status, headers, body = self._json("400 Bad Request", {"error": str(error)})
        except Exception as error:  # surfaced as 500 and counted; details go to the log only
            LOG.exception("dashboard request failed")
            METRICS.incr("http_errors")
            status, headers, body = self._json("500 Internal Server Error",
                                               {"error": "internal error", "type": type(error).__name__})
        elapsed = time.perf_counter() - started
        METRICS.incr("http_requests")
        METRICS.observe("http_request", elapsed)
        LOG.info(json.dumps({"path": path, "status": int(status.split()[0]), "ms": round(elapsed * 1000, 2)}))
        headers += [("X-Content-Type-Options", "nosniff"), ("Cache-Control", "no-store"),
                    ("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'"),
                    ("Content-Length", str(len(body)))]
        start_response(status, headers)
        return [body]

    def _authorized(self, environ) -> bool:
        if not self.token:
            return True
        supplied = environ.get("HTTP_AUTHORIZATION", "")
        return supplied.startswith("Bearer ") and hmac.compare_digest(supplied[7:], self.token)

    @staticmethod
    def _json(status: str, payload) -> tuple[str, list, bytes]:
        return status, [("Content-Type", "application/json")], json.dumps(payload, default=str, indent=2).encode()

    def _route(self, path: str, params: dict):
        if path == "/healthz":
            report = health(self.db, thresholds=self.thresholds)
            code = "503 Service Unavailable" if report["status"] in ("critical", "down") else "200 OK"
            return self._json(code, report)
        if path == "/metrics":
            return "200 OK", [("Content-Type", "text/plain; version=0.0.4")], METRICS.prometheus().encode()
        generation = self.reader.active_generation()
        generation_id = generation["_id"] if generation else None
        if path == "/api/generation":
            return self._json("200 OK", {"generation": generation and _strip(generation)})
        query = _query(params)
        if path == "/api/visits":
            return self._json("200 OK", self.reader.visits(query, generation_id) if generation_id
                              else {"generation": None, "visits": [], "total": 0})
        if path == "/api/summary":
            return self._json("200 OK", self.reader.summary(query, generation_id) if generation_id
                              else {"generation": None, "by_port_status": []})
        if path == "/api/hourly":
            return self._json("200 OK", self.reader.hourly(query.port_id, generation_id) if generation_id else [])
        if path == "/api/provisional":
            return self._json("200 OK", self.reader.provisional(query))
        if path == "/":
            page = self._page(generation, query, params)
            return "200 OK", [("Content-Type", "text/html; charset=utf-8")], page.encode()
        return self._json("404 Not Found", {"error": "not found"})

    # ------------------------------------------------------------------ HTML
    def _page(self, generation, query: VisitQuery, params: dict) -> str:
        parts = [f"<!doctype html><html lang='en'><head><meta charset='utf-8'>"
                 f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
                 f"<title>HarborTrace Dashboard</title><style>{STYLE}</style></head><body><main>",
                 "<h1>HarborTrace observed port presence</h1>",
                 "<p class='muted'>Observed-presence periods reconstructed from AIS positions. "
                 "They are not verified port calls, berth occupancy or congestion measures.</p>"]
        if generation is None:
            parts.append("<div class='card banner'><strong>No published results yet.</strong> "
                         "Import a completed batch artifact with <code>scripts/serving_import.py</code>.</div>")
            parts.append(self._provisional_section(query))
            return "".join(parts) + "</main></body></html>"
        generation_id = generation["_id"]
        label = "SYNTHETIC fixture data" if generation.get("synthetic") else "Real-data run"
        parts.append(
            f"<div class='card banner'><strong>{_e(label)}</strong> · generation <code>{_e(generation_id)}</code> · "
            f"run <code>{_e(generation.get('run_id'))}</code> · rule <code>{_e(generation.get('rule_version'))}</code> · "
            f"activated {_t(generation.get('activated_at'))} UTC</div>")
        parts.append(self._form(params))
        summary = self.reader.summary(query, generation_id)
        visits = self.reader.visits(query, generation_id)
        hourly = self.reader.hourly(query.port_id, generation_id)
        parts.append(self._summary_section(summary))
        parts.append(self._hourly_section(hourly, query))
        parts.append(self._visit_section(visits))
        parts.append(self._provisional_section(query))
        links = urlencode({k: v[0] for k, v in params.items() if v and k != "token"})
        parts.append(f"<p class='muted'>JSON: <a href='/api/visits?{_e(links)}'>visits</a> · "
                     f"<a href='/api/summary?{_e(links)}'>summary</a> · <a href='/healthz'>health</a></p>")
        return "".join(parts) + "</main></body></html>"

    def _form(self, params: dict) -> str:
        def value(name):
            return _e((params.get(name) or [""])[0])
        port_value = (params.get("port") or [""])[0]
        options = "".join(f"<option value='{_e(p)}'{' selected' if p == port_value else ''}>{_e(p)}</option>"
                          for p in self.reader.ports())
        status_value = (params.get("status") or [""])[0]
        statuses = "".join(f"<option value='{s}'{' selected' if s == status_value else ''}>{s}</option>"
                           for s in ("reconciled", "candidate"))
        return ("<form method='get' class='card'>"
                f"<label>Port<select name='port'><option value=''>All</option>{options}</select></label>"
                f"<label>From (UTC date)<input type='date' name='start' value='{value('start')}'></label>"
                f"<label>To (UTC date, inclusive)<input type='date' name='end' value='{value('end')}'></label>"
                f"<label>Status<select name='status'><option value=''>All</option>{statuses}</select></label>"
                f"<label>MMSI<input name='mmsi' value='{value('mmsi')}' size='10'></label>"
                "<button type='submit'>Apply</button></form>")

    @staticmethod
    def _summary_section(summary: dict) -> str:
        rows = summary["by_port_status"]
        if not rows:
            return "<h2>Summary</h2><div class='card'>No visits match these filters.</div>"
        body = "".join(
            f"<tr><td>{_e(r['port_id'])}</td><td>{_e(r['status'])}</td><td>{r['visits']}</td>"
            f"<td>{r['observations']}</td><td>{r['left_censored']}</td><td>{r['right_censored']}</td>"
            f"<td>{r['span_seconds'] / 3600:.1f}</td></tr>" for r in rows)
        return ("<h2>Summary</h2><div class='card scroll'><table><tr><th>Port</th><th>Status</th><th>Visits</th>"
                "<th>Observations</th><th>Entry unknown (left-censored)</th><th>Exit unknown (right-censored)</th>"
                f"<th>Observed span total (h)</th></tr>{body}</table>"
                "<p class='muted'>Candidate visits did not meet the confirmation rule and are counted separately.</p></div>")

    @staticmethod
    def _hourly_section(hourly: list[dict], query: VisitQuery) -> str:
        title = "<h2>Confirmed arrivals over time</h2>"
        if query.start or query.end:
            hourly = [h for h in hourly if (not query.start or h["hour"] >= query.start)
                      and (not query.end or h["hour"] < query.end)]
        if not hourly:
            return title + "<div class='card'>No reconciled hourly aggregates for these filters.</div>"
        series = _time_buckets(hourly)
        unit = "hour" if len(series) <= 1 or series[1][0] - series[0][0] == timedelta(hours=1) else "day"
        width, height, pad = 720, 150, 28
        peak = max(count for _, count in series) or 1
        step = (width - 2 * pad) / len(series)
        bars = []
        for i, (bucket, count) in enumerate(series):
            bar_h = (height - 2 * pad) * count / peak
            bar_w = min(max(step - 2, 1), 24)
            bars.append(f"<rect class='bar' x='{pad + i * step + (step - bar_w) / 2:.1f}' y='{height - pad - bar_h:.1f}' "
                        f"width='{bar_w:.1f}' height='{bar_h:.1f}'><title>{bucket:%Y-%m-%d %H:00} UTC: "
                        f"{count} confirmed</title></rect>")
        axis = (f"<line x1='{pad}' x2='{width - pad}' y1='{height - pad}' y2='{height - pad}' stroke='currentColor' stroke-opacity='.3'/>"
                f"<text class='axis' x='{pad}' y='{height - 8}'>{series[0][0]:%Y-%m-%d %H:00}</text>"
                f"<text class='axis' x='{width - pad}' y='{height - 8}' text-anchor='end'>{series[-1][0]:%Y-%m-%d %H:00}</text>"
                f"<text class='axis' x='{pad - 4}' y='{pad + 4}' text-anchor='end'>{peak}</text>"
                f"<text class='axis' x='{pad - 4}' y='{height - pad}' text-anchor='end'>0</text>")
        return (title + "<div class='card scroll'>"
                f"<svg viewBox='0 0 {width} {height}' width='100%' role='img' aria-label='Confirmed visits per UTC {unit}'>"
                f"{''.join(bars)}{axis}</svg><p class='muted'>Confirmed (reconciled) visits per UTC {unit} of the first "
                "observed inside point, summed over the selected ports; empty periods are shown as gaps.</p></div>")

    @staticmethod
    def _visit_section(result: dict) -> str:
        rows = result["visits"]
        if not rows:
            return "<h2>Visits</h2><div class='card'>No visits match these filters.</div>"
        body = []
        for v in rows:
            tags = "".join(f"<span class='tag{' c' if 'censored' in f else ''}'>{_e(f)}</span>" for f in v["quality_flags"])
            body.append(
                f"<tr><td>{_e(v['mmsi'])}</td><td>{_e(v['port_id'])}</td><td>{_e(v['status'])}</td>"
                f"<td>{_t(v['possible_entry_after'])} → {_t(v['possible_entry_by'])}</td>"
                f"<td>{_t(v['possible_exit_after'])} → {_t(v['possible_exit_by'])}</td>"
                f"<td>{v['observed_span_seconds'] / 60:.0f}</td><td>{v['maximum_gap_seconds'] / 60:.0f}</td>"
                f"<td>{v['observation_count']}</td><td>{tags}</td>"
                f"<td title='source digest {_e(v['source_digest'])}'>{v['source_record_count']} rows · "
                f"<code>{_e(v['source_digest'][:10])}</code> · {_e(v['rule_version'])}</td></tr>")
        note = (f"<p class='muted'>Showing {len(rows)} of {result['total']}; narrow the filters to see the rest.</p>"
                if result.get("truncated") else f"<p class='muted'>{result['total']} visits.</p>")
        return ("<h2>Visits</h2><div class='card scroll'><table><tr><th>MMSI</th><th>Port</th><th>Status</th>"
                "<th>Entry between (UTC)</th><th>Exit between (UTC)</th><th>Observed span (min)</th>"
                "<th>Max gap (min)</th><th>Obs.</th><th>Flags</th><th>Provenance</th></tr>"
                f"{''.join(body)}</table>{note}"
                "<p class='muted'>Entry and exit are intervals bounded by observations; “unknown” marks a censored bound.</p></div>")

    def _provisional_section(self, query: VisitQuery) -> str:
        result = self.reader.provisional(query)
        header = "<h2>Provisional (streaming) results</h2>"
        if not result["cohorts"]:
            return header + "<div class='card'>No provisional updates received.</div>"
        cohorts = ", ".join(f"{_e(c['cohort_id'])} seq {c['seq']} emitted {_t(c['emitted_at'])}" for c in result["cohorts"])
        if not result["visits"]:
            return header + f"<div class='card'>Cohorts: {cohorts}. No provisional visits match these filters.</div>"
        body = "".join(
            f"<tr><td>{_e(v['mmsi'])}</td><td>{_e(v['port_id'])}</td><td>{_e(v['status'])}</td>"
            f"<td>{_t(v['first_inside'])}</td><td>{_t(v['last_inside'])}</td><td>{_e(v['cohort_id'])}#{v['seq']}</td></tr>"
            for v in result["visits"])
        return (header + "<div class='card scroll banner'><p><strong>Provisional:</strong> these rows may be revised or "
                f"removed by later updates or by the next batch generation. Cohorts: {cohorts}.</p>"
                "<table><tr><th>MMSI</th><th>Port</th><th>Status</th><th>First inside</th><th>Last inside</th>"
                f"<th>Cohort / seq</th></tr>{body}</table></div>")


def _strip(generation: dict) -> dict:
    return {k: v for k, v in generation.items() if k not in ("fence_token",)}


def make_app(db, **kwargs) -> Dashboard:
    return Dashboard(db, **kwargs)
