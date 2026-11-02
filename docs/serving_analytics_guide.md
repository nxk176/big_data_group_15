# Serving and analytics component guide

Owner: Nguyen Quang Huy (20235505). Component scope from the team plan: MongoDB result storage and publication, the dashboard, Spark analytical summaries, MLlib/GraphFrames evaluation and application monitoring.

This guide records what the component does, how to run it, the evidence observed so far and what is still missing. **Every result below comes from synthetic fixtures or labelled synthetic visits.** These results check application and contract behaviour only. They do not describe real vessels, and they do not pass an integration gate.

## Boundaries

```text
Khai (engine) --completed batch artifact--> serving.load_artifact --> publish --> MongoDB generation --> dashboard
Khai (stream) --provisional cohort update--> serving.apply_update --> provisional cohort -----------^
batch artifact / active generation --> analytics (Spark) --> profiles, series, plans, MLlib, graph
```

| Boundary | Format (contract `1.0`) | Behaviour enforced here |
|---|---|---|
| Completed batch | Directory with `visits.jsonl` (one complete `Visit` record per line) and `manifest.json` (`artifact_type=completed_batch`, `contract_version`, `complete`, `run_id`, `rule_version`, `synthetic`, `visit_count`, `status_counts`, `visits_sha256`). Writers create the manifest last. | The artifact is fully validated before any database write. Every `Visit` field is checked, including type, UTC offset, interval order, censoring/null consistency, span arithmetic, bounded source sample, sorted flags and the recomputed `visit_id`. Hash, counts, status counts, rule version and duplicate IDs must also agree. |
| Engine output without a manifest | Week-5 style `visits.jsonl` | `serving_import.py package` wraps the file into an artifact and records the source file's hash. |
| Provisional update | `{contract_version, cohort_id, seq, update_id = SHA256(cohort_id, seq), mode: snapshot\|upsert\|delete, emitted_at, visits, deleted_visit_ids}` | `seq` increases strictly, and gaps are allowed. A retry of an applied `seq` with the same content does nothing. Reusing a `seq` with different content is a conflict. An unseen `seq` at or below the applied one is stale. Each applied `seq` stores the whole cohort under that `seq`, then a compare-and-set moves the cohort pointer. Readers therefore never see a mixture of two sequences. |
| Serving generation | MongoDB collections `generations`, `visits`, `aggregates`, `serving_state`, `leases`, `provisional_visits`, `provisional_cohorts`, `audit_log` | The generation ID is deterministic: `SHA256(run_id, visits_sha256)`. Document IDs are deterministic too, so retries are idempotent. Only a `complete` generation whose stored counts equal the manifest can be activated. The pointer move is a compare-and-set guarded by a lease fencing token. |

The flow inside `serving.publish` is: acquire lease (fencing token N) → generation `loading` → upsert visits and hourly aggregates → verify stored counts → check token → mark `complete` → move the `active` pointer, but only if no newer token has set it → release lease. A crash at any step leaves the previous generation active. Recovery is one of two actions: rerun the same import, which resumes idempotently, or run `recover`, which marks abandoned `loading` generations `failed`.

The dashboard looks up the active generation **once per request** and passes that ID to every query. A page therefore never mixes generations, even if the pointer moves mid-request (`test_one_request_uses_one_generation`).

## Files

| Path | Purpose |
|---|---|
| `src/harbortrace/serving.py` | Validation, artifacts, publication/fencing/rollback/recovery, provisional updates, `Reader`, metrics, health/alerts |
| `src/harbortrace/analytics.py` | Spark analytics, the synthetic visit generator, MLlib with a usable-sample gate, the port-transition graph |
| `dashboard/app.py` | Read-only WSGI dashboard and JSON/health/metrics endpoints (no extra dependencies) |
| `scripts/serving_import.py` | Result-import runner: `import`, `package`, `apply-updates`, `status`, `activate`, `rollback`, `recover`, `demo` |
| `scripts/serve_dashboard.py` | Dashboard server |
| `scripts/analytics_run.py` | Analytics runner (writes ignored outputs under `data/analytics/`) |
| `scripts/make_serving_fixtures.py` | Rebuilds or checks `tests/fixtures/serving/` (synthetic) |
| `tests/unit/test_serving.py`, `test_dashboard.py`, `test_analytics.py` | Component checks |
| `tests/integration/test_mongo_server.py` | Opt-in check against a real MongoDB server |

## Run it

Ubuntu/WSL2 with Python 3.12 and Java 17 (the team baseline), from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-serving.txt
```

Run all component tests (no MongoDB server is required, because the unit tests use `mongomock`):

```bash
python -m pytest -q
```

Run the publication walkthrough with checks:

```bash
PYTHONPATH=src python scripts/serving_import.py --memory demo
```

Start the dashboard on synthetic fixtures (then open <http://127.0.0.1:8050/>):

```bash
PYTHONPATH=src:. python scripts/serve_dashboard.py --memory --load-fixtures
```

Run the Spark analytics on synthetic visits:

```bash
PYTHONPATH=src python scripts/analytics_run.py --synthetic
```

Run the same analytics on any completed batch artifact:

```bash
PYTHONPATH=src python scripts/analytics_run.py --artifact tests/fixtures/serving/generation_a
```

With a MongoDB server, use an isolated database. Import a generation:

```bash
export HARBORTRACE_MONGO_URI=mongodb://127.0.0.1:27017
PYTHONPATH=src python scripts/serving_import.py --database harbortrace_dev import tests/fixtures/serving/generation_a
```

Apply the provisional update fixture:

```bash
PYTHONPATH=src python scripts/serving_import.py --database harbortrace_dev apply-updates tests/fixtures/serving/provisional_updates.json
```

Check status and health:

```bash
PYTHONPATH=src python scripts/serving_import.py --database harbortrace_dev status
```

Serve the dashboard from that database:

```bash
PYTHONPATH=src:. python scripts/serve_dashboard.py --database harbortrace_dev
```

Run the opt-in integration test against the server:

```bash
HARBORTRACE_TEST_MONGO_URI=$HARBORTRACE_MONGO_URI python -m pytest tests/integration -q
```

To serve the week-5 engine output (the current published processing path), run the pipeline, package its visits and import them:

```bash
PYTHONPATH=src python scripts/week05_ingestion.py
```

```bash
PYTHONPATH=src python scripts/serving_import.py package data/week05/<run>/visits.jsonl --run-id week05-fixture --synthetic --out data/serving/artifacts/week05-fixture
```

```bash
PYTHONPATH=src python scripts/serving_import.py --database harbortrace_dev import data/serving/artifacts/week05-fixture
```

GraphFrames is optional. When its package can be downloaded, add `--packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 --repositories https://repos.spark-packages.org` to `analytics_run.py`. Without it, the graph section reports DataFrame-computed degrees and transitions and says that PageRank was not run.

## Observed results so far (2 November 2026, synthetic input)

| Check | Command | Observed |
|---|---|---|
| Unit and component tests | `python -m pytest -q` in a fresh venv from `requirements-serving.txt` | 72 passed, 1 skipped (the real-MongoDB test without a server) |
| Fixture reproducibility | `make_serving_fixtures.py --check` | `fixtures up to date` |
| Publication walkthrough | `serving_import.py --memory demo` | 11/11 checks passed. Generation A was published, and retrying it did nothing. A simulated crash after 4 of 11 visits left A active, and readers saw only A. The retry completed B. Rollback reactivated A. A stale fencing token was refused. All 7 provisional steps (applied, duplicate, conflict, delete, stale, snapshot) behaved as expected. |
| Engine output → serving | week-5 runner → `package` → `import` | The 2 Spark-written visits passed full validation and were published as one generation |
| Dashboard | HTTP smoke test plus headless render at 1280 px and 390 px | Pages, filters, empty states, synthetic label, uncertainty intervals, provisional section, `/healthz` and `/metrics` work. On phone width, tables scroll inside their cards. |
| Analytics (`--synthetic`, 1,979 visits, 12 synthetic ports, Spark 3.5.7 `local[2]`) | `analytics_run.py --synthetic` | Pivot/unpivot preserved the totals. Broadcast and sort-merge plans were selected as hinted, and their rows were equal (1,979). The multiple join returned 1,979 rows. The time series had 170 port-days with 12 flagged. MLlib KMeans picked k=2 (silhouette 0.578, seed agreement 1.00; k=3 averaged 0.524 with agreement 0.75). Graph: 12 vertices and 144 edges, using the DataFrame engine. |
| MLlib gate on the 12-visit fixture | `analytics_run.py --artifact tests/fixtures/serving/generation_a` | `insufficient_data`: 0 qualifying ports, so no model was fitted |

These synthetic numbers come from a generator with port profiles that were set on purpose. The KMeans result therefore shows only that the pipeline and evaluator work. The span/observation correlation of ≈1.0 is also an artifact of the generator's fixed 5-minute reporting interval.

## Analytics design (week-6 definition)

- **Features per port** (reconciled visits only): median observed span, median absolute deviation of span, censored share, long-gap share, night-arrival share (00–05 UTC), visits per vessel and mean observations per visit. The features are standardized before KMeans.
- **Usable-sample criteria:** at least 10 ports, each with at least 20 complete (uncensored) reconciled visits. This is the project's method choice, not a course rule. `cluster_ports` refuses to fit below this threshold and never pads its input. With only the two Los Angeles/Long Beach polygons, the real-data gate cannot pass. Before week 8, Vu's coverage audit must either add qualifying ports or the team must agree on a documented alternative experiment.
- **Evaluation:** silhouette for k ∈ {2, 3, 4} over three seeds, plus pairwise co-membership agreement across seeds (stability). Report the chosen k with its stability. Interpret clusters only for real data with enough samples.
- **Time series:** daily confirmed arrivals per port with empty calendar days filled, a trailing 7-day baseline that excludes the current day, and |z| ≥ 2 flags. A flag marks something to review; it is not proof of congestion.
- **Graph:** a directed port-transition graph built from consecutive visits of the same MMSI. Outputs are in/out degree and top edges, plus PageRank and strongly connected components when GraphFrames is available.
- **Required Spark techniques:** window functions (`lag`/`lead`/`row_number`/rolling average/`dense_rank`), pivot and `DataFrame.unpivot`, a Python UDF (`span_bucket`), custom aggregates written as zero/merge/finish folds (`censored_share_agg`, `span_mad_agg`), multistage transformations, broadcast vs sort-merge vs multiple joins with equality checks and saved plans, and `approxQuantile`/`corr`/`skewness`.

The custom aggregates run inside the JVM. An earlier pandas grouped-aggregate UDF version needed Apache Arrow, which fails on Java 21 with Spark 3.5. The fold version works on Java 17 and 21, and the tests check it against the same values computed in Python.

## Monitoring, security and recovery (application side)

- **Metrics:** counters, gauges and timings for imports, activations, fenced writes, publish failures, provisional applied/duplicate/stale/conflict, update lag, reads and HTTP requests/errors/latency. They are exported at `/metrics` (Prometheus text) and in `serving_import.py demo`.
- **Health and alerts:** `serving.health` and `/healthz` raise these alerts: no active generation, active generation incomplete, active count mismatch (critical, HTTP 503), stale generation (> 36 h), failed generations, publication stuck loading (> 30 min) and provisional lag (> 15 min). The thresholds can be overridden.
- **Audit:** every loading, completion, activation, rollback, failure and drop is appended to `audit_log` and written as a JSON log line.
- **Access:** the dashboard is GET-only and can require a bearer token (`HARBORTRACE_DASHBOARD_TOKEN`, compared in constant time). `/healthz` stays open for probes. Responses send `nosniff`, `no-store` and a restrictive Content-Security-Policy, and all output is HTML-escaped. `connect(..., read_only=True)` is intended to run with a read-only MongoDB user. Creating that user is a deployment task.
- **Durability:** connections use majority write and read concerns. Old generations are kept for rollback until a deliberate `drop_generation`, which refuses to drop the active generation or the rollback generation.

## Limitations and evidence still required

| Plan item | Status |
|---|---|
| Week 6–8: fixtures, isolated import/query, dashboard filters/uncertainty, generation activation, update replacement, retries and stale checks | Implemented and tested on synthetic fixtures with `mongomock`. **Not yet run against a real MongoDB server here**: the sandbox could not install one. Run `tests/integration` on the team machine. |
| Week 9 gate: import actual engine output and reconcile dashboard counts | The path works with the published week-5 output. The gate needs Khai's real-data artifacts and a joint count reconciliation. |
| Week 10: versioned remote connections | The URI, database and token are parameters. A deployed MongoDB and dashboard depend on Vu's environment. |
| Week 11: MLlib with real inputs, GraphFrames, time-series interpretation | The code and the gate are ready. Real evaluation is blocked until the coverage audit supplies qualifying data. GraphFrames needs the package on the team machine. |
| Week 12: query measurements, alerts, deployed read consistency | Local timings and alerts exist. Measurements on deployed infrastructure are still required. |
| Week 13: crash, fencing and sink-retry tests, access checks, application recovery | Simulated in-process (crash after N visits, stale token, retries, rollback, recover). Repeat these against the deployed MongoDB, including a killed process and a database restart. |
| Week 14–15: report sections, evidence index, presentation | Not written. Use the measured results from the gates, not these fixture numbers. |
| Prototype `streaming.py` database-write logic | `streaming.py` is not in the published repository. Once Khai publishes the stream engine, it should emit the provisional-update contract above instead of writing to MongoDB. |

Ownership note: per the team plan, `contracts.py`, `config.py`, `visits.py` and `spark_core.py` belong to other owners and were not modified. This component only reads them. The code here was produced with AI assistance (Claude Code) at the owner's request. Disclose that according to course policy. The owner remains responsible for reviewing and explaining it.
