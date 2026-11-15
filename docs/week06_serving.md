# Week 6: serving evidence

Component owner: Nguyen Quang Huy (20235505).
Execution date: 15 November 2026, Asia/Ho_Chi_Minh.

Week 6 requires complete synthetic visits and updates, an isolated MongoDB import, queries, and schema validation.
A schema defines the required fields and their types.
The existing serving implementation and committed fixtures supply these functions.
This work adds real-server tests and execution evidence for that implementation.

## Input and boundaries

All inputs are synthetic and use contract `1.0` and rule `visits-v1`.
They demonstrate application behavior and do not establish accuracy for real vessels.
A reconciled visit represents the completed batch reconstruction.
A cohort groups visits that one update replaces.

| Input | Expected content |
|---|---|
| `tests/fixtures/serving/generation_a/` | 12 complete visits: 10 reconciled and 2 candidates, across ports 4110 and 4120 |
| `tests/fixtures/serving/generation_b/` | 11 complete visits: 10 reconciled and 1 candidate, after a late observation bridges a split |
| `tests/fixtures/serving/provisional_updates.json` | Seven delivery steps: snapshot, retry, upsert, conflict, delete, stale update, replacement snapshot |

Each visit includes all fields from `Visit` in `src/harbortrace/contracts.py`.
Censoring marks an unknown entry or exit boundary.
The validator covers identity, UTC times, nullable uncertainty bounds, censoring, durations, source evidence, and field types.
The artifact validator also compares the file hash, counts, status totals, rule version, and duplicate identities.
A completed batch artifact contains visit records and a manifest.

The analytics design for week 6 remains in the [component guide](serving_analytics_guide.md#analytics-design-week-6-definition).
It specifies port features, sample thresholds, and evaluation methods.
The clustering threshold requires ten ports with twenty complete reconciled visits each.
These two-port fixtures cannot support that evaluation.

## Reproduce the tests

Use Python 3.12 and a MongoDB Community server.
Install the server with the [official MongoDB instructions](https://www.mongodb.com/docs/manual/administration/install-community/).
The observed run used MongoDB 8.0.17 on macOS 15.7.2, Apple Silicon, without Docker.

From the repository root, prepare the Python environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-serving.txt
PYTHONPATH=src python scripts/make_serving_fixtures.py --check
```

In a separate terminal at the repository root, start a temporary server:

```bash
mkdir -p data/serving/week06-mongo/db
mongod --dbpath "$PWD/data/serving/week06-mongo/db" \
  --bind_ip 127.0.0.1 --port 27027
```

The `mongod` command requires its installed executable on `PATH`.
The server binds to the local machine and uses the separate directory above.
If port 27027 is occupied, select another free port in both commands.

In the activated Python terminal, run the real-server tests:

```bash
HARBORTRACE_TEST_MONGO_URI=mongodb://127.0.0.1:27027 \
  python -m pytest tests/integration/test_mongo_server.py -q
```

Each test creates a database named `harbortrace_it_<random UUID>`.
Each test removes its database and closes its connection during cleanup.
An unreachable server causes failure when the URI is set.
Without the URI, pytest skips the real-server tests.

If unrelated Python plugins interfere, use this command from the activated Python terminal:

```bash
env -u PYTHONPATH PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  HARBORTRACE_TEST_MONGO_URI=mongodb://127.0.0.1:27027 \
  python -m pytest tests/integration/test_mongo_server.py -q
```

This command removes inherited Python paths and disables automatic pytest plugins for that process.
The observed machine required this isolation because its shell loaded ROS plugins.

For the full suite, install `requirements-engine.txt` in the same Python environment.
Set `JAVA_HOME` to the directory of your installed Java 17 runtime.
Keep the temporary MongoDB server running until this command finishes:

```bash
env -u PYTHONPATH PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  JAVA_HOME="$JAVA_HOME" SPARK_LOCAL_IP=127.0.0.1 \
  HARBORTRACE_TEST_MONGO_URI=mongodb://127.0.0.1:27027 \
  python -m pytest -q -rs
```

Stop the temporary server with Ctrl+C in its terminal after all requested tests finish.

## Observed results

| Behavior | Observed result |
|---|---|
| Fixture reproduction | `fixtures up to date` |
| Real-server integration suite | 12 tests passed |
| Serving, dashboard, and real-server tests together | 63 tests passed in 3.53 seconds |
| Full suite with Java 17 and the real MongoDB server | 153 passed, 2 skipped, 16 warnings in 47.33 seconds |
| Complete import and readback | All 12 records matched their input fields and values |
| Port, status, date, and empty-result queries | Port 4110 returned 6 visits; candidates returned 2; the late-evening interval returned 2; unknown port returned 0 |
| Hourly totals | Reconciled totals summed to 10 |
| Repeated import | One generation and 12 visits remained after retry |
| Invalid artifacts | Six cases failed before changing any database collection |
| Invalid updates | Three cases failed without changing the existing cohort |
| Provisional sequence | All seven outcomes and visible visit identities matched the fixture; the final cohort contained one visit at sequence 5 |
| Interrupted publication | The previous generation remained visible; retry completed the next generation |
| Query index | MongoDB reported an `IXSCAN` for the generation and port query |
| Existing CLI demonstration on the real server | All 11 assertions passed, including rollback and refusal of an outdated publisher token |

The six artifact failures cover a missing field, incorrect duration, incorrect hash, incomplete manifest, incorrect count, and incorrect status totals.
The three update failures cover a missing field, incorrect visit identity, and a timestamp without a UTC offset.
Tests compare collection contents before and after rejection.

The CLI demonstration reported `degraded` health because the fixture update times are fixed at 2 November 2026.
Its only alert was `provisional_lag`, which correctly reports old updates.
The batch-only health assertion returned `ok`.

The full run skipped two HDFS tests because the machine did not contain the Hadoop command.
The warnings concern deprecated `distutils` version comparisons in PySpark.
An initial Java 21 run failed three streaming tests with an Arrow direct-buffer error.
The same suite passed with Java 17, which is the documented project baseline.

## Scope and remaining work

This evidence completes the executable week-6 serving requirements on a real local MongoDB server.
The original fixture generator, validators, importer, and analytics design predate this change.
The new contribution consists of expanded real-server coverage and reproducible evidence.
Codex assisted with these additions and their execution.

Peer review and the joint schema discussion remain team activities.
This record does not claim that either activity occurred.
The revised November schedule also includes engine review and an updates-directory applier.
Those tasks are separate from the original week-6 deliverable recorded here.

This run uses one local standalone server and synthetic data.
It does not demonstrate a deployed cluster, replication, authentication, real-data analytics, or a completed integration gate.
Raw logs, server binaries, and database files remain under ignored `data/` paths.
