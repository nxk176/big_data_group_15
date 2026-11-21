# Processing engine guide

Owner: Nguyen Xuan Khai (20235508). Component scope from the team plan: Spark batch and streaming, visit reconstruction, late-data reconciliation, correctness checks and processing performance.

This guide records what the engine does, how to run it, what was measured and what is still missing. Results marked **real** use NOAA AIS data for the Los Angeles/Long Beach region (ports 4120 and 4110) prepared by the week-five ingestion; everything else is synthetic and checks behaviour only.

## Boundaries

```text
canonical observations (Parquet/JSONL, local or hdfs://)
    --> engine.run_batch (Spark) --> completed-batch artifact --> serving import
replay messages (Kafka topic or JSONL directory)
    --> streaming.start_stream (Structured Streaming) --> provisional updates --> serving apply_update
```

| Boundary | Format (contract `1.0`) | What the engine guarantees |
|---|---|---|
| Batch input | Canonical observations with the week-five columns | Read with an explicit schema. A run stops before writing if any row lacks a vessel, a parseable time or an identity, or if the input is empty. |
| Batch output | Directory with `visits.jsonl`, `manifest.json`, `spark-plan.txt` | Visits are sorted by `(mmsi, port_id, first_inside, visit_id)` and serialized canonically, so equal input gives an equal `visits_sha256` whatever the partitioning or input order. The directory gets its final name only after the manifest is written; a failed run leaves nothing. An existing artifact is never overwritten. |
| Stream input | Replay messages (canonical fields plus replay identity) | Messages that fail `replay.validate_message` are counted as `invalid` and skipped. |
| Stream output | `updates/batch=<id>/` holding one provisional update per line (`mode: upsert`), `metrics/batch=<id>/`, `checkpoint/`, `report.json` | A cohort is one vessel's activity between long gaps. `seq` increases by one per update. A retried microbatch rewrites the same directory with the same updates, and the serving component ignores an update it has already applied. |

The batch manifest follows the serving component's completed-batch format and records the inputs, row count, ports, rule thresholds, Spark version, master, partitions and elapsed time under `source`.

## How reconstruction runs

**Batch.** Observations are repartitioned by `mmsi` and sorted inside each partition by `(mmsi, event time, logical_id, source_record_id)`: one shuffle and one sort. `mapPartitions` then runs the pure state machine from `visits.py` over each vessel in order. The visit schema is declared, so Spark does not scan the output a second time to infer it, and the result is persisted once for the checks and the ordered write. The package is shipped to the Python workers with `addPyFile`, so the job does not depend on a checkout existing on worker machines. `--master submit` leaves the master to `spark-submit`.

**Streaming.** Messages are grouped by `mmsi` under an event-time watermark (newest event time seen minus the allowed lateness, 10 minutes by default) with `applyInPandasWithState`. For each vessel the saved state holds:

- a buffer of observations newer than the watermark, whose order may still change;
- one constant-size visit machine per port, fed only with observations the watermark has passed, in the batch order.

Observations are not kept after they are released, so state does not grow with a vessel's history. The running `source_digest` uses a SHA-256 whose intermediate state can be saved (`stream_state.ResumableSha256`, checked against `hashlib`), because a `hashlib` object cannot be stored. A timer wakes a vessel when its oldest buffered observation becomes releasable, and removes its state once the watermark is more than the long-gap threshold (30 minutes) past its last observation.

Consequences:

- The provisional view after each microbatch equals the batch reconstruction of the observations released so far. Visits are added or replaced under a stable `visit_id`; none is retracted.
- The view trails the newest event by the allowed lateness.
- An observation is dropped and counted as `late` when it arrives after its vessel's machines have already been fed a later (or equal) event time. Spark does not filter these rows before the state function, so the function does it. A late arrival whose vessel has not yet been released past it is still used. The batch engine reads the retained input and corrects the view. This is the Lambda reconciliation path.
- If a vessel's buffer exceeds `--max-buffer` (5,000), the oldest observations are released early and counted as `forced`; the job does not fail.
- One known difference: a visit still open when a vessel's state expires is reported with `input_range_ended`. Batch, which sees the vessel return later, closes it with `long_gap_unknown_continuity` and counts that gap in `maximum_gap_seconds`.

## Files

| Path | Purpose |
|---|---|
| `src/harbortrace/engine.py` | Spark session, schema-checked input, reconstruction, artifact publication |
| `src/harbortrace/stream_state.py` | Pure per-vessel streaming state: buffer, saveable machines, update construction |
| `src/harbortrace/streaming.py` | Structured Streaming query, Kafka/file sources, per-batch sink, progress report |
| `scripts/engine_batch.py`, `scripts/engine_stream.py` | Runners |
| `scripts/engine_experiments.py` | Replay preparation, late-arrival experiment, kill-and-restart experiment, streaming settings comparison (`tune`), vessel skew and partition count (`skew`) |
| `tests/unit/test_engine.py`, `test_stream_state.py`, `test_streaming.py`, `test_tracker.py` | Component checks |
| `src/harbortrace/visits.py` | Reference state machine (week 4, unchanged) plus `VesselTracker`, which keeps machines only for ports with an open visit |
| `src/harbortrace/spark_core.py` | Week 4–5 partition adapter (unchanged; the week-five script still uses it) |

## Run it

Ubuntu/WSL2 with Python 3.12 and Java 17, from the repository root:

```bash
python3 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements-engine.txt
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # needed by the HDFS client tests if unset
python -m pytest tests/unit/test_engine.py tests/unit/test_stream_state.py tests/unit/test_streaming.py -q
PYTHONPATH=src python scripts/engine_batch.py --fixture
PYTHONPATH=src python scripts/engine_stream.py --fixture
```

Real data (the canonical Parquet files are local and excluded from Git):

```bash
PYTHONPATH=src python scripts/engine_batch.py --real --run-id pilot-2024-01-01 --ports 4120 4110 \
  --input data/canonical/ais-2024-01-01-regional.parquet --output data/engine/pilot-2024-01-01
PYTHONPATH=src python scripts/serving_import.py --memory import data/engine/pilot-2024-01-01
PYTHONPATH=src python scripts/engine_stream.py --source data/replay/pilot-2024-01-01 \
  --output data/stream/pilot-2024-01-01 --stream-id pilot-2024-01-01 --ports 4120 4110 \
  --max-per-trigger 1 --compare-artifact data/engine/pilot-2024-01-01
```

A Kafka source uses `--source kafka:<topic> --kafka-bootstrap <host:port>` and downloads the Spark Kafka connector on first use. Rerunning with the same `--output` resumes from its checkpoint. Add `--spark-ui` to keep the Spark UI on port 4040.

Streaming settings: `--state-store rocksdb` keeps vessel state in RocksDB instead of the default in-memory store (fixed by the first run on an output directory), and `--conf KEY=VALUE` passes any other Spark setting, for example `--conf spark.sql.streaming.minBatchesToRetain=10` to keep fewer old state versions in the checkpoint. The report records wall time, time inside microbatches and checkpoint size.

```bash
# the same replay under several settings; fails unless every setting gives identical updates
PYTHONPATH=src python scripts/engine_experiments.py tune --replay data/replay/all-ports-2024-01-01 --ports all \
  --out data/experiments/tune --driver-memory 3g \
  --variant cores4 "local[4]" 8 default --variant cores8 "local[8]" 8 default --variant rocksdb "local[8]" 8 rocksdb
# observations per vessel and per partition, and batch time for several partition counts
PYTHONPATH=src python scripts/engine_experiments.py skew --canonical data/wide/all-2024-01-01/canonical \
  --ports all --partitions 4 8 16 32 --out data/experiments/skew.json
```

## Observed results (12 November 2026, one laptop, Spark 3.5.7 `local[2]`)

`python -m pytest -q` on the merged tree: **143 passed, 1 skipped** (the opt-in MongoDB server test). The engine checks are in `test_engine.py`, `test_stream_state.py`, `test_streaming.py` and `test_tracker.py`.

### Batch, real data

| Run | Input | Visits | Time | Check |
|---|---|---|---|---|
| Pilot, 1 January 2024 | 235,713 observations, 1 file | 370 (271 reconciled, 99 candidate) | 9 s, 2 partitions | Every field of every visit equals the earlier local run of the same day |
| Core, 2–15 January 2024 | 3,264,274 observations, 14 files | 5,081 (3,869 reconciled, 1,212 candidate) | 46 s, 4 partitions | Every field of every visit equals the earlier local 14-day run |

Both artifacts pass the serving component's `load_artifact` and import into an in-memory generation (370 visits/44 hourly aggregates; 5,081 visits/608 hourly aggregates). `analytics_run.py --artifact` runs on the 14-day artifact; its port clustering stays gated because only two ports qualify.

### Streaming, real data

The 1 January canonical file was turned into 236,213 replay messages with `replay.replay_messages` (500 seeded duplicate copies) and replayed twice: from 48 half-hour JSONL files, and from a four-partition Kafka 3.9.1 topic keyed by `mmsi` on a temporary single broker.

| Measure | File source | Kafka source |
|---|---|---|
| Microbatches | 49 | 49 (5,000 offsets per trigger) |
| Accepted / duplicates / late / forced / invalid | 235,713 / 500 / 0 / 0 / 0 | 235,713 / 500 / 0 / 0 / 0 |
| Rows dropped by the watermark | 0 | 0 |
| Vessel states kept at the end | 483 | 483 |
| State store memory at the end | 2.97 MB | 2.23 MB |
| Largest total buffer in one microbatch | 7,308 observations | 6,431 observations |
| Provisional visits | 370 | 370 |
| Wall time | 93 s | 112 s |

Compared with the batch artifact for the same day, both runs give the same 370 `visit_id` values: 235 visits identical in every field; 121 where batch includes observations after the final watermark; 3 whose exit was confirmed after the watermark; 11 that differ only in the end flag and gap described above; none unexplained. The earlier prototype kept every message per vessel and raised an error above 5,000; a synthetic vessel reporting 20,000 times inside one port now keeps under 8 KB of state (`test_state_stays_bounded_for_a_vessel_that_never_leaves`).

### Scaling experiment: every port in the boundary layer (real data, not a published pipeline)

The two-port scope leaves most of the source unused (235,713 of 7,293,408 rows on 1 January) and too few ports for the analytics. To measure the engine beyond it, an unpublished local script assigned the whole 1 January source to all usable USACE port polygons with a spatial index, applying the week-five rules. Restricted to ports 4120 and 4110 it reproduces the existing canonical file exactly (235,713 rows, no field differs). This preparation belongs to the data component and is not part of the engine.

| Measure | Two ports | All ports |
|---|---|---|
| Port polygons used | 2 | 368 (2 rejected: repair changes area by more than 15%) |
| Canonical observations | 235,713 | 7,293,107 |
| Observations inside a port / in overlapping polygons | 85,613 / 1 | 4,510,851 / 2,048 |
| Vessels | 589 | 14,857 |
| Ports with visits | 2 | 204 |
| Visits (reconciled / candidate) | 370 (271 / 99) | 20,487 (16,752 / 3,735) |
| Batch time | 9 s, `local[2]`, 2 partitions | 61 s, `local[4]`, 8 partitions |

With `--ports all` the engine keeps a state machine only for ports where a vessel has an open visit (`visits.VesselTracker`), so the work per observation does not grow with the number of ports. The tracker equals one machine per port on 40 random seven-port tracks, and the two-port artifacts keep the same SHA-256 as before the change. The all-port artifact imports into serving (20,487 visits) and `analytics_run.py` fits its port clustering on it (58 ports with at least 20 complete visits) and finds 458 port-to-port transition edges. These are one-day numbers on one laptop.

The same day was also streamed at this scope. `engine_experiments.py replay-spark` wrote the 7,293,107 canonical rows as 48 half-hour replay files (no duplicate copies), and `engine_stream.py --ports all` ran on them with `local[4]`, 8 partitions, 3 GB driver memory and one file per microbatch.

| Measure | Two ports (file source) | All ports (file source) |
|---|---|---|
| Messages accepted / late / forced | 235,713 / 0 / 0 | 7,293,107 / 0 / 0 |
| Microbatches | 49 | 49 |
| Updates emitted | 6,274 | 323,353 (no `(cohort_id, seq)` repeated) |
| Vessel states kept at the end / expired | 483 / – | 11,349 / 7,131 |
| State store memory at the end | 2.97 MB | 65 MB |
| Largest total buffer in one microbatch | 7,308 observations | 211,608 observations |
| Checkpoint directory at the end | – | 1.9 GB |
| Wall time | 93 s | 31 min 26 s for 46 of the 49 microbatches (see below) |
| Largest resident memory of one process | – | 2.1 GB |

The first attempt was stopped from outside after three microbatches because the laptop ran low on memory. It was restarted with the same command and output directory, resumed from the checkpoint, and the final output has every message accepted exactly once and no repeated update identity, so this run is a second, unplanned restart test. Compared with the all-port batch artifact: 20,375 provisional visits, all with a batch `visit_id`; 12,477 identical in every field; 6,382 where batch has observations after the final watermark; 51 whose exit confirmation was not released; 1,465 that differ only in the end flag and gap; 112 batch visits that start after the watermark; none unexplained. Streaming is about 30 times slower than the batch run of the same day, mostly per-microbatch state handling; the checkpoint size comes from the default state store keeping several versions of the state.

### Streaming throughput (20 November 2026, real data sample)

The all-port stream above was slow for two reasons found by profiling. The running `source_digest` was a pure-Python SHA-256 whose state was copied for every update; it now uses OpenSSL through `ctypes` when available (checked against `hashlib` at import, with the pure-Python version as fallback and the exported state interchangeable between the two). The sink wrote the updates with `coalesce(1)` first, which computed every state partition inside one task; it now writes the per-partition metrics first, so the state step runs in parallel and the single-file write reads the cached result.

Sample: the first 8 half-hour replay files of 1 January at all-port scope (1,208,054 messages, 9 microbatches, 53,739 updates), 3 GB driver memory, one file per microbatch.

| Code and setting | Wall time | Checkpoint | State memory |
|---|---|---|---|
| Committed code (`4d6a580`), `local[4]`, 8 partitions | 305 s (`/usr/bin/time`, includes JVM start) | – | 64.9 MB |
| New code, `local[1]`, 8 partitions | 197 s | 299.5 MB | 65.0 MB |
| New code, `local[2]`, 8 partitions | 126 s | 299.5 MB | 65.0 MB |
| New code, `local[4]`, 8 partitions | 93–97 s (two runs) | 299.5 MB | 65.0 MB |
| New code, `local[4]`, 4 partitions | 96 s | 299.4 MB | 66.2 MB |
| New code, `local[8]`, 8 partitions | 61–84 s (two runs) | 299.5 MB | 65.0 MB |
| New code, `local[8]`, 16 partitions | 89 s | 299.6 MB | 69.1 MB |
| New code, RocksDB state store, `local[4]`, 8 partitions | 99 s | 415.2 MB | 67.2 MB |
| New code, RocksDB state store, `local[8]`, 16 partitions | 70 s | 415.6 MB | 133.8 MB |

Every run gives the same 53,739 updates apart from `emitted_at` and the stream name inside `cohort_id`/`update_id`; with the same stream name the committed and the new code give byte-identical updates. At the same setting the new code is about 3.2 times faster; with 8 cores about 4–5 times. More cores help up to the 8 tested; the partition count barely matters; RocksDB is not faster here and its checkpoint is 40% larger, so the default store stays. The spread between repeated runs (61 and 84 s) comes from other load on the laptop, so differences under about 20% are not meaningful. These are sample numbers on one laptop; the 14-day all-port run has not been done yet.

### Late arrivals: what the watermark costs and what batch restores (real data)

`engine_experiments.py late` took the 236,213 replay messages of 1 January, made a seeded 2% of them (4,724) arrive 60 minutes after their event time, and ran the stream once per watermark setting. One microbatch held 30 minutes of arrivals. "Wrong" counts provisional visits that differ in any field from the batch reconstruction of all observations up to the final watermark, other than the end-flag difference described above.

| Allowed lateness | Messages dropped | Visits expected at the final watermark | Wrong until batch corrects | Largest total buffer |
|---|---|---|---|---|
| 5 min | 3,945 | 370 | 216 | 6,267 |
| 10 min | 3,199 | 370 | 211 | 7,172 |
| 30 min | 0 | 362 | 0 | 10,748 |
| 60 min | 0 | 355 | 0 | 16,179 |

A short watermark gives a fresher view with a smaller buffer but loses late observations, and losing under 2% of the messages touches more than half of the visits, because long visits each contain many observations. A longer watermark loses nothing but delays the view (fewer visits are visible at the end) and buffers more. The batch artifact for the day contains all 370 visits with every observation, so it replaces the damaged provisional records. A message delayed by 20 minutes was never dropped at any setting in this layout, because a vessel's state is released one microbatch behind the watermark.

### Killed job (real data)

`engine_experiments.py crash` ran the same replay, sent SIGKILL to the driver, JVM and Python workers after 20 microbatch directories existed (2,404 updates written), and restarted the command on the same output directory. Result: 49 microbatches, 6,274 updates, identical to an uninterrupted run apart from the `emitted_at` wall-clock field; no `(cohort_id, seq)` repeated; 235,713 messages accepted in both. This covers a whole-job crash on one machine. Loss of a single executor on a cluster is not tested.

### Standalone Spark master (one laptop)

The all-port batch was also submitted to a temporary Spark standalone master with two worker processes (2 cores, 2 GB each) on the same laptop. The package reached the executors through `addPyFile`, and the artifact has the same SHA-256 as the `local[4]` run (70 s against 61 s). This checks the non-local code path only; it is not a multi-machine measurement.

## Not yet done

- No multi-machine run. Everything above is one laptop; `hdfs://` inputs and `--master submit` are written for a cluster but unmeasured there.
- No end-to-end run with the team's long-lived services: the Kafka test used a temporary broker, updates were applied to an in-memory database in tests, and the real MongoDB server was not used.
- Streaming was measured on replays at full speed. Loss of one executor during a microbatch, Kafka partition skew and a live-rate source are not measured. The late-arrival and crash experiments used the file source, not Kafka.
- No tuning study yet (partition counts, state store provider such as RocksDB, trigger interval, vessel skew). The all-port stream is the case that needs it: 31 minutes and a 1.9 GB checkpoint for one day.
- Updates only grow a cohort; retention of old provisional visits after a batch generation covers them is not defined.
- The default watermark delays the provisional view by 10 minutes. No measurement yet relates this delay to real late arrivals.

## Provenance

`visits.py` and `spark_core.py` come from the prototype built before the repository's first commit and were published in weeks 4–5. The prototype's batch runner wrote Parquet without the serving manifest and required a laptop-specific configuration; its streaming job kept every message per vessel and wrote to MongoDB directly. `engine.py`, `stream_state.py`, `streaming.py`, their runners and tests are new code written for this milestone against the fixtures and formats published by the other two components. Development used AI coding assistance (Claude Code); the owner reviews and is responsible for the result.
