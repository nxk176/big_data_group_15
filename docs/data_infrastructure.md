# Data and infrastructure component

Owner: Do Dang Vu — 20235578. This component prepares source data, publishes canonical observations (normalized position records) and replay messages, stores them in HDFS (Hadoop Distributed File System) and runs the supporting services. It produces the inputs for the processing engine; it does not reconstruct visits or write MongoDB.

| Part | Files |
|---|---|
| Source preparation, quality, spatial assignment | `src/harbortrace/ingest.py`, `quality.py`, `spatial.py` (inherited week-five prototype; `spatial.PortIndex` added) |
| Spark preparation into day-partitioned canonical data | `src/harbortrace/spark_prepare.py` |
| Replay identity and accounting | `src/harbortrace/replay.py` |
| Kafka replay publisher and diagnostic consumer | `src/harbortrace/kafka_replay.py` |
| HDFS publication and readback | `src/harbortrace/hdfs.py` |
| Native HDFS and Kafka service scripts | `deploy/native/` |
| Entry script | `scripts/data_storage.py` |
| Boundary fixtures | `tests/fixtures/canonical/` |
| Checks | `tests/unit/test_replay.py`, `test_hdfs.py`, `test_kafka_replay.py`, `test_spatial_index.py`, `test_spark_prepare.py` |

To bring up HDFS and Kafka on an empty machine, follow the [quickstart](services_quickstart.md). Install with `python -m pip install -r requirements-data.txt` (the week-five preparation stack plus numpy and the `confluent-kafka` client).

## Week 6: boundary fixtures and verified HDFS readback

### Canonical and replay fixtures for the engine

`tests/fixtures/canonical/` contains **synthetic** boundary examples prepared by the real week-five preparation path from `tests/fixtures/week05_ingestion.json`:

| File | Content |
|---|---|
| `week06_canonical.jsonl` | 7 canonical observations, one JSON object per line, sorted keys, ordered by `mmsi`, `event_time`. Each line loads with `Observation.from_json`. |
| `week06_replay.jsonl` | 8 replay messages: the 7 observations in event-time order plus one seeded producer-retry copy (`copy_index` 1). |
| `week06_manifest.json` | Origin fixture hash, file SHA-256 (Secure Hash Algorithm, 256-bit) values, source row accounting (14 = 7 canonical + 3 invalid + 1 duplicate + 2 conflicting + 1 outside region) and replay accounting. |

A replay message carries every canonical field plus `contract_version`, `replay_run_id`, `copy_index`, `ingested_at` and `replay_id = SHA256(run ID, source record ID, copy index)`, as defined in the [data contracts](week03_contracts.md). The Kafka key is `mmsi`, so one vessel's messages share a partition and keep their order. A copy has a new `replay_id` but the same `source_record_id` and `logical_id`: it is a transport retry, not new vessel activity. `harbortrace.replay.validate_message` rejects missing fields, another contract version, naive times or a `replay_id` that does not match its identity; `account` returns the sent/distinct counts that the Kafka diagnostic consumer will compare in week 7.

Regenerate, or check that the committed files still match a fresh preparation:

```bash
PYTHONPATH=src python scripts/data_storage.py fixtures          # rewrite
PYTHONPATH=src python scripts/data_storage.py fixtures --check  # compare, write nothing
```

The fixtures establish contract behavior. They do not measure accuracy on real AIS (Automatic Identification System) data.

### HDFS on a new machine

The scripts in `deploy/native/` now derive paths from the checkout instead of the original WSL (Windows Subsystem for Linux) laptop path. `env.sh` sets the defaults; override them when needed:

| Variable | Default |
|---|---|
| `HARBORTRACE_DATA_DIR` | `<checkout>/data` (NameNode, DataNode, PID and log directories) |
| `HARBORTRACE_RUNTIME_DIR` | `~/.local/harbortrace` (Hadoop runtime) |
| `JAVA_HOME` | Java 17 from `/usr/libexec/java_home`, Homebrew `openjdk@17`, or the Ubuntu package path |
| `HDFS_URI` | `hdfs://127.0.0.1:9000` |

`core-site.xml` and `hdfs-site.xml` read `${env.HARBORTRACE_DATA_DIR}`. Checksums use `sha256sum`/`sha512sum` on Linux and `shasum` on macOS. `hdfs-stop.sh` stops only processes recorded by `hdfs-start.sh`.

```bash
bash deploy/native/hdfs-install.sh
bash deploy/native/hdfs-start.sh --init-empty   # first start of a new, empty store only
bash deploy/native/hdfs-smoke.sh
bash deploy/native/hdfs-stop.sh
```

**Defect found and fixed:** with no `log4j.properties` in `HADOOP_CONF_DIR`, Hadoop printed its native-library warning to standard output, so `hdfs dfs -cat` returned the file bytes plus a log line and the week-two smoke reported a checksum mismatch on macOS. `deploy/native/log4j.properties` now sends client and daemon logs to standard error, so `-cat` output carries only file bytes.

### Canonical HDFS readback

```bash
PYTHONPATH=src python scripts/data_storage.py hdfs-readback
```

The command prepares the synthetic source into canonical Parquet (column-oriented files) and publishes it to `/harbortrace/canonical/<name>-<first 16 hex digits of the Parquet SHA-256>/`. Each upload goes to a temporary name and is renamed only after completion. The command then:

1. reads the stored bytes back with `hdfs dfs -cat` and compares byte count and SHA-256 with the local file;
2. reads the HDFS Parquet with Spark through the `hdfs://` URI and compares the row count with the preparation manifest;
3. checks that every local `(logical_id, source_record_id)` pair is present in the HDFS copy;
4. writes a report to `data/storage/hdfs-<timestamp>/hdfs-readback.json`.

The dataset is content-addressed: rerunning with identical Parquet reuses the stored file after verifying it, and a different file at the same path is refused instead of overwritten. Each preparation's manifest (with its run time and local paths) is kept as a separate provenance record under `manifests/<SHA-256 prefix>.json`. Use `--canonical <path>.parquet` to publish an existing prepared file with its `.manifest.json`, such as the real January 1 pilot, and `--uri` for another NameNode.

### Observed result

Run on 2 November 2026 on macOS 26.5 (Apple silicon) with Python 3.12.16, OpenJDK 17.0.21, Hadoop 3.5.0 (one NameNode, one DataNode) and PySpark 3.5.7:

```text
HDFS smoke passed: live_datanodes=1 bytes=110 sha256=e06fb5d61ce57e0a6889afc5ff74ad478835d0bab292191e7ef0a221605d440d path=hdfs://127.0.0.1:9000/harbortrace/week02/hdfs_week02.txt
```

| Check | First run | Second run |
|---|---|---|
| Parquet bytes / SHA-256 match | 6,855 B, match | match, reused |
| Manifest bytes / SHA-256 match | 1,488 B, match | match, new provenance record |
| Manifest canonical rows / Spark rows from HDFS | 7 / 7 | 7 / 7 |
| Local rows absent in HDFS | 0 | 0 |

`fixtures --check` passed, and `python -m pytest tests/unit -q` passed 18 checks: the 11 existing reference checks plus 5 replay/fixture checks and 2 HDFS checks. The HDFS checks run the real `hdfs dfs` client against a temporary `file://` store; they need the Hadoop runtime but no running services, and are skipped without it.

### Limitations

- One DataNode with `dfs.replication=1` on one laptop: this shows publication and readback, not block replication, placement across workers or recovery. Those are week 10–13 deliverables on actual worker machines.
- The canonical input here is synthetic. The real one-day pilot can be published with `--canonical`, but its source files are not in Git and must be acquired as described in the [week-five guide](week05_ingestion.md).
- Preparation is still single-process PyArrow/Shapely code, not a distributed Spark job.

## Week of 12 November: Kafka replay with sent/received accounting

### HDFS client without `JAVA_HOME`

Hadoop's `bin/hdfs` launcher refuses to start without `JAVA_HOME`; Spark only needs `java` on `PATH`. With the variable unset, the HDFS client tests therefore failed on machines where every Spark test passed. `Hdfs.from_environment` now resolves Java 17 in the order used by `deploy/native/env.sh` (then the real path of `java` on `PATH`), checks that it runs and passes it to the command. With no usable Java it raises `HdfsError`, so the tests skip. `env -u JAVA_HOME python -m pytest tests/unit/test_hdfs.py` passes 4 checks on macOS (2 failed before).

### Native Kafka broker

```bash
bash deploy/native/kafka-install.sh                 # Kafka 4.1.2 (Scala 2.13), pinned SHA-512
bash deploy/native/kafka-start.sh --init-empty      # first start of a new, empty broker store only
bash deploy/native/kafka-stop.sh
```

One KRaft (Kafka Raft metadata mode, no ZooKeeper) process acts as broker and controller, bound to `127.0.0.1:9092` (clients) and `:9093` (controller). Logs are in `$HARBORTRACE_DATA_DIR/kafka/logs`. Like the HDFS scripts, the start script refuses to format a non-empty store, to replace a foreign process or to start on an occupied port, and stops only the PID it recorded. Automatic topic creation is off; the publisher creates replay topics explicitly with unlimited retention (`retention.ms=-1`), because the retained replay is the input that batch reconciliation and restarts rely on. The Kafka command-line tools need `JAVA_HOME`; in bash, `source deploy/native/env.sh` sets it.

Kafka 4.1.2 was chosen because `dlcdn.apache.org` now serves only 4.x releases and `archive.apache.org` was unreachable during setup; the installer tries both. Spark's Kafka connector (client 3.4.1) reads it unchanged, as the integration run below shows. The configuration has no authentication, encryption or replication: it is a local profile, not the deployment profile.

### Replay files, publisher and diagnostic consumer

```bash
# 1. Replay JSONL files from a prepared canonical Parquet (event-time windows, seeded retry copies)
PYTHONPATH=src python scripts/data_storage.py replay-files --canonical <prepared>.parquet --run-id <id> --duplicates 500
# 2. Publish and immediately read back (default input: the committed fixture)
PYTHONPATH=src python scripts/data_storage.py kafka-publish --input data/replay/<id> --topic <topic>
# 3. Read back again later, e.g. after a broker restart
PYTHONPATH=src python scripts/data_storage.py kafka-verify --report data/storage/kafka-<stamp>/publish.json
```

- **Publisher** (`kafka_replay.publish`): streams the replay files line by line. A directory contributes every `*.jsonl` file and every Spark text part (`bucket=<time>/part-*`, as written by `engine_experiments.py replay-spark`) below it, ordered by relative path, so buckets are sent in time order; `_SUCCESS`, `.crc` and hidden files are skipped. It validates each message against the replay contract, and sends it keyed by `mmsi`. The producer is idempotent with `acks=all`, so a retried send cannot duplicate or reorder a partition's messages, and uses the `murmur2_random` partitioner: Java clients such as Spark map a key to the same partition. Every delivery report is counted; the report records sent, delivered and failed messages, each partition's first/last offset, the replay accounting, and a digest of the sorted replay-ID multiset. A topic that already holds messages is refused unless `--append` is given, so two runs are not mixed by accident.
- **Diagnostic consumer** (`kafka_replay.verify`): a fresh consumer group that commits nothing reads exactly the recorded offset ranges. It checks that the partitions still retain them, revalidates every message, counts messages from other runs, wrong keys and keys seen on two partitions, and compares message count, accounting and replay-ID digest with the publish report. Kafka keeps each partition in send order, so the number of per-vessel event-time regressions must be the same in what was received as in the published input (lines inside a Spark bucket are not sorted by event time); a difference means the order changed. Any difference makes the result `FAIL`.
- **Accounting at scale:** `ReplayTally` keeps 8-byte fingerprints (the first 64 bits of each SHA-256 identity) in arrays instead of whole messages, so distinct counts for a day stay within tens of megabytes. `account` uses it and returns the same fields as before; the committed fixture manifest is unchanged.

`replay-files` still loads one prepared file into memory (about 240,000 messages for the two-port day). Producing an all-port day without loading it is a later item.

Eight unit checks run the publisher and consumer against an in-memory broker: a complete round trip, a lost message, a tampered message, offsets removed by retention, delivery failures, an invalid input line reported with its file and line number, directory ordering, and tally/digest order independence. A ninth check runs against a real broker when `HARBORTRACE_KAFKA_BOOTSTRAP` is set.

### Observed results, 15 November 2026

Single local broker (Kafka 4.1.2, OpenJDK 17.0.21, macOS 26.5, Apple silicon), four partitions, `confluent-kafka` 2.6.1:

| Input | Sent / delivered / received | Retry copies | Replay-ID digest | Result |
|---|---|---|---|---|
| Committed fixture (synthetic) | 8 / 8 / 8 | 1 | equal | PASS |
| Real 1 January 2024, two ports, 48 half-hour files | 236,213 / 236,213 / 236,213 | 500 | equal | PASS |
| Same report after stopping and restarting the broker | 236,213 retained and read | 500 | equal | PASS |

The real publish took 3.4 s of sending (about 69,000 messages/s) and 6.1 s for the whole command including read-back, with a peak resident memory of 295 MB. Partitions received 54,961 / 67,039 / 49,315 / 64,898 messages; no key appeared on two partitions, no vessel's event time went backwards, and there were no delivery failures, invalid, foreign or wrongly keyed messages. The real-broker unit check also passed.

**Integration with the processing engine.** The streaming engine read the same topic (`engine_stream.py --source kafka:<topic> --max-per-trigger 5000`) in 49 microbatches: 235,713 accepted, 500 duplicates, 0 late or invalid, 370 provisional visits. Compared with the batch artifact of the same canonical file (370 visits; 271 reconciled, 99 candidate): 235 identical, 121 where batch includes observations after the final watermark, 3 exits confirmed after it, 11 differing only in end flag or gap, **0 unexplained**. These are the figures the engine guide reports for its own Kafka run.

**Source snapshot finding.** The AIS file downloaded on 15 November matches the pinned SHA-256, but the freshly downloaded USACE boundary GeoJSON does not (`81d652e9…` instead of `3419b3ba…`), although it reports the same 370 features, the same provider edit date (July 2025) and the same 8.89% repair of port 4120. `week05_ingestion.py --download-pilot` therefore refuses it, as designed. Preparing the day with it reproduces every pinned count (7,293,408 source rows; 235,713 canonical; 7 duplicates; 7,057,688 outside the region), both rows of `tests/fixtures/real_ais_excerpt.jsonl` field for field, and the 370 batch visits above. The difference is most likely serialization by the provider service; the pinned hash was not changed and needs a joint review.

**Spark bucket input, 23 November.** The 1 January two-port canonical data was written as 48 `bucket=*/part-*.txt` directories by `engine_experiments.py replay-spark` from `main` and published with `kafka-publish --input <directory>`: 235,713 sent, delivered and received, equal digest, and 91,829 per-vessel event-time regressions both in the input and in the topic (lines inside each bucket are unsorted). PASS.

### Limitations

- One broker without replication, authentication or encryption. Broker loss, replica recovery and access control belong to the deployment weeks.
- Only the two-port day was replayed. The all-port day (about 7.3 million messages) needs bounded-memory replay generation first.
- The consumer verifies retained offsets and identities; it does not measure end-to-end latency or a live-rate source.

## Week of 19 November: Spark preparation and day-partitioned HDFS storage

### Port assignment without the per-row loop

`spatial.assign` tests one point against every port polygon. With every usable polygon of the USACE layer that is 368 tests per position. `spatial.PortIndex` assigns a whole batch: an STRtree gives bounding-box candidates, then `shapely.covers` with prepared polygons and a vectorized boundary distance give membership and flags. It returns exactly what `assign` returns point by point: the same GEOS predicate and distance, and the same scalar latitude scaling for the near-boundary flag. Passing the predicate to `STRtree.query` was tried first; it prepares the points rather than the polygons and was slower than the scalar loop for the two large Los Angeles/Long Beach polygons.

`spatial.load_usable_ports` loads every polygon that passes the existing repair rule and lists the rejected ones: on the current layer 368 are usable and 2 are rejected (2331 and 2373, repair changes the area by more than 15%). A symmetric self-intersecting polygon has zero signed area; it used to raise `ZeroDivisionError` and is now rejected with a reason.

| Check (real layer, 1 January points) | Points | Result | Vectorized | Per-point `assign` |
|---|---:|---|---:|---:|
| Two ports | 20,060 | identical | 0.20 s | 0.47 s |
| All 368 usable ports | 21,141 | identical | 0.20 s | 33.9 s |

### Spark preparation

```bash
PYTHONPATH=src python scripts/data_storage.py spark-prepare                       # synthetic source, compared with ingest.prepare
PYTHONPATH=src python scripts/data_storage.py spark-prepare --source data/source/ais-2024-01-*.parquet \
    --ports 4120 4110 --output hdfs://127.0.0.1:9000/harbortrace/canonical/<name> --replication 2
```

`spark_prepare.prepare_spark` applies the rules of `ingest.prepare` with the same identities. Spark reads the source files with their name and row index (`_metadata.file_name`, `_metadata.row_index`, which equal the PyArrow row ordinal), passes every row through `quality.canonicalize` inside `mapInArrow` batches, assigns ports with `PortIndex` (named ports, or `--ports all`), and resolves same-vessel/same-time groups with window functions: identical positions keep their first `(source_file, source_row)`, differing positions reject the whole group. The file is part of the comparison because row indexes repeat across files; with the row index alone, the same observation at the same row of two files was kept twice (fixed 23 November, covered by a unit test). The output is

```text
<output>/day=YYYY-MM-DD/part-*.parquet   canonical observations, sorted by mmsi and event time
<output>/_quarantine/part-*.json         rejected rows: source file and row, reason, kept file and row for duplicates
<output>/_manifest.json                  sources with SHA-256, boundary hash, ports used/rejected, row accounting, rows per day
```

Spark and the processing engine skip the `_` paths, so the directory reads as canonical Parquet (the engine selects the contract columns and ignores `day`). The directory is written under a hidden partial name and renamed only after the manifest exists; an existing output is never overwritten. `compare_with_reference` compares an output row for row with an `ingest.prepare` canonical file and its quarantine. For an `hdfs://` output, `--replication` sets the replication factor (default: the cluster's `dfs.replication`) and the command adds an `hdfs fsck` block report.

**Defect found and fixed: memory.** The first fifteen-day all-port run failed after 6.7 hours with `OutOfMemoryError` in Spark's `MemoryStore`: `persist()` held the 106 million prepared rows and the window result as objects in an 8 GB local heap. One day had fitted. The prepared rows and the duplicate/conflict resolution are now written to a staging directory beside the output (`.<name>.staging`) and read back, so later passes stream from files and sorts spill to disk; the staging directory is removed before publication. The rerun took 21 minutes.

**Defect found and fixed: replication.** The first HDFS run with `--replication 2` produced files with replication 3. Spark's Hadoop client defaults to 3 regardless of the cluster setting, and a `FileSystem` cached before the setting keeps the old value. The setting is now applied before the output filesystem is opened, every written file gets `setReplication` before publication, and the command waits until the NameNode reports no under-replicated block.

### Two local DataNodes and block reports

`HARBORTRACE_DATANODES=2 bash deploy/native/hdfs-start.sh` starts a second DataNode on the same host with its own configuration, storage directory, ports (+10) and Hadoop PID directory; `hdfs-stop.sh` stops every recorded DataNode. `hdfs.block_report` parses `hdfs fsck <path> -files -blocks -locations` into health status, blocks, live replicas, replicas per DataNode and under-replicated/missing/corrupt counts. Both DataNodes share one disk and one machine: this exercises replication and placement, not protection against losing a machine.

### Observed results, 16 November 2026

macOS 26.5 (Apple silicon, 10 cores, 32 GB), Spark 3.5.7 `local[*]`, Hadoop 3.5.0 with two local DataNodes. Real NOAA AIS days of January 2024.

| Run | Source rows | Canonical rows | Rejected / outside | Time | Check |
|---|---:|---:|---|---:|---|
| 1 January, two ports, local | 7,293,408 | 235,713 | 7 duplicates / 7,057,688 | 26.5 s (`local[10]`) | **Equal** to `ingest.prepare` row for row, quarantine and accounting (about 268 s single-process) |
| 1 January, two ports, HDFS, replication 2 | 7,293,408 | 235,713 | same | | Equal; 3 blocks, each on both DataNodes, HEALTHY |
| 1 January, all 368 usable ports, local | 7,293,408 | 7,270,776 | 215 duplicates, 86 conflicts / 22,331 | 122 s | 20,000 sampled rows: assignment equal to `assign` |
| 1–15 January, two ports, HDFS, replication 2 | 106,206,861 | 3,499,987 | 231 duplicates, 126 conflicts / 102,706,517 | 290 s | 15 day partitions; 17 blocks, each on both DataNodes, HEALTHY |
| 1–15 January, all 368 usable ports, HDFS, replication 2 | 106,206,861 | 105,953,905 | 3,036 duplicates, 530 conflicts / 249,390 | 1,273 s | 15 day partitions of 5.7–7.4 million rows; 15.6 GB (31.3 GB with replicas); 135 blocks, each on both DataNodes, HEALTHY |

The fifteen-day all-port preparation was repeated on 23 November after the duplicate fix above, to a local directory (1,246 s): every count is identical, so these daily files hold no observation at the same row of two files. All-port day 1: 14,733 vessels, 204 ports with observations, 62% of canonical positions inside a port polygon, 1.1 GB of Parquet. An earlier unpublished all-port experiment reported 7,293,107 canonical rows; it differed only by not applying the region filter (7,293,408 − 7,293,107 = 301 = 215 duplicates + 86 conflicts). This preparation keeps the reference rule, a 0.35° margin around the selected polygons, which drops 22,331 positions far from every port.

**Engine on HDFS.** `engine_batch.py` read the fourteen partitions `day=2024-01-02` … `day=2024-01-15` straight from HDFS: 3,264,274 observations, the same input size as the engine guide's core run, and 5,081 visits (3,869 reconciled, 1,212 candidate), the counts that guide reports. The `visits.jsonl` SHA-256 (`53dc5618…`) is identical when the same partitions are copied out of HDFS and read locally, and both runs took about 231 s on `local[4]`, so reading from HDFS did not change the result or the time.

**Early replica-loss check.** With the fifteen-day two-port dataset at replication 2, the second DataNode was killed (`kill -9`). Spark still read all 3,499,987 rows with 3,499,987 distinct identities from the remaining replicas. The NameNode marked the node dead after 10 min 36 s (its default heartbeat expiry). After restarting the node, both DataNodes were live again within seconds, every block had two replicas and `fsck` reported HEALTHY. **Unexplained:** while the node was marked dead, `fsck` still counted its replicas as live and reported no under-replicated block. This has to be understood before the replica-loss tests of the deployment weeks; two DataNodes on one host may be the cause.

### Limitations

- Two DataNodes on one laptop share the disk and machine; real placement across workers, rack awareness and machine loss need the cloud deployment.
- The single-process reference cannot run all 368 ports on a whole day in reasonable time (about 34 s per 21,000 points), so the all-port output is checked by sampled assignments and accounting, not row for row.
- `replay-files` still loads one day into memory; all-port replay generation is the remaining bounded-memory item.
- The all-port scope is a team decision that has not been taken; these runs measure its cost.

Development used Claude Code assistance; the owner reviewed and ran every command above.
