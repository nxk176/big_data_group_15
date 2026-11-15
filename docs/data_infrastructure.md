# Data and infrastructure component

Owner: Do Dang Vu — 20235578. This component prepares source data, publishes canonical observations (normalized position records) and replay messages, stores them in HDFS (Hadoop Distributed File System) and runs the supporting services. It produces the inputs for the processing engine; it does not reconstruct visits or write MongoDB.

| Part | Files |
|---|---|
| Source preparation, quality, spatial assignment | `src/harbortrace/ingest.py`, `quality.py`, `spatial.py` (inherited week-five prototype) |
| Replay identity and accounting | `src/harbortrace/replay.py` |
| Kafka replay publisher and diagnostic consumer | `src/harbortrace/kafka_replay.py` |
| HDFS publication and readback | `src/harbortrace/hdfs.py` |
| Native HDFS and Kafka service scripts | `deploy/native/` |
| Entry script | `scripts/data_storage.py` |
| Boundary fixtures | `tests/fixtures/canonical/` |
| Checks | `tests/unit/test_replay.py`, `tests/unit/test_hdfs.py`, `tests/unit/test_kafka_replay.py` |

Install with `python -m pip install -r requirements-data.txt` (the week-five preparation stack plus numpy and the `confluent-kafka` client).

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

- **Publisher** (`kafka_replay.publish`): streams the JSONL files line by line (sorted by name for a directory), validates each message against the replay contract, and sends it keyed by `mmsi`. The producer is idempotent with `acks=all`, so a retried send cannot duplicate or reorder a partition's messages, and uses the `murmur2_random` partitioner: Java clients such as Spark map a key to the same partition. Every delivery report is counted; the report records sent, delivered and failed messages, each partition's first/last offset, the replay accounting, and a digest of the sorted replay-ID multiset. A topic that already holds messages is refused unless `--append` is given, so two runs are not mixed by accident.
- **Diagnostic consumer** (`kafka_replay.verify`): a fresh consumer group that commits nothing reads exactly the recorded offset ranges. It checks that the partitions still retain them, revalidates every message, counts messages from other runs, wrong keys, keys seen on two partitions and per-vessel event-time regressions, and compares message count, accounting and replay-ID digest with the publish report. Any difference makes the result `FAIL`.
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

### Limitations

- One broker without replication, authentication or encryption. Broker loss, replica recovery and access control belong to the deployment weeks.
- Only the two-port day was replayed. The all-port day (about 7.3 million messages) needs bounded-memory replay generation first.
- The consumer verifies retained offsets and identities; it does not measure end-to-end latency or a live-rate source.

Development used Claude Code assistance; the owner reviewed and ran every command above.
