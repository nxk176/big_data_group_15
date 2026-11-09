# Data and infrastructure component

Owner: Do Dang Vu — 20235578. This component prepares source data, publishes canonical observations (normalized position records) and replay messages, stores them in HDFS (Hadoop Distributed File System) and runs the supporting services. It produces the inputs for the processing engine; it does not reconstruct visits or write MongoDB.

| Part | Files |
|---|---|
| Source preparation, quality, spatial assignment | `src/harbortrace/ingest.py`, `quality.py`, `spatial.py` (inherited week-five prototype) |
| Replay identity and accounting | `src/harbortrace/replay.py` |
| HDFS publication and readback | `src/harbortrace/hdfs.py` |
| Native HDFS service scripts | `deploy/native/` |
| Entry script | `scripts/data_storage.py` |
| Boundary fixtures | `tests/fixtures/canonical/` |
| Checks | `tests/unit/test_replay.py`, `tests/unit/test_hdfs.py` |

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
- Replay messages are files; publishing them to Kafka and consuming them back is the week-7 deliverable.
- Preparation is still single-process PyArrow/Shapely code, not a distributed Spark job.

Development used Claude Code assistance; the owner reviewed and ran every command above.
