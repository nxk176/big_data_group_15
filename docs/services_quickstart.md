# HDFS and Kafka on a new machine

Native single-host setup of the storage and transport services: no Docker, no root, every file under the checkout or `~/.local/harbortrace`. It takes about ten minutes plus the downloads (Hadoop 554 MB, Kafka 127 MB). Design, measurements and limits are in the [data and infrastructure guide](data_infrastructure.md).

## 1. Prerequisites

| Item | Ubuntu 24.04 / WSL2 | macOS (Homebrew) |
|---|---|---|
| Java 17 | `sudo apt install openjdk-17-jdk-headless` | `brew install openjdk@17` |
| Python 3.12 | `sudo apt install python3.12-venv` | `brew install python@3.12` |
| Tools | `curl`, `tar` (installed by default) | same |

`JAVA_HOME` may stay unset: the scripts find Java 17 at `/usr/libexec/java_home`, Homebrew `openjdk@17` or `/usr/lib/jvm/java-17-openjdk-amd64`. Set it explicitly if Java 17 is elsewhere.

```bash
git clone https://github.com/nxk176/big_data_group_15.git
cd big_data_group_15
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-engine.txt -r requirements-serving.txt -r requirements-data.txt
```

## 2. HDFS

```bash
bash deploy/native/hdfs-install.sh                                 # Hadoop 3.5.0, SHA-512 checked
HARBORTRACE_DATANODES=2 bash deploy/native/hdfs-start.sh --init-empty
bash deploy/native/hdfs-smoke.sh
```

`--init-empty` formats a **new, empty** store and is needed only the first time; later starts omit it, and the script refuses to format a store that already holds data. `HARBORTRACE_DATANODES=2` adds a second DataNode on the same host so that replication 2 can be used; omit it for one DataNode. Expected:

```text
HDFS smoke passed: live_datanodes=2 bytes=110 sha256=e06fb5d61ce57e0a6889afc5ff74ad478835d0bab292191e7ef0a221605d440d path=hdfs://127.0.0.1:9000/harbortrace/week02/hdfs_week02.txt
```

Write canonical data to HDFS and check replication and block placement:

```bash
PYTHONPATH=src python scripts/data_storage.py spark-prepare \
    --output hdfs://127.0.0.1:9000/harbortrace/canonical/fixture --replication 2   # synthetic source
source deploy/native/env.sh   # bash: puts Hadoop's JAVA_HOME/HADOOP_CONF_DIR in this shell
$HADOOP_HOME/bin/hdfs fsck /harbortrace/canonical/fixture -files -blocks -locations
```

## 3. Kafka

```bash
bash deploy/native/kafka-install.sh                    # Kafka 4.1.2 (KRaft, no ZooKeeper), SHA-512 checked
bash deploy/native/kafka-start.sh --init-empty         # first start only, like HDFS
PYTHONPATH=src python scripts/data_storage.py kafka-publish --topic harbortrace.fixture
```

The last command creates the topic (4 partitions, unlimited retention), publishes the committed replay fixture keyed by `mmsi` and reads it back; it ends with `RESULT: PASS` and 8 messages sent and received. Publish a replay directory, including the engine's `bucket=*/part-*.txt` output, with `--input <directory> --topic <new topic>`. The streaming engine then reads `--source kafka:<topic> --kafka-bootstrap 127.0.0.1:9092`.

## 4. Stop and restart

```bash
bash deploy/native/kafka-stop.sh
bash deploy/native/hdfs-stop.sh
HARBORTRACE_DATANODES=2 bash deploy/native/hdfs-start.sh   # data is kept; no --init-empty
bash deploy/native/kafka-start.sh
```

The stop scripts end only the processes their start scripts recorded.

## Ports and files

| Service | Address | Data and logs (under `$HARBORTRACE_DATA_DIR`, default `<checkout>/data`) |
|---|---|---|
| HDFS NameNode | RPC `127.0.0.1:9000`, web `127.0.0.1:9870` | `hdfs/name`, `logs/native/namenode.log` |
| DataNode 1 | `127.0.0.1:9866`, `:9864`, `:9867` | `hdfs/datanode`, `logs/native/datanode.log` |
| DataNode 2 | `127.0.0.1:9876`, `:9874`, `:9877` | `hdfs/datanode2`, `hdfs/conf-datanode2`, `logs/native/datanode2.log` |
| Kafka broker / controller | `127.0.0.1:9092` / `:9093` | `kafka/logs`, `logs/kafka/kafka.out` |

Everything binds to `127.0.0.1` without authentication. This is a development profile, not a secured or multi-machine deployment.

## When something fails

| Symptom | Cause and fix |
|---|---|
| `No HarborTrace NameNode namespace` / `No HarborTrace Kafka metadata` | First start: add `--init-empty`. |
| `Port 9000 is occupied` (or 9092, 9866, …) | Another HDFS/Kafka is running. Stop it, or use the stop script if it is this project's. |
| `JAVA_HOME is not set and could not be found` | Java 17 is in a non-standard place: `export JAVA_HOME=<path>`. |
| `NameNode did not leave Safe Mode` | A DataNode did not start; read `logs/native/datanode*.log`. |
| Download fails | `archive.apache.org` is sometimes unreachable; the Kafka installer also tries `dlcdn.apache.org`. Rerun: partial downloads resume. |
| `kafka-publish` refuses a topic that holds messages | Use a new `--topic`, or `--append` to add to it deliberately. |

Checked on 23 November 2026 on macOS 26.5 with an empty `HARBORTRACE_DATA_DIR`: two DataNodes live, smoke passed, Kafka fixture publish PASS, both stop scripts clean.
