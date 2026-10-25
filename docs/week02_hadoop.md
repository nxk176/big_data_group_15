# Week 2: Hadoop ecosystem and HDFS (Hadoop Distributed File System)

The [week-one proposal](week01_proposal.md) sets HarborTrace's data and target architecture. Hadoop (an ecosystem of distributed-data tools) includes HDFS. A NameNode (metadata manager) tracks the namespace (file directory); DataNodes (storage processes) hold blocks (file pieces). Later milestones retain AIS (Automatic Identification System) source data, canonical (normalized) observations, and boundaries in HDFS. Spark (a distributed processing engine) can read those files for batch (fixed-input) work.

## Local setup

Tested on Ubuntu 24.04 under WSL2 (Windows Subsystem for Linux, version 2) with Java 17. The project stays at `E:\big_data` (`/mnt/e/big_data` in WSL). `hdfs-install.sh` verifies the archive checksum (a file-integrity fingerprint) and installs Hadoop 3.5.0 under the WSL user's home. Service data, PID (process identifier) files, and logs are under the project root and ignored by Git (source version control).

```bash
cd /mnt/e/big_data
bash deploy/native/hdfs-install.sh
bash deploy/native/hdfs-start.sh
bash deploy/native/hdfs-smoke.sh
```

For a **new, empty project store only**, use `bash deploy/native/hdfs-start.sh --init-empty` on the first start. The script refuses to format a nonempty NameNode directory, or an existing DataNode directory without a recognized NameNode. Do not apply it to an unrelated HDFS installation. Normal restarts do not format storage. The script starts only the project's NameNode and DataNode; Kafka and MongoDB are outside this milestone.

Configuration is in `deploy/native/core-site.xml` and `deploy/native/hdfs-site.xml`. The NameNode RPC (remote procedure call) endpoint is `127.0.0.1:9000`, its web UI (user interface) is `127.0.0.1:9870`, and all bindings are local. `dfs.replication=1` matches the one-DataNode laptop profile. Permissions are disabled only for this isolated demonstration. These settings are **not** a secured or fault-tolerant deployment profile.

## Check performed on 2026-10-11

The smoke script (a small functional check) checks `hdfs dfsadmin -report` for a live DataNode, waits at most 40 seconds for Safe Mode (the NameNode's startup protection state) to end, writes `tests/fixtures/hdfs_week02.txt`, reads it back with `hdfs dfs -cat`, and compares SHA-256 (Secure Hash Algorithm with a 256-bit output) and byte count. A second run checks the existing file rather than overwriting it. It does not upload AIS data.

Observed output from the successful run:

```text
HDFS smoke passed: live_datanodes=1 bytes=110 sha256=e06fb5d61ce57e0a6889afc5ff74ad478835d0bab292191e7ef0a221605d440d path=hdfs://127.0.0.1:9000/harbortrace/week02/hdfs_week02.txt
```

The first smoke attempt occurred immediately after HDFS startup and failed while the NameNode was in Safe Mode. The bounded wait above resolved that startup race. A live process alone was insufficient evidence; the final check proved the HDFS client could write and read the fixture. It does not prove replication, multi-node placement, cloud execution, or recovery from DataNode loss. Those require later milestones and actual worker machines.

The fuller prototype existed locally before this repository's first commit. This weekly commit records and exposes this checked storage slice; commit dates should not be interpreted as the original development dates of every local component.
