# Week 2: Hadoop ecosystem and a local HDFS round trip

The [week-one proposal](week01_proposal.md) sets HarborTrace's data and target architecture. This milestone verifies the first storage component on the same laptop. Hadoop HDFS separates namespace metadata (NameNode) from file blocks (DataNode). HarborTrace stores its immutable AIS source, selected canonical observations, and boundary snapshot in HDFS in later milestones. Spark can read those HDFS paths for batch work.

## Local setup

Tested on Ubuntu 24.04 under WSL2 with Java 17. The project stays at `E:\big_data` (`/mnt/e/big_data` in WSL). `hdfs-install.sh` checksum-verifies the Apache Hadoop 3.5.0 archive and installs runtime binaries under the WSL user's home. NameNode/DataNode data, PID files, and logs are under `E:\big_data\data` and are ignored by Git.

```bash
cd /mnt/e/big_data
bash deploy/native/hdfs-install.sh
bash deploy/native/hdfs-start.sh
bash deploy/native/hdfs-smoke.sh
```

For a **new, empty project store only**, use `bash deploy/native/hdfs-start.sh --init-empty` on the first start. The script refuses to format a nonempty NameNode directory, or an existing DataNode directory without a recognized NameNode. Do not apply it to an unrelated HDFS installation. Normal restarts do not format storage. The script starts only the project's NameNode and DataNode; Kafka and MongoDB are outside this milestone.

Configuration is in `deploy/native/core-site.xml` and `deploy/native/hdfs-site.xml`. The NameNode RPC endpoint is `127.0.0.1:9000`, its web UI is `127.0.0.1:9870`, and all bindings are local. `dfs.replication=1` matches the one-DataNode laptop profile. Permissions are disabled only for this isolated local demonstration. These settings are **not** a secured or fault-tolerant deployment profile.

## Check performed on 2026-10-11

The smoke script checks `hdfs dfsadmin -report` for at least one live DataNode, waits at most 40 seconds for NameNode Safe Mode to end, writes the small `tests/fixtures/hdfs_week02.txt` to `/harbortrace/week02/hdfs_week02.txt`, reads it back with `hdfs dfs -cat`, and compares SHA-256 and byte count. A second run checks the already-present file rather than overwriting it. It does not upload AIS data.

Observed output from the successful run:

```text
HDFS smoke passed: live_datanodes=1 bytes=110 sha256=e06fb5d61ce57e0a6889afc5ff74ad478835d0bab292191e7ef0a221605d440d path=hdfs://127.0.0.1:9000/harbortrace/week02/hdfs_week02.txt
```

The first smoke attempt occurred immediately after HDFS startup and failed while the NameNode was in Safe Mode. The bounded wait above resolved that startup race. A live process alone was insufficient evidence; the final check proved the HDFS client could write and read the fixture. It does not prove replication, multi-node placement, cloud execution, or recovery from DataNode loss. Those require later milestones and actual worker machines.

The fuller prototype existed locally before this repository's first commit. This weekly commit records and exposes this checked storage slice; commit dates should not be interpreted as the original development dates of every local component.
