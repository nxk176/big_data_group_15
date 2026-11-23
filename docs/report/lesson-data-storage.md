# Lesson: Data Storage

Author: Do Dang Vu. Evidence: [data and infrastructure guide](../data_infrastructure.md), [k3s deployment](../deploy_k3s.md), [services quickstart](../services_quickstart.md).

## Problem Description

The pipeline stores data with very different access patterns:

| Data | Volume | Access |
|---|---|---|
| Source snapshots (NOAA AIS, USACE ports) | 227 MB per day, 3.2 GB for 15 days | written once, read by preparation, must never change silently |
| Canonical observations | 15.6 GB for 15 days, all ports | written once per preparation, scanned by Spark batch jobs, often for a date range |
| Replay messages | about 640 bytes per message as JSON | appended in event-time order, read sequentially by streaming, re-read after failures |
| Serving data (visits, aggregates) | thousands of documents | queried by the dashboard, replaced by generation |

Storage therefore has to be distributed and replicated for the large files, partitioned for selective scans, immutable for provenance, retained for replay, and queryable for serving, all within one laptop now and a cluster later.

## Approaches Tried

| Approach | Outcome |
|---|---|
| Local files only (weeks 4–5) | Simple and reproducible, but no distribution, no replication and no shared access from a cluster |
| HDFS with one DataNode and replication 1 (week 2) | Write/readback worked; no protection against any disk or process failure |
| Copying files with `hdfs dfs -put` and trusting the result | The first macOS smoke test reported a checksum mismatch: Hadoop's own warning was printed to stdout and mixed into `hdfs dfs -cat`. Every publication now reads the bytes back and compares SHA-256 |
| One Parquet file per preparation | Fine for one day; a date-range query must read everything |
| Spark writing to HDFS with `--replication 2` | Files were created with replication 3 (Spark's client default; a cached `FileSystem` ignored the setting); `fsck` reported every block under-replicated |
| JSON Lines for canonical data | Readable, but the two-port day is 151 MB as replay JSON against 34 MB as Snappy Parquet |

## Final Solution

- **Formats and compression.** Canonical data is Parquet with Snappy compression (about 150 bytes per observation: 35,162,083 bytes for 235,713 rows); replay is JSON Lines, the contract format Kafka carries, compressed with LZ4 on the wire. Sources stay in the provider's GeoParquet.
- **Partitioning.** `spark-prepare` writes `day=YYYY-MM-DD` partitions (5.7–7.4 million rows each for all ports). The batch engine read the fourteen partitions `day=2024-01-02` … `day=2024-01-15` straight from HDFS: 3,264,274 observations, 5,081 visits, with the same `visits.jsonl` SHA-256 as from a local copy and the same time (about 231 s), so HDFS changed neither the result nor the speed.
- **Replication.** HDFS runs a NameNode and two DataNodes (`HARBORTRACE_DATANODES=2`). Files are written with the requested factor, `setReplication` is applied to every written file, and the job waits until `fsck` (or the FileSystem API inside a Spark pod) reports no under-replicated block. Fifteen all-port days: 135 blocks, each on both DataNodes, 31.3 GB with replicas.
- **Immutability and provenance.** Datasets are written under a hidden partial name and published by rename after their manifest exists; an existing path is never overwritten. HDFS publication is content-addressed (`<name>-<first 16 hex of SHA-256>`), so an identical file is reused after verification and a different file at the same path is refused.
- **Retention.** Replay topics are created explicitly with `retention.ms=-1`: the retained replay is the input that batch reconciliation and streaming restarts rely on. After a broker stop and start, all 236,213 messages were still there and verified by offsets and digest.
- **Hot, warm and cold data.** Hot: the active MongoDB generation behind the dashboard and the current Kafka topic. Warm: canonical day partitions in HDFS, scanned by Spark. Cold: pinned source snapshots with SHA-256 manifests, kept for reproduction.
- **Serving storage** (owned by the serving component): MongoDB 7 on k3s with authentication; batch results arrive as immutable generations and one is active at a time.

## Key Takeaways

- Verify storage by reading back, not by exit codes: a byte-for-byte SHA-256 comparison found a real defect on day one.
- Replication must be checked where it is applied: a configured factor is not the factor the files received. `fsck` (or block locations through the API) is the evidence.
- Partition by the key the readers filter on; day partitions let the engine select fourteen of fifteen days without reading the rest.
- Columnar compression matters: Parquet stored the same observations in about a quarter of the JSON size.
- Treat stored data as immutable and content-addressed; reruns become cheap and accidental overwrites impossible.
- Limits: both DataNodes share one disk and machine; MongoDB and Kafka run one replica each; there is no tiering to cheaper storage, only the hot/warm/cold separation above.
