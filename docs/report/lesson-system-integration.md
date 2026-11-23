# Lesson: System Integration

Author: Do Dang Vu. Evidence: [k3s deployment](../deploy_k3s.md), [data and infrastructure guide](../data_infrastructure.md).

## Problem Description

HarborTrace joins four separately developed components on different runtimes: preparation (Python and Spark), the processing engine (Spark batch and Structured Streaming), transport and storage (Kafka and HDFS) and serving (MongoDB and a dashboard). They had to work together first on laptops and then on one Kubernetes (k3s) node, where every Spark job runs as pods. The integration questions were:

- **Service discovery:** how does each process find MongoDB, Kafka, HDFS and the Spark driver, both from pods and from programs on the node?
- **Error handling:** what happens when a message, a file or a service is wrong or missing?
- **Circuit breaking:** how does a caller stop waiting on a failing dependency?
- **Load balancing:** how is work spread over partitions, executors and replicas?

## Approaches Tried

Bringing the components together (on the laptop between 15 and 23 November, then as Spark pods on k3s) failed at nine boundaries, each fixed and recorded:

| Failure | Cause | Fix |
|---|---|---|
| `spark-submit` rejected the kubeconfig | Spark 3.5's Kubernetes client cannot read k3s's EC keys without BouncyCastle | submit with a two-hour token of the namespace's `spark` service account (also least privilege) |
| Local paths became HDFS paths | `spark-submit` ships `HADOOP_CONF_DIR` (`fs.defaultFS = hdfs://`) into the pods | do not ship it; name HDFS with `hdfs://` URIs |
| Executors stayed Pending | with `hostNetwork`, executors asked for the driver's port 7079 | fixed driver ports, random executor ports |
| `FetchFailedException` | executors sharing the node address read each other's shuffle files as "local" | `spark.shuffle.readHostLocalDisk=false` |
| `harbortrace.zip` conflicts, then access denied | executors downloaded files into the checkout, then into a directory the user cannot write | executors work in `/tmp` |
| `FileNotFoundError: git` | the Spark image has no git | the launcher passes the code revision |
| `Failed to find data source: kafka` | a connector added when the session starts is too late in cluster mode | `--packages` at submission |
| `JAVA_HOME` not found | the scripts assumed an amd64 Java path | detect Java 17 for any architecture |
| `NOT_LEADER_FOR_PARTITION` right after topic creation | the consumer's metadata predated the topic | wait for leaders; retry transient metadata errors |

## Final Solution

**Service discovery.** Inside the cluster, Kubernetes DNS names: `mongodb.harbortrace.svc.cluster.local:27017`, `kafka.harbortrace.svc.cluster.local:9092`, and a headless service per Spark driver that executors use. For programs on the node, fixed node ports (`127.0.0.1:30017` MongoDB, `:30092` Kafka, `:30050` dashboard). Kafka advertises two listeners, because a client connects to whatever address the broker advertises: `INTERNAL` for pods and `EXTERNAL` for the node. Spark pods use `hostNetwork` with `ClusterFirstWithHostNet`, so they reach the native HDFS at `127.0.0.1:9000` and still resolve cluster names. Credentials reach pods through a Secret; humans load them from `data/k3s/credentials.env`.

**Error handling.** Every boundary validates and reports instead of guessing:

- replay messages are checked against the contract (fields, version, identity); the diagnostic consumer reports lost, tampered, foreign, reordered or no-longer-retained messages as `FAIL` with reasons;
- datasets and artifacts are published by rename only when complete, and existing outputs are never overwritten, so a crash leaves nothing that looks valid;
- retries with backoff where failures are transient: HTTP downloads (5 retries, resumable), Kafka sends (idempotent producer, so a retry cannot duplicate), Kafka metadata after topic creation; streaming updates carry receipts, so reapplying 49 batches applied 0 updates twice.

**Circuit breaking.** There is no circuit-breaker library. Its two effects are obtained by other means: callers fail fast with bounded timeouts (MongoDB server selection 5 s, Kafka verification without progress 60 s, HDFS safe-mode wait 40 s), and Kubernetes readiness probes (`/healthz` for the dashboard, `ping` for MongoDB, a TCP check for Kafka) take a failing pod out of its service, so traffic stops reaching it until it recovers. A real breaker that stops retrying a dependency for a cool-down period is future work.

**Load balancing.** Kafka spreads replay messages over 4 partitions by MMSI with the Java-compatible murmur2 partitioner, which keeps each vessel's order and its partition across producers: 54,961 / 67,039 / 49,315 / 64,898 messages (largest/smallest 1.36). Spark spreads tasks over two executor pods; the engine repartitions by MMSI. Kubernetes services would balance over replicas, but every service runs one replica here, so this is untested.

**Result.** On k3s, from an empty node: NOAA day → Spark preparation pods → HDFS (2 replicas) → engine batch pods (370 visits, same SHA-256 as locally) → MongoDB (writer user) → dashboard (reader user); replay → Kafka on k3s (236,213 sent = received) → streaming pods (370 provisional visits, 0 unexplained differences from batch) → 6,273 updates in MongoDB.

## Key Takeaways

- Most integration defects were environmental, not algorithmic: networks, paths, users, classpaths. Each was visible only when the components ran together, which is why the end-to-end run came days before the demo.
- Discovery is about the address a component *advertises*, not only where it listens (Kafka listeners, Spark driver service, HDFS DataNodes registering as `127.0.0.1`).
- Fail loudly with a reason at every boundary; an explicit `FAIL: received 7 of 8 delivered messages` is worth more than a silent partial result.
- Idempotent operations (replay IDs, idempotent producer, update receipts, immutable outputs) turn retries from a risk into a recovery mechanism.
- Limits: single node; no circuit-breaker implementation; load balancing across service replicas was not exercised.
