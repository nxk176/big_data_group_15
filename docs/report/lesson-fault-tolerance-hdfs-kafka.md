# Lesson: Fault Tolerance (HDFS and Kafka)

Author: Do Dang Vu. Serving-side fault tolerance (MongoDB generations, backup and restore) is in the serving component's lesson. Evidence: [k3s deployment](../deploy_k3s.md#losing-a-datanode), [data and infrastructure guide](../data_infrastructure.md).

## Problem Description

The storage and transport layer must survive the failures a small cluster actually sees, without losing or silently duplicating data:

- a DataNode (disk or process) disappears while data is being read;
- the Kafka broker stops and starts again;
- a producer retries a send whose acknowledgement was lost;
- a preparation job crashes halfway through writing;
- a message is lost, altered or reordered somewhere between producer and consumer.

For each case we wanted evidence, not assumptions: a test that injects the failure and a measurement of what was kept.

## Approaches Tried

| Approach | What we learned |
|---|---|
| One DataNode, replication 1 (week 2) | Any DataNode loss makes data unreadable; acceptable only for a smoke test |
| Two DataNodes, `kill -9` of one, then `fsck` once the NameNode reported it dead (16 November) | Reads continued, but `fsck` still listed the dead node's replicas as live, which looked like a monitoring bug |
| The same test on Linux with timestamps from the NameNode log (23 November) | The node is reported dead when its heartbeat expires, but its replicas are removed only at the next periodic heartbeat check; the first `fsck` had run in between |
| Trusting Kafka's delivery acknowledgements | Proves that the broker accepted a message, not that a consumer can read back exactly what was sent |
| Caching a 106-million-row job in memory | It crashed after 6.7 hours (`OutOfMemoryError`): the work was lost, and only the hidden partial name kept the half-written output from looking like a result |

## Final Solution

**HDFS replication 2, tested by DataNode loss.** Canonical data is written with two replicas on two DataNodes and checked with `fsck`. The test on the k3s node (1 January canonical data, 3 blocks):

| Moment | Live DataNodes | Replicas per block | Under-replicated | Missing |
|---|---:|---:|---:|---:|
| before | 2 | 2 | 0 | 0 |
| `kill -9` of DataNode 2; Spark reads 235,713 of 235,713 rows | 2 (not yet detected) | 2 (stale) | 0 | 0 |
| NameNode removed the node, about 15 min later | 1 | 1 | 3 (100 %) | 0 |
| DataNode 2 restarted | 2 | 2 | 0 | 0 |

No block was lost and reads never stopped. An earlier run on fifteen two-port days read all 3,499,987 rows with one DataNode killed. With only two DataNodes, missing replicas cannot be recreated elsewhere until the node returns; a third DataNode would let the NameNode re-replicate on its own.

**Kafka: no loss, no duplicates, provable.**

- The producer is idempotent with `acks=all`: a retried send cannot create a second copy or reorder a partition.
- Replay topics have unlimited retention; after a broker stop and start all 236,213 messages were still retained.
- The diagnostic consumer reads back exactly the offset ranges the producer recorded and compares the count, the accounting and a SHA-256 digest of the sorted replay-ID multiset. Unit tests inject a lost message, a tampered message, a reordered partition, offsets removed by retention and failed deliveries; each is reported as `FAIL`. On real data: 236,213 sent = delivered = received, digest equal, also on Kafka in k3s.
- Business duplicates are separated from transport duplicates: 500 seeded retry copies carry new `replay_id`s but the same observation identity, and the streaming engine counted exactly 500 duplicates and produced the same 370 visits as batch.

**Crash safety of writes.** Every dataset is built under a hidden `.partial` name with a `.staging` directory beside it and renamed only after its manifest exists; leftovers of a crashed run are removed at the next start; existing outputs are never overwritten. A crash can waste work but cannot publish half a dataset.

## Key Takeaways

- Fault tolerance is a measured property: inject the failure, then count what survived. The replica test turned "replication 2 is configured" into "235,713 of 235,713 rows readable with a DataNode killed".
- Know the detection delay of your failure detector. HDFS took about 15 minutes to act on a silent DataNode with default settings; monitoring in between shows a stale picture.
- Acknowledgements are not end-to-end proof. Comparing what was sent with what can be read back (counts plus an identity digest) is cheap and catches loss, duplication and tampering.
- Make every step idempotent or atomic, so retries and restarts repair rather than corrupt.
- Not covered: Kafka broker replication (one broker), NameNode high availability, and loss of the whole machine. Both DataNodes share one host, so a disk or host failure is still a single point of failure until worker machines are used.
