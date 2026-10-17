# Week 3: Lambda architecture and capstone design

HarborTrace reconstructs **observed vessel presence** in the Los Angeles and Long Beach port areas from historical AIS positions. The [proposal](week01_proposal.md) defines the problem and source limitations; the [week-two HDFS check](week02_hadoop.md) verifies one local storage component. This milestone fixes the end-to-end design and the contracts that later implementation commits must meet.

## Data flow

```mermaid
flowchart LR
  N[NOAA historical AIS] --> I[Immutable source + manifest]
  U[USACE port polygons] --> I
  I --> C[Spark validation + canonical observations]
  C --> H[(HDFS Parquet)]
  H --> B[Spark batch: visits + hourly aggregates]
  C --> R[Seeded historical replay]
  R --> K[(Kafka)]
  K --> S[Spark Structured Streaming: provisional state]
  K --> A[Retained replay audit]
  A --> H
  B --> P[Validated immutable generation]
  S --> M[(MongoDB provisional results)]
  P --> M
  M --> D[Dashboard]
```

This is a **Lambda architecture**: a deterministic batch path is the authority for reconciled history, while a speed path shows provisional state during replay. A replay carries original event time and a separate ingestion time. It simulates delayed and duplicated arrivals from a historical source; it is not a live AIS receiver. Both paths use the same canonical observation and visit-rule versions. A changed or late observation is retained for investigation and a batch rebuild can correct the provisional result. The design does not claim that an event-time watermark can recover data already discarded from streaming state.

| Component | Role | Planned check |
|---|---|---|
| NOAA and USACE snapshots | Immutable, checksummed source and boundary provenance | Source manifest, boundary metadata, row accounting |
| HDFS and Parquet | Distributed storage interface for source, canonical, replay audit, batch output | Read/write check first; later Spark read and partition scan |
| Kafka | Bounded, seeded historical replay | Message IDs, duplicate and delay injection, retained count |
| Spark batch | Canonicalization, spatial join, deterministic visit reconstruction, aggregates | Reference fixtures, full input counts, repeatable output IDs |
| Spark Structured Streaming | Event-time provisional visits with bounded state and watermark | Late data, duplicate, restart, and state-limit scenarios |
| MongoDB | Versioned serving collections and active generation pointer | Count validation, reader consistency, failed-publication recovery |
| Dashboard | Clearly labelled reconciled and provisional views | Reads one complete generation and displays uncertainty |

## Authority and publication

1. Keep source bytes and original boundary geometry unchanged. A manifest records checksums, provider, extraction time, and rule/config versions.
2. Account for every source row as canonical, outside the regional selection, duplicate, conflicting, or invalid. Retain observations outside a port polygon but within the regional selection to establish possible exits.
3. Sort each vessel's observations by UTC event time and stable IDs before applying the [visit decision table](visit_decision_table.md). The batch result is reproducible for fixed source, boundary, config, and code versions.
4. The speed path labels output `provisional`. Its watermark and state limits are explicit; delayed events outside that limit require batch reconciliation.
5. Write a complete batch generation, validate counts, then switch one serving pointer. Readers pin that generation for a request. Retain old generations for audit until a separate cleanup decision.

## Course requirements and present boundary

The proposed pipeline covers ingestion, Spark processing, HDFS storage, Kafka messaging, MongoDB serving, and visualization. Later milestones must show actual Spark window/aggregation, joins and physical plans, partition and cache decisions, streaming watermark/recovery, analytics, monitoring, and a deployment beyond Docker alone. The [data contract](week03_contracts.md) defines the evidence those checks should use.

The week-one Spark smoke and week-two **single-DataNode** HDFS round trip are the published executable checks so far. The complete local prototype was developed before these staged weekly commits; these documents record the reviewed week-three design, not original code authorship dates. This local profile is not evidence of multi-node fault tolerance, Kubernetes/cloud deployment, or true berth occupancy. No dataset or generated result belongs in this commit.
