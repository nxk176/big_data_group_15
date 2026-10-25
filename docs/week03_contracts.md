# Week 3: data and evaluation contracts

Contract (shared data-field and validation requirements) version `1.0` for the [Lambda design](week03_architecture.md) (batch reconstruction plus provisional streaming). Field names and identity rules below are intended interfaces for later published code. Values describe *observations*, not verified arrival or berth times.

MMSI (Maritime Mobile Service Identity) identifies maritime radio stations; UTC (Coordinated Universal Time) normalizes time; SHA-256 (Secure Hash Algorithm with a 256-bit output) provides hashes. A URL (Uniform Resource Locator) identifies a source location and an ID (identifier) distinguishes a record. Canonical observations (normalized records) and visits (reconstructed periods of observed presence) use these terms below.

| Layer | Required fields or identity | Check |
|---|---|---|
| Source snapshot | Provider URL, source date, byte length, SHA-256, retrieval time; original rows | Source is immutable; manifest and bytes agree |
| Boundary snapshot | Provider, feature IDs, geometry, coordinate system, vintage, checksum | Original geometry retained; repaired geometry flagged separately |
| Canonical observation | `mmsi`, UTC `event_time`, longitude, latitude, `source_file`, `source_row`, `source_record_id`, `logical_id`, port assignment, quality flags | `source_record_id = SHA256(source file, row ordinal)`; `logical_id = SHA256(MMSI, UTC time, point)`; invalid coordinates/time/MMSI quarantined |
| Replay message | Canonical fields, `replay_run_id`, `replay_id`, historical event time, `ingested_at` | `replay_id = SHA256(run ID, source record ID, copy index)`; replay retries do not imply new vessel activity |
| Visit | `visit_id`, MMSI, port, first/last observed inside, possible entry/exit intervals, span, gap, status, censoring/quality flags, source count/hash, rule version | `visit_id = SHA256(MMSI, port, first inside, rule version)`; source sample is bounded |
| Hourly aggregate | Port, UTC hour of first observed inside, confirmed count, observed-span statistic, censoring counts | Built from reconciled visits only; candidate/provisional counts remain separate |
| Serving generation | Generation ID, completeness flag, visit and aggregate counts, publication pointer | Readers pin one complete generation; incomplete writes never become active |

## Time, duplicates, and uncertainty

- `event_time` is the provider observation time in UTC. `ingested_at` is when a replay message enters the local pipeline. Processing time does not replace event time in visit rules.
- Two identical logical observations collapse for business state but retain source accounting. Same-MMSI/same-time observations with conflicting positions are quarantined rather than arbitrarily selected. Every source row must reconcile to canonical, outside-region, duplicate, conflict, or invalid status.
- The geometry predicate is `covers`, including an exact polygon boundary point. Near-boundary, overlapping, or repaired-boundary assignments carry flags; overlap alone does not prove membership in either port.
- A candidate becomes confirmed after at least 10 minutes between observed inside points. Exit needs two outside observations at least 10 minutes apart. An observation gap greater than 30 minutes breaks continuity. The detailed transitions are in the [decision table](visit_decision_table.md).
- Entry and exit are intervals constrained by observations. A missing preceding outside point left-censors entry; a missing confirmed exit right-censors the visit. `observed_span_seconds` is the difference between first and last observed inside points, not continuous occupancy.
- A watermark (an event-time progress threshold) may bound streaming state and exclude very late data. Replay retention and batch preserve the basis for correction. “Exactly once” is limited to tested sink (output destination) and idempotency (unchanged effects on retry) behavior; Kafka (an event-streaming platform) delivery alone cannot guarantee one business event.

## Acceptance evidence for later milestones

1. A reference fixture (test input with expected output) covers normal entry/exit, excursions, duplicates, conflicts, boundary ambiguity, gaps, and censoring (missing entry/exit evidence). Spark (a distributed processing engine) batch results must agree with the reference state machine (logic retaining state between observations).
2. Source manifests account for all input rows, list reject reasons, and identify boundary and rule versions. Repeated batch runs over fixed inputs produce the same visit IDs and counts.
3. Streaming checks inject delay and duplicate messages, restart from a checkpoint, and compare provisional versus reconciled results. Report state bounds and late records outside the watermark.
4. Physical Spark plans and row-equivalence checks demonstrate broadcast and sort-merge joins, partition pruning, aggregation and cache decisions on measured input sizes.
5. A publication test verifies count checks, one active generation, and consistent dashboard reads during a failed or retried write.

The two-port scope is not a representative estimate of all port congestion. MMSI may not uniquely identify a vessel over time, AIS (Automatic Identification System) coverage is incomplete, and port polygons (closed geographic boundaries) are not berth boundaries. Human validation against independent evidence remains necessary.
