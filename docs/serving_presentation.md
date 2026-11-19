# HarborTrace: Huy's presentation source

Owner: Nguyen Quang Huy (20235505). Prepared: 20 November 2026.

This Markdown source provides eight component slides for assembly into the team deck. It is not a completed team deck or evidence that the full report passed review. Speaker notes expand the concise slide text. Use the [current evidence record](serving_evidence.md) for final commands and measured results.

## Slide 1: From engine output to a reviewable view

- Completed batch artifact → validated MongoDB generation → dashboard.
- Streaming updates → versioned cohort snapshot → provisional view.
- Spark analytics → descriptive statistics, time series, clustering, and graphs.

Speaker notes:

Huy owns serving and analytics. Khai owns engine computation, and Vu owns data and infrastructure. A visit describes observed presence inside a port area. It does not establish a commercial port call or congestion. Keep this distinction visible throughout the team presentation.

Sources: [architecture](week03_architecture.md), [report](serving_report.md#scope-and-evidence).

## Slide 2: System Integration

- Publish complete generations through one active pointer.
- Pin a generation for each dashboard request.
- Read update directories in numeric order and stop at an unfinished batch.
- Store import receipts and preserve prior cohort snapshots.

Speaker notes:

A completion marker identifies finished engine files. A receipt records completed serving work. The importer makes sure that a batch satisfies the contract before its first cohort update. Its source lease already writes database state. Deterministic snapshot identities isolate competing writers. A compare-and-set moves the pointer only if the expected prior state matches.

The importer does not wrap all cohort updates in one transaction. If a later update fails, earlier committed updates can remain. Retry uses individual update identities. Old snapshots remain available for pinned readers and need an explicit retention policy.

Sources: [integration lesson](serving_report.md#system-integration), [current evidence](serving_evidence.md).

## Slide 3: One filter selection across the dashboard

- Select port, UTC dates, status, and MMSI.
- Show uncertainty and source evidence beside visits.
- Summarize all matches beyond the table display limit.
- Count reconciled arrivals in the hourly chart.

Speaker notes:

MMSI groups observations by vessel identity. The date form includes the selected end date. The internal bound is the next midnight. The hourly chart uses first observed inside times, not occupancy. Candidate-only filters produce no reconciled hourly bars. A real-data label identifies an input type, not independent validation of each visit.

Demonstrate one fixture selection and one empty result. Use only counts recorded for that run. A small real excerpt can demonstrate the interface boundary, but it cannot replace the larger real integration gate.

Sources: [dashboard design](serving_report.md#final-solution), [current evidence](serving_evidence.md).

## Slide 4: Four different analytics questions

| Method | Question | Limit |
|---|---|---|
| Statistics | What spans and uncertainty occur in this input? | Observations do not establish causation. |
| Time series | Which daily arrival counts differ from recent days? | A flag requires review. |
| MLlib | Do qualifying port profiles form stable groups? | At least ten ports with twenty complete visits each. |
| GraphFrames | How do consecutive visits connect ports? | Transitions do not establish routes. |

Speaker notes:

The daily series fills missing dates within each port's observed range. Its seven-day baseline excludes the current day. Flags require seven prior days, positive variation, and an absolute z-score of at least two.

KMeans uses standardized profiles, three candidate cluster counts, and three seeds. Selection uses mean silhouette, with separate stability and representative-seed results. Complete visits are reconciled and uncensored at both ends. The two-port regional input cannot pass the ten-port gate.

GraphFrames adds unweighted PageRank and strongly connected components. Transition counts remain edge attributes. Without GraphFrames execution, the output reports DataFrame transition metrics and the reason for fallback. Graph input can include candidate visits. Synthetic results test execution and do not support industry conclusions.

Sources: [analytics design and evaluation](serving_report.md#serving-and-analytics-design-and-evaluation), [current evidence](serving_evidence.md).

## Slide 5: Monitoring and Debugging

- `/healthz`: reachability, consistency, freshness, and delayed updates.
- `/metrics`: request, publication, update, and timing records.
- Audit entries: loading, completion, activation, rollback, and failure.
- Analytics outputs: input identity, configuration, plans, and phase timings.

Speaker notes:

A service can respond while its results are stale. Distinguish application availability, result freshness, and count consistency. Fixed fixture dates intentionally cause provisional-lag alerts in some demonstrations. They do not measure live ingestion delay.

Metrics reside in process memory. Deployed monitoring must collect and retain them. Timings include declared phases and exports. The work does not measure a speedup against a prior version. Local application recovery does not demonstrate cluster recovery.

Sources: [monitoring lesson](serving_report.md#monitoring-and-debugging), [current evidence](serving_evidence.md).

## Slide 6: Data Quality and Testing

- Validate fields, identities, times, intervals, censoring, hashes, and totals.
- Test retries, conflicts, expired leases, racing writers, and pinned readers.
- Compare filtered tables, summaries, and charts.
- Keep synthetic tests, real excerpts, and human review separate.

Speaker notes:

The dated week-6 run reports 153 passed tests and two HDFS skips on 15 November 2026. Its serving inputs were synthetic, although MongoDB was a real local server. These are historical counts. Present this revision's totals only from the current evidence record.

CSV round trips and provenance tests protect analytical output. The committed real excerpt contains two observations from one vessel. It supports a small integration example. The 26-case human review and independent peer review remain pending. Automated fixtures do not replace either review.

Sources: [testing lesson](serving_report.md#data-quality-and-testing), [week-6 record](week06_serving.md), [current evidence](serving_evidence.md).

## Slide 7: Security and Governance

- GET-only dashboard with optional bearer-token protection.
- Separate dashboard and publisher accounts in deployment.
- Configure MongoDB TLS and dashboard HTTPS.
- Preserve checksums, versions, audit records, and rollback generations.

Speaker notes:

Token protection is optional and requires a configured secret. The health endpoint remains open for probes. The `read_only=True` connection option disables retryable writes. Database account permissions enforce read access. Local tests do not establish configured account isolation or transport encryption.

Majority read and write concerns do not create replicas on a standalone server. Application audit entries are not tamper-resistant storage. Deployment still needs access tests, backup restoration, database restart, and cluster recovery evidence.

Sources: [security lesson](serving_report.md#security-and-governance), [current evidence](serving_evidence.md).

## Slide 8: What remains before team acceptance

- Reconcile larger real engine artifacts and dashboard totals jointly.
- Obtain qualifying real analytical samples or document an alternative.
- Test deployed accounts, transport security, monitoring, and recovery.
- Complete the 26-case human review and independent peer review.
- Assemble and review the full team report and deck.

Speaker notes:

The component provides runnable paths and locally testable safeguards. Completion claims depend on the corresponding evidence, not calendar weeks. Use the evidence record to distinguish this revision's measured runs from pending work. Do not present synthetic clusters as real port categories or a small excerpt as the full real-data gate.

AI assistants supported this component and these materials. Huy must review and explain the implementation and disclose assistance under course policy.

Sources: [remaining acceptance work](serving_report.md#remaining-acceptance-work), [delivery plan](serving_delivery_plan.md), [current evidence](serving_evidence.md).
