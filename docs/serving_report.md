# HarborTrace serving and analytics: Huy's report contribution

Owner: Nguyen Quang Huy (20235505). Document date: 20 November 2026.

This source supplies Huy's component sections for the team report. It describes implemented behavior, recorded experiments, and remaining evidence. It does not certify the complete team report or completion of weeks 7–15.

HarborTrace reconstructs observed vessel presence from Automatic Identification System (AIS) positions. A visit describes observations inside a port area, with uncertainty about entry and exit. It does not establish a verified port call, berth occupancy, or congestion.

## Scope and evidence

Huy owns MongoDB publication, the dashboard, Spark analytics, model evaluation, and application monitoring. Khai owns the processing engine. Vu owns data preparation and infrastructure. The components exchange contract `1.0` artifacts and provisional updates.

The [current evidence record](serving_evidence.md) is the source for this revision's commands, measurements, environments, and input labels. The [component guide](serving_analytics_guide.md) supplies operational commands. The [delivery plan](serving_delivery_plan.md) defines the work and its acceptance boundaries.

The historical [week-6 record](week06_serving.md) reports 153 passed tests and two Hadoop Distributed File System (HDFS) skips on 15 November 2026. That run used Java 17 and a real local MongoDB server. Its serving inputs were synthetic. Those counts describe that dated run, not the current suite or a deployed cluster.

Three evidence classes remain separate:

| Input or environment | What it can establish | Limit |
|---|---|---|
| Labeled synthetic visits and updates | Contract behavior, expected counts, retries, failure scenarios, and analytics execution | Cannot establish real vessel accuracy or useful clusters |
| Committed real AIS excerpt | A small, reproducible path from public observations to a served result | Two observations from one vessel do not represent the full real pilot |
| Larger real artifacts and deployed services | Intended basis for joint reconciliation, model interpretation, and operational evaluation | Require separate recorded runs and team review |

## Serving and Analytics Design and Evaluation

### Problem Description

The dashboard needs a consistent result while a new batch loads or provisional visits change. Analytics also needs traceable inputs and enough independent ports for evaluation. A successful demonstration with generated visits cannot answer a question about actual maritime activity.

### Approaches Tried

The component guide records synthetic publication and analytics experiments. The week-6 work adds tests against a real local MongoDB server. The current revision extends directory import, snapshot isolation, dashboard filters, and analytics output records.

The guide also records an earlier grouped pandas aggregate approach. It required Arrow and encountered a Java 21 compatibility failure with Spark 3.5. The implementation uses Spark expressions for custom aggregate folds. Java 17 remains the project baseline.

### Final Solution

The batch importer makes sure that records and their manifest satisfy the contract before publication. A manifest records identity, rule version, counts, source information, and a file hash. The publisher writes one generation, compares stored counts, marks it complete, and then moves the active pointer. Dashboard requests pin one complete generation.

The dashboard applies port, date, status, and vessel filters to its views. Dates use Coordinated Universal Time (UTC). The Maritime Mobile Service Identity (MMSI) identifies a vessel group. The date form includes the selected end date. Internally, the query uses the next midnight as an exclusive bound. The hourly chart counts reconciled visits by their first inside observation.

Candidate-only selections contain no reconciled hourly bars. Summaries aggregate all matching records even when the visit table reaches its display limit.

The analytics methods answer separate questions:

| Method | Implementation and output | Interpretation boundary |
|---|---|---|
| Descriptive statistics | Reconciled span quantiles, mean, standard deviation, skewness, censoring share, and correlation between span and observation count | Describe observed spans and missing boundaries. Correlation does not establish causation. |
| Time series | Daily reconciled arrivals, zero-filled days within each port's observed range, and a trailing seven-day baseline that excludes the current day | Flags require seven prior days and positive baseline variation. An absolute z-score of at least two marks a review case. |
| MLlib | Standardized port profiles and KMeans for `k` in `{2, 3, 4}`, evaluated over three seeds | Requires at least ten ports with twenty complete reconciled visits each. Below this gate, no model is fitted. |
| GraphFrames | Directed transitions between consecutive input visits for each MMSI, PageRank, and strongly connected components | Graph metrics describe the supplied visit sequence, including candidates when present. They do not reconstruct physical routes. |

Complete visits have neither entry nor exit censoring. The ten-port, twenty-visit threshold is a project method choice, not a course rule. The two-port regional scope cannot pass that gate without a wider dataset or a documented alternative experiment.

Port features include median span, median absolute deviation, censoring share, long-gap share, night-arrival share, visits per vessel, and mean observation count. KMeans selection uses mean silhouette across seeds. Pairwise co-membership agreement measures stability without relying on cluster labels. The exported assignment uses the first seed and records that seed's score separately.

GraphFrames execution is optional and explicitly reported. Without a working Python package and Spark JAR, DataFrame calculations retain transition totals and top edges. PageRank and component results require GraphFrames execution. The implemented GraphFrames PageRank is unweighted, even though transition counts are retained on edges.

The Spark exercises also include windows, pivot/unpivot, a Python duration-bucket function, custom aggregate folds, and multistage transformations. Join experiments compare broadcast and sort-merge output rows and save physical plans. A multiple join adds per-vessel statistics. These experiments establish selected strategies and row agreement, not a distributed performance advantage.

The runner writes comma-separated values (CSV) with the standard CSV writer. It records artifact identity or synthetic generator parameters, input hashes, Spark configuration, phase timings, and output counts. Cached intermediate tables avoid some repeated actions. The work does not measure a speedup against a prior version. Driver-side collection and per-group arrays still limit scale.

### Key Takeaways

Data sufficiency is part of model evaluation. Synthetic profiles test the evaluator, while real interpretation requires qualifying real inputs. Timings need an environment and a defined scope. Preserving uncertainty and provenance makes a result reviewable.

## System Integration

### Problem Description

The engine emits completed batch artifacts and streaming update directories. Serving must distinguish complete files from unfinished writes. Retries and restarts must preserve committed results. Competing writers must not expose a mixture of provisional states.

### Approaches Tried

The original walkthrough imports completed fixtures and applies a JSON update sequence. The published engine also emits Spark directories named `updates/batch=<number>`. Manual selection of those files does not provide durable progress tracking. The current revision adds a directory importer and tests for ordering, completion markers, retries, conflicts, and restart behavior.

### Final Solution

`apply-directory` discovers batch directories, sorts their numbers numerically, and stops at the first directory without `_SUCCESS`. It makes sure that every update satisfies the contract before its first cohort update. A source lease coordinates importers. The lease itself writes database state.

A database receipt records each completed imported batch and its checksum. Repeating the same content reuses that receipt. Changed content under an imported batch number causes a conflict. A stable `--source-id` preserves receipt identity when files move. Without that argument, identity uses the resolved updates path.

Provisional updates replace, upsert, or delete visits within a cohort. A cohort groups visits that one update manages. Deterministic snapshot identities separate competing payloads and their base states. The writer stages immutable rows and moves the cohort pointer with a compare-and-set operation. This operation succeeds only if the expected prior pointer still matches.

Readers select rows from the pinned cohort snapshot. Prior and abandoned snapshots remain stored, so an update does not delete records still needed by a reader. Retention needs a separate maintenance policy.

The directory importer does not provide a transaction across all cohorts or all batches. Earlier cohort updates can remain committed if a later update fails. An interrupted batch has no completed receipt. A retry uses each update's identity to resume safely.

### Key Takeaways

A completion marker and a database receipt solve different problems. The marker identifies complete engine output. The receipt records completed serving work. Publication guarantees must state their scope: a complete batch generation or an individual cohort snapshot.

## Monitoring and Debugging

### Problem Description

A responding dashboard can still show stale results or a broken active pointer. Operators need to distinguish database failure, incomplete publication, count mismatch, and delayed provisional updates. Debugging also needs reproducible input and environment records.

### Approaches Tried

The fixture walkthrough injects interrupted publication and outdated publisher tokens. The week-6 run repeats publication behavior against a real local server. It reports a provisional-lag alert because fixture timestamps are fixed. This is expected evidence of the alert, not proof of a live ingestion delay.

### Final Solution

The application exports request counters, errors, timings, publication outcomes, fenced writes, and update outcomes at `/metrics`. `/healthz` reports database reachability, active-generation consistency, stale generations, failed generations, stuck publication, and provisional lag. Critical or down health produces HTTP 503.

Publication decisions enter `audit_log` and JSON logs. The analytics runner records input provenance and phase timings. The evidence record links commands to observed outputs. Application recovery can retry an import, reactivate a previous generation, or mark abandoned loading generations failed.

Metrics reside in process memory. Continuous collection and retention require deployed monitoring. Application recovery tests do not establish database restart recovery or failover between cluster members.

### Key Takeaways

Availability, freshness, and correctness need separate signals. Fixed test timestamps can intentionally trigger freshness alerts. A diagnostic claim needs the input, command, environment, and observed output together.

## Data Quality and Testing

### Problem Description

Malformed records can pass through a permissive database and distort summaries. Correct totals alone do not prove that identities, durations, uncertainty bounds, and source evidence agree. A passing local suite also cannot establish accuracy against human judgments.

### Approaches Tried

Synthetic fixtures provide known visit and update outcomes. Unit tests use `mongomock` for isolated behavior. Real-server integration tests exercise MongoDB operations and index use. The week-6 record compares database contents before and after invalid input rejection.

### Final Solution

The validator enforces contract versions, required fields, types, UTC offsets, interval order, span arithmetic, censoring consistency, bounded source samples, flags, and recomputed identities. Artifact validation also checks hashes, counts, status totals, rule version, and duplicates.

Regression coverage targets expired leases, racing provisional writers, pinned readers, numeric directory order, incomplete directories, interrupted imports, filtered charts, and summaries beyond the table limit. Analytics coverage includes CSV round trips, provenance, aggregate values, empty inputs, sample gates, and evaluation outputs. Current executed outcomes belong in the evidence record.

The real AIS excerpt establishes only a small integration example. It cannot replace all-port reconciliation on the larger real artifact. Human review of 26 cases and independent peer review remain pending. Automated reference fixtures do not constitute either activity.

### Key Takeaways

Tests need invariants and expected outcomes, not only successful execution. Record rejection must preserve the relevant published view. State explicitly which failures are simulated and which use a real service.

## Security and Governance

### Problem Description

Serving exposes source-derived records and publication controls. Deployment needs separate reader and publisher accounts, protected credentials, transport security, and traceable publication decisions. Application options alone cannot establish those controls.

### Approaches Tried

The local component uses a GET-only dashboard, optional bearer-token protection, escaped HTML, and restrictive response headers. MongoDB connections request majority read and write concerns. Local standalone tests exercise application behavior, but they do not demonstrate configured accounts or replicated durability.

### Final Solution

The dashboard compares bearer tokens in constant time when `HARBORTRACE_DASHBOARD_TOKEN` is set. Without a configured token, access is open. `/healthz` remains available for probes. Responses use `no-store`, `nosniff`, and a restrictive Content Security Policy.

`connect(..., read_only=True)` disables retryable writes. MongoDB account permissions enforce database read access. Deployment must create and test a restricted dashboard account and a separate publisher account. Credentials belong in the deployment environment, outside committed files.

Transport Layer Security (TLS) encrypts network connections. The deployment must configure and test TLS for MongoDB and HTTPS for the dashboard. This report does not claim that the local plaintext runs establish either control.

Generation identity, checksums, rule versions, and audit entries support traceability. The application appends audit entries, but ordinary database privileges can still permit alteration. No tamper-resistant audit storage is claimed.

Majority concerns request MongoDB acknowledgment and visibility rules. A standalone server does not gain replicas through this configuration. Replica-set deployment, backup restoration, process termination, database restart, and cluster recovery require separate evidence.

### Key Takeaways

Separate implemented safeguards from deployed controls. Read-only behavior needs both application restrictions and database permissions. Durable publication needs tested infrastructure as well as pointer logic.

## Remaining acceptance work

| Requirement | Evidence still needed |
|---|---|
| Joint integration gate | Import larger real engine artifacts and reconcile complete dashboard counts with Khai and Vu |
| Real analytics | Sufficient real port coverage, complete-visit counts, and documented interpretation or an agreed alternative experiment |
| Deployment | Remote service configuration, TLS/HTTPS, restricted accounts, and access tests |
| Operational recovery | Deployed process termination, database restart, backup restoration, and cluster recovery |
| Human evaluation | Recorded review of 26 cases and disagreement resolution |
| Peer review | Independent review with findings and responses |
| Team submission | Assemble component sections and slides, reconcile shared results, and review the complete report and deck |

These items remain acceptance conditions after the local component work. The [presentation source](serving_presentation.md) supplies Huy's slides for assembly into the team deck. AI assistants supported implementation and writing. Huy remains responsible for reviewing, explaining, and disclosing that assistance under course policy.
