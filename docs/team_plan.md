# HarborTrace: team delivery plan, weeks 6–15

Planning baseline: 1 November 2026, after the week-five release `10b881b`. This is a proposed allocation for Group 15 to confirm together, not a record of work already completed by its members. Week numbers follow the existing course sequence; confirm submission dates and holidays with the lecturer.

## Outcome and scope

Deliver a reproducible Lambda architecture (batch processing plus streaming processing) that turns AIS (Automatic Identification System) vessel positions and published port boundaries into searchable observed-presence periods, uncertainty-aware summaries, and a dashboard. Demonstrate the full path from ingestion to processing, storage, and visualization on an actual distributed deployment.

The useful questions are: which vessels were observed in each port area, over what supported time intervals, how activity changes over time, and which records need review because evidence is incomplete. A period inside a port boundary is not automatically a verified commercial call, berth occupancy, exact turnaround time, or congestion measurement.

The final system must cover Apache Spark, HDFS (Hadoop Distributed File System) or an equivalent distributed store, Apache Kafka (a message broker), MongoDB (a document-oriented NoSQL database), and actual Kubernetes or Cloud deployment. Continue the existing native-service approach without Docker; the planned cloud deployment remains a future action requiring resources and authorization.

## Starting point: shared code versus local prototype

| State | What it means for this plan |
|---|---|
| Published through week 5 | Proposal, local HDFS smoke test, architecture/contracts, Spark reconstruction checkpoint, validated ingestion and spatial assignment, small fixtures, and English run instructions are in Git. |
| Local prototype only | Batch orchestration, Kafka replay/streaming, MongoDB publication, dashboard, analytics, benchmark scripts, and deployment templates exist locally. They are starting material to review and improve, not automatically available to a teammate cloning GitHub. |
| Still required | Reproducible team releases; independent review; defensible machine-learning results; actual distributed deployment; stronger integration/performance/failure evidence; final report in the required format. |

Do not rewrite weeks 1–5 or assign their existing commits to another person. Week 6 is the first new shared work cycle. An existing prototype file becoming tracked is not proof that its new committer originally wrote it: describe inherited code, actual changes, validation, and assistance honestly.

## Balanced ownership

| Member | Primary engineering responsibility | Cross-cutting responsibility |
|---|---|---|
| Nguyen Xuan Khai — 20235508 | Reconstruction rules, Spark batch/stream state, reconciliation correctness | Correctness experiments and integration contracts |
| Do Dang Vu — 20235578 | Source quality, HDFS, Kafka replay, native/cloud infrastructure | Deployment, resource measurements, recovery |
| Nguyen Quang Huy — 20235505 | MongoDB serving, dashboard, Spark analytics | Analytics evaluation, monitoring, user-visible evidence |

All three implement code, run meaningful checks, write their own report sections, review another member's work, and demonstrate their own component. Khai is not the default owner of every integration problem. Vu and Huy each lead five weekly releases below.

Planning budget per member per week: **6–8 focused hours**, including roughly 4–5 hours implementation, 1–2 hours verification/evidence, and 1 hour review/report. This is an estimate, not time already spent. Record actual effort and redistribute work if a member is overloaded for two consecutive weeks. Split a large task into smaller slices without silently dropping a rubric item.

## Weekly schedule at a glance

Each cell is a personally owned work package. Every member commits their own substantive changes. The release lead checks dependencies, runs the integrated demonstration, and merges the reviewed work; they do not take authorship of teammates' changes.

| Week | Khai | Vu | Huy | Release lead | Visible result |
|---|---|---|---|---|---|
| 6 | File-based batch entry point and repeatable visit output | HDFS ingestion/readback and input accounting | MongoDB generation schema and isolated read/write example | Vu | Prepared input → batch output; independently runnable storage examples |
| 7 | Batch publication interface and generation consistency tests | Seeded Kafka historical replay and retained-message accounting | MongoDB-backed dashboard with port/date filters | Huy | First reproducible local batch → database → dashboard path; Kafka producer ready |
| 8 | Structured Streaming state, watermark and late-event fixture | Kafka → Spark connectivity, retained input and restart scenario | Provisional output adapter and dashboard status separation | Vu | Historical replay → streaming → provisional display |
| 9 | Late-data batch reconciliation and publication failure checks | Multi-day inputs and bounded scale run; review quality cases | Statistics, time series, aggregate transformations and join experiments | Huy | Replay corrected by batch; real-data summaries and measured query plans |
| 10 | Spark cluster execution and one/two-worker result equality | Execute approved cloud deployment, HDFS placement and access checks | Remote MongoDB/dashboard connection and end-to-end health evidence | Vu | Actual deployed pipeline, with task and data placement evidence |
| 11 | MLlib evaluation design and robustness checks | Expanded real-data coverage and data-quality review | Run Spark MLlib and GraphFrames; explain results and limitations | Huy | Evaluated analytics on adequate real data |
| 12 | Multiple-join, partition-count and persistence experiments | Pruning/bucketing, compression and horizontal/vertical measurements | Dashboard query/index optimization and monitoring alerts | Vu | Correctness-preserving performance comparisons and an observable system |
| 13 | Stream/sink retry and publication crash recovery | Replica-loss and isolated backup/restore exercise | Monitoring, integration resilience and access-control checks | Huy | Reproducible failure/recovery evidence with honest guarantee boundaries |
| 14 | Problem/method/correctness report and requirement audit | Data/storage/deployment report and clean-environment rehearsal | Analytics/visualization report and final presentation | Vu | Complete report, evidence index, and reproducible release candidate |
| 15 | Fix reconstruction/integration defects and rehearse defense | Fix deployment/recovery defects and rehearse defense | Fix analytics/dashboard defects and rehearse defense | Huy | Final tested release and three individual demonstrations |

## Weekly commit cards

Paths below identify existing modules or planned additions. A named future script or test is a deliverable to create, not a command claimed to work today. Each week also produces `docs/weekNN_<topic>.md` with the actual setup, exact successful commands, expected output, and limitations; the release lead assembles it from all three members' evidence. Update the contribution ledger after merging.

### Week 6 — storage and batch foundations

- **Khai:** review/adapt `src/harbortrace/batch.py`; add a small batch entry point with explicit input, run ID, output path, and manifest. Add `tests/integration/test_spark_batch.py` coverage comparing saved visits against reference expectations and verifying refusal to overwrite a completed run. Suggested commit: `feat(batch): persist visits with reproducible run manifests`.
- **Vu:** review/adapt `src/harbortrace/hdfs.py` and the published HDFS scripts; demonstrate canonical upload/readback and checksum/row accounting. Add a missing-input/failure check and document source boundaries, duplicates, and licensing attribution. Suggested commit: `feat(storage): verify canonical HDFS upload and readback`.
- **Huy:** review/adapt `src/harbortrace/serving.py`; provide generation metadata, indexes, and queries using an isolated test database. Test round-trip data and missing-generation behavior. Suggested commit: `feat(serving): add generation schema and isolated Mongo smoke check`.
- **Acceptance:** each teammate runs from a clean clone of the shared changes without hidden local modules or hardcoded Khai paths. Both local-file batch and HDFS readback work; MongoDB round-trip counts match. Record which dependencies are required by each command.
- **Early decisions:** all three review contracts, select a feasible deployment environment/budget and ownership by week end, and assess whether existing source coverage can support the planned MLlib port profiles. Resource decisions are separate from provisioning. If resources are unresolved, mark the deployment requirement at risk now.

### Week 7 — first complete batch-to-dashboard path

- **Khai:** connect batch output to the publication interface; validate counts before activation, generation-pinned reads, and absence of mixed-generation results. Extend publication tests. Suggested commit: `feat(batch): validate and hand off complete visit generations`.
- **Vu:** publish/adapt `src/harbortrace/replay.py`, Kafka configuration, and `tests/unit/test_replay.py`. Use a fixed seed, explicit limits and stable record IDs; inject controlled duplicates and delays. Suggested commit: `feat(replay): add bounded deterministic Kafka replay`.
- **Huy:** connect `dashboard/app.py` to MongoDB; provide port/date selection, observed spans, missing bounds and provenance. Test empty data and active-generation switching in an isolated database. Suggested commit: `feat(dashboard): display versioned visits and evidence bounds`.
- **Acceptance:** a documented small run goes from prepared positions through Spark batch and MongoDB to the dashboard. A separate replay run reconciles sent/retained counts. Historical data must be labelled as replay. Reuse the existing active pilot only for reads; tests use their own database.

### Week 8 — streaming path

- **Khai:** adapt `src/harbortrace/streaming.py`; test per-vessel state, event-time watermark, timeouts, out-of-order input, and events beyond the lateness limit. Cover append/update output modes where appropriate. Suggested commit: `feat(stream): reconstruct provisional visits with bounded state`.
- **Vu:** verify Kafka connector dependencies, topic partitions, consumer progress, retained source messages, and checkpoint restart. Supply a repeatable small replay command and failure diagnostics. Suggested commit: `feat(stream-input): verify Kafka consumption and checkpoint recovery`.
- **Huy:** implement/test idempotent provisional sink updates and add separate provisional/final views. Include stream progress, lag and stale-data labels. Suggested commit: `feat(serving): expose provisional stream results separately`.
- **Acceptance:** fixed input produces the expected logical observations despite duplicate delivery. Show late-event behavior, state growth/cleanup and restart evidence. State precisely what is deduplicated and what remains provisional; do not infer universal exactly-once behavior from a checkpoint.

### Week 9 — reconciliation and descriptive analytics

- **Khai:** implement/review the late-bridge example where two provisional periods become one after batch reconciliation. Test removed/replaced visits, incomplete publication and reader consistency. Suggested commit: `feat(reconcile): replace provisional cohorts with validated batch results`.
- **Vu:** provide explicit multi-day source/input manifests and a bounded real-data run with row accounting, runtime, memory and disk evidence. Review 27 cases from the existing 80-visit review pack and record uncertainty/repair flags. Suggested commit: `feat(data): add reproducible multi-day inputs and quality review`.
- **Huy:** implement/review `analytics.py` for window functions, pivot/unpivot, custom aggregation, business UDF (user-defined function), statistics/time series, broadcast and sort-merge joins. Save executed plans and equality checks. Suggested commit: `feat(analytics): add port activity summaries and verified join plans`.
- **Acceptance:** one named immutable dataset drives the outputs; summaries reconcile to its visit rows; the late-data example converges to batch. Review-pack findings are human assessments of available evidence, not independently confirmed commercial calls. Khai/Huy complete their 27/26 review cases by week 11, without simply accepting their own algorithm's output as truth.

### Week 10 — actual distributed deployment

- **Khai:** remove laptop-specific Spark assumptions; run the same batch/stream entry points on cluster workers and compare local/cluster results. Capture actual executors and task placement. Suggested commit: `feat(cluster): run Spark jobs with verified distributed execution`.
- **Vu:** execute the agreed, authorized native cloud plan; configure private networking, service identity, persistent storage, HDFS replication and project-scoped startup/stop. Capture live nodes and block placement. Suggested commit: `feat(deploy): add verified cloud runbook and storage placement evidence`.
- **Huy:** configure remote database/dashboard connections through approved credentials and access controls; capture service health, display correctness and a full small end-to-end run. Suggested commit: `feat(observability): verify deployed serving and pipeline health`.
- **Acceptance:** actual hosts/workers process data, distributed block placement is visible, and the displayed counts match the run manifest. Templates alone do not pass. The current proposed coordinator plus two workers is a team design choice, not a lecturer-mandated machine count. Keep the coordinator's remaining failure limits explicit.
- **Contingency:** if Cloud is unavailable, decide with the team by week 7 whether an accessible Kubernetes deployment can meet the course requirement and the project's no-Docker constraint. Revise the implementation plan before week 10. A laptop-only demo does not silently replace required deployment. Do not wait until week 15 to report the gap.

### Week 11 — advanced analytics with a defensible evaluation

- **Khai:** define features and evaluation before interpreting results; check leakage, censored visits, seed/k sensitivity, stability and whether profile differences support the stated conclusions. Complete 27 human-review cases. Suggested commit: `test(analytics): evaluate clustering stability and feature assumptions`.
- **Vu:** prepare adequate real port/day coverage within an agreed data/resource budget, with input accounting and boundary quality. Publish acquisition/preparation commands and manifests, not large source files. Suggested commit: `feat(data): prepare validated multi-port analytics inputs`.
- **Huy:** run Spark MLlib (Spark's machine-learning library) port-profile clustering and GraphFrames (graph analysis for Spark) on observed transitions; add descriptive time-series views and evaluation results. Complete 26 human-review cases. Suggested commit: `feat(analytics): publish evaluated activity profiles and transition graph`.
- **Acceptance:** executed model results with sample sizes, parameters, preprocessing, comparison across seeds/k and explicit limitations; executed graph and statistical results are separate evidence. The existing proposed gate is ten qualifying ports with twenty sufficiently complete visits each. This is our method choice, not a course minimum. Assess it in week 6; if infeasible, obtain agreement on a justified alternative MLlib experiment early and document the change. Do not fabricate data, lower gates just to force a result, or report an unexecuted model as complete.

### Week 12 — measured optimization and monitoring

- **Khai:** compare multiple-join ordering/strategy, partition counts, and persistence choices on fixed inputs; capture plans, memory/spill, time and equality of outputs. Suggested commit: `perf(spark): measure join and persistence strategies`.
- **Vu:** run date-pruning, bucketing and compression checks; compare one/two workers and at least one vertical resource change with fixed data. Separate cold/warm runs and record costs from the chosen environment. Suggested commit: `perf(storage): measure pruning bucketing and scaling behavior`.
- **Huy:** measure indexed serving queries/dashboard load; collect service logs and metrics and test an alert for a controlled stale/stopped component. Suggested commit: `feat(monitoring): add tested pipeline alerts and serving metrics`.
- **Acceptance:** baseline and changed output agree; timing conditions and repeated runs are recorded; failed/no-improvement experiments are retained honestly. Distinguish raw source volume from regional processing volume. Explain bottlenecks, resource planning and auto-scaling applicability; do not claim automatic scaling was deployed unless it was executed.

### Week 13 — resilience, security and recovery

- **Khai:** test duplicate replay, sink retries, checkpoint restart, publisher crashes before/after activation, competing publishers, and eventual batch convergence. Suggested commit: `test(recovery): verify retry and publication invariants`.
- **Vu:** stop only designated project workers/services; measure replica availability and restore to an isolated namespace/database from verified backups. Document remaining single points of failure. Suggested commit: `test(deploy): verify replica loss and isolated restore`.
- **Huy:** verify authentication/access restrictions, encrypted connections, audit-log content, timeout/retry behavior and an appropriate circuit breaker. Check service discovery and load-balancing behavior or document tested limits of the chosen single-instance serving setup. Suggested commit: `test(integration): verify access controls and service failure behavior`.
- **Acceptance:** retain input, fault, recovery time, output checks and residual limits for each experiment. Backups are not considered proven until a restore is checked. Address encryption, access control, audit, data attribution/retention and compliance in the report. Never expose the unauthenticated laptop configuration to the network.

### Week 14 — final report and independent reproduction

- **Khai:** write problem/method/correctness sections and assigned lessons; audit every Spark requirement against executable code and evidence. Suggested commit: `docs(report): explain reconstruction correctness and Spark evidence`.
- **Vu:** write source/storage/deployment/scaling sections and assigned lessons; reproduce the documented release from a clean environment and log missing prerequisites. Suggested commit: `docs(report): document deployment storage and reproduction results`.
- **Huy:** write analytics/visualization sections and assigned lessons; consolidate evidence links and English presentation with all members' input. Suggested commit: `docs(report): explain analytics evaluation and prepare final presentation`.
- **Acceptance:** every course requirement has code, command, observed result and limitation, or is visibly incomplete with an owner and deadline. All eleven lesson categories use the four required subsections below. Screenshots/results must identify the actual dataset/run. All three rehearse the complete system and their own code.

### Week 15 — defects, final release and defense

- **Khai:** resolve remaining rule/state/reconciliation defects and verify regression behavior. Suggested commit: `fix(reconstruction): resolve final acceptance findings` only when a real fix exists.
- **Vu:** resolve deployment/recovery/reproduction findings and record exact release environment. Suggested commit: `fix(deploy): resolve final reproduction findings` only when applicable.
- **Huy:** resolve dashboard/analytics findings, check all displayed claims, and finalize presentation evidence. Suggested commit: `fix(serving): resolve final demonstration findings` only when applicable.
- **Acceptance:** run the agreed final end-to-end rehearsal and relevant regression suite; match the report to the final commit; each member explains their code, one test, one limitation, and an actual result. If no fixes are needed, commit substantive final reproduction/evaluation notes rather than manufactured code changes. Do not add new major features in the final week.

## Interfaces and dependency rules

1. Freeze observation, visit, manifest and generation contracts in week 6 against the published `docs/week03_contracts.md`. Stable vessel/event/source IDs, UTC (Coordinated Universal Time), rule version, uncertainty flags and observed-span semantics must remain consistent.
2. Vu's prepared Parquet/input manifests feed Khai's reconstruction. Khai's versioned visit output feeds Huy's database/dashboard/analytics. Huy's sink schema is agreed before streaming integration. Use small agreed fixtures while upstream work is in progress.
3. Use separate clones/worktrees and per-member test database names/output directories. Only one active owner edits a shared module at a time; split analytics/state/sink changes into sequential pull requests (PRs: proposed Git change sets). Do not concurrently edit Khai's full local prototype directory.
4. The release lead checks each PR's imports, runtime dependencies, configuration and fixtures. Publish only the reviewed dependency closure. A clean clone must not depend on an unpublished `cli.py`, private config, laptop path, or generated local artifact.
5. Explain real intended changes to inherited modules, run them, and document attribution. Do not shuffle unchanged prototype files among authors just to create a contribution history.

## Commits, review and fair attribution

The stated lecturer rule is **each member must have at least one commit**. The team's stronger working target is **one substantive, individually authored contribution per member per week from week 6 onward**, with a single coordinated weekly release. It is a team practice, not an extra lecturer requirement.

If only one of Vu/Huy performs the weekly merge/push, use the release-lead rotation above. The other member still authors their own commits on their branch. Git author and person pushing are different roles. Preserve those commits when merging so each member's contribution remains visible in `main`; avoid collapsing all three people's work into one commit authored only by the release lead.

- Each member uses their own Git identity and an email associated with their GitHub account. Configure identity in their own clone, not another member's shared checkout. Never impersonate another member or backdate work.
- Branch example: `week06/vu-hdfs-readback`. Submit small explicit file changes with a useful commit message, evidence and limitations. Avoid `git add .` in a checkout containing the full untracked prototype or local data.
- Review rotation: Khai reviews Vu, Vu reviews Huy, Huy reviews Khai. For cross-module changes, include the consuming component's owner too.
- Suggested weekly rhythm: agree contracts/tasks early; implement separately; open PRs before the last day; review and run the integrated demo; merge and update the evidence ledger. Resolve shared-file conflicts with the owner.
- Measure contributions by completed difficulty-adjusted work, checks, review, explanations and reported effort. Commit count and lines changed are not a fair grading formula. The lecturer decides grades; the ledger supplies transparent evidence.
- Shared implementation may use accurate co-author attribution with both contributors' agreement, but each person should still have an independently authored substantive commit to satisfy the stated individual rule clearly. Do not invent contribution credits or report peers' work as one's own. Record AI/external assistance according to course policy.

Every work package is done when its owner can show **changed files + successful check + result/limitations + explanation**, a reviewer has checked it, and the integrated release can reproduce it. Record this in [the contribution ledger](team_contributions.md).

## Final course coverage and ownership

| Requirement | Delivery weeks | Lead / reviewer | Evidence required |
|---|---|---|---|
| Problem, Lambda design, ingestion → visualization | 6–9, 14 | All three | Contracts, actual dataflow, full run and limits |
| Spark processing | 6, 8–9 | Khai / Huy | Reconstruction tests and executed plans |
| HDFS, Kafka, NoSQL | 6–8, 10 | Vu + Huy / Khai | Readback, replay accounting, versioned query checks |
| Kubernetes or Cloud | 10, 12–13 | Vu / Khai + Huy | Live deployment, task/block placement, access and recovery evidence |
| Windows, pivot/unpivot, custom aggregation | 9 | Huy / Khai | Named transformations and independently checked result counts |
| Multistage transformations and UDF | 6, 9 | Huy + Khai / Vu | Business meaning, code, edge cases and result checks |
| Broadcast, sort-merge, multiple-join optimization | 9, 12 | Huy + Khai / Vu | Actual physical plans, equality and measured comparisons |
| Pruning, bucketing, caching/persistence, query plans | 12 | Vu + Khai / Huy | Scan/plan evidence, fixed-input comparison, correctness |
| Streaming modes, watermark, lateness, state, exactly-once boundaries | 8–9, 13 | Khai / Vu + Huy | Replay/late/restart/sink tests and precise guarantee scope |
| MLlib, GraphFrames, statistics/time series | 9, 11 | Huy / Khai + Vu | Three separately executed and interpreted analytics outputs |
| Report and eleven lessons | Incrementally; assembled 14 | All three | Actual challenges, attempted approaches, measurements and takeaways |

### Report lesson allocation

Khai owns three lessons plus report problem/method/integration editing; Vu and Huy each own four lessons. This compensates for Khai's additional core-method writing rather than assigning him all report assembly.

| Lesson category | Writer | Reviewer |
|---|---|---|
| Data Ingestion | Vu | Khai |
| Data Processing with Spark | Khai | Huy |
| Stream Processing | Khai | Vu |
| Data Storage | Vu | Huy |
| System Integration | Huy | Vu |
| Performance Optimization | Khai | Huy |
| Monitoring & Debugging | Huy | Khai |
| Scaling | Vu | Khai |
| Data Quality & Testing | Huy | Vu |
| Security & Governance | Huy | Vu |
| Fault Tolerance | Vu | Khai |

Each lesson must use exactly these subsections: **Problem Description; Approaches Tried; Final Solution; Key Takeaways**. Cover every subtopic listed by the lecturer, including memory/partition/cost-based optimization; hot/cold data; service discovery/circuit breakers/load balancing; alerts/log aggregation; horizontal/vertical/auto-scaling/cost; validation and performance testing; access/encryption/audit/compliance; replication/backup/disaster recovery. Distinguish implemented behavior, measured limits and proposed improvements. Writing about an unexecuted required capability does not complete its implementation requirement.

## Submission boundaries

Public source, PRs, commit messages and documentation remain in English. The single existing Vietnamese reading guide remains ignored locally. Commit small readable evidence summaries, configurations without secrets, and reproducible commands; keep large datasets, credentials, raw logs and generated outputs out of Git. Verify attribution and licensing for shared excerpts.

This document schedules work; it does not provision resources, change an active database generation, publish future modules, or certify that the team has already completed these tasks. Final rubric compliance depends on the actual evidence. No grade is guaranteed by this plan or by the number of commits.
