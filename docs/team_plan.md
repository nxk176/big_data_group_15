# HarborTrace: three independently developed workstreams

Revised 2 November 2026 after team feedback. The baseline remains the published week-five release `10b881b`; new engineering work starts in week 6. This replaces the weekly shared-release rotation with **one substantial component owned by each member through week 15**. Future tasks are planned, not completed contributions. Confirm actual course dates with the lecturer.

## Final outcome

Build a Lambda architecture (batch plus streaming processing) from ingestion to processing, storage and visualization. AIS (Automatic Identification System) positions and port boundaries become searchable observed-presence periods, uncertainty-aware summaries and exploratory analytics. These outputs support reviewing vessel activity and incomplete evidence; they do not establish verified commercial calls, berth occupancy or congestion.

Retain the lecturer's full requirements: Apache Spark, HDFS (Hadoop Distributed File System) or equivalent distributed storage, Kafka or another message queue, a NoSQL database, and actual Kubernetes or Cloud deployment. MLlib (Spark's machine-learning library), GraphFrames (graph analysis for Spark) and statistical/time-series work remain separately tracked. Development remains native without Docker; remote execution needs the agreed resources and authorization.

## One owner per major component

| Owner | Component owned through completion | Deliverable | Can start without |
|---|---|---|---|
| Nguyen Xuan Khai — 20235508 | **Processing engine:** Spark batch, stream state, reconstruction, late-data reconciliation, processing correctness and performance | A runnable engine accepting canonical observations and producing versioned visit outputs and run metadata | Vu's live Kafka/HDFS and Huy's MongoDB/dashboard |
| Do Dang Vu — 20235578 | **Data and infrastructure:** acquisition/quality/spatial assignment, HDFS, Kafka replay, service deployment, infrastructure monitoring and recovery | A reproducible data pipeline and service environment, with normalized files/messages, provenance and health checks | Khai's visit algorithm and Huy's application |
| Nguyen Quang Huy — 20235505 | **Serving and analytics:** MongoDB, generation publication, dashboard, Spark analytical summaries, MLlib/GraphFrames evaluation and application monitoring | A standalone result-import/serving application with evaluated analytics and a dashboard | Vu's data acquisition and Khai's latest processing implementation |

Each owner implements, tests, documents and demonstrates their whole component. Ownership includes its adapters and failures, not just its happy path. A small boundary review is shared; implementation does not move between people every week. All three contribute code and report evidence, and each explains the final integrated pipeline.

## How independent development works

Independence means **independently runnable development and checks**, not three unrelated projects. The real system still connects the components. Fix the format at the boundaries, use portable sample artifacts, and connect real services at scheduled integration gates.

```text
Vu: source + boundaries --> canonical files / Kafka messages + manifests
                                    |
Khai: canonical files / messages --> Spark batch + stream --> visit outputs
                                                                |
Huy: visit files / provisional updates --> MongoDB + analytics + dashboard

During development each receiver can read a local fixture at its boundary.
During integration replace fixtures with the producer's actual output.
```

### Contracts agreed at the start of week 6

Use the already published [data contracts](week03_contracts.md), [Visit model](../src/harbortrace/contracts.py), [reference cases](../tests/fixtures/reference_cases.json) and [real excerpt](../tests/fixtures/real_ais_excerpt.jsonl) as the starting point. Keep contract version `1.0` until a reviewed change is needed. Agree exact serialization, required/nullable fields, identity rules, time format and sample expectations in one short joint session. Do not wait for another component to be finished before writing code.

| Boundary | Producer / consumer | Portable interface and independently testable behavior |
|---|---|---|
| Normalized observations | Vu / Khai | Canonical Parquet or JSONL (one JSON record per line), source/config manifests, stable observation IDs, UTC (Coordinated Universal Time), port assignment and quality flags. Count checks account for rejected/duplicate/conflicting input. |
| Historical replay | Vu / Khai | Canonical observations plus replay identity and event/ingestion times. Vu can write/inspect messages with a diagnostic consumer; Khai can feed the same payloads through local file microbatches before the real Kafka connector is attached. |
| Completed batch | Khai / Huy | Complete visit records, run/rule versions and counts in a manifest; immutable output directory. Huy imports only complete validated artifacts. Khai does not need to write MongoDB. |
| Provisional output | Khai / Huy | Versioned cohort snapshot/update contract with explicit replacement/deletion behavior, not just append-only visit rows. Include a deterministic retry identity and ordering rule. Huy rejects stale updates and tests retries against fixture update sequences. |
| Serving generation | Huy owns internally | Import validation, immutable generation, active pointer, reader consistency, retry/fencing and recovery. Khai supplies artifacts, not database-pointer implementation. |

Week-six fixture deliverables are planned additions, not files claimed to exist today: Vu owns the canonical/replay examples; Khai owns the engine's expected input/output comparison cases; Huy owns standalone complete visit/generation/update fixtures and their validators. Each builds their own side from the published schema immediately, then cross-checks during the initial session. The reference-case `expected` entries are partial assertions, not complete MongoDB documents: Huy must construct and validate full records against the published Visit fields.

Synthetic fixtures must be labelled synthetic. They establish application/contract behavior, not model quality or real-world accuracy. A read-only, checksummed export from the existing local prototype can accelerate real-data work after review and attribution, but no person's development may depend on an unpublished private file.

### Starting independently this week

- **Khai:** run the published checkpoint, then create an engine entry point that reads a local canonical fixture and writes visits plus a manifest. Implement local file-input batch/stream adapters first; later attach HDFS/Kafka adapters. Keep reconstruction logic independent of the MongoDB client.
- **Vu:** run the published week-five ingestion example, then test preparation, HDFS readback and Kafka publish/consume using the normalized fixture. Infrastructure checks can use a tiny Spark job and diagnostic consumers; they do not require reconstructed visits or the dashboard.
- **Huy:** construct small complete synthetic visit and generation fixtures from the published schema, import into an isolated MongoDB database, and build the dashboard and analytics input adapters against those files. The engine need not be running. Model plumbing can use synthetic input; final evaluated results require sufficient real data.

## File ownership and safe handoff

Some modules below exist only in the local prototype. They are reviewed starting material, not dependencies automatically available to a teammate cloning GitHub. In week 6, provide each owner only their reviewed baseline and shared published dependencies, explaining prior authorship and actual changes. Owners may implement against published contracts while that handoff happens. Do not assign unchanged inherited files to new authors as invented contribution credit.

| Owner | Main code boundary | Own tests, tools and documentation |
|---|---|---|
| Khai | `visits.py`, `spark_core.py`, `batch.py`, `streaming.py`, processing `benchmark.py` | Batch/state/reconciliation tests, engine runner, processing benchmarks, processing guide |
| Vu | `ingest.py`, `quality.py`, `spatial.py`, `hdfs.py`, `replay.py`, `deploy/native/` | Ingestion/replay/storage checks, source manifests, infrastructure metrics, cluster runbook |
| Huy | `serving.py`, `analytics.py`, `dashboard/` | Publication/sink/dashboard/analytics tests, result-import runner, application metrics, serving guide |

Paths in the code column are under `src/harbortrace/` unless a directory is stated. Each owner writes a separate entry script under `scripts/` and a component guide; no one waits for a central CLI (command-line interface). Khai maintains shared `contracts.py`/`config.py` after affected owners review changes; published contracts remain usable while changes are discussed. Avoid concurrent editing of the same module. In particular, move prototype database-write logic out of `streaming.py` into Huy's serving adapter when defining the boundary.

Configuration accepts file paths/service addresses as parameters, not another person's laptop paths. Use separate clones, branches, output directories and test databases. A component change must carry its dependencies and a small runnable example, so its clean-clone demonstration needs no untracked private modules.

## Weekly commits within each owned component

Each cell is a substantive weekly commit target, with implementation and its relevant check/evidence. These are progress slices inside the same long-term ownership, not handoffs that must finish in order. Work in the other columns continues if one column slips. A component PR (pull request: a proposed change set) can be reviewed and merged when its own checks pass; it does not wait for a weekly all-or-nothing release.

| Week | Khai — processing engine | Vu — data and infrastructure | Huy — serving and analytics |
|---|---|---|---|
| 6 | Local fixture → batch visits + manifest; assert expected rows and immutable output | Source preparation and canonical fixtures; HDFS upload/readback with count/hash checks | Full synthetic visit/update fixtures; isolated Mongo import/query with schema checks |
| 7 | File-fed streaming state; normal/gap/censored cases and bounded-state checks | Seeded Kafka replay + diagnostic consumer; sent/retained counts and duplicates | Dashboard port/date filters, uncertainty/provenance; empty-state and query checks |
| 8 | Event-time watermark, append/update modes, late-event and restart tests | Multi-day manifests; data-coverage audit and bounded acquisition/preparation for analytics | Generation activation, provisional update replacement, retries and stale-update tests |
| 9 | Batch reconciliation of late bridge/split cases; expose real output at first integration gate | Supply real canonical files/replay; verify source-to-engine counts at first integration gate | Import actual engine output; reconcile dashboard counts; introduce aggregate/time-series pipeline |
| 10 | HDFS/Kafka adapters and remote Spark execution; compare with fixture/local results | Execute agreed cloud deployment; capture workers, block placement and service access evidence | Versioned remote connections; statistics/windows/pivot/unpivot/custom aggregate/UDF and join checks |
| 11 | Spark query-plan experiments: broadcast/sort-merge/multiple joins, partitions and persistence | Validate real analytics input coverage and boundary quality; measure service resource use | MLlib evaluation with appropriate real inputs; GraphFrames and time-series interpretation |
| 12 | Pruning/bucketing/cache benchmarks, equality checks and deployed processing evidence | Fixed-input one/two-worker and vertical-resource comparison; tested infrastructure alerts | Analytics/dashboard query measurements, application alerts and deployed read consistency |
| 13 | Replay/late/restart invariants and engine failure evidence | Replica-loss and isolated restore tests; security, backups and resource/cost record | Publisher crash/fencing/sink retry tests, access checks and application recovery |
| 14 | Processing/correctness report and final component reproduction | Data/storage/deployment report and clean-environment reproduction | Serving/analytics report, evaluated results and presentation assembly |
| 15 | Resolve engine findings, final evidence and individual defense | Resolve infrastructure findings, final evidence and individual defense | Resolve application findings, final evidence and individual defense |

Commit message examples: Khai `feat(engine): persist validated batch artifacts`; Vu `feat(storage): verify canonical HDFS readback`; Huy `feat(serving): import fixture generations into MongoDB`. Later messages name the actual weekly change and check. Do not manufacture edits if a week is devoted to meaningful reproducibility/evaluation evidence.

### Definition of done for a component contribution

The owner supplies changed files, exact runnable command, required environment, fixed input/fixture, observed result, limitation and explanation. A peer reviews the change; it passes its own behavior/contract checks and existing relevant checks. A fixture-only result is labelled as such. Full-pipeline completion is assessed separately at integration gates. Update the [contribution ledger](team_contributions.md) independently for each owner; one incomplete component does not erase another's completed work.

## Planned integration gates, not weekly serial dependencies

| Gate | Joint acceptance | Coordination and defect ownership |
|---|---|---|
| Start of week 6 | Confirm boundary versions, complete examples and runnable component commands | Khai records schema decisions; each owner implements/validates their own adapter. Keep the session small. |
| End of week 9 | First real local end-to-end run: source → batch → MongoDB → dashboard, plus bounded Kafka/stream output and a late-data reconciliation example | Khai coordinates input/output comparison. Vu fixes preparation/transport; Khai fixes processing; Huy fixes import/publication/display. |
| End of week 12 | The same pipeline on actual distributed deployment, with actual workers/block placement and evaluated real-data analytics | Vu coordinates environment and deployment evidence. Khai and Huy run their own components and resolve their own defects. |
| End of week 14 | Clean reproduction, final report, failure evidence, and all rubric gaps recorded | Huy coordinates the presentation/evidence index. Each member verifies their own artifacts and another member's instructions. |

Week 15 is the correction/defense buffer. Do not defer the first connection until the final week. If a gate fails, record the boundary failure, its owner and a near-term retry; unaffected development continues on fixtures. Fixtures do not count as passing a failed real integration gate.

## Risks to resolve early

- **Cloud resources:** Vu checks provider/access/budget by week 6 and resolves availability by week 7; all three agree the scope before remote execution. The planned coordinator plus two workers is our design choice, not a lecturer-issued minimum. If the intended Cloud path is unavailable, review an accessible Kubernetes option against the course requirement and the no-Docker constraint early. A laptop-only demonstration cannot silently replace deployment. Templates are not deployment evidence.
- **Analytics data:** Huy defines features, usable-sample criteria and evaluation in week 6; Vu audits coverage and returns a feasible preparation plan by week 8. The existing port-clustering design uses ten qualifying ports and twenty sufficiently complete visits per port, a project method choice rather than a course rule. If infeasible, agree a justified alternative MLlib experiment early, documenting the design change. Do not fabricate samples or force a clustering result on inadequate inputs. Huy can develop file-based analytics independently, but final validity depends on real data and cannot be mocked away.
- **Schema changes:** propose a versioned change with an example and migration adapter; retain the existing version until consumers can accept the new one. Shared review is a limited dependency, not a reason to halt unrelated work.
- **Inherited prototype:** publish only reviewed component dependencies with attribution. Existing local runs are historical evidence; they do not prove a teammate's new command or a new deployment works.

## Balanced workload, commits and report ownership

Start with approximately **6–8 hours per person per week**, including implementation, checks, review and report notes; these are estimates, not recorded effort. Compare actual workload every two weeks. Vu has infrastructure risk, Khai has state/correctness complexity, and Huy has serving plus analytics/evaluation complexity. Adjust an optional feature or transfer a clearly bounded subtask if one component grows; do not silently drop a rubric requirement or make Khai responsible for all integration and report work.

There is **no alternating Vu/Huy weekly release gate** in this revision. Each person authors and pushes their component branch when ready. Review and merge independently while preserving authorship; never make an unrelated unfinished component block a passing PR. Any person may perform an authorized merge, but cannot claim others' work. Suggested review rotation: Khai reviews Vu, Vu reviews Huy, Huy reviews Khai; boundary consumers review schema changes.

The lecturer's rule remains at least one commit per member. Meet it through substantive work in week 6, then target meaningful weekly contributions by all three. This weekly target is the team's practice, not an additional lecturer rule. Use each person's real Git identity; do not impersonate/backdate or rewrite weeks 1–5. Credit inherited code, external material and AI assistance according to course policy. Contribution evidence includes working features, checks, reviews and explanations, not just commit count or line count.

Each member writes their component's design, implementation, evaluation and limitations. Huy assembles the presentation from their material, not everyone's technical prose. Khai writes problem/method framing; Vu writes deployment/data framing. Keep all eleven lessons, with **Problem Description; Approaches Tried; Final Solution; Key Takeaways** as the four required subsections:

| Lesson | Writer | Evidence supplied by |
|---|---|---|
| Data Ingestion | Vu | Vu |
| Data Processing with Spark | Khai | Khai and Huy |
| Stream Processing | Khai | Khai, Vu and Huy at boundaries |
| Data Storage | Vu | Vu and Huy |
| System Integration | Huy | All three at the gates |
| Performance Optimization | Khai | All three for their components |
| Monitoring & Debugging | Huy | Huy application metrics; Vu infrastructure metrics |
| Scaling | Vu | Vu and Khai |
| Data Quality & Testing | Huy | All three |
| Security & Governance | Huy | Vu infrastructure controls; Huy application controls |
| Fault Tolerance | Vu | All three for their components |

For the existing 80-visit review exercise, Khai/Vu/Huy review 27/27/26 cases by week 11 and discuss disagreements. Assess original observations and boundary uncertainty, not just algorithm output. This is a human review of evidence; independent ground-truth accuracy needs independent reference information. It does not validate commercial calls by itself.

## Course requirement coverage and evidence

| Requirement | Accountable owner | Acceptance evidence / target |
|---|---|---|
| Lambda and ingestion → visualization | Each owner at their boundary | Real integrated run, weeks 9 and 12 |
| Spark batch/stream processing | Khai | Deterministic reference comparisons, runnable engine, weeks 6–13 |
| HDFS and Kafka | Vu | Storage readback/placement, replay accounting, weeks 6–12 |
| NoSQL | Huy | MongoDB import, versioned publication and query consistency, weeks 6–13 |
| Kubernetes or Cloud | Vu environment; Khai/Huy applications | Actual deployed execution and data placement, weeks 10–12 |
| Window functions, pivot/unpivot, custom aggregation, multistage transformations and UDF (user-defined function) | Huy analytics; Vu preparation | Meaningful results and edge-case checks, weeks 9–10 |
| Broadcast/sort-merge/multiple-join optimization | Khai benchmark; Huy analytical query | Executed plans, fixed-input row equality and measured comparison, weeks 10–12 |
| Pruning, bucketing, caching/persistence and query optimization | Khai engine experiments | Real scan/plan evidence, memory/spill and repeated cold/warm comparisons, weeks 11–12 |
| Structured Streaming modes, watermark, lateness and state | Khai | Local-file fixtures then real Kafka late/restart checks, weeks 7–9 and 13 |
| Exactly-once guarantee boundaries | Khai state; Huy sink; Vu retained input | Explicit tested source/state/sink scope, retries/crashes and batch convergence, week 13 |
| MLlib, GraphFrames, statistics/time series | Huy | Three separately executed and evaluated results, weeks 9–12 |
| Report and eleven lessons | Writers above | Code, command, measured result and limitation for every required topic, week 14 |

Report subtopics remain required: memory/partition/cost-based optimization; formats/compression/hot-cold data; service discovery/errors/circuit breakers/load balancing; metrics/alerts/log aggregation/root cause; horizontal/vertical/auto-scaling/resource and cost planning; validation/unit/integration/performance testing; access/encryption/audit/compliance; replication/backup/disaster recovery. State tested limits and unimplemented ideas clearly. A proposed improvement or a single-node restart is not proof of deployed auto-scaling or high availability.

## Publication and completion boundaries

Source, commits and public documents are English. The single Vietnamese guide stays local. Keep large data, raw logs, credentials and generated outputs out of Git; publish concise evidence summaries and reproducible commands. Do not change an existing active database or provision cloud resources merely to edit this plan. Final compliance depends on actual execution evidence; the plan does not guarantee a grade or claim that all future work is complete.
