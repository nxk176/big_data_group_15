# HarborTrace

HarborTrace reconstructs **observed vessel presence in the Los Angeles and Long Beach port areas** from AIS (Automatic Identification System) positions published by NOAA (National Oceanic and Atmospheric Administration) and boundaries from USACE (U.S. Army Corps of Engineers). The target uses Lambda architecture (batch reconstruction plus provisional streaming), Apache Spark (a distributed processing engine), HDFS (Hadoop Distributed File System), Kafka (an event-streaming platform), MongoDB (a document database), and a dashboard (a visual data interface).

The **26 October checkpoint** includes executable Spark batch reconstruction, a synthetic dataset with expected results, a small real NOAA excerpt, and the [system architecture](docs/week03_architecture.md).

## Team responsibilities and weekly contributions

**Group 15. Week 5 is already published (`10b881b`); the following work starts in week 6.** These are planned responsibilities, not claims that future work has been completed. Week 5 delivers validated ingestion and geographic port assignment; the next step adds persistent batch output and storage integration.

| Member | Primary engineering responsibility | Cross-cutting responsibility |
|---|---|---|
| Nguyen Xuan Khai — 20235508 | Reconstruction rules, Spark batch/stream state, reconciliation correctness | Correctness experiments and integration contracts |
| Do Dang Vu — 20235578 | Source quality, HDFS, Kafka replay, native/cloud infrastructure | Deployment, resource measurements, recovery |
| Nguyen Quang Huy — 20235505 | MongoDB serving, dashboard, Spark analytics | Analytics evaluation, monitoring, user-visible evidence |

All three members own implementation, meaningful checks, documentation, peer review and a demonstration of their work. Plan for approximately 6–8 hours per member per week and rebalance using actual effort and task difficulty.

### Who commits and who releases each week?

**Each member authors commits for their own substantive work.** The release lead integrates reviewed changes, checks the combined demonstration and pushes the weekly release while preserving each contributor's authorship.

- **Do Dang Vu:** release lead for weeks **6, 8, 10, 12 and 14**.
- **Nguyen Quang Huy:** release lead for weeks **7, 9, 11, 13 and 15**.
- **Nguyen Xuan Khai:** contributes and commits the Khai work package every week, alongside Vu and Huy.

The lecturer requires at least one commit from every member. The team targets a substantive contribution from each person every week starting in week 6, and should meet the lecturer's minimum during that first shared cycle. The person pushing a release is not necessarily the author of its commits. Evaluate contribution through completed work, tests, review and explanations, rather than commit counts alone.

### Weeks 6–15: individual deliverables

MLlib is Spark's machine-learning library; GraphFrames provides graph analysis for Spark. A generation is a versioned set of results that readers can access consistently.

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

**Start with week 6:** Khai implements reproducible file-based Spark batch output; Vu verifies canonical-data upload/readback through HDFS; Huy prepares MongoDB generation storage and isolated read/write checks. These are independently testable foundations. Week 7 connects batch output, the database and dashboard into the first reproducible local serving path.

The [full delivery plan](docs/team_plan.md) contains per-person commit suggestions, file scope, dependencies, acceptance checks and report assignments. Record actual commit links, results and review in the [contribution ledger](docs/team_contributions.md). Use separate branches/clones and individual Git identities; preserve authorship when merging. Existing local prototype code must be reviewed, attributed and made reproducible before it becomes a shared deliverable.

## Run week 5: ingestion and spatial preparation

Week five makes the stage before reconstruction executable: validate position records, assign ports from geographic boundaries, quarantine rejected rows and feed normalized Parquet into Spark. The included example is synthetic and runs without downloading data or starting services.

From a fresh checkout in Ubuntu with Python 3.12 and Java 17:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-week05.txt
PYTHONPATH=src python scripts/week05_ingestion.py
```

On the existing laptop:

```bash
cd /mnt/e/big_data
source ~/.venvs/harbortrace/bin/activate
export PYTHONPATH="$PWD/src"
python scripts/week05_ingestion.py
```

Expected: 14 input observations account for 7 normalized rows, 3 invalid rows, 1 duplicate, 2 conflicting positions and 1 outside-region row; Spark reconstructs 2 confirmed observed-presence records. Each run saves a manifest (input/run metadata), rejected-row audit, visits and physical plan under ignored `data/week05/`.

See the [week-five guide](docs/week05_ingestion.md) for the complete processing path, optional reviewed real pilot (7,293,408 source rows), explicit acquisition command and limitations. Run the reference checks with:

```bash
PYTHONPATH=src python -m pytest tests/unit/test_reference.py -q
```

## Run the week-four checkpoint

Use Ubuntu / WSL2 (Windows Subsystem for Linux, version 2), Python 3.12, and Java 17. PySpark (the Python interface to Spark) runs the processing code. From a fresh checkout:

```bash
git clone https://github.com/nxk176/big_data_group_15.git
cd big_data_group_15
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-week04.txt
PYTHONPATH=src python scripts/week04_checkpoint.py
```

If the repository is already present, run the setup and demo commands from its root. The laptop's existing environment and an explanation of every result are in the [demo guide](docs/week04_demo_guide.md). The demo runs locally and does not require HDFS, Kafka, or MongoDB to be started.

The command prints synthetic visit, real NOAA excerpt, and summary tables. The summary is:

```text
CHECKPOINT SUMMARY
+-----------------------------+-------+
| Metric                      | Value |
+-----------------------------+-------+
| Synthetic scenarios checked | 5     |
| Synthetic observations      | 23    |
| Synthetic visits            | 6     |
| Reconciled synthetic visits | 5     |
| Candidate synthetic visits  | 1     |
| Real excerpt observations   | 2     |
| Real excerpt visits         | 1     |
| Sort in physical plan       | yes   |
| Spark version               | 3.5.7 |
| Result                      | PASS  |
+-----------------------------+-------+
```

Spark repartitions by MMSI (Maritime Mobile Service Identity, a maritime radio identifier), sorts observations within partitions, and applies the visit state machine. The command checks the expected fields for five synthetic scenarios and one real excerpt visit. The default output is formatted for a terminal presentation. Add `--details` for a provenance table and physical plan, or `--json` for the machine-readable summary. `PASS` verifies these bounded checks; it does not measure accuracy or scalability on the full dataset.

## Data and architecture

- [Checkpoint and data provenance](docs/week04_checkpoint.md): source providers, checksums, row accounting, local pilot measurements, and the demonstration sequence.
- [Synthetic scenarios](tests/fixtures/reference_cases.json): normal entry/exit, crossing midnight, a long gap, censored input, and a short excursion.
- [Real NOAA excerpt](tests/fixtures/real_ais_excerpt.jsonl): two canonical (normalized) observations producing one visit with an observed span of 1,080 seconds and both ends censored.
- [Lambda architecture](docs/week03_architecture.md), [data contracts](docs/week03_contracts.md), and [visit decision table](docs/visit_decision_table.md): component roles and reconstruction rules.

The full January 1 NOAA pilot is stored locally: 7,293,408 source rows and 235,713 regional canonical observations. Those large files are excluded from Git; their provenance and checksums are documented. Port boundaries come from the USACE **Ports layer, ID 0**, within the Port and Port Statistical Area service. Observed area presence does not establish berth occupancy or a verified commercial port call.

## Earlier weekly milestones

Week 1 provides the [proposal](docs/week01_proposal.md) and a minimal Spark runtime smoke test:

```bash
python -m pip install -r requirements-week01.txt
python scripts/spark_smoke.py
```

The expected output includes `range(5).count() = 5`.

Week 2 provides the [Hadoop/HDFS note](docs/week02_hadoop.md) and a separate local storage check. On the documented WSL laptop at `/mnt/e/big_data`:

```bash
bash deploy/native/hdfs-install.sh
bash deploy/native/hdfs-start.sh
bash deploy/native/hdfs-smoke.sh
```

For a new, empty project store, pass `--init-empty` on the first HDFS start as described in the note. The measured smoke passed on one DataNode; multi-node deployment remains a later milestone.

Week 3 documents the Lambda architecture and data contracts linked above. The week-four checkpoint publishes the runnable batch slice; the broader architecture remains a guide for later implementation releases and deployment evidence.

A fuller prototype was built locally before the repository's first commit. Weekly commits record reviewed deliverables rather than original code authorship dates. Bulk datasets, environments, credentials, and generated reports are excluded from Git; the small checkpoint fixtures are included. Development used Codex assistance and requires team review.
