# HarborTrace

HarborTrace reconstructs **observed vessel presence in the Los Angeles and Long Beach port areas** from AIS (Automatic Identification System) positions published by NOAA (National Oceanic and Atmospheric Administration) and boundaries from USACE (U.S. Army Corps of Engineers). The target uses Lambda architecture (batch reconstruction plus provisional streaming), Apache Spark (a distributed processing engine), HDFS (Hadoop Distributed File System), Kafka (an event-streaming platform), MongoDB (a document database), and a dashboard (a visual data interface).

The **26 October checkpoint** includes executable Spark batch reconstruction, a synthetic dataset with expected results, a small real NOAA excerpt, and the [system architecture](docs/week03_architecture.md).

## Team responsibilities and weekly contributions

**Group 15: one independently developed component per member, from week 6 through completion.** Week 5 (`10b881b`) is already published. This allocation was revised after team feedback on 2 November 2026; the weekly targets below are future work, not completed contributions.

| Owner | Component owned through completion | Deliverable | Can start without |
|---|---|---|---|
| Nguyen Xuan Khai — 20235508 | **Processing engine:** Spark batch, stream state, reconstruction, late-data reconciliation, processing correctness and performance | A runnable engine accepting canonical observations and producing versioned visit outputs and run metadata | Vu's live Kafka/HDFS and Huy's MongoDB/dashboard |
| Do Dang Vu — 20235578 | **Data and infrastructure:** acquisition/quality/spatial assignment, HDFS, Kafka replay, service deployment, infrastructure monitoring and recovery | A reproducible data pipeline and service environment, with normalized files/messages, provenance and health checks | Khai's visit algorithm and Huy's application |
| Nguyen Quang Huy — 20235505 | **Serving and analytics:** MongoDB, generation publication, dashboard, Spark analytical summaries, MLlib/GraphFrames evaluation and application monitoring | A standalone result-import/serving application with evaluated analytics and a dashboard | Vu's data acquisition and Khai's latest processing implementation |

### Work independently, integrate at planned gates

- **Khai** develops Spark processing with local observation fixtures and writes visit artifacts; live HDFS/Kafka or MongoDB is not needed for engine development.
- **Vu** develops ingestion, storage, replay and deployment with canonical fixtures, diagnostic consumers and tiny Spark jobs; the final reconstruction algorithm/dashboard is not needed for infrastructure checks.
- **Huy** develops MongoDB import/publication, analytics and the dashboard with complete synthetic visit/update fixtures matching the published contract; the latest engine does not need to run.

At the start of week 6, agree serialization, fields, versions and sample expectations using the [published contracts](docs/week03_contracts.md). Each owner validates their own side and keeps a standalone run command. Synthetic fixtures support development, not claims about real-data model quality. The [detailed plan](docs/team_plan.md) defines file ownership, adapters, data handoff and acceptance checks.

Integrate **at the end of week 9** for the first real local pipeline, **week 12** for the deployed system and evaluated analytics, and **week 14** for final reproduction. Each owner fixes defects in their component. Earlier component reviews/merges do not wait for these gates; a failed gate remains an explicit integration gap.

### Weekly commits inside each component

**Each member commits and pushes their own component branch when ready. There is no rotating weekly release gate.** Review and merge passing component changes independently, preserving authorship. The lecturer requires at least one commit from each member; the team targets a meaningful contribution from all three each week. Track work, checks, reviews and actual effort in the [contribution ledger](docs/team_contributions.md), rather than judging fairness by commit counts alone.

MLlib is Spark's machine-learning library; GraphFrames provides graph analysis for Spark. A generation is a versioned set of results that readers access consistently.

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

All three own code, tests, documentation and an individual demonstration. Start with 6–8 hours per person per week, review actual workload every two weeks, and rebalance concrete subtasks when needed. The full plan retains the course requirements, report/lesson allocation and actual deployment/analytics evidence gates. Existing local prototype modules require reviewed handoff and attribution; they are not automatically present in a clean GitHub clone.

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
