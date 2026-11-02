# HarborTrace

HarborTrace reconstructs **observed vessel presence in the Los Angeles and Long Beach port areas** from AIS (Automatic Identification System) positions published by NOAA (National Oceanic and Atmospheric Administration) and boundaries from USACE (U.S. Army Corps of Engineers). The target uses Lambda architecture (batch reconstruction plus provisional streaming), Apache Spark (a distributed processing engine), HDFS (Hadoop Distributed File System), Kafka (an event-streaming platform), MongoDB (a document database), and a dashboard (a visual data interface).

The **26 October checkpoint** includes executable Spark batch reconstruction, a synthetic dataset with expected results, a small real NOAA excerpt, and the [system architecture](docs/week03_architecture.md).

<a id="team-responsibilities-and-weekly-contributions"></a>

## Team responsibilities

Group 15 assigns one major component to each member. Each owner is responsible for implementation, tests, documentation and demonstrating their component. These responsibilities describe project scope, not completed work.

| Member | Owned component | Responsibilities |
|---|---|---|
| Nguyen Xuan Khai — 20235508 | **Processing engine** | Spark batch and streaming, visit reconstruction, late-data reconciliation, correctness checks and processing performance |
| Do Dang Vu — 20235578 | **Data and infrastructure** | Source acquisition and quality, spatial assignment, HDFS, Kafka replay, deployment, infrastructure monitoring and recovery |
| Nguyen Quang Huy — 20235505 | **Serving and analytics** | MongoDB result storage and publication, dashboard, Spark analytics, model evaluation and application monitoring |

The components share [versioned data contracts](docs/week03_contracts.md). Each member can develop and test with small labelled fixtures (sample inputs with expected behavior), then connect their component to actual upstream outputs during integration. Synthetic examples verify behavior; real-data conclusions require real-data evaluation.

Members author their own changes and review one another's work. Integration defects are resolved by the owner of the affected component. Commit descriptions and test evidence identify the actual change, including adaptations of existing prototype code. Published milestones below document what can currently be run and the limits of its verification.

## Run the serving and analytics component

The serving component validates completed batch outputs and publishes them as immutable MongoDB generations with a single active pointer. It also applies provisional streaming updates, serves a read-only dashboard and runs Spark analytics (window functions, pivot/unpivot, UDF and custom aggregates, join plans, time series, MLlib behind a usable-sample gate, and a port-transition graph). The commands below use only synthetic fixtures and need no MongoDB server:

```bash
python -m pip install -r requirements-serving.txt
python -m pytest -q
PYTHONPATH=src python scripts/serving_import.py --memory demo
PYTHONPATH=src:. python scripts/serve_dashboard.py --memory --load-fixtures
PYTHONPATH=src python scripts/analytics_run.py --synthetic
```

The [serving and analytics guide](docs/serving_analytics_guide.md) covers the boundary formats, MongoDB commands, observed results and the evidence still required. Real-data and deployment evidence is still outstanding.

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
