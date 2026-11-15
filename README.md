# HarborTrace

HarborTrace reconstructs **observed vessel presence in the Los Angeles and Long Beach port areas** from AIS (Automatic Identification System) positions published by NOAA (National Oceanic and Atmospheric Administration) and port boundaries from USACE (U.S. Army Corps of Engineers). It uses a Lambda architecture: a batch path reconstructs complete visits from stored data, and a streaming path publishes provisional visits while data arrives, later corrected by batch. The system is built on Apache Spark (a distributed processing engine), HDFS (Hadoop Distributed File System), Kafka (an event-streaming platform), MongoDB (a document database) and a dashboard (a visual data interface).

```text
                  Data and infrastructure                Processing engine              Serving and analytics
NOAA AIS  ──►  canonical observations ──► HDFS  ──►  Spark batch     ──► completed batch ──►  MongoDB generations,
USACE ports    replay messages        ──► Kafka ──►  Spark streaming ──► provisional      ──►  dashboard, analytics
```

The [system architecture](docs/week03_architecture.md) and the [data contracts](docs/week03_contracts.md) define the components and the formats between them.

<a id="team-responsibilities-and-weekly-contributions"></a>

## Team responsibilities

Group 15 assigns one major component to each member. Each owner is responsible for the design, implementation, tests, documentation and demonstration of their component.

| Member | Owned component | Responsibilities |
|---|---|---|
| Nguyen Xuan Khai — 20235508 | **Processing engine** | Spark batch and streaming, visit reconstruction, late-data reconciliation, correctness checks and processing performance |
| Do Dang Vu — 20235578 | **Data and infrastructure** | Source acquisition and quality, spatial assignment, HDFS, Kafka replay, deployment, infrastructure monitoring and recovery |
| Nguyen Quang Huy — 20235505 | **Serving and analytics** | MongoDB result storage and publication, dashboard, Spark analytics, model evaluation and application monitoring |

The components meet only at the versioned data contracts, so each owner develops and tests against small labelled fixtures (sample inputs with expected behavior) and connects to the real upstream output at integration. Synthetic examples verify behavior; real-data conclusions require real-data evaluation. Members author their own changes and review one another's; an integration defect is fixed by the owner of the affected component.

## Setup

Ubuntu / WSL2 (Windows Subsystem for Linux, version 2), Python 3.12 and Java 17. From a fresh checkout:

```bash
git clone https://github.com/nxk176/big_data_group_15.git
cd big_data_group_15
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-engine.txt -r requirements-serving.txt
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # path of your Java 17
python -m pytest -q
```

The commands below use committed fixtures and need no running service unless stated.

## Data and infrastructure — Do Dang Vu

Validates position records, assigns ports from the USACE boundaries, quarantines rejected rows, and publishes canonical observations (normalized position records) and replay messages. Canonical data is stored in HDFS with verified readback; replay messages are the Kafka input of the streaming engine.

```bash
PYTHONPATH=src python scripts/week05_ingestion.py                # synthetic source -> canonical Parquet, rejected-row audit
PYTHONPATH=src python scripts/data_storage.py fixtures --check   # boundary fixtures still match a fresh preparation
PYTHONPATH=src python scripts/data_storage.py hdfs-readback      # needs a running HDFS (deploy/native/hdfs-*.sh)
```

The synthetic source accounts for 14 rows: 7 canonical, 3 invalid, 1 duplicate, 2 conflicting and 1 outside the region. Guides: [data and infrastructure](docs/data_infrastructure.md) (fixtures, portable HDFS scripts, readback results, limits) and [ingestion and spatial preparation](docs/week05_ingestion.md) (processing path, real pilot of 7,293,408 source rows, acquisition command).

## Processing engine — Nguyen Xuan Khai

Turns canonical observations into the completed-batch artifact that serving imports, and replay messages into provisional updates with Spark Structured Streaming. Streaming state per vessel is bounded: observations wait in a buffer until the event-time watermark passes them, then feed constant-size visit state machines and are discarded.

```bash
PYTHONPATH=src python scripts/engine_batch.py --fixture
PYTHONPATH=src python scripts/engine_stream.py --fixture
```

On one laptop, batch reconstructed 5,081 visits from 3,264,274 real observations (2–15 January 2024), and streaming replayed 236,213 real messages for 1 January from files and from Kafka, producing the same 370 visit identifiers as batch. The [processing engine guide](docs/processing_engine.md) gives the design, commands, measurements, late-arrival and restart experiments, and limits. Cluster and long-running service evidence is still outstanding.

The first batch slice (repartition by MMSI, the Maritime Mobile Service Identity; sort within partitions; apply the visit state machine) is still runnable with `PYTHONPATH=src python scripts/week04_checkpoint.py`. It checks five synthetic scenarios and one real excerpt visit; the [demo guide](docs/week04_demo_guide.md) explains each result.

## Serving and analytics — Nguyen Quang Huy

Validates completed batch outputs and publishes them as immutable MongoDB generations with a single active pointer, applies provisional streaming updates, serves a read-only dashboard and runs Spark analytics (window functions, pivot/unpivot, UDF and custom aggregates, join plans, time series, MLlib behind a usable-sample gate, and a port-transition graph).

```bash
PYTHONPATH=src python scripts/serving_import.py --memory demo
PYTHONPATH=src:. python scripts/serve_dashboard.py --memory --load-fixtures
PYTHONPATH=src python scripts/analytics_run.py --synthetic
```

The [serving and analytics guide](docs/serving_analytics_guide.md) covers the boundary formats, MongoDB commands, observed results and the evidence still required. Real-data and deployment evidence is still outstanding.

## Data and design references

- [Proposal](docs/week01_proposal.md) and [Hadoop/HDFS note](docs/week02_hadoop.md).
- [Lambda architecture](docs/week03_architecture.md), [data contracts](docs/week03_contracts.md) and [visit decision table](docs/visit_decision_table.md): component roles and reconstruction rules.
- [Checkpoint and data provenance](docs/week04_checkpoint.md): source providers, checksums, row accounting and local pilot measurements.
- [Synthetic scenarios](tests/fixtures/reference_cases.json) and [real NOAA excerpt](tests/fixtures/real_ais_excerpt.jsonl).

The full 1 January NOAA pilot is stored locally: 7,293,408 source rows and 235,713 regional canonical observations. Large files are excluded from Git; their provenance and checksums are documented. Port boundaries come from the USACE **Ports layer, ID 0**, within the Port and Port Statistical Area service. Observed area presence does not establish berth occupancy or a verified commercial port call.

## Provenance

A fuller prototype was built locally before the repository's first commit; commits record reviewed changes, not the dates code was first written. Development used AI coding assistants (Codex and Claude); each owner reviews and tests the code of their component. Bulk datasets, environments, credentials and generated reports are excluded from Git.
