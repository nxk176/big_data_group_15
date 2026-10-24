# HarborTrace

HarborTrace reconstructs **observed vessel presence in the Los Angeles and Long Beach port areas** from historical NOAA AIS positions and USACE port boundaries. The target system uses Lambda architecture with Spark batch and streaming, HDFS, Kafka, MongoDB, and a dashboard.

The **26 October checkpoint** includes executable Spark batch reconstruction, a synthetic dataset with expected results, a small real NOAA excerpt, and the [system architecture](docs/week03_architecture.md).

## Run the checkpoint

Use Ubuntu / WSL2, Python 3.12, and Java 17. From a fresh checkout:

```bash
git clone https://github.com/nxk176/big_data_group_15.git
cd big_data_group_15
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-week04.txt
PYTHONPATH=src python scripts/week04_checkpoint.py --details
```

If the repository is already present, run the setup and demo commands from its root. The laptop's existing environment and an explanation of every result are in the [Vietnamese demo guide](docs/week04_demo_guide_vi.md). The demo runs locally and does not require HDFS, Kafka, or MongoDB to be started.

Expected final output:

```json
{"fixture_cases":5,"input_observations":23,"real_excerpt_observations":2,"real_excerpt_visits":1,"result":"PASS","sort_in_physical_plan":true,"spark_version":"3.5.7","status_counts":{"candidate":1,"reconciled":5},"visits":6}
```

Spark repartitions by MMSI, sorts observations within partitions, and applies the visit state machine. The command checks the expected fields for five synthetic scenarios and one real excerpt visit. `--details` prints case results, the physical plan, and the real visit. `PASS` verifies these bounded checks; it does not measure accuracy or scalability on the full dataset.

## Data and architecture

- [Checkpoint and data provenance](docs/week04_checkpoint.md): source providers, checksums, row accounting, local pilot measurements, and the demonstration sequence.
- [Synthetic scenarios](tests/fixtures/reference_cases.json): normal entry/exit, crossing midnight, a long gap, censored input, and a short excursion.
- [Real NOAA excerpt](tests/fixtures/real_ais_excerpt.jsonl): two canonical observations producing one visit with an observed span of 1,080 seconds and both ends censored.
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
