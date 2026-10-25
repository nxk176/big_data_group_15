# Week 4: executable checkpoint for 26 October

For setup instructions, detailed output, a project explanation, and a presentation walkthrough, use the [demo guide](week04_demo_guide.md).

The [week-three Lambda diagram](week03_architecture.md) shows the full target system. This checkpoint adds a runnable Spark **batch reconstruction slice** and a small dataset to Git. It exercises canonical observations → repartition by MMSI → sort by event time and stable IDs → per-vessel visit state machine → result checks. [Week two](week02_hadoop.md) separately demonstrates an HDFS write/read round trip on the local DataNode.

## Run from a fresh checkout

If the existing laptop environment is already installed, open Ubuntu directly and run:

```bash
cd /mnt/e/big_data
source ~/.venvs/harbortrace/bin/activate
export PYTHONPATH="$PWD/src"
python scripts/week04_checkpoint.py
```

Use Ubuntu 24.04 / WSL2, Python 3.12, and Java 17. Run the commands below from the repository root; on the existing laptop, first run `cd /mnt/e/big_data`. The [README](../README.md) includes the clone command. After installing dependencies, the demo needs only the included fixtures and does not require Kafka, MongoDB, or the large NOAA file.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-week04.txt
PYTHONPATH=src python scripts/week04_checkpoint.py
```

The default output contains three readable tables: synthetic visits, the real NOAA excerpt visit, and checkpoint summary metrics. Add `--details` for a real-visit provenance table and the Spark physical plan. Use `--json` for the machine-readable summary instead. The real excerpt row count is measured and checked, rather than only reported as a constant.

The command reads `tests/fixtures/reference_cases.json` and `tests/fixtures/real_ais_excerpt.jsonl`. It fails if the Spark physical plan has no sort, if any synthetic rule case differs from its expected visit fields, or if the real excerpt yields a different result. The presentation output reports these checked values:

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

Release verification on 2026-10-25 exported the staged Git files to a clean directory and ran `PYTHONPATH=src python scripts/week04_checkpoint.py --details` using the existing WSL Python environment. The command exited 0 with the same summary. The clean directory contained only the Git release files, so the demo did not rely on unpublished project modules or the local full dataset.

JSONL fixtures use LF line endings enforced by `.gitattributes`, so Git checkouts preserve the documented sample checksum on Windows and Linux.

This command is safe to repeat. It creates only ignored Spark temporary files under `data/` and does not publish to a database. The synthetic fixture uses predetermined port assignments to isolate the visit rules. The real excerpt retains the canonical assignment and quality flags generated from the recorded USACE boundary snapshot. Neither sample proves that every raw AIS position was assigned correctly.

## Data provenance and readiness

The source is [NOAA OCM historical AIS GeoParquet for 2024](https://github.com/ocm-marinecadastre/ais-vessel-traffic/blob/main/data/ais-broadcast-points-2024-readme.md), with the [USACE **Ports** layer (ID 0)](https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/Port_Statistical_Area/FeatureServer/0) for boundaries. The service also has a separate [**PortStatisticalAreas** layer (ID 1)](https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/Port_Statistical_Area/FeatureServer/1); HarborTrace does not use it. Earlier project wording called the selected polygons “statistical port areas”; the precise term is **port boundaries from the Ports layer**. The local January 1 source snapshot is 227,181,435 bytes, SHA-256 `3124557f70bb216e3a4d5817cf019368a6ca47d86aec8d3d2aa8c8e6d1e92560`. Its local manifest records the provider URL, content MD5, CC0 license, and download time. The original boundary snapshot checksum is `3419b3badc0e5d5f31ccbe784e1476be60bdff4182edb06090f5401185bbc779`.

The January 1 source has 7,293,408 rows. The regional canonical preparation accounted for every row: 235,713 canonical rows, 7 duplicates, and 7,057,688 outside the selected region. The canonical Parquet checksum is `4304d041cce2095508c0225f264e95481ae8bdf94218155c84af7a9df7662b93`. The full real pilot Spark batch yielded 370 visits, including 271 reconciled and 99 candidate visits. These figures were measured in the local prototype; the checkpoint command above tests only the committed excerpt and fixtures.

The included `real_ais_excerpt.jsonl` contains exactly two canonical rows from one NOAA vessel on January 1. Both fall in port-area ID `4120`; their observed span is 1,080 seconds. They produce one reconciled visit with both ends censored because the excerpt has no supporting outside observations. The excerpt SHA-256 is `7b972df16a1c7eb440f0aff39960af628da4d9a920b8fc23f54bb296cc652657`. The sample is public source data, not a synthetic vessel trajectory. The full 227 MB source and 36 MB canonical file remain local and outside Git to keep checkout and review small.

## What to show at the checkpoint

1. Open the [architecture diagram](week03_architecture.md) and identify batch authority, provisional Kafka path, HDFS, and MongoDB serving.
2. Show the source provenance and row accounting above, then open the two-row real excerpt and five-case synthetic fixture.
3. Run `PYTHONPATH=src python scripts/week04_checkpoint.py`; explain the physical sort, 6 fixture visits, and one censored real excerpt visit.
4. If HDFS is already configured on the laptop, repeat the [week-two storage smoke](week02_hadoop.md). This is a separate check; the checkpoint Spark demo does not use HDFS.

The full local prototype has later-stage processing and data, but this reviewed Git slice is bounded. No multi-node deployment, live AIS ingestion, or independent berth/call truth is demonstrated by this checkpoint.
