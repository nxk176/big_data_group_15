# Week 5: source preparation and spatial assignment

This release adds the stage before the [week-four reconstruction](week04_checkpoint.md). Inputs now contain vessel identity, time and coordinates, without prepared port labels. The program validates them, assigns a port using a geographic boundary, accounts for every row, preserves rejected records and feeds the resulting Parquet (column-oriented file format) into Apache Spark (a distributed processing engine).

## Run the reproducible example

Run in Ubuntu with Python 3.12 and Java 17, from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-week05.txt
PYTHONPATH=src python scripts/week05_ingestion.py
```

On the existing project laptop, use its installed environment instead:

```bash
cd /mnt/e/big_data
source ~/.venvs/harbortrace/bin/activate
export PYTHONPATH="$PWD/src"
python scripts/week05_ingestion.py
```

The committed [input](../tests/fixtures/week05_ingestion.json) is explicitly synthetic: a fictional square port and fourteen raw-shaped observations. The command creates a small source Parquet and GeoJSON (geographic JSON) boundary under a new `data/week05/fixture-<timestamp>/` directory. It needs no source download, Hadoop service, message broker or database. It does not alter earlier results.

Expected accounting:

| Category | Rows |
|---|---:|
| Source | 14 |
| Canonical (normalized observations retained) | 7 |
| Invalid identity, coordinate or time | 3 |
| Duplicate | 1 |
| Conflicting same-vessel/time positions | 2 |
| Outside the selected geographic region | 1 |

**7 + 3 + 1 + 2 + 1 = 14.** The seven retained observations include four assigned to the fictional port and three outside its boundary. Spark reconstructs two confirmed observed-presence records. One has entry and exit evidence; the other lacks both bounds and retains a boundary-proximity flag. Neither is a verified commercial port call.

## Processing and artifacts

1. Decode Point WKB (well-known binary geometry) into longitude/latitude; validate the MMSI (Maritime Mobile Service Identity, a nine-digit maritime identifier), time and speed.
2. Keep a region around the port so outside-port observations remain available for entry/exit intervals.
3. Assign a port with an exact point-in-polygon predicate. Boundary and overlap uncertainty remain explicit.
4. Use a bounded-memory file scan and a disk-backed index to order and group records. Retain one logical duplicate; reject all positions in a conflicting vessel/time group.
5. Save normalized observations, a quarantine (rejected-row audit) file and a manifest (input/run metadata record) with SHA-256 (Secure Hash Algorithm, 256-bit) checksums and row accounting.
6. Check repeated preparation reuses the immutable output, then run the same Spark adapter as week four. Compare the synthetic results with hand-specified expected counts, intervals and flags.

Each new run retains `canonical/*.parquet`, its manifest/quarantine, `visits.jsonl`, `spark-plan.txt` and `summary.json`. Only generated artifacts go under ignored `data/`; the input fixture and code are in Git (source version control).

The position validation and geographic preparation in this release use PyArrow, Shapely and a SQLite disk index. Spark performs reconstruction from the normalized Parquet; the release does not claim that geographic preparation is a distributed Spark job.

Run the published reference checks:

```bash
PYTHONPATH=src python -m pytest tests/unit/test_reference.py -q
```

## Run the reviewed real pilot

If the existing official snapshots are already present locally:

```bash
PYTHONPATH=src python scripts/week05_ingestion.py --real-pilot
```

This validates the reviewed source/boundary hashes, prepares a new isolated result directory and processes the complete one-day source. It verifies 7,293,408 input rows, 235,713 regional normalized observations, seven duplicates and 7,057,688 outside-region rows, followed by 370 reconstructed visits: 271 confirmed by the rules and 99 candidates. The source file contains 227,181,435 bytes. Those are one-day figures, separate from the additional fourteen-day local experiment.

To explicitly acquire missing official snapshots before processing:

```bash
PYTHONPATH=src python scripts/week05_ingestion.py --download-pilot
```

The download path preserves source bytes, checks lengths/checksums, retries transient failures and supports resuming a partial download. Existing matching snapshots are reused. The program applies download/disk limits and rejects a pilot differing from the reviewed hashes; updated provider data needs an explicit review rather than silently changing expected results. The real modes take longer than the fixture and retain their new output directories.

Providers are NOAA (National Oceanic and Atmospheric Administration) [2024 AIS (Automatic Identification System) positions](https://github.com/ocm-marinecadastre/ais-vessel-traffic/blob/main/data/ais-broadcast-points-2024-readme.md) and the USACE (U.S. Army Corps of Engineers) [Ports layer, ID 0](https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/Port_Statistical_Area/FeatureServer/0). The provider's source product already uses GeoParquet/Snappy compression and additional cleaning; it is source data for this pipeline, not untouched radio traffic.

The selected boundary vintage is 2020 versus 2024 observations. The Los Angeles polygon needs topology repair, with about 8.89% planar-coordinate-area change; affected assignments are flagged. Reconstruction is an engineering result under explicit thresholds, not independently verified berth occupancy or actual congestion.

## Milestone boundary

Release verification on 1 November 2026 ran the fixture and all eleven reference checks both in the workspace and from an export containing only the staged Git files. The existing week-four demo also passed. A new isolated full-pilot preparation and Spark run matched the one-day accounting and 370-visit reference above. These checks used the installed Ubuntu environment; they do not claim a fresh cloud deployment or independent validation of real port calls.

This is a reviewed weekly release of an existing local prototype plus a new reproducible ingestion demonstration. It advances the Lambda architecture (historical batch plus provisional streaming) by making raw-shaped input preparation executable from a checkout. It does not yet publish the whole local streaming/serving prototype, establish distributed scale-out, provision cloud services or change any active database generation. Final deployment, analytics and evaluation requirements remain separate milestones.
