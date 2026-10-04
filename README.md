# HarborTrace

**Week 1 course milestone:** a proposal and reproducible local Spark smoke test for reconstructing observed vessel presence in the Los Angeles and Long Beach port areas from historical AIS data.

The [week-one proposal](docs/week01_proposal.md) states the problem, planned NOAA and USACE inputs, target Lambda architecture, and limitations. A visit will describe observed presence under explicit rules; it will not claim verified berth occupancy or true congestion.

## Run the week-one check

Use Ubuntu 24.04 under WSL2, Python 3.12, and Java 17:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-week01.txt
python scripts/spark_smoke.py
```

The expected output includes `range(5).count() = 5`. The check ran successfully with Spark 3.5.7 on the local laptop. Later commits will add the ingestion, reconstruction, streaming, storage, and dashboard implementation.

**History note:** a fuller prototype was built locally before this repository's first commit. This commit records the week-one deliverable, not the original authorship date of that prototype. Data files, environments, credentials, and generated reports are excluded from Git. Development used Codex assistance and requires team review.
