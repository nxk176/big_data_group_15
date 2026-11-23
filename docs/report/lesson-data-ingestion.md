# Lesson: Data Ingestion

Author: Do Dang Vu. Evidence: [data and infrastructure guide](../data_infrastructure.md), [week-five ingestion](../week05_ingestion.md), [k3s deployment](../deploy_k3s.md).

## Problem Description

HarborTrace ingests NOAA AIS (Automatic Identification System) position files and the USACE port-boundary layer. One day is a 227 MB GeoParquet file with 7,293,408 positions for the whole United States; the port layer has 370 polygons, some of them geometrically invalid. Before any visit can be reconstructed, every position must be

- validated (nine-digit MMSI, UTC time, finite coordinates, non-negative speed) and either kept or quarantined with a reason;
- given stable identities (`source_record_id` from file and row, `logical_id` from vessel, time and point) so reruns, replays and retries can be recognised;
- assigned to a port polygon with explicit uncertainty (overlap, repaired boundary, near boundary);
- deduplicated: identical positions kept once, conflicting positions for one vessel and time rejected;
- accounted for: canonical + invalid + duplicate + conflict + outside region = source rows.

Two delivery paths are needed: files for batch (fourteen days are about 106 million rows) and a replay stream into Kafka for the streaming engine. Both had to fit one laptop and later a Kubernetes node.

## Approaches Tried

| Approach | Result |
|---|---|
| Single-process preparation: PyArrow batches, Shapely point-in-polygon per row, SQLite disk index for duplicate groups (week five) | Correct and the row-for-row reference, but 268 s for one day with two ports; with all 368 usable ports the per-point test costs 33.9 s per 21,141 points, hours per day |
| `STRtree.query(points, predicate="covered_by")` | Slower than the scalar loop for the two large Los Angeles/Long Beach polygons: the predicate prepares the points, not the polygons |
| Spark job caching intermediate results with `persist()` | Worked for one day; on fifteen days (106,206,861 rows) it failed after 6.7 hours with `OutOfMemoryError` in Spark's memory store (8 GB heap) |
| Duplicate rule "keep the row with the smallest `source_row`" | Kept the same observation twice when it sat at the same row of two files (row numbers restart in every file); found by a unit test with the source written twice |
| Kafka publisher reading only `*.jsonl` and requiring sorted event time per vessel | Could not publish the engine's `bucket=*/part-*.txt` replay output, and flagged its unsorted buckets as failures |

## Final Solution

**Batch: `spark_prepare.prepare_spark`.** Spark reads the source with its file name and row index (`_metadata.file_name`, `_metadata.row_index`, equal to the PyArrow ordinal), so identities match the reference exactly. Rows are validated by the same `quality.canonicalize` inside `mapInArrow` batches. Ports come from `spatial.PortIndex`: STRtree candidates, then `covers` with prepared polygons and a vectorized boundary distance; it equals the per-point rule on every sampled point and is 170 times faster for all ports (0.20 s against 33.9 s). Duplicate and conflict groups (`mmsi`, `event_time`) are resolved with window functions, keeping the minimum `(source_file, source_row)`. Intermediate results are staged as Parquet next to the output instead of cached. The output is `day=YYYY-MM-DD` Parquet partitions, a quarantine with reasons and a manifest with source hashes and row accounting, published by rename and never overwritten.

| Run | Source rows | Canonical rows | Time | Check |
|---|---:|---:|---:|---|
| 1 Jan 2024, two ports | 7,293,408 | 235,713 | 26.5 s | equal to the single-process reference row for row, including quarantine (268 s) |
| 1 Jan 2024, 368 ports | 7,293,408 | 7,270,776 | 122 s | 20,000 sampled assignments equal to the per-point rule |
| 1–15 Jan 2024, 368 ports | 106,206,861 | 105,953,905 | 1,246 s | 3,036 duplicates + 530 conflicts + 249,390 outside the region; accounting closes |
| 1 Jan 2024 as Spark pods on k3s, to HDFS | 7,293,408 | 235,713 | 46.5 s | pinned counts; batch engine on the result gives the same `visits.jsonl` SHA-256 as locally |

**Streaming: replay into Kafka.** `replay_id = SHA256(run, source record, copy index)` marks transport retries without inventing vessel activity. The publisher streams any replay files line by line (including Spark bucket directories, in time order), keys by MMSI and uses an idempotent `acks=all` producer. Real day: 236,213 messages (500 seeded retry copies) at about 69,000 messages/s, every one delivered and read back.

**Provenance.** Source snapshots are pinned by SHA-256. A fresh download of the USACE layer on 15 November had different bytes (`81d652e9…` instead of `3419b3ba…`); the pipeline refused it, and only after it reproduced every pinned count was it documented, without changing the pinned hash.

## Key Takeaways

- Keep a slow, simple reference implementation and compare the fast one row for row. The vectorized assignment, the Spark preparation and the duplicate rule were all accepted on equality, and the duplicate bug was found by such a comparison.
- Identity design decides what deduplication can do: a row number is unique only within its file.
- Caching is not free at scale; staging to files trades a little disk I/O for bounded memory (6.7 h failure → 21 min).
- Account for every input row. A closing equation (here 106,206,861 = 105,953,905 + 3,036 + 530 + 249,390) catches silent loss immediately.
- Pin and verify sources by hash; a provider can change bytes without changing content, and that must be a reviewed decision, not a silent one.
- Limits: the all-port output is checked by accounting and sampling, not row for row (the reference is too slow); replay generation still holds one day in memory.
