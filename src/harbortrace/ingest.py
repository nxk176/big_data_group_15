"""Immutable acquisition and bounded-memory regional canonicalization."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import __version__
from .config import Config
from .contracts import CONTRACT_VERSION, Observation
from .quality import canonicalize
from .spatial import assign, load_ports, region_bounds

AIS_BASE = "https://ocmgeodatastor1.blob.core.windows.net/marinecadastre/ais2024"
PORT_LAYER = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/Port_Statistical_Area/FeatureServer/0"


def session() -> requests.Session:
    client = requests.Session()
    adapter = HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]))
    client.mount("https://", adapter)
    return client


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def check_budget(config: Config, incoming_bytes: int = 0):
    root = Path(config["project_root"])
    free = shutil.disk_usage(root).free
    if free - incoming_bytes < config["minimum_free_bytes"]:
        raise RuntimeError(f"Free-space floor would be crossed: free={free}, incoming={incoming_bytes}")
    data = config.data_dir
    used = sum(path.stat().st_size for path in data.rglob("*") if path.is_file()) if data.exists() else 0
    if used + incoming_bytes > config["project_data_cap_bytes"]:
        raise RuntimeError(f"Project data cap would be crossed: used={used}, incoming={incoming_bytes}")
    return {"free_bytes": free, "project_data_bytes": used}


def download_ais(config: Config, day: str) -> dict:
    parsed = datetime.strptime(day, "%Y-%m-%d")
    if parsed.year != 2024:
        raise ValueError("This source profile is for 2024 daily AIS files")
    filename = f"ais-{day}.parquet"
    url = f"{AIS_BASE}/{filename}"
    destination = config.data_dir / "source" / filename
    manifest_path = destination.with_suffix(".manifest.json")
    if destination.exists() and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("sha256") == sha256_file(destination) and manifest.get("url") == url:
            return manifest
    client = session()
    head = client.head(url, timeout=30)
    head.raise_for_status()
    size = int(head.headers["Content-Length"])
    if size > config["download_cap_bytes"]:
        raise RuntimeError(f"Source is {size} bytes, above download cap {config['download_cap_bytes']}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    existing = partial.stat().st_size if partial.exists() else 0
    if existing > size:
        raise RuntimeError("Partial download exceeds advertised source size")
    check_budget(config, size - existing)
    headers = {"Range": f"bytes={existing}-"} if existing else {}
    with client.get(url, headers=headers, stream=True, timeout=(30, 120)) as response:
        response.raise_for_status()
        if existing and response.status_code != 206:
            existing = 0
        mode = "ab" if existing else "wb"
        with partial.open(mode) as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    if partial.stat().st_size != size:
        raise RuntimeError(f"Incomplete download: {partial.stat().st_size} != {size}")
    checksum = sha256_file(partial)
    os.replace(partial, destination)
    manifest = {"contract_version": CONTRACT_VERSION, "url": url, "path": str(destination),
                "bytes": size, "sha256": checksum, "etag": head.headers.get("ETag"),
                "content_md5": head.headers.get("Content-MD5"), "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "license": "CC0 1.0", "citation": "Martin et al. (2025), Nationwide Automatic Identification System 2024, NOAA OCM"}
    atomic_json(manifest_path, manifest)
    return manifest


def download_range(config: Config, start_day: str, days: int) -> dict:
    if not 1 <= days <= 14:
        raise ValueError("One bounded range may contain 1..14 days")
    start = datetime.strptime(start_day, "%Y-%m-%d")
    dates = [(start + timedelta(days=index)).strftime("%Y-%m-%d") for index in range(days)]
    if any(not day.startswith("2024-") for day in dates):
        raise ValueError("This source profile only accepts 2024 dates")
    client = session()
    new_bytes = 0
    for day in dates:
        destination = config.data_dir / "source" / f"ais-{day}.parquet"
        manifest = destination.with_suffix(".manifest.json")
        if destination.exists() and manifest.exists():
            recorded = json.loads(manifest.read_text(encoding="utf-8"))
            if recorded.get("sha256") == sha256_file(destination):
                continue
        head = client.head(f"{AIS_BASE}/ais-{day}.parquet", timeout=30)
        head.raise_for_status()
        partial = destination.with_suffix(".parquet.part")
        new_bytes += max(0, int(head.headers["Content-Length"]) - (partial.stat().st_size if partial.exists() else 0))
    if new_bytes > config["download_cap_bytes"]:
        raise RuntimeError(f"Range needs {new_bytes} new bytes, above configured cap {config['download_cap_bytes']}")
    check_budget(config, new_bytes)
    manifests = [download_ais(config, day) for day in dates]
    result = {"start_day": start_day, "days": days, "new_bytes_planned": new_bytes,
              "source_files": [{"url": item["url"], "path": item["path"], "sha256": item["sha256"],
                                "bytes": item["bytes"]} for item in manifests],
              "completed_at_utc": datetime.now(timezone.utc).isoformat()}
    atomic_json(config.data_dir / "source" / "ranges" / f"{start_day}_{days}.json", result)
    return result


def download_boundaries(config: Config) -> dict:
    destination = config.data_dir / "boundaries" / "usace_ports.geojson"
    manifest_path = destination.with_suffix(".manifest.json")
    if destination.exists() and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("sha256") == sha256_file(destination):
            load_ports(destination, config["ports"])
            return manifest
    client = session()
    metadata_response = client.get(PORT_LAYER, params={"f": "json"}, timeout=30)
    metadata_response.raise_for_status()
    metadata = metadata_response.json()
    if not metadata.get("advancedQueryCapabilities", {}).get("supportsPagination"):
        raise RuntimeError("Port layer does not report pagination support")
    object_id_field = metadata["objectIdField"]
    query_url = PORT_LAYER + "/query"
    common = {"where": "1=1", "f": "json"}
    count = client.get(query_url, params={**common, "returnCountOnly": "true"}, timeout=30).json()["count"]
    ids_response = client.get(query_url, params={**common, "returnIdsOnly": "true"}, timeout=30)
    ids_response.raise_for_status()
    ids = sorted(ids_response.json()["objectIds"])
    if len(ids) != count or len(ids) != len(set(ids)):
        raise RuntimeError(f"Port object ID count mismatch: count={count}, IDs={len(ids)}")
    features: list[dict] = []
    seen: set[int] = set()
    page_size = min(200, int(metadata["maxRecordCount"]))
    for offset in range(0, count, page_size):
        page_ids = ids[offset: offset + page_size]
        response = client.get(query_url, params={"objectIds": ",".join(map(str, page_ids)),
                          "outFields": f"{object_id_field},PORTIDPK,FEATURENAME,DATA_YEAR", "outSR": 4326,
                          "f": "geojson"}, timeout=120)
        response.raise_for_status()
        page = response.json()
        if page.get("type") != "FeatureCollection" or page.get("exceededTransferLimit"):
            raise RuntimeError("Incomplete or invalid port page")
        for feature in page["features"]:
            feature_id = int(feature.get("id", feature["properties"].get(object_id_field)))
            if feature_id in seen:
                raise RuntimeError(f"Duplicate port object ID {feature_id}")
            seen.add(feature_id)
            features.append(feature)
    if seen != set(ids):
        raise RuntimeError(f"Port snapshot incomplete: expected {count}, received {len(seen)}")
    snapshot = {"type": "FeatureCollection", "features": features}
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".geojson.tmp")
    temporary.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
    selected_ports = load_ports(temporary, config["ports"])
    os.replace(temporary, destination)
    manifest = {"contract_version": CONTRACT_VERSION, "url": PORT_LAYER, "sha256": sha256_file(destination),
                "path": str(destination), "object_id_field": object_id_field, "object_id_count": count,
                "selected_port_ids": list(map(str, config["ports"])), "out_sr": 4326,
                "selected_repairs": {p.port_id: {"reason": p.repair_reason, "area_change_fraction": p.area_change_fraction}
                                     for p in selected_ports if p.repair_reason},
                "source_crs_wkid": metadata["spatialReference"]["wkid"],
                "source_data_last_edit_ms": metadata.get("editingInfo", {}).get("dataLastEditDate"),
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(), "attribution": "US Army Corps of Engineers Ports layer (ID 0)"}
    atomic_json(manifest_path, manifest)
    return manifest


CANONICAL_SCHEMA = pa.schema([
    ("mmsi", pa.string()), ("event_time", pa.string()), ("longitude", pa.float64()), ("latitude", pa.float64()),
    ("source_file", pa.string()), ("source_row", pa.int64()), ("source_record_id", pa.string()),
    ("logical_id", pa.string()), ("replay_id", pa.string()), ("sog", pa.float64()),
    ("quality_flags", pa.list_(pa.string())), ("assignment", pa.string()), ("assignment_flags", pa.list_(pa.string())),
])


def _code_revision() -> str | None:
    result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def prepare(config: Config, source: Path | None = None, boundaries: Path | None = None) -> dict:
    source = source or config.data_dir / "source" / "ais-2024-01-01.parquet"
    boundaries = boundaries or config.data_dir / "boundaries" / "usace_ports.geojson"
    if not source.exists() or not boundaries.exists():
        raise FileNotFoundError("Download AIS and boundary snapshots before prepare")
    source_hash, boundary_hash = sha256_file(source), sha256_file(boundaries)
    destination = config.data_dir / "canonical" / (source.stem + "-regional.parquet")
    manifest_path = destination.with_suffix(".manifest.json")
    if destination.exists() and manifest_path.exists():
        old = json.loads(manifest_path.read_text(encoding="utf-8"))
        if old.get("source_sha256") == source_hash and old.get("boundary_sha256") == boundary_hash and old.get("config_sha256") == config.hash and old.get("sha256") == sha256_file(destination):
            return old
        raise RuntimeError("Canonical output already exists with different input/config; choose a new source or preserve it")
    check_budget(config, source.stat().st_size * 2)
    ports = load_ports(boundaries, config["ports"])
    bounds = region_bounds(ports, config["region_margin_degrees"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    working = config.data_dir / "working" / f"{source.stem}-{int(time.time())}.sqlite"
    working.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(working)
    connection.execute("CREATE TABLE observations (mmsi TEXT, event_time TEXT, logical_id TEXT, source_row INTEGER, payload TEXT)")
    accounting = Counter()
    quarantine_path = destination.with_suffix(".quarantine.jsonl")
    quarantine_temp = quarantine_path.with_suffix(quarantine_path.suffix + ".tmp")
    parquet = pq.ParquetFile(source)
    raw_schema = parquet.schema_arrow
    required = {"mmsi", "base_date_time", "geometry"}
    if not required.issubset(raw_schema.names):
        raise ValueError(f"Missing AIS columns: {required - set(raw_schema.names)}")
    columns = list(required) + (["sog"] if "sog" in raw_schema.names else [])
    source_row = 0
    with quarantine_temp.open("w", encoding="utf-8") as rejected:
        for batch in parquet.iter_batches(batch_size=20000, columns=columns):
            rows = batch.to_pylist()
            accepted = []
            for row in rows:
                accounting["source_rows"] += 1
                try:
                    item = canonicalize(row, source.name, source_row)
                except (ValueError, TypeError, OverflowError) as error:
                    accounting["invalid"] += 1
                    rejected.write(json.dumps({"source_file": source.name, "source_row": source_row, "reason": str(error)}) + "\n")
                    source_row += 1
                    continue
                source_row += 1
                if not (bounds[0] <= item.longitude <= bounds[2] and bounds[1] <= item.latitude <= bounds[3]):
                    accounting["outside_region"] += 1
                    continue
                port_id, flags = assign(item.longitude, item.latitude, ports, config["boundary_jitter_meters"])
                payload = item.as_json()
                payload["assignment"], payload["assignment_flags"] = port_id, flags
                accepted.append((item.mmsi, item.event_time.isoformat(), item.logical_id, item.source_row, json.dumps(payload)))
                accounting["region_rows"] += 1
            connection.executemany("INSERT INTO observations VALUES (?,?,?,?,?)", accepted)
            connection.commit()
        connection.execute("CREATE INDEX observation_time ON observations(mmsi,event_time,logical_id,source_row)")
        cursor = connection.execute("SELECT mmsi,event_time,logical_id,source_row,payload FROM observations ORDER BY mmsi,event_time,logical_id,source_row")
        writer = pq.ParquetWriter(destination.with_suffix(".parquet.tmp"), CANONICAL_SCHEMA, compression="snappy")
        output: list[dict] = []
        group_key = None
        group: list[tuple] = []

        def flush_group():
            if not group:
                return
            distinct = {row[2] for row in group}
            if len(distinct) > 1:
                accounting["conflict_rows"] += len(group)
                for row in group:
                    rejected.write(json.dumps({"source_file": source.name, "source_row": row[3], "reason": "same_time_position_conflict"}) + "\n")
                return
            output.append(json.loads(group[0][4]))
            accounting["canonical_rows"] += 1
            for row in group[1:]:
                accounting["duplicate_rows"] += 1
                rejected.write(json.dumps({"source_file": source.name, "source_row": row[3], "reason": "duplicate", "kept_source_row": group[0][3]}) + "\n")

        for row in cursor:
            key = row[0], row[1]
            if group_key is not None and key != group_key:
                flush_group()
                group = []
                if len(output) >= 10000:
                    writer.write_table(pa.Table.from_pylist(output, schema=CANONICAL_SCHEMA))
                    output.clear()
            group_key = key
            group.append(row)
            if len(group) > 10000:
                raise RuntimeError(f"More than 10,000 records for one MMSI/time group: {group_key}; inspect source")
        flush_group()
        if output:
            writer.write_table(pa.Table.from_pylist(output, schema=CANONICAL_SCHEMA))
        writer.close()
    connection.close()
    os.replace(destination.with_suffix(".parquet.tmp"), destination)
    os.replace(quarantine_temp, quarantine_path)
    accounting["accounted_rows"] = accounting["invalid"] + accounting["outside_region"] + accounting["conflict_rows"] + accounting["duplicate_rows"] + accounting["canonical_rows"]
    if accounting["accounted_rows"] != accounting["source_rows"]:
        raise RuntimeError(f"Row accounting failure: {accounting}")
    manifest = {"contract_version": CONTRACT_VERSION, "source": str(source), "source_sha256": source_hash,
                "boundary": str(boundaries), "boundary_sha256": boundary_hash, "config_sha256": config.hash,
                "code_revision": _code_revision(), "harbortrace_version": __version__, "seed": config["seed"],
                "schema": str(raw_schema), "geo_metadata": raw_schema.metadata.get(b"geo", b"").decode(errors="replace") if raw_schema.metadata else "",
                "canonical": str(destination), "sha256": sha256_file(destination), "quarantine": str(quarantine_path),
                "row_accounting": dict(accounting), "prepared_at_utc": datetime.now(timezone.utc).isoformat()}
    atomic_json(manifest_path, manifest)
    return manifest
