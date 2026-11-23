"""Spark preparation equals the single-process reference on the synthetic week-five source."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from harbortrace.engine import engine_session  # noqa: E402
from harbortrace.ingest import prepare  # noqa: E402
from harbortrace.spark_prepare import PrepareError, compare_with_reference, prepare_spark, read_text  # noqa: E402
from week05_ingestion import FIXTURE, configuration, write_fixture  # noqa: E402

SPEC = json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = engine_session("harbortrace-prepare-tests", shuffle_partitions=2,
                             warehouse=str(tmp_path_factory.mktemp("wh")))
    yield session
    session.stop()


def test_fixture_equals_reference(spark, tmp_path):
    source, boundaries = write_fixture(tmp_path, SPEC)
    reference = prepare(configuration(tmp_path, [9001]), source=source, boundaries=boundaries)
    output = str(tmp_path / "spark")
    manifest = prepare_spark(spark, [source], boundaries, output, ports=["9001"], kind="synthetic")
    comparison = compare_with_reference(spark, output, reference)
    assert comparison["result"] == "EQUAL", comparison
    assert manifest["row_accounting"] == {**SPEC["expected_accounting"], "region_rows": 10, "accounted_rows": 14,
                                          "source_rows": 14}
    assert manifest["days"] == {"2024-01-01": 7}
    assert json.loads(read_text(spark, f"{output}/_manifest.json")) == manifest
    assert sorted(path.name for path in Path(output).iterdir() if not path.name.startswith(".")) == [
        "_SUCCESS", "_manifest.json", "_quarantine", "day=2024-01-01"]


def test_several_sources_split_by_day_and_output_is_immutable(spark, tmp_path):
    first, boundaries = write_fixture(tmp_path / "a", SPEC)
    later = {**SPEC, "rows": [{**row, "base_date_time": row["base_date_time"].replace("2024-01-01", "2024-01-02")}
                              if row["base_date_time"] else row for row in SPEC["rows"]]}
    second, _ = write_fixture(tmp_path / "b", later)
    renamed = second.with_name("week05-fixture-day2.parquet")
    second.rename(renamed)
    output = str(tmp_path / "spark")
    manifest = prepare_spark(spark, [first, renamed], boundaries, output, ports=["9001"], kind="synthetic")
    assert manifest["days"] == {"2024-01-01": 7, "2024-01-02": 7}
    assert [source["rows"] for source in manifest["sources"]] == [14, 14]
    assert manifest["row_accounting"]["canonical_rows"] == 14
    identities = spark.read.parquet(output).select("source_file", "source_record_id").distinct().count()
    assert identities == 14  # the same rows in two files keep distinct source identities
    with pytest.raises(PrepareError, match="immutable"):
        prepare_spark(spark, [first], boundaries, output, ports=["9001"])
    assert not (tmp_path / ".spark.partial").exists() and not (tmp_path / ".spark.staging").exists()



def test_same_rows_in_two_files_are_kept_once(spark, tmp_path):
    """Identical rows at the same row index of two files: one copy is canonical, the other a duplicate."""
    first, boundaries = write_fixture(tmp_path / "a", SPEC)
    second, _ = write_fixture(tmp_path / "b", SPEC)
    copy = second.with_name("week05-fixture-copy.parquet")
    second.rename(copy)
    output = str(tmp_path / "spark")
    manifest = prepare_spark(spark, [first, copy], boundaries, output, ports=["9001"], kind="synthetic")
    accounting = manifest["row_accounting"]
    assert (accounting["canonical_rows"], accounting["duplicate_rows"], accounting["conflict_rows"]) == (7, 9, 4)
    assert accounting["accounted_rows"] == accounting["source_rows"] == 28
    canonical = spark.read.parquet(output)
    assert canonical.select("logical_id").distinct().count() == canonical.count() == 7
    duplicates = [row.asDict() for row in spark.read.json(f"{output}/_quarantine").where("reason = 'duplicate'").collect()]
    assert len(duplicates) == 9
    kept = {(row["source_file"], row["source_row"]) for row in canonical.select("source_file", "source_row").collect()}
    assert all((row["kept_source_file"], row["kept_source_row"]) in kept for row in duplicates)


def test_replay_input_reads_a_spark_prepare_directory(spark, tmp_path):
    from data_storage import read_canonical
    source, boundaries = write_fixture(tmp_path, SPEC)
    reference = prepare(configuration(tmp_path, [9001]), source=source, boundaries=boundaries)
    output = tmp_path / "spark"
    prepare_spark(spark, [source], boundaries, str(output), ports=["9001"], kind="synthetic")
    rows, manifest = read_canonical(output)
    expected, _ = read_canonical(Path(reference["canonical"]))
    key = lambda row: row["source_record_id"]  # noqa: E731
    assert sorted(rows, key=key) == sorted(expected, key=key)
    assert manifest["row_accounting"]["canonical_rows"] == 7
