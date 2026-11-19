"""Runner exports preserve exact values and identify their validated input."""
import csv
import hashlib
from pathlib import Path

from harbortrace.serving import load_artifact
from scripts import analytics_run

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/serving/generation_a"


class CollectedFrame:
    columns = ['port,name', 'note', 'visits', 'anomaly']

    def __init__(self):
        self.collections = 0

    def collect(self):
        self.collections += 1
        assert self.collections == 1, "CSV summaries must reuse the exported rows"
        return [('Port, One', 'Line 1\n"Line 2"\r\nViệt Nam', 3, True),
                ('Port Two', None, 5, False)]


def test_csv_round_trips_commas_quotes_and_newlines(tmp_path):
    frame = CollectedFrame()
    target = tmp_path / 'table.csv'
    analytics_run.save_csv(frame, target)
    with target.open(encoding='utf-8', newline='') as handle:
        rows = list(csv.reader(handle))
    assert rows == [frame.columns, ['Port, One', 'Line 1\n"Line 2"\r\nViệt Nam', '3', 'True'],
                    ['Port Two', '', '5', 'False']]


def test_csv_summaries_reuse_the_exported_rows(tmp_path):
    result = analytics_run.save_csv(CollectedFrame(), tmp_path / 'table.csv', sum_columns=('visits', 'anomaly'))
    assert result == {'row_count': 2, 'sums': {'visits': 8, 'anomaly': 1}}


def test_artifact_provenance_retains_validated_manifest_and_generation():
    artifact = load_artifact(FIXTURE)
    provenance = analytics_run.input_provenance(list(artifact.visits), artifact=artifact)
    assert provenance['contract_version'] == '1.0'
    assert provenance['rule_version'] == 'visits-v1'
    assert provenance['run_id'] == 'synthetic-fixture-run-a'
    assert provenance['visits_sha256'] == '62893366e0cd1bd142ca74e2eff3dad53c6018be270207feb0acc9de94f62135'
    assert provenance['synthetic'] is True
    assert provenance['generation_id'] == artifact.generation_id
    assert provenance['created_at'] == '2026-11-02T00:00:00+00:00'
    assert provenance['source']['description'] == 'reference_cases.json events, two synthetic vessels per case'
    assert provenance['artifact_path'] == str(FIXTURE.resolve())


def test_synthetic_provenance_is_reproducible_and_seed_labelled():
    # Empty input avoids generating a large Spark fixture; the canonical empty JSONL hash is known.
    first = analytics_run.input_provenance([], seed=42, created_at='2026-11-20T00:00:00+00:00')
    second = analytics_run.input_provenance([], seed=42, created_at='2026-11-20T00:00:00+00:00')
    assert first == second
    assert first['synthetic'] is True
    assert first['visits_sha256'] == hashlib.sha256(b'').hexdigest()
    assert first['source']['generator_parameters'] == {'seed': 42, 'ports': 12, 'vessels': 60, 'days': 14}
    assert first['input_type'] == 'generated_synthetic'
