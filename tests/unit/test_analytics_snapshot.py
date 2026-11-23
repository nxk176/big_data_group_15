"""Local Spark must read the same bytes that passed artifact validation."""
import shutil
from pathlib import Path

import pytest

from harbortrace import serving


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/serving/generation_a"


def test_snapshot_rejects_changes_since_validation(tmp_path):
    source = shutil.copytree(FIXTURE, tmp_path / "batch")
    artifact = serving.load_artifact(source)
    (source / "visits.jsonl").write_bytes(b"changed after validation\n")
    with pytest.raises(serving.ValidationError, match="SHA-256"):
        serving.snapshot_visit_file(artifact, tmp_path / "visits.jsonl")
