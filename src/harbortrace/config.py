"""Validated, explicitly selected local configuration."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    path: Path
    values: dict

    def __getitem__(self, key: str):
        return self.values[key]

    @property
    def data_dir(self) -> Path:
        return Path(self.values["data_dir"])

    @property
    def hash(self) -> str:
        return hashlib.sha256(json.dumps(self.values, sort_keys=True).encode()).hexdigest()


def load_config(path: str | Path | None = None) -> Config:
    candidate = path or os.environ.get("HARBORTRACE_CONFIG")
    if not candidate:
        raise ValueError("Select --config or HARBORTRACE_CONFIG explicitly")
    resolved = Path(candidate).resolve()
    values = json.loads(resolved.read_text(encoding="utf-8"))
    required = ("project_root", "data_dir", "ports", "confirm_minutes", "exit_minutes", "max_gap_minutes")
    absent = [key for key in required if key not in values]
    if absent:
        raise ValueError(f"Missing config keys: {absent}")
    if any(values[k] <= 0 for k in ("confirm_minutes", "exit_minutes", "max_gap_minutes")):
        raise ValueError("Visit timing thresholds must be positive")
    if Path(values["project_root"]).resolve() != resolved.parent.parent.resolve():
        raise ValueError("Config project_root must equal the workspace root")
    if not Path(values["data_dir"]).resolve().is_relative_to(Path(values["project_root"]).resolve()):
        raise ValueError("Project data_dir must be inside project_root")
    return Config(resolved, values)
