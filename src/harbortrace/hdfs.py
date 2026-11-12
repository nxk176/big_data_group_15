"""Immutable HDFS publication with byte-for-byte readback verification.

Wraps the `hdfs dfs` command line so the same code works against a local
single-node store, a remote cluster, or a `file://` URI in tests.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from .ingest import sha256_file


class HdfsError(RuntimeError):
    pass


@dataclass(frozen=True)
class Hdfs:
    binary: Path
    uri: str
    env: dict = field(default_factory=dict)

    @classmethod
    def from_environment(cls, uri: str | None = None) -> "Hdfs":
        home = os.environ.get("HADOOP_HOME") or str(Path.home() / ".local/harbortrace/hadoop-3.5.0")
        binary = Path(home) / "bin" / "hdfs"
        if not binary.is_file():
            raise HdfsError(f"hdfs command not found at {binary}; set HADOOP_HOME")
        conf = os.environ.get("HADOOP_CONF_DIR") or str(Path(__file__).resolve().parents[2] / "deploy/native")
        return cls(binary, (uri or os.environ.get("HDFS_URI") or "hdfs://127.0.0.1:9000").rstrip("/"),
                   {"HADOOP_HOME": home, "HADOOP_CONF_DIR": conf})

    def url(self, path: str) -> str:
        if "://" in path:
            return path
        return self.uri + "/" + path.lstrip("/")

    def _environment(self) -> dict:
        return {**os.environ, **self.env}

    def dfs(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        result = subprocess.run([str(self.binary), "dfs", *args], capture_output=True, text=True,
                                env=self._environment(), check=False)
        if check and result.returncode != 0:
            raise HdfsError(f"hdfs dfs {' '.join(args)} failed ({result.returncode}): {result.stderr.strip()[-500:]}")
        return result

    def exists(self, path: str) -> bool:
        return self.dfs("-test", "-e", self.url(path), check=False).returncode == 0

    def stat(self, path: str) -> dict:
        # %b bytes, %r replication, %o block size
        values = self.dfs("-stat", "%b %r %o", self.url(path)).stdout.split()
        return {"bytes": int(values[0]), "replication": int(values[1]), "block_size": int(values[2])}

    def sha256(self, path: str) -> str:
        """Stream the stored bytes back; stdout must carry file bytes only."""
        digest = hashlib.sha256()
        process = subprocess.Popen([str(self.binary), "dfs", "-cat", self.url(path)], stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=self._environment())
        for chunk in iter(lambda: process.stdout.read(1024 * 1024), b""):
            digest.update(chunk)
        stderr = process.stderr.read().decode(errors="replace")
        if process.wait() != 0:
            raise HdfsError(f"hdfs dfs -cat {path} failed: {stderr.strip()[-500:]}")
        return digest.hexdigest()


def publish_file(hdfs: Hdfs, local: Path, remote: str) -> dict:
    """Upload once, then verify bytes and SHA-256 against the local file.

    An existing remote file is reused only when its bytes match; a different
    file at the same path is an error, never an overwrite.
    """
    local = Path(local)
    if not local.is_file():
        raise FileNotFoundError(local)
    local_bytes, local_sha = local.stat().st_size, sha256_file(local)
    target = hdfs.url(remote)
    reused = hdfs.exists(target)
    if not reused:
        temporary = f"{target}.upload-{os.getpid()}"
        if hdfs.exists(temporary):
            raise HdfsError(f"Unfinished upload exists: {temporary}")
        hdfs.dfs("-mkdir", "-p", target.rsplit("/", 1)[0])
        hdfs.dfs("-put", str(local), temporary)
        hdfs.dfs("-mv", temporary, target)
    remote_stat, remote_sha = hdfs.stat(target), hdfs.sha256(target)
    if remote_stat["bytes"] != local_bytes or remote_sha != local_sha:
        problem = "Existing remote file differs from" if reused else "Readback differs from"
        raise HdfsError(f"{problem} {local}: local {local_bytes}B {local_sha}, remote {remote_stat['bytes']}B {remote_sha}")
    return {"local": str(local), "remote": target, "bytes": local_bytes, "sha256": local_sha,
            "remote_sha256": remote_sha, "replication": remote_stat["replication"],
            "block_size": remote_stat["block_size"], "reused": reused}
