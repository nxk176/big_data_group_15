"""Immutable HDFS publication with byte-for-byte readback verification.

Wraps the `hdfs dfs` command line so the same code works against a local
single-node store, a remote cluster, or a `file://` URI in tests.
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from .ingest import sha256_file


class HdfsError(RuntimeError):
    pass


def _runs_java(home: str | Path) -> bool:
    java = Path(home) / "bin" / "java"
    return java.is_file() and subprocess.run([str(java), "-version"], capture_output=True,
                                             check=False).returncode == 0


def find_java_home() -> str | None:
    """Java home for Hadoop's launcher, which unlike Spark refuses to start without JAVA_HOME.

    Same order as deploy/native/env.sh, then the `java` on PATH (Ubuntu's
    /usr/bin/java links into the JDK; macOS's /usr/bin/java is only a stub).
    """
    candidates = [os.environ.get("JAVA_HOME")]
    if Path("/usr/libexec/java_home").is_file():
        found = subprocess.run(["/usr/libexec/java_home", "-v", "17"], capture_output=True, text=True, check=False)
        candidates.append(found.stdout.strip() if found.returncode == 0 else None)
    candidates += ["/opt/homebrew/opt/openjdk@17", "/usr/local/opt/openjdk@17", "/usr/lib/jvm/java-17-openjdk-amd64",
                   "/usr/lib/jvm/java-17-openjdk-arm64"]
    on_path = shutil.which("java")
    if on_path:
        candidates.append(str(Path(on_path).resolve().parents[1]))
    return next((str(home) for home in candidates if home and _runs_java(home)), None)


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
        java_home = find_java_home()
        if java_home is None:
            raise HdfsError("No Java runtime found for the hdfs command; install Java 17 or set JAVA_HOME")
        conf = os.environ.get("HADOOP_CONF_DIR") or str(Path(__file__).resolve().parents[2] / "deploy/native")
        return cls(binary, (uri or os.environ.get("HDFS_URI") or "hdfs://127.0.0.1:9000").rstrip("/"),
                   {"HADOOP_HOME": home, "HADOOP_CONF_DIR": conf, "JAVA_HOME": java_home})

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

    def configured(self, key: str) -> str:
        """A value from the cluster configuration (HADOOP_CONF_DIR), e.g. dfs.replication."""
        result = subprocess.run([str(self.binary), "getconf", "-confKey", key], capture_output=True, text=True,
                                env=self._environment(), check=False)
        if result.returncode != 0:
            raise HdfsError(f"hdfs getconf {key} failed: {result.stderr.strip()[-500:]}")
        return result.stdout.strip()

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


def parse_fsck(text: str) -> dict:
    """Summary and per-DataNode replica counts from `hdfs fsck <path> -files -blocks -locations`."""
    blocks, per_node, live = 0, {}, {}
    for line in text.splitlines():
        found = re.search(r"Live_repl=(\d+)", line)
        if not found:
            continue
        blocks += 1
        live[int(found.group(1))] = live.get(int(found.group(1)), 0) + 1
        for node in re.findall(r"DatanodeInfoWithStorage\[([^,\]]+)", line):
            per_node[node] = per_node.get(node, 0) + 1

    def number(label: str) -> int | None:
        found = re.search(rf"^\s*{re.escape(label)}:\s*(\d+)", text, re.MULTILINE)
        return int(found.group(1)) if found else None

    status = re.search(r"^Status: (\w+)", text, re.MULTILINE)
    return {"status": status.group(1) if status else None, "files": number("Total files"),
            "blocks": blocks, "datanodes": number("Number of data-nodes"),
            "live_replicas": {str(k): v for k, v in sorted(live.items())},
            "replicas_per_datanode": dict(sorted(per_node.items())),
            "under_replicated": number("Under-replicated blocks"), "missing_blocks": number("Missing blocks"),
            "corrupt_blocks": number("Corrupt blocks")}


def block_report(hdfs: Hdfs, path: str) -> dict:
    """Where every block of ``path`` lives, as the NameNode reports it."""
    result = subprocess.run([str(hdfs.binary), "fsck", hdfs.url(path).replace(hdfs.uri, "", 1) or "/",
                             "-files", "-blocks", "-locations"], capture_output=True, text=True,
                            env=hdfs._environment(), check=False)
    report = parse_fsck(result.stdout)
    if report["status"] is None:
        raise HdfsError(f"hdfs fsck {path} gave no status: {result.stderr.strip()[-500:]}")
    return report


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
