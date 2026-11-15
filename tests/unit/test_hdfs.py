from pathlib import Path

import pytest

from harbortrace.hdfs import Hdfs, HdfsError, parse_fsck, publish_file


@pytest.fixture(scope="module")
def local_hdfs(tmp_path_factory):
    """The real `hdfs dfs` client against a file:// store; needs Hadoop, not running daemons."""
    try:
        client = Hdfs.from_environment()
    except HdfsError as error:
        pytest.skip(str(error))
    root = tmp_path_factory.mktemp("hdfs-store")
    return Hdfs(client.binary, root.as_uri(), client.env)


def test_publish_uploads_then_reuses_identical_bytes(local_hdfs, tmp_path):
    local = tmp_path / "part.jsonl"
    local.write_bytes(b'{"mmsi": "123456789"}\n' * 1000)
    first = publish_file(local_hdfs, local, "/canonical/part.jsonl")
    assert not first["reused"] and first["sha256"] == first["remote_sha256"]
    assert first["bytes"] == local.stat().st_size
    second = publish_file(local_hdfs, local, "/canonical/part.jsonl")
    assert second["reused"] and second["remote_sha256"] == first["sha256"]


def test_publish_refuses_to_overwrite_different_bytes(local_hdfs, tmp_path):
    original, changed = tmp_path / "a.txt", tmp_path / "b.txt"
    original.write_text("original\n")
    changed.write_text("changed!\n")
    publish_file(local_hdfs, original, "/immutable/data.txt")
    with pytest.raises(HdfsError, match="Existing remote file differs"):
        publish_file(local_hdfs, changed, "/immutable/data.txt")
    assert publish_file(local_hdfs, original, "/immutable/data.txt")["reused"]


def test_java_home_is_found_without_the_variable(monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    try:
        client = Hdfs.from_environment()
    except HdfsError as error:
        pytest.skip(str(error))
    assert (Path(client.env["JAVA_HOME"]) / "bin" / "java").is_file()


def test_missing_java_is_an_hdfs_error_not_a_failed_command(monkeypatch):
    import harbortrace.hdfs as module
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(module, "_runs_java", lambda home: False)
    monkeypatch.setattr(Path, "is_file", lambda self: True)  # pretend Hadoop is installed
    with pytest.raises(HdfsError, match="No Java runtime"):
        Hdfs.from_environment()


def test_fsck_report_is_parsed():
    text = """FSCK started by user for path /x
/x/a.parquet 300 bytes, replicated: replication=2, 2 block(s):  OK
0. BP-1:blk_1 len=200 Live_repl=2  [DatanodeInfoWithStorage[127.0.0.1:9866,DS-a,DISK], DatanodeInfoWithStorage[127.0.0.1:9876,DS-b,DISK]]
1. BP-1:blk_2 len=100 Live_repl=1  [DatanodeInfoWithStorage[127.0.0.1:9876,DS-b,DISK]]

Status: HEALTHY
 Number of data-nodes:	2
 Total files:	1
 Under-replicated blocks:	1 (50.0 %)
 Missing blocks:		0
 Corrupt blocks:		0
"""
    assert parse_fsck(text) == {
        "status": "HEALTHY", "files": 1, "blocks": 2, "datanodes": 2, "live_replicas": {"1": 1, "2": 1},
        "replicas_per_datanode": {"127.0.0.1:9866": 1, "127.0.0.1:9876": 2},
        "under_replicated": 1, "missing_blocks": 0, "corrupt_blocks": 0}
