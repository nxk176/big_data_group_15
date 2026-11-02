import pytest

from harbortrace.hdfs import Hdfs, HdfsError, publish_file


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
