import json
import os
import uuid
from pathlib import Path

import pytest

pytest.importorskip("confluent_kafka")

from confluent_kafka import KafkaError  # noqa: E402

from harbortrace.kafka_replay import KafkaReplayError, kafka_clients, ensure_topic, publish, replay_files, verify  # noqa: E402
from harbortrace.replay import ReplayTally, account  # noqa: E402

FIXTURE = Path(__file__).parents[1] / "fixtures" / "canonical" / "week06_replay.jsonl"
MANIFEST = Path(__file__).parents[1] / "fixtures" / "canonical" / "week06_manifest.json"


class Record:
    def __init__(self, partition, offset, key, value, error=None):
        self._partition, self._offset, self._key, self._value, self._error = partition, offset, key, value, error

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def error(self):
        return self._error


class EndOfPartition:
    def code(self):
        return KafkaError._PARTITION_EOF


class FakeBroker:
    """In-memory topic: per-partition logs, a producer that acks on flush and a consumer."""

    def __init__(self, partitions=4, fail_keys=()):
        self.logs = {p: [] for p in range(partitions)}
        self.low = {p: 0 for p in range(partitions)}
        self.queue = []
        self.fail_keys = set(fail_keys)
        self.cursor = []

    # producer
    def produce(self, topic, key, value, on_delivery):
        self.queue.append((key, value, on_delivery))

    def poll(self, timeout=0):
        if self.cursor is not None and timeout and self.cursor:
            return self.cursor.pop(0)
        return None

    def flush(self, timeout=0):
        for key, value, callback in self.queue:
            if key in self.fail_keys:
                callback("simulated broker timeout", None)
                continue
            partition = sum(key) % len(self.logs)
            self.logs[partition].append((key, value))
            callback(None, Record(partition, len(self.logs[partition]) - 1, key, value))
        self.queue.clear()
        return 0

    # consumer
    def get_watermark_offsets(self, topic_partition, timeout=0):
        return self.low[topic_partition.partition], len(self.logs[topic_partition.partition])

    def assign(self, partitions):
        self.cursor = []
        for item in partitions:
            log = self.logs[item.partition]
            for offset in range(max(item.offset, self.low[item.partition]), len(log)):
                self.cursor.append(Record(item.partition, offset, *log[offset]))
            self.cursor.append(Record(item.partition, len(log), None, None, EndOfPartition()))


def roundtrip(broker, files=None):
    report = publish(files or [FIXTURE], "replay", broker)
    return report, verify(report, broker, timeout=2)


def test_fixture_roundtrip_accounts_for_every_message():
    broker = FakeBroker()
    report, check = roundtrip(broker)
    assert check["result"] == "PASS", check["problems"]
    expected = json.loads(MANIFEST.read_text())["replay"]["accounting"]
    assert report["tally"] == check["received"] == expected
    assert report["sent"] == report["delivered"] == 8 and report["failed"] == 0
    assert sum(span["messages"] for span in report["offsets"].values()) == 8
    assert check["counts"] == {"read": 8, "foreign": 0, "invalid": 0, "wrong_key": 0, "key_moved": 0, "out_of_order": 0}


def test_lost_message_is_reported():
    broker = FakeBroker()
    report = publish([FIXTURE], "replay", broker)
    partition = next(p for p, log in broker.logs.items() if len(log) > 1)
    broker.logs[partition][-1] = broker.logs[partition][-2]  # last message replaced by a repeat of the previous one
    check = verify(report, broker, timeout=2)
    assert check["result"] == "FAIL"
    assert any("multiset differs" in problem for problem in check["problems"])


def test_tampered_message_is_counted_invalid():
    broker = FakeBroker()
    report = publish([FIXTURE], "replay", broker)
    partition = next(p for p, log in broker.logs.items() if log)
    key, value = broker.logs[partition][0]
    broker.logs[partition][0] = (key, json.dumps({**json.loads(value), "copy_index": 7}).encode())
    check = verify(report, broker, timeout=2)
    assert check["result"] == "FAIL" and check["counts"]["invalid"] == 1
    assert any("received 7 of 8" in problem for problem in check["problems"])


def test_offsets_removed_by_retention_are_reported():
    broker = FakeBroker()
    report = publish([FIXTURE], "replay", broker)
    partition = next(p for p, log in broker.logs.items() if log)
    broker.low[partition] = 1
    check = verify(report, broker, timeout=2)
    assert check["result"] == "FAIL"
    assert any("no longer retains" in problem for problem in check["problems"])


def test_delivery_failures_are_counted():
    first_key = json.loads(FIXTURE.read_text().splitlines()[0])["mmsi"].encode()
    broker = FakeBroker(fail_keys={first_key})
    report, check = roundtrip(broker)
    assert report["failed"] > 0 and report["delivered"] + report["failed"] == report["sent"]
    assert report["failures"] == ["simulated broker timeout"] * min(report["failed"], 5)
    assert check["result"] == "FAIL"


def test_invalid_input_names_the_file_and_line(tmp_path):
    lines = FIXTURE.read_text().splitlines()
    bad = json.loads(lines[2])
    bad["replay_id"] = "0" * 64
    path = tmp_path / "bad.jsonl"
    path.write_text("\n".join(lines[:2] + [json.dumps(bad)]) + "\n")
    with pytest.raises(KafkaReplayError, match=r"bad.jsonl:3: replay_id"):
        publish([path], "replay", FakeBroker())


def test_directory_input_is_read_in_name_order(tmp_path):
    lines = FIXTURE.read_text().splitlines(keepends=True)
    (tmp_path / "b.jsonl").write_text("".join(lines[4:]))
    (tmp_path / "a.jsonl").write_text("".join(lines[:4]))
    assert [path.name for path in replay_files([tmp_path])] == ["a.jsonl", "b.jsonl"]
    _, check = roundtrip(FakeBroker(), replay_files([tmp_path]))
    assert check["result"] == "PASS"


def test_tally_matches_account_and_digest_ignores_order():
    messages = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
    forward, backward = ReplayTally(), ReplayTally()
    for message in messages:
        forward.add(message)
    for message in reversed(messages):
        backward.add(message)
    assert forward.summary() == backward.summary() == account(messages)
    assert forward.digest() == backward.digest()
    repeated = ReplayTally()
    for message in messages + messages[:1]:
        repeated.add(message)
    assert repeated.summary()["repeated_replay_ids"] == 1
    assert repeated.digest() != forward.digest()


@pytest.mark.skipif(not os.environ.get("HARBORTRACE_KAFKA_BOOTSTRAP"),
                    reason="set HARBORTRACE_KAFKA_BOOTSTRAP to run against a real broker")
def test_real_broker_roundtrip():
    producer, admin, consumer = kafka_clients(os.environ["HARBORTRACE_KAFKA_BOOTSTRAP"])
    topic = f"harbortrace.test.{uuid.uuid4().hex[:12]}"
    try:
        assert ensure_topic(admin, topic, 3)["created"]
        report = publish([FIXTURE], topic, producer)
        check = verify(report, consumer, timeout=30)
        assert check["result"] == "PASS", check["problems"]
        assert check["received"] == json.loads(MANIFEST.read_text())["replay"]["accounting"]
    finally:
        consumer.close()
        admin.delete_topics([topic])[topic].result(30)
