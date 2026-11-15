"""Publish replay messages to Kafka and verify what the topic retains.

The publisher streams JSONL replay files line by line, so memory does not grow
with the number of messages beyond the replay tally. Its report records the
offset range each partition received; the diagnostic consumer reads exactly
those ranges back and compares counts and the replay-ID multiset digest.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .contracts import CONTRACT_VERSION
from .ingest import sha256_file
from .replay import ReplayTally, message_key, validate_message

# murmur2 is the Java client's partitioner, so Spark or any Java producer maps a key to the same partition.
PRODUCER_DEFAULTS = {"enable.idempotence": True, "acks": "all", "partitioner": "murmur2_random",
                     "linger.ms": 20, "compression.type": "lz4"}


class KafkaReplayError(RuntimeError):
    pass


def replay_files(paths: Iterable[Path | str]) -> list[Path]:
    """JSONL files in a stable order: a directory contributes its *.jsonl files sorted by name."""
    files: list[Path] = []
    for path in map(Path, paths):
        files.extend(sorted(path.glob("*.jsonl")) if path.is_dir() else [path])
    if not files or not all(path.is_file() for path in files):
        raise FileNotFoundError(f"No replay JSONL files in {[str(path) for path in paths]}")
    return files


def read_messages(files: list[Path]) -> Iterator[tuple[Path, int, dict]]:
    for path in files:
        with path.open(encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                if line.strip():
                    yield path, number, json.loads(line)


def ensure_topic(admin, topic: str, partitions: int, timeout: float = 30) -> dict:
    """Create the topic once with unlimited retention; an existing topic must match the partition count."""
    from confluent_kafka.admin import NewTopic
    existing = admin.list_topics(timeout=timeout).topics.get(topic)
    if existing is None or existing.error is not None:
        future = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1,
                                               config={"retention.ms": "-1", "cleanup.policy": "delete"})])[topic]
        future.result(timeout)
        # Creation returns before every partition has a leader; offset queries fail until then.
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            metadata = admin.list_topics(topic, timeout=timeout).topics.get(topic)
            if (metadata is not None and metadata.error is None and len(metadata.partitions) == partitions
                    and all(item.leader >= 0 and item.error is None for item in metadata.partitions.values())):
                return {"topic": topic, "partitions": partitions, "created": True}
            time.sleep(0.2)
        raise KafkaReplayError(f"Topic {topic} partitions have no leader after {timeout} s")
    if len(existing.partitions) != partitions:
        raise KafkaReplayError(f"Topic {topic} has {len(existing.partitions)} partitions, expected {partitions}")
    return {"topic": topic, "partitions": partitions, "created": False}


def watermarks(consumer, topic: str, partition: int, timeout: float = 30) -> tuple[int, int]:
    """Low and high offsets, retrying while a client's metadata still predates a new topic or leader."""
    from confluent_kafka import KafkaError, KafkaException, TopicPartition
    transient = {KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE,
                 KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_PARTITION, KafkaError._UNKNOWN_TOPIC}
    deadline = time.monotonic() + timeout
    while True:
        try:
            return consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)
        except KafkaException as error:
            if error.args[0].code() not in transient or time.monotonic() > deadline:
                raise
            time.sleep(0.5)


def retained_messages(consumer, topic: str, partitions: int, timeout: float = 30) -> int:
    return sum(high - low for low, high in (watermarks(consumer, topic, partition, timeout)
                                            for partition in range(partitions)))


def publish(files: list[Path], topic: str, producer, *, progress_every: int = 0) -> dict:
    """Send every message keyed by MMSI and account for each delivery report."""
    tally = ReplayTally()
    ranges: dict[int, list[int]] = {}
    key_partition: dict[str, int] = {}
    failures: list[str] = []
    counts = {"delivered": 0, "failed": 0, "key_moved": 0}

    def delivered(error, message):
        if error is not None:
            counts["failed"] += 1
            if len(failures) < 5:
                failures.append(str(error))
            return
        counts["delivered"] += 1
        partition, offset = message.partition(), message.offset()
        span = ranges.setdefault(partition, [offset, offset, 0])
        span[0], span[1], span[2] = min(span[0], offset), max(span[1], offset), span[2] + 1
        key = message.key().decode()
        if key_partition.setdefault(key, partition) != partition:
            counts["key_moved"] += 1

    started = time.monotonic()
    for path, number, message in read_messages(files):
        try:
            tally.add(message)
        except (KeyError, ValueError) as error:
            raise KafkaReplayError(f"{path}:{number}: {error}") from error
        value = json.dumps(message, sort_keys=True, separators=(",", ":")).encode()
        while True:
            try:
                producer.produce(topic, key=message_key(message).encode(), value=value, on_delivery=delivered)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        if progress_every and tally.messages % progress_every == 0:
            print(f"  sent {tally.messages:,} messages", flush=True)
    remaining = producer.flush(120)
    seconds = time.monotonic() - started
    if remaining:
        counts["failed"] += remaining
        failures.append(f"{remaining} messages still queued after flush")
    return {"contract_version": CONTRACT_VERSION, "topic": topic,
            "files": [{"path": str(path), "sha256": sha256_file(path)} for path in files],
            "sent": tally.messages, **counts, "failures": failures,
            "offsets": {str(p): {"first": span[0], "last": span[1], "messages": span[2]}
                        for p, span in sorted(ranges.items())},
            "tally": tally.summary(), "replay_id_digest": tally.digest(),
            "seconds": round(seconds, 3), "messages_per_second": round(tally.messages / seconds) if seconds else None,
            "published_at_utc": datetime.now(timezone.utc).isoformat()}


def verify(report: dict, consumer, *, timeout: float = 60) -> dict:
    """Read back the offset ranges a publish report recorded and compare them with it."""
    from confluent_kafka import KafkaError, TopicPartition
    topic, run_ids = report["topic"], set(report["tally"]["run_ids"])
    ranges = {int(p): span for p, span in report["offsets"].items()}
    problems: list[str] = []
    retained = {}
    for partition, span in ranges.items():
        low, high = watermarks(consumer, topic, partition, timeout)
        retained[str(partition)] = {"low": low, "high": high}
        if low > span["first"] or high <= span["last"]:
            problems.append(f"partition {partition} no longer retains offsets {span['first']}..{span['last']} "
                            f"(log holds {low}..{high - 1})")
    consumer.assign([TopicPartition(topic, partition, span["first"]) for partition, span in ranges.items()])
    pending = {partition: span["last"] for partition, span in ranges.items()}
    tally = ReplayTally()
    counts = {"read": 0, "foreign": 0, "invalid": 0, "wrong_key": 0, "key_moved": 0, "out_of_order": 0}
    key_partition: dict[str, int] = {}
    last_event: dict[str, str] = {}
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                problems.append(f"consumer error: {message.error()}")
            continue
        partition, offset = message.partition(), message.offset()
        if partition not in pending or offset > pending[partition]:
            continue
        deadline = time.monotonic() + timeout
        if offset == pending[partition]:
            del pending[partition]
        counts["read"] += 1
        try:
            value = json.loads(message.value())
            validate_message(value)
        except (KeyError, ValueError, TypeError):
            counts["invalid"] += 1
            continue
        if value["replay_run_id"] not in run_ids:
            counts["foreign"] += 1
            continue
        tally.add(value, validate=False)
        key = (message.key() or b"").decode()
        if key != message_key(value):
            counts["wrong_key"] += 1
        if key_partition.setdefault(key, partition) != partition:
            counts["key_moved"] += 1
        if value["event_time"] < last_event.get(key, ""):
            counts["out_of_order"] += 1
        last_event[key] = value["event_time"]
    if pending:
        problems.append(f"timed out before reaching the end of partitions {sorted(pending)}")
    received = tally.summary()
    if received["messages"] != report["delivered"]:
        problems.append(f"received {received['messages']} of {report['delivered']} delivered messages")
    if tally.digest() != report["replay_id_digest"]:
        problems.append("replay-ID multiset differs from the published one")
    if received != report["tally"]:
        problems.append("received accounting differs from the published accounting")
    for name in ("invalid", "wrong_key", "key_moved", "out_of_order"):
        if counts[name]:
            problems.append(f"{counts[name]} messages: {name.replace('_', ' ')}")
    if report["failed"]:
        problems.append(f"{report['failed']} messages were not delivered at publish time")
    return {"result": "FAIL" if problems else "PASS", "problems": problems, "topic": topic,
            "published": {"sent": report["sent"], "delivered": report["delivered"]},
            "received": received, "counts": counts, "retained": retained,
            "replay_id_digest": tally.digest(), "verified_at_utc": datetime.now(timezone.utc).isoformat()}


def kafka_clients(bootstrap: str):
    """Producer, admin client and a fresh, non-committing diagnostic consumer."""
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient
    common = {"bootstrap.servers": bootstrap}
    producer = Producer({**common, **PRODUCER_DEFAULTS, "client.id": "harbortrace-replay"})
    consumer = Consumer({**common, "group.id": f"harbortrace-diagnostic-{uuid.uuid4()}",
                         "enable.auto.commit": False, "enable.partition.eof": True,
                         "auto.offset.reset": "error"})
    return producer, AdminClient(common), consumer
