"""Regression tests for high-priority Kafka correctness fixes."""

from __future__ import annotations

import sys
import threading
import unittest
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from ray_dispatcher.models import DispatchRequest, KafkaSource, SourceKind
from ray_dispatcher.readers import KafkaPayloadReader
from ray_dispatcher.sources import SourceObserver


class _FakeKafkaError:
    _PARTITION_EOF = 1

    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return f"KafkaError({self._code})"


class _EofMessage:
    def error(self) -> _FakeKafkaError:
        return _FakeKafkaError(_FakeKafkaError._PARTITION_EOF)


class KafkaPayloadReaderEofTests(unittest.TestCase):
    def test_partition_eof_before_end_offset_raises(self) -> None:
        source = KafkaSource("events", ("broker",), "events")
        request = DispatchRequest(
            "fetch-1",
            "fetch:events",
            "events",
            SourceKind.KAFKA,
            0,
            1,
            partition=0,
            start_offset=0,
            end_offset=5,
            topic="events",
        )
        consumer = MagicMock()
        consumer.poll.return_value = _EofMessage()

        fake = ModuleType("confluent_kafka")
        fake.Consumer = MagicMock(return_value=consumer)  # type: ignore[attr-defined]
        fake.TopicPartition = (  # type: ignore[attr-defined]
            lambda topic, partition, offset: SimpleNamespace(
                topic=topic, partition=partition, offset=offset
            )
        )
        fake.KafkaError = _FakeKafkaError  # type: ignore[attr-defined]

        previous = sys.modules.get("confluent_kafka")
        sys.modules["confluent_kafka"] = fake
        try:
            reader = KafkaPayloadReader(timeout=0.1)
            with self.assertRaisesRegex(RuntimeError, "partition EOF"):
                reader.fetch(request, source)
            consumer.close.assert_called_once()
        finally:
            if previous is None:
                sys.modules.pop("confluent_kafka", None)
            else:
                sys.modules["confluent_kafka"] = previous


class SourceObserverLockTests(unittest.TestCase):
    def test_shared_consumer_ops_use_reentrant_lock(self) -> None:
        observer = SourceObserver()
        self.assertEqual(type(observer._kafka_lock), type(threading.RLock()))

        topic_md = SimpleNamespace(partitions={0: object()}, error=None)
        metadata = SimpleNamespace(topics={"events": topic_md})
        consumer = MagicMock()
        consumer.list_topics.return_value = metadata
        consumer.get_watermark_offsets.return_value = (1, 4)
        observer._kafka_consumers[(None, "broker")] = consumer

        fake = ModuleType("confluent_kafka")
        fake.KafkaException = RuntimeError  # type: ignore[attr-defined]
        fake.TopicPartition = MagicMock()  # type: ignore[attr-defined]

        previous = sys.modules.get("confluent_kafka")
        sys.modules["confluent_kafka"] = fake
        try:
            # Hold the lock while calling sync path to prove reentrancy.
            with observer._kafka_lock:
                result = observer._kafka_watermarks_sync(
                    KafkaSource("events", ("broker",), "events")
                )
        finally:
            if previous is None:
                sys.modules.pop("confluent_kafka", None)
            else:
                sys.modules["confluent_kafka"] = previous

        self.assertEqual({0: (1, 4)}, result)
        consumer.list_topics.assert_called_once()


if __name__ == "__main__":
    unittest.main()
