"""Unit tests for production PayloadReaders and merge_fetch_results."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from ray_dispatcher.models import (
    DispatchRequest,
    KafkaSource,
    PostgresCursor,
    PostgresSource,
    SourceKind,
)
from ray_dispatcher.readers import (
    CompositePayloadReader,
    KafkaPayloadReader,
    PostgresPayloadReader,
    merge_fetch_results,
)


class _FakeKafkaError:
    _PARTITION_EOF = 1

    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return f"KafkaError({self._code})"


class _Message:
    def __init__(self, offset: int, value: Any, *, error: Any = None) -> None:
        self._offset = offset
        self._value = value
        self._error = error

    def error(self) -> Any:
        return self._error

    def offset(self) -> int:
        return self._offset

    def value(self) -> Any:
        return self._value


def _install_module(name: str, module: ModuleType) -> ModuleType | None:
    previous = sys.modules.get(name)
    sys.modules[name] = module
    return previous


def _restore_module(name: str, previous: ModuleType | None) -> None:
    if previous is None:
        sys.modules.pop(name, None)
    else:
        sys.modules[name] = previous


def _install_confluent(consumer: Any) -> ModuleType | None:
    fake = ModuleType("confluent_kafka")
    fake.Consumer = MagicMock(return_value=consumer)  # type: ignore[attr-defined]
    fake.TopicPartition = (  # type: ignore[attr-defined]
        lambda topic, partition, offset: SimpleNamespace(
            topic=topic, partition=partition, offset=offset
        )
    )
    fake.KafkaError = _FakeKafkaError  # type: ignore[attr-defined]
    return _install_module("confluent_kafka", fake)


class MergeFetchResultsTests(unittest.TestCase):
    def test_merges_partition_payloads_by_source_id(self) -> None:
        merged = merge_fetch_results(
            ("orders", "orders", "payments"),
            [{"a": 1}],
            [{"a": 2}],
            [{"p": 1}],
        )
        self.assertEqual(
            {"orders": [{"a": 1}, {"a": 2}], "payments": [{"p": 1}]},
            merged,
        )

    def test_pads_missing_sources_when_fewer_payloads(self) -> None:
        merged = merge_fetch_results(("orders", "payments"), [{"a": 1}])
        self.assertEqual({"orders": [{"a": 1}], "payments": []}, merged)

    def test_rejects_extra_payloads(self) -> None:
        with self.assertRaisesRegex(ValueError, "refusing to drop data"):
            merge_fetch_results(("orders",), [{"a": 1}], [{"a": 2}])

    def test_rejects_non_list_payload(self) -> None:
        with self.assertRaisesRegex(TypeError, "must be a list"):
            merge_fetch_results(("orders",), {"a": 1})


class KafkaPayloadReaderTests(unittest.TestCase):
    def test_reads_half_open_offset_range(self) -> None:
        source = KafkaSource("events", ("broker",), "events")
        request = DispatchRequest(
            "fetch-1",
            "fetch:events",
            "events",
            SourceKind.KAFKA,
            0,
            1,
            partition=0,
            start_offset=10,
            end_offset=13,
            topic="events",
        )
        consumer = MagicMock()
        consumer.poll.side_effect = [
            _Message(10, b"a"),
            _Message(11, b"b"),
            _Message(12, b"c"),
        ]
        previous = _install_confluent(consumer)
        try:
            records = KafkaPayloadReader(timeout=0.1).fetch(request, source)
        finally:
            _restore_module("confluent_kafka", previous)
        self.assertEqual([b"a", b"b", b"c"], records)
        consumer.close.assert_called_once()

    def test_timeout_raises(self) -> None:
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
            end_offset=2,
            topic="events",
        )
        consumer = MagicMock()
        consumer.poll.return_value = None
        previous = _install_confluent(consumer)
        try:
            with self.assertRaisesRegex(TimeoutError, "timed out reading"):
                KafkaPayloadReader(timeout=0.1).fetch(request, source)
        finally:
            _restore_module("confluent_kafka", previous)

    def test_requires_kafka_bounds(self) -> None:
        reader = KafkaPayloadReader()
        source = KafkaSource("events", ("broker",), "events")
        request = DispatchRequest(
            "fetch-1", "fetch:events", "events", SourceKind.KAFKA, 0, 1
        )
        with self.assertRaises(ValueError):
            reader.fetch(request, source)


class PostgresPayloadReaderTests(unittest.TestCase):
    def test_builds_ordered_cursor_query(self) -> None:
        source = PostgresSource(
            "orders",
            "postgresql://db/app",
            "public.orders",
            "updated_at",
            "id",
        )
        start = PostgresCursor(datetime(2024, 1, 1, tzinfo=timezone.utc), 1)
        end = PostgresCursor(datetime(2024, 1, 2, tzinfo=timezone.utc), 9)
        request = DispatchRequest(
            "fetch-1",
            "fetch:orders",
            "orders",
            SourceKind.POSTGRES,
            0,
            1,
            start_cursor=start,
            end_cursor=end,
            table="public.orders",
        )
        connection = MagicMock()
        connection.fetch = AsyncMock(
            return_value=[
                {"id": 2, "updated_at": end.timestamp},
                {"id": 3, "updated_at": end.timestamp},
            ]
        )
        connection.close = AsyncMock()
        asyncpg = ModuleType("asyncpg")
        asyncpg.connect = AsyncMock(return_value=connection)  # type: ignore[attr-defined]
        previous = _install_module("asyncpg", asyncpg)
        try:
            records = PostgresPayloadReader(timeout=1.0).fetch(request, source)
        finally:
            _restore_module("asyncpg", previous)

        self.assertEqual(
            [
                {"id": 2, "updated_at": end.timestamp},
                {"id": 3, "updated_at": end.timestamp},
            ],
            records,
        )
        sql = connection.fetch.await_args.args[0]
        self.assertIn('FROM "public"."orders"', sql)
        self.assertIn('ORDER BY "updated_at", "id"', sql)
        self.assertEqual(
            (
                start.timestamp,
                start.primary_key,
                end.timestamp,
                end.primary_key,
            ),
            connection.fetch.await_args.args[1:5],
        )
        connection.close.assert_awaited()

    def test_rejects_call_inside_running_loop(self) -> None:
        source = PostgresSource(
            "orders", "postgresql://db/app", "orders", "updated_at", "id"
        )
        cursor = PostgresCursor(datetime(2024, 1, 1, tzinfo=timezone.utc), 1)
        request = DispatchRequest(
            "fetch-1",
            "fetch:orders",
            "orders",
            SourceKind.POSTGRES,
            0,
            1,
            start_cursor=cursor,
            end_cursor=cursor,
            table="orders",
        )
        asyncpg = ModuleType("asyncpg")
        asyncpg.connect = AsyncMock()  # type: ignore[attr-defined]
        previous = _install_module("asyncpg", asyncpg)

        async def _inside_loop() -> None:
            with self.assertRaisesRegex(RuntimeError, "running event loop"):
                PostgresPayloadReader().fetch(request, source)

        try:
            import asyncio

            asyncio.run(_inside_loop())
        finally:
            _restore_module("asyncpg", previous)


class CompositePayloadReaderTests(unittest.TestCase):
    def test_routes_by_source_kind(self) -> None:
        kafka = MagicMock()
        kafka.fetch.return_value = [1]
        postgres = MagicMock()
        postgres.fetch.return_value = [2]
        reader = CompositePayloadReader(kafka=kafka, postgres=postgres)
        kafka_source = KafkaSource("events", ("b",), "events")
        pg_source = PostgresSource(
            "orders", "postgresql://x", "orders", "updated_at", "id"
        )
        kafka_request = DispatchRequest(
            "k",
            "w",
            "events",
            SourceKind.KAFKA,
            0,
            1,
            partition=0,
            start_offset=0,
            end_offset=1,
        )
        pg_request = DispatchRequest(
            "p",
            "w",
            "orders",
            SourceKind.POSTGRES,
            0,
            1,
            start_cursor=PostgresCursor(datetime(2024, 1, 1, tzinfo=timezone.utc), 0),
            end_cursor=PostgresCursor(datetime(2024, 1, 1, tzinfo=timezone.utc), 1),
        )
        self.assertEqual([1], reader.fetch(kafka_request, kafka_source))
        self.assertEqual([2], reader.fetch(pg_request, pg_source))
        kafka.fetch.assert_called_once()
        postgres.fetch.assert_called_once()


if __name__ == "__main__":
    unittest.main()
