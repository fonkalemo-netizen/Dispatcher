from __future__ import annotations

import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import patch

from ray_dispatcher import (
    ExecutionResult,
    KafkaSource,
    PostgresCursor,
    PostgresSource,
    RayDispatcher,
    WorkerSpec,
)
from source_clients import KafkaPostgresSourceClient
from worker_discovery import WorkerDiscoveryError, discover_workers


class RecordingSourceClient:
    def __init__(self) -> None:
        self.kafka_calls: list[tuple[tuple[str, ...], str]] = []

    async def kafka_watermarks(self, source: KafkaSource):
        self.kafka_calls.append((source.brokers, source.topic))
        return {0: (0, 7), 1: (4, 10)}

    async def postgres_high_watermark(self, source: Any):
        raise AssertionError("not used")

    async def postgres_count(self, source: Any, start: Any, end: Any):
        raise AssertionError("not used")


class FakeRayBackend:
    def __init__(self) -> None:
        self.submissions: list[Any] = []
        self.ready: dict[str, ExecutionResult] = {}

    def submit(self, worker: WorkerSpec, request: Any) -> str:
        self.submissions.append(request)
        return f"ref-{len(self.submissions)}"

    def poll(self, refs: Mapping[str, Any]):
        outcomes = {
            run_id: self.ready.pop(ref)
            for run_id, ref in refs.items()
            if ref in self.ready
        }
        return outcomes

    def available_cpus(self) -> float:
        return 8.0

    def finish(self, ref: str, *, error: str | None = None) -> None:
        self.ready[ref] = ExecutionResult(
            success=error is None,
            value="ok" if error is None else None,
            error=error,
        )


class WorkerDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_multiple_handlers_share_observation_but_keep_independent_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "orders.py").write_text(
                '''
HANDLERS = [
    {
        "name": "to_jsonl",
        "entrypoint": "to_jsonl",
        "sources": [{
            "kind": "kafka",
            "source_id": "orders",
            "brokers": ["broker:9092"],
            "topic": "orders",
            "initial_offset": "earliest",
        }],
        "output": {
            "connection_id": "files",
            "target": "/tmp/jsonl",
            "output_format": "jsonl",
            "max_parallelism": 1,
        },
        "batch_size": 2,
        "max_parallelism": 4,
        "max_retries": 0,
    },
    {
        "name": "to_csv",
        "entrypoint": "to_csv",
        "sources": [{
            "kind": "kafka",
            "source_id": "orders",
            "brokers": ["broker:9092"],
            "topic": "orders",
            "initial_offset": "earliest",
        }],
        "output": {
            "connection_id": "files",
            "target": "/tmp/csv",
            "output_format": "csv",
            "max_parallelism": 2,
        },
        "batch_size": 2,
        "max_parallelism": 4,
        "max_retries": 0,
    },
]

def to_jsonl(request):
    return request.dispatch_id

def to_csv(request):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            source_client = RecordingSourceClient()
            backend = FakeRayBackend()
            dispatcher = RayDispatcher.from_worker_directory(
                directory, backend, source_client=source_client
            )

            backlog = await dispatcher.data_listener()
            run_ids = await dispatcher.ray_trigger()

            for run in dispatcher.state.runs.values():
                backend.finish(
                    run.ref,
                    error=(
                        "csv sink unavailable"
                        if run.request.handler_id.endswith("to_csv")
                        else None
                    ),
                )
            await dispatcher.ray_status()

        self.assertEqual(1, len(source_client.kafka_calls))
        self.assertEqual(
            {"orders:to_jsonl", "orders:to_csv"}, set(dispatcher.workers)
        )
        self.assertEqual(7, backlog["orders:to_jsonl:orders:0"])
        self.assertEqual(7, backlog["orders:to_csv:orders:0"])
        requests = [run.request for run in dispatcher.state.runs.values()]
        jsonl = [request for request in requests if request.handler_id.endswith("to_jsonl")]
        csv = [request for request in requests if request.handler_id.endswith("to_csv")]
        self.assertEqual(1, len(jsonl))
        self.assertEqual(2, len(csv))
        self.assertEqual(2, jsonl[0].end_offset - jsonl[0].start_offset)
        self.assertEqual(
            4,
            sum(request.end_offset - request.start_offset for request in csv),
        )
        self.assertEqual("jsonl", jsonl[0].output_format)
        self.assertEqual("/tmp/csv", csv[0].output_target)
        self.assertEqual(3, len(run_ids))
        self.assertEqual(
            2,
            dispatcher.state.sources["orders:to_jsonl:orders:0"].committed,
        )
        # Permanent csv failure skips its wave and unblocks the source.
        self.assertEqual(
            4,
            dispatcher.state.sources["orders:to_csv:orders:0"].committed,
        )
        self.assertIsNone(
            dispatcher.state.sources["orders:to_csv:orders:0"].active_batch_id
        )

    async def test_directory_metadata_drives_active_topic_observation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "events.py").write_text(
                '''
WORKER_CONFIG = {
    "name": "events-worker",
    "entrypoint": "process",
    "max_parallelism": 2,
    "batch_size": 10,
    "sources": [{
        "kind": "kafka",
        "source_id": "events-v1",
        "brokers": ["kafka-a:9092", "kafka-b:9092"],
        "topic": "events",
        "initial_offset": "earliest",
    }],
}

def process(request):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            source_client = RecordingSourceClient()
            dispatcher = RayDispatcher.from_worker_directory(
                directory,
                FakeRayBackend(),
                source_client=source_client,
            )

            backlog = await dispatcher.data_listener()

        self.assertEqual(
            [(('kafka-a:9092', 'kafka-b:9092'), 'events')],
            source_client.kafka_calls,
        )
        self.assertEqual(7, backlog["events-worker:events-v1:0"])
        self.assertEqual(6, backlog["events-worker:events-v1:1"])
        self.assertEqual("events", dispatcher.workers["events-worker"].sources[0].topic)

    def test_bad_worker_module_reports_its_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "bad.py")
            path.write_text("VALUE = 1\n", encoding="utf-8")
            with self.assertRaisesRegex(WorkerDiscoveryError, "bad.py"):
                discover_workers(directory)

    async def test_production_kafka_client_queries_every_discovered_partition(self) -> None:
        calls: list[tuple[str, int, bool]] = []

        class TopicPartition:
            def __init__(self, topic: str, partition: int) -> None:
                self.topic = topic
                self.partition = partition

        class Consumer:
            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def list_topics(self, topic: str, timeout: float):
                partition_metadata = {0: object(), 2: object()}
                topic_metadata = types.SimpleNamespace(
                    error=None, partitions=partition_metadata
                )
                return types.SimpleNamespace(topics={topic: topic_metadata})

            def get_watermark_offsets(
                self, topic_partition: TopicPartition, timeout: float, cached: bool
            ):
                calls.append(
                    (topic_partition.topic, topic_partition.partition, cached)
                )
                return (topic_partition.partition, 100 + topic_partition.partition)

            def close(self) -> None:
                pass

        fake_module = types.ModuleType("confluent_kafka")
        fake_module.Consumer = Consumer
        fake_module.KafkaException = RuntimeError
        fake_module.TopicPartition = TopicPartition
        source = KafkaSource("events", ("broker:9092",), "events")
        client = KafkaPostgresSourceClient()

        with patch.dict(sys.modules, {"confluent_kafka": fake_module}):
            result = await client.kafka_watermarks(source)
            await client.close()

        self.assertEqual({0: (0, 100), 2: (2, 102)}, result)
        self.assertEqual([("events", 0, False), ("events", 2, False)], calls)

    async def test_production_postgres_client_queries_cursor_and_count(self) -> None:
        captured: dict[str, Any] = {}
        start = PostgresCursor(datetime(2026, 1, 1, tzinfo=timezone.utc), 9)
        middle = start.timestamp + timedelta(milliseconds=500)
        end = PostgresCursor(start.timestamp + timedelta(seconds=1), 14)

        class Pool:
            async def fetchrow(self, sql: str, *, timeout: float):
                captured["watermark_sql"] = sql
                return {
                    "cursor_timestamp": end.timestamp,
                    "cursor_primary_key": end.primary_key,
                }

            async def fetchval(self, sql: str, *args: Any, timeout: float):
                captured["count_sql"] = sql
                captured["count_args"] = args
                return 3

            async def fetch(self, sql: str, *args: Any, timeout: float):
                captured["range_sql"] = sql
                captured["range_args"] = args
                return [
                    {
                        "cursor_timestamp": middle,
                        "cursor_primary_key": 11,
                        "row_number": 2,
                    },
                    {
                        "cursor_timestamp": end.timestamp,
                        "cursor_primary_key": 13,
                        "row_number": 4,
                    },
                    {
                        "cursor_timestamp": end.timestamp,
                        "cursor_primary_key": 14,
                        "row_number": 5,
                    },
                ]

            async def close(self) -> None:
                pass

        pool = Pool()

        async def create_pool(*args: Any, **kwargs: Any):
            return pool

        fake_module = types.ModuleType("asyncpg")
        fake_module.create_pool = create_pool
        source = PostgresSource(
            "orders",
            "postgresql://db/orders",
            "public.orders",
            "updated_at",
            "id",
            initial_cursor=start,
        )
        client = KafkaPostgresSourceClient()

        with patch.dict(sys.modules, {"asyncpg": fake_module}):
            observed = await client.postgres_high_watermark(source)
            count = await client.postgres_count(source, start, end)
            ranges = await client.postgres_ranges(source, start, end, 3, 2)
            await client.close()

        self.assertEqual(end, observed)
        self.assertEqual(3, count)
        self.assertIn('FROM "public"."orders"', captured["watermark_sql"])
        self.assertEqual(
            (start.timestamp, 9, end.timestamp, 14), captured["count_args"]
        )
        self.assertEqual([2, 2, 1], [item_count for _, _, item_count in ranges])
        self.assertEqual(start, ranges[0][0])
        self.assertEqual(end, ranges[-1][1])
        self.assertEqual(6, captured["range_args"][-2])
        self.assertEqual(2, captured["range_args"][-1])


if __name__ == "__main__":
    unittest.main()
