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
    ExecutionMode,
    ExecutionResult,
    KafkaSource,
    PostgresCursor,
    PostgresSource,
    RayDispatcher,
    SourceObserver,
    WorkerDiscoveryError,
    HandlerSpec,
    discover_workers,
)
from ray_dispatcher.registries import build_resource_registry, build_source_registry
from ray_dispatcher.resources import ResourceLoader


class RecordingSourceObserver:
    def __init__(self) -> None:
        self.kafka_calls: list[tuple[tuple[str, ...], str]] = []

    async def kafka_watermarks(self, source: KafkaSource):
        self.kafka_calls.append((source.brokers, source.topic))
        return {0: (0, 7), 1: (4, 10)}

    async def postgres_high_watermark(self, source: Any):
        raise AssertionError("not used")

    async def postgres_count(self, source: Any, start: Any, end: Any):
        raise AssertionError("not used")


class FakeRayAdapter:
    def __init__(self) -> None:
        self.submissions: list[Any] = []
        self.fetch_submissions: list[tuple[Any, Any, Any]] = []
        self.ready: dict[str, ExecutionResult] = {}
        self.values: dict[str, Any] = {}
        self.resource_loader = None

    def submit(self, worker: HandlerSpec, request: Any, data_ref: Any = None) -> str:
        self.submissions.append(request)
        return f"ref-{len(self.submissions)}"

    def submit_fetch(self, worker: HandlerSpec, request: Any, source: Any, **_: Any) -> str:
        self.fetch_submissions.append((worker, request, source))
        return f"fetch-ref-{len(self.fetch_submissions)}"

    def poll(self, refs: Mapping[str, Any]):
        outcomes = {
            run_id: self.ready.pop(ref)
            for run_id, ref in refs.items()
            if ref in self.ready
        }
        return outcomes

    def available_cpus(self) -> float:
        return 8.0

    async def get(self, ref: Any) -> Any:
        return self.values[ref]

    def finish(self, ref: str, *, error: str | None = None, value: Any = "ok") -> None:
        self.ready[ref] = ExecutionResult(
            success=error is None,
            value=value if error is None else None,
            error=error,
        )
        if error is None:
            self.values[ref] = value


ORDERS_REGISTRY = build_source_registry(
    {
        "orders": {
            "kind": "kafka",
            "brokers": ["broker:9092"],
            "topic": "orders",
            "initial_offset": "earliest",
        }
    }
)

EVENTS_REGISTRY = build_source_registry(
    {
        "events-v1": {
            "kind": "kafka",
            "brokers": ["kafka-a:9092", "kafka-b:9092"],
            "topic": "events",
            "initial_offset": "earliest",
        }
    }
)


class WorkerDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_multiple_handlers_auto_share_one_source_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "orders.py").write_text(
                '''
HANDLERS = [
    {
        "entrypoint": "to_jsonl",
        "sources": ["orders"],
        "output": {
            "path": "/tmp/jsonl",
        },
        "batch_size": 0,
        "max_retries": 0,
    },
    {
        "entrypoint": "to_csv",
        "sources": ["orders"],
        "output": {
            "path": "/tmp/csv",
        },
        "batch_size": 0,
        "max_retries": 0,
    },
]

def to_jsonl(request, records):
    return request.dispatch_id

def to_csv(request, records):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            source_observer = RecordingSourceObserver()

            async def single_partition(source):
                source_observer.kafka_calls.append((source.brokers, source.topic))
                return {0: (0, 7)}

            source_observer.kafka_watermarks = single_partition  # type: ignore[method-assign]
            backend = FakeRayAdapter()
            dispatcher = RayDispatcher(
                directory,
                ray_adapter=backend,
                source_registry=ORDERS_REGISTRY,
            )
            dispatcher.source_observer = source_observer

            backlog = await dispatcher.data_listener()
            fetch_ids = await dispatcher.ray_trigger()

            for run in dispatcher.state.runs.values():
                if run.kind != "fetch":
                    continue
                backend.finish(run.ref, value=[{"offset": 0}, {"offset": 1}])
            await dispatcher.ray_status()

            for run in dispatcher.state.runs.values():
                if run.kind != "handler":
                    continue
                backend.finish(
                    run.ref,
                    error=(
                        "csv sink unavailable"
                        if run.request.handler_id.endswith("to_csv")
                        else None
                    ),
                )
            await dispatcher.ray_status()

        self.assertEqual(1, len(source_observer.kafka_calls))
        self.assertEqual(
            {"orders:to_jsonl", "orders:to_csv"}, set(dispatcher.workers)
        )
        self.assertEqual({"shared:orders:0": 7}, backlog)
        self.assertEqual(1, len(fetch_ids))
        self.assertEqual(1, len(backend.fetch_submissions))
        handler_requests = [
            run.request
            for run in dispatcher.state.runs.values()
            if run.kind == "handler"
        ]
        jsonl = [
            request
            for request in handler_requests
            if request.handler_id.endswith("to_jsonl")
        ]
        csv = [
            request
            for request in handler_requests
            if request.handler_id.endswith("to_csv")
        ]
        self.assertEqual(1, len(jsonl))
        self.assertEqual(1, len(csv))
        self.assertEqual({"path": "/tmp/jsonl"}, dict(jsonl[0].output or {}))
        self.assertEqual("/tmp/csv", (csv[0].output or {})["path"])
        # Shared permanent failure advances the one source checkpoint.
        self.assertEqual(7, dispatcher.state.sources["shared:orders:0"].committed)
        self.assertIsNone(dispatcher.state.sources["shared:orders:0"].active_batch_id)

    async def test_directory_metadata_drives_active_topic_observation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "events.py").write_text(
                '''
HANDLERS = [
    {
        "handler_id": "events-worker",
        "entrypoint": "process",
        "batch_size": 10,
        "sources": ["events-v1"],
    },
]

def process(request, records):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            source_observer = RecordingSourceObserver()
            backend = FakeRayAdapter()
            dispatcher = RayDispatcher(
                directory,
                ray_adapter=backend,
                source_registry=EVENTS_REGISTRY,
            )
            dispatcher.source_observer = source_observer

            backlog = await dispatcher.data_listener()

        self.assertEqual(
            [(('kafka-a:9092', 'kafka-b:9092'), 'events')],
            source_observer.kafka_calls,
        )
        self.assertEqual(7, backlog["shared:events-v1:0"])
        self.assertEqual(6, backlog["shared:events-v1:1"])
        self.assertEqual(
            "events", dispatcher.workers["events-worker"].sources[0].topic
        )

    def test_modules_without_handlers_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "legacy.py").write_text(
                '''
WORKER_CONFIG = {
    "entrypoint": "process",
    "sources": ["orders"],
}

def process(request, records):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            Path(directory, "helpers.py").write_text(
                '''
SOURCES = {
    "shared-orders": {
        "kind": "kafka",
        "brokers": ["broker:9092"],
        "topic": "orders",
    }
}
RESOURCES = {
    "dim": {"kind": "static", "data": {1: {"name": "a"}}},
}
HANDLERS = []
''',
                encoding="utf-8",
            )
            Path(directory, "worker.py").write_text(
                '''
HANDLERS = [{
    "entrypoint": "process",
    "sources": ["shared-orders"],
    "resources": ["dim"],
}]

def process(request, records, resources):
    return resources["dim"]
''',
                encoding="utf-8",
            )
            workers, sources, resources = discover_workers(directory)
        self.assertEqual(1, len(workers))
        self.assertEqual("shared-orders", workers[0].sources[0].source_id)
        self.assertEqual("orders", sources["shared-orders"].topic)
        self.assertEqual({1: {"name": "a"}}, resources["dim"].data)

    def test_directory_with_only_handlerless_modules_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "bad.py").write_text("VALUE = 1\n", encoding="utf-8")
            Path(directory, "empty.py").write_text("HANDLERS = []\n", encoding="utf-8")
            with self.assertRaisesRegex(WorkerDiscoveryError, "no worker modules found"):
                discover_workers(directory)

    def test_resource_registry_resolves_named_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "enriched.py").write_text(
                '''
HANDLERS = [
    {
        "entrypoint": "process",
        "sources": ["orders"],
        "resources": ["user-dim-v1"],
    },
]

def process(request, records, resources):
    return resources["user-dim-v1"]
''',
                encoding="utf-8",
            )
            resource_registry = build_resource_registry(
                {
                    "user-dim-v1": {
                        "kind": "static",
                        "data": {1: {"name": "alice"}},
                    }
                }
            )
            workers, _, resources = discover_workers(
                directory,
                source_registry=ORDERS_REGISTRY,
                resource_registry=resource_registry,
            )
        self.assertEqual(("user-dim-v1",), workers[0].resource_ids)
        self.assertEqual("static", resources["user-dim-v1"].kind)

    def test_module_sources_and_resources_are_merged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "orders.py").write_text(
                '''
SOURCES = {
    "orders": {
        "kind": "kafka",
        "brokers": ["broker:9092"],
        "topic": "orders",
        "initial_offset": "earliest",
    }
}
RESOURCES = {
    "user-dim-v1": {
        "kind": "static",
        "data": {1: {"name": "alice"}},
    }
}
HANDLERS = [
    {
        "entrypoint": "process",
        "sources": ["orders"],
        "resources": ["user-dim-v1"],
    },
]

def process(request, records, resources):
    return resources["user-dim-v1"]
''',
                encoding="utf-8",
            )
            workers, sources, resources = discover_workers(directory)
        self.assertEqual(1, len(workers))
        self.assertEqual("orders", sources["orders"].topic)
        self.assertEqual({1: {"name": "alice"}}, resources["user-dim-v1"].data)
        loader = ResourceLoader(resources)
        self.assertEqual({1: {"name": "alice"}}, loader.get("user-dim-v1"))

    def test_equal_module_source_declarations_merge(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "a.py").write_text(
                '''
SOURCES = {
    "orders": {
        "kind": "kafka",
        "brokers": ["broker:9092"],
        "topic": "orders",
    }
}
HANDLERS = [{"entrypoint": "process", "sources": ["orders"]}]

def process(request, records):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            Path(directory, "b.py").write_text(
                '''
SOURCES = {
    "orders": {
        "kind": "kafka",
        "brokers": ["broker:9092"],
        "topic": "orders",
    }
}
HANDLERS = [{"entrypoint": "process", "sources": ["orders"]}]

def process(request, records):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            workers, sources, _ = discover_workers(directory)
        self.assertEqual(2, len(workers))
        self.assertEqual(["orders"], list(sources))

    def test_conflicting_module_source_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "a.py").write_text(
                '''
SOURCES = {
    "orders": {
        "kind": "kafka",
        "brokers": ["broker:9092"],
        "topic": "orders",
    }
}
HANDLERS = [{"entrypoint": "process", "sources": ["orders"]}]

def process(request, records):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            Path(directory, "b.py").write_text(
                '''
SOURCES = {
    "orders": {
        "kind": "kafka",
        "brokers": ["other:9092"],
        "topic": "orders",
    }
}
HANDLERS = [{"entrypoint": "process", "sources": ["orders"]}]

def process(request, records):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(WorkerDiscoveryError, "conflicting source"):
                discover_workers(directory)

    def test_file_resource_loads_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dim_path = Path(directory, "users.json")
            dim_path.write_text('{"1": {"name": "alice"}}', encoding="utf-8")
            Path(directory, "enriched.py").write_text(
                f'''
SOURCES = {{
    "orders": {{
        "kind": "kafka",
        "brokers": ["broker:9092"],
        "topic": "orders",
    }}
}}
RESOURCES = {{
    "user-dim-v1": {{
        "kind": "file",
        "path": {str(dim_path)!r},
    }}
}}
HANDLERS = [
    {{
        "entrypoint": "process",
        "sources": ["orders"],
        "resources": ["user-dim-v1"],
    }},
]

def process(request, records, resources):
    return resources["user-dim-v1"]
''',
                encoding="utf-8",
            )
            _, _, resources = discover_workers(directory)
            loaded = ResourceLoader(resources).get("user-dim-v1")
        self.assertEqual({"1": {"name": "alice"}}, loaded)

    def test_unknown_resource_name_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "missing.py").write_text(
                '''
HANDLERS = [
    {
        "entrypoint": "process",
        "sources": ["orders"],
        "resources": ["missing-dim"],
    },
]

def process(request, records, resources):
    return request.dispatch_id
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(WorkerDiscoveryError, "unknown resource"):
                discover_workers(
                    directory,
                    source_registry=ORDERS_REGISTRY,
                    resource_registry={},
                )

    def test_multisource_handlers_require_kafka_only(self) -> None:
        registry = build_source_registry(
            {
                "orders": {
                    "kind": "kafka",
                    "brokers": ["broker:9092"],
                    "topic": "orders",
                },
                "dim": {
                    "kind": "postgres",
                    "dsn": "postgresql://example",
                    "table": "dim",
                    "timestamp_column": "updated_at",
                    "primary_key_column": "id",
                },
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "join.py").write_text(
                '''
HANDLERS = [
    {
        "entrypoint": "join",
        "sources": ["orders", "dim"],
    },
]

def join(request, records):
    return records
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                WorkerDiscoveryError, "multi-source handlers must declare only Kafka"
            ):
                discover_workers(directory, source_registry=registry)

    def test_multisource_kafka_handler_discovers_group_key(self) -> None:
        registry = build_source_registry(
            {
                "orders": {
                    "kind": "kafka",
                    "brokers": ["broker:9092"],
                    "topic": "orders",
                },
                "payments": {
                    "kind": "kafka",
                    "brokers": ["broker:9092"],
                    "topic": "payments",
                },
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "join.py").write_text(
                '''
HANDLERS = [
    {
        "entrypoint": "join",
        "sources": ["orders", "payments"],
    },
]

def join(request, records):
    return records
''',
                encoding="utf-8",
            )
            workers, _, _ = discover_workers(directory, source_registry=registry)
        self.assertEqual(1, len(workers))
        self.assertTrue(workers[0].is_multisource)
        self.assertEqual("ms:orders+payments", workers[0].group_key)

    def test_bad_worker_module_reports_its_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "bad.py")
            path.write_text("HANDLERS = [{'entrypoint': 1}]\n", encoding="utf-8")
            with self.assertRaisesRegex(WorkerDiscoveryError, "bad.py"):
                discover_workers(directory)

    def test_task_with_resources_requires_three_params(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "bad_task.py").write_text(
                '''
SOURCES = {
    "orders": {"kind": "kafka", "brokers": ["b:1"], "topic": "orders"}
}
RESOURCES = {"dim": {"kind": "static", "data": {1: 1}}}
HANDLERS = [{
    "entrypoint": "process",
    "sources": ["orders"],
    "resources": ["dim"],
}]

def process(request, records):
    return records
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                WorkerDiscoveryError, "request, records, resources"
            ):
                discover_workers(directory)

    def test_task_without_resources_rejects_required_resources_param(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "bad_arity.py").write_text(
                '''
SOURCES = {
    "orders": {"kind": "kafka", "brokers": ["b:1"], "topic": "orders"}
}
HANDLERS = [{"entrypoint": "process", "sources": ["orders"]}]

def process(request, records, resources):
    return records
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                WorkerDiscoveryError, "without resources must accept"
            ):
                discover_workers(directory)

    def test_actor_with_resources_validates_init_and_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "ok_actor.py").write_text(
                '''
SOURCES = {
    "orders": {"kind": "kafka", "brokers": ["b:1"], "topic": "orders"}
}
RESOURCES = {"dim": {"kind": "static", "data": {1: 1}}}
HANDLERS = [{
    "entrypoint": "Worker",
    "sources": ["orders"],
    "resources": ["dim"],
    "mode": "actor",
}]

class Worker:
    def __init__(self, resources):
        self.resources = resources

    def process(self, request, records):
        return records
''',
                encoding="utf-8",
            )
            workers, _, _ = discover_workers(directory)
        self.assertEqual(ExecutionMode.ACTOR, workers[0].mode)
        self.assertEqual(("dim",), workers[0].resource_ids)

    def test_actor_rejects_function_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "fn_actor.py").write_text(
                '''
SOURCES = {
    "orders": {"kind": "kafka", "brokers": ["b:1"], "topic": "orders"}
}
HANDLERS = [{
    "entrypoint": "process",
    "sources": ["orders"],
    "mode": "actor",
}]

def process(request, records):
    return records
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(WorkerDiscoveryError, "class entrypoint"):
                discover_workers(directory)

    def test_actor_with_resources_requires_init(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "no_init.py").write_text(
                '''
SOURCES = {
    "orders": {"kind": "kafka", "brokers": ["b:1"], "topic": "orders"}
}
RESOURCES = {"dim": {"kind": "static", "data": {1: 1}}}
HANDLERS = [{
    "entrypoint": "Worker",
    "sources": ["orders"],
    "resources": ["dim"],
    "mode": "actor",
}]

class Worker:
    def process(self, request, records):
        return records
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                WorkerDiscoveryError, "__init__\\(self, resources\\)"
            ):
                discover_workers(directory)

    def test_actor_process_must_not_require_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "bad_process.py").write_text(
                '''
SOURCES = {
    "orders": {"kind": "kafka", "brokers": ["b:1"], "topic": "orders"}
}
RESOURCES = {"dim": {"kind": "static", "data": {1: 1}}}
HANDLERS = [{
    "entrypoint": "Worker",
    "sources": ["orders"],
    "resources": ["dim"],
    "mode": "actor",
}]

class Worker:
    def __init__(self, resources):
        self.resources = resources

    def process(self, request, records, resources):
        return records
''',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                WorkerDiscoveryError, "must not require resources"
            ):
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
        client = SourceObserver()

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
        client = SourceObserver()

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
