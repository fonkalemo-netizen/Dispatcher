"""Tests for postgres resources, preload, hot reload, and locked snapshot."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

from ray_dispatcher import (
    HandlerSpec,
    KafkaSource,
    MemoryCheckpointStore,
    RayDispatcher,
    ResourceSpec,
    create_event_log,
)
from ray_dispatcher.registries import build_resource_registry, resource_from_mapping
from ray_dispatcher.resources import ResourceLoader
from tests.test_ray_dispatcher import FakeRayAdapter, FakeSourceObserver


class ResourcePreloadTests(unittest.IsolatedAsyncioTestCase):
    def test_postgres_resource_spec_requires_query_xor_table(self) -> None:
        with self.assertRaises(ValueError):
            ResourceSpec(
                "dim",
                kind="postgres",
                dsn="postgresql://x",
                key_column="id",
            )
        with self.assertRaises(ValueError):
            ResourceSpec(
                "dim",
                kind="postgres",
                dsn="postgresql://x",
                key_column="id",
                query="select 1",
                table="users",
            )

    def test_resource_from_mapping_postgres(self) -> None:
        spec = resource_from_mapping(
            {
                "resource_id": "users",
                "kind": "postgres",
                "dsn": "postgresql://db/app",
                "key_column": "id",
                "table": "users",
            }
        )
        self.assertEqual("postgres", spec.kind)
        self.assertEqual("users", spec.table)
        self.assertIn("dsn", spec.canonical_dict())

    async def test_preload_loads_static_and_mocked_postgres(self) -> None:
        registry = build_resource_registry(
            {
                "cfg": {"kind": "static", "data": {"a": 1}},
                "users": {
                    "kind": "postgres",
                    "dsn": "postgresql://db/app",
                    "key_column": "id",
                    "query": "select id, name from users",
                },
            }
        )
        loader = ResourceLoader(registry)
        with patch.object(
            ResourceLoader,
            "_load_postgres",
            new=AsyncMock(return_value={1: {"id": 1, "name": "alice"}}),
        ):
            await loader.preload()
        self.assertEqual({"a": 1}, loader.get("cfg"))
        self.assertEqual({1: {"id": 1, "name": "alice"}}, loader.get("users"))
        self.assertTrue(loader._preloaded)

    async def test_preload_failure_restores_previous_cache(self) -> None:
        registry = build_resource_registry(
            {
                "cfg": {"kind": "static", "data": {"ok": True}},
                "users": {
                    "kind": "postgres",
                    "dsn": "postgresql://db/app",
                    "key_column": "id",
                    "table": "users",
                },
            }
        )
        loader = ResourceLoader({"cfg": registry["cfg"]})
        await loader.preload()
        loader.replace_registry(registry)
        with patch.object(
            ResourceLoader,
            "_load_postgres",
            new=AsyncMock(side_effect=RuntimeError("db down")),
        ):
            with self.assertRaises(RuntimeError):
                await loader.preload()
        # replace_registry cleared cache; failed preload restores empty previous
        self.assertFalse(loader._preloaded)
        self.assertEqual({}, loader._cache)

    async def test_dispatcher_start_preloads_resources(self) -> None:
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        resources = build_resource_registry(
            {"dim": {"kind": "static", "data": {"x": 1}}}
        )
        worker = HandlerSpec(
            "worker", object(), (source,), resource_ids=("dim",)
        )
        backend = FakeRayAdapter()
        dispatcher = RayDispatcher(
            (worker,),
            ray_adapter=backend,
            resource_registry=resources,
            event_log=create_event_log(default_logging=False),
            event_log_interval=0,
        )
        self.assertFalse(dispatcher.resource_loader._preloaded)
        await dispatcher.start()
        try:
            self.assertTrue(dispatcher.resource_loader._preloaded)
            self.assertEqual({"x": 1}, dispatcher.resource_loader.get("dim"))
        finally:
            await dispatcher.stop()


class WorkersReloadTests(unittest.IsolatedAsyncioTestCase):
    def _write_worker(self, directory: Path, *, name: str, batch_size: int) -> None:
        (directory / "orders.py").write_text(
            f"""
SOURCES = {{
    "events": {{
        "kind": "kafka",
        "brokers": ["broker"],
        "topic": "events",
        "initial_offset": "earliest",
    }}
}}
RESOURCES = {{}}
HANDLERS = [
    {{
        "handler_id": "{name}",
        "entrypoint": "handle",
        "sources": ["events"],
        "batch_size": {batch_size},
        "max_retries": 0,
    }}
]

def handle(request, records):
    return {{"count": len(records)}}
""",
            encoding="utf-8",
        )

    async def test_reload_applies_when_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_worker(root, name="worker-a", batch_size=5)
            backend = FakeRayAdapter()
            dispatcher = RayDispatcher(
                root,
                ray_adapter=backend,
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                reload_interval=0,
            )
            await dispatcher.start()
            try:
                self.assertIn("worker-a", dispatcher.workers)
                self._write_worker(root, name="worker-b", batch_size=7)
                changed = await dispatcher.reload_workers()
                self.assertTrue(changed)
                self.assertIn("worker-b", dispatcher.workers)
                self.assertNotIn("worker-a", dispatcher.workers)
                self.assertEqual(7, dispatcher.workers["worker-b"].batch_size)
            finally:
                await dispatcher.stop()

    async def test_reload_defers_when_batch_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_worker(root, name="worker-a", batch_size=5)
            backend = FakeRayAdapter()
            client = FakeSourceObserver()
            dispatcher = RayDispatcher(
                root,
                ray_adapter=backend,
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
            )
            dispatcher.source_observer = client
            client.kafka["events"] = {0: (0, 5)}
            await dispatcher.start()
            try:
                await dispatcher.data_listener()
                await dispatcher.ray_trigger()
                self.assertTrue(dispatcher._has_inflight_work())
                self._write_worker(root, name="worker-b", batch_size=9)
                changed = await dispatcher.reload_workers()
                self.assertFalse(changed)
                self.assertIn("worker-a", dispatcher.workers)
            finally:
                await dispatcher.stop()

    async def test_reload_detects_handler_code_only_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_worker(root, name="worker-a", batch_size=5)
            path = root / "orders.py"
            original = path.read_text(encoding="utf-8")
            backend = FakeRayAdapter()
            dispatcher = RayDispatcher(
                root,
                ray_adapter=backend,
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                reload_interval=0,
            )
            await dispatcher.start()
            try:
                before = dispatcher._config_fingerprint
                # Same HANDLERS config; only entrypoint body changes.
                path.write_text(
                    original.replace(
                        'return {"count": len(records)}',
                        'return {"count": len(records), "v": 2}',
                    ),
                    encoding="utf-8",
                )
                changed = await dispatcher.reload_workers()
                self.assertTrue(changed)
                self.assertNotEqual(before, dispatcher._config_fingerprint)
            finally:
                await dispatcher.stop()


class SnapshotLockTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_is_async_and_consistent(self) -> None:
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=5)
        backend = FakeRayAdapter()
        dispatcher = RayDispatcher(
            (worker,),
            ray_adapter=backend,
            event_log=create_event_log(default_logging=False),
            event_log_interval=0,
        )
        dispatcher.source_observer = FakeSourceObserver()
        dispatcher.source_observer.kafka["events"] = {0: (0, 3)}
        await dispatcher.data_listener()
        snap = await dispatcher.snapshot()
        self.assertIn("shared:events:0", snap["sources"])
        self.assertEqual([], snap["loop_errors"])
        # Holding the lock, build snapshot without awaiting nested lock.
        async with dispatcher._lock:
            locked = dispatcher._build_snapshot()
        self.assertEqual(snap["sources"].keys(), locked["sources"].keys())


if __name__ == "__main__":
    unittest.main()
