"""Tests for plugin store, validation, and hot enable/disable."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from ray_dispatcher import (
    MemoryCheckpointStore,
    RayDispatcher,
    create_event_log,
)
from ray_dispatcher.plugins.manager import PluginManager, ReloadResult
from ray_dispatcher.plugins.store import PluginConflictError, PluginStore
from ray_dispatcher.plugins.validate import validate_worker_py
from tests.test_ray_dispatcher import FakeRayAdapter, FakeSourceObserver


SAFE_PLUGIN = '''\
from __future__ import annotations
from typing import Any

SOURCES = {{
    "plugin-events": {{
        "kind": "kafka",
        "brokers": ["demo:9092"],
        "topic": "events",
        "initial_offset": "earliest",
    }}
}}

HANDLERS = [
    {{
        "handler_id": "{plugin_id}:run",
        "entrypoint": "run",
        "sources": ["plugin-events"],
        "batch_size": [1, 10],
    }}
]

def run(request: Any, records: list[Any]) -> dict[str, Any]:
    return {{"n": len(records)}}
'''

DANGEROUS_PLUGIN = '''\
from __future__ import annotations
from typing import Any
import subprocess

SOURCES = {
    "plugin-events": {
        "kind": "kafka",
        "brokers": ["demo:9092"],
        "topic": "events",
        "initial_offset": "earliest",
    }
}

HANDLERS = [
    {
        "handler_id": "bad:run",
        "entrypoint": "run",
        "sources": ["plugin-events"],
        "batch_size": 1,
    }
]

def run(request: Any, records: list[Any]) -> Any:
    subprocess.run(["echo", "pwned"], check=False)
    return {}
'''

NO_PREFIX_PLUGIN = '''\
from __future__ import annotations
from typing import Any

SOURCES = {
    "plugin-events": {
        "kind": "kafka",
        "brokers": ["demo:9092"],
        "topic": "events",
        "initial_offset": "earliest",
    }
}

HANDLERS = [
    {
        "handler_id": "wrong:run",
        "entrypoint": "run",
        "sources": ["plugin-events"],
        "batch_size": 1,
    }
]

def run(request: Any, records: list[Any]) -> Any:
    return {}
'''


def _write_builtin(root: Path, *, name: str = "builtin-worker") -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "builtin.py").write_text(
        f'''\
from __future__ import annotations
from typing import Any

SOURCES = {{
    "demo-orders": {{
        "kind": "kafka",
        "brokers": ["demo:9092"],
        "topic": "orders",
        "initial_offset": "earliest",
    }}
}}

HANDLERS = [
    {{
        "handler_id": "{name}",
        "entrypoint": "process",
        "sources": ["demo-orders"],
        "batch_size": [1, 5],
    }}
]

def process(request: Any, records: list[Any]) -> dict[str, Any]:
    return {{"n": len(records)}}
''',
        encoding="utf-8",
    )


class ValidateWorkerPyTests(unittest.TestCase):
    def test_rejects_dangerous_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "worker.py"
            path.write_text(DANGEROUS_PLUGIN, encoding="utf-8")
            result = validate_worker_py(path, plugin_id="bad", run_discover=False)
            self.assertFalse(result.ok)
            self.assertTrue(
                any("subprocess" in err or "forbidden" in err for err in result.errors)
            )

    def test_accepts_controlled_write_sample(self) -> None:
        sample = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "plugins"
            / "orders_filter_v1"
            / "worker.py"
        )
        result = validate_worker_py(
            sample, plugin_id="orders_filter_v1", run_discover=True
        )
        self.assertTrue(result.ok, result.errors)
        self.assertIn("orders_filter_v1:filter_orders", result.handler_ids)
        self.assertEqual([], result.warnings)

    def test_rejects_handler_without_plugin_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "worker.py"
            path.write_text(NO_PREFIX_PLUGIN, encoding="utf-8")
            result = validate_worker_py(path, plugin_id="good", run_discover=False)
            self.assertFalse(result.ok)
            self.assertTrue(any("must start with" in err for err in result.errors))


class PluginHotReloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_upload_enable_disable_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            workers = base / "workers"
            data = base / "data"
            _write_builtin(workers)
            store = PluginStore(data)
            backend = FakeRayAdapter()
            dispatcher = RayDispatcher(
                workers,
                ray_adapter=backend,
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                plugin_store=store,
            )
            manager = PluginManager(store, dispatcher)
            await manager.startup()

            plugin_id = "orders_v2"
            uploaded = await manager.upload(
                plugin_id, SAFE_PLUGIN.format(plugin_id=plugin_id).encode("utf-8")
            )
            self.assertTrue(uploaded.record.validation["ok"])
            self.assertFalse(uploaded.record.desired_enabled)
            self.assertFalse(uploaded.record.effective_enabled)
            self.assertTrue(store.staging_path(plugin_id).is_file())
            self.assertFalse(store.active_path(plugin_id).is_file())

            before = dispatcher._config_fingerprint
            enabled = await manager.enable(plugin_id)
            self.assertEqual(ReloadResult.SWAPPED, enabled.reload_result)
            self.assertTrue(enabled.record.desired_enabled)
            self.assertTrue(enabled.record.effective_enabled)
            self.assertFalse(enabled.record.reload_pending)
            self.assertIn(f"{plugin_id}:run", dispatcher.workers)
            self.assertNotEqual(before, dispatcher._config_fingerprint)
            self.assertTrue(store.active_path(plugin_id).is_file())

            disabled = await manager.disable(plugin_id)
            self.assertFalse(disabled.record.desired_enabled)
            self.assertFalse(disabled.record.effective_enabled)
            self.assertNotIn(f"{plugin_id}:run", dispatcher.workers)
            self.assertFalse(store.active_path(plugin_id).is_file())

            await manager.delete(plugin_id)
            self.assertIsNone(store.get(plugin_id))

    async def test_enable_pending_when_inflight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            workers = base / "workers"
            data = base / "data"
            _write_builtin(workers)
            store = PluginStore(data)
            backend = FakeRayAdapter()
            client = FakeSourceObserver()
            dispatcher = RayDispatcher(
                workers,
                ray_adapter=backend,
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                plugin_store=store,
            )
            dispatcher.source_observer = client
            client.kafka["demo-orders"] = {0: (0, 5)}
            manager = PluginManager(store, dispatcher)

            plugin_id = "pending_v1"
            await manager.upload(
                plugin_id, SAFE_PLUGIN.format(plugin_id=plugin_id).encode("utf-8")
            )
            await dispatcher.data_listener()
            await dispatcher.ray_trigger()
            self.assertTrue(dispatcher.has_inflight_work())

            enabled = await manager.enable(plugin_id)
            self.assertEqual(ReloadResult.PENDING_INFLIGHT, enabled.reload_result)
            self.assertTrue(enabled.record.desired_enabled)
            self.assertFalse(enabled.record.effective_enabled)
            self.assertTrue(enabled.record.reload_pending)
            self.assertNotIn(f"{plugin_id}:run", dispatcher.workers)
            self.assertTrue(store.active_path(plugin_id).is_file())
            self.assertEqual([], store.list_active_roots())

            # Finish in-flight work, then apply pending.
            for _, _, _, ref in backend.fetch_submissions:
                backend.finish(ref, value=[{"x": 1}])
            await dispatcher.ray_status()
            for _, request, ref in backend.submissions:
                backend.finish(ref, value="ok")
            await dispatcher.ray_status()
            self.assertFalse(dispatcher.has_inflight_work())

            results = await manager.apply_pending_reloads()
            self.assertTrue(results)
            self.assertEqual(ReloadResult.SWAPPED, results[0].reload_result)
            self.assertIn(f"{plugin_id}:run", dispatcher.workers)
            record = store.get(plugin_id)
            assert record is not None
            self.assertTrue(record.effective_enabled)
            self.assertFalse(record.reload_pending)

    async def test_pending_enable_roots_not_in_list_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            workers = base / "workers"
            data = base / "data"
            _write_builtin(workers)
            store = PluginStore(data)
            dispatcher = RayDispatcher(
                workers,
                ray_adapter=FakeRayAdapter(),
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                plugin_store=store,
            )
            manager = PluginManager(store, dispatcher)
            plugin_id = "pend_roots"
            await manager.upload(
                plugin_id, SAFE_PLUGIN.format(plugin_id=plugin_id).encode("utf-8")
            )
            await store.promote_active(plugin_id)
            record = store.get(plugin_id)
            assert record is not None
            record.desired_enabled = True
            record.effective_enabled = False
            record.reload_pending = True
            await store.update_record(record)

            self.assertEqual([], store.list_active_roots())
            self.assertEqual(
                [store.active_root(plugin_id)],
                store.list_pending_enable_roots(),
            )
            results = await manager.apply_pending_reloads()
            self.assertTrue(results)
            self.assertEqual(ReloadResult.SWAPPED, results[0].reload_result)
            self.assertIn(f"{plugin_id}:run", dispatcher.workers)
            self.assertEqual(
                [store.active_root(plugin_id)],
                store.list_active_roots(),
            )
            self.assertEqual([], store.list_pending_enable_roots())

    async def test_pending_enable_survives_discover_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            workers = base / "workers"
            data = base / "data"
            _write_builtin(workers)
            store = PluginStore(data)
            dispatcher = RayDispatcher(
                workers,
                ray_adapter=FakeRayAdapter(),
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                plugin_store=store,
            )
            manager = PluginManager(store, dispatcher)
            plugin_id = "bad_discover"
            await manager.upload(
                plugin_id, SAFE_PLUGIN.format(plugin_id=plugin_id).encode("utf-8")
            )
            await store.promote_active(plugin_id)
            store.active_path(plugin_id).write_text(
                "this is not valid python (((", encoding="utf-8"
            )
            record = store.get(plugin_id)
            assert record is not None
            record.desired_enabled = True
            record.effective_enabled = False
            record.reload_pending = True
            await store.update_record(record)

            results = await manager.apply_pending_reloads()
            self.assertTrue(results)
            self.assertEqual(ReloadResult.FAILED_DISCOVER, results[0].reload_result)
            record = store.get(plugin_id)
            assert record is not None
            self.assertTrue(record.desired_enabled)
            self.assertFalse(record.effective_enabled)
            self.assertTrue(record.reload_pending)
            self.assertEqual([], store.list_active_roots())

    async def test_disable_pending_keeps_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            workers = base / "workers"
            data = base / "data"
            _write_builtin(workers)
            store = PluginStore(data)
            backend = FakeRayAdapter()
            client = FakeSourceObserver()
            dispatcher = RayDispatcher(
                workers,
                ray_adapter=backend,
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                plugin_store=store,
            )
            dispatcher.source_observer = client
            manager = PluginManager(store, dispatcher)
            plugin_id = "keep_active"
            await manager.upload(
                plugin_id, SAFE_PLUGIN.format(plugin_id=plugin_id).encode("utf-8")
            )
            await manager.enable(plugin_id)
            self.assertIn(f"{plugin_id}:run", dispatcher.workers)

            # Create in-flight on builtin + plugin sources.
            client.kafka["demo-orders"] = {0: (0, 3)}
            client.kafka["plugin-events"] = {0: (0, 3)}
            await dispatcher.data_listener()
            await dispatcher.ray_trigger()
            self.assertTrue(dispatcher.has_inflight_work())

            disabled = await manager.disable(plugin_id)
            self.assertEqual(ReloadResult.PENDING_INFLIGHT, disabled.reload_result)
            self.assertFalse(disabled.record.desired_enabled)
            self.assertTrue(disabled.record.effective_enabled)
            self.assertTrue(disabled.record.reload_pending)
            self.assertTrue(store.active_path(plugin_id).is_file())
            self.assertIn(f"{plugin_id}:run", dispatcher.workers)

    async def test_delete_rejects_enabled_or_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            workers = base / "workers"
            data = base / "data"
            _write_builtin(workers)
            store = PluginStore(data)
            dispatcher = RayDispatcher(
                workers,
                ray_adapter=FakeRayAdapter(),
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                plugin_store=store,
            )
            manager = PluginManager(store, dispatcher)
            plugin_id = "blocked_del"
            await manager.upload(
                plugin_id, SAFE_PLUGIN.format(plugin_id=plugin_id).encode("utf-8")
            )
            await manager.enable(plugin_id)
            with self.assertRaises(PluginConflictError):
                await manager.delete(plugin_id)

    async def test_conflict_with_builtin_handler_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            workers = base / "workers"
            data = base / "data"
            _write_builtin(workers, name="orders_v2:run")
            store = PluginStore(data)
            dispatcher = RayDispatcher(
                workers,
                ray_adapter=FakeRayAdapter(),
                checkpoint_store=MemoryCheckpointStore(),
                event_log=create_event_log(default_logging=False),
                event_log_interval=0,
                plugin_store=store,
            )
            manager = PluginManager(store, dispatcher)
            plugin_id = "orders_v2"
            await manager.upload(
                plugin_id, SAFE_PLUGIN.format(plugin_id=plugin_id).encode("utf-8")
            )
            enabled = await manager.enable(plugin_id)
            self.assertEqual(ReloadResult.FAILED_CONFLICT, enabled.reload_result)
            self.assertFalse(enabled.record.effective_enabled)


if __name__ == "__main__":
    unittest.main()
