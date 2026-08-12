"""Tests for postgres resources, preload, hot reload, and locked snapshot."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

from ray_dispatcher import (
    EqRuleLabeler,
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

    def test_resource_from_mapping_ttl_policy(self) -> None:
        spec = resource_from_mapping(
            {
                "resource_id": "rules",
                "kind": "file",
                "path": "/tmp/rules.json",
                "refresh_policy": {"type": "ttl", "seconds": 30},
            }
        )
        self.assertEqual("ttl", spec.refresh_policy)
        self.assertEqual(30.0, spec.ttl_seconds)
        self.assertEqual("ttl", spec.canonical_dict()["refresh_policy"])
        self.assertEqual(30.0, spec.canonical_dict()["ttl_seconds"])

    async def test_ttl_refresh_reloads_file_resource(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rules.json"
            path.write_text('{"version": 1}', encoding="utf-8")
            registry = build_resource_registry(
                {
                    "rules": {
                        "kind": "file",
                        "path": str(path),
                        "refresh_policy": {"type": "ttl", "seconds": 60},
                    }
                }
            )
            loader = ResourceLoader(registry)
            await loader.preload()
            first_version = loader.version
            self.assertEqual({"version": 1}, loader.get("rules"))

            path.write_text('{"version": 2}', encoding="utf-8")
            loader._loaded_at["rules"] -= 61
            refreshed = await loader.refresh_expired(("rules",))

            self.assertEqual({"rules"}, refreshed)
            self.assertGreater(loader.version, first_version)
            self.assertEqual({"version": 2}, loader.get("rules"))

    async def test_resource_formatter_runs_after_load(self) -> None:
        registry = build_resource_registry(
            {
                "users": {
                    "kind": "static",
                    "data": {
                        "u1": {"id": "u1", "tier": "gold"},
                        "u2": {"id": "u2", "tier": "silver"},
                    },
                    "formatter": "tiers_only",
                }
            }
        )

        def tiers_only(rows: dict[str, dict[str, Any]]) -> dict[str, str]:
            return {key: row["tier"] for key, row in rows.items()}

        loader = ResourceLoader(registry, formatters={"users": tiers_only})
        await loader.preload()

        self.assertEqual({"u1": "gold", "u2": "silver"}, loader.get("users"))

    async def test_manual_policy_does_not_ttl_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rules.json"
            path.write_text('{"version": 1}', encoding="utf-8")
            registry = build_resource_registry(
                {"rules": {"kind": "file", "path": str(path)}}
            )
            loader = ResourceLoader(registry)
            await loader.preload()
            path.write_text('{"version": 2}', encoding="utf-8")
            loader._loaded_at["rules"] -= 3600

            self.assertEqual(set(), await loader.refresh_expired(("rules",)))
            self.assertEqual({"version": 1}, loader.get("rules"))

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

    async def test_preload_isolates_failed_resource(self) -> None:
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
        loader = ResourceLoader(registry)
        with patch.object(
            ResourceLoader,
            "_load_postgres",
            new=AsyncMock(side_effect=OSError("Network is unreachable")),
        ):
            failed = await loader.preload()
        self.assertTrue(loader._preloaded)
        self.assertEqual({"ok": True}, loader.get("cfg"))
        self.assertIn("users", failed)
        self.assertIn("Network is unreachable", failed["users"])
        self.assertEqual(failed, dict(loader.failed_resources))
        with self.assertRaises(RuntimeError) as ctx:
            loader.get("users")
        self.assertIn("unavailable", str(ctx.exception))

    async def test_ensure_available_retries_then_isolates(self) -> None:
        registry = build_resource_registry(
            {
                "users": {
                    "kind": "postgres",
                    "dsn": "postgresql://db/app",
                    "key_column": "id",
                    "query": "select id from users",
                },
            }
        )
        loader = ResourceLoader(registry)
        with patch.object(
            ResourceLoader,
            "_load_postgres",
            new=AsyncMock(side_effect=RuntimeError("db down")),
        ):
            await loader.preload()
            with self.assertRaises(RuntimeError) as ctx:
                await loader.ensure_available(("users",))
        self.assertIn("handler resources unavailable", str(ctx.exception))
        self.assertIn("users", loader.failed_resources)

        with patch.object(
            ResourceLoader,
            "_load_postgres",
            new=AsyncMock(return_value={1: {"id": 1}}),
        ):
            await loader.ensure_available(("users",))
        self.assertEqual({1: {"id": 1}}, loader.get("users"))
        self.assertEqual({}, dict(loader.failed_resources))

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

    async def test_dispatcher_start_isolates_failed_resource(self) -> None:
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        resources = build_resource_registry(
            {
                "dim": {"kind": "static", "data": {"x": 1}},
                "users": {
                    "kind": "postgres",
                    "dsn": "postgresql://db/app",
                    "key_column": "id",
                    "table": "users",
                },
            }
        )
        ok_worker = HandlerSpec(
            "ok", object(), (source,), resource_ids=("dim",)
        )
        bad_worker = HandlerSpec(
            "bad", object(), (source,), resource_ids=("users",)
        )
        backend = FakeRayAdapter()
        dispatcher = RayDispatcher(
            (ok_worker, bad_worker),
            ray_adapter=backend,
            resource_registry=resources,
            event_log=create_event_log(default_logging=False),
            event_log_interval=0,
        )
        with patch.object(
            ResourceLoader,
            "_load_postgres",
            new=AsyncMock(side_effect=OSError("Network is unreachable")),
        ):
            await dispatcher.start()
        try:
            self.assertTrue(dispatcher.resource_loader._preloaded)
            self.assertEqual({"x": 1}, dispatcher.resource_loader.get("dim"))
            self.assertIn("users", dispatcher.resource_loader.failed_resources)
            self.assertTrue(
                any(
                    item.startswith("resource_preload:users:")
                    for item in dispatcher.state.loop_errors
                )
            )
            await dispatcher.resource_loader.ensure_available(("dim",))
            with self.assertRaises(RuntimeError):
                await dispatcher.resource_loader.ensure_available(("users",))
            snap = await dispatcher.snapshot()
            self.assertIn("users", snap["failed_resources"])
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


class EqRuleLabelerTests(unittest.IsolatedAsyncioTestCase):
    def test_attr_mode_matches_without_dict_conversion(self) -> None:
        class Order:
            def __init__(self, status: str, channel: str, amount: int) -> None:
                self.status = status
                self.channel = channel
                self.amount = amount

        labeler = EqRuleLabeler(
            [
                {
                    "when": {"status": "paid", "channel": "app"},
                    "label": "app-paid",
                },
                {"when": {"status": "paid"}, "label": "paid"},
            ]
        )

        self.assertEqual(
            ("app-paid", "paid"),
            labeler.match(Order("paid", "app", 100)),
        )
        self.assertEqual(("paid",), labeler.match(Order("paid", "web", 100)))
        self.assertEqual((), labeler.match(Order("cancelled", "app", 100)))
        self.assertEqual((("app-paid", "paid"), ()), tuple(labeler.match_many([
            Order("paid", "app", 100),
            Order("cancelled", "app", 100),
        ])))

    def test_dict_mode_and_first_match(self) -> None:
        labeler = EqRuleLabeler(
            [
                {"when": {"type": "order"}, "label": "order"},
                {"when": {"type": "order", "status": "paid"}, "label": "paid-order"},
            ],
            record_mode="dict",
            multi_match=False,
        )

        self.assertEqual(
            ("order",),
            labeler.match({"type": "order", "status": "paid"}),
        )
        self.assertEqual((), labeler.match({"status": "paid"}))

    async def test_resource_loader_builds_inline_rule_labeler(self) -> None:
        registry = build_resource_registry(
            {
                "labels": {
                    "kind": "eq_rule_labeler",
                    "record_mode": "dict",
                    "rules": [
                        {"when": {"status": "paid"}, "label": "paid"},
                    ],
                }
            }
        )
        loader = ResourceLoader(registry)
        await loader.preload()

        labeler = loader.get("labels")

        self.assertIsInstance(labeler, EqRuleLabeler)
        self.assertEqual(("paid",), labeler.match({"status": "paid"}))
        self.assertEqual(
            {
                "kind": "eq_rule_labeler",
                "resource_id": "labels",
                "refresh_policy": "manual",
                "rules": [{"when": {"status": "paid"}, "label": "paid"}],
                "record_mode": "dict",
                "multi_match": True,
            },
            registry["labels"].canonical_dict(),
        )

    async def test_ttl_refresh_reloads_rule_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rules.json"
            path.write_text(
                '{"rules": [{"when": {"status": "paid"}, "label": "v1"}]}',
                encoding="utf-8",
            )
            registry = build_resource_registry(
                {
                    "labels": {
                        "kind": "eq_rule_labeler",
                        "record_mode": "dict",
                        "path": str(path),
                        "refresh_policy": {"type": "ttl", "seconds": 60},
                    }
                }
            )
            loader = ResourceLoader(registry)
            await loader.preload()
            self.assertEqual(("v1",), loader.get("labels").match({"status": "paid"}))

            path.write_text(
                '{"rules": [{"when": {"status": "paid"}, "label": "v2"}]}',
                encoding="utf-8",
            )
            loader._loaded_at["labels"] -= 61
            refreshed = await loader.refresh_expired(("labels",))

            self.assertEqual({"labels"}, refreshed)
            self.assertEqual(("v2",), loader.get("labels").match({"status": "paid"}))


if __name__ == "__main__":
    unittest.main()
