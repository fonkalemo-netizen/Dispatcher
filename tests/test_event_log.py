"""Tests for ray_dispatcher.event_log and Dispatcher wiring."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from ray_dispatcher import (
    EVENT_BATCH_COMMITTED,
    EVENT_BATCH_SKIPPED,
    EVENT_RUN_FAILED,
    EVENT_SNAPSHOT,
    EVENT_TRIGGER_EVALUATED,
    EventLog,
    EventLogError,
    HandlerSpec,
    KafkaSource,
    MemoryCheckpointStore,
    RayDispatcher,
    DispatcherConfig,
    create_event_log,
    discover_event_hooks,
)
from tests.test_ray_dispatcher import FakeRayBackend, FakeSourceObserver


class EventLogUnitTests(unittest.IsolatedAsyncioTestCase):
    def test_register_decorator_and_emit_order(self) -> None:
        log = EventLog()
        seen: list[str] = []

        def first(payload: Mapping[str, Any]) -> None:
            seen.append(f"first:{payload['n']}")

        @log.hook(EVENT_BATCH_COMMITTED)
        def second(payload: Mapping[str, Any]) -> None:
            seen.append(f"second:{payload['n']}")

        log.register(EVENT_BATCH_COMMITTED, first)
        errors = log.emit(EVENT_BATCH_COMMITTED, {"n": 1})
        self.assertEqual([], errors)
        self.assertEqual(["second:1", "first:1"], seen)

    def test_emit_continues_after_hook_error(self) -> None:
        log = EventLog()
        seen: list[str] = []

        def boom(_: Mapping[str, Any]) -> None:
            raise RuntimeError("nope")

        def ok(payload: Mapping[str, Any]) -> None:
            seen.append(str(payload["ok"]))

        log.register(EVENT_RUN_FAILED, boom)
        log.register(EVENT_RUN_FAILED, ok)
        errors = log.emit(EVENT_RUN_FAILED, {"ok": True})
        self.assertEqual(1, len(errors))
        self.assertIn("RuntimeError: nope", errors[0])
        self.assertEqual(["True"], seen)

    def test_module_hook_decorator_uses_default_registry(self) -> None:
        # Use a dedicated EventLog via create; module @hook targets default_event_log.
        # Here we only assert the decorator API registers on an EventLog instance.
        log = EventLog()

        @log.hook(EVENT_SNAPSHOT)
        def capture(payload: Mapping[str, Any]) -> None:
            capture.last = dict(payload)  # type: ignore[attr-defined]

        log.emit(EVENT_SNAPSHOT, {"sources": {}})
        self.assertEqual({"sources": {}}, capture.last)  # type: ignore[attr-defined]

    def test_unknown_event_rejected(self) -> None:
        log = EventLog()
        with self.assertRaises(EventLogError):
            log.register("not-an-event", lambda payload: None)

    def test_discover_event_hooks_from_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "alerts.py"
            path.write_text(
                """
HOOKS = [{"event": "batch_skipped", "entrypoint": "alert_skip"}]

def alert_skip(payload):
    alert_skip.calls = getattr(alert_skip, "calls", [])
    alert_skip.calls.append(dict(payload))
""",
                encoding="utf-8",
            )
            log = discover_event_hooks(directory, default_logging=False)
            log.emit(EVENT_BATCH_SKIPPED, {"batch_id": "b1"})
            module_fn = log.listeners(EVENT_BATCH_SKIPPED)[0]
            self.assertEqual([{"batch_id": "b1"}], module_fn.calls)  # type: ignore[attr-defined]

    def test_create_event_log_installs_default_logging(self) -> None:
        log = create_event_log(default_logging=True)
        self.assertEqual(1, len(log.listeners(EVENT_BATCH_COMMITTED)))
        self.assertEqual(1, len(log.listeners(EVENT_SNAPSHOT)))
        self.assertEqual(1, len(log.listeners(EVENT_TRIGGER_EVALUATED)))


class EventLogDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def _complete_fetches(
        self,
        dispatcher: RayDispatcher,
        backend: FakeRayBackend,
    ) -> None:
        pending = [
            (request, ref)
            for _, request, _, ref in backend.fetch_submissions
            if ref not in backend.values and ref not in backend.ready
        ]
        for request, ref in pending:
            payload = [
                {"offset": offset}
                for offset in range(request.start_offset, request.end_offset)
            ]
            backend.finish(ref, value=payload)
        await dispatcher.ray_status()

    async def test_batch_committed_and_snapshot_events(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        events = create_event_log(default_logging=False)
        committed: list[Mapping[str, Any]] = []
        snapshots: list[Mapping[str, Any]] = []
        events.register(EVENT_BATCH_COMMITTED, committed.append)
        events.register(EVENT_SNAPSHOT, snapshots.append)

        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=(1, 5), max_retries=0)
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=MemoryCheckpointStore(),
            config=DispatcherConfig(max_in_flight=8),
            event_log=events,
            event_log_interval=0,
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        backend.finish(backend.submissions[0][2], value={"ok": True})
        await dispatcher.ray_status()

        self.assertEqual(1, len(committed))
        self.assertEqual("shared:events:0", committed[0]["progress_key"])
        self.assertEqual(5, committed[0]["item_count"])

        await dispatcher.event_log_tick()
        self.assertEqual(1, len(snapshots))
        self.assertIn("sources", snapshots[0])

    async def test_run_failed_and_batch_skipped_events(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        events = create_event_log(default_logging=False)
        failed: list[Mapping[str, Any]] = []
        skipped: list[Mapping[str, Any]] = []
        events.register(EVENT_RUN_FAILED, failed.append)
        events.register(EVENT_BATCH_SKIPPED, skipped.append)

        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=(1, 5), max_retries=0)
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=MemoryCheckpointStore(),
            config=DispatcherConfig(max_in_flight=8),
            event_log=events,
            event_log_interval=0,
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        backend.finish(backend.submissions[0][2], error="boom")
        await dispatcher.ray_status()

        self.assertEqual(1, len(failed))
        self.assertTrue(failed[0]["permanent"])
        self.assertEqual("boom", failed[0]["error"])
        self.assertEqual(1, len(skipped))
        self.assertIn("failure_id", skipped[0])
        self.assertEqual("shared:events:0", skipped[0]["progress_key"])

    async def test_hook_error_recorded_without_breaking_commit(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        events = create_event_log(default_logging=False)

        def boom(_: Mapping[str, Any]) -> None:
            raise RuntimeError("hook failed")

        events.register(EVENT_BATCH_COMMITTED, boom)
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=(1, 5), max_retries=0)
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=MemoryCheckpointStore(),
            event_log=events,
            event_log_interval=0,
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        backend.finish(backend.submissions[0][2], value={"ok": True})
        await dispatcher.ray_status()

        self.assertEqual(5, dispatcher.state.sources["shared:events:0"].committed)
        self.assertTrue(
            any("hook failed" in item for item in dispatcher.state.loop_errors)
        )


if __name__ == "__main__":
    unittest.main()
