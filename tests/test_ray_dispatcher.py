from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from ray_dispatcher import (
    BatchStatus,
    CheckpointDocument,
    Decision,
    DispatchRequest,
    DispatcherConfig,
    ExecutionMode,
    ExecutionResult,
    HandlerRequest,
    HandlerSpec,
    KafkaRetentionGap,
    KafkaSource,
    MemoryCheckpointStore,
    MemoryFailureStore,
    NativeRayBackend,
    PostgresCursor,
    PostgresSource,
    RayDispatcher,
    RunStatus,
    SoftAction,
    SourceKind,
    SourceState,
    SQLiteCheckpointStore,
    BacklogPressure,
    HasBacklog,
    HasCapacity,
    SourceIdle,
    TargetReached,
    TriggerContext,
    TriggerPolicy,
)
from ray_dispatcher.registries import ResourceSpec
from ray_dispatcher.resources import ResourceLoader


class FakeSourceObserver:
    def __init__(self) -> None:
        self.kafka: dict[str, dict[int, tuple[int, int]]] = {}
        self.pg_upper: dict[str, PostgresCursor] = {}
        self.pg_count: dict[str, int] = {}
        self.pg_count_calls = 0
        self.event_time_highs: dict[str, dict[int, datetime]] = {}
        # source_id -> partition -> sorted (timestamp, offset) pairs
        self.offsets_for_times: dict[str, dict[int, list[tuple[datetime, int]]]] = {}

    async def kafka_watermarks(
        self, source: KafkaSource
    ) -> Mapping[int, tuple[int, int]]:
        return self.kafka[source.source_id]

    async def kafka_event_time_highs(
        self,
        source: KafkaSource,
        watermarks: Mapping[int, tuple[int, int]] | None = None,
    ) -> Mapping[int, datetime]:
        return self.event_time_highs.get(source.source_id, {})

    async def kafka_offsets_for_times(
        self,
        source: KafkaSource,
        timestamps: Mapping[int, datetime],
    ) -> Mapping[int, int]:
        table = self.offsets_for_times.get(source.source_id, {})
        result: dict[int, int] = {}
        for partition, moment in timestamps.items():
            points = table.get(partition, [])
            for ts, offset in points:
                if ts >= moment:
                    result[partition] = offset
                    break
        return result

    async def postgres_high_watermark(self, source: PostgresSource) -> PostgresCursor:
        return self.pg_upper[source.source_id]

    async def postgres_count(
        self,
        source: PostgresSource,
        start_exclusive: PostgresCursor,
        end_inclusive: PostgresCursor,
    ) -> int:
        self.pg_count_calls += 1
        return self.pg_count[source.source_id]


class FakeRayBackend:
    def __init__(self, cpus: float | None = 100.0) -> None:
        self.cpus = cpus
        self.sequence = 0
        self.submissions: list[tuple[Any, Any, str]] = []
        self.ready: dict[str, ExecutionResult] = {}
        self.values: dict[str, Any] = {}
        self.fail_submissions = 0
        self.fetch_submissions: list[tuple[Any, Any, Any, str]] = []
        self.merge_submissions: list[tuple[Any, tuple[str, ...], tuple[Any, ...], str]] = []
        self.data_refs: dict[str, Any] = {}
        self.resource_loader = ResourceLoader()
        self._actors_seen: set[str] = set()

    def prepare_actor_restore(self, worker: HandlerSpec) -> bool:
        if worker.mode is not ExecutionMode.ACTOR:
            return False
        if worker.name in self._actors_seen:
            return False
        self._actors_seen.add(worker.name)
        return True

    def drop_actor(self, handler_name: str) -> None:
        self._actors_seen.discard(handler_name)

    def submit(
        self, worker: HandlerSpec, request: Any, data_ref: Any = None
    ) -> str:
        if self.fail_submissions:
            self.fail_submissions -= 1
            raise RuntimeError("temporary submit failure")
        self.sequence += 1
        ref = f"ref-{self.sequence}"
        self.submissions.append((worker, request, ref))
        self.data_refs[ref] = data_ref
        return ref

    def submit_fetch(self, worker: HandlerSpec, request: Any, source: Any, **_: Any) -> str:
        self.sequence += 1
        ref = f"fetch-ref-{self.sequence}"
        self.fetch_submissions.append((worker, request, source, ref))
        return ref

    def submit_merge(
        self,
        worker: HandlerSpec,
        source_ids: tuple[str, ...],
        fetch_refs: Any,
    ) -> str:
        from ray_dispatcher.readers import merge_fetch_results

        self.sequence += 1
        ref = f"merge-ref-{self.sequence}"
        refs = tuple(fetch_refs)
        payloads = [self.values.get(item, []) for item in refs]
        merged = merge_fetch_results(tuple(source_ids), *payloads)
        self.merge_submissions.append((worker, tuple(source_ids), refs, ref))
        self.values[ref] = merged
        return ref

    def poll(self, refs: Mapping[str, Any]) -> Mapping[str, ExecutionResult]:
        results: dict[str, ExecutionResult] = {}
        for run_id, ref in refs.items():
            if ref in self.ready:
                results[run_id] = self.ready.pop(ref)
        return results

    def available_cpus(self) -> float | None:
        return self.cpus

    async def get(self, ref: Any) -> Any:
        if ref not in self.values:
            raise KeyError(f"unknown ref for get: {ref!r}")
        return self.values[ref]

    def finish(self, ref: str, *, value: Any = None, error: str | None = None) -> None:
        if error is None and value is None and ref in self.values:
            value = self.values[ref]
        self.ready[ref] = ExecutionResult(error is None, value=value, error=error)
        if error is None:
            self.values[ref] = value


class ReadyObjectRef:
    def __init__(self, value: Any) -> None:
        self.value = value

    def __await__(self):
        async def completed() -> Any:
            return self.value

        return completed().__await__()


class FakeNativeRayModule:
    @staticmethod
    def wait(refs: list[Any], *, num_returns: int, timeout: int):
        assert timeout == 0
        return refs[:num_returns], refs[num_returns:]

    @staticmethod
    def available_resources() -> dict[str, float]:
        return {"CPU": 2.0}


class RecordingRemoteFunction:
    def __init__(self) -> None:
        self.options_calls: list[dict[str, Any]] = []
        self.remote_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def options(self, **kwargs: Any) -> "RecordingRemoteFunction":
        self.options_calls.append(kwargs)
        return self

    def remote(self, *args: Any, **kwargs: Any) -> str:
        self.remote_calls.append((args, kwargs))
        return "object-ref"


class RecordingActorClass:
    def __init__(self) -> None:
        self.options_calls: list[dict[str, Any]] = []
        self.remote_instances: list["RecordingActorInstance"] = []

    def options(self, **kwargs: Any) -> "RecordingActorClass":
        self.options_calls.append(kwargs)
        return self

    def remote(self) -> "RecordingActorInstance":
        instance = RecordingActorInstance()
        self.remote_instances.append(instance)
        return instance


class RecordingActorInstance:
    def __init__(self) -> None:
        self.process = RecordingRemoteFunction()


class RayDispatcherTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _shared_workers(source: KafkaSource) -> tuple[HandlerSpec, HandlerSpec]:
        common = {
            "sources": (source,),
            "batch_size": (1, 10),
        }
        return (
            HandlerSpec("json-handler", object(), **common),
            HandlerSpec("csv-handler", object(), **common),
        )

    async def _complete_fetches(
        self,
        dispatcher: RayDispatcher,
        backend: FakeRayBackend,
        *,
        value: Any | None = None,
    ) -> None:
        pending = [
            (request, ref)
            for _, request, _, ref in backend.fetch_submissions
            if ref not in backend.values and ref not in backend.ready
        ]
        for request, ref in pending:
            if value is not None:
                payload = value
            elif request.start_offset is not None and request.end_offset is not None:
                payload = [
                    {"offset": offset}
                    for offset in range(request.start_offset, request.end_offset)
                ]
            else:
                payload = [{"row": True}]
            backend.finish(ref, value=payload)
        await dispatcher.ray_status()

    async def test_shared_source_fetches_once_and_commits_after_all_handlers(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        checkpoints = MemoryCheckpointStore()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        workers = self._shared_workers(source)
        dispatcher = RayDispatcher(
            workers,
            ray_backend=backend,
            checkpoint_store=checkpoints,
            config=DispatcherConfig(max_in_flight=3),
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 10)}

        backlog = await dispatcher.data_listener()
        fetch_ids = await dispatcher.ray_trigger()

        key = "shared:events:0"
        self.assertEqual({key: 10}, backlog)
        self.assertEqual(1, len(fetch_ids))
        self.assertEqual(1, len(backend.fetch_submissions))
        self.assertEqual([], backend.submissions)
        self.assertIs(source, backend.fetch_submissions[0][2])

        await self._complete_fetches(dispatcher, backend)

        self.assertEqual(2, len(backend.submissions))
        fetch_ref = backend.fetch_submissions[0][3]
        self.assertTrue(
            all(
                backend.data_refs[handler_ref] == fetch_ref
                for _, _, handler_ref in backend.submissions
            )
        )
        self.assertEqual(0, checkpoints.values[key].progress)

        backend.finish(backend.submissions[0][2], value="json-ok")
        await dispatcher.ray_status()
        self.assertEqual(0, checkpoints.values[key].progress)

        backend.finish(backend.submissions[1][2], value="csv-ok")
        await dispatcher.ray_status()
        self.assertEqual(10, checkpoints.values[key].progress)
        self.assertEqual(0, dispatcher.state.sources[key].backlog)
        batch = next(iter(dispatcher.state.batches.values()))
        self.assertEqual(BatchStatus.SUCCEEDED, batch.status)
        self.assertTrue(
            all(
                dispatcher.state.runs[run_id].ref is None
                for run_id in [*batch.fetch_run_ids, *batch.run_ids]
            )
        )

    async def test_single_handler_also_uses_fetch_path(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        checkpoints = MemoryCheckpointStore()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec(
            "worker", object(), (source,), batch_size=(1, 10)
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}

        backlog = await dispatcher.data_listener()
        key = "shared:events:0"
        self.assertEqual({key: 5}, backlog)
        fetch_ids = await dispatcher.ray_trigger()
        self.assertEqual(1, len(fetch_ids))
        self.assertEqual(1, len(backend.fetch_submissions))
        self.assertEqual([], backend.submissions)

        await self._complete_fetches(dispatcher, backend)
        self.assertEqual(1, len(backend.submissions))
        self.assertEqual(
            backend.fetch_submissions[0][3],
            backend.data_refs[backend.submissions[0][2]],
        )

        backend.finish(backend.submissions[0][2], value="ok")
        await dispatcher.ray_status()
        self.assertEqual(5, checkpoints.values[key].progress)

    async def test_shared_handler_retry_reuses_fetch_ref(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        workers = self._shared_workers(source)
        dispatcher = RayDispatcher(
            workers,
            ray_backend=backend,
            config=DispatcherConfig(max_in_flight=3),
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 4)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend, value=[1, 2, 3, 4])
        fetch_ref = backend.fetch_submissions[0][3]

        first_handler_ref = backend.submissions[0][2]
        failed_handler_ref = backend.submissions[1][2]
        backend.finish(first_handler_ref, value="ok")
        backend.finish(failed_handler_ref, error="temporary sink error")
        await dispatcher.ray_status()

        self.assertEqual(1, len(backend.fetch_submissions))
        self.assertEqual(3, len(backend.submissions))
        retry_ref = backend.submissions[-1][2]
        self.assertEqual(fetch_ref, backend.data_refs[retry_ref])

        backend.finish(retry_ref, value="retry-ok")
        await dispatcher.ray_status()
        self.assertEqual(4, dispatcher.state.sources["shared:events:0"].committed)

    async def test_kafka_batch_window_single_wave_and_gates(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        checkpoints = MemoryCheckpointStore()
        source = KafkaSource(
            "events", ("broker:9092",), "events", connection_id="primary-kafka"
        )
        worker = HandlerSpec(
            "event-worker",
            object(),
            (source,),
            batch_size=10,
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            config=DispatcherConfig(max_in_flight=10),
        )
        dispatcher.source_observer = client

        client.kafka["events"] = {0: (0, 100)}
        key = "shared:events:0"
        self.assertEqual({key: 0}, await dispatcher.data_listener())
        self.assertEqual(100, checkpoints.values[key].progress)

        client.kafka["events"] = {0: (0, 125)}
        backlog = await dispatcher.data_listener()
        self.assertEqual(25, backlog[key])

        # int 10 => [10,]: one wave takes the full backlog.
        fetch_ids = await dispatcher.ray_trigger()
        self.assertEqual(1, len(fetch_ids))
        request = backend.fetch_submissions[0][1]
        self.assertEqual((100, 125), (request.start_offset, request.end_offset))
        self.assertEqual(1, request.n)
        self.assertEqual("events", request.topic)
        self.assertEqual("primary-kafka", request.source_connection_id)

        await self._complete_fetches(dispatcher, backend)
        self.assertEqual(1, len(backend.submissions))
        backend.finish(backend.submissions[0][2], value="ok")
        await dispatcher.ray_status()
        self.assertEqual(125, checkpoints.values[key].progress)
        self.assertEqual(0, dispatcher.state.sources[key].backlog)

        # Cap with [,10].
        backend2 = FakeRayBackend()
        dispatcher2 = RayDispatcher(
            (HandlerSpec("cap", object(), (source,), batch_size=(None, 10)),),
            ray_backend=backend2,
            checkpoint_store=MemoryCheckpointStore({key: 100}),
            config=DispatcherConfig(max_in_flight=10),
        )
        dispatcher2.source_observer = client
        await dispatcher2.data_listener()
        self.assertEqual(1, len(await dispatcher2.ray_trigger()))
        capped = backend2.fetch_submissions[0][1]
        self.assertEqual((100, 110), (capped.start_offset, capped.end_offset))

        # Min gate [20,] with backlog 15.
        backend3 = FakeRayBackend()
        dispatcher3 = RayDispatcher(
            (HandlerSpec("min", object(), (source,), batch_size=(20, None)),),
            ray_backend=backend3,
            checkpoint_store=MemoryCheckpointStore({key: 110}),
            config=DispatcherConfig(max_in_flight=10),
        )
        dispatcher3.source_observer = client
        await dispatcher3.data_listener()
        self.assertEqual(15, dispatcher3.state.sources[key].backlog)
        self.assertEqual([], await dispatcher3.ray_trigger())

    async def test_failed_ref_is_retried_with_same_dispatch_id(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = HandlerSpec(
            "worker", object(), (source,), batch_size=(1, 100), max_retries=1
        )
        dispatcher = RayDispatcher((worker,), ray_backend=backend)
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        [run_id] = [
            run_id
            for run_id, run in dispatcher.state.runs.items()
            if run.kind == "handler"
        ]
        original_dispatch_id = backend.submissions[0][1].dispatch_id

        backend.finish(backend.submissions[0][2], error="transient")
        statuses = await dispatcher.ray_status()
        self.assertEqual(RunStatus.SUBMITTED, statuses[run_id])
        self.assertEqual(original_dispatch_id, backend.submissions[1][1].dispatch_id)
        self.assertEqual(2, dispatcher.state.runs[run_id].attempt)

        backend.finish(backend.submissions[1][2], value="ok")
        await dispatcher.ray_status()
        self.assertEqual(5, dispatcher.state.sources["shared:events:0"].committed)

    async def test_initial_submission_failure_is_automatically_retried(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        backend.fail_submissions = 1
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = HandlerSpec(
            "worker", object(), (source,), max_retries=1, batch_size=0
        )
        dispatcher = RayDispatcher((worker,), ray_backend=backend)
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)

        [run_id] = [
            run_id
            for run_id, run in dispatcher.state.runs.items()
            if run.kind == "handler"
        ]
        self.assertIsNone(dispatcher.state.runs[run_id].ref)

        statuses = await dispatcher.ray_status()
        self.assertEqual(RunStatus.RUNNING, statuses[run_id])
        self.assertIsNotNone(dispatcher.state.runs[run_id].ref)
        self.assertEqual(2, dispatcher.state.runs[run_id].attempt)

        backend.finish(backend.submissions[-1][2], value="ok")
        await dispatcher.ray_status()
        self.assertEqual(5, dispatcher.state.sources["shared:events:0"].committed)

    async def test_postgres_composite_cursor_window_uses_task_slices(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        start = PostgresCursor(datetime(2026, 1, 1, tzinfo=timezone.utc), "0")
        end = PostgresCursor(start.timestamp + timedelta(minutes=1), "999")
        source = PostgresSource(
            "orders",
            "postgresql://example/db",
            "orders",
            "updated_at",
            "id",
            initial_cursor=start,
        )
        worker = HandlerSpec(
            "order-worker", object(), (source,), batch_size=(1, 10)
        )
        checkpoints = MemoryCheckpointStore()
        dispatcher = RayDispatcher(
            (worker,), ray_backend=backend, checkpoint_store=checkpoints
        )
        dispatcher.source_observer = client
        client.pg_upper["orders"] = end
        client.pg_count["orders"] = 25

        key = "shared:orders:orders"
        backlog = await dispatcher.data_listener()
        self.assertEqual(25, backlog[key])
        fetch_ids = await dispatcher.ray_trigger()
        # Custom clients without postgres_ranges safely use one task instead of
        # process-dependent Python hash partitioning.
        self.assertEqual(1, len(fetch_ids))
        await self._complete_fetches(dispatcher, backend)
        fetch_requests = [item[1] for item in backend.fetch_submissions]
        self.assertTrue(all(request.start_cursor == start for request in fetch_requests))
        self.assertTrue(all(request.end_cursor == end for request in fetch_requests))
        self.assertEqual([0], [request.task_index for request in fetch_requests])
        requests = [item[1] for item in backend.submissions]
        self.assertTrue(all(isinstance(request, HandlerRequest) for request in requests))
        self.assertEqual(
            {"order-worker"},
            {request.handler_id for request in requests},
        )

        for _, _, ref in backend.submissions:
            backend.finish(ref, value={"processed": True})
        await dispatcher.ray_status()
        self.assertEqual(end, checkpoints.values[key].progress)

    async def test_postgres_numeric_primary_keys_keep_database_order(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start = PostgresCursor(timestamp, 9)
        end = PostgresCursor(timestamp, 10)
        source = PostgresSource(
            "orders", "dsn", "orders", "updated_at", "id", initial_cursor=start
        )
        worker = HandlerSpec("worker", object(), (source,))
        dispatcher = RayDispatcher((worker,), ray_backend=backend)
        dispatcher.source_observer = client
        client.pg_upper["orders"] = end
        client.pg_count["orders"] = 1

        backlog = await dispatcher.data_listener()
        self.assertEqual(1, backlog["shared:orders:orders"])
        self.assertEqual(1, client.pg_count_calls)

    async def test_retention_gap_fails_without_silently_skipping_data(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource("events", ("broker",), "events")
        worker = HandlerSpec("worker", object(), (source,))
        key = "shared:events:0"
        checkpoints = MemoryCheckpointStore({key: 5})
        dispatcher = RayDispatcher(
            (worker,), ray_backend=backend, checkpoint_store=checkpoints
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (10, 20)}

        with self.assertRaises(KafkaRetentionGap):
            await dispatcher.data_listener()
        self.assertIn("below retained", dispatcher.state.sources[key].retention_gap)

    async def test_explicit_retention_reset_resumes_from_new_low_watermark(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events",
            ("broker",),
            "events",
            retention_policy="reset_to_earliest",
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=(1, 100))
        key = "shared:events:0"
        checkpoints = MemoryCheckpointStore({key: 5})
        dispatcher = RayDispatcher(
            (worker,), ray_backend=backend, checkpoint_store=checkpoints
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (10, 20)}

        backlog = await dispatcher.data_listener()
        self.assertEqual(10, backlog[key])
        self.assertIsNone(dispatcher.state.sources[key].retention_gap)
        self.assertEqual(10, checkpoints.values[key].progress)
        self.assertEqual(1, len(await dispatcher.ray_trigger()))

    async def test_retention_reset_does_not_overwrite_an_active_batch(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events",
            ("broker",),
            "events",
            initial_offset="earliest",
            retention_policy="reset_to_earliest",
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=0)
        checkpoints = MemoryCheckpointStore()
        dispatcher = RayDispatcher(
            (worker,), ray_backend=backend, checkpoint_store=checkpoints
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 20)}
        key = "shared:events:0"
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)

        client.kafka["events"] = {0: (30, 40)}
        with self.assertRaises(KafkaRetentionGap):
            await dispatcher.data_listener()
        self.assertEqual(0, checkpoints.values[key].progress)

        backend.finish(backend.submissions[0][2], value="old range completed")
        await dispatcher.ray_status()
        self.assertEqual(20, checkpoints.values[key].progress)
        backlog = await dispatcher.data_listener()
        self.assertEqual(10, backlog[key])
        self.assertEqual(30, checkpoints.values[key].progress)

    async def test_actor_pool_can_accept_a_second_wave_when_free_cpu_is_zero(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend(cpus=1)
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = HandlerSpec(
            "worker",
            object(),
            (source,),
            mode=ExecutionMode.ACTOR,
            remote_method="process",
            batch_size=0,
        )
        dispatcher = RayDispatcher((worker,), ray_backend=backend)
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 1)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        backend.finish(backend.submissions[0][2], value="first")
        await dispatcher.ray_status()

        client.kafka["events"] = {0: (0, 2)}
        await dispatcher.data_listener()
        # Actor CPUs are already reserved; disable free-CPU gating so the next
        # fetch/handler wave can still schedule.
        backend.cpus = None
        second_wave = await dispatcher.ray_trigger()
        self.assertEqual(1, len(second_wave))

    def test_native_backend_reuses_a_single_actor_per_handler(self) -> None:
        backend = NativeRayBackend(FakeNativeRayModule())
        actor_cls = RecordingActorClass()
        source = KafkaSource("events", ("broker",), "events")
        worker = HandlerSpec(
            "worker",
            actor_cls,
            (source,),
            mode=ExecutionMode.ACTOR,
            remote_method="process",
        )
        request = HandlerRequest(dispatch_id="dispatch", handler_id="worker")
        data_ref = object()

        backend.submit(worker, request, data_ref)
        backend.submit(worker, request, data_ref)
        backend.submit(worker, request, data_ref)

        self.assertEqual(1, len(actor_cls.remote_instances))
        self.assertEqual(3, len(actor_cls.remote_instances[0].process.remote_calls))
        self.assertEqual(
            (request, data_ref),
            actor_cls.remote_instances[0].process.remote_calls[0][0],
        )

    def test_native_backend_loads_resource_snapshot_once(self) -> None:
        data = {1: {"name": "alice"}}
        loader = ResourceLoader(
            {
                "user-dim-v1": ResourceSpec(
                    "user-dim-v1", kind="static", data=data
                )
            }
        )
        backend = NativeRayBackend(FakeNativeRayModule(), resource_loader=loader)
        remote = RecordingRemoteFunction()
        source = KafkaSource("events", ("broker",), "events")
        worker = HandlerSpec(
            "worker",
            remote,
            (source,),
            resource_ids=("user-dim-v1",),
        )
        request = HandlerRequest(dispatch_id="dispatch", handler_id="worker")
        data_ref = object()

        backend.submit(worker, request, data_ref)
        backend.submit(worker, request, data_ref)

        self.assertIs(loader.get("user-dim-v1"), data)
        resources = {"user-dim-v1": data}
        self.assertEqual(
            ((request, data_ref, resources), {}),
            remote.remote_calls[0],
        )
        self.assertEqual(remote.remote_calls[0][0][2], remote.remote_calls[1][0][2])
        self.assertIs(
            remote.remote_calls[0][0][2]["user-dim-v1"],
            remote.remote_calls[1][0][2]["user-dim-v1"],
        )

    def test_trigger_rank_key_is_progressive(self) -> None:
        trigger = TriggerPolicy.default()
        source = KafkaSource("events", ("broker",), "events")
        high = HandlerSpec("high", object(), (source,), priority=1)
        low = HandlerSpec("low", object(), (source,), priority=0)
        now = 100.0
        most = SourceState(
            "a", "low", "events", SourceKind.KAFKA, "0", 0, 100, backlog=100
        )
        slow = SourceState(
            "b",
            "low",
            "events",
            SourceKind.KAFKA,
            "1",
            0,
            50,
            backlog=50,
            processing_rate=1.0,
            last_scheduled_at=now,
        )
        fast = SourceState(
            "c",
            "low",
            "events",
            SourceKind.KAFKA,
            "2",
            0,
            50,
            backlog=50,
            processing_rate=10.0,
            last_scheduled_at=now,
        )
        ranked = SourceState(
            "d", "high", "events", SourceKind.KAFKA, "3", 0, 1, backlog=1
        )

        ordered = sorted(
            [most, slow, fast, ranked],
            key=lambda state: trigger.rank_key(
                high if state.worker_name == "high" else low, state, now
            ),
            reverse=True,
        )
        self.assertEqual(["d", "a", "b", "c"], [state.key for state in ordered])

    async def test_permanent_failure_skips_range_and_unblocks_source(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        checkpoints = MemoryCheckpointStore()
        failures = MemoryFailureStore()
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = HandlerSpec(
            "worker",
            object(),
            (source,),
            batch_size=(1, 5),
            max_retries=0,
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            failure_store=failures,
            # One slice needs fetch + handler; keep a single batch in flight.
            config=DispatcherConfig(max_in_flight=2),
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 12)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        key = "shared:events:0"
        payload = [{"offset": offset} for offset in range(5)]
        await self._complete_fetches(dispatcher, backend, value=payload)
        [run_id] = [
            run_id
            for run_id, run in dispatcher.state.runs.items()
            if run.kind == "handler"
        ]
        self.assertEqual(0, dispatcher.state.sources[key].committed)
        self.assertIsNotNone(dispatcher.state.sources[key].active_batch_id)

        backend.finish(backend.submissions[0][2], error="poison")
        await dispatcher.ray_status()

        state = dispatcher.state.sources[key]
        batch = next(iter(dispatcher.state.batches.values()))
        self.assertEqual(RunStatus.FAILED, dispatcher.state.runs[run_id].status)
        self.assertEqual(BatchStatus.FAILED, batch.status)
        self.assertEqual(5, state.committed)
        self.assertEqual(5, checkpoints.values[key].progress)
        self.assertIsNone(state.active_batch_id)
        self.assertEqual(7, state.backlog)
        recorded = await failures.list_failures()
        self.assertEqual(1, len(recorded))
        self.assertEqual(batch.batch_id, recorded[0].batch_id)
        self.assertEqual(0, recorded[0].start)
        self.assertEqual(5, recorded[0].end)
        self.assertEqual(payload, recorded[0].payload)
        self.assertIn("poison", recorded[0].runs[-1].error or "")
        self.assertFalse(hasattr(dispatcher, "retry_failed_batch"))

        next_ids = await dispatcher.ray_trigger()
        self.assertEqual(1, len(next_ids))
        self.assertEqual(5, backend.fetch_submissions[-1][1].start_offset)
        self.assertEqual(10, backend.fetch_submissions[-1][1].end_offset)

    async def test_shared_permanent_failure_records_fetch_payload(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        failures = MemoryFailureStore()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        common = {
            "sources": (source,),
            "batch_size": (1, 10),
            "max_retries": 0,
        }
        workers = (
            HandlerSpec("json-handler", object(), **common),
            HandlerSpec("csv-handler", object(), **common),
        )
        dispatcher = RayDispatcher(
            workers,
            ray_backend=backend,
            failure_store=failures,
            config=DispatcherConfig(max_in_flight=8),
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 4)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        payload = [{"offset": 0}, {"offset": 1}, {"offset": 2}, {"offset": 3}]
        await self._complete_fetches(dispatcher, backend, value=payload)
        backend.finish(backend.submissions[0][2], value="ok")
        backend.finish(backend.submissions[1][2], error="poison sink")
        await dispatcher.ray_status()

        recorded = await failures.list_failures()
        self.assertEqual(1, len(recorded))
        self.assertEqual(payload, recorded[0].payload)
        self.assertTrue(
            any("poison sink" in (run.error or "") for run in recorded[0].runs)
        )
        key = "shared:events:0"
        self.assertEqual(4, dispatcher.state.sources[key].committed)
        self.assertIsNone(dispatcher.state.sources[key].active_batch_id)

    async def test_periodic_loops_start_and_stop_cleanly(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource("events", ("broker",), "events")
        worker = HandlerSpec("worker", object(), (source,))
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            listener_interval=0.01,
            trigger_interval=0.01,
            status_interval=0.01,
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 0)}
        await dispatcher.start()
        await asyncio.sleep(0.03)
        await dispatcher.stop()
        self.assertEqual([], dispatcher.state.loop_errors)

    async def test_native_backend_poll_maps_original_object_refs(self) -> None:
        backend = NativeRayBackend(FakeNativeRayModule())
        first = ReadyObjectRef("first-result")
        second = ReadyObjectRef("second-result")

        results = await backend.poll({"run-1": first, "run-2": second})

        self.assertEqual({"run-1", "run-2"}, set(results))
        self.assertEqual("first-result", results["run-1"].value)
        self.assertEqual("second-result", results["run-2"].value)

    async def test_native_backend_passes_shared_ref_as_top_level_argument(self) -> None:
        backend = NativeRayBackend(FakeNativeRayModule())
        remote = RecordingRemoteFunction()
        source = KafkaSource("events", ("broker",), "events")
        worker = HandlerSpec("worker", remote, (source,))
        request = HandlerRequest(dispatch_id="dispatch", handler_id="worker")
        data_ref = object()

        result = backend.submit(worker, request, data_ref)

        self.assertEqual("object-ref", result)
        self.assertEqual(((request, data_ref), {}), remote.remote_calls[0])

    async def test_sqlite_checkpoints_survive_store_recreation(self) -> None:
        cursor = PostgresCursor(
            datetime(2026, 1, 1, tzinfo=timezone.utc), 123
        )
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/dispatcher.sqlite3"
            first_store = SQLiteCheckpointStore(path)
            await first_store.save("shared:events:0", 42)
            await first_store.save("shared:orders:orders", cursor)

            restarted_store = SQLiteCheckpointStore(path)
            loaded_events = await restarted_store.load("shared:events:0")
            loaded_orders = await restarted_store.load("shared:orders:orders")
            self.assertIsNotNone(loaded_events)
            self.assertIsNotNone(loaded_orders)
            assert loaded_events is not None
            assert loaded_orders is not None
            self.assertEqual(42, loaded_events.progress)
            self.assertIsNone(loaded_events.state)
            self.assertEqual(cursor, loaded_orders.progress)
            self.assertIsNone(loaded_orders.state)

    async def test_multisource_window_merges_records_dict(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t_right = t0 + timedelta(seconds=100)
        orders = KafkaSource(
            "orders", ("broker",), "orders", initial_offset="earliest"
        )
        payments = KafkaSource(
            "payments", ("broker",), "payments", initial_offset="earliest"
        )
        worker = HandlerSpec(
            "join-handler",
            object(),
            (orders, payments),
            batch_size=(1, 100),
        )
        checkpoints = MemoryCheckpointStore(
            {RayDispatcher._mswin_checkpoint_key(worker.group_key): t0}
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            config=DispatcherConfig(max_in_flight=16, max_window_seconds=60.0),
        )
        dispatcher.source_observer = client
        client.kafka["orders"] = {0: (0, 5)}
        client.kafka["payments"] = {0: (0, 3)}
        client.event_time_highs["orders"] = {0: t_right}
        client.event_time_highs["payments"] = {0: t_right}
        client.offsets_for_times["orders"] = {
            0: [(t0, 0), (t0 + timedelta(seconds=60), 4), (t_right, 5)]
        }
        client.offsets_for_times["payments"] = {
            0: [(t0, 0), (t0 + timedelta(seconds=60), 2), (t_right, 3)]
        }

        await dispatcher.data_listener()
        window = dispatcher.state.multisource_windows[worker.group_key]
        self.assertEqual(t0, window.committed_time)
        self.assertEqual(t_right, window.observed_time)

        fetch_ids = await dispatcher.ray_trigger()
        self.assertEqual(2, len(fetch_ids))
        self.assertEqual(2, len(backend.fetch_submissions))
        fetched_sources = {source.source_id for _, _, source, _ in backend.fetch_submissions}
        self.assertEqual({"orders", "payments"}, fetched_sources)

        await self._complete_fetches(dispatcher, backend)
        self.assertEqual(1, len(backend.merge_submissions))
        merge_ref = backend.merge_submissions[0][3]
        backend.finish(merge_ref)
        await dispatcher.ray_status()

        self.assertEqual(1, len(backend.submissions))
        handler_ref = backend.submissions[0][2]
        data_ref = backend.data_refs[handler_ref]
        self.assertEqual(merge_ref, data_ref)
        merged = backend.values[merge_ref]
        self.assertIsInstance(merged, dict)
        self.assertEqual({"orders", "payments"}, set(merged))
        self.assertEqual(
            [{"offset": 0}, {"offset": 1}, {"offset": 2}, {"offset": 3}],
            merged["orders"],
        )
        self.assertEqual(
            [{"offset": 0}, {"offset": 1}],
            merged["payments"],
        )

        backend.finish(handler_ref, value="joined")
        await dispatcher.ray_status()
        self.assertEqual(
            t0 + timedelta(seconds=60),
            checkpoints.values[
                RayDispatcher._mswin_checkpoint_key(worker.group_key)
            ].progress,
        )
        self.assertEqual(4, checkpoints.values["shared:orders:0"].progress)
        self.assertEqual(2, checkpoints.values["shared:payments:0"].progress)
        self.assertIsNone(window.active_batch_id)
        batch = next(iter(dispatcher.state.batches.values()))
        self.assertEqual(BatchStatus.SUCCEEDED, batch.status)

    async def test_multisource_missing_offset_for_times_uses_high(self) -> None:
        """Missing offsets_for_times must not fall back to low (silent catch-up)."""

        client = FakeSourceObserver()
        backend = FakeRayBackend()
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t_right = t0 + timedelta(seconds=100)
        orders = KafkaSource(
            "orders", ("broker",), "orders", initial_offset="earliest"
        )
        payments = KafkaSource(
            "payments", ("broker",), "payments", initial_offset="earliest"
        )
        worker = HandlerSpec(
            "join-handler",
            object(),
            (orders, payments),
            batch_size=(1, 100),
        )
        checkpoints = MemoryCheckpointStore(
            {RayDispatcher._mswin_checkpoint_key(worker.group_key): t0}
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            config=DispatcherConfig(max_in_flight=16, max_window_seconds=60.0),
        )
        dispatcher.source_observer = client
        client.kafka["orders"] = {0: (0, 10)}
        client.kafka["payments"] = {0: (0, 10)}
        client.event_time_highs["orders"] = {0: t_right}
        client.event_time_highs["payments"] = {0: t_right}
        # Only right-bound answers for payments: left bound missing → high.
        client.offsets_for_times["orders"] = {
            0: [(t0, 0), (t0 + timedelta(seconds=60), 4), (t_right, 5)]
        }
        client.offsets_for_times["payments"] = {
            0: [(t0 + timedelta(seconds=60), 10), (t_right, 10)]
        }

        await dispatcher.data_listener()
        fetch_ids = await dispatcher.ray_trigger()
        self.assertEqual(1, len(fetch_ids))
        self.assertEqual(1, len(backend.fetch_submissions))
        _worker, request, source, _ref = backend.fetch_submissions[0]
        self.assertEqual("orders", source.source_id)
        self.assertEqual(0, request.start_offset)
        self.assertEqual(4, request.end_offset)

    async def test_sqlite_legacy_and_document_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/checkpoints.sqlite3"
            store = SQLiteCheckpointStore(path)
            # Simulate a legacy flat row.
            import sqlite3

            connection = sqlite3.connect(path)
            connection.execute(
                """
                INSERT INTO ray_dispatcher_checkpoints (
                    checkpoint_key, value_type, value_json, updated_at
                ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                ("shared:legacy:0", "kafka_offset", "7"),
            )
            connection.commit()
            connection.close()

            legacy = await store.load("shared:legacy:0")
            self.assertEqual(
                CheckpointDocument(progress=7, state=None), legacy
            )

            await store.save(
                "actor:worker:shared:events:0",
                CheckpointDocument(progress=10, state={"cache": [1, 2]}),
            )
            await store.save_many(
                {
                    "shared:events:0": 10,
                    "actor:other:shared:events:0": CheckpointDocument(
                        progress=10, state={"n": 1}
                    ),
                }
            )
            restarted = SQLiteCheckpointStore(path)
            doc = await restarted.load("actor:worker:shared:events:0")
            self.assertEqual(
                CheckpointDocument(progress=10, state={"cache": [1, 2]}), doc
            )
            shared = await restarted.load("shared:events:0")
            self.assertEqual(CheckpointDocument(progress=10, state=None), shared)
            other = await restarted.load("actor:other:shared:events:0")
            self.assertEqual(
                CheckpointDocument(progress=10, state={"n": 1}), other
            )

    async def test_handler_checkpoint_state_commits_and_restores_on_actor(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec(
            "stateful",
            object(),
            (source,),
            mode=ExecutionMode.ACTOR,
            remote_method="process",
            batch_size=(1, 5),
            max_retries=0,
        )
        checkpoints = MemoryCheckpointStore()
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            config=DispatcherConfig(max_in_flight=8),
        )
        dispatcher.source_observer = client
        key = "shared:events:0"
        client.kafka["events"] = {0: (0, 5)}

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        self.assertEqual(1, len(backend.submissions))
        first_request = backend.submissions[0][1]
        self.assertIsNone(first_request.checkpoint_state)
        handler_ref = backend.submissions[0][2]
        backend.finish(
            handler_ref,
            value={"ok": True, "checkpoint_state": {"seen": 5}},
        )
        await dispatcher.ray_status()

        self.assertEqual(5, checkpoints.values[key].progress)
        actor_key = RayDispatcher._actor_checkpoint_key("stateful", key)
        self.assertEqual(
            CheckpointDocument(progress=5, state={"seen": 5}),
            checkpoints.values[actor_key],
        )
        run = next(
            run
            for run in dispatcher.state.runs.values()
            if run.kind == "handler"
        )
        self.assertEqual({"ok": True}, run.result)
        self.assertEqual({"seen": 5}, run.checkpoint_state)

        backend.drop_actor("stateful")
        client.kafka["events"] = {0: (0, 10)}
        await dispatcher.data_listener()
        backend.cpus = None
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        self.assertEqual(2, len(backend.submissions))
        restored_request = backend.submissions[1][1]
        self.assertEqual({"seen": 5}, restored_request.checkpoint_state)

    async def test_actor_retry_clears_checkpoint_state(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec(
            "stateful",
            object(),
            (source,),
            mode=ExecutionMode.ACTOR,
            remote_method="process",
            batch_size=5,
            max_retries=1,
        )
        checkpoints = MemoryCheckpointStore(
            {
                RayDispatcher._actor_checkpoint_key("stateful", "shared:events:0"): (
                    CheckpointDocument(progress=0, state={"seen": 1})
                )
            }
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            config=DispatcherConfig(max_in_flight=8),
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        first_request = backend.submissions[0][1]
        self.assertEqual({"seen": 1}, first_request.checkpoint_state)

        backend.finish(backend.submissions[0][2], error="transient")
        await dispatcher.ray_status()
        self.assertEqual(2, len(backend.submissions))
        retry_request = backend.submissions[1][1]
        self.assertIsNone(retry_request.checkpoint_state)
        self.assertEqual(first_request.dispatch_id, retry_request.dispatch_id)

        backend.finish(backend.submissions[1][2], value={"ok": True})
        await dispatcher.ray_status()
        self.assertEqual(5, checkpoints.values["shared:events:0"].progress)

    async def test_multisource_fetch_failure_skips_window(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        failures = MemoryFailureStore()
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t_right = t0 + timedelta(seconds=100)
        orders = KafkaSource(
            "orders", ("broker",), "orders", initial_offset="earliest"
        )
        payments = KafkaSource(
            "payments", ("broker",), "payments", initial_offset="earliest"
        )
        worker = HandlerSpec(
            "join-handler",
            object(),
            (orders, payments),
            batch_size=(1, 100),
            max_retries=0,
        )
        checkpoints = MemoryCheckpointStore(
            {RayDispatcher._mswin_checkpoint_key(worker.group_key): t0}
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            failure_store=failures,
            config=DispatcherConfig(
                max_in_flight=16, max_window_seconds=60.0, fetch_max_retries=0
            ),
        )
        dispatcher.source_observer = client
        client.kafka["orders"] = {0: (0, 5)}
        client.kafka["payments"] = {0: (0, 3)}
        client.event_time_highs["orders"] = {0: t_right}
        client.event_time_highs["payments"] = {0: t_right}
        client.offsets_for_times["orders"] = {
            0: [(t0, 0), (t0 + timedelta(seconds=60), 4), (t_right, 5)]
        }
        client.offsets_for_times["payments"] = {
            0: [(t0, 0), (t0 + timedelta(seconds=60), 2), (t_right, 3)]
        }

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        self.assertEqual(2, len(backend.fetch_submissions))
        backend.finish(backend.fetch_submissions[0][3], error="orders fetch down")
        backend.finish(
            backend.fetch_submissions[1][3],
            value=[{"offset": 0}, {"offset": 1}],
        )
        await dispatcher.ray_status()

        window = dispatcher.state.multisource_windows[worker.group_key]
        self.assertEqual(t0 + timedelta(seconds=60), window.committed_time)
        self.assertIsNone(window.active_batch_id)
        self.assertEqual(
            t0 + timedelta(seconds=60),
            checkpoints.values[
                RayDispatcher._mswin_checkpoint_key(worker.group_key)
            ].progress,
        )
        recorded = await failures.list_failures()
        self.assertEqual(1, len(recorded))
        self.assertIn("orders fetch down", recorded[0].runs[0].error or "")

    async def test_multisource_handler_failure_skips_window(self) -> None:
        client = FakeSourceObserver()
        backend = FakeRayBackend()
        failures = MemoryFailureStore()
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t_right = t0 + timedelta(seconds=100)
        orders = KafkaSource(
            "orders", ("broker",), "orders", initial_offset="earliest"
        )
        payments = KafkaSource(
            "payments", ("broker",), "payments", initial_offset="earliest"
        )
        worker = HandlerSpec(
            "join-handler",
            object(),
            (orders, payments),
            batch_size=(1, 100),
            max_retries=0,
        )
        checkpoints = MemoryCheckpointStore(
            {RayDispatcher._mswin_checkpoint_key(worker.group_key): t0}
        )
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
            failure_store=failures,
            config=DispatcherConfig(max_in_flight=16, max_window_seconds=60.0),
        )
        dispatcher.source_observer = client
        client.kafka["orders"] = {0: (0, 5)}
        client.kafka["payments"] = {0: (0, 3)}
        client.event_time_highs["orders"] = {0: t_right}
        client.event_time_highs["payments"] = {0: t_right}
        client.offsets_for_times["orders"] = {
            0: [(t0, 0), (t0 + timedelta(seconds=60), 4), (t_right, 5)]
        }
        client.offsets_for_times["payments"] = {
            0: [(t0, 0), (t0 + timedelta(seconds=60), 2), (t_right, 3)]
        }

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        merge_ref = backend.merge_submissions[0][3]
        backend.finish(merge_ref)
        await dispatcher.ray_status()
        self.assertEqual(1, len(backend.submissions))
        backend.finish(backend.submissions[0][2], error="join sink down")
        await dispatcher.ray_status()

        window = dispatcher.state.multisource_windows[worker.group_key]
        self.assertEqual(t0 + timedelta(seconds=60), window.committed_time)
        self.assertIsNone(window.active_batch_id)
        recorded = await failures.list_failures()
        self.assertEqual(1, len(recorded))
        errors = [detail.error or "" for detail in recorded[0].runs]
        self.assertTrue(any("join sink down" in error for error in errors), errors)

    async def test_slow_data_listener_does_not_block_ray_status(self) -> None:
        """Remote observe IO must not hold _lock across awaits."""

        entered = asyncio.Event()
        status_finished = asyncio.Event()

        class SlowObserver(FakeSourceObserver):
            async def kafka_watermarks(
                self, source: KafkaSource
            ) -> Mapping[int, tuple[int, int]]:
                entered.set()
                await asyncio.sleep(0.2)
                return {0: (0, 5)}

        client = SlowObserver()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=(1, 5))
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=MemoryCheckpointStore(),
        )
        dispatcher.source_observer = client

        listener_task = asyncio.create_task(dispatcher.data_listener())
        await asyncio.wait_for(entered.wait(), timeout=1.0)

        status_started = time.monotonic()
        await dispatcher.ray_status()
        status_elapsed = time.monotonic() - status_started
        status_finished.set()

        backlog = await listener_task
        self.assertLess(
            status_elapsed,
            0.15,
            f"ray_status blocked for {status_elapsed:.3f}s during slow observe",
        )
        self.assertTrue(status_finished.is_set())
        self.assertEqual({"shared:events:0": 5}, backlog)

    async def test_slow_checkpoint_commit_does_not_block_data_listener(self) -> None:
        """Commit durability must not hold _lock across checkpoint IO."""

        entered = asyncio.Event()

        class SlowCheckpointStore(MemoryCheckpointStore):
            async def save_many(self, items: Mapping[str, Any]) -> None:
                entered.set()
                await asyncio.sleep(0.2)
                await super().save_many(items)

        client = FakeSourceObserver()
        backend = FakeRayBackend()
        checkpoints = SlowCheckpointStore()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=(1, 5), max_retries=0)
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=checkpoints,
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5)}

        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        await self._complete_fetches(dispatcher, backend)
        handler_ref = backend.submissions[0][2]
        backend.finish(handler_ref, value="ok")

        status_task = asyncio.create_task(dispatcher.ray_status())
        await asyncio.wait_for(entered.wait(), timeout=1.0)

        listener_started = time.monotonic()
        backlog = await dispatcher.data_listener()
        listener_elapsed = time.monotonic() - listener_started

        self.assertLess(
            listener_elapsed,
            0.15,
            f"data_listener blocked for {listener_elapsed:.3f}s during slow commit",
        )
        # Durability still in flight: committed not advanced in memory yet.
        self.assertEqual({"shared:events:0": 5}, backlog)
        await status_task
        self.assertEqual(0, dispatcher.state.sources["shared:events:0"].backlog)
        self.assertEqual(5, checkpoints.values["shared:events:0"].progress)
        self.assertEqual(
            BatchStatus.SUCCEEDED,
            next(iter(dispatcher.state.batches.values())).status,
        )


class TriggerPolicyTests(unittest.TestCase):
    def _ctx(
        self,
        *,
        backlog: int = 10,
        free_slots: int = 8,
        available_cpus: float | None = 4.0,
        active_batch_id: str | None = None,
        retention_gap: str | None = None,
        slots_per_slice: int = 2,
        phase_cpus: float = 1.0,
    ) -> TriggerContext:
        source = KafkaSource("events", ("broker",), "events")
        handler = HandlerSpec("worker", object(), (source,))
        state = SourceState(
            "events:0",
            "worker",
            "events",
            SourceKind.KAFKA,
            "0",
            0,
            backlog,
            backlog=backlog,
            active_batch_id=active_batch_id,
            retention_gap=retention_gap,
        )
        return TriggerContext(
            handler=handler,
            source_state=state,
            free_slots=free_slots,
            available_cpus=available_cpus,
            now=0.0,
            config=DispatcherConfig(),
            slots_per_slice=slots_per_slice,
            phase_cpus=phase_cpus,
        )

    def test_default_chain_triggers(self) -> None:
        decision = TriggerPolicy.default().evaluate(self._ctx())
        self.assertEqual(Decision.TRIGGER, decision.decision)

    def test_default_chain_skips_empty_backlog(self) -> None:
        decision = TriggerPolicy.default().evaluate(self._ctx(backlog=0))
        self.assertEqual(Decision.SKIP, decision.decision)
        self.assertEqual("has_backlog", decision.results[-1].name)

    def test_default_chain_blocks_without_capacity(self) -> None:
        decision = TriggerPolicy.default().evaluate(
            self._ctx(free_slots=1, slots_per_slice=2)
        )
        self.assertEqual(Decision.BLOCK, decision.decision)
        self.assertEqual("has_capacity", decision.results[-1].name)

    def test_target_reached_skips_until_min_items(self) -> None:
        policy = TriggerPolicy(
            conditions=(HasBacklog(), SourceIdle(), TargetReached(min_items=100), HasCapacity())
        )
        decision = policy.evaluate(self._ctx(backlog=50))
        self.assertEqual(Decision.SKIP, decision.decision)
        self.assertEqual("target_reached", decision.results[-1].name)

    def test_backlog_pressure_degrades_urgent(self) -> None:
        policy = TriggerPolicy(
            conditions=(
                HasBacklog(),
                SourceIdle(),
                BacklogPressure(max_backlog=5, on_fail=SoftAction.URGENT),
                HasCapacity(),
            )
        )
        decision = policy.evaluate(self._ctx(backlog=20))
        self.assertEqual(Decision.DEGRADE, decision.decision)
        self.assertEqual(SoftAction.URGENT, decision.soft_action)


class TriggerDispatcherIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_trigger_evaluated_event_and_block(self) -> None:
        from ray_dispatcher import EVENT_TRIGGER_EVALUATED, create_event_log

        client = FakeSourceObserver()
        backend = FakeRayBackend()
        events = create_event_log(default_logging=False)
        evaluated: list[Mapping[str, Any]] = []
        events.register(EVENT_TRIGGER_EVALUATED, evaluated.append)

        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        worker = HandlerSpec("worker", object(), (source,), batch_size=(1, 5), max_retries=0)
        # One slice needs fetch+handler (2 slots). max_in_flight=3 leaves 1 slot
        # after the first batch — enough to evaluate the next partition, not enough
        # to schedule → BLOCK via HasCapacity.
        dispatcher = RayDispatcher(
            (worker,),
            ray_backend=backend,
            checkpoint_store=MemoryCheckpointStore(),
            config=DispatcherConfig(max_in_flight=3),
            event_log=events,
        )
        dispatcher.source_observer = client
        client.kafka["events"] = {0: (0, 5), 1: (0, 5)}
        await dispatcher.data_listener()
        submitted = await dispatcher.ray_trigger()
        self.assertTrue(submitted)
        self.assertTrue(evaluated)
        self.assertEqual("trigger", evaluated[0]["decision"])

        evaluated.clear()
        more = await dispatcher.ray_trigger()
        self.assertEqual([], more)
        self.assertTrue(evaluated)
        self.assertEqual("block", evaluated[0]["decision"])


if __name__ == "__main__":
    unittest.main()
