from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from ray_dispatcher import (
    BatchStatus,
    DispatchRequest,
    ExecutionMode,
    ExecutionResult,
    KafkaRetentionGap,
    KafkaSource,
    MemoryCheckpointStore,
    NativeRayBackend,
    PostgresCursor,
    PostgresSource,
    RayDispatcher,
    RunStatus,
    SchedulingPolicy,
    SourceKind,
    SourceState,
    SQLiteCheckpointStore,
    WorkerSpec,
)


class FakeSourceClient:
    def __init__(self) -> None:
        self.kafka: dict[str, dict[int, tuple[int, int]]] = {}
        self.pg_upper: dict[str, PostgresCursor] = {}
        self.pg_count: dict[str, int] = {}
        self.pg_count_calls = 0

    async def kafka_watermarks(
        self, source: KafkaSource
    ) -> Mapping[int, tuple[int, int]]:
        return self.kafka[source.source_id]

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
    def __init__(self, cpus: float = 100.0) -> None:
        self.cpus = cpus
        self.sequence = 0
        self.submissions: list[tuple[Any, Any, str]] = []
        self.ready: dict[str, ExecutionResult] = {}
        self.fail_submissions = 0
        self.fetch_submissions: list[tuple[Any, Any, str]] = []
        self.data_refs: dict[str, Any] = {}

    def submit(
        self, worker: WorkerSpec, request: Any, data_ref: Any = None
    ) -> str:
        if self.fail_submissions:
            self.fail_submissions -= 1
            raise RuntimeError("temporary submit failure")
        self.sequence += 1
        ref = f"ref-{self.sequence}"
        self.submissions.append((worker, request, ref))
        self.data_refs[ref] = data_ref
        return ref

    def submit_fetch(self, worker: WorkerSpec, request: Any) -> str:
        self.sequence += 1
        ref = f"fetch-ref-{self.sequence}"
        self.fetch_submissions.append((worker, request, ref))
        return ref

    def poll(self, refs: Mapping[str, Any]) -> Mapping[str, ExecutionResult]:
        results: dict[str, ExecutionResult] = {}
        for run_id, ref in refs.items():
            if ref in self.ready:
                results[run_id] = self.ready.pop(ref)
        return results

    def available_cpus(self) -> float | None:
        return self.cpus

    def finish(self, ref: str, *, value: Any = None, error: str | None = None) -> None:
        self.ready[ref] = ExecutionResult(error is None, value=value, error=error)


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
    def _shared_workers(source: KafkaSource) -> tuple[WorkerSpec, WorkerSpec]:
        fetcher = object()
        common = {
            "sources": (source,),
            "max_parallelism": 2,
            "batch_size": 10,
            "shared_source_group": "events-fanout",
            "data_fetcher": fetcher,
            "fetcher_id": "events-reader-v1",
        }
        return (
            WorkerSpec("json-handler", object(), **common),
            WorkerSpec("csv-handler", object(), **common),
        )

    async def test_shared_source_fetches_once_and_commits_after_all_handlers(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        checkpoints = MemoryCheckpointStore()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        workers = self._shared_workers(source)
        dispatcher = RayDispatcher(
            workers,
            backend,
            source_client=client,
            checkpoint_store=checkpoints,
            policy=SchedulingPolicy(max_in_flight=3),
        )
        client.kafka["events"] = {0: (0, 10)}

        backlog = await dispatcher.data_listener()
        fetch_ids = await dispatcher.ray_trigger()

        key = "shared:events-fanout:events:0"
        self.assertEqual({key: 10}, backlog)
        self.assertEqual(1, len(fetch_ids))
        self.assertEqual(1, len(backend.fetch_submissions))
        self.assertEqual([], backend.submissions)

        fetch_ref = backend.fetch_submissions[0][2]
        backend.finish(fetch_ref, value=[{"offset": value} for value in range(10)])
        await dispatcher.ray_status()

        self.assertEqual(2, len(backend.submissions))
        self.assertTrue(
            all(
                backend.data_refs[handler_ref] == fetch_ref
                for _, _, handler_ref in backend.submissions
            )
        )
        self.assertEqual(0, checkpoints.values[key])

        backend.finish(backend.submissions[0][2], value="json-ok")
        await dispatcher.ray_status()
        self.assertEqual(0, checkpoints.values[key])

        backend.finish(backend.submissions[1][2], value="csv-ok")
        await dispatcher.ray_status()
        self.assertEqual(10, checkpoints.values[key])
        self.assertEqual(0, dispatcher.state.sources[key].backlog)
        batch = next(iter(dispatcher.state.batches.values()))
        self.assertEqual(BatchStatus.SUCCEEDED, batch.status)
        self.assertTrue(
            all(
                dispatcher.state.runs[run_id].ref is None
                for run_id in [*batch.fetch_run_ids, *batch.run_ids]
            )
        )

    async def test_shared_handler_retry_reuses_fetch_ref(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events", ("broker",), "events", initial_offset="earliest"
        )
        workers = self._shared_workers(source)
        dispatcher = RayDispatcher(
            workers,
            backend,
            source_client=client,
            policy=SchedulingPolicy(max_in_flight=3),
        )
        client.kafka["events"] = {0: (0, 4)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        fetch_ref = backend.fetch_submissions[0][2]
        backend.finish(fetch_ref, value=[1, 2, 3, 4])
        await dispatcher.ray_status()

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
        self.assertEqual(
            4,
            dispatcher.state.sources[
                "shared:events-fanout:events:0"
            ].committed,
        )

    async def test_kafka_increment_is_split_and_committed_after_all_refs_finish(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        checkpoints = MemoryCheckpointStore()
        source = KafkaSource(
            "events", ("broker:9092",), "events", connection_id="primary-kafka"
        )
        worker = WorkerSpec(
            "event-worker",
            object(),
            (source,),
            max_parallelism=4,
            batch_size=10,
        )
        dispatcher = RayDispatcher(
            (worker,),
            backend,
            source_client=client,
            checkpoint_store=checkpoints,
            policy=SchedulingPolicy(max_in_flight=10),
        )

        client.kafka["events"] = {0: (0, 100)}
        self.assertEqual({"event-worker:events:0": 0}, await dispatcher.data_listener())
        self.assertEqual(100, checkpoints.values["event-worker:events:0"])

        client.kafka["events"] = {0: (0, 125)}
        backlog = await dispatcher.data_listener()
        self.assertEqual(25, backlog["event-worker:events:0"])

        run_ids = await dispatcher.ray_trigger()
        self.assertEqual(3, len(run_ids))
        requests = [submission[1] for submission in backend.submissions]
        self.assertEqual([(100, 109), (109, 117), (117, 125)], [
            (request.start_offset, request.end_offset) for request in requests
        ])
        self.assertEqual({0, 1, 2}, {request.task_index for request in requests})
        self.assertTrue(all(request.n == 3 for request in requests))
        self.assertTrue(all(request.topic == "events" for request in requests))
        self.assertTrue(
            all(request.source_connection_id == "primary-kafka" for request in requests)
        )
        self.assertEqual(3, len(dispatcher.state.refs))

        backend.finish(backend.submissions[1][2], value="middle")
        await dispatcher.ray_status()
        self.assertEqual(100, checkpoints.values["event-worker:events:0"])

        backend.finish(backend.submissions[0][2], value="first")
        backend.finish(backend.submissions[2][2], value="last")
        await dispatcher.ray_status()
        self.assertEqual(125, checkpoints.values["event-worker:events:0"])
        self.assertEqual(0, dispatcher.state.sources["event-worker:events:0"].backlog)
        batch = next(iter(dispatcher.state.batches.values()))
        self.assertEqual(BatchStatus.SUCCEEDED, batch.status)

    async def test_failed_ref_is_retried_with_same_dispatch_id(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = WorkerSpec(
            "worker", object(), (source,), max_parallelism=1, batch_size=100, max_retries=1
        )
        dispatcher = RayDispatcher((worker,), backend, source_client=client)
        client.kafka["events"] = {0: (0, 5)}
        await dispatcher.data_listener()
        [run_id] = await dispatcher.ray_trigger()
        original_dispatch_id = backend.submissions[0][1].dispatch_id

        backend.finish(backend.submissions[0][2], error="transient")
        statuses = await dispatcher.ray_status()
        self.assertEqual(RunStatus.SUBMITTED, statuses[run_id])
        self.assertEqual(original_dispatch_id, backend.submissions[1][1].dispatch_id)
        self.assertEqual(2, dispatcher.state.runs[run_id].attempt)

        backend.finish(backend.submissions[1][2], value="ok")
        await dispatcher.ray_status()
        self.assertEqual(5, dispatcher.state.sources["worker:events:0"].committed)

    async def test_initial_submission_failure_is_automatically_retried(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        backend.fail_submissions = 1
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = WorkerSpec(
            "worker", object(), (source,), max_parallelism=1, max_retries=1
        )
        dispatcher = RayDispatcher((worker,), backend, source_client=client)
        client.kafka["events"] = {0: (0, 5)}
        await dispatcher.data_listener()
        [run_id] = await dispatcher.ray_trigger()
        self.assertIsNone(dispatcher.state.runs[run_id].ref)

        statuses = await dispatcher.ray_status()
        self.assertEqual(RunStatus.RUNNING, statuses[run_id])
        self.assertIsNotNone(dispatcher.state.runs[run_id].ref)
        self.assertEqual(2, dispatcher.state.runs[run_id].attempt)

        backend.finish(backend.submissions[-1][2], value="ok")
        await dispatcher.ray_status()
        self.assertEqual(5, dispatcher.state.sources["worker:events:0"].committed)

    async def test_postgres_composite_cursor_window_uses_task_slices(self) -> None:
        client = FakeSourceClient()
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
        worker = WorkerSpec(
            "order-worker", object(), (source,), max_parallelism=4, batch_size=10
        )
        checkpoints = MemoryCheckpointStore()
        dispatcher = RayDispatcher(
            (worker,), backend, source_client=client, checkpoint_store=checkpoints
        )
        client.pg_upper["orders"] = end
        client.pg_count["orders"] = 25

        backlog = await dispatcher.data_listener()
        self.assertEqual(25, backlog["order-worker:orders:orders"])
        run_ids = await dispatcher.ray_trigger()
        # Custom clients without postgres_ranges safely use one task instead of
        # process-dependent Python hash partitioning.
        self.assertEqual(1, len(run_ids))
        requests = [item[1] for item in backend.submissions]
        self.assertTrue(all(request.start_cursor == start for request in requests))
        self.assertTrue(all(request.end_cursor == end for request in requests))
        self.assertEqual([0], [request.task_index for request in requests])

        for _, _, ref in backend.submissions:
            backend.finish(ref, value={"processed": True})
        await dispatcher.ray_status()
        self.assertEqual(end, checkpoints.values["order-worker:orders:orders"])

    async def test_postgres_numeric_primary_keys_keep_database_order(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
        start = PostgresCursor(timestamp, 9)
        end = PostgresCursor(timestamp, 10)
        source = PostgresSource(
            "orders", "dsn", "orders", "updated_at", "id", initial_cursor=start
        )
        worker = WorkerSpec("worker", object(), (source,))
        dispatcher = RayDispatcher((worker,), backend, source_client=client)
        client.pg_upper["orders"] = end
        client.pg_count["orders"] = 1

        backlog = await dispatcher.data_listener()
        self.assertEqual(1, backlog["worker:orders:orders"])
        self.assertEqual(1, client.pg_count_calls)

    async def test_retention_gap_fails_without_silently_skipping_data(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        source = KafkaSource("events", ("broker",), "events")
        worker = WorkerSpec("worker", object(), (source,))
        checkpoints = MemoryCheckpointStore({"worker:events:0": 5})
        dispatcher = RayDispatcher(
            (worker,), backend, source_client=client, checkpoint_store=checkpoints
        )
        client.kafka["events"] = {0: (10, 20)}

        with self.assertRaises(KafkaRetentionGap):
            await dispatcher.data_listener()
        self.assertIn("below retained", dispatcher.state.sources["worker:events:0"].retention_gap)

    async def test_explicit_retention_reset_resumes_from_new_low_watermark(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events",
            ("broker",),
            "events",
            retention_policy="reset_to_earliest",
        )
        worker = WorkerSpec("worker", object(), (source,), batch_size=100)
        checkpoints = MemoryCheckpointStore({"worker:events:0": 5})
        dispatcher = RayDispatcher(
            (worker,), backend, source_client=client, checkpoint_store=checkpoints
        )
        client.kafka["events"] = {0: (10, 20)}

        backlog = await dispatcher.data_listener()
        self.assertEqual(10, backlog["worker:events:0"])
        self.assertIsNone(dispatcher.state.sources["worker:events:0"].retention_gap)
        self.assertEqual(10, checkpoints.values["worker:events:0"])
        self.assertEqual(1, len(await dispatcher.ray_trigger()))

    async def test_retention_reset_does_not_overwrite_an_active_batch(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        source = KafkaSource(
            "events",
            ("broker",),
            "events",
            initial_offset="earliest",
            retention_policy="reset_to_earliest",
        )
        worker = WorkerSpec("worker", object(), (source,), max_parallelism=1)
        checkpoints = MemoryCheckpointStore()
        dispatcher = RayDispatcher(
            (worker,), backend, source_client=client, checkpoint_store=checkpoints
        )
        client.kafka["events"] = {0: (0, 20)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()

        client.kafka["events"] = {0: (30, 40)}
        with self.assertRaises(KafkaRetentionGap):
            await dispatcher.data_listener()
        self.assertEqual(0, checkpoints.values["worker:events:0"])

        backend.finish(backend.submissions[0][2], value="old range completed")
        await dispatcher.ray_status()
        self.assertEqual(20, checkpoints.values["worker:events:0"])
        backlog = await dispatcher.data_listener()
        self.assertEqual(10, backlog["worker:events:0"])
        self.assertEqual(30, checkpoints.values["worker:events:0"])

    async def test_actor_pool_can_accept_a_second_wave_when_free_cpu_is_zero(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend(cpus=1)
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = WorkerSpec(
            "worker",
            object(),
            (source,),
            mode=ExecutionMode.ACTOR,
            remote_method="process",
            max_parallelism=1,
            cache_history=True,
        )
        dispatcher = RayDispatcher((worker,), backend, source_client=client)
        client.kafka["events"] = {0: (0, 1)}
        await dispatcher.data_listener()
        await dispatcher.ray_trigger()
        backend.finish(backend.submissions[0][2], value="first")
        await dispatcher.ray_status()

        client.kafka["events"] = {0: (0, 2)}
        await dispatcher.data_listener()
        backend.cpus = 0
        second_wave = await dispatcher.ray_trigger()
        self.assertEqual(1, len(second_wave))

    def test_native_backend_reuses_a_single_actor_per_handler(self) -> None:
        backend = NativeRayBackend(FakeNativeRayModule())
        actor_cls = RecordingActorClass()
        source = KafkaSource("events", ("broker",), "events")
        worker = WorkerSpec(
            "worker",
            actor_cls,
            (source,),
            mode=ExecutionMode.ACTOR,
            remote_method="process",
            max_parallelism=4,
        )
        request = DispatchRequest(
            "dispatch", "worker", "events", SourceKind.KAFKA, 0, 1
        )

        backend.submit(worker, request)
        backend.submit(worker, request)
        backend.submit(worker, request)

        self.assertEqual(1, len(actor_cls.remote_instances))
        self.assertEqual(3, len(actor_cls.remote_instances[0].process.remote_calls))

    def test_scheduling_priority_is_progressive(self) -> None:
        policy = SchedulingPolicy()
        source = KafkaSource("events", ("broker",), "events")
        high = WorkerSpec("high", object(), (source,), priority=1)
        low = WorkerSpec("low", object(), (source,), priority=0)
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
            key=lambda state: policy.priority(
                high if state.worker_name == "high" else low, state, now
            ),
            reverse=True,
        )
        self.assertEqual(["d", "a", "b", "c"], [state.key for state in ordered])

    async def test_permanent_failure_skips_range_and_unblocks_source(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        checkpoints = MemoryCheckpointStore()
        source = KafkaSource("events", ("broker",), "events", initial_offset="earliest")
        worker = WorkerSpec(
            "worker",
            object(),
            (source,),
            max_parallelism=1,
            batch_size=5,
            max_retries=0,
        )
        dispatcher = RayDispatcher(
            (worker,), backend, source_client=client, checkpoint_store=checkpoints
        )
        client.kafka["events"] = {0: (0, 12)}
        await dispatcher.data_listener()
        [run_id] = await dispatcher.ray_trigger()
        key = "worker:events:0"
        self.assertEqual(0, dispatcher.state.sources[key].committed)
        self.assertIsNotNone(dispatcher.state.sources[key].active_batch_id)

        backend.finish(backend.submissions[0][2], error="poison")
        await dispatcher.ray_status()

        state = dispatcher.state.sources[key]
        batch = next(iter(dispatcher.state.batches.values()))
        self.assertEqual(RunStatus.FAILED, dispatcher.state.runs[run_id].status)
        self.assertEqual(BatchStatus.FAILED, batch.status)
        self.assertEqual(5, state.committed)
        self.assertEqual(5, checkpoints.values[key])
        self.assertIsNone(state.active_batch_id)
        self.assertEqual(7, state.backlog)

        next_ids = await dispatcher.ray_trigger()
        self.assertEqual(1, len(next_ids))
        self.assertEqual(5, backend.submissions[-1][1].start_offset)
        self.assertEqual(10, backend.submissions[-1][1].end_offset)

    async def test_periodic_loops_start_and_stop_cleanly(self) -> None:
        client = FakeSourceClient()
        backend = FakeRayBackend()
        source = KafkaSource("events", ("broker",), "events")
        worker = WorkerSpec("worker", object(), (source,))
        dispatcher = RayDispatcher(
            (worker,),
            backend,
            source_client=client,
            listener_interval=0.01,
            trigger_interval=0.01,
            status_interval=0.01,
        )
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
        worker = WorkerSpec("worker", remote, (source,))
        request = DispatchRequest(
            "dispatch", "worker", "events", SourceKind.KAFKA, 0, 1
        )
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
            await first_store.save("handler:events:0", 42)
            await first_store.save("handler:orders:orders", cursor)

            restarted_store = SQLiteCheckpointStore(path)
            self.assertEqual(42, await restarted_store.load("handler:events:0"))
            self.assertEqual(
                cursor, await restarted_store.load("handler:orders:orders")
            )


if __name__ == "__main__":
    unittest.main()
