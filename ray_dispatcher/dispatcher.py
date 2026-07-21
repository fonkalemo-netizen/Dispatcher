"""RayDispatcher: observe sources, schedule work and monitor Ray refs."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Union

from ray_dispatcher.checkpoint import MemoryCheckpointStore
from ray_dispatcher.failures import MemoryFailureStore
from ray_dispatcher.models import (
    BatchRun,
    BatchStatus,
    DispatchRequest,
    DispatcherState,
    ExecutionMode,
    FailureRecord,
    FailureRunDetail,
    HandlerSpec,
    KafkaSource,
    PostgresCursor,
    PostgresSource,
    RunStatus,
    SourceKind,
    SourceSpec,
    SourceState,
    TaskRun,
    WorkerSpec,
)
from ray_dispatcher.policy import SchedulingPolicy
from ray_dispatcher.protocols import CheckpointStore, FailureStore, RayBackend
from ray_dispatcher.sources import SourceObserver



class KafkaRetentionGap(RuntimeError):
    pass


class StaleBatchError(RuntimeError):
    pass


class RayDispatcher:
    """Observe source increments, schedule work and monitor Ray references.

    The public methods perform one non-blocking cycle each. ``start`` runs the
    three cycles periodically until ``stop`` is called.

    ``workers`` may be a sequence of ``WorkerSpec`` or a directory path; a path
    is scanned with :func:`ray_dispatcher.discovery.discover_workers`.
    """

    def __init__(
        self,
        workers: Union[Sequence[WorkerSpec], str, Path],
        *,
        ray_backend: RayBackend | None = None,
        checkpoint_store: CheckpointStore | None = None,
        failure_store: FailureStore | None = None,
        policy: SchedulingPolicy | None = None,
        listener_interval: float = 5.0,
        trigger_interval: float = 1.0,
        status_interval: float = 1.0,
        operation_timeout: float = 30.0,
    ) -> None:
        if ray_backend is None:
            from ray_dispatcher.backend import NativeRayBackend

            ray_backend = NativeRayBackend()
        self.ray_backend = ray_backend

        if isinstance(workers, (str, Path)):
            from ray_dispatcher.discovery import discover_workers

            workers = discover_workers(
                workers,
                ray_module=getattr(ray_backend, "ray", None),
            )

        if not workers:
            raise ValueError("at least one worker is required")
        names = [worker.name for worker in workers]
        if len(names) != len(set(names)):
            raise ValueError("worker names must be unique")
        for worker in workers:
            source_ids = [source.source_id for source in worker.sources]
            if len(source_ids) != len(set(source_ids)):
                raise ValueError(f"source IDs for worker {worker.name!r} must be unique")
        if min(listener_interval, trigger_interval, status_interval, operation_timeout) <= 0:
            raise ValueError("polling intervals and operation_timeout must be positive")

        self.workers = {worker.name: worker for worker in workers}
        self._shared_groups: dict[str, tuple[HandlerSpec, ...]] = {}
        shared_groups: dict[str, list[HandlerSpec]] = {}
        for worker in workers:
            if worker.shared_source_group is not None:
                shared_groups.setdefault(worker.shared_source_group, []).append(worker)
        for group_name, members in shared_groups.items():
            if len(members) < 2:
                raise ValueError(
                    f"shared source group {group_name!r} requires at least two handlers"
                )
            physical_keys = {
                self._physical_source_key(member.sources[0]) for member in members
            }
            fetcher_ids = {member.fetcher_id for member in members}
            fetch_policies = {
                (member.fetch_cpus, member.fetch_max_retries)
                for member in members
            }
            if (
                len(physical_keys) != 1
                or len(fetcher_ids) != 1
                or len(fetch_policies) != 1
            ):
                raise ValueError(
                    f"shared source group {group_name!r} must use one physical "
                    "source, fetcher_id and fetch policy"
                )
            source_ids = {member.sources[0].source_id for member in members}
            if len(source_ids) != 1:
                raise ValueError(
                    f"shared source group {group_name!r} must use one source_id"
                )
            representative_source = members[0].sources[0]
            if isinstance(representative_source, KafkaSource):
                startup_policies = {
                    (
                        member.sources[0].initial_offset,
                        member.sources[0].retention_policy,
                    )
                    for member in members
                    if isinstance(member.sources[0], KafkaSource)
                }
                if len(startup_policies) != 1:
                    raise ValueError(
                        f"shared source group {group_name!r} must use identical "
                        "initial_offset and retention_policy"
                    )
            else:
                initial_cursors = {
                    repr(member.sources[0].initial_cursor)
                    for member in members
                    if isinstance(member.sources[0], PostgresSource)
                }
                if len(initial_cursors) != 1:
                    raise ValueError(
                        f"shared source group {group_name!r} must use one initial_cursor"
                    )
            self._shared_groups[group_name] = tuple(members)
        self._output_limits: dict[tuple[str, str], int] = {}
        for worker in workers:
            if worker.output is None:
                continue
            output_key = (worker.output.connection_id, worker.output.target)
            configured = self._output_limits.get(output_key)
            self._output_limits[output_key] = (
                worker.output.max_parallelism
                if configured is None
                else min(configured, worker.output.max_parallelism)
            )
        self.source_observer = SourceObserver()
        self.checkpoint_store = checkpoint_store or MemoryCheckpointStore()
        self.failure_store = failure_store or MemoryFailureStore()
        self.policy = policy or SchedulingPolicy()
        self.listener_interval = listener_interval
        self.trigger_interval = trigger_interval
        self.status_interval = status_interval
        self.operation_timeout = operation_timeout
        self.state = DispatcherState()
        self._lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()

    @staticmethod
    def _state_key(worker: str, source: str, shard: str) -> str:
        return f"{worker}:{source}:{shard}"

    @staticmethod
    def _shared_state_key(group: str, source: str, shard: str) -> str:
        return f"shared:{group}:{source}:{shard}"

    @staticmethod
    def _stable_id(*parts: Any) -> str:
        raw = "|".join(repr(part) for part in parts).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:24]

    @staticmethod
    def _ewma(old: float, new: float, alpha: float) -> float:
        return new if old == 0 else alpha * new + (1 - alpha) * old

    async def data_listener(self) -> dict[str, int]:
        """Observe each physical source once, then fan out to handler checkpoints."""

        async with self._lock:
            errors: list[Exception] = []
            source_bindings: dict[tuple[Any, ...], list[tuple[HandlerSpec, SourceSpec]]] = {}
            for worker in self.workers.values():
                for source in worker.sources:
                    source_bindings.setdefault(self._physical_source_key(source), []).append(
                        (worker, source)
                    )

            for bindings in source_bindings.values():
                representative = bindings[0][1]
                try:
                    if isinstance(representative, KafkaSource):
                        watermarks = await self._io(
                            self.source_observer.kafka_watermarks(representative)
                        )
                        observed_shared_groups: set[str] = set()
                        for worker, source in bindings:
                            group = worker.shared_source_group
                            if group is not None:
                                if group in observed_shared_groups:
                                    continue
                                observed_shared_groups.add(group)
                            try:
                                await self._observe_kafka(worker, source, watermarks)
                            except Exception as exc:
                                errors.append(exc)
                    else:
                        upper = await self._io(
                            self.source_observer.postgres_high_watermark(representative)
                        )
                        observed_shared_groups = set()
                        for worker, source in bindings:
                            group = worker.shared_source_group
                            if group is not None:
                                if group in observed_shared_groups:
                                    continue
                                observed_shared_groups.add(group)
                            try:
                                await self._observe_postgres(worker, source, upper)
                            except Exception as exc:
                                errors.append(exc)
                except Exception as exc:
                    # One broken physical source must not prevent later sources
                    # from refreshing their independent watermarks.
                    errors.append(exc)
            if errors:
                raise errors[0]
            return {key: state.backlog for key, state in self.state.sources.items()}

    @staticmethod
    def _physical_source_key(source: SourceSpec) -> tuple[Any, ...]:
        if isinstance(source, KafkaSource):
            connection_key: Any = source.connection_id or tuple(
                sorted(source.brokers)
            )
            return (SourceKind.KAFKA, connection_key, source.topic)
        return (
            SourceKind.POSTGRES,
            source.connection_id or source.dsn,
            source.table,
            source.timestamp_column,
            source.primary_key_column,
        )

    async def _io(self, awaitable: Any) -> Any:
        return await asyncio.wait_for(awaitable, timeout=self.operation_timeout)

    async def _observe_kafka(
        self,
        worker: HandlerSpec,
        source: KafkaSource,
        watermarks: Mapping[int, tuple[int, int]] | None = None,
    ) -> None:
        if watermarks is None:
            watermarks = await self._io(self.source_observer.kafka_watermarks(source))
        now = time.monotonic()
        for partition, (low, high) in watermarks.items():
            if low < 0 or high < low:
                raise ValueError(f"invalid Kafka watermarks for {source.source_id}/{partition}")
            key = (
                self._shared_state_key(
                    worker.shared_source_group, source.source_id, str(partition)
                )
                if worker.shared_source_group is not None
                else self._state_key(worker.name, source.source_id, str(partition))
            )
            state = self.state.sources.get(key)
            if state is None:
                checkpoint = await self._io(self.checkpoint_store.load(key))
                if checkpoint is not None and not isinstance(checkpoint, int):
                    raise TypeError(f"Kafka checkpoint {key} must be int")
                committed = checkpoint if checkpoint is not None else (
                    low if source.initial_offset == "earliest" else high
                )
                if checkpoint is None:
                    # Persist the baseline immediately. Otherwise a restart
                    # after this poll could adopt a newer high watermark and
                    # silently skip records that arrived in between.
                    await self._io(self.checkpoint_store.save(key, committed))
                state = SourceState(
                    key,
                    worker.name,
                    source.source_id,
                    source.kind,
                    str(partition),
                    committed,
                    high,
                    shared_source_group=worker.shared_source_group,
                )
                self.state.sources[key] = state

            committed = int(state.committed)
            if committed < low or committed > high:
                relation = "below retained low watermark" if committed < low else "above high watermark"
                boundary = low if committed < low else high
                message = f"checkpoint {committed} is {relation} {boundary}"
                state.retention_gap = message
                if state.active_batch_id is not None:
                    raise KafkaRetentionGap(
                        f"{key}: {message}; cannot reset while batch "
                        f"{state.active_batch_id} is active"
                    )
                if source.retention_policy == "error":
                    raise KafkaRetentionGap(f"{key}: {message}")
                committed = low
                state.committed = low
                await self._io(self.checkpoint_store.save(key, low))
                state.retention_gap = None
            else:
                state.retention_gap = None

            elapsed = max(now - state.last_observed_at, 1e-6)
            previous_high = int(state.observed)
            arrived = max(0, high - previous_high)
            state.arrival_rate = self._ewma(
                state.arrival_rate, arrived / elapsed, self.policy.ewma_alpha
            )
            state.observed = high
            state.backlog = max(0, high - committed)
            state.last_observed_at = now

    async def _observe_postgres(
        self,
        worker: HandlerSpec,
        source: PostgresSource,
        upper: PostgresCursor | None = None,
    ) -> None:
        if upper is None:
            upper = await self._io(self.source_observer.postgres_high_watermark(source))
        key = (
            self._shared_state_key(
                worker.shared_source_group, source.source_id, source.table
            )
            if worker.shared_source_group is not None
            else self._state_key(worker.name, source.source_id, source.table)
        )
        state = self.state.sources.get(key)
        if state is None:
            checkpoint = await self._io(self.checkpoint_store.load(key))
            if checkpoint is not None and not isinstance(checkpoint, PostgresCursor):
                raise TypeError(f"Postgres checkpoint {key} must be PostgresCursor")
            committed = checkpoint or source.initial_cursor or upper
            if checkpoint is None:
                await self._io(self.checkpoint_store.save(key, committed))
            state = SourceState(
                key,
                worker.name,
                source.source_id,
                source.kind,
                source.table,
                committed,
                upper,
                shared_source_group=worker.shared_source_group,
            )
            self.state.sources[key] = state

        committed = state.committed
        if not isinstance(committed, PostgresCursor):
            raise TypeError(f"invalid Postgres state for {key}")
        count = 0 if upper <= committed else await self._io(
            self.source_observer.postgres_count(source, committed, upper)
        )
        now = time.monotonic()
        elapsed = max(now - state.last_observed_at, 1e-6)
        previous = state.backlog
        arrived = max(0, count - previous)
        state.arrival_rate = self._ewma(
            state.arrival_rate, arrived / elapsed, self.policy.ewma_alpha
        )
        state.observed = upper
        state.backlog = max(0, count)
        state.last_observed_at = now

    async def ray_trigger(self) -> list[str]:
        """Schedule one wave of eligible source ranges and return run IDs."""

        submitted: list[str] = []
        async with self._lock:
            active_global = sum(
                run.status in (RunStatus.SUBMITTED, RunStatus.RUNNING)
                for run in self.state.runs.values()
            )
            active_global += sum(
                batch.reserved_handler_count
                for batch in self.state.batches.values()
                if batch.status is BatchStatus.RUNNING
            )
            free_slots = max(0, self.policy.max_in_flight - active_global)
            available_cpus = self.ray_backend.available_cpus()
            now = time.monotonic()
            candidates = [
                state
                for state in self.state.sources.values()
                if state.backlog > 0 and state.active_batch_id is None and not state.retention_gap
            ]
            candidates.sort(
                key=lambda state: self.policy.priority(
                    self.workers[state.worker_name], state, now
                ),
                reverse=True,
            )

            for source_state in candidates:
                if free_slots <= 0:
                    break
                if source_state.shared_source_group is not None:
                    members = self._shared_groups[source_state.shared_source_group]
                    n = self._shared_task_count(
                        members,
                        source_state,
                        free_slots,
                        available_cpus,
                    )
                    if n == 0:
                        continue
                    run_ids = await self._create_shared_batch(
                        members, source_state, n
                    )
                    submitted.extend(run_ids)
                    reserved = len(run_ids) * (len(members) + 1)
                    free_slots = max(0, free_slots - reserved)
                    if available_cpus is not None:
                        available_cpus = max(
                            0.0,
                            available_cpus - len(run_ids) * members[0].fetch_cpus,
                        )
                    continue
                worker = self.workers[source_state.worker_name]
                worker_active = sum(
                    run.worker_name == worker.name
                    and run.status in (RunStatus.SUBMITTED, RunStatus.RUNNING)
                    for run in self.state.runs.values()
                )
                worker_slots = max(0, worker.max_parallelism - worker_active)
                output_slots = worker_slots
                if worker.output is not None:
                    output_key = (
                        worker.output.connection_id,
                        worker.output.target,
                    )
                    output_active = sum(
                        run.status in (RunStatus.SUBMITTED, RunStatus.RUNNING)
                        and self.workers[run.worker_name].output is not None
                        and (
                            self.workers[run.worker_name].output.connection_id,
                            self.workers[run.worker_name].output.target,
                        )
                        == output_key
                        for run in self.state.runs.values()
                    )
                    output_slots = max(
                        0, self._output_limits[output_key] - output_active
                    )
                # Actors already own their CPU allocation. Requiring free cluster
                # CPU again for each method call would prevent a full actor pool
                # from ever accepting its second wave.
                cpu_budget = (
                    None if worker.mode is ExecutionMode.ACTOR else available_cpus
                )
                n = self.policy.task_count(
                    worker,
                    source_state.backlog,
                    min(free_slots, worker_slots, output_slots),
                    cpu_budget,
                )
                if n == 0:
                    continue
                run_ids = await self._create_batch(worker, source_state, n)
                submitted.extend(run_ids)
                free_slots -= len(run_ids)
                if available_cpus is not None and worker.mode is ExecutionMode.TASK:
                    available_cpus = max(0.0, available_cpus - len(run_ids) * worker.cpus_per_task)
            return submitted

    def _shared_task_count(
        self,
        members: tuple[HandlerSpec, ...],
        state: SourceState,
        free_slots: int,
        available_cpus: float | None,
    ) -> int:
        batch_size = min(member.batch_size for member in members)
        per_slice_slots = len(members) + 1
        count = min(
            math.ceil(state.backlog / batch_size),
            free_slots // per_slice_slots,
        )
        output_multiplicity: dict[tuple[str, str], int] = {}
        for member in members:
            active = sum(
                run.worker_name == member.name
                and run.kind == "handler"
                and run.status in (RunStatus.SUBMITTED, RunStatus.RUNNING)
                for run in self.state.runs.values()
            )
            count = min(count, max(0, member.max_parallelism - active))
            if member.output is not None:
                output_key = (
                    member.output.connection_id,
                    member.output.target,
                )
                output_multiplicity[output_key] = (
                    output_multiplicity.get(output_key, 0) + 1
                )
        for output_key, per_slice in output_multiplicity.items():
            output_active = sum(
                run.kind == "handler"
                and run.status in (RunStatus.SUBMITTED, RunStatus.RUNNING)
                and self.workers[run.worker_name].output is not None
                and (
                    self.workers[run.worker_name].output.connection_id,
                    self.workers[run.worker_name].output.target,
                )
                == output_key
                for run in self.state.runs.values()
            )
            count = min(
                count,
                max(0, self._output_limits[output_key] - output_active)
                // per_slice,
            )
        if available_cpus is not None:
            downstream_cpus = sum(
                member.cpus_per_task
                for member in members
                if member.mode is ExecutionMode.TASK
            )
            phase_cpus = max(members[0].fetch_cpus, downstream_cpus)
            count = min(count, math.floor(available_cpus / phase_cpus))
        return max(0, count)

    async def _create_shared_batch(
        self,
        members: tuple[HandlerSpec, ...],
        source_state: SourceState,
        n: int,
    ) -> list[str]:
        group = source_state.shared_source_group
        if group is None:
            raise ValueError("shared batch requires shared_source_group")
        representative = members[0]
        source = representative.sources[0]
        start, observed = source_state.committed, source_state.observed
        batch_size = min(member.batch_size for member in members)
        item_count = source_state.backlog
        ranges: list[
            tuple[int | PostgresCursor, int | PostgresCursor, int]
        ] = []

        if isinstance(source, KafkaSource):
            start_i, observed_i = int(start), int(observed)
            end: int | PostgresCursor = min(
                observed_i, start_i + n * batch_size
            )
            item_count = int(end) - start_i
            n = min(n, item_count)
            base, remainder = divmod(item_count, n)
            cursor = start_i
            for index in range(n):
                size = base + (1 if index < remainder else 0)
                ranges.append((cursor, cursor + size, size))
                cursor += size
        else:
            if not isinstance(start, PostgresCursor) or not isinstance(
                observed, PostgresCursor
            ):
                raise TypeError("Postgres shared batch requires cursor bounds")
            splitter = getattr(self.source_observer, "postgres_ranges", None)
            if splitter is None:
                ranges = [(start, observed, source_state.backlog)]
            else:
                ranges = list(
                    await self._io(
                        splitter(source, start, observed, n, batch_size)
                    )
                )
            if not ranges:
                return []
            n = len(ranges)
            end = ranges[-1][1]
            item_count = sum(count for _, _, count in ranges)

        batch_id = self._stable_id("shared", group, source_state.key, start, end)
        fetch_run_ids: list[str] = []
        batch = BatchRun(
            batch_id,
            source_state.key,
            representative.name,
            start,
            end,
            item_count,
            [],
            shared_source_group=group,
            worker_names=tuple(member.name for member in members),
            fetch_run_ids=fetch_run_ids,
            reserved_handler_count=n * len(members),
        )
        self.state.batches[batch_id] = batch
        source_state.active_batch_id = batch_id
        source_state.last_scheduled_at = time.monotonic()

        for index, (chunk_start, chunk_end, _) in enumerate(ranges):
            fetch_id = self._stable_id(
                batch_id, "fetch", index, chunk_start, chunk_end
            )
            request = DispatchRequest(
                fetch_id,
                f"fetch:{group}",
                source.source_id,
                source.kind,
                index,
                n,
                partition=(
                    int(source_state.shard) if isinstance(source, KafkaSource) else None
                ),
                start_offset=(int(chunk_start) if isinstance(source, KafkaSource) else None),
                end_offset=(int(chunk_end) if isinstance(source, KafkaSource) else None),
                start_cursor=(chunk_start if isinstance(source, PostgresSource) else None),
                end_cursor=(chunk_end if isinstance(source, PostgresSource) else None),
                source_connection_id=source.connection_id,
                topic=source.topic if isinstance(source, KafkaSource) else None,
                table=source.table if isinstance(source, PostgresSource) else None,
            )
            try:
                ref = self.ray_backend.submit_fetch(representative, request)
                run = TaskRun(
                    fetch_id,
                    batch_id,
                    representative.name,
                    request,
                    ref,
                    kind="fetch",
                )
            except Exception as exc:
                run = TaskRun(
                    fetch_id,
                    batch_id,
                    representative.name,
                    request,
                    None,
                    error=f"fetch submission failed: {type(exc).__name__}: {exc}",
                    kind="fetch",
                )
            self.state.runs[fetch_id] = run
            fetch_run_ids.append(fetch_id)
        return fetch_run_ids

    async def _create_batch(
        self, worker: WorkerSpec, source_state: SourceState, n: int
    ) -> list[str]:
        start, end = source_state.committed, source_state.observed
        item_count = source_state.backlog
        if source_state.kind is SourceKind.KAFKA:
            # A constrained wave must not silently turn one task into an
            # unbounded batch. Leave the remainder for later trigger cycles.
            end = min(int(end), int(start) + n * worker.batch_size)
            item_count = int(end) - int(start)
        batch_id = self._stable_id(worker.name, source_state.key, start, end)
        requests: list[DispatchRequest] = []
        if source_state.kind is SourceKind.KAFKA:
            source = next(
                candidate
                for candidate in worker.sources
                if isinstance(candidate, KafkaSource)
                and candidate.source_id == source_state.source_id
            )
            start_i, end_i = int(start), int(end)
            n = min(n, end_i - start_i)
            base, remainder = divmod(end_i - start_i, n)
            cursor = start_i
            ranges: list[tuple[int, int]] = []
            for index in range(n):
                size = base + (1 if index < remainder else 0)
                ranges.append((cursor, cursor + size))
                cursor += size
            for index, (chunk_start, chunk_end) in enumerate(ranges):
                dispatch_id = self._stable_id(batch_id, index, chunk_start, chunk_end)
                requests.append(
                    DispatchRequest(
                        dispatch_id,
                        worker.name,
                        source_state.source_id,
                        source_state.kind,
                        index,
                        n,
                        partition=int(source_state.shard),
                        start_offset=chunk_start,
                        end_offset=chunk_end,
                        output_connection_id=(
                            worker.output.connection_id if worker.output else None
                        ),
                        output_target=worker.output.target if worker.output else None,
                        output_format=worker.output.output_format if worker.output else None,
                        source_connection_id=source.connection_id,
                        topic=source.topic,
                    )
                )
        else:
            if not isinstance(start, PostgresCursor) or not isinstance(end, PostgresCursor):
                raise TypeError("Postgres batch requires PostgresCursor bounds")
            source = next(
                candidate
                for candidate in worker.sources
                if isinstance(candidate, PostgresSource)
                and candidate.source_id == source_state.source_id
                and candidate.table == source_state.shard
            )
            splitter = getattr(self.source_observer, "postgres_ranges", None)
            if splitter is None:
                postgres_ranges = [(start, end, source_state.backlog)]
            else:
                postgres_ranges = list(
                    await self._io(
                        splitter(source, start, end, n, worker.batch_size)
                    )
                )
            if not postgres_ranges:
                return []
            n = len(postgres_ranges)
            end = postgres_ranges[-1][1]
            item_count = sum(count for _, _, count in postgres_ranges)
            batch_id = self._stable_id(worker.name, source_state.key, start, end)
            for index, (chunk_start, chunk_end, _) in enumerate(postgres_ranges):
                dispatch_id = self._stable_id(
                    batch_id, index, chunk_start, chunk_end
                )
                requests.append(
                    DispatchRequest(
                        dispatch_id,
                        worker.name,
                        source_state.source_id,
                        source_state.kind,
                        index,
                        n,
                        start_cursor=chunk_start,
                        end_cursor=chunk_end,
                        output_connection_id=(
                            worker.output.connection_id if worker.output else None
                        ),
                        output_target=worker.output.target if worker.output else None,
                        output_format=worker.output.output_format if worker.output else None,
                        source_connection_id=source.connection_id,
                        table=source.table,
                    )
                )

        run_ids: list[str] = []
        batch = BatchRun(
            batch_id,
            source_state.key,
            worker.name,
            start,
            end,
            item_count,
            run_ids,
        )
        self.state.batches[batch_id] = batch
        source_state.active_batch_id = batch_id
        source_state.last_scheduled_at = time.monotonic()
        for request in requests:
            run_id = request.dispatch_id
            try:
                ref = self.ray_backend.submit(worker, request)
                run = TaskRun(run_id, batch_id, worker.name, request, ref)
            except Exception as exc:
                run = TaskRun(
                    run_id,
                    batch_id,
                    worker.name,
                    request,
                    None,
                    status=RunStatus.SUBMITTED,
                    error=f"submission failed: {type(exc).__name__}: {exc}",
                )
            self.state.runs[run_id] = run
            run_ids.append(run_id)
        return run_ids

    async def ray_status(self) -> dict[str, RunStatus]:
        """Poll current refs, retry failures, and commit completed batches."""

        async with self._lock:
            # A local Ray submission can fail before an ObjectRef exists. Treat
            # that exactly like a failed Ray execution so it gets the configured
            # automatic retries instead of wedging the source immediately.
            for run in self.state.runs.values():
                if run.ref is None and run.status is RunStatus.SUBMITTED:
                    await self._handle_failure(run, run.error or "submission failed")
            refs = {
                run_id: run.ref
                for run_id, run in self.state.runs.items()
                if run.ref is not None and run.status in (RunStatus.SUBMITTED, RunStatus.RUNNING)
            }
            for run_id in refs:
                if self.state.runs[run_id].status is RunStatus.SUBMITTED:
                    self.state.runs[run_id].status = RunStatus.RUNNING
            outcomes = self.ray_backend.poll(refs)
            if inspect.isawaitable(outcomes):
                outcomes = await outcomes
            for run_id, outcome in outcomes.items():
                run = self.state.runs.get(run_id)
                if run is None or run.status not in (RunStatus.SUBMITTED, RunStatus.RUNNING):
                    continue
                if outcome.success:
                    run.status = RunStatus.SUCCEEDED
                    # Fetch payloads stay in Ray's object store and are shared
                    # through run.ref; retaining outcome.value here would pin a
                    # second driver-side reference for the life of Dispatcher.
                    run.result = outcome.value if run.kind == "handler" else None
                    run.finished_at = time.monotonic()
                else:
                    await self._handle_failure(run, outcome.error or "unknown Ray failure")

            for batch in list(self.state.batches.values()):
                if batch.status is not BatchStatus.RUNNING:
                    continue
                if batch.shared_source_group is not None:
                    fetch_runs = [
                        self.state.runs[run_id]
                        for run_id in batch.fetch_run_ids
                    ]
                    if fetch_runs and self._all_terminal(fetch_runs):
                        if any(run.status is RunStatus.FAILED for run in fetch_runs):
                            batch.reserved_handler_count = 0
                            await self._record_failed_batch(batch)
                            await self._skip_failed_batch(batch)
                            continue
                    if (
                        fetch_runs
                        and all(
                            run.status is RunStatus.SUCCEEDED for run in fetch_runs
                        )
                        and not batch.run_ids
                    ):
                        await self._submit_shared_handlers(batch)
                runs = [self.state.runs[run_id] for run_id in batch.run_ids]
                if not runs or not self._all_terminal(runs):
                    continue
                if any(run.status is RunStatus.FAILED for run in runs):
                    await self._record_failed_batch(batch)
                    await self._skip_failed_batch(batch)
                else:
                    await self._commit_batch(batch)
            return {run_id: run.status for run_id, run in self.state.runs.items()}

    async def _submit_shared_handlers(self, batch: BatchRun) -> list[str]:
        members = tuple(self.workers[name] for name in batch.worker_names)
        submitted: list[str] = []
        for fetch_run_id in batch.fetch_run_ids:
            fetch_run = self.state.runs[fetch_run_id]
            request = fetch_run.request
            for member in members:
                run_id = self._stable_id(
                    batch.batch_id,
                    member.name,
                    request.task_index,
                    request.start_offset,
                    request.end_offset,
                    request.start_cursor,
                    request.end_cursor,
                )
                handler_request = DispatchRequest(
                    run_id,
                    member.name,
                    request.source_id,
                    request.source_kind,
                    request.task_index,
                    request.task_count,
                    partition=request.partition,
                    start_offset=request.start_offset,
                    end_offset=request.end_offset,
                    start_cursor=request.start_cursor,
                    end_cursor=request.end_cursor,
                    output_connection_id=(
                        member.output.connection_id if member.output else None
                    ),
                    output_target=member.output.target if member.output else None,
                    output_format=(
                        member.output.output_format if member.output else None
                    ),
                    source_connection_id=request.source_connection_id,
                    topic=request.topic,
                    table=request.table,
                )
                try:
                    ref = self.ray_backend.submit(
                        member, handler_request, fetch_run.ref
                    )
                    run = TaskRun(
                        run_id,
                        batch.batch_id,
                        member.name,
                        handler_request,
                        ref,
                        data_ref=fetch_run.ref,
                    )
                except Exception as exc:
                    run = TaskRun(
                        run_id,
                        batch.batch_id,
                        member.name,
                        handler_request,
                        None,
                        error=(
                            "handler submission failed: "
                            f"{type(exc).__name__}: {exc}"
                        ),
                        data_ref=fetch_run.ref,
                    )
                self.state.runs[run_id] = run
                batch.run_ids.append(run_id)
                submitted.append(run_id)
        batch.reserved_handler_count = 0
        return submitted

    async def _handle_failure(self, run: TaskRun, error: str) -> None:
        worker = self.workers[run.worker_name]
        max_retries = (
            worker.fetch_max_retries if run.kind == "fetch" else worker.max_retries
        )
        if run.attempt <= max_retries:
            run.attempt += 1
            run.error = error
            try:
                run.ref = (
                    self.ray_backend.submit_fetch(worker, run.request)
                    if run.kind == "fetch"
                    else self.ray_backend.submit(worker, run.request, run.data_ref)
                    if run.data_ref is not None
                    else self.ray_backend.submit(worker, run.request)
                )
                run.status = RunStatus.SUBMITTED
                run.submitted_at = time.monotonic()
                return
            except Exception as exc:
                error = f"retry submission failed: {type(exc).__name__}: {exc}"
        run.status = RunStatus.FAILED
        run.error = error
        run.finished_at = time.monotonic()

    @staticmethod
    def _all_terminal(runs: Sequence[TaskRun]) -> bool:
        return all(
            run.status in (RunStatus.SUCCEEDED, RunStatus.FAILED) for run in runs
        )

    async def _commit_batch(self, batch: BatchRun) -> None:
        source_state = self.state.sources[batch.source_state_key]
        if (
            source_state.active_batch_id != batch.batch_id
            or source_state.committed != batch.start
        ):
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            raise StaleBatchError(
                f"refusing stale commit for {batch.batch_id}: source cursor or generation changed"
            )
        await self._io(self.checkpoint_store.save(source_state.key, batch.end))
        source_state.committed = batch.end
        source_state.backlog = max(0, source_state.backlog - batch.item_count)
        source_state.active_batch_id = None
        elapsed = max(time.monotonic() - batch.started_at, 1e-6)
        measured = batch.item_count / elapsed
        source_state.processing_rate = self._ewma(
            source_state.processing_rate, measured, self.policy.ewma_alpha
        )
        batch.status = BatchStatus.SUCCEEDED
        batch.finished_at = time.monotonic()
        # Release completed ObjectRefs after checkpoint durability. Until this
        # point they must remain pinned so failed handlers can reuse the fetch.
        for run_id in [*batch.fetch_run_ids, *batch.run_ids]:
            run = self.state.runs[run_id]
            run.ref = None
            run.data_ref = None

    async def _record_failed_batch(self, batch: BatchRun) -> None:
        """Persist failure metadata and any recoverable payload before skip."""

        run_ids = [*batch.fetch_run_ids, *batch.run_ids]
        runs = [self.state.runs[run_id] for run_id in run_ids if run_id in self.state.runs]
        payload: Any = None
        payload_error: str | None = None
        fetch_payloads: list[Any] = []
        for run in runs:
            if run.kind != "fetch" or run.status is not RunStatus.SUCCEEDED:
                continue
            if run.ref is None:
                continue
            try:
                fetch_payloads.append(await self.ray_backend.get(run.ref))
            except Exception as exc:
                payload_error = (
                    f"failed to materialize fetch {run.run_id}: "
                    f"{type(exc).__name__}: {exc}"
                )
                break
        if payload_error is None and fetch_payloads:
            payload = fetch_payloads[0] if len(fetch_payloads) == 1 else fetch_payloads

        record = FailureRecord(
            failure_id=self._stable_id("failure", batch.batch_id, batch.start, batch.end),
            batch_id=batch.batch_id,
            source_state_key=batch.source_state_key,
            shared_source_group=batch.shared_source_group,
            start=batch.start,
            end=batch.end,
            item_count=batch.item_count,
            worker_names=batch.worker_names
            or ((batch.worker_name,) if batch.worker_name else ()),
            runs=tuple(
                FailureRunDetail(
                    run_id=run.run_id,
                    kind=run.kind,
                    worker_name=run.worker_name,
                    status=run.status.value,
                    attempt=run.attempt,
                    error=run.error,
                    request=self._request_summary(run.request),
                )
                for run in runs
            ),
            payload=payload,
            payload_error=payload_error,
        )
        try:
            await self._io(self.failure_store.save_failure(record))
        except Exception as exc:
            self.state.loop_errors.append(
                f"failure_store: {type(exc).__name__}: {exc}"
            )

    @staticmethod
    def _request_summary(request: DispatchRequest) -> dict[str, Any]:
        return {
            "dispatch_id": request.dispatch_id,
            "worker_name": request.worker_name,
            "source_id": request.source_id,
            "source_kind": request.source_kind.value,
            "task_index": request.task_index,
            "task_count": request.task_count,
            "partition": request.partition,
            "start_offset": request.start_offset,
            "end_offset": request.end_offset,
            "start_cursor": (
                None
                if request.start_cursor is None
                else {
                    "timestamp": request.start_cursor.timestamp.isoformat(),
                    "primary_key": request.start_cursor.primary_key,
                }
            ),
            "end_cursor": (
                None
                if request.end_cursor is None
                else {
                    "timestamp": request.end_cursor.timestamp.isoformat(),
                    "primary_key": request.end_cursor.primary_key,
                }
            ),
            "output_connection_id": request.output_connection_id,
            "output_target": request.output_target,
            "output_format": request.output_format,
            "source_connection_id": request.source_connection_id,
            "topic": request.topic,
            "table": request.table,
        }

    async def _skip_failed_batch(self, batch: BatchRun) -> None:
        """Mark the batch failed, advance past its range, and unblock the source."""

        source_state = self.state.sources[batch.source_state_key]
        if source_state.active_batch_id != batch.batch_id:
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            return
        if source_state.committed != batch.start:
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            source_state.active_batch_id = None
            return
        await self._io(self.checkpoint_store.save(source_state.key, batch.end))
        source_state.committed = batch.end
        source_state.backlog = max(0, source_state.backlog - batch.item_count)
        source_state.active_batch_id = None
        batch.status = BatchStatus.FAILED
        batch.finished_at = time.monotonic()
        for run_id in [*batch.fetch_run_ids, *batch.run_ids]:
            run = self.state.runs[run_id]
            run.ref = None
            run.data_ref = None

    async def retry_failed_batch(self, batch_id: str) -> list[str]:
        """No-op: permanent failures are recorded then skipped to unblock the source.

        Kept for API compatibility. Reprocess via FailureStore payload/range
        data (rewind checkpoint manually if needed); this method does not resubmit.
        """

        return []

    async def start(self) -> None:
        """Start the listener, trigger and status loops."""

        if self._tasks:
            return
        self._stopping.clear()
        loops = (
            ("data_listener", self.data_listener, self.listener_interval),
            ("ray_trigger", self.ray_trigger, self.trigger_interval),
            ("ray_status", self.ray_status, self.status_interval),
        )
        self._tasks = [
            asyncio.create_task(self._periodic(name, callback, interval), name=name)
            for name, callback, interval in loops
        ]

    async def __aenter__(self) -> "RayDispatcher":
        await self.start()
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.stop()

    async def _periodic(
        self, name: str, callback: Callable[[], Any], interval: float
    ) -> None:
        while not self._stopping.is_set():
            started = time.monotonic()
            try:
                result = callback()
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.loop_errors.append(f"{name}: {type(exc).__name__}: {exc}")
            remaining = max(0.0, interval - (time.monotonic() - started))
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                pass

    async def stop(self) -> None:
        """Stop all periodic loops; in-flight Ray work is not cancelled."""

        self._stopping.set()
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        close = getattr(self.source_observer, "close", None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                await result

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-friendly operational snapshot (refs are represented)."""

        return {
            "sources": {
                key: {
                    "handler": state.worker_name,
                    "shared_source_group": state.shared_source_group,
                    "source": state.source_id,
                    "kind": state.kind.value,
                    "shard": state.shard,
                    "committed": repr(state.committed),
                    "observed": repr(state.observed),
                    "backlog": state.backlog,
                    "arrival_rate": state.arrival_rate,
                    "processing_rate": state.processing_rate,
                    "active_batch_id": state.active_batch_id,
                    "retention_gap": state.retention_gap,
                }
                for key, state in self.state.sources.items()
            },
            "batches": {
                key: {
                    "status": batch.status.value,
                    "handler": batch.worker_name,
                    "handlers": list(batch.worker_names),
                    "shared_source_group": batch.shared_source_group,
                    "item_count": batch.item_count,
                    "fetch_run_ids": list(batch.fetch_run_ids),
                    "run_ids": list(batch.run_ids),
                }
                for key, batch in self.state.batches.items()
            },
            "runs": {
                key: {
                    "status": run.status.value,
                    "kind": run.kind,
                    "attempt": run.attempt,
                    "ref": repr(run.ref),
                    "error": run.error,
                    "output_connection": run.request.output_connection_id,
                    "output_target": run.request.output_target,
                    "output_format": run.request.output_format,
                    "source_connection": run.request.source_connection_id,
                    "topic": run.request.topic,
                    "table": run.request.table,
                }
                for key, run in self.state.runs.items()
            },
            "loop_errors": list(self.state.loop_errors),
            "failures": {
                "store": type(self.failure_store).__name__,
            },
        }
