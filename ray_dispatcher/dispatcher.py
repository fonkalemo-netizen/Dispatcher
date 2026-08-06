"""RayDispatcher: observe sources, schedule work and monitor Ray refs."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Union

from ray_dispatcher.adapter import RayAdapter
from ray_dispatcher.checkpoint import (
    CheckpointDocument,
    CheckpointStore,
    MemoryCheckpointStore,
)
from ray_dispatcher.event_log import (
    EVENT_BATCH_COMMITTED,
    EVENT_BATCH_SKIPPED,
    EVENT_RUN_FAILED,
    EVENT_SNAPSHOT,
    EVENT_TRIGGER_EVALUATED,
    EVENT_WINDOW_OVER_CAPACITY,
    EventLog,
    create_event_log,
)
from ray_dispatcher.failures import FailureStore, MemoryFailureStore
from ray_dispatcher.models import (
    BatchRun,
    BatchStatus,
    DispatchRequest,
    DispatcherState,
    ExecutionMode,
    FailureRecord,
    FailureRunDetail,
    HandlerRequest,
    HandlerSpec,
    KafkaSource,
    MultiSourceWindowState,
    PostgresCursor,
    PostgresSource,
    RunStatus,
    SourceKind,
    SourceSpec,
    SourceState,
    TaskRun,
    handler_shard_size,
    merge_batch_windows,
)
from ray_dispatcher.policy import (
    Decision,
    DispatcherConfig,
    SoftAction,
    TriggerContext,
    TriggerPolicy,
    window_bounds,
)
from ray_dispatcher.readers import CompositePayloadReader, PayloadReader
from ray_dispatcher.registries import (
    ResourceRegistry,
    SourceRegistry,
    canonical_json,
    source_canonical_dict,
)
from ray_dispatcher.resources import ResourceLoader
from ray_dispatcher.sources import SourceObserver



class KafkaRetentionGap(RuntimeError):
    pass


class StaleBatchError(RuntimeError):
    pass


@dataclass(frozen=True)
class _KafkaPartitionPrep:
    key: str
    worker_name: str
    source_id: str
    shard: str
    low: int
    high: int
    create: bool
    committed: int
    reset_committed: int | None
    snapshot_committed: int | None
    snapshot_observed: int | None
    snapshot_last_observed_at: float | None
    snapshot_arrival_rate: float | None
    retention_message: str | None
    retention_error: bool


@dataclass(frozen=True)
class _KafkaObservePrep:
    partitions: tuple[_KafkaPartitionPrep, ...]
    now: float


@dataclass(frozen=True)
class _PostgresObservePrep:
    key: str
    worker_name: str
    source_id: str
    table: str
    create: bool
    committed: PostgresCursor | datetime
    upper: PostgresCursor | datetime
    count: int
    snapshot_committed: PostgresCursor | datetime | None
    snapshot_backlog: int | None
    snapshot_last_observed_at: float | None
    snapshot_arrival_rate: float | None
    now: float
    mode: str = "cursor"
    over_capacity: bool = False
    idle_jump: bool = False


@dataclass
class _MultisourceSourcePlan:
    source: KafkaSource
    watermarks: dict[int, tuple[int, int]]
    need_watermarks: bool


@dataclass
class _MultisourceObservePlan:
    group_key: str
    sources: list[_MultisourceSourcePlan]
    need_checkpoint: bool


@dataclass(frozen=True)
class _MultisourceObserveResult:
    group_key: str
    observed_time: datetime | None
    checkpoint_progress: datetime | None
    error: Exception | None = None


@dataclass
class _PendingBatchDurability:
    """Checkpoint / failure IO prepared under lock, executed outside it."""

    batch_id: str
    action: str  # "commit" | "skip"
    writes: dict[str, Any]
    failure_id: str | None = None
    source_state_key: str = ""
    start: Any = None
    end: Any = None
    item_count: int = 0
    worker_names: tuple[str, ...] = ()
    run_details: tuple[FailureRunDetail, ...] = ()
    fetch_refs: tuple[Any, ...] = ()
    persist_error: Exception | None = None
    failure_store_error: Exception | None = None
    materialized_payload: Any = None
    payload_error: str | None = None


class RayDispatcher:
    """Observe source increments, schedule work and monitor Ray references.

    The public methods perform one non-blocking cycle each. ``start`` runs the
    listener, trigger, status, event-log, and (when enabled) workers-reload
    cycles periodically until ``stop`` is called.

    ``workers`` may be a sequence of ``HandlerSpec`` or a directory path; a path
    is scanned with :func:`ray_dispatcher.discovery.discover_workers`. Directory
    mode may also enable hot reload via ``reload_interval``.

    Handlers that share the same ``group_key`` automatically share fetches and
    checkpoints. Single-source groups key by ``source_id``; multi-source Kafka
    groups key by ``ms:...`` and schedule aligned event-time windows.
    """

    def __init__(
        self,
        workers: Union[Sequence[HandlerSpec], str, Path],
        *,
        ray_adapter: RayAdapter | None = None,
        checkpoint_store: CheckpointStore | None = None,
        failure_store: FailureStore | None = None,
        trigger: TriggerPolicy | None = None,
        config: DispatcherConfig | None = None,
        listener_interval: float = 5.0,
        trigger_interval: float = 1.0,
        status_interval: float = 1.0,
        event_log_interval: float = 5.0,
        reload_interval: float = 0.0,
        operation_timeout: float = 30.0,
        source_registry: SourceRegistry | None = None,
        resource_registry: ResourceRegistry | None = None,
        payload_reader: PayloadReader | None = None,
        event_log: EventLog | None = None,
        plugin_store: Any | None = None,
    ) -> None:
        self._injected_source_registry = source_registry
        self._injected_resource_registry = resource_registry
        self.source_registry = source_registry
        self.resource_registry = resource_registry
        self.payload_reader = payload_reader or CompositePayloadReader()
        self.resource_loader = ResourceLoader(resource_registry)
        self._workers_dir: Path | None = None
        self._plugin_store = plugin_store

        if ray_adapter is None:
            from ray_dispatcher.adapter import NativeRayAdapter

            ray_adapter = NativeRayAdapter(resource_loader=self.resource_loader)
        self.ray_adapter = ray_adapter

        if hasattr(ray_adapter, "set_payload_fetch_remote") and getattr(
            ray_adapter, "ray", None
        ):
            from ray_dispatcher.readers import bind_payload_fetch

            remote = ray_adapter.ray.remote(max_retries=0)(
                bind_payload_fetch(self.payload_reader)
            )
            ray_adapter.set_payload_fetch_remote(remote)
        if hasattr(ray_adapter, "set_merge_remote") and getattr(
            ray_adapter, "ray", None
        ):
            from ray_dispatcher.readers import merge_fetch_results

            ray_adapter.set_merge_remote(
                ray_adapter.ray.remote(max_retries=0)(merge_fetch_results)
            )
        if hasattr(ray_adapter, "resource_loader"):
            ray_adapter.resource_loader = self.resource_loader

        if isinstance(workers, (str, Path)):
            from ray_dispatcher.discovery import discover_workers

            self._workers_dir = Path(workers).expanduser().resolve()
            workers, merged_sources, merged_resources = discover_workers(
                self._discover_roots(),
                ray_module=getattr(ray_adapter, "ray", None),
                source_registry=source_registry,
                resource_registry=resource_registry,
            )
            self.source_registry = merged_sources
            self.resource_registry = merged_resources
            self.resource_loader = ResourceLoader(merged_resources)
            if hasattr(ray_adapter, "resource_loader"):
                ray_adapter.resource_loader = self.resource_loader

        if not workers:
            raise ValueError("at least one worker is required")
        names = [worker.name for worker in workers]
        if len(names) != len(set(names)):
            raise ValueError("worker names must be unique")
        if min(listener_interval, trigger_interval, status_interval, operation_timeout) <= 0:
            raise ValueError("polling intervals and operation_timeout must be positive")
        if reload_interval < 0:
            raise ValueError("reload_interval must be >= 0")
        if reload_interval > 0 and self._workers_dir is None:
            raise ValueError("reload_interval requires workers to be a directory path")

        self._install_worker_groups(tuple(workers))
        self.source_observer = SourceObserver()
        self.checkpoint_store = checkpoint_store or MemoryCheckpointStore()
        self.failure_store = failure_store or MemoryFailureStore()
        self.event_log = event_log if event_log is not None else create_event_log()
        self.trigger = trigger or TriggerPolicy.default()
        self.config = config or DispatcherConfig()
        self.listener_interval = listener_interval
        self.trigger_interval = trigger_interval
        self.status_interval = status_interval
        self.event_log_interval = event_log_interval
        self.reload_interval = reload_interval
        self.operation_timeout = operation_timeout
        self.state = DispatcherState()
        self._sync_multisource_windows()
        self._config_fingerprint = self._fingerprint_config(
            self.workers,
            self.source_registry,
            self.resource_registry,
            worker_roots=self._discover_roots(),
        )
        self._lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()

    def _discover_roots(self) -> list[Path]:
        roots: list[Path] = []
        if self._workers_dir is not None:
            roots.append(self._workers_dir)
        store = self._plugin_store
        if store is not None:
            roots.extend(store.list_active_roots())
        return roots

    @property
    def plugin_store(self) -> Any | None:
        return self._plugin_store

    @plugin_store.setter
    def plugin_store(self, value: Any | None) -> None:
        self._plugin_store = value

    @staticmethod
    def _shared_state_key(source_id: str, shard: str) -> str:
        return f"shared:{source_id}:{shard}"

    @staticmethod
    def _mswin_checkpoint_key(group_key: str) -> str:
        return f"mswin:{group_key}"

    @staticmethod
    def _stable_id(*parts: Any) -> str:
        raw = "|".join(repr(part) for part in parts).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:24]

    @staticmethod
    def _ewma(old: float, new: float, alpha: float) -> float:
        return new if old == 0 else alpha * new + (1 - alpha) * old

    def _install_worker_groups(self, workers: Sequence[HandlerSpec]) -> None:
        self.workers = {worker.name: worker for worker in workers}
        self._shared_groups = {}
        shared_groups: dict[str, list[HandlerSpec]] = {}
        for worker in workers:
            shared_groups.setdefault(worker.group_key, []).append(worker)
        for group_key, members in shared_groups.items():
            if members[0].is_multisource:
                source_signatures = {
                    tuple(
                        (source.source_id, self._physical_source_key(source))
                        for source in member.sources
                    )
                    for member in members
                }
                if len(source_signatures) != 1:
                    raise ValueError(
                        f"handlers sharing group_key {group_key!r} must use identical "
                        "sources"
                    )
                self._shared_groups[group_key] = tuple(members)
                continue

            physical_keys = {
                self._physical_source_key(member.sources[0]) for member in members
            }
            if len(physical_keys) != 1:
                raise ValueError(
                    f"handlers sharing source_id {group_key!r} must use one physical "
                    "source"
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
                        f"handlers sharing source_id {group_key!r} must use identical "
                        "initial_offset and retention_policy"
                    )
            else:
                modes = {
                    member.sources[0].mode
                    for member in members
                    if isinstance(member.sources[0], PostgresSource)
                }
                if len(modes) != 1:
                    raise ValueError(
                        f"handlers sharing source_id {group_key!r} must use one "
                        "PostgresSource.mode"
                    )
                mode = next(iter(modes))
                if mode == "event_time":
                    initial_times = {
                        (
                            None
                            if member.sources[0].initial_time is None
                            else member.sources[0].initial_time.isoformat()
                        )
                        for member in members
                        if isinstance(member.sources[0], PostgresSource)
                    }
                    if len(initial_times) != 1:
                        raise ValueError(
                            f"handlers sharing source_id {group_key!r} must use one "
                            "initial_time"
                        )
                else:
                    initial_cursors = {
                        repr(member.sources[0].initial_cursor)
                        for member in members
                        if isinstance(member.sources[0], PostgresSource)
                    }
                    if len(initial_cursors) != 1:
                        raise ValueError(
                            f"handlers sharing source_id {group_key!r} must use one "
                            "initial_cursor"
                        )
            self._shared_groups[group_key] = tuple(members)

    def _sync_multisource_windows(self) -> None:
        active = {
            group_key
            for group_key, members in self._shared_groups.items()
            if members[0].is_multisource
        }
        for group_key in list(self.state.multisource_windows):
            if group_key not in active:
                del self.state.multisource_windows[group_key]
        for group_key, members in self._shared_groups.items():
            if not members[0].is_multisource:
                continue
            if group_key in self.state.multisource_windows:
                continue
            self.state.multisource_windows[group_key] = MultiSourceWindowState(
                group_key=group_key,
                source_ids=tuple(source.source_id for source in members[0].sources),
            )

    @staticmethod
    def _fingerprint_config(
        workers: Mapping[str, HandlerSpec],
        source_registry: SourceRegistry | None,
        resource_registry: ResourceRegistry | None,
        *,
        workers_dir: Path | None = None,
        worker_roots: Sequence[Path] | None = None,
    ) -> str:
        from ray_dispatcher.discovery import worker_roots_code_fingerprint

        handler_rows = []
        for worker in sorted(workers.values(), key=lambda item: item.name):
            handler_rows.append(
                {
                    "name": worker.name,
                    "mode": worker.mode.value,
                    "remote_method": worker.remote_method,
                    "batch_size": list(worker.batch_window),
                    "cpus_per_task": worker.cpus_per_task,
                    "max_retries": worker.max_retries,
                    "priority": worker.priority,
                    "output": None if worker.output is None else dict(worker.output),
                    "resource_ids": list(worker.resource_ids),
                    "sources": [
                        source_canonical_dict(source) for source in worker.sources
                    ],
                }
            )
        sources = {
            name: source_canonical_dict(source)
            for name, source in sorted((source_registry or {}).items())
        }
        resources = {
            name: spec.canonical_dict()
            for name, spec in sorted((resource_registry or {}).items())
        }
        if worker_roots is None:
            roots = [workers_dir] if workers_dir is not None else []
        else:
            roots = list(worker_roots)
        return canonical_json(
            {
                "handlers": handler_rows,
                "sources": sources,
                "resources": resources,
                "code": worker_roots_code_fingerprint(roots),
            }
        )

    @staticmethod
    def _workers_dir_fingerprint(
        workers_dir: Path | None,
    ) -> list[tuple[str, str]]:
        """Stable digest of worker module files (relative path, sha256)."""

        from ray_dispatcher.discovery import worker_roots_code_fingerprint

        if workers_dir is None:
            return []
        return worker_roots_code_fingerprint([workers_dir])

    def _has_inflight_work(self) -> bool:
        if any(
            batch.status is BatchStatus.RUNNING for batch in self.state.batches.values()
        ):
            return True
        if any(state.active_batch_id for state in self.state.sources.values()):
            return True
        if any(
            window.active_batch_id for window in self.state.multisource_windows.values()
        ):
            return True
        return False

    def _emit_event(self, event: str, payload: Mapping[str, Any]) -> None:
        try:
            errors = self.event_log.emit(event, payload)
        except Exception as exc:  # noqa: BLE001 - never break scheduling on hooks
            self.state.loop_errors.append(
                f"event_log:{event}:{type(exc).__name__}: {exc}"
            )
            return
        self.state.loop_errors.extend(errors)

    def _batch_event_payload(
        self,
        batch: BatchRun,
        *,
        progress_key: str,
        failure_id: str | None = None,
    ) -> dict[str, Any]:
        handlers = list(
            batch.worker_names
            or ((batch.worker_name,) if batch.worker_name else ())
        )
        payload: dict[str, Any] = {
            "batch_id": batch.batch_id,
            "progress_key": progress_key,
            "start": repr(batch.start),
            "end": repr(batch.end),
            "item_count": batch.item_count,
            "handlers": handlers,
        }
        if failure_id is not None:
            payload["failure_id"] = failure_id
        elif batch.failure_id is not None:
            payload["failure_id"] = batch.failure_id
        return payload

    @staticmethod
    def _actor_checkpoint_key(handler_id: str, progress_key: str) -> str:
        return f"actor:{handler_id}:{progress_key}"

    @staticmethod
    def _split_handler_result(value: Any) -> tuple[Any, Any | None]:
        if isinstance(value, dict) and "checkpoint_state" in value:
            state = value["checkpoint_state"]
            rest = {key: item for key, item in value.items() if key != "checkpoint_state"}
            return rest, state
        return value, None

    async def _build_handler_request(
        self,
        member: HandlerSpec,
        run_id: str,
        progress_key: str,
    ) -> HandlerRequest:
        checkpoint_state = None
        if member.mode is ExecutionMode.ACTOR:
            needs_restore = False
            prepare = getattr(self.ray_adapter, "prepare_actor_restore", None)
            if callable(prepare):
                needs_restore = bool(prepare(member))
            else:
                claim = getattr(self.ray_adapter, "claim_actor_restore", None)
                if callable(claim):
                    needs_restore = bool(claim(member.name))
            if needs_restore:
                document = await self._io(
                    self.checkpoint_store.load(
                        self._actor_checkpoint_key(member.name, progress_key)
                    )
                )
                if document is not None:
                    checkpoint_state = document.state
        return HandlerRequest(
            dispatch_id=run_id,
            handler_id=member.name,
            output=member.output,
            checkpoint_state=checkpoint_state,
        )

    @staticmethod
    def _handler_request_for_retry(request: HandlerRequest) -> HandlerRequest:
        """Drop restore payload on retry; Actor already applied it on first submit."""

        if request.checkpoint_state is None:
            return request
        return HandlerRequest(
            dispatch_id=request.dispatch_id,
            handler_id=request.handler_id,
            output=request.output,
            checkpoint_state=None,
        )

    def _checkpoint_writes_for_batch(
        self,
        batch: BatchRun,
        *,
        progress_key: str,
        progress: Any,
    ) -> dict[str, CheckpointDocument | Any]:
        writes: dict[str, CheckpointDocument | Any] = {progress_key: progress}
        for run_id in batch.run_ids:
            run = self.state.runs[run_id]
            if run.checkpoint_state is None:
                continue
            writes[self._actor_checkpoint_key(run.worker_name, progress_key)] = (
                CheckpointDocument(progress=progress, state=run.checkpoint_state)
            )
        return writes

    async def data_listener(self) -> dict[str, int]:
        """Observe each physical source once, then fan out to handler checkpoints.

        Remote watermark / checkpoint / count IO runs outside ``_lock`` (physical
        sources in parallel). The lock is held only while merging into memory.
        """

        errors: list[Exception] = []
        source_bindings = self._collect_source_bindings()
        fetched = await asyncio.gather(
            *[
                self._fetch_physical_source(bindings)
                for bindings in source_bindings.values()
            ],
            return_exceptions=True,
        )

        prepared: list[tuple[str, Any]] = []
        for item in fetched:
            if isinstance(item, BaseException):
                # One broken physical source must not prevent later sources
                # from refreshing their independent watermarks.
                errors.append(item if isinstance(item, Exception) else Exception(str(item)))
                continue
            kind, bindings, payload = item
            try:
                if kind == "kafka":
                    assert isinstance(payload, Mapping)
                    observed_source_ids: set[str] = set()
                    for worker, source in bindings:
                        assert isinstance(source, KafkaSource)
                        if source.source_id in observed_source_ids:
                            continue
                        observed_source_ids.add(source.source_id)
                        try:
                            prepared.append(
                                (
                                    "kafka",
                                    await self._prepare_kafka_observe(
                                        worker, source, payload
                                    ),
                                )
                            )
                        except Exception as exc:
                            errors.append(exc)
                elif kind == "postgres_event_time":
                    assert isinstance(payload, datetime)
                    observed_source_ids = set()
                    for worker, source in bindings:
                        assert isinstance(source, PostgresSource)
                        if source.source_id in observed_source_ids:
                            continue
                        observed_source_ids.add(source.source_id)
                        try:
                            prepared.append(
                                (
                                    "postgres_event_time",
                                    await self._prepare_postgres_event_time_observe(
                                        worker, source, payload
                                    ),
                                )
                            )
                        except Exception as exc:
                            errors.append(exc)
                else:
                    assert isinstance(payload, PostgresCursor)
                    observed_source_ids = set()
                    for worker, source in bindings:
                        assert isinstance(source, PostgresSource)
                        if source.source_id in observed_source_ids:
                            continue
                        observed_source_ids.add(source.source_id)
                        try:
                            prepared.append(
                                (
                                    "postgres",
                                    await self._prepare_postgres_observe(
                                        worker, source, payload
                                    ),
                                )
                            )
                        except Exception as exc:
                            errors.append(exc)
            except Exception as exc:
                errors.append(exc)

        async with self._lock:
            for kind, prep in prepared:
                try:
                    if kind == "kafka":
                        self._apply_kafka_observe(prep)
                    else:
                        self._apply_postgres_observe(prep)
                except Exception as exc:
                    errors.append(exc)
            ms_plans = self._plan_multisource_observe()

        ms_results = await self._fetch_multisource_observe(ms_plans)

        async with self._lock:
            for result in ms_results:
                if result.error is not None:
                    errors.append(result.error)
                    continue
                try:
                    self._apply_multisource_observe(result)
                except Exception as exc:
                    errors.append(exc)
            if errors:
                raise errors[0]
            return {key: state.backlog for key, state in self.state.sources.items()}

    def _collect_source_bindings(
        self,
    ) -> dict[tuple[Any, ...], list[tuple[HandlerSpec, SourceSpec]]]:
        source_bindings: dict[tuple[Any, ...], list[tuple[HandlerSpec, SourceSpec]]] = {}
        for worker in self.workers.values():
            for source in worker.sources:
                source_bindings.setdefault(self._physical_source_key(source), []).append(
                    (worker, source)
                )
        return source_bindings

    async def _fetch_physical_source(
        self, bindings: list[tuple[HandlerSpec, SourceSpec]]
    ) -> tuple[str, list[tuple[HandlerSpec, SourceSpec]], Any]:
        representative = bindings[0][1]
        if isinstance(representative, KafkaSource):
            watermarks = await self._io(
                self.source_observer.kafka_watermarks(representative)
            )
            return ("kafka", bindings, watermarks)
        assert isinstance(representative, PostgresSource)
        if representative.mode == "event_time":
            watermark = await self._io(
                self.source_observer.postgres_event_watermark(representative)
            )
            return ("postgres_event_time", bindings, watermark)
        upper = await self._io(
            self.source_observer.postgres_high_watermark(representative)
        )
        return ("postgres", bindings, upper)

    @staticmethod
    def _physical_source_key(source: SourceSpec) -> tuple[Any, ...]:
        if isinstance(source, KafkaSource):
            return (SourceKind.KAFKA, tuple(sorted(source.brokers)), source.topic)
        return (
            SourceKind.POSTGRES,
            source.mode,
            source.dsn,
            source.table,
            source.timestamp_column,
            source.primary_key_column,
        )

    async def _io(self, awaitable: Any) -> Any:
        return await asyncio.wait_for(awaitable, timeout=self.operation_timeout)

    async def _prepare_kafka_observe(
        self,
        worker: HandlerSpec,
        source: KafkaSource,
        watermarks: Mapping[int, tuple[int, int]],
    ) -> _KafkaObservePrep:
        keys = [
            self._shared_state_key(source.source_id, str(partition))
            for partition in watermarks
        ]
        async with self._lock:
            snapshots: dict[str, tuple[int, str | None, int, float, float]] = {}
            for key in keys:
                state = self.state.sources.get(key)
                if state is None:
                    continue
                snapshots[key] = (
                    int(state.committed),
                    state.active_batch_id,
                    int(state.observed),
                    state.last_observed_at,
                    state.arrival_rate,
                )

        partitions: list[_KafkaPartitionPrep] = []
        for partition, (low, high) in watermarks.items():
            if low < 0 or high < low:
                raise ValueError(
                    f"invalid Kafka watermarks for {source.source_id}/{partition}"
                )
            key = self._shared_state_key(source.source_id, str(partition))
            snapshot = snapshots.get(key)
            if snapshot is None:
                checkpoint = await self._io(self.checkpoint_store.load(key))
                progress = None if checkpoint is None else checkpoint.progress
                if progress is not None and not isinstance(progress, int):
                    raise TypeError(f"Kafka checkpoint {key} must be int")
                committed = progress if progress is not None else (
                    low if source.initial_offset == "earliest" else high
                )
                if progress is None:
                    # Persist the baseline immediately. Otherwise a restart
                    # after this poll could adopt a newer high watermark and
                    # silently skip records that arrived in between.
                    await self._io(self.checkpoint_store.save(key, committed))
                create = True
                active_batch_id = None
                snapshot_committed = None
                snapshot_observed = None
                snapshot_last_observed_at = None
                snapshot_arrival_rate = None
            else:
                (
                    committed,
                    active_batch_id,
                    snapshot_observed,
                    snapshot_last_observed_at,
                    snapshot_arrival_rate,
                ) = snapshot
                create = False
                snapshot_committed = committed

            reset_committed: int | None = None
            retention_message: str | None = None
            retention_error = False
            if committed < low or committed > high:
                relation = (
                    "below retained low watermark"
                    if committed < low
                    else "above high watermark"
                )
                boundary = low if committed < low else high
                retention_message = f"checkpoint {committed} is {relation} {boundary}"
                if active_batch_id is not None:
                    raise KafkaRetentionGap(
                        f"{key}: {retention_message}; cannot reset while batch "
                        f"{active_batch_id} is active"
                    )
                if source.retention_policy == "error":
                    retention_error = True
                else:
                    await self._io(self.checkpoint_store.save(key, low))
                    reset_committed = low
                    committed = low
                    retention_message = None

            partitions.append(
                _KafkaPartitionPrep(
                    key=key,
                    worker_name=worker.name,
                    source_id=source.source_id,
                    shard=str(partition),
                    low=low,
                    high=high,
                    create=create,
                    committed=committed,
                    reset_committed=reset_committed,
                    snapshot_committed=snapshot_committed,
                    snapshot_observed=snapshot_observed,
                    snapshot_last_observed_at=snapshot_last_observed_at,
                    snapshot_arrival_rate=snapshot_arrival_rate,
                    retention_message=retention_message,
                    retention_error=retention_error,
                )
            )
        return _KafkaObservePrep(partitions=tuple(partitions), now=time.monotonic())

    def _apply_kafka_observe(self, prep: _KafkaObservePrep) -> None:
        now = prep.now
        for item in prep.partitions:
            state = self.state.sources.get(item.key)
            if state is None:
                if not item.create:
                    continue
                state = SourceState(
                    item.key,
                    item.worker_name,
                    item.source_id,
                    SourceKind.KAFKA,
                    item.shard,
                    item.committed,
                    item.high,
                )
                self.state.sources[item.key] = state
            elif item.create:
                # Another loop created the key; keep existing committed / batch.
                pass

            if item.retention_error:
                message = item.retention_message or (
                    f"checkpoint {int(state.committed)} is outside watermarks"
                )
                state.retention_gap = message
                raise KafkaRetentionGap(f"{item.key}: {message}")

            if item.reset_committed is not None:
                if state.active_batch_id is not None:
                    message = item.retention_message or (
                        f"checkpoint {int(state.committed)} is outside watermarks"
                    )
                    state.retention_gap = message
                    raise KafkaRetentionGap(
                        f"{item.key}: {message}; cannot reset while batch "
                        f"{state.active_batch_id} is active"
                    )
                if int(state.committed) < item.low or int(state.committed) > item.high:
                    state.committed = item.reset_committed

            committed = int(state.committed)
            if committed < item.low or committed > item.high:
                relation = (
                    "below retained low watermark"
                    if committed < item.low
                    else "above high watermark"
                )
                boundary = item.low if committed < item.low else item.high
                message = f"checkpoint {committed} is {relation} {boundary}"
                state.retention_gap = message
                raise KafkaRetentionGap(f"{item.key}: {message}")

            state.retention_gap = None
            previous_high = (
                item.snapshot_observed
                if item.snapshot_observed is not None
                else int(state.observed)
            )
            last_observed_at = (
                item.snapshot_last_observed_at
                if item.snapshot_last_observed_at is not None
                else state.last_observed_at
            )
            arrival_rate = (
                item.snapshot_arrival_rate
                if item.snapshot_arrival_rate is not None
                else state.arrival_rate
            )
            elapsed = max(now - last_observed_at, 1e-6)
            arrived = max(0, item.high - previous_high)
            state.arrival_rate = self._ewma(
                arrival_rate, arrived / elapsed, self.config.ewma_alpha
            )
            state.observed = item.high
            state.backlog = max(0, item.high - committed)
            state.last_observed_at = now

    async def _prepare_postgres_observe(
        self,
        worker: HandlerSpec,
        source: PostgresSource,
        upper: PostgresCursor,
    ) -> _PostgresObservePrep:
        key = self._shared_state_key(source.source_id, source.table)
        async with self._lock:
            state = self.state.sources.get(key)
            snapshot: tuple[PostgresCursor, int, float, float] | None = None
            if state is not None:
                if not isinstance(state.committed, PostgresCursor):
                    raise TypeError(f"invalid Postgres state for {key}")
                snapshot = (
                    state.committed,
                    state.backlog,
                    state.last_observed_at,
                    state.arrival_rate,
                )

        create = False
        if snapshot is None:
            checkpoint = await self._io(self.checkpoint_store.load(key))
            progress = None if checkpoint is None else checkpoint.progress
            if progress is not None and not isinstance(progress, PostgresCursor):
                raise TypeError(f"Postgres checkpoint {key} must be PostgresCursor")
            committed = progress or source.initial_cursor or upper
            if progress is None:
                await self._io(self.checkpoint_store.save(key, committed))
            create = True
            snapshot_committed = None
            snapshot_backlog = None
            snapshot_last_observed_at = None
            snapshot_arrival_rate = None
        else:
            (
                committed,
                snapshot_backlog,
                snapshot_last_observed_at,
                snapshot_arrival_rate,
            ) = snapshot
            snapshot_committed = committed

        count = 0 if upper <= committed else await self._io(
            self.source_observer.postgres_count(source, committed, upper)
        )
        return _PostgresObservePrep(
            key=key,
            worker_name=worker.name,
            source_id=source.source_id,
            table=source.table,
            create=create,
            committed=committed,
            upper=upper,
            count=count,
            snapshot_committed=snapshot_committed,
            snapshot_backlog=snapshot_backlog,
            snapshot_last_observed_at=snapshot_last_observed_at,
            snapshot_arrival_rate=snapshot_arrival_rate,
            now=time.monotonic(),
            mode="cursor",
        )

    async def _prepare_postgres_event_time_observe(
        self,
        worker: HandlerSpec,
        source: PostgresSource,
        watermark: datetime,
    ) -> _PostgresObservePrep:
        key = self._shared_state_key(source.source_id, source.table)
        async with self._lock:
            state = self.state.sources.get(key)
            snapshot: tuple[datetime, int, float, float] | None = None
            if state is not None:
                if not isinstance(state.committed, datetime):
                    raise TypeError(
                        f"invalid Postgres event_time state for {key}"
                    )
                snapshot = (
                    state.committed,
                    state.backlog,
                    state.last_observed_at,
                    state.arrival_rate,
                )

        create = False
        if snapshot is None:
            checkpoint = await self._io(self.checkpoint_store.load(key))
            progress = None if checkpoint is None else checkpoint.progress
            if progress is not None and not isinstance(progress, datetime):
                raise TypeError(
                    f"Postgres event_time checkpoint {key} must be datetime"
                )
            committed = progress or source.initial_time
            if committed is None:
                raise ValueError(
                    f"{source.source_id}: event_time mode requires checkpoint "
                    "progress or initial_time"
                )
            if progress is None:
                await self._io(self.checkpoint_store.save(key, committed))
            create = True
            snapshot_committed = None
            snapshot_backlog = None
            snapshot_last_observed_at = None
            snapshot_arrival_rate = None
        else:
            (
                committed,
                snapshot_backlog,
                snapshot_last_observed_at,
                snapshot_arrival_rate,
            ) = snapshot
            snapshot_committed = committed

        left, right, count, over_capacity = await self._io(
            self.source_observer.postgres_plan_event_window(
                source, committed, watermark
            )
        )
        idle_jump = count == 0
        if idle_jump and right != committed:
            # Advance past empty gap to watermark so observe does not re-scan.
            await self._io(self.checkpoint_store.save(key, right))
            left = right
        return _PostgresObservePrep(
            key=key,
            worker_name=worker.name,
            source_id=source.source_id,
            table=source.table,
            create=create,
            committed=left,
            upper=right,
            count=count,
            snapshot_committed=snapshot_committed,
            snapshot_backlog=snapshot_backlog,
            snapshot_last_observed_at=snapshot_last_observed_at,
            snapshot_arrival_rate=snapshot_arrival_rate,
            now=time.monotonic(),
            mode="event_time",
            over_capacity=over_capacity,
            idle_jump=idle_jump,
        )

    def _apply_postgres_observe(self, prep: _PostgresObservePrep) -> None:
        state = self.state.sources.get(prep.key)
        if state is None:
            if not prep.create:
                return
            state = SourceState(
                prep.key,
                prep.worker_name,
                prep.source_id,
                SourceKind.POSTGRES,
                prep.table,
                prep.committed,
                prep.upper,
            )
            self.state.sources[prep.key] = state
        elif prep.create:
            # Key appeared concurrently; keep existing committed.
            if state.committed != prep.committed and not prep.idle_jump:
                return
        elif (
            prep.snapshot_committed is not None
            and state.committed != prep.snapshot_committed
        ):
            # Commit landed between prepare and apply; skip stale rate update.
            return

        if prep.mode == "event_time":
            if not isinstance(state.committed, datetime):
                raise TypeError(f"invalid Postgres event_time state for {prep.key}")
        elif not isinstance(state.committed, PostgresCursor):
            raise TypeError(f"invalid Postgres state for {prep.key}")

        if prep.idle_jump:
            # Empty (L, W]: checkpoint already advanced; keep backlog at 0.
            state.committed = prep.committed
            state.observed = prep.upper
            state.backlog = 0
            state.over_capacity = False
            state.last_observed_at = prep.now
            return

        previous = (
            prep.snapshot_backlog
            if prep.snapshot_backlog is not None
            else state.backlog
        )
        last_observed_at = (
            prep.snapshot_last_observed_at
            if prep.snapshot_last_observed_at is not None
            else state.last_observed_at
        )
        arrival_rate = (
            prep.snapshot_arrival_rate
            if prep.snapshot_arrival_rate is not None
            else state.arrival_rate
        )
        elapsed = max(prep.now - last_observed_at, 1e-6)
        arrived = max(0, prep.count - previous)
        state.arrival_rate = self._ewma(
            arrival_rate, arrived / elapsed, self.config.ewma_alpha
        )
        # Gap align may advance committed to pred(min_ts) before scheduling.
        if prep.mode == "event_time":
            state.committed = prep.committed
        state.observed = prep.upper
        state.backlog = max(0, prep.count)
        state.over_capacity = prep.over_capacity
        state.last_observed_at = prep.now
        if prep.over_capacity:
            self._emit_event(
                EVENT_WINDOW_OVER_CAPACITY,
                {
                    "source_id": prep.source_id,
                    "source_key": prep.key,
                    "committed": (
                        prep.committed.isoformat()
                        if isinstance(prep.committed, datetime)
                        else str(prep.committed)
                    ),
                    "observed": (
                        prep.upper.isoformat()
                        if isinstance(prep.upper, datetime)
                        else str(prep.upper)
                    ),
                    "count": prep.count,
                    "alert": (
                        "event_time window exceeds max_rows at min_window_seconds; "
                        "fetching full window"
                    ),
                },
            )
            self.state.loop_errors.append(
                f"window_over_capacity:{prep.source_id}:count={prep.count}"
            )

    def _plan_multisource_observe(self) -> list[_MultisourceObservePlan]:
        plans: list[_MultisourceObservePlan] = []
        for group_key, members in self._shared_groups.items():
            if not members[0].is_multisource:
                continue
            window = self.state.multisource_windows[group_key]
            source_plans: list[_MultisourceSourcePlan] = []
            for source in members[0].sources:
                assert isinstance(source, KafkaSource)
                watermarks: dict[int, tuple[int, int]] = {}
                for state in self.state.sources.values():
                    if state.source_id != source.source_id:
                        continue
                    partition = int(state.shard)
                    high = int(state.observed)
                    # Low is unknown here; event-time highs only need highs.
                    watermarks[partition] = (0, high)
                source_plans.append(
                    _MultisourceSourcePlan(
                        source=source,
                        watermarks=watermarks,
                        need_watermarks=not watermarks,
                    )
                )
            plans.append(
                _MultisourceObservePlan(
                    group_key=group_key,
                    sources=source_plans,
                    need_checkpoint=window.committed_time is None,
                )
            )
        return plans

    async def _fetch_multisource_observe(
        self, plans: Sequence[_MultisourceObservePlan]
    ) -> list[_MultisourceObserveResult]:
        async def fetch_one(plan: _MultisourceObservePlan) -> _MultisourceObserveResult:
            try:
                async def source_event_high(
                    source_plan: _MultisourceSourcePlan,
                ) -> datetime | None:
                    watermarks = source_plan.watermarks
                    if source_plan.need_watermarks:
                        watermarks = dict(
                            await self._io(
                                self.source_observer.kafka_watermarks(source_plan.source)
                            )
                        )
                    event_highs = await self._io(
                        self.source_observer.kafka_event_time_highs(
                            source_plan.source, watermarks
                        )
                    )
                    if not event_highs:
                        return None
                    return max(event_highs.values())

                highs = await asyncio.gather(
                    *[source_event_high(item) for item in plan.sources]
                )
                if any(high is None for high in highs) or not highs:
                    t_right: datetime | None = None
                else:
                    t_right = min(high for high in highs if high is not None)

                checkpoint_progress: datetime | None = None
                if plan.need_checkpoint:
                    checkpoint = await self._io(
                        self.checkpoint_store.load(
                            self._mswin_checkpoint_key(plan.group_key)
                        )
                    )
                    if checkpoint is not None:
                        if not isinstance(checkpoint.progress, datetime):
                            raise TypeError(
                                f"multi-source checkpoint {plan.group_key} must be datetime"
                            )
                        checkpoint_progress = checkpoint.progress
                return _MultisourceObserveResult(
                    group_key=plan.group_key,
                    observed_time=t_right,
                    checkpoint_progress=checkpoint_progress,
                )
            except Exception as exc:
                return _MultisourceObserveResult(
                    group_key=plan.group_key,
                    observed_time=None,
                    checkpoint_progress=None,
                    error=exc,
                )

        if not plans:
            return []
        return list(await asyncio.gather(*[fetch_one(plan) for plan in plans]))

    def _apply_multisource_observe(self, result: _MultisourceObserveResult) -> None:
        window = self.state.multisource_windows[result.group_key]
        if window.committed_time is None and result.checkpoint_progress is not None:
            window.committed_time = result.checkpoint_progress
        window.observed_time = result.observed_time

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
            free_slots = max(0, self.config.max_in_flight - active_global)
            available_cpus = self.ray_adapter.available_cpus()
            now = time.monotonic()
            # Coarse prefilter; TriggerPolicy is authoritative.
            candidates = [
                state
                for state in self.state.sources.values()
                if state.backlog > 0
                and state.active_batch_id is None
                and not state.retention_gap
            ]
            candidates.sort(
                key=lambda state: self.trigger.rank_key(
                    self.workers[state.worker_name], state, now
                ),
                reverse=True,
            )

            for source_state in candidates:
                if free_slots <= 0:
                    break
                members = self._shared_groups.get(source_state.source_id)
                if members is None or members[0].is_multisource:
                    continue
                representative = members[0]
                event_time = self._is_postgres_event_time(representative)
                handler_slots = self._shared_handler_slots(
                    members, source_state.backlog, event_time=event_time
                )
                slots_per_slice = handler_slots + 1
                phase_cpus = self._phase_cpus(members)
                decision = self.trigger.evaluate(
                    TriggerContext(
                        handler=representative,
                        source_state=source_state,
                        free_slots=free_slots,
                        available_cpus=available_cpus,
                        now=now,
                        config=self.config,
                        slots_per_slice=slots_per_slice,
                        phase_cpus=phase_cpus,
                    )
                )
                self._emit_event(
                    EVENT_TRIGGER_EVALUATED,
                    decision.to_event_payload(source_key=source_state.key),
                )
                if decision.decision is Decision.SKIP:
                    continue
                if decision.decision is Decision.BLOCK:
                    break
                min_items, max_items = merge_batch_windows(
                    tuple(member.batch_window for member in members)
                )
                # event_time: batch_size is shard size only; backlog>0 is enough.
                if not event_time and source_state.backlog < min_items:
                    continue
                n = self._shared_wave_slots(
                    members,
                    free_slots,
                    available_cpus,
                    handler_slots=handler_slots,
                )
                if (
                    decision.decision is Decision.DEGRADE
                    and decision.soft_action is SoftAction.THROTTLE
                    and decision.concurrency_factor is not None
                ):
                    n = max(0, math.floor(n * decision.concurrency_factor))
                if n == 0:
                    continue
                run_ids = await self._create_shared_batch(
                    members,
                    source_state,
                    max_items=None if event_time else max_items,
                )
                submitted.extend(run_ids)
                reserved = (
                    (1 + handler_slots)
                    if run_ids
                    else 0
                )
                free_slots = max(0, free_slots - reserved)
                if available_cpus is not None:
                    available_cpus = max(
                        0.0,
                        available_cpus - len(run_ids) * self.config.fetch_cpus,
                    )

            for group_key, window in self.state.multisource_windows.items():
                if free_slots <= 0:
                    break
                members = self._shared_groups[group_key]
                if window.observed_time is None:
                    continue
                if window.committed_time is None:
                    checkpoint = await self._io(
                        self.checkpoint_store.load(
                            self._mswin_checkpoint_key(group_key)
                        )
                    )
                    if checkpoint is not None:
                        if not isinstance(checkpoint.progress, datetime):
                            raise TypeError(
                                f"multi-source checkpoint {group_key} must be datetime"
                            )
                        window.committed_time = checkpoint.progress
                    else:
                        window.committed_time = window.observed_time
                        await self._io(
                            self.checkpoint_store.save(
                                self._mswin_checkpoint_key(group_key),
                                window.committed_time,
                            )
                        )
                        continue
                t_left, t_right = window_bounds(
                    window, max_window_seconds=self.config.max_window_seconds
                )
                representative = members[0]
                slots_per_slice = len(members) + 1
                phase_cpus = self._phase_cpus(members)
                decision = self.trigger.evaluate(
                    TriggerContext(
                        handler=representative,
                        source_state=None,
                        free_slots=free_slots,
                        available_cpus=available_cpus,
                        now=now,
                        config=self.config,
                        window=window,
                        window_start=t_left,
                        window_end=t_right,
                        slots_per_slice=slots_per_slice,
                        phase_cpus=phase_cpus,
                    ),
                    multisource=True,
                )
                self._emit_event(
                    EVENT_TRIGGER_EVALUATED,
                    decision.to_event_payload(group_key=group_key),
                )
                if decision.decision is Decision.SKIP:
                    continue
                if decision.decision is Decision.BLOCK:
                    break
                assert t_left is not None and t_right is not None
                min_items, max_items = merge_batch_windows(
                    tuple(member.batch_window for member in members)
                )
                run_ids = await self._create_multisource_window_batch(
                    members,
                    window,
                    t_left,
                    t_right,
                    min_items=min_items,
                    max_items=max_items,
                )
                if not run_ids and not any(
                    batch.group_key == group_key
                    and batch.status is BatchStatus.RUNNING
                    for batch in self.state.batches.values()
                ):
                    continue
                submitted.extend(run_ids)
                batch = self.state.batches.get(window.active_batch_id or "")
                reserved = (
                    len(run_ids) + 1 + len(members)
                    if batch is not None
                    else 0
                )
                free_slots = max(0, free_slots - reserved)
                if available_cpus is not None and run_ids:
                    available_cpus = max(
                        0.0,
                        available_cpus - len(run_ids) * self.config.fetch_cpus,
                    )
            return submitted

    def _phase_cpus(self, members: Sequence[HandlerSpec]) -> float:
        downstream_cpus = sum(
            member.cpus_per_task
            for member in members
            if member.mode is ExecutionMode.TASK
        )
        return max(self.config.fetch_cpus, downstream_cpus)

    @staticmethod
    def _is_postgres_event_time(handler: HandlerSpec) -> bool:
        source = handler.sources[0]
        return isinstance(source, PostgresSource) and source.mode == "event_time"

    @staticmethod
    def _shared_handler_slots(
        members: tuple[HandlerSpec, ...],
        backlog: int,
        *,
        event_time: bool,
    ) -> int:
        if not event_time or backlog <= 0:
            return len(members)
        total = 0
        for member in members:
            chunk = handler_shard_size(member.batch_window)
            total += max(1, math.ceil(backlog / chunk))
        return total

    def _shared_wave_slots(
        self,
        members: tuple[HandlerSpec, ...],
        free_slots: int,
        available_cpus: float | None,
        *,
        handler_slots: int | None = None,
    ) -> int:
        """Return 1 when capacity allows a single shared wave, else 0."""

        per_slice_slots = (handler_slots if handler_slots is not None else len(members)) + 1
        if free_slots < per_slice_slots:
            return 0
        if available_cpus is not None:
            phase_cpus = self._phase_cpus(members)
            if available_cpus < phase_cpus:
                return 0
        return 1

    async def _create_shared_batch(
        self,
        members: tuple[HandlerSpec, ...],
        source_state: SourceState,
        *,
        max_items: int | None,
    ) -> list[str]:
        group = source_state.source_id
        representative = members[0]
        source = representative.sources[0]
        start, observed = source_state.committed, source_state.observed
        backlog = source_state.backlog
        take = backlog if max_items is None else min(backlog, max_items)
        if take <= 0:
            return []
        ranges: list[
            tuple[int | PostgresCursor | datetime, int | PostgresCursor | datetime, int]
        ] = []
        window_start: datetime | None = None
        window_end: datetime | None = None
        event_time = isinstance(source, PostgresSource) and source.mode == "event_time"

        if isinstance(source, KafkaSource):
            start_i = int(start)
            end_i = min(int(observed), start_i + take)
            item_count = end_i - start_i
            if item_count <= 0:
                return []
            ranges.append((start_i, end_i, item_count))
            end: int | PostgresCursor | datetime = end_i
        elif event_time:
            if not isinstance(start, datetime) or not isinstance(observed, datetime):
                raise TypeError("Postgres event_time batch requires datetime bounds")
            item_count = take
            ranges.append((start, observed, item_count))
            end = observed
            window_start = start
            window_end = observed
        else:
            if not isinstance(start, PostgresCursor) or not isinstance(
                observed, PostgresCursor
            ):
                raise TypeError("Postgres shared batch requires cursor bounds")
            splitter = getattr(self.source_observer, "postgres_ranges", None)
            if splitter is None:
                ranges = [(start, observed, take)]
            else:
                ranges = list(
                    await self._io(
                        splitter(source, start, observed, 1, take)
                    )
                )
            if not ranges:
                return []
            end = ranges[-1][1]
            item_count = sum(count for _, _, count in ranges)

        handler_slots = self._shared_handler_slots(
            members, item_count, event_time=event_time
        )
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
            worker_names=tuple(member.name for member in members),
            fetch_run_ids=fetch_run_ids,
            reserved_handler_count=handler_slots,
            window_start=window_start,
            window_end=window_end,
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
                1,
                partition=(
                    int(source_state.shard) if isinstance(source, KafkaSource) else None
                ),
                start_offset=(int(chunk_start) if isinstance(source, KafkaSource) else None),
                end_offset=(int(chunk_end) if isinstance(source, KafkaSource) else None),
                start_cursor=(
                    chunk_start
                    if isinstance(source, PostgresSource) and not event_time
                    else None
                ),
                end_cursor=(
                    chunk_end
                    if isinstance(source, PostgresSource) and not event_time
                    else None
                ),
                topic=source.topic if isinstance(source, KafkaSource) else None,
                table=source.table if isinstance(source, PostgresSource) else None,
                window_start=window_start if event_time else None,
                window_end=window_end if event_time else None,
            )
            try:
                ref = self.ray_adapter.submit_fetch(
                    representative,
                    request,
                    source,
                    fetch_cpus=self.config.fetch_cpus,
                )
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

    async def _create_multisource_window_batch(
        self,
        members: tuple[HandlerSpec, ...],
        window: MultiSourceWindowState,
        t_left: datetime,
        t_right: datetime,
        *,
        min_items: int,
        max_items: int | None,
    ) -> list[str]:
        representative = members[0]
        group_key = window.group_key
        source_ids = tuple(source.source_id for source in representative.sources)
        planned: list[
            tuple[KafkaSource, SourceState, int, int]
        ] = []
        partition_commits: dict[str, int] = {}
        source_state_keys: list[str] = []
        involved_states: list[SourceState] = []
        item_count = 0

        for source in representative.sources:
            assert isinstance(source, KafkaSource)
            partition_states = [
                state
                for state in self.state.sources.values()
                if state.source_id == source.source_id
            ]
            if any(state.active_batch_id is not None for state in partition_states):
                return []
            if not partition_states:
                continue
            partitions = [int(state.shard) for state in partition_states]
            watermarks = await self._io(self.source_observer.kafka_watermarks(source))
            start_map = await self._io(
                self.source_observer.kafka_offsets_for_times(
                    source, {partition: t_left for partition in partitions}
                )
            )
            end_map = await self._io(
                self.source_observer.kafka_offsets_for_times(
                    source, {partition: t_right for partition in partitions}
                )
            )
            for state in partition_states:
                partition = int(state.shard)
                low, high = watermarks.get(partition, (0, int(state.observed)))
                start_offset = int(start_map.get(partition, high))
                end_offset = int(end_map.get(partition, high))
                committed = int(state.committed)
                observed_high = int(state.observed)
                start_offset = max(start_offset, committed)
                end_offset = min(end_offset, observed_high)
                if end_offset < start_offset:
                    end_offset = start_offset
                source_state_keys.append(state.key)
                partition_commits[state.key] = end_offset
                involved_states.append(state)
                if end_offset > start_offset:
                    item_count += end_offset - start_offset
                    planned.append((source, state, start_offset, end_offset))

        if item_count < min_items:
            return []
        if max_items is not None and item_count > max_items:
            planned, partition_commits, item_count = self._trim_multisource_planned(
                planned, partition_commits, max_items
            )
            if item_count < min_items:
                return []

        batch_id = self._stable_id("mswin", group_key, t_left, t_right)
        fetch_run_ids: list[str] = []
        fetch_source_ids: list[str] = []
        batch = BatchRun(
            batch_id,
            group_key,
            representative.name,
            t_left,
            t_right,
            item_count,
            [],
            worker_names=tuple(member.name for member in members),
            fetch_run_ids=fetch_run_ids,
            reserved_handler_count=len(members),
            window_start=t_left,
            window_end=t_right,
            source_state_keys=tuple(source_state_keys),
            partition_commits=partition_commits,
            group_key=group_key,
            fetch_source_ids=tuple(fetch_source_ids),
        )
        self.state.batches[batch_id] = batch
        window.active_batch_id = batch_id
        window.last_scheduled_at = time.monotonic()
        for state in involved_states:
            state.active_batch_id = batch_id
            state.last_scheduled_at = time.monotonic()

        for source, state, start_offset, end_offset in planned:
            fetch_id = self._stable_id(
                batch_id,
                "fetch",
                source.source_id,
                state.shard,
                start_offset,
                end_offset,
            )
            request = DispatchRequest(
                fetch_id,
                f"fetch:{group_key}",
                source.source_id,
                source.kind,
                0,
                1,
                partition=int(state.shard),
                start_offset=start_offset,
                end_offset=end_offset,
                topic=source.topic,
                window_start=t_left,
                window_end=t_right,
                source_ids=source_ids,
            )
            try:
                ref = self.ray_adapter.submit_fetch(
                    representative,
                    request,
                    source,
                    fetch_cpus=self.config.fetch_cpus,
                )
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
            fetch_source_ids.append(source.source_id)
        batch.fetch_source_ids = tuple(fetch_source_ids)
        return fetch_run_ids

    @staticmethod
    def _trim_multisource_planned(
        planned: list[tuple[KafkaSource, SourceState, int, int]],
        partition_commits: dict[str, int],
        max_items: int,
    ) -> tuple[
        list[tuple[KafkaSource, SourceState, int, int]],
        dict[str, int],
        int,
    ]:
        """Cap total multisource items to ``max_items`` without multi-slice waves."""

        remaining = max_items
        trimmed: list[tuple[KafkaSource, SourceState, int, int]] = []
        for source, state, start_offset, end_offset in planned:
            size = end_offset - start_offset
            if remaining <= 0:
                partition_commits[state.key] = start_offset
                continue
            if size <= remaining:
                trimmed.append((source, state, start_offset, end_offset))
                remaining -= size
                continue
            new_end = start_offset + remaining
            trimmed.append((source, state, start_offset, new_end))
            partition_commits[state.key] = new_end
            remaining = 0
        item_count = sum(end - start for _, _, start, end in trimmed)
        return trimmed, partition_commits, item_count

    async def _submit_multisource_merge(self, batch: BatchRun) -> str:
        members = tuple(self.workers[name] for name in batch.worker_names)
        representative = members[0]
        group_source_ids = tuple(
            source.source_id for source in representative.sources
        )
        fetch_refs = [
            self.state.runs[run_id].ref for run_id in batch.fetch_run_ids
        ]
        # Parallel ids for zip; append missing group sources so merge fills [].
        merge_source_ids = batch.fetch_source_ids + tuple(
            source_id
            for source_id in group_source_ids
            if source_id not in batch.fetch_source_ids
        )
        merge_id = self._stable_id(batch.batch_id, "merge", batch.start, batch.end)
        request = DispatchRequest(
            merge_id,
            f"merge:{batch.group_key}",
            representative.source_id,
            SourceKind.KAFKA,
            0,
            1,
            window_start=batch.window_start,
            window_end=batch.window_end,
            source_ids=merge_source_ids,
        )
        try:
            ref = self.ray_adapter.submit_merge(
                representative, merge_source_ids, fetch_refs
            )
            run = TaskRun(
                merge_id,
                batch.batch_id,
                representative.name,
                request,
                ref,
                kind="merge",
            )
        except Exception as exc:
            run = TaskRun(
                merge_id,
                batch.batch_id,
                representative.name,
                request,
                None,
                error=f"merge submission failed: {type(exc).__name__}: {exc}",
                kind="merge",
            )
        self.state.runs[merge_id] = run
        batch.merge_run_id = merge_id
        return merge_id

    async def _submit_multisource_handlers(self, batch: BatchRun) -> list[str]:
        members = tuple(self.workers[name] for name in batch.worker_names)
        assert batch.merge_run_id is not None
        merge_run = self.state.runs[batch.merge_run_id]
        assert batch.group_key is not None
        actor_progress_key = self._mswin_checkpoint_key(batch.group_key)
        submitted: list[str] = []
        for member in members:
            run_id = self._stable_id(
                batch.batch_id,
                member.name,
                batch.window_start,
                batch.window_end,
            )
            handler_request = await self._build_handler_request(
                member, run_id, actor_progress_key
            )
            try:
                ref = self.ray_adapter.submit(
                    member, handler_request, merge_run.ref
                )
                run = TaskRun(
                    run_id,
                    batch.batch_id,
                    member.name,
                    handler_request,
                    ref,
                    data_ref=merge_run.ref,
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
                    data_ref=merge_run.ref,
                )
            self.state.runs[run_id] = run
            batch.run_ids.append(run_id)
            submitted.append(run_id)
        batch.reserved_handler_count = 0
        return submitted

    async def ray_status(self) -> dict[str, RunStatus]:
        """Poll current refs, retry failures, and commit completed batches.

        Checkpoint / failure durability runs outside ``_lock``; the lock is held
        only while polling refs, mutating run/batch state, and finalizing after IO.
        """

        pending: list[_PendingBatchDurability] = []
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
            outcomes = self.ray_adapter.poll(refs)
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
                    if run.kind == "handler":
                        result, checkpoint_state = self._split_handler_result(
                            outcome.value
                        )
                        run.result = result
                        run.checkpoint_state = checkpoint_state
                    else:
                        run.result = None
                    run.finished_at = time.monotonic()
                else:
                    await self._handle_failure(run, outcome.error or "unknown Ray failure")

            for batch in list(self.state.batches.values()):
                if batch.status is not BatchStatus.RUNNING:
                    continue
                fetch_runs = [
                    self.state.runs[run_id]
                    for run_id in batch.fetch_run_ids
                ]
                if fetch_runs and self._all_terminal(fetch_runs):
                    if any(run.status is RunStatus.FAILED for run in fetch_runs):
                        batch.reserved_handler_count = 0
                        pending.append(self._begin_skip_failed_batch(batch))
                        continue
                if batch.group_key:
                    fetches_ready = (not fetch_runs) or all(
                        run.status is RunStatus.SUCCEEDED for run in fetch_runs
                    )
                    if (
                        fetches_ready
                        and not batch.merge_run_id
                        and not batch.run_ids
                    ):
                        await self._submit_multisource_merge(batch)
                    if batch.merge_run_id and not batch.run_ids:
                        merge_run = self.state.runs[batch.merge_run_id]
                        if merge_run.status is RunStatus.FAILED:
                            batch.reserved_handler_count = 0
                            pending.append(self._begin_skip_failed_batch(batch))
                            continue
                        if merge_run.status is RunStatus.SUCCEEDED:
                            await self._submit_multisource_handlers(batch)
                elif (
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
                    pending.append(self._begin_skip_failed_batch(batch))
                else:
                    pending.append(self._begin_commit_batch(batch))

        for item in pending:
            await self._persist_batch_durability(item)

        async with self._lock:
            for item in pending:
                self._finalize_batch_durability(item)
            return {run_id: run.status for run_id, run in self.state.runs.items()}

    async def _submit_shared_handlers(self, batch: BatchRun) -> list[str]:
        members = tuple(self.workers[name] for name in batch.worker_names)
        submitted: list[str] = []
        event_time = batch.window_start is not None and batch.window_end is not None
        for fetch_run_id in batch.fetch_run_ids:
            fetch_run = self.state.runs[fetch_run_id]
            request = fetch_run.request
            for member in members:
                shard_size = (
                    handler_shard_size(member.batch_window) if event_time else None
                )
                n_shards = (
                    max(1, math.ceil(batch.item_count / shard_size))
                    if shard_size is not None and batch.item_count > 0
                    else 1
                )
                if batch.item_count == 0:
                    n_shards = 0
                for shard_index in range(n_shards):
                    assert shard_size is not None or n_shards == 1
                    start_i = shard_index * (shard_size or 0)
                    end_i = (
                        min(batch.item_count, start_i + shard_size)
                        if shard_size is not None
                        else batch.item_count
                    )
                    run_id = self._stable_id(
                        batch.batch_id,
                        member.name,
                        request.task_index,
                        request.start_offset,
                        request.end_offset,
                        request.start_cursor,
                        request.end_cursor,
                        request.window_start,
                        request.window_end,
                        shard_index,
                        start_i,
                        end_i,
                    )
                    handler_request = await self._build_handler_request(
                        member, run_id, batch.source_state_key
                    )
                    data_ref = fetch_run.ref
                    try:
                        if event_time and data_ref is not None:
                            data_ref = self.ray_adapter.submit_slice(
                                data_ref, start_i, end_i
                            )
                        ref = self.ray_adapter.submit(
                            member, handler_request, data_ref
                        )
                        run = TaskRun(
                            run_id,
                            batch.batch_id,
                            member.name,
                            handler_request,
                            ref,
                            data_ref=data_ref,
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
                            data_ref=data_ref,
                        )
                    self.state.runs[run_id] = run
                    batch.run_ids.append(run_id)
                    submitted.append(run_id)
        batch.reserved_handler_count = 0
        return submitted

    async def _handle_failure(self, run: TaskRun, error: str) -> None:
        worker = self.workers[run.worker_name]
        max_retries = (
            self.config.fetch_max_retries
            if run.kind in ("fetch", "merge")
            else worker.max_retries
        )
        if run.attempt <= max_retries:
            run.attempt += 1
            run.error = error
            try:
                if run.kind == "fetch":
                    source = next(
                        (
                            item
                            for item in worker.sources
                            if item.source_id == run.request.source_id
                        ),
                        worker.sources[0],
                    )
                    run.ref = self.ray_adapter.submit_fetch(
                        worker,
                        run.request,
                        source,
                        fetch_cpus=self.config.fetch_cpus,
                    )
                elif run.kind == "merge":
                    batch = self.state.batches[run.batch_id]
                    fetch_refs = [
                        self.state.runs[run_id].ref
                        for run_id in batch.fetch_run_ids
                    ]
                    run.ref = self.ray_adapter.submit_merge(
                        worker,
                        run.request.source_ids or batch.fetch_source_ids,
                        fetch_refs,
                    )
                elif run.data_ref is not None:
                    assert isinstance(run.request, HandlerRequest)
                    run.request = self._handler_request_for_retry(run.request)
                    run.ref = self.ray_adapter.submit(
                        worker, run.request, run.data_ref
                    )
                else:
                    assert isinstance(run.request, HandlerRequest)
                    run.request = self._handler_request_for_retry(run.request)
                    run.ref = self.ray_adapter.submit(worker, run.request)
                run.status = RunStatus.SUBMITTED
                run.submitted_at = time.monotonic()
                return
            except Exception as exc:
                error = f"retry submission failed: {type(exc).__name__}: {exc}"
        run.status = RunStatus.FAILED
        run.error = error
        run.finished_at = time.monotonic()
        self._emit_event(
            EVENT_RUN_FAILED,
            {
                "run_id": run.run_id,
                "batch_id": run.batch_id,
                "handler": run.worker_name,
                "kind": run.kind,
                "attempt": run.attempt,
                "error": error,
                "permanent": True,
            },
        )

    @staticmethod
    def _all_terminal(runs: Sequence[TaskRun]) -> bool:
        return all(
            run.status in (RunStatus.SUCCEEDED, RunStatus.FAILED) for run in runs
        )

    def _begin_commit_batch(self, batch: BatchRun) -> _PendingBatchDurability:
        """Validate and mark COMMITTING under lock; return checkpoint writes."""

        if batch.group_key:
            assert batch.group_key is not None
            window = self.state.multisource_windows[batch.group_key]
            if (
                window.active_batch_id != batch.batch_id
                or window.committed_time != batch.start
            ):
                batch.status = BatchStatus.FAILED
                batch.finished_at = time.monotonic()
                raise StaleBatchError(
                    f"refusing stale commit for {batch.batch_id}: "
                    "window cursor or generation changed"
                )
            progress_key = self._mswin_checkpoint_key(batch.group_key)
            writes = self._checkpoint_writes_for_batch(
                batch, progress_key=progress_key, progress=batch.end
            )
            for state_key, end_offset in batch.partition_commits.items():
                writes[state_key] = end_offset
            batch.status = BatchStatus.COMMITTING
            return _PendingBatchDurability(
                batch_id=batch.batch_id,
                action="commit",
                writes=writes,
            )

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
        writes = self._checkpoint_writes_for_batch(
            batch, progress_key=source_state.key, progress=batch.end
        )
        batch.status = BatchStatus.COMMITTING
        return _PendingBatchDurability(
            batch_id=batch.batch_id,
            action="commit",
            writes=writes,
        )

    def _begin_skip_failed_batch(self, batch: BatchRun) -> _PendingBatchDurability:
        """Validate and mark SKIPPING under lock; prepare failure + checkpoint IO."""

        run_ids = [*batch.fetch_run_ids, *batch.run_ids]
        if batch.merge_run_id:
            run_ids.append(batch.merge_run_id)
        runs = [self.state.runs[run_id] for run_id in run_ids if run_id in self.state.runs]
        fetch_refs = tuple(
            run.ref
            for run in runs
            if run.kind == "fetch"
            and run.status is RunStatus.SUCCEEDED
            and run.ref is not None
        )
        failure_id = self._stable_id("failure", batch.batch_id, batch.start, batch.end)
        run_details = tuple(
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
        )
        worker_names = batch.worker_names or (
            (batch.worker_name,) if batch.worker_name else ()
        )

        if batch.group_key:
            window = self.state.multisource_windows[batch.group_key]
            if window.active_batch_id != batch.batch_id:
                batch.status = BatchStatus.FAILED
                batch.finished_at = time.monotonic()
                return _PendingBatchDurability(
                    batch_id=batch.batch_id,
                    action="skip_abort",
                    writes={},
                )
            if window.committed_time != batch.start:
                batch.status = BatchStatus.FAILED
                batch.finished_at = time.monotonic()
                window.active_batch_id = None
                for state_key in batch.source_state_keys:
                    state = self.state.sources[state_key]
                    if state.active_batch_id == batch.batch_id:
                        state.active_batch_id = None
                return _PendingBatchDurability(
                    batch_id=batch.batch_id,
                    action="skip_abort",
                    writes={},
                )
            writes: dict[str, Any] = {
                self._mswin_checkpoint_key(batch.group_key): batch.end
            }
            for state_key, end_offset in batch.partition_commits.items():
                writes[state_key] = end_offset
            batch.status = BatchStatus.SKIPPING
            return _PendingBatchDurability(
                batch_id=batch.batch_id,
                action="skip",
                writes=writes,
                failure_id=failure_id,
                source_state_key=batch.source_state_key,
                start=batch.start,
                end=batch.end,
                item_count=batch.item_count,
                worker_names=worker_names,
                run_details=run_details,
                fetch_refs=fetch_refs,
            )

        source_state = self.state.sources[batch.source_state_key]
        if source_state.active_batch_id != batch.batch_id:
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            return _PendingBatchDurability(
                batch_id=batch.batch_id,
                action="skip_abort",
                writes={},
            )
        if source_state.committed != batch.start:
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            source_state.active_batch_id = None
            return _PendingBatchDurability(
                batch_id=batch.batch_id,
                action="skip_abort",
                writes={},
            )
        batch.status = BatchStatus.SKIPPING
        return _PendingBatchDurability(
            batch_id=batch.batch_id,
            action="skip",
            writes={source_state.key: batch.end},
            failure_id=failure_id,
            source_state_key=batch.source_state_key,
            start=batch.start,
            end=batch.end,
            item_count=batch.item_count,
            worker_names=worker_names,
            run_details=run_details,
            fetch_refs=fetch_refs,
        )

    async def _persist_batch_durability(self, pending: _PendingBatchDurability) -> None:
        if pending.action == "skip_abort":
            return
        try:
            if pending.action == "skip":
                payload: Any = None
                payload_error: str | None = None
                fetch_payloads: list[Any] = []
                for ref in pending.fetch_refs:
                    try:
                        fetch_payloads.append(await self.ray_adapter.get(ref))
                    except Exception as exc:
                        payload_error = (
                            f"failed to materialize fetch: "
                            f"{type(exc).__name__}: {exc}"
                        )
                        fetch_payloads = []
                        break
                if payload_error is None and fetch_payloads:
                    payload = (
                        fetch_payloads[0]
                        if len(fetch_payloads) == 1
                        else fetch_payloads
                    )
                pending.materialized_payload = payload
                pending.payload_error = payload_error
                assert pending.failure_id is not None
                record = FailureRecord(
                    failure_id=pending.failure_id,
                    batch_id=pending.batch_id,
                    source_state_key=pending.source_state_key,
                    start=pending.start,
                    end=pending.end,
                    item_count=pending.item_count,
                    worker_names=pending.worker_names,
                    runs=pending.run_details,
                    payload=payload,
                    payload_error=payload_error,
                )
                try:
                    await self._io(self.failure_store.save_failure(record))
                except Exception as exc:
                    pending.failure_store_error = exc
            if pending.writes:
                await self._io(self.checkpoint_store.save_many(pending.writes))
        except Exception as exc:
            pending.persist_error = exc

    def _finalize_batch_durability(self, pending: _PendingBatchDurability) -> None:
        batch = self.state.batches.get(pending.batch_id)
        if batch is None:
            return
        if pending.action == "skip_abort":
            return
        if pending.persist_error is not None:
            self.state.loop_errors.append(
                f"ray_status durability: {type(pending.persist_error).__name__}: "
                f"{pending.persist_error}"
            )
            # Retry on the next status tick while the source stays blocked.
            batch.status = BatchStatus.RUNNING
            return
        if pending.action == "commit":
            self._apply_commit_batch(batch)
            return
        if pending.failure_store_error is not None:
            self.state.loop_errors.append(
                f"failure_store: {type(pending.failure_store_error).__name__}: "
                f"{pending.failure_store_error}"
            )
        if pending.failure_id is not None:
            batch.failure_id = pending.failure_id
        self._apply_skip_failed_batch(batch)

    def _apply_commit_batch(self, batch: BatchRun) -> None:
        if batch.group_key:
            self._apply_commit_multisource_batch(batch)
            return
        source_state = self.state.sources[batch.source_state_key]
        if (
            source_state.active_batch_id != batch.batch_id
            or source_state.committed != batch.start
            or batch.status is not BatchStatus.COMMITTING
        ):
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            raise StaleBatchError(
                f"refusing stale commit for {batch.batch_id}: source cursor or generation changed"
            )
        source_state.committed = batch.end
        source_state.backlog = max(0, source_state.backlog - batch.item_count)
        source_state.active_batch_id = None
        elapsed = max(time.monotonic() - batch.started_at, 1e-6)
        measured = batch.item_count / elapsed
        source_state.processing_rate = self._ewma(
            source_state.processing_rate, measured, self.config.ewma_alpha
        )
        batch.status = BatchStatus.SUCCEEDED
        batch.finished_at = time.monotonic()
        self._release_batch_refs(batch)
        self._emit_event(
            EVENT_BATCH_COMMITTED,
            self._batch_event_payload(batch, progress_key=source_state.key),
        )

    def _apply_commit_multisource_batch(self, batch: BatchRun) -> None:
        assert batch.group_key is not None
        window = self.state.multisource_windows[batch.group_key]
        progress_key = self._mswin_checkpoint_key(batch.group_key)
        if (
            window.active_batch_id != batch.batch_id
            or window.committed_time != batch.start
            or batch.status is not BatchStatus.COMMITTING
        ):
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            raise StaleBatchError(
                f"refusing stale commit for {batch.batch_id}: "
                "window cursor or generation changed"
            )
        window.committed_time = (
            batch.end if isinstance(batch.end, datetime) else window.committed_time
        )
        window.active_batch_id = None
        elapsed = max(time.monotonic() - batch.started_at, 1e-6)
        measured = batch.item_count / elapsed
        for state_key, end_offset in batch.partition_commits.items():
            state = self.state.sources[state_key]
            previous = int(state.committed)
            state.committed = end_offset
            state.backlog = max(0, int(state.observed) - end_offset)
            state.active_batch_id = None
            state.processing_rate = self._ewma(
                state.processing_rate,
                (end_offset - previous) / elapsed if elapsed else measured,
                self.config.ewma_alpha,
            )
        for state_key in batch.source_state_keys:
            if state_key in batch.partition_commits:
                continue
            state = self.state.sources[state_key]
            state.active_batch_id = None
        batch.status = BatchStatus.SUCCEEDED
        batch.finished_at = time.monotonic()
        self._release_batch_refs(batch)
        self._emit_event(
            EVENT_BATCH_COMMITTED,
            self._batch_event_payload(batch, progress_key=progress_key),
        )

    def _release_batch_refs(self, batch: BatchRun) -> None:
        run_ids = [*batch.fetch_run_ids, *batch.run_ids]
        if batch.merge_run_id:
            run_ids.append(batch.merge_run_id)
        for run_id in run_ids:
            run = self.state.runs[run_id]
            run.ref = None
            run.data_ref = None

    def _apply_skip_failed_batch(self, batch: BatchRun) -> None:
        """Advance past a poison range after durable skip IO succeeded."""

        if batch.group_key:
            self._apply_skip_failed_multisource_batch(batch)
            return
        source_state = self.state.sources[batch.source_state_key]
        if (
            source_state.active_batch_id != batch.batch_id
            or source_state.committed != batch.start
            or batch.status is not BatchStatus.SKIPPING
        ):
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            if source_state.active_batch_id == batch.batch_id:
                source_state.active_batch_id = None
            return
        source_state.committed = batch.end
        source_state.backlog = max(0, source_state.backlog - batch.item_count)
        source_state.active_batch_id = None
        batch.status = BatchStatus.FAILED
        batch.finished_at = time.monotonic()
        self._release_batch_refs(batch)
        self._emit_event(
            EVENT_BATCH_SKIPPED,
            self._batch_event_payload(batch, progress_key=source_state.key),
        )

    def _apply_skip_failed_multisource_batch(self, batch: BatchRun) -> None:
        assert batch.group_key is not None
        window = self.state.multisource_windows[batch.group_key]
        if (
            window.active_batch_id != batch.batch_id
            or window.committed_time != batch.start
            or batch.status is not BatchStatus.SKIPPING
        ):
            batch.status = BatchStatus.FAILED
            batch.finished_at = time.monotonic()
            if window.active_batch_id == batch.batch_id:
                window.active_batch_id = None
            for state_key in batch.source_state_keys:
                state = self.state.sources[state_key]
                if state.active_batch_id == batch.batch_id:
                    state.active_batch_id = None
            return
        window.committed_time = (
            batch.end if isinstance(batch.end, datetime) else window.committed_time
        )
        window.active_batch_id = None
        for state_key, end_offset in batch.partition_commits.items():
            state = self.state.sources[state_key]
            state.committed = end_offset
            state.backlog = max(0, int(state.observed) - end_offset)
            state.active_batch_id = None
        for state_key in batch.source_state_keys:
            state = self.state.sources[state_key]
            if state.active_batch_id == batch.batch_id:
                state.active_batch_id = None
        batch.status = BatchStatus.FAILED
        batch.finished_at = time.monotonic()
        self._release_batch_refs(batch)
        self._emit_event(
            EVENT_BATCH_SKIPPED,
            self._batch_event_payload(
                batch,
                progress_key=self._mswin_checkpoint_key(batch.group_key),
            ),
        )

    @staticmethod
    def _request_summary(request: DispatchRequest | HandlerRequest) -> dict[str, Any]:
        if isinstance(request, HandlerRequest):
            return {
                "dispatch_id": request.dispatch_id,
                "handler_id": request.handler_id,
                "output": None if request.output is None else dict(request.output),
            }
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
            "topic": request.topic,
            "table": request.table,
            "window_start": (
                None
                if request.window_start is None
                else request.window_start.isoformat()
            ),
            "window_end": (
                None if request.window_end is None else request.window_end.isoformat()
            ),
            "source_ids": list(request.source_ids),
        }

    async def start(self) -> None:
        """Preload resources, then start periodic dispatcher loops."""

        if self._tasks:
            return
        await self.resource_loader.preload()
        self._stopping.clear()
        loops: list[tuple[str, Callable[[], Any], float]] = [
            ("data_listener", self.data_listener, self.listener_interval),
            ("ray_trigger", self.ray_trigger, self.trigger_interval),
            ("ray_status", self.ray_status, self.status_interval),
        ]
        if self.event_log_interval > 0:
            loops.append(
                ("event_log", self.event_log_tick, self.event_log_interval)
            )
        if self.reload_interval > 0:
            loops.append(
                ("workers_reload", self._reload_workers_loop, self.reload_interval)
            )
        self._tasks = [
            asyncio.create_task(self._periodic(name, callback, interval), name=name)
            for name, callback, interval in loops
        ]

    async def event_log_tick(self) -> None:
        """Emit a periodic operational snapshot through the event log."""

        self._emit_event(EVENT_SNAPSHOT, await self.snapshot())

    async def reload_workers(
        self,
        *,
        candidate_plugin_roots: Sequence[Path] | None = None,
    ) -> bool:
        """Rescan worker roots and swap config when safe.

        ``candidate_plugin_roots`` are plugin directories only (not builtin). When
        omitted, uses ``list_active_roots()`` via ``_discover_roots()``.
        This method does not mutate PluginStore metadata except
        ``finalize_after_reload`` after a successful swap.

        Returns True when a new configuration was installed.
        """

        if self._workers_dir is None:
            return False
        from ray_dispatcher.discovery import discover_workers

        store = self._plugin_store
        if candidate_plugin_roots is None:
            roots = self._discover_roots()
        else:
            roots = [self._workers_dir, *[Path(p) for p in candidate_plugin_roots]]

        try:
            workers, merged_sources, merged_resources = discover_workers(
                roots,
                ray_module=getattr(self.ray_adapter, "ray", None),
                source_registry=self._injected_source_registry,
                resource_registry=self._injected_resource_registry,
            )
        except Exception as exc:
            self.state.loop_errors.append(
                f"workers_reload:discover:{type(exc).__name__}: {exc}"
            )
            return False

        worker_map = {worker.name: worker for worker in workers}
        fingerprint = self._fingerprint_config(
            worker_map,
            merged_sources,
            merged_resources,
            worker_roots=roots,
        )
        if fingerprint == self._config_fingerprint:
            return False

        async with self._lock:
            if self._has_inflight_work():
                return False

        new_loader = ResourceLoader(merged_resources)
        try:
            await new_loader.preload()
        except Exception as exc:
            self.state.loop_errors.append(
                f"workers_reload:preload:{type(exc).__name__}: {exc}"
            )
            return False

        async with self._lock:
            if self._has_inflight_work():
                return False
            if fingerprint == self._config_fingerprint:
                return False
            old_names = set(self.workers)
            self._install_worker_groups(tuple(workers))
            self.source_registry = merged_sources
            self.resource_registry = merged_resources
            self.resource_loader = new_loader
            if hasattr(self.ray_adapter, "resource_loader"):
                self.ray_adapter.resource_loader = new_loader
            self._sync_multisource_windows()
            self._config_fingerprint = fingerprint
            drop = getattr(self.ray_adapter, "drop_actor", None)
            if callable(drop):
                for name in old_names | set(self.workers):
                    drop(name)
            if store is not None:
                store.finalize_after_reload(set(self.workers))
            return True

    def has_inflight_work(self) -> bool:
        """Public view of whether reload must wait."""

        return self._has_inflight_work()

    async def _reload_workers_loop(self) -> bool:
        """Periodic reload: prefer desired candidate plugin roots when store set."""

        store = self._plugin_store
        if store is not None:
            return await self.reload_workers(
                candidate_plugin_roots=store.list_candidate_plugin_roots()
            )
        return await self.reload_workers()

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

    def _build_snapshot(self) -> dict[str, Any]:
        """Build a JSON-friendly operational snapshot (caller must hold lock)."""

        return {
            "sources": {
                key: {
                    "handler": state.worker_name,
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
                    **(
                        {
                            "handler_id": run.request.handler_id,
                            "output": (
                                None
                                if run.request.output is None
                                else dict(run.request.output)
                            ),
                        }
                        if isinstance(run.request, HandlerRequest)
                        else {
                            "output": None,
                            "topic": run.request.topic,
                            "table": run.request.table,
                        }
                    ),
                }
                for key, run in self.state.runs.items()
            },
            "loop_errors": list(self.state.loop_errors),
            "failures": {
                "store": type(self.failure_store).__name__,
            },
        }

    async def snapshot(self) -> dict[str, Any]:
        """Return a lock-consistent JSON-friendly operational snapshot."""

        async with self._lock:
            return self._build_snapshot()
