"""Domain models, enums and request/state dataclasses."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import total_ordering
from typing import Any, Literal, Mapping, Optional, Sequence, Tuple, Union

BatchSizeConfig = Union[int, Tuple[Optional[int], Optional[int]]]
BatchWindow = Tuple[int, Optional[int]]  # (min_items, max_items); max None = unlimited


def normalize_batch_size(value: BatchSizeConfig) -> BatchWindow:
    """Normalize ``batch_size`` to ``(min_items, max_items)``.

    - ``n`` → ``(n, None)``  (``[n,]``: trigger at n, take all)
    - ``(None, n)`` → ``(1, n)``  (``[,n]``)
    - ``(n, None)`` → ``(n, None)``
    - ``(n, m)`` → ``(n, m)`` with ``n <= m``
    """

    if isinstance(value, bool):
        raise TypeError("batch_size must be an int or a (min, max) tuple")
    if isinstance(value, int):
        if value < 0:
            raise ValueError("batch_size int must be >= 0")
        return (value, None)
    if not isinstance(value, tuple) or len(value) != 2:
        raise TypeError("batch_size must be an int or a (min, max) tuple")
    raw_min, raw_max = value
    if raw_min is not None and (not isinstance(raw_min, int) or isinstance(raw_min, bool)):
        raise TypeError("batch_size min must be an int or None")
    if raw_max is not None and (not isinstance(raw_max, int) or isinstance(raw_max, bool)):
        raise TypeError("batch_size max must be an int or None")
    if raw_min is not None and raw_min < 0:
        raise ValueError("batch_size min must be >= 0")
    if raw_max is not None and raw_max < 1:
        raise ValueError("batch_size max must be >= 1")
    min_items = 1 if raw_min is None else raw_min
    max_items = raw_max
    if max_items is not None and min_items > max_items:
        raise ValueError("batch_size min must be <= max")
    return (min_items, max_items)


def merge_batch_windows(windows: Sequence[BatchWindow]) -> BatchWindow:
    """Strictest shared window: max of mins, min of finite maxes."""

    if not windows:
        raise ValueError("windows cannot be empty")
    min_items = max(window[0] for window in windows)
    finite_maxes = [window[1] for window in windows if window[1] is not None]
    max_items = min(finite_maxes) if finite_maxes else None
    if max_items is not None and min_items > max_items:
        # Conflicting member configs: still schedule only when both satisfied;
        # take at most max_items once triggered (min gate may never pass).
        pass
    return (min_items, max_items)


def handler_shard_size(window: BatchWindow) -> int:
    """In-memory fanout shard size from a handler ``batch_size`` window.

    Uses max when set, otherwise min; always at least 1.
    """

    min_items, max_items = window
    size = max_items if max_items is not None else min_items
    return max(1, int(size))


class SourceKind(str, Enum):
    KAFKA = "kafka"
    POSTGRES = "postgres"


class ExecutionMode(str, Enum):
    TASK = "task"
    ACTOR = "actor"


class RunStatus(str, Enum):
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class BatchStatus(str, Enum):
    RUNNING = "running"
    COMMITTING = "committing"
    SKIPPING = "skipping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@total_ordering
@dataclass(frozen=True)
class PostgresCursor:
    """A deterministic cursor.

    Timestamps should be timezone-aware. Naive values (common when reading
    PostgreSQL ``timestamp without time zone`` via asyncpg) are treated as UTC.
    """

    timestamp: datetime
    primary_key: Any

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            object.__setattr__(
                self, "timestamp", self.timestamp.replace(tzinfo=timezone.utc)
            )

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, PostgresCursor):
            return NotImplemented
        if self.timestamp != other.timestamp:
            return self.timestamp < other.timestamp
        try:
            return self.primary_key < other.primary_key
        except TypeError as exc:
            raise TypeError(
                "Postgres cursor primary keys must preserve one comparable database type"
            ) from exc


@dataclass(frozen=True)
class KafkaSource:
    source_id: str
    brokers: tuple[str, ...]
    topic: str
    initial_offset: Literal["latest", "earliest"] = "latest"
    retention_policy: Literal["error", "reset_to_earliest"] = "error"
    kind: SourceKind = field(default=SourceKind.KAFKA, init=False)


@dataclass(frozen=True)
class PostgresSource:
    """Postgres incremental source.

    ``mode="cursor"`` (default): composite ``(timestamp, primary_key)`` progress.
    ``mode="event_time"``: closed watermark ``now()-lag``, time-only windows,
    no ``ORDER BY`` (for stores that cannot sort).
    """

    source_id: str
    dsn: str
    table: str
    timestamp_column: str
    primary_key_column: str = ""
    initial_cursor: PostgresCursor | None = None
    mode: Literal["cursor", "event_time"] = "cursor"
    watermark_lag_seconds: float = 60.0
    max_window_seconds: float = 300.0
    min_window_seconds: float = 60.0
    max_rows: int = 100_000
    initial_time: datetime | None = None
    kind: SourceKind = field(default=SourceKind.POSTGRES, init=False)

    def __post_init__(self) -> None:
        if self.mode not in ("cursor", "event_time"):
            raise ValueError(f"unsupported PostgresSource.mode: {self.mode!r}")
        if self.mode == "cursor":
            if not self.primary_key_column:
                raise ValueError(
                    "PostgresSource.primary_key_column is required when mode='cursor'"
                )
            return
        if self.watermark_lag_seconds <= 0:
            raise ValueError("watermark_lag_seconds must be positive")
        if self.max_window_seconds <= 0 or self.min_window_seconds <= 0:
            raise ValueError("max_window_seconds and min_window_seconds must be positive")
        if self.min_window_seconds > self.max_window_seconds:
            raise ValueError("min_window_seconds must be <= max_window_seconds")
        if self.max_rows < 1:
            raise ValueError("max_rows must be >= 1")
        if self.initial_time is not None and self.initial_time.tzinfo is None:
            object.__setattr__(
                self,
                "initial_time",
                self.initial_time.replace(tzinfo=timezone.utc),
            )


SourceSpec = Union[KafkaSource, PostgresSource]


@dataclass(frozen=True)
class DispatchRequest:
    """Scheduling request used for fetch/merge and internal batch state.

    Production Postgres observers return disjoint ordered composite-cursor
    ranges. Custom observers without that capability safely use one task for
    the full window rather than process-dependent Python hash partitioning.

    Business handlers receive :class:`HandlerRequest` instead of this type.
    """

    dispatch_id: str
    worker_name: str
    source_id: str
    source_kind: SourceKind
    task_index: int
    task_count: int
    partition: int | None = None
    start_offset: int | None = None
    end_offset: int | None = None
    start_cursor: PostgresCursor | None = None
    end_cursor: PostgresCursor | None = None
    topic: str | None = None
    table: str | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None
    source_ids: tuple[str, ...] = ()

    @property
    def n(self) -> int:
        return self.task_count

    @property
    def handler_id(self) -> str:
        return self.worker_name


@dataclass(frozen=True)
class HandlerRequest:
    """Slim argument passed to business handlers.

    Contains only identity and write-side passthrough. Scheduling bounds stay on
    :class:`DispatchRequest` for fetch/internal use; payload arrives as
    ``records``.

    ``checkpoint_state`` is set only for Actor handlers on the first submit after
    the Actor is (re)created, so they can restore the last successful cache.
    Automatic retries clear this field so restore is not applied twice.
    """

    dispatch_id: str
    handler_id: str
    output: Mapping[str, Any] | None = None
    checkpoint_state: Any | None = None


@dataclass(frozen=True)
class HandlerSpec:
    """One independently scheduled processing function.

    Single-source handlers share fetches by ``source_id``. Multi-source Kafka
    handlers (``len(sources) >= 2``) share event-time windows for the same
    ordered source tuple.
    """

    name: str
    worker: Any
    sources: tuple[SourceSpec, ...]
    mode: ExecutionMode = ExecutionMode.TASK
    remote_method: str | None = None
    batch_size: BatchSizeConfig = 10_000
    cpus_per_task: float = 1.0
    max_retries: int = 2
    max_parallelism: int = 1
    priority: int = 0
    output: Mapping[str, Any] | None = None
    resource_ids: tuple[str, ...] = ()
    external_kafka_json: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("worker name cannot be empty")
        # Validate batch_size eagerly so bad configs fail at construction.
        normalize_batch_size(self.batch_size)
        if self.cpus_per_task <= 0:
            raise ValueError("cpus_per_task must be positive")
        if self.max_parallelism < 1:
            raise ValueError("max_parallelism must be >= 1")
        if self.mode is ExecutionMode.ACTOR and not self.remote_method:
            raise ValueError("actor workers require remote_method")
        if self.mode is ExecutionMode.ACTOR and self.max_parallelism != 1:
            raise ValueError("actor workers require max_parallelism=1")
        if len(self.sources) < 1:
            raise ValueError("handlers must declare at least one source")
        if len(self.sources) > 1 and any(
            not isinstance(source, KafkaSource) for source in self.sources
        ):
            raise ValueError("multi-source handlers must declare only Kafka sources")
        source_ids = [source.source_id for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("handler source_ids must be unique")
        if len(self.resource_ids) != len(set(self.resource_ids)):
            raise ValueError("resource_ids must be unique")

    @property
    def batch_window(self) -> BatchWindow:
        """Normalized ``(min_items, max_items)`` scheduling window."""

        return normalize_batch_size(self.batch_size)

    @property
    def handler_id(self) -> str:
        return self.name

    @property
    def source(self) -> SourceSpec:
        return self.sources[0]

    @property
    def source_id(self) -> str:
        return self.source.source_id

    @property
    def is_multisource(self) -> bool:
        return len(self.sources) > 1

    @property
    def group_key(self) -> str:
        if not self.is_multisource:
            return self.source_id
        return "ms:" + "+".join(source.source_id for source in self.sources)


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    value: Any = None
    error: str | None = None


@dataclass(frozen=True)
class FailureRunDetail:
    """One task's contribution to a permanently failed batch."""

    run_id: str
    kind: Literal["fetch", "handler", "merge"]
    worker_name: str
    status: str
    attempt: int
    error: str | None
    request: Mapping[str, Any]


@dataclass(frozen=True)
class FailureRecord:
    """Durable record of a skipped poison batch and any recoverable payload."""

    failure_id: str
    batch_id: str
    source_state_key: str
    start: Any
    end: Any
    item_count: int
    worker_names: tuple[str, ...]
    runs: tuple[FailureRunDetail, ...]
    payload: Any = None
    payload_error: str | None = None
    created_at: float = field(default_factory=time.time)


@dataclass
class SourceState:
    key: str
    worker_name: str
    source_id: str
    kind: SourceKind
    shard: str
    committed: int | PostgresCursor | datetime
    observed: int | PostgresCursor | datetime
    backlog: int = 0
    arrival_rate: float = 0.0
    processing_rate: float = 0.0
    last_observed_at: float = field(default_factory=time.monotonic)
    last_scheduled_at: float = field(default_factory=time.monotonic)
    active_batch_id: str | None = None
    active_batch_ids: set[str] = field(default_factory=set)
    reserved_until: int | PostgresCursor | datetime | None = None
    retention_gap: str | None = None
    over_capacity: bool = False


@dataclass
class TaskRun:
    run_id: str
    batch_id: str
    worker_name: str
    request: DispatchRequest | HandlerRequest
    ref: Any
    status: RunStatus = RunStatus.SUBMITTED
    attempt: int = 1
    submitted_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None
    result: Any = None
    error: str | None = None
    kind: Literal["fetch", "handler", "merge"] = "handler"
    data_ref: Any = None
    checkpoint_state: Any | None = None


@dataclass
class BatchRun:
    batch_id: str
    source_state_key: str
    worker_name: str
    start: int | PostgresCursor | datetime
    end: int | PostgresCursor | datetime
    item_count: int
    run_ids: list[str]
    status: BatchStatus = BatchStatus.RUNNING
    started_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None
    worker_names: tuple[str, ...] = ()
    fetch_run_ids: list[str] = field(default_factory=list)
    reserved_handler_count: int = 0
    window_start: datetime | None = None
    window_end: datetime | None = None
    source_state_keys: tuple[str, ...] = ()
    partition_commits: dict[str, int] = field(default_factory=dict)
    merge_run_id: str | None = None
    group_key: str | None = None
    fetch_source_ids: tuple[str, ...] = ()
    failure_id: str | None = None
    handler_slice_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class MultiSourceWindowState:
    """Aligned event-time window progress for a multi-Kafka handler group."""

    group_key: str
    source_ids: tuple[str, ...]
    committed_time: datetime | None = None
    observed_time: datetime | None = None
    active_batch_id: str | None = None
    last_scheduled_at: float = field(default_factory=time.monotonic)


@dataclass
class DispatcherState:
    sources: dict[str, SourceState] = field(default_factory=dict)
    batches: dict[str, BatchRun] = field(default_factory=dict)
    runs: dict[str, TaskRun] = field(default_factory=dict)
    loop_errors: list[str] = field(default_factory=list)
    multisource_windows: dict[str, MultiSourceWindowState] = field(default_factory=dict)
