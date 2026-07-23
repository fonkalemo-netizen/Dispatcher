"""Domain models, enums and request/state dataclasses."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import total_ordering
from typing import Any, Literal, Mapping, Union


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
    """A deterministic cursor; timestamps must be timezone-aware."""

    timestamp: datetime
    primary_key: Any

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("PostgresCursor.timestamp must be timezone-aware")

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
    connection_id: str | None = None
    kind: SourceKind = field(default=SourceKind.KAFKA, init=False)


@dataclass(frozen=True)
class PostgresSource:
    source_id: str
    dsn: str
    table: str
    timestamp_column: str
    primary_key_column: str
    initial_cursor: PostgresCursor | None = None
    connection_id: str | None = None
    kind: SourceKind = field(default=SourceKind.POSTGRES, init=False)


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
    source_connection_id: str | None = None
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
    batch_size: int = 10_000
    cpus_per_task: float = 1.0
    max_retries: int = 2
    priority: int = 0
    output: Mapping[str, Any] | None = None
    resource_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("worker name cannot be empty")
        if self.batch_size < 1:
            raise ValueError("batch_size must be positive")
        if self.cpus_per_task <= 0:
            raise ValueError("cpus_per_task must be positive")
        if self.mode is ExecutionMode.ACTOR and not self.remote_method:
            raise ValueError("actor workers require remote_method")
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
    committed: int | PostgresCursor
    observed: int | PostgresCursor
    backlog: int = 0
    arrival_rate: float = 0.0
    processing_rate: float = 0.0
    last_observed_at: float = field(default_factory=time.monotonic)
    last_scheduled_at: float = field(default_factory=time.monotonic)
    active_batch_id: str | None = None
    retention_gap: str | None = None


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
