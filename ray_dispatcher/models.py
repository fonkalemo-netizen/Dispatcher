"""Domain models, enums and request/state dataclasses."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import total_ordering
from typing import Any, Callable, Literal, Mapping, Union

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
class OutputSpec:
    """Serializable output metadata used for routing and sink backpressure.

    ``connection_id`` references external connection configuration; credentials
    and live client objects must not be placed in this object or DispatchRequest.
    """

    connection_id: str
    target: str
    output_format: str
    max_parallelism: int = 8

    def __post_init__(self) -> None:
        if not self.connection_id or not self.target or not self.output_format:
            raise ValueError("output connection_id, target and output_format are required")
        if self.max_parallelism < 1:
            raise ValueError("output max_parallelism must be positive")


@dataclass(frozen=True)
class DispatchRequest:
    """The single argument passed to a worker by default.

    Production Postgres observers return disjoint ordered composite-cursor
    ranges. Custom observers without that capability safely use one task for
    the full window rather than process-dependent Python hash partitioning.
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
    output_connection_id: str | None = None
    output_target: str | None = None
    output_format: str | None = None
    source_connection_id: str | None = None
    topic: str | None = None
    table: str | None = None

    @property
    def n(self) -> int:
        return self.task_count

    @property
    def handler_id(self) -> str:
        return self.worker_name



ArgsBuilder = Callable[[DispatchRequest], tuple[tuple[Any, ...], dict[str, Any]]]


def offset_kwargs_builder(request: DispatchRequest) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Compatibility builder for workers accepting startoffset/endoffset/n."""

    return (), {
        "startoffset": request.start_offset,
        "endoffset": request.end_offset,
        "n": request.task_count,
        "task_index": request.task_index,
        "partition": request.partition,
        "topic": request.topic,
        "dispatch_id": request.dispatch_id,
    }


@dataclass(frozen=True)
class HandlerSpec:
    """One independently scheduled processing function.

    A Python module may export many handlers. By default each owns independent
    source state. Handlers opting into one shared_source_group share source
    fetches/checkpoints while retaining independent retries and output limits.
    """

    name: str
    worker: Any
    sources: tuple[SourceSpec, ...]
    mode: ExecutionMode = ExecutionMode.TASK
    remote_method: str | None = None
    max_parallelism: int = 4
    batch_size: int = 10_000
    cpus_per_task: float = 1.0
    max_retries: int = 2
    cache_history: bool = False
    priority: int = 0
    args_builder: ArgsBuilder | None = None
    output: OutputSpec | None = None
    shared_source_group: str | None = None
    data_fetcher: Any | None = None
    fetcher_id: str | None = None
    fetch_cpus: float = 0.25
    fetch_max_retries: int = 2

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("worker name cannot be empty")
        if self.max_parallelism < 1 or self.batch_size < 1:
            raise ValueError("max_parallelism and batch_size must be positive")
        if self.cpus_per_task <= 0:
            raise ValueError("cpus_per_task must be positive")
        if self.mode is ExecutionMode.ACTOR and not self.remote_method:
            raise ValueError("actor workers require remote_method")
        if self.cache_history and self.mode is not ExecutionMode.ACTOR:
            raise ValueError("cache_history requires actor mode")
        if (self.shared_source_group is None) != (self.data_fetcher is None):
            raise ValueError(
                "shared_source_group and data_fetcher must be configured together"
            )
        if self.shared_source_group is not None:
            if len(self.sources) != 1:
                raise ValueError("shared-source handlers must declare exactly one source")
            if self.args_builder is not None:
                raise ValueError("shared-source handlers do not support args_builder")
            if not self.fetcher_id:
                raise ValueError("shared-source handlers require fetcher_id")
        if self.fetch_cpus <= 0 or self.fetch_max_retries < 0:
            raise ValueError("fetch_cpus must be positive and fetch_max_retries non-negative")

    @property
    def handler_id(self) -> str:
        return self.name


# Backwards-compatible public name for applications using the original API.
WorkerSpec = HandlerSpec


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    value: Any = None
    error: str | None = None


@dataclass(frozen=True)
class FailureRunDetail:
    """One task's contribution to a permanently failed batch."""

    run_id: str
    kind: Literal["fetch", "handler"]
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
    shared_source_group: str | None
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
    shared_source_group: str | None = None


@dataclass
class TaskRun:
    run_id: str
    batch_id: str
    worker_name: str
    request: DispatchRequest
    ref: Any
    status: RunStatus = RunStatus.SUBMITTED
    attempt: int = 1
    submitted_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None
    result: Any = None
    error: str | None = None
    kind: Literal["fetch", "handler"] = "handler"
    data_ref: Any = None


@dataclass
class BatchRun:
    batch_id: str
    source_state_key: str
    worker_name: str
    start: int | PostgresCursor
    end: int | PostgresCursor
    item_count: int
    run_ids: list[str]
    status: BatchStatus = BatchStatus.RUNNING
    started_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None
    shared_source_group: str | None = None
    worker_names: tuple[str, ...] = ()
    fetch_run_ids: list[str] = field(default_factory=list)
    reserved_handler_count: int = 0


@dataclass
class DispatcherState:
    sources: dict[str, SourceState] = field(default_factory=dict)
    batches: dict[str, BatchRun] = field(default_factory=dict)
    runs: dict[str, TaskRun] = field(default_factory=dict)
    loop_errors: list[str] = field(default_factory=list)

    @property
    def refs(self) -> dict[str, Any]:
        return {run_id: run.ref for run_id, run in self.runs.items() if run.ref is not None}
