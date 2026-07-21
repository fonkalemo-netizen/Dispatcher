"""Backlog-aware Ray data dispatcher."""

from __future__ import annotations

from ray_dispatcher.backend import NativeRayBackend
from ray_dispatcher.checkpoint import MemoryCheckpointStore, SQLiteCheckpointStore
from ray_dispatcher.discovery import (
    WorkerDiscoveryError,
    discover_handlers,
    discover_workers,
)
from ray_dispatcher.dispatcher import KafkaRetentionGap, RayDispatcher, StaleBatchError
from ray_dispatcher.failures import MemoryFailureStore, SQLiteFailureStore
from ray_dispatcher.models import (
    BatchStatus,
    DispatchRequest,
    ExecutionMode,
    ExecutionResult,
    FailureRecord,
    FailureRunDetail,
    HandlerSpec,
    KafkaSource,
    OutputSpec,
    PostgresCursor,
    PostgresSource,
    RunStatus,
    SourceKind,
    SourceState,
    WorkerSpec,
    offset_kwargs_builder,
)
from ray_dispatcher.policy import SchedulingPolicy
from ray_dispatcher.sources import (
    EmptyPostgresSource,
    SourceDependencyError,
    SourceObserver,
)

__all__ = [
    "BatchStatus",
    "DispatchRequest",
    "EmptyPostgresSource",
    "ExecutionMode",
    "ExecutionResult",
    "FailureRecord",
    "FailureRunDetail",
    "HandlerSpec",
    "KafkaRetentionGap",
    "KafkaSource",
    "MemoryCheckpointStore",
    "MemoryFailureStore",
    "NativeRayBackend",
    "OutputSpec",
    "PostgresCursor",
    "PostgresSource",
    "RayDispatcher",
    "RunStatus",
    "SQLiteCheckpointStore",
    "SQLiteFailureStore",
    "SchedulingPolicy",
    "SourceDependencyError",
    "SourceKind",
    "SourceObserver",
    "SourceState",
    "StaleBatchError",
    "WorkerDiscoveryError",
    "WorkerSpec",
    "discover_handlers",
    "discover_workers",
    "offset_kwargs_builder",
]
