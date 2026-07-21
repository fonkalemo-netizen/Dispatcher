"""Injectable protocols for checkpoints, failures and Ray execution."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence

from ray_dispatcher.models import (
    DispatchRequest,
    FailureRecord,
    PostgresCursor,
    WorkerSpec,
)


class CheckpointStore(Protocol):
    async def load(self, key: str) -> int | PostgresCursor | None: ...

    async def save(self, key: str, value: int | PostgresCursor) -> None: ...


class FailureStore(Protocol):
    async def save_failure(self, record: FailureRecord) -> None: ...

    async def get_failure(self, failure_id: str) -> FailureRecord | None: ...

    async def list_failures(self, *, limit: int = 100) -> Sequence[FailureRecord]: ...


class RayBackend(Protocol):
    def submit(
        self,
        worker: WorkerSpec,
        request: DispatchRequest,
        data_ref: Any | None = None,
    ) -> Any: ...

    def submit_fetch(self, worker: WorkerSpec, request: DispatchRequest) -> Any: ...

    def poll(self, refs: Mapping[str, Any]) -> Any: ...

    def available_cpus(self) -> float | None: ...

    async def get(self, ref: Any) -> Any: ...
