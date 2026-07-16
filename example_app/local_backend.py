"""Threaded fallback used only when the demo cannot start local Ray."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Mapping

from ray_dispatcher import DispatchRequest, ExecutionResult, HandlerSpec


class LocalThreadBackend:
    def __init__(self, max_workers: int = 4) -> None:
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="ray-dispatcher-demo",
        )

    def submit(
        self,
        handler: HandlerSpec,
        request: DispatchRequest,
        data_ref: Future[Any] | None = None,
    ) -> Future[Any]:
        if handler.mode.value != "task":
            raise RuntimeError("the demo fallback supports task handlers only")
        if data_ref is not None:
            def invoke_shared() -> Any:
                return handler.worker(request, data_ref.result())

            return self.executor.submit(invoke_shared)
        args, kwargs = handler.args_builder(request) if handler.args_builder else ((request,), {})
        return self.executor.submit(handler.worker, *args, **kwargs)

    def submit_fetch(
        self, handler: HandlerSpec, request: DispatchRequest
    ) -> Future[Any]:
        if handler.data_fetcher is None:
            raise ValueError(f"handler {handler.name!r} has no data_fetcher")
        return self.executor.submit(handler.data_fetcher, request)

    def poll(
        self, refs: Mapping[str, Future[Any]]
    ) -> Mapping[str, ExecutionResult]:
        outcomes: dict[str, ExecutionResult] = {}
        for run_id, future in refs.items():
            if not future.done():
                continue
            try:
                outcomes[run_id] = ExecutionResult(True, value=future.result())
            except Exception as exc:
                outcomes[run_id] = ExecutionResult(
                    False,
                    error=f"{type(exc).__name__}: {exc}",
                )
        return outcomes

    def available_cpus(self) -> float:
        return float(self.max_workers)

    def close(self) -> None:
        self.executor.shutdown(wait=True)
