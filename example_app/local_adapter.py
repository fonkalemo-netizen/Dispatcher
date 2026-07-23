"""Threaded fallback used only when the demo cannot start local Ray."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Mapping

from ray_dispatcher import (
    DispatchRequest,
    ExecutionResult,
    HandlerRequest,
    HandlerSpec,
    SourceSpec,
)
from ray_dispatcher.resources import ResourceLoader


class LocalThreadAdapter:
    """无 Ray 时的本地线程适配，方法语义与 ``RayAdapter`` 一致。"""

    def __init__(
        self,
        max_workers: int = 4,
        *,
        payload_reader: Any | None = None,
        resource_loader: ResourceLoader | None = None,
    ) -> None:
        self.max_workers = max_workers
        self.payload_reader = payload_reader
        self.resource_loader = resource_loader or ResourceLoader()
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="ray-dispatcher-demo",
        )

    def submit(
        self,
        handler: HandlerSpec,
        request: HandlerRequest,
        data_ref: Future[Any] | None = None,
    ) -> Future[Any]:
        """提交业务 Handler：``request`` → ``records`` → ``resources?``。

        本地线程无 Object Store：resources 仍从 ``ResourceLoader`` 缓存读取
        （同进程共享，不会像 Ray 那样按 task 重复序列化整表）。
        """

        if handler.mode.value != "task":
            raise RuntimeError("the demo fallback supports task handlers only")
        if data_ref is None:
            raise ValueError("handlers require a fetched data_ref")

        def invoke() -> Any:
            records = data_ref.result()
            if handler.resource_ids:
                resources = self.resource_loader.load_many(handler.resource_ids)
                return handler.worker(request, records, resources)
            return handler.worker(request, records)

        return self.executor.submit(invoke)

    def submit_fetch(
        self,
        handler: HandlerSpec,
        request: DispatchRequest,
        source: SourceSpec,
        *,
        fetch_cpus: float = 0.25,
    ) -> Future[Any]:
        """提交读数任务：按区间从源拉取 records。"""

        if self.payload_reader is None:
            raise RuntimeError("payload_reader is not configured on LocalThreadAdapter")
        del fetch_cpus  # thread adapter does not enforce Ray CPU quotas
        return self.executor.submit(self.payload_reader.fetch, request, source)

    def poll(
        self, refs: Mapping[str, Future[Any]]
    ) -> Mapping[str, ExecutionResult]:
        """非阻塞检查 Future；只返回已完成项。"""

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
        """本地线程池规模，用作可用 CPU 近似。"""

        return float(self.max_workers)

    async def get(self, ref: Future[Any]) -> Any:
        """物化 Future 结果。"""

        return ref.result()

    def close(self) -> None:
        self.executor.shutdown(wait=True)
