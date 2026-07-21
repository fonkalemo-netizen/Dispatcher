"""Native Ray execution backend."""

from __future__ import annotations

import asyncio
from typing import Any, Mapping

from ray_dispatcher.models import DispatchRequest, ExecutionMode, ExecutionResult, WorkerSpec

class NativeRayBackend:
    """Ray adapter supporting remote functions and one cached actor per handler."""

    def __init__(self, ray_module: Any | None = None) -> None:
        if ray_module is None:
            try:
                import ray as ray_module  # type: ignore[import-not-found]
            except ImportError as exc:
                raise RuntimeError("Ray is not installed; pass a custom RayBackend") from exc
        self.ray = ray_module
        self._actors: dict[str, Any] = {}

    def _actor(self, spec: WorkerSpec) -> Any:
        # One long-lived actor per handler. Concurrency is limited by
        # max_parallelism on in-flight method calls; Ray queues extras.
        actor = self._actors.get(spec.name)
        if actor is None:
            actor = spec.worker.options(num_cpus=spec.cpus_per_task).remote()
            self._actors[spec.name] = actor
        return actor

    def submit(
        self,
        spec: WorkerSpec,
        request: DispatchRequest,
        data_ref: Any | None = None,
    ) -> Any:
        args, kwargs = spec.args_builder(request) if spec.args_builder else ((request,), {})
        if data_ref is not None:
            # Keep the ObjectRef as a top-level Ray argument so Ray resolves the
            # dependency and all handlers reuse the same object-store value.
            args = (*args, data_ref)
        if spec.mode is ExecutionMode.TASK:
            target = getattr(spec.worker, spec.remote_method) if spec.remote_method else spec.worker
            return target.options(num_cpus=spec.cpus_per_task).remote(*args, **kwargs)

        actor = self._actor(spec)
        method = getattr(actor, spec.remote_method or "process")
        return method.remote(*args, **kwargs)

    def submit_fetch(self, spec: WorkerSpec, request: DispatchRequest) -> Any:
        if spec.data_fetcher is None:
            raise ValueError(f"handler {spec.name!r} has no data_fetcher")
        return spec.data_fetcher.options(
            num_cpus=spec.fetch_cpus, max_retries=0
        ).remote(request)

    async def poll(self, refs: Mapping[str, Any]) -> Mapping[str, ExecutionResult]:
        if not refs:
            return {}
        reverse = {ref: run_id for run_id, ref in refs.items()}
        # asyncio.wait() may return wrapper Tasks instead of the original Ray
        # ObjectRefs. ray.wait() guarantees that its ready list contains the
        # original ObjectRefs, so the reverse mapping remains valid.
        ready, _ = await asyncio.to_thread(
            self.ray.wait,
            list(reverse),
            num_returns=len(reverse),
            timeout=0,
        )
        results: dict[str, ExecutionResult] = {}
        for ref in ready:
            run_id = reverse[ref]
            try:
                results[run_id] = ExecutionResult(True, value=await self.get(ref))
            except Exception as exc:  # Ray wraps application errors.
                results[run_id] = ExecutionResult(False, error=f"{type(exc).__name__}: {exc}")
        return results

    async def get(self, ref: Any) -> Any:
        """Materialize an ObjectRef for failure capture or result inspection."""

        if hasattr(ref, "__await__"):
            return await ref
        return await asyncio.to_thread(self.ray.get, ref)

    def available_cpus(self) -> float | None:
        return float(self.ray.available_resources().get("CPU", 0.0))
