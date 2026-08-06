"""Native Ray execution adapter."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping, Protocol, Sequence

from ray_dispatcher.models import (
    DispatchRequest,
    ExecutionMode,
    ExecutionResult,
    HandlerRequest,
    HandlerSpec,
    SourceSpec,
)
from ray_dispatcher.resources import ResourceLoader
from ray_dispatcher.readers import merge_fetch_results


def take_slice(records: Sequence[Any], start: int, end: int) -> list[Any]:
    """Return ``records[start:end]`` for Ray / local slice fanout."""

    return list(records[start:end])


def decode_kafka_json_records(records: Any) -> Any:
    """Decode uploaded-plugin Kafka payloads into dict records."""

    if isinstance(records, Mapping):
        return {
            str(source_id): _decode_kafka_json_list(payload)
            for source_id, payload in records.items()
        }
    return _decode_kafka_json_list(records)


def _decode_kafka_json_list(records: Any) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise TypeError(
            f"external Kafka records must be a list, got {type(records).__name__}"
        )
    return [_decode_kafka_json_item(item) for item in records]


def _decode_kafka_json_item(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return item
    if isinstance(item, (bytes, bytearray, memoryview)):
        text = bytes(item).decode("utf-8")
    elif isinstance(item, str):
        text = item
    else:
        raise TypeError(
            "external Kafka record must be bytes, str, or dict, "
            f"got {type(item).__name__}"
        )
    value = json.loads(text)
    if not isinstance(value, dict):
        raise TypeError(
            "external Kafka record JSON must decode to an object/dict, "
            f"got {type(value).__name__}"
        )
    return value


class RayAdapter(Protocol):
    """Dispatcher 与执行层之间的适配接口（鸭子类型）。"""

    def submit(
        self,
        worker: HandlerSpec,
        request: HandlerRequest,
        data_ref: Any | None = None,
    ) -> Any:
        """提交业务 Handler 任务。

        Task：``request`` → ``records``（``data_ref``）→ ``resources?``（共享 ObjectRef）。
        Actor：``request`` → ``records``；``resources`` 仅在 Actor 构造时注入一次。
        """
        ...

    def submit_fetch(
        self,
        worker: HandlerSpec,
        request: DispatchRequest,
        source: SourceSpec,
        *,
        fetch_cpus: float = 0.25,
    ) -> Any:
        """提交读数任务：按 ``request`` 区间从源拉取 records。

        同名源共享时只 fetch 一次，再把 ObjectRef 扇出给多个 Handler。
        ``fetch_cpus`` 由 Dispatcher 的 DispatcherConfig 提供。
        """
        ...

    def submit_merge(
        self,
        worker: HandlerSpec,
        source_ids: tuple[str, ...],
        fetch_refs: Sequence[Any],
    ) -> Any:
        """把多路 fetch ObjectRef 合并为 ``dict[source_id, records]``。"""
        ...

    def submit_slice(self, data_ref: Any, start: int, end: int) -> Any:
        """Return a ref to ``records[start:end]`` without driver-side ``get``."""
        ...

    def submit_decode_kafka_json(self, data_ref: Any) -> Any:
        """Return a ref with uploaded-plugin Kafka payloads decoded to dicts."""
        ...

    def poll(self, refs: Mapping[str, Any]) -> Any:
        """非阻塞检查一批引用。

        已完成的返回 ``run_id -> ExecutionResult``；未完成的不出现在结果里。
        Dispatcher 的 ``ray_status`` 循环用它收结果。
        """
        ...

    def available_cpus(self) -> float | None:
        """查询当前可用 CPU，供调度卡控；``None`` 表示不做 CPU 限制。"""
        ...

    async def get(self, ref: Any) -> Any:
        """物化某个引用的值（如永久失败时取出 fetch payload 写入 FailureStore）。"""
        ...


class NativeRayAdapter:
    """Ray 适配：支持 remote function，以及每个 Handler 复用一个 Actor。"""

    def __init__(
        self,
        ray_module: Any | None = None,
        *,
        payload_fetch_remote: Any | None = None,
        resource_loader: ResourceLoader | None = None,
    ) -> None:
        if ray_module is None:
            try:
                import ray as ray_module  # type: ignore[import-not-found]
            except ImportError as exc:
                raise RuntimeError(
                    "Ray is not installed; pass a custom RayAdapter"
                ) from exc
        self.ray = ray_module
        self._actors: dict[str, Any] = {}
        self._actors_need_restore: set[str] = set()
        self._payload_fetch_remote = payload_fetch_remote
        self._merge_remote = None
        self._slice_remote = None
        self._decode_kafka_json_remote = None
        self._resource_loader = resource_loader or ResourceLoader()
        # Task-mode shared puts: (loader version, resource_ids) -> ObjectRef.
        self._resource_put_refs: dict[tuple[int, tuple[str, ...]], Any] = {}

    @property
    def resource_loader(self) -> ResourceLoader:
        return self._resource_loader

    @resource_loader.setter
    def resource_loader(self, value: ResourceLoader) -> None:
        self._resource_loader = value
        self._resource_put_refs.clear()

    def set_payload_fetch_remote(self, remote_fn: Any) -> None:
        """绑定框架 PayloadReader 的 Ray remote，供 ``submit_fetch`` 使用。"""

        self._payload_fetch_remote = remote_fn

    def set_merge_remote(self, remote_fn: Any) -> None:
        """绑定多源 fetch 合并 remote。"""

        self._merge_remote = remote_fn

    def claim_actor_restore(self, handler_name: str) -> bool:
        """Return True once after an Actor is (re)created, then clear the flag."""

        if handler_name not in self._actors_need_restore:
            return False
        self._actors_need_restore.discard(handler_name)
        return True

    def prepare_actor_restore(self, spec: HandlerSpec) -> bool:
        """Create the Actor if needed; return True when restore state should be injected."""

        if spec.mode is not ExecutionMode.ACTOR:
            return False
        self._actor(spec)
        return self.claim_actor_restore(spec.name)

    def drop_actor(self, handler_name: str) -> None:
        """Forget a cached Actor handle (tests / forced rebuild)."""

        self._actors.pop(handler_name, None)
        self._actors_need_restore.discard(handler_name)

    def _resources_payload(self, spec: HandlerSpec) -> dict[str, Any] | None:
        if not spec.resource_ids:
            return None
        return self.resource_loader.load_many(spec.resource_ids)

    def _resources_put_ref(self, spec: HandlerSpec) -> Any | None:
        """Return a cached ``ray.put`` ObjectRef for this handler's resource set."""

        if not spec.resource_ids:
            return None
        key = (self.resource_loader.version, spec.resource_ids)
        cached = self._resource_put_refs.get(key)
        if cached is not None:
            return cached
        payload = self._resources_payload(spec)
        assert payload is not None
        key = (self.resource_loader.version, spec.resource_ids)
        cached = self._resource_put_refs.get(key)
        if cached is not None:
            return cached
        put = getattr(self.ray, "put", None)
        if put is None:
            raise RuntimeError(
                "ray.put is required to share Task-mode resources; "
                "pass a Ray module that implements put()"
            )
        ref = put(payload)
        self._resource_put_refs[key] = ref
        return ref

    def _actor(self, spec: HandlerSpec) -> Any:
        # One long-lived actor per handler. Resources are injected once at
        # construction; process() only receives request / records.
        actor = self._actors.get(spec.name)
        if actor is None:
            resources = self._resources_payload(spec)
            options = spec.worker.options(num_cpus=spec.cpus_per_task)
            if resources is not None:
                actor = options.remote(resources)
            else:
                actor = options.remote()
            self._actors[spec.name] = actor
            self._actors_need_restore.add(spec.name)
        return actor

    def submit(
        self,
        spec: HandlerSpec,
        request: HandlerRequest,
        data_ref: Any | None = None,
    ) -> Any:
        """提交业务 Handler：Task 共享 resources ObjectRef；Actor 构造期注入。"""

        args: tuple[Any, ...] = (request,)
        if data_ref is not None:
            # Keep the ObjectRef as a top-level Ray argument so Ray resolves the
            # dependency and all handlers reuse the same object-store value.
            args = (*args, data_ref)
        if spec.mode is ExecutionMode.TASK:
            resources_ref = self._resources_put_ref(spec)
            if resources_ref is not None:
                args = (*args, resources_ref)
            target = (
                getattr(spec.worker, spec.remote_method)
                if spec.remote_method
                else spec.worker
            )
            return target.options(num_cpus=spec.cpus_per_task).remote(*args)

        actor = self._actor(spec)
        method = getattr(actor, spec.remote_method or "process")
        return method.remote(*args)

    def submit_fetch(
        self,
        spec: HandlerSpec,
        request: DispatchRequest,
        source: SourceSpec,
        *,
        fetch_cpus: float = 0.25,
    ) -> Any:
        """提交读数任务：按区间从源拉取 records，返回 ObjectRef。"""

        if self._payload_fetch_remote is None:
            raise RuntimeError("payload fetch remote is not configured")
        return self._payload_fetch_remote.options(
            num_cpus=fetch_cpus, max_retries=0
        ).remote(request, source)

    def submit_merge(
        self,
        spec: HandlerSpec,
        source_ids: tuple[str, ...],
        fetch_refs: Sequence[Any],
    ) -> Any:
        """提交多源 merge：返回 ``dict[source_id, records]`` ObjectRef。"""

        merge_remote = self._merge_remote
        if merge_remote is None and self.ray is not None:
            merge_remote = self.ray.remote(max_retries=0)(merge_fetch_results)
            self._merge_remote = merge_remote
        if merge_remote is None:
            raise RuntimeError("merge remote is not configured")
        return merge_remote.options(num_cpus=0.1, max_retries=0).remote(
            source_ids, *fetch_refs
        )

    def submit_slice(self, data_ref: Any, start: int, end: int) -> Any:
        """Slice a fetch ObjectRef into ``records[start:end]`` on a Ray worker."""

        slice_remote = self._slice_remote
        if slice_remote is None and self.ray is not None:
            slice_remote = self.ray.remote(max_retries=0)(take_slice)
            self._slice_remote = slice_remote
        if slice_remote is None:
            raise RuntimeError("slice remote is not configured")
        return slice_remote.options(num_cpus=0.05, max_retries=0).remote(
            data_ref, start, end
        )

    def submit_decode_kafka_json(self, data_ref: Any) -> Any:
        """Decode Kafka bytes/strings for uploaded plugin handlers."""

        remote = self._decode_kafka_json_remote
        if remote is None and self.ray is not None:
            remote = self.ray.remote(max_retries=0)(decode_kafka_json_records)
            self._decode_kafka_json_remote = remote
        if remote is None:
            raise RuntimeError("Kafka JSON decode remote is not configured")
        return remote.options(num_cpus=0.05, max_retries=0).remote(data_ref)

    async def poll(self, refs: Mapping[str, Any]) -> Mapping[str, ExecutionResult]:
        """非阻塞轮询 ObjectRef；只返回已完成项的 ``ExecutionResult``。"""

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
        """物化 ObjectRef（失败落盘或结果检查时使用）。"""

        if hasattr(ref, "__await__"):
            return await ref
        return await asyncio.to_thread(self.ray.get, ref)

    def available_cpus(self) -> float | None:
        """返回集群当前可用 CPU 数。"""

        return float(self.ray.available_resources().get("CPU", 0.0))


__all__ = [
    "NativeRayAdapter",
    "RayAdapter",
    "decode_kafka_json_records",
    "take_slice",
]
