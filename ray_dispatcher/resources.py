"""Resource loading for declarative resource specs.

Resources are preloaded into an in-process cache after the final registry is
merged (startup / hot-reload). Submit paths should hit the cache only.
"""

from __future__ import annotations

import inspect
import json
import re
import time
from pathlib import Path
from typing import Any

from ray_dispatcher.registries import ResourceRegistry, ResourceSpec, resolve_resource

_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ResourceLoader:
    """Load and cache named resources for handler injection."""

    def __init__(
        self,
        registry: ResourceRegistry | None = None,
        *,
        formatters: dict[str, Any] | None = None,
    ) -> None:
        self.registry = registry
        self.formatters = formatters or {}
        self._cache: dict[str, Any] = {}
        self._loaded_at: dict[str, float] = {}
        self._preloaded = False
        self._version = 0

    def replace_registry(
        self,
        registry: ResourceRegistry | None,
        *,
        formatters: dict[str, Any] | None = None,
    ) -> None:
        """Swap the registry; clears cache until the next successful preload."""

        self.registry = registry
        self.formatters = formatters or {}
        self._cache.clear()
        self._loaded_at.clear()
        self._preloaded = False
        self._version += 1

    def clear_cache(self) -> None:
        self._cache.clear()
        self._loaded_at.clear()
        self._preloaded = False
        self._version += 1

    @property
    def version(self) -> int:
        """Monotonic cache version; changes after preload/refresh/clear."""

        return self._version

    async def preload(self) -> None:
        """Load every registry entry into cache atomically.

        On failure the previous cache is restored and the error is re-raised.
        """

        if not self.registry:
            self._cache = {}
            self._preloaded = True
            return

        previous = self._cache
        previous_loaded_at = self._loaded_at
        previous_flag = self._preloaded
        built: dict[str, Any] = {}
        loaded_at: dict[str, float] = {}
        try:
            for resource_id, spec in self.registry.items():
                if spec.resource_id != resource_id:
                    raise ValueError(
                        f"resource registry key {resource_id!r} must equal "
                        f"resource_id {spec.resource_id!r}"
                    )
                built[resource_id] = await self._load_and_format_async(spec)
                loaded_at[resource_id] = time.monotonic()
        except Exception:
            self._cache = previous
            self._loaded_at = previous_loaded_at
            self._preloaded = previous_flag
            raise
        self._cache = built
        self._loaded_at = loaded_at
        self._preloaded = True
        self._version += 1

    def get(self, resource_id: str) -> Any:
        if resource_id in self._cache:
            return self._cache[resource_id]
        spec = resolve_resource(self.registry, resource_id)
        if spec.kind == "postgres":
            raise RuntimeError(
                f"postgres resource {resource_id!r} must be preloaded before use"
            )
        value = self._load_and_format_sync(spec)
        self._cache[resource_id] = value
        self._loaded_at[resource_id] = time.monotonic()
        self._version += 1
        return value

    def load_many(self, resource_ids: tuple[str, ...]) -> dict[str, Any]:
        return {resource_id: self.get(resource_id) for resource_id in resource_ids}

    async def refresh_expired(self, resource_ids: tuple[str, ...]) -> set[str]:
        """Refresh TTL resources that expired before handler submission.

        Returns the resource IDs that received a new value. If a refresh fails,
        the previous cached value remains installed and the exception is raised.
        """

        if not resource_ids or not self.registry:
            return set()
        refreshed: set[str] = set()
        now = time.monotonic()
        for resource_id in resource_ids:
            spec = resolve_resource(self.registry, resource_id)
            if not self._should_refresh(spec, now):
                continue
            value = await self._load_and_format_async(spec)
            self._cache[resource_id] = value
            self._loaded_at[resource_id] = time.monotonic()
            refreshed.add(resource_id)
        if refreshed:
            self._version += 1
        return refreshed

    def _should_refresh(self, spec: ResourceSpec, now: float) -> bool:
        if spec.refresh_policy != "ttl":
            return False
        if spec.resource_id not in self._cache:
            return True
        loaded_at = self._loaded_at.get(spec.resource_id)
        if loaded_at is None:
            return True
        assert spec.ttl_seconds is not None
        return now - loaded_at >= spec.ttl_seconds

    @classmethod
    async def _load_async(cls, spec: ResourceSpec) -> Any:
        if spec.kind in ("static", "file"):
            return cls._load_sync(spec)
        if spec.kind == "postgres":
            return await cls._load_postgres(spec)
        raise ValueError(f"unsupported resource kind: {spec.kind!r}")

    async def _load_and_format_async(self, spec: ResourceSpec) -> Any:
        value = await self._load_async(spec)
        formatter = self._formatter_for(spec)
        if formatter is None:
            return value
        formatted = formatter(value)
        if inspect.isawaitable(formatted):
            return await formatted
        return formatted

    def _load_and_format_sync(self, spec: ResourceSpec) -> Any:
        value = self._load_sync(spec)
        formatter = self._formatter_for(spec)
        if formatter is None:
            return value
        formatted = formatter(value)
        if inspect.isawaitable(formatted):
            raise RuntimeError(
                f"resource formatter {spec.formatter!r} for {spec.resource_id!r} "
                "is async but this resource is being loaded synchronously"
            )
        return formatted

    def _formatter_for(self, spec: ResourceSpec) -> Any | None:
        if not spec.formatter:
            return None
        try:
            formatter = self.formatters[spec.resource_id]
        except KeyError as exc:
            raise KeyError(
                f"resource {spec.resource_id!r} references formatter "
                f"{spec.formatter!r}, but no formatter was registered"
            ) from exc
        if not callable(formatter):
            raise TypeError(
                f"resource formatter {spec.formatter!r} for "
                f"{spec.resource_id!r} is not callable"
            )
        return formatter

    @staticmethod
    def _load_sync(spec: ResourceSpec) -> Any:
        if spec.kind == "static":
            return spec.data
        if spec.kind == "file":
            path = Path(str(spec.path)).expanduser()
            return json.loads(path.read_text(encoding="utf-8"))
        raise ValueError(f"unsupported sync resource kind: {spec.kind!r}")

    @classmethod
    async def _load_postgres(cls, spec: ResourceSpec) -> dict[Any, dict[str, Any]]:
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "Postgres resources require: pip install asyncpg"
            ) from exc
        assert spec.dsn is not None
        assert spec.key_column is not None
        if spec.query:
            sql = spec.query
        else:
            assert spec.table is not None
            if not _TABLE_NAME_RE.fullmatch(spec.table):
                raise ValueError(f"invalid postgres resource table name: {spec.table!r}")
            sql = f'SELECT * FROM "{spec.table}"'

        connection = await asyncpg.connect(dsn=spec.dsn)
        try:
            rows = await connection.fetch(sql)
        finally:
            await connection.close()

        key_column = spec.key_column
        result: dict[Any, dict[str, Any]] = {}
        for row in rows:
            mapping = dict(row)
            if key_column not in mapping:
                raise KeyError(
                    f"postgres resource {spec.resource_id!r} row missing "
                    f"key_column {key_column!r}"
                )
            key = cls._jsonish(mapping[key_column])
            result[key] = {column: cls._jsonish(value) for column, value in mapping.items()}
        return result

    @staticmethod
    def _jsonish(value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, (bytes, bytearray)):
            return bytes(value).hex()
        return str(value)


__all__ = ["ResourceLoader"]
