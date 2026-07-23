"""Resource loading for declarative resource specs.

Resources are preloaded into an in-process cache after the final registry is
merged (startup / hot-reload). Submit paths should hit the cache only.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ray_dispatcher.registries import ResourceRegistry, ResourceSpec, resolve_resource

_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ResourceLoader:
    """Load and cache named resources for handler injection."""

    def __init__(self, registry: ResourceRegistry | None = None) -> None:
        self.registry = registry
        self._cache: dict[str, Any] = {}
        self._preloaded = False

    def replace_registry(self, registry: ResourceRegistry | None) -> None:
        """Swap the registry; clears cache until the next successful preload."""

        self.registry = registry
        self._cache.clear()
        self._preloaded = False

    def clear_cache(self) -> None:
        self._cache.clear()
        self._preloaded = False

    async def preload(self) -> None:
        """Load every registry entry into cache atomically.

        On failure the previous cache is restored and the error is re-raised.
        """

        if not self.registry:
            self._cache = {}
            self._preloaded = True
            return

        previous = self._cache
        previous_flag = self._preloaded
        built: dict[str, Any] = {}
        try:
            for resource_id, spec in self.registry.items():
                if spec.resource_id != resource_id:
                    raise ValueError(
                        f"resource registry key {resource_id!r} must equal "
                        f"resource_id {spec.resource_id!r}"
                    )
                built[resource_id] = await self._load_async(spec)
        except Exception:
            self._cache = previous
            self._preloaded = previous_flag
            raise
        self._cache = built
        self._preloaded = True

    def get(self, resource_id: str) -> Any:
        if resource_id in self._cache:
            return self._cache[resource_id]
        spec = resolve_resource(self.registry, resource_id)
        if spec.kind == "postgres":
            raise RuntimeError(
                f"postgres resource {resource_id!r} must be preloaded before use"
            )
        value = self._load_sync(spec)
        self._cache[resource_id] = value
        return value

    def load_many(self, resource_ids: tuple[str, ...]) -> dict[str, Any]:
        return {resource_id: self.get(resource_id) for resource_id in resource_ids}

    @classmethod
    async def _load_async(cls, spec: ResourceSpec) -> Any:
        if spec.kind in ("static", "file"):
            return cls._load_sync(spec)
        if spec.kind == "postgres":
            return await cls._load_postgres(spec)
        raise ValueError(f"unsupported resource kind: {spec.kind!r}")

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
