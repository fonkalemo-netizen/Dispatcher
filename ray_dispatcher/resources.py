"""Resource loading for declarative resource specs.

Resources are preloaded into an in-process cache after the final registry is
merged (startup / hot-reload). Per-resource load failures are isolated: other
resources stay available, and only handlers that depend on a failed resource
are blocked at submit time. Submit paths should hit the cache only after
``ensure_available``.
"""

from __future__ import annotations

import inspect
import json
import re
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from ray_dispatcher.registries import ResourceRegistry, ResourceSpec, resolve_resource
from ray_dispatcher.rules import build_eq_rule_labeler

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
        self._failed: dict[str, str] = {}
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
        self._failed.clear()
        self._preloaded = False
        self._version += 1

    def clear_cache(self) -> None:
        self._cache.clear()
        self._loaded_at.clear()
        self._failed.clear()
        self._preloaded = False
        self._version += 1

    @property
    def version(self) -> int:
        """Monotonic cache version; changes after preload/refresh/clear."""

        return self._version

    @property
    def failed_resources(self) -> Mapping[str, str]:
        """resource_id -> last load error for resources not currently cached."""

        return dict(self._failed)

    async def preload(self) -> dict[str, str]:
        """Load every registry entry best-effort.

        Successful entries populate the cache. Per-resource load failures are
        recorded in ``failed_resources`` and omitted from the cache; they do
        not raise. Returns ``{resource_id: error}`` for failures.

        Structural registry errors (e.g. key/resource_id mismatch) still raise.
        """

        if not self.registry:
            self._cache = {}
            self._loaded_at = {}
            self._failed = {}
            self._preloaded = True
            self._version += 1
            return {}

        built: dict[str, Any] = {}
        loaded_at: dict[str, float] = {}
        failed: dict[str, str] = {}
        for resource_id, spec in self.registry.items():
            if spec.resource_id != resource_id:
                raise ValueError(
                    f"resource registry key {resource_id!r} must equal "
                    f"resource_id {spec.resource_id!r}"
                )
            try:
                built[resource_id] = await self._load_and_format_async(spec)
                loaded_at[resource_id] = time.monotonic()
            except Exception as exc:
                failed[resource_id] = f"{type(exc).__name__}: {exc}"

        self._cache = built
        self._loaded_at = loaded_at
        self._failed = failed
        self._preloaded = True
        self._version += 1
        return dict(failed)

    async def ensure_available(self, resource_ids: Sequence[str]) -> None:
        """Ensure ``resource_ids`` are cached, retrying missing/failed loads.

        Raises ``RuntimeError`` listing any resources that remain unavailable.
        Handlers that do not depend on those ids are unaffected.
        """

        if not resource_ids:
            return
        errors: list[str] = []
        changed = False
        for resource_id in resource_ids:
            if resource_id in self._cache:
                continue
            try:
                spec = resolve_resource(self.registry, resource_id)
                value = await self._load_and_format_async(spec)
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                self._failed[resource_id] = message
                errors.append(f"{resource_id}: {message}")
                continue
            self._cache[resource_id] = value
            self._loaded_at[resource_id] = time.monotonic()
            self._failed.pop(resource_id, None)
            changed = True
        if changed:
            self._version += 1
        if errors:
            raise RuntimeError(
                "handler resources unavailable: " + "; ".join(errors)
            )

    def get(self, resource_id: str) -> Any:
        if resource_id in self._cache:
            return self._cache[resource_id]
        if resource_id in self._failed:
            raise RuntimeError(
                f"resource {resource_id!r} unavailable: {self._failed[resource_id]}"
            )
        spec = resolve_resource(self.registry, resource_id)
        if spec.kind == "postgres":
            raise RuntimeError(
                f"postgres resource {resource_id!r} must be preloaded before use"
            )
        value = self._load_and_format_sync(spec)
        self._cache[resource_id] = value
        self._loaded_at[resource_id] = time.monotonic()
        self._failed.pop(resource_id, None)
        self._version += 1
        return value

    def load_many(self, resource_ids: tuple[str, ...]) -> dict[str, Any]:
        return {resource_id: self.get(resource_id) for resource_id in resource_ids}

    async def refresh_expired(self, resource_ids: tuple[str, ...]) -> set[str]:
        """Refresh TTL resources that expired before handler submission.

        Returns the resource IDs that received a new value. If a refresh fails
        and a previous cached value exists, the cache is kept and the error is
        recorded in ``failed_resources`` without raising. If refresh fails with
        no cached value, the exception is raised (submit path isolates per handler).
        """

        if not resource_ids or not self.registry:
            return set()
        refreshed: set[str] = set()
        now = time.monotonic()
        for resource_id in resource_ids:
            spec = resolve_resource(self.registry, resource_id)
            if not self._should_refresh(spec, now):
                continue
            previous = self._cache.get(resource_id)
            try:
                value = await self._load_and_format_async(spec)
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                self._failed[resource_id] = message
                if previous is not None:
                    continue
                raise RuntimeError(
                    f"resource {resource_id!r} unavailable: {message}"
                ) from exc
            self._cache[resource_id] = value
            self._loaded_at[resource_id] = time.monotonic()
            self._failed.pop(resource_id, None)
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
        if spec.kind in ("static", "file", "eq_rule_labeler"):
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
        if spec.kind == "eq_rule_labeler":
            payload = spec.rules
            if payload is None:
                path = Path(str(spec.path)).expanduser()
                payload = json.loads(path.read_text(encoding="utf-8"))
            return build_eq_rule_labeler(
                payload,
                record_mode=spec.record_mode,
                multi_match=spec.multi_match,
            )
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
