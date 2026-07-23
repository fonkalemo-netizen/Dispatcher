"""External registries for named sources and resources."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping

from ray_dispatcher.models import (
    KafkaSource,
    PostgresCursor,
    PostgresSource,
    SourceSpec,
)


SourceRegistry = Mapping[str, SourceSpec]

ResourceKind = Literal["static", "file", "postgres"]


@dataclass(frozen=True)
class ResourceSpec:
    """Named declarative resource; loading is handled by ResourceLoader."""

    resource_id: str
    kind: ResourceKind
    data: Any | None = None
    path: str | None = None
    dsn: str | None = None
    key_column: str | None = None
    query: str | None = None
    table: str | None = None

    def __post_init__(self) -> None:
        if not self.resource_id:
            raise ValueError("resource_id cannot be empty")
        if self.kind not in ("static", "file", "postgres"):
            raise ValueError("resource kind must be 'static', 'file', or 'postgres'")
        if self.kind == "static" and self.data is None:
            raise ValueError("static resource requires data")
        if self.kind == "file" and not self.path:
            raise ValueError("file resource requires path")
        if self.kind == "postgres":
            if not self.dsn:
                raise ValueError("postgres resource requires dsn")
            if not self.key_column:
                raise ValueError("postgres resource requires key_column")
            has_query = bool(self.query)
            has_table = bool(self.table)
            if has_query == has_table:
                raise ValueError(
                    "postgres resource requires exactly one of query or table"
                )

    def canonical_dict(self) -> dict[str, Any]:
        """Stable mapping used for merge equality checks."""

        payload: dict[str, Any] = {
            "resource_id": self.resource_id,
            "kind": self.kind,
        }
        if self.kind == "static":
            payload["data"] = self.data
        elif self.kind == "file":
            payload["path"] = self.path
        else:
            payload["dsn"] = self.dsn
            payload["key_column"] = self.key_column
            if self.query:
                payload["query"] = self.query
            else:
                payload["table"] = self.table
        return payload


ResourceRegistry = Mapping[str, ResourceSpec]


def source_from_mapping(raw: Mapping[str, Any]) -> SourceSpec:
    """Parse a concrete source mapping used inside a SourceRegistry."""

    kind = str(raw["kind"]).lower()
    source_id = str(raw["source_id"])
    if kind == "kafka":
        brokers = raw["brokers"]
        if isinstance(brokers, str):
            brokers = tuple(part.strip() for part in brokers.split(",") if part.strip())
        return KafkaSource(
            source_id=source_id,
            brokers=tuple(brokers),
            topic=str(raw["topic"]),
            initial_offset=str(raw.get("initial_offset", "latest")),
            retention_policy=str(raw.get("retention_policy", "error")),
        )
    if kind == "postgres":
        initial = raw.get("initial_cursor")
        initial_cursor: PostgresCursor | None
        if initial is None or isinstance(initial, PostgresCursor):
            initial_cursor = initial
        elif isinstance(initial, Mapping):
            timestamp = initial["timestamp"]
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            initial_cursor = PostgresCursor(timestamp, initial["primary_key"])
        else:
            raise TypeError("initial_cursor must be PostgresCursor or mapping")
        return PostgresSource(
            source_id=source_id,
            dsn=str(raw["dsn"]),
            table=str(raw["table"]),
            timestamp_column=str(raw["timestamp_column"]),
            primary_key_column=str(raw["primary_key_column"]),
            initial_cursor=initial_cursor,
        )
    raise ValueError(f"unsupported source kind: {kind!r}")


def resource_from_mapping(raw: Mapping[str, Any]) -> ResourceSpec:
    """Parse a concrete resource mapping used inside a ResourceRegistry."""

    kind = str(raw["kind"]).lower()
    resource_id = str(raw["resource_id"])
    if kind == "static":
        return ResourceSpec(
            resource_id=resource_id,
            kind="static",
            data=raw["data"],
        )
    if kind == "file":
        return ResourceSpec(
            resource_id=resource_id,
            kind="file",
            path=str(raw["path"]),
        )
    if kind == "postgres":
        return ResourceSpec(
            resource_id=resource_id,
            kind="postgres",
            dsn=str(raw["dsn"]),
            key_column=str(raw["key_column"]),
            query=(str(raw["query"]) if raw.get("query") is not None else None),
            table=(str(raw["table"]) if raw.get("table") is not None else None),
        )
    raise ValueError(f"unsupported resource kind: {kind!r}")


def resolve_source(registry: SourceRegistry | None, name: str) -> SourceSpec:
    if registry is None:
        raise KeyError(
            f"source {name!r} was referenced but no source_registry was provided"
        )
    try:
        source = registry[name]
    except KeyError as exc:
        raise KeyError(f"unknown source {name!r}") from exc
    if source.source_id != name:
        raise ValueError(
            f"source registry key {name!r} must equal source.source_id "
            f"{source.source_id!r}"
        )
    return source


def resolve_resource(registry: ResourceRegistry | None, name: str) -> ResourceSpec:
    if registry is None:
        raise KeyError(
            f"resource {name!r} was referenced but no resource_registry was provided"
        )
    try:
        resource = registry[name]
    except KeyError as exc:
        raise KeyError(f"unknown resource {name!r}") from exc
    if resource.resource_id != name:
        raise ValueError(
            f"resource registry key {name!r} must equal resource_id "
            f"{resource.resource_id!r}"
        )
    return resource


def build_source_registry(
    items: Mapping[str, SourceSpec | Mapping[str, Any]],
) -> dict[str, SourceSpec]:
    """Build a source registry from SourceSpec objects or plain mappings."""

    resolved: dict[str, SourceSpec] = {}
    for name, raw in items.items():
        if isinstance(raw, (KafkaSource, PostgresSource)):
            source = raw
        else:
            payload = dict(raw)
            payload.setdefault("source_id", name)
            source = source_from_mapping(payload)
        if source.source_id != name:
            raise ValueError(
                f"source registry key {name!r} must equal source_id {source.source_id!r}"
            )
        resolved[name] = source
    return resolved


def build_resource_registry(
    items: Mapping[str, ResourceSpec | Mapping[str, Any]],
) -> dict[str, ResourceSpec]:
    """Build a resource registry from ResourceSpec objects or plain mappings."""

    resolved: dict[str, ResourceSpec] = {}
    for name, raw in items.items():
        if isinstance(raw, ResourceSpec):
            resource = raw
        else:
            payload = dict(raw)
            payload.setdefault("resource_id", name)
            resource = resource_from_mapping(payload)
        if resource.resource_id != name:
            raise ValueError(
                f"resource registry key {name!r} must equal resource_id "
                f"{resource.resource_id!r}"
            )
        resolved[name] = resource
    return resolved


def source_canonical_dict(source: SourceSpec) -> dict[str, Any]:
    """Stable mapping for merge equality on sources."""

    if isinstance(source, KafkaSource):
        return {
            "kind": "kafka",
            "source_id": source.source_id,
            "brokers": list(source.brokers),
            "topic": source.topic,
            "initial_offset": source.initial_offset,
            "retention_policy": source.retention_policy,
        }
    if isinstance(source, PostgresSource):
        initial: Any = None
        if source.initial_cursor is not None:
            initial = {
                "timestamp": source.initial_cursor.timestamp.isoformat(),
                "primary_key": source.initial_cursor.primary_key,
            }
        return {
            "kind": "postgres",
            "source_id": source.source_id,
            "dsn": source.dsn,
            "table": source.table,
            "timestamp_column": source.timestamp_column,
            "primary_key_column": source.primary_key_column,
            "initial_cursor": initial,
        }
    raise TypeError(f"unsupported source type: {type(source).__name__}")


def canonical_json(value: Any) -> str:
    """Stable JSON used to compare declarative configs."""

    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


__all__ = [
    "ResourceKind",
    "ResourceRegistry",
    "ResourceSpec",
    "SourceRegistry",
    "build_resource_registry",
    "build_source_registry",
    "canonical_json",
    "resolve_resource",
    "resolve_source",
    "resource_from_mapping",
    "source_canonical_dict",
    "source_from_mapping",
]
