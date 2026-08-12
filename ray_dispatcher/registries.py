"""External registries for named sources and resources."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping

from ray_dispatcher.models import (
    KafkaSource,
    PostgresCursor,
    PostgresSource,
    SourceSpec,
)


SourceRegistry = Mapping[str, SourceSpec]

ResourceKind = Literal["static", "file", "postgres", "eq_rule_labeler"]
RefreshPolicy = Literal["manual", "ttl"]
RuleRecordMode = Literal["attr", "dict"]


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
    rules: Any | None = None
    record_mode: RuleRecordMode = "attr"
    multi_match: bool = True
    refresh_policy: RefreshPolicy = "manual"
    ttl_seconds: float | None = None
    formatter: str | None = None

    def __post_init__(self) -> None:
        if not self.resource_id:
            raise ValueError("resource_id cannot be empty")
        if self.kind not in ("static", "file", "postgres", "eq_rule_labeler"):
            raise ValueError(
                "resource kind must be 'static', 'file', 'postgres', "
                "or 'eq_rule_labeler'"
            )
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
        if self.kind == "eq_rule_labeler":
            if self.rules is None and not self.path:
                raise ValueError("eq_rule_labeler resource requires rules or path")
            if self.record_mode not in ("attr", "dict"):
                raise ValueError("record_mode must be 'attr' or 'dict'")
        if self.refresh_policy not in ("manual", "ttl"):
            raise ValueError("refresh_policy must be 'manual' or 'ttl'")
        if self.refresh_policy == "ttl":
            if self.ttl_seconds is None or self.ttl_seconds <= 0:
                raise ValueError("ttl refresh_policy requires ttl_seconds > 0")
        if self.formatter is not None and not self.formatter:
            raise ValueError("formatter cannot be empty")

    def canonical_dict(self) -> dict[str, Any]:
        """Stable mapping used for merge equality checks."""

        payload: dict[str, Any] = {
            "resource_id": self.resource_id,
            "kind": self.kind,
            "refresh_policy": self.refresh_policy,
        }
        if self.ttl_seconds is not None:
            payload["ttl_seconds"] = self.ttl_seconds
        if self.formatter is not None:
            payload["formatter"] = self.formatter
        if self.kind == "static":
            payload["data"] = self.data
        elif self.kind == "file":
            payload["path"] = self.path
        elif self.kind == "postgres":
            payload["dsn"] = self.dsn
            payload["key_column"] = self.key_column
            if self.query:
                payload["query"] = self.query
            else:
                payload["table"] = self.table
        else:
            if self.rules is not None:
                payload["rules"] = self.rules
            if self.path is not None:
                payload["path"] = self.path
            payload["record_mode"] = self.record_mode
            payload["multi_match"] = self.multi_match
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
        mode = str(raw.get("mode", "cursor")).lower()
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
        initial_time_raw = raw.get("initial_time")
        initial_time: datetime | None
        if initial_time_raw is None or isinstance(initial_time_raw, datetime):
            initial_time = initial_time_raw
        elif isinstance(initial_time_raw, str):
            initial_time = datetime.fromisoformat(
                initial_time_raw.replace("Z", "+00:00")
            )
            if initial_time.tzinfo is None:
                initial_time = initial_time.replace(tzinfo=timezone.utc)
        else:
            raise TypeError("initial_time must be datetime or ISO string")
        primary_key_column = str(raw.get("primary_key_column", ""))
        return PostgresSource(
            source_id=source_id,
            dsn=str(raw["dsn"]),
            table=str(raw["table"]),
            timestamp_column=str(raw["timestamp_column"]),
            primary_key_column=primary_key_column,
            initial_cursor=initial_cursor,
            mode=mode,  # type: ignore[arg-type]
            watermark_lag_seconds=float(raw.get("watermark_lag_seconds", 60)),
            max_window_seconds=float(raw.get("max_window_seconds", 300)),
            min_window_seconds=float(raw.get("min_window_seconds", 60)),
            max_rows=int(raw.get("max_rows", 100_000)),
            initial_time=initial_time,
        )
    raise ValueError(f"unsupported source kind: {kind!r}")


def resource_from_mapping(raw: Mapping[str, Any]) -> ResourceSpec:
    """Parse a concrete resource mapping used inside a ResourceRegistry."""

    kind = str(raw["kind"]).lower()
    resource_id = str(raw["resource_id"])
    refresh_policy, ttl_seconds = _refresh_policy_from_mapping(raw)
    if kind == "static":
        return ResourceSpec(
            resource_id=resource_id,
            kind="static",
            data=raw["data"],
            refresh_policy=refresh_policy,
            ttl_seconds=ttl_seconds,
            formatter=(str(raw["formatter"]) if raw.get("formatter") else None),
        )
    if kind == "file":
        return ResourceSpec(
            resource_id=resource_id,
            kind="file",
            path=str(raw["path"]),
            refresh_policy=refresh_policy,
            ttl_seconds=ttl_seconds,
            formatter=(str(raw["formatter"]) if raw.get("formatter") else None),
        )
    if kind == "postgres":
        return ResourceSpec(
            resource_id=resource_id,
            kind="postgres",
            dsn=str(raw["dsn"]),
            key_column=str(raw["key_column"]),
            query=(str(raw["query"]) if raw.get("query") is not None else None),
            table=(str(raw["table"]) if raw.get("table") is not None else None),
            refresh_policy=refresh_policy,
            ttl_seconds=ttl_seconds,
            formatter=(str(raw["formatter"]) if raw.get("formatter") else None),
        )
    if kind == "eq_rule_labeler":
        return ResourceSpec(
            resource_id=resource_id,
            kind="eq_rule_labeler",
            rules=raw.get("rules", raw.get("data")),
            path=(str(raw["path"]) if raw.get("path") is not None else None),
            record_mode=str(raw.get("record_mode", "attr")),  # type: ignore[arg-type]
            multi_match=bool(raw.get("multi_match", True)),
            refresh_policy=refresh_policy,
            ttl_seconds=ttl_seconds,
            formatter=(str(raw["formatter"]) if raw.get("formatter") else None),
        )
    raise ValueError(f"unsupported resource kind: {kind!r}")


def _refresh_policy_from_mapping(
    raw: Mapping[str, Any],
) -> tuple[RefreshPolicy, float | None]:
    policy = raw.get("refresh_policy")
    ttl_seconds = raw.get("ttl_seconds")
    if policy is None:
        return "manual", None
    if isinstance(policy, str):
        name = policy.lower()
        if name in {"manual", "reload"}:
            return "manual", None
        if name == "ttl":
            if ttl_seconds is None:
                raise ValueError("ttl refresh_policy requires ttl_seconds")
            return "ttl", float(ttl_seconds)
        raise ValueError(f"unsupported refresh_policy: {policy!r}")
    if isinstance(policy, Mapping):
        name = str(policy.get("type", "manual")).lower()
        if name in {"manual", "reload"}:
            return "manual", None
        if name == "ttl":
            seconds = policy.get("seconds", policy.get("ttl_seconds", ttl_seconds))
            if seconds is None:
                raise ValueError("ttl refresh_policy requires seconds")
            return "ttl", float(seconds)
        raise ValueError(f"unsupported refresh_policy type: {name!r}")
    raise TypeError("refresh_policy must be a string or mapping")


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
            "mode": source.mode,
            "watermark_lag_seconds": source.watermark_lag_seconds,
            "max_window_seconds": source.max_window_seconds,
            "min_window_seconds": source.min_window_seconds,
            "max_rows": source.max_rows,
            "initial_time": (
                None
                if source.initial_time is None
                else source.initial_time.isoformat()
            ),
        }
    raise TypeError(f"unsupported source type: {type(source).__name__}")


def canonical_json(value: Any) -> str:
    """Stable JSON used to compare declarative configs."""

    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


__all__ = [
    "ResourceKind",
    "ResourceRegistry",
    "ResourceSpec",
    "RuleRecordMode",
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
