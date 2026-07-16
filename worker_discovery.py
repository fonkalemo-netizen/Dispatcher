"""Discover trusted local Ray workers from a directory of Python modules."""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Sequence

from ray_dispatcher import (
    ExecutionMode,
    HandlerSpec,
    KafkaSource,
    OutputSpec,
    PostgresCursor,
    PostgresSource,
    WorkerSpec,
)


class WorkerDiscoveryError(RuntimeError):
    pass


def discover_workers(
    directory: str | Path,
    *,
    ray_module: Any | None = None,
) -> tuple[WorkerSpec, ...]:
    """Import ``*.py`` files and build their :class:`WorkerSpec` objects.

    Modules are trusted application code and execute during import. A module may
    export one of the following:

    1. ``HANDLERS`` containing multiple handler mappings/specs;
    2. ``get_worker_spec() -> WorkerSpec | Mapping``;
    3. ``WORKER_SPEC``;
    4. ``WORKER_CONFIG`` plus a function/class named ``process`` (or a name set
       by ``WORKER_CONFIG['entrypoint']``).

    Plain Python functions/classes are wrapped with ``ray.remote`` when a Ray
    module is supplied. Already-remote Ray objects are left unchanged.
    """

    root = Path(directory).expanduser().resolve()
    if not root.is_dir():
        raise WorkerDiscoveryError(f"worker directory does not exist: {root}")

    workers: list[WorkerSpec] = []
    for path in sorted(root.glob("*.py")):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue
        module = _load_module(path)
        raw_specs, qualify_names = _module_specs(module, path)
        for raw in raw_specs:
            spec = raw if isinstance(raw, HandlerSpec) else _mapping_to_spec(
                module, raw, path, qualify_name=qualify_names
            )
            workers.append(_remote_wrap(spec, ray_module))

    if not workers:
        raise WorkerDiscoveryError(f"no worker modules found in {root}")
    names = [worker.name for worker in workers]
    if len(names) != len(set(names)):
        raise WorkerDiscoveryError(f"duplicate worker names discovered: {names}")
    return tuple(workers)


def _load_module(path: Path) -> ModuleType:
    digest = hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:12]
    module_name = f"ray_dispatcher_worker_{path.stem}_{digest}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise WorkerDiscoveryError(f"cannot load worker module: {path}")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise WorkerDiscoveryError(
            f"worker module {path} failed during import: {type(exc).__name__}: {exc}"
        ) from exc
    return module


def _module_specs(
    module: ModuleType, path: Path
) -> tuple[list[HandlerSpec | Mapping[str, Any]], bool]:
    if hasattr(module, "HANDLERS"):
        raw = module.HANDLERS
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise WorkerDiscoveryError(f"HANDLERS in {path} must be a sequence")
        handlers = list(raw)
        if not handlers:
            raise WorkerDiscoveryError(f"HANDLERS in {path} cannot be empty")
        if not all(isinstance(item, (HandlerSpec, Mapping)) for item in handlers):
            raise WorkerDiscoveryError(f"invalid HANDLERS entry in {path}")
        return handlers, True
    if hasattr(module, "get_worker_spec"):
        raw = module.get_worker_spec()
    elif hasattr(module, "WORKER_SPEC"):
        raw = module.WORKER_SPEC
    elif hasattr(module, "WORKER_CONFIG"):
        raw = module.WORKER_CONFIG
    else:
        raise WorkerDiscoveryError(
            f"{path} must export get_worker_spec(), WORKER_SPEC or WORKER_CONFIG"
        )
    if not isinstance(raw, (WorkerSpec, Mapping)):
        raise WorkerDiscoveryError(f"invalid worker metadata in {path}: {type(raw).__name__}")
    return [raw], False


def _mapping_to_spec(
    module: ModuleType,
    config: Mapping[str, Any],
    path: Path,
    *,
    qualify_name: bool = False,
) -> WorkerSpec:
    try:
        entrypoint = str(config.get("entrypoint", "process"))
        target = config.get("worker") or getattr(module, entrypoint)
        sources = tuple(_source_from_config(item) for item in config["sources"])
        mode = ExecutionMode(str(config.get("mode", "task")))
        remote_method = config.get("remote_method")
        if mode is ExecutionMode.ACTOR and not remote_method:
            remote_method = "process"
        configured_name = str(config.get("name", path.stem))
        raw_fetcher = config.get("data_fetcher")
        data_fetcher = (
            getattr(module, raw_fetcher)
            if isinstance(raw_fetcher, str)
            else raw_fetcher
        )
        fetcher_id = config.get("fetcher_id")
        if fetcher_id is None and isinstance(raw_fetcher, str):
            fetcher_id = raw_fetcher
        explicit_handler_id = config.get("handler_id")
        name = (
            str(explicit_handler_id)
            if explicit_handler_id is not None
            else f"{path.stem}:{configured_name}"
            if qualify_name
            else configured_name
        )
        return HandlerSpec(
            name=name,
            worker=target,
            sources=sources,
            output=_output_from_config(config.get("output")),
            mode=mode,
            remote_method=remote_method,
            max_parallelism=int(config.get("max_parallelism", 4)),
            batch_size=int(config.get("batch_size", 10_000)),
            cpus_per_task=float(config.get("cpus_per_task", 1.0)),
            max_retries=int(config.get("max_retries", 2)),
            cache_history=bool(config.get("cache_history", False)),
            priority=int(config.get("priority", 0)),
            args_builder=config.get("args_builder"),
            shared_source_group=(
                str(config["shared_source_group"])
                if config.get("shared_source_group")
                else None
            ),
            data_fetcher=data_fetcher,
            fetcher_id=str(fetcher_id) if fetcher_id is not None else None,
            fetch_cpus=float(config.get("fetch_cpus", 0.25)),
            fetch_max_retries=int(config.get("fetch_max_retries", 2)),
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise WorkerDiscoveryError(f"invalid WORKER_CONFIG in {path}: {exc}") from exc


def _source_from_config(raw: Any) -> KafkaSource | PostgresSource:
    if isinstance(raw, (KafkaSource, PostgresSource)):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError(f"source metadata must be a mapping, got {type(raw).__name__}")
    kind = str(raw["kind"]).lower()
    if kind == "kafka":
        brokers = raw["brokers"]
        if isinstance(brokers, str):
            brokers = tuple(part.strip() for part in brokers.split(",") if part.strip())
        return KafkaSource(
            source_id=str(raw["source_id"]),
            brokers=tuple(brokers),
            topic=str(raw["topic"]),
            initial_offset=str(raw.get("initial_offset", "latest")),
            retention_policy=str(raw.get("retention_policy", "error")),
            connection_id=(
                str(raw["connection_id"]) if raw.get("connection_id") else None
            ),
        )
    if kind == "postgres":
        return PostgresSource(
            source_id=str(raw["source_id"]),
            dsn=str(raw["dsn"]),
            table=str(raw["table"]),
            timestamp_column=str(raw["timestamp_column"]),
            primary_key_column=str(raw["primary_key_column"]),
            initial_cursor=_cursor_from_config(raw.get("initial_cursor")),
            connection_id=(
                str(raw["connection_id"]) if raw.get("connection_id") else None
            ),
        )
    raise ValueError(f"unsupported source kind: {kind!r}")


def _output_from_config(raw: Any) -> OutputSpec | None:
    if raw is None or isinstance(raw, OutputSpec):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError("output metadata must be OutputSpec or mapping")
    return OutputSpec(
        connection_id=str(raw["connection_id"]),
        target=str(raw["target"]),
        output_format=str(raw["output_format"]),
        max_parallelism=int(raw.get("max_parallelism", 8)),
    )


def _cursor_from_config(raw: Any) -> PostgresCursor | None:
    if raw is None or isinstance(raw, PostgresCursor):
        return raw
    if not isinstance(raw, Mapping):
        raise TypeError("initial_cursor must be PostgresCursor or mapping")
    timestamp = raw["timestamp"]
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return PostgresCursor(timestamp, raw["primary_key"])


def _remote_wrap(spec: WorkerSpec, ray_module: Any | None) -> WorkerSpec:
    if ray_module is None:
        return spec
    worker = spec.worker
    if not hasattr(worker, "remote"):
        if inspect.isclass(worker):
            worker = ray_module.remote(max_restarts=0)(worker)
        else:
            worker = ray_module.remote(max_retries=0)(worker)
    fetcher = spec.data_fetcher
    if fetcher is not None and not hasattr(fetcher, "remote"):
        fetcher = ray_module.remote(max_retries=0)(fetcher)
    return replace(spec, worker=worker, data_fetcher=fetcher)


discover_handlers = discover_workers


__all__ = ["WorkerDiscoveryError", "discover_handlers", "discover_workers"]
