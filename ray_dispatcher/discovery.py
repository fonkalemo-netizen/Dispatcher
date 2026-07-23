"""Discover trusted local Ray workers from a directory of Python modules."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import inspect
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Sequence

from ray_dispatcher.models import (
    ExecutionMode,
    HandlerSpec,
    KafkaSource,
    PostgresSource,
    SourceSpec,
)
from ray_dispatcher.registries import (
    ResourceRegistry,
    ResourceSpec,
    SourceRegistry,
    build_resource_registry,
    build_source_registry,
    canonical_json,
    resolve_resource,
    resolve_source,
    source_canonical_dict,
)


class WorkerDiscoveryError(RuntimeError):
    pass


def discover_workers(
    directory: str | Path,
    *,
    ray_module: Any | None = None,
    source_registry: SourceRegistry | None = None,
    resource_registry: ResourceRegistry | None = None,
) -> tuple[tuple[HandlerSpec, ...], dict[str, SourceSpec], dict[str, ResourceSpec]]:
    """Import ``*.py`` files and build handlers plus merged registries.

    Modules are trusted application code and execute during import. Each module
    must export ``HANDLERS`` (a non-empty sequence of mappings/specs). Each item
    normally only needs ``entrypoint`` (ID defaults to
    ``{module}:{entrypoint}``, override with ``handler_id``).

    Modules may also export declarative ``SOURCES`` / ``RESOURCES`` mappings.
    Those are merged with any injected registries (inject first, then modules).
    Same name with unequal canonical config raises; equal config keeps one copy.
    Handler entries resolve source/resource names against the merged registries.
    """

    root = Path(directory).expanduser().resolve()
    if not root.is_dir():
        raise WorkerDiscoveryError(f"worker directory does not exist: {root}")

    loaded: list[tuple[Path, ModuleType]] = []
    for path in sorted(root.glob("*.py")):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue
        loaded.append((path, _load_module(path)))

    if not loaded:
        raise WorkerDiscoveryError(f"no worker modules found in {root}")

    merged_sources = _merge_source_registry(
        source_registry,
        [(_module_sources(module, path), path) for path, module in loaded],
    )
    merged_resources = _merge_resource_registry(
        resource_registry,
        [(_module_resources(module, path), path) for path, module in loaded],
    )

    workers: list[HandlerSpec] = []
    for path, module in loaded:
        for raw in _module_handlers(module, path):
            spec = raw if isinstance(raw, HandlerSpec) else _mapping_to_spec(
                module,
                raw,
                path,
                source_registry=merged_sources,
                resource_registry=merged_resources,
            )
            try:
                _validate_resolved_resources(spec, merged_resources)
            except KeyError as exc:
                raise WorkerDiscoveryError(f"invalid resources in {path}: {exc}") from exc
            workers.append(_remote_wrap(spec, ray_module))

    if not workers:
        raise WorkerDiscoveryError(f"no worker modules found in {root}")
    names = [worker.name for worker in workers]
    if len(names) != len(set(names)):
        raise WorkerDiscoveryError(f"duplicate worker names discovered: {names}")
    return tuple(workers), merged_sources, merged_resources


def _load_module(path: Path) -> ModuleType:
    # Bust bytecode cache so hot-reload sees freshly written worker files even
    # when mtime resolution would keep a stale .pyc valid.
    importlib.invalidate_caches()
    cache_dir = path.parent / "__pycache__"
    if cache_dir.is_dir():
        for stale in cache_dir.glob(f"{path.stem}.cpython-*.pyc"):
            try:
                stale.unlink()
            except OSError:
                pass
    digest = hashlib.sha256(
        f"{path}:{path.stat().st_mtime_ns}".encode("utf-8")
    ).hexdigest()[:12]
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


def _module_handlers(
    module: ModuleType, path: Path
) -> list[HandlerSpec | Mapping[str, Any]]:
    if not hasattr(module, "HANDLERS"):
        raise WorkerDiscoveryError(f"{path} must export HANDLERS")
    raw = module.HANDLERS
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise WorkerDiscoveryError(f"HANDLERS in {path} must be a sequence")
    handlers = list(raw)
    if not handlers:
        raise WorkerDiscoveryError(f"HANDLERS in {path} cannot be empty")
    if not all(isinstance(item, (HandlerSpec, Mapping)) for item in handlers):
        raise WorkerDiscoveryError(f"invalid HANDLERS entry in {path}")
    return handlers


def _module_sources(module: ModuleType, path: Path) -> Mapping[str, Any]:
    if not hasattr(module, "SOURCES"):
        return {}
    raw = module.SOURCES
    if not isinstance(raw, Mapping):
        raise WorkerDiscoveryError(f"SOURCES in {path} must be a mapping")
    return raw


def _module_resources(module: ModuleType, path: Path) -> Mapping[str, Any]:
    if not hasattr(module, "RESOURCES"):
        return {}
    raw = module.RESOURCES
    if not isinstance(raw, Mapping):
        raise WorkerDiscoveryError(f"RESOURCES in {path} must be a mapping")
    return raw


def _merge_source_registry(
    injected: SourceRegistry | None,
    module_maps: Sequence[tuple[Mapping[str, Any], Path]],
) -> dict[str, SourceSpec]:
    merged: dict[str, SourceSpec] = {}
    if injected:
        for name, source in injected.items():
            if source.source_id != name:
                raise WorkerDiscoveryError(
                    f"source registry key {name!r} must equal source_id "
                    f"{source.source_id!r}"
                )
            merged[name] = source
    for raw_map, path in module_maps:
        if not raw_map:
            continue
        try:
            built = build_source_registry(raw_map)
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkerDiscoveryError(f"invalid SOURCES in {path}: {exc}") from exc
        for name, source in built.items():
            _put_source(merged, name, source, origin=str(path))
    return merged


def _merge_resource_registry(
    injected: ResourceRegistry | None,
    module_maps: Sequence[tuple[Mapping[str, Any], Path]],
) -> dict[str, ResourceSpec]:
    merged: dict[str, ResourceSpec] = {}
    if injected:
        for name, resource in injected.items():
            if resource.resource_id != name:
                raise WorkerDiscoveryError(
                    f"resource registry key {name!r} must equal resource_id "
                    f"{resource.resource_id!r}"
                )
            merged[name] = resource
    for raw_map, path in module_maps:
        if not raw_map:
            continue
        try:
            built = build_resource_registry(raw_map)
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkerDiscoveryError(f"invalid RESOURCES in {path}: {exc}") from exc
        for name, resource in built.items():
            _put_resource(merged, name, resource, origin=str(path))
    return merged


def _put_source(
    merged: dict[str, SourceSpec],
    name: str,
    source: SourceSpec,
    *,
    origin: str,
) -> None:
    existing = merged.get(name)
    if existing is None:
        merged[name] = source
        return
    if canonical_json(source_canonical_dict(existing)) != canonical_json(
        source_canonical_dict(source)
    ):
        raise WorkerDiscoveryError(
            f"conflicting source {name!r} from {origin}: "
            "same name with unequal configuration"
        )


def _put_resource(
    merged: dict[str, ResourceSpec],
    name: str,
    resource: ResourceSpec,
    *,
    origin: str,
) -> None:
    existing = merged.get(name)
    if existing is None:
        merged[name] = resource
        return
    if canonical_json(existing.canonical_dict()) != canonical_json(
        resource.canonical_dict()
    ):
        raise WorkerDiscoveryError(
            f"conflicting resource {name!r} from {origin}: "
            "same name with unequal configuration"
        )


def _mapping_to_spec(
    module: ModuleType,
    config: Mapping[str, Any],
    path: Path,
    *,
    source_registry: SourceRegistry | None = None,
    resource_registry: ResourceRegistry | None = None,
) -> HandlerSpec:
    try:
        entrypoint = str(config.get("entrypoint", "process"))
        target = config.get("worker") or getattr(module, entrypoint)
        sources = tuple(
            _resolve_source_entry(item, source_registry) for item in config["sources"]
        )
        mode = ExecutionMode(str(config.get("mode", "task")))
        remote_method = config.get("remote_method")
        if mode is ExecutionMode.ACTOR and not remote_method:
            remote_method = "process"
        # Default ID is "{module}:{entrypoint}"; handler_id overrides when set.
        if config.get("handler_id") is not None:
            name = str(config["handler_id"])
        else:
            name = f"{path.stem}:{entrypoint}"
        resource_ids = tuple(str(item) for item in config.get("resources", ()))
        return HandlerSpec(
            name=name,
            worker=target,
            sources=sources,
            output=_output_from_config(config.get("output")),
            mode=mode,
            remote_method=remote_method,
            batch_size=_batch_size_from_config(config.get("batch_size", 10_000)),
            cpus_per_task=float(config.get("cpus_per_task", 1.0)),
            max_retries=int(config.get("max_retries", 2)),
            priority=int(config.get("priority", 0)),
            resource_ids=resource_ids,
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise WorkerDiscoveryError(f"invalid HANDLERS entry in {path}: {exc}") from exc


def _batch_size_from_config(raw: Any) -> int | tuple[int | None, int | None]:
    if isinstance(raw, bool):
        raise TypeError("batch_size must be an int or [min, max]")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, (list, tuple)):
        if len(raw) != 2:
            raise ValueError("batch_size range must have length 2")
        left, right = raw[0], raw[1]
        return (
            None if left is None else int(left),
            None if right is None else int(right),
        )
    raise TypeError("batch_size must be an int or [min, max]")


def _resolve_source_entry(
    raw: Any, source_registry: SourceRegistry | None
) -> SourceSpec:
    if isinstance(raw, (KafkaSource, PostgresSource)):
        return raw
    if isinstance(raw, str):
        return resolve_source(source_registry, raw)
    if isinstance(raw, Mapping):
        raise TypeError(
            "inline source mappings are not allowed in HANDLERS; "
            "declare SOURCES in the worker module or inject a source_registry"
        )
    raise TypeError(f"source entry must be a name string, got {type(raw).__name__}")


def _validate_resolved_resources(
    spec: HandlerSpec, resource_registry: ResourceRegistry | None
) -> None:
    for resource_id in spec.resource_ids:
        resolve_resource(resource_registry, resource_id)


def _output_from_config(raw: Any) -> Mapping[str, Any] | None:
    """Opaque write-side mapping; dispatcher only forwards it onto requests."""

    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise TypeError("output metadata must be a mapping")
    return dict(raw)


def _remote_wrap(spec: HandlerSpec, ray_module: Any | None) -> HandlerSpec:
    if ray_module is None:
        return spec
    worker = spec.worker
    if not hasattr(worker, "remote"):
        if inspect.isclass(worker):
            worker = ray_module.remote(max_restarts=0)(worker)
        else:
            worker = ray_module.remote(max_retries=0)(worker)
    return replace(spec, worker=worker)


__all__ = ["WorkerDiscoveryError", "discover_workers"]
