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


_LAST_RESOURCE_FORMATTERS: dict[str, Any] = {}


def last_resource_formatters() -> dict[str, Any]:
    """Return resource formatter callables from the last discovery pass."""

    return dict(_LAST_RESOURCE_FORMATTERS)


def discover_workers(
    directory: str | Path | Sequence[str | Path],
    *,
    ray_module: Any | None = None,
    source_registry: SourceRegistry | None = None,
    resource_registry: ResourceRegistry | None = None,
) -> tuple[tuple[HandlerSpec, ...], dict[str, SourceSpec], dict[str, ResourceSpec]]:
    """Import ``*.py`` files and build handlers plus merged registries.

    ``directory`` may be one root or a sequence of roots (builtin + plugins).
    Modules are trusted application code and execute during import. Each module
    may export ``HANDLERS`` (a sequence of mappings/specs). Modules without
    ``HANDLERS`` or with an empty list are skipped for handler discovery (they
    may still contribute ``SOURCES`` / ``RESOURCES``). Each handler item
    normally only needs ``entrypoint`` (ID defaults to
    ``{module}:{entrypoint}``, override with ``handler_id``). Uploaded plugin
    roots under ``plugin_uploads/{id}`` or ``plugin_active/{id}`` default to
    ``{id}:{entrypoint}`` so users do not need to declare a handler ID.

    Modules may also export declarative ``SOURCES`` / ``RESOURCES`` mappings.
    Those are merged with any injected registries (inject first, then modules).
    Same name with unequal canonical config raises; equal config keeps one copy.
    Handler entries resolve source/resource names against the merged registries.
    Task/Actor callables are checked against resource injection rules before
    optional ``ray.remote`` wrapping.
    """

    if isinstance(directory, (str, Path)):
        roots: list[Path] = [Path(directory)]
    else:
        roots = [Path(item) for item in directory]
    return discover_worker_roots(
        roots,
        ray_module=ray_module,
        source_registry=source_registry,
        resource_registry=resource_registry,
    )


def discover_worker_roots(
    directories: Sequence[str | Path],
    *,
    ray_module: Any | None = None,
    source_registry: SourceRegistry | None = None,
    resource_registry: ResourceRegistry | None = None,
) -> tuple[tuple[HandlerSpec, ...], dict[str, SourceSpec], dict[str, ResourceSpec]]:
    """Discover and merge workers from multiple directory roots."""

    if not directories:
        raise WorkerDiscoveryError("at least one worker root is required")

    loaded: list[tuple[Path, ModuleType]] = []
    resolved_roots: list[Path] = []
    for directory in directories:
        root = Path(directory).expanduser().resolve()
        if not root.is_dir():
            raise WorkerDiscoveryError(f"worker directory does not exist: {root}")
        resolved_roots.append(root)
        for path in sorted(root.glob("*.py")):
            if path.name == "__init__.py" or path.name.startswith("_"):
                continue
            loaded.append((path, _load_module(path)))

    if not loaded:
        joined = ", ".join(str(root) for root in resolved_roots)
        raise WorkerDiscoveryError(f"no worker modules found in: {joined}")

    merged_sources = _merge_source_registry(
        source_registry,
        [(_module_sources(module, path), path) for path, module in loaded],
    )
    merged_resources = _merge_resource_registry(
        resource_registry,
        [(_module_resources(module, path), path) for path, module in loaded],
    )
    resource_formatters = _collect_resource_formatters(
        [(_module_resources(module, path), module, path) for path, module in loaded],
        merged_resources,
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
                _validate_handler_callable(spec)
            except KeyError as exc:
                raise WorkerDiscoveryError(f"invalid resources in {path}: {exc}") from exc
            except TypeError as exc:
                raise WorkerDiscoveryError(
                    f"invalid handler callable in {path} ({spec.name}): {exc}"
                ) from exc
            workers.append(_remote_wrap(spec, ray_module))

    if not workers:
        joined = ", ".join(str(root) for root in resolved_roots)
        raise WorkerDiscoveryError(f"no worker modules found in: {joined}")
    names = [worker.name for worker in workers]
    if len(names) != len(set(names)):
        raise WorkerDiscoveryError(f"duplicate worker names discovered: {names}")
    global _LAST_RESOURCE_FORMATTERS
    _LAST_RESOURCE_FORMATTERS = resource_formatters
    return tuple(workers), merged_sources, merged_resources


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def worker_roots_code_fingerprint(
    roots: Sequence[Path | None],
) -> list[tuple[str, str]]:
    """Stable ``(label, sha256)`` rows for ``*.py`` under each root."""

    rows: list[tuple[str, str]] = []
    for index, workers_dir in enumerate(roots):
        if workers_dir is None:
            continue
        root = Path(workers_dir)
        if not root.is_dir():
            continue
        label_prefix = f"root{index}"
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                digest = file_sha256(path)
                rel = str(path.relative_to(root))
            except OSError:
                continue
            rows.append((f"{label_prefix}:{rel}", digest))
    return rows


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
        return []
    raw = module.HANDLERS
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise WorkerDiscoveryError(f"HANDLERS in {path} must be a sequence")
    handlers = list(raw)
    if not handlers:
        return []
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


def _collect_resource_formatters(
    module_maps: Sequence[tuple[Mapping[str, Any], ModuleType, Path]],
    merged_resources: Mapping[str, ResourceSpec],
) -> dict[str, Any]:
    formatters: dict[str, Any] = {}
    for raw_map, module, path in module_maps:
        for resource_id, raw in raw_map.items():
            if not isinstance(raw, Mapping):
                continue
            formatter_name = raw.get("formatter")
            if not formatter_name:
                continue
            resource_id = str(resource_id)
            if resource_id not in merged_resources:
                continue
            formatter_name = str(formatter_name)
            formatter = getattr(module, formatter_name, None)
            if not callable(formatter):
                raise WorkerDiscoveryError(
                    f"resource {resource_id!r} in {path} references formatter "
                    f"{formatter_name!r}, but it is missing or not callable"
                )
            existing = formatters.get(resource_id)
            if existing is not None and existing is not formatter:
                raise WorkerDiscoveryError(
                    f"conflicting formatter for resource {resource_id!r} in {path}"
                )
            formatters[resource_id] = formatter
    return formatters


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
        # Default ID is "{module}:{entrypoint}" for builtin workers, and
        # "{plugin_id}:{entrypoint}" for uploaded plugin roots.
        if config.get("handler_id") is not None:
            name = str(config["handler_id"])
        else:
            name = f"{_default_worker_id_prefix(path)}:{entrypoint}"
        resource_ids = tuple(str(item) for item in config.get("resources", ()))
        sources_tuple = sources
        return HandlerSpec(
            name=name,
            worker=target,
            sources=sources_tuple,
            output=_output_from_config(config.get("output")),
            mode=mode,
            remote_method=remote_method,
            batch_size=_batch_size_from_config(config.get("batch_size", 10_000)),
            cpus_per_task=float(config.get("cpus_per_task", 1.0)),
            max_retries=int(config.get("max_retries", 2)),
            priority=int(config.get("priority", 0)),
            resource_ids=resource_ids,
            external_kafka_json=(
                _is_plugin_worker_path(path)
                and all(isinstance(source, KafkaSource) for source in sources_tuple)
            ),
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise WorkerDiscoveryError(f"invalid HANDLERS entry in {path}: {exc}") from exc


def _default_worker_id_prefix(path: Path) -> str:
    if _is_plugin_worker_path(path):
        return path.parent.name
    return path.stem


def _is_plugin_worker_path(path: Path) -> bool:
    return path.parent.parent.name in {"plugin_uploads", "plugin_active"}


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


def _validate_handler_callable(spec: HandlerSpec) -> None:
    """Fail fast when Task/Actor signatures disagree with resource injection."""

    wants_resources = bool(spec.resource_ids)
    worker = spec.worker
    if spec.mode is ExecutionMode.ACTOR:
        _validate_actor_callable(
            worker,
            spec.remote_method or "process",
            wants_resources=wants_resources,
        )
        return
    _validate_task_callable(worker, wants_resources=wants_resources)


def _positional_business_params(
    fn: Any, *, skip_self: bool
) -> tuple[list[inspect.Parameter], int] | None:
    """Return (positional params, required_count) or None when uninspectable."""

    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return None
    params: list[inspect.Parameter] = []
    for name, param in signature.parameters.items():
        if skip_self and name in ("self", "cls"):
            continue
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            return None
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            continue
        if param.kind is inspect.Parameter.KEYWORD_ONLY:
            continue
        params.append(param)
    required = sum(1 for param in params if param.default is inspect.Parameter.empty)
    return params, required


def _accepts_business_args(fn: Any, count: int, *, skip_self: bool) -> bool | None:
    """True/False if ``fn`` can take ``count`` business args; None to skip."""

    info = _positional_business_params(fn, skip_self=skip_self)
    if info is None:
        return None
    params, required = info
    return required <= count <= len(params)


def _requires_business_args(fn: Any, count: int, *, skip_self: bool) -> bool:
    """True when ``fn`` requires at least ``count`` business positional args."""

    info = _positional_business_params(fn, skip_self=skip_self)
    if info is None:
        return False
    _params, required = info
    return required >= count


def _validate_task_callable(worker: Any, *, wants_resources: bool) -> None:
    if inspect.isclass(worker):
        raise TypeError("task mode requires a function entrypoint, not a class")
    if not callable(worker):
        raise TypeError("task mode requires a callable entrypoint")
    # Already Ray-wrapped remote functions are opaque; skip arity checks.
    if hasattr(worker, "remote") and not inspect.isfunction(worker):
        return
    expected = 3 if wants_resources else 2
    accepted = _accepts_business_args(worker, expected, skip_self=False)
    if accepted is False:
        if wants_resources:
            raise TypeError(
                "task handlers that declare resources must accept "
                "(request, records, resources)"
            )
        raise TypeError(
            "task handlers without resources must accept (request, records); "
            "do not require a resources parameter"
        )


def _validate_actor_callable(
    worker: Any, method_name: str, *, wants_resources: bool
) -> None:
    if hasattr(worker, "remote") and not inspect.isclass(worker):
        # Already wrapped ActorClass — underlying signature is not available.
        return
    if not inspect.isclass(worker):
        raise TypeError("actor mode requires a class entrypoint")
    init_fn = _actor_init_function(worker)
    if wants_resources:
        if init_fn is None:
            raise TypeError(
                "actor handlers that declare resources require "
                "__init__(self, resources)"
            )
        init_ok = _accepts_business_args(init_fn, 1, skip_self=True)
        if init_ok is False:
            raise TypeError(
                "actor handlers that declare resources require "
                "__init__(self, resources)"
            )
    elif init_fn is not None:
        init_ok = _accepts_business_args(init_fn, 0, skip_self=True)
        if init_ok is False:
            raise TypeError(
                "actor handlers without resources must not require an __init__ "
                "resources argument"
            )
    if not hasattr(worker, method_name):
        raise TypeError(f"actor class missing method {method_name!r}")
    raw_method = _class_attr_function(worker, method_name)
    if raw_method is None or not callable(raw_method):
        raise TypeError(f"actor method {method_name!r} is not callable")
    if _requires_business_args(raw_method, 3, skip_self=True):
        raise TypeError(
            f"actor method {method_name!r} must not require resources; "
            "inject snapshots via __init__(self, resources)"
        )
    method_ok = _accepts_business_args(raw_method, 2, skip_self=True)
    if method_ok is False:
        raise TypeError(
            f"actor method {method_name!r} must accept (request, records)"
        )


def _actor_init_function(cls: type) -> Any | None:
    """Return the class's custom ``__init__`` function, or None for ``object.__init__``."""

    return _class_attr_function(cls, "__init__")


def _class_attr_function(cls: type, name: str) -> Any | None:
    """Return the raw function for ``name`` from the class MRO, unwrapping descriptors."""

    for base in cls.__mro__:
        if base is object and name == "__init__":
            return None
        if name not in base.__dict__:
            continue
        attr = base.__dict__[name]
        if isinstance(attr, (staticmethod, classmethod)):
            return attr.__func__
        return attr
    return None


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


__all__ = [
    "WorkerDiscoveryError",
    "discover_worker_roots",
    "discover_workers",
    "file_sha256",
    "last_resource_formatters",
    "worker_roots_code_fingerprint",
]
