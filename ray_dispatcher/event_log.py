"""Function-hook event log for RayDispatcher runtime facts.

Users extend behavior by registering plain functions (``register`` / ``@hook`` /
module ``HOOKS``), not by implementing an events class. Payloads are small
structured mappings; they do not include fetch records or actor state blobs.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Mapping, MutableMapping, Sequence

EventHook = Callable[[Mapping[str, Any]], Any]

EVENT_BATCH_COMMITTED = "batch_committed"
EVENT_BATCH_SKIPPED = "batch_skipped"
EVENT_RUN_FAILED = "run_failed"
EVENT_SNAPSHOT = "snapshot"
EVENT_TRIGGER_EVALUATED = "trigger_evaluated"
EVENT_WINDOW_OVER_CAPACITY = "window_over_capacity"

KNOWN_EVENTS = frozenset(
    {
        EVENT_BATCH_COMMITTED,
        EVENT_BATCH_SKIPPED,
        EVENT_RUN_FAILED,
        EVENT_SNAPSHOT,
        EVENT_TRIGGER_EVALUATED,
        EVENT_WINDOW_OVER_CAPACITY,
    }
)

LOGGER = logging.getLogger("ray_dispatcher.event_log")


class EventLogError(ValueError):
    """Invalid event-hook configuration."""


class EventLog:
    """Ordered multi-listener registry for dispatcher runtime events."""

    def __init__(self) -> None:
        self._hooks: MutableMapping[str, list[EventHook]] = defaultdict(list)

    def register(self, event: str, fn: EventHook) -> EventHook:
        self._validate_event(event)
        if not callable(fn):
            raise EventLogError(f"hook for {event!r} must be callable")
        self._hooks[event].append(fn)
        return fn

    def unregister(self, event: str, fn: EventHook) -> None:
        self._validate_event(event)
        listeners = self._hooks.get(event)
        if not listeners:
            return
        try:
            listeners.remove(fn)
        except ValueError:
            return
        if not listeners:
            self._hooks.pop(event, None)

    def clear(self, event: str | None = None) -> None:
        if event is None:
            self._hooks.clear()
            return
        self._validate_event(event)
        self._hooks.pop(event, None)

    def listeners(self, event: str) -> tuple[EventHook, ...]:
        self._validate_event(event)
        return tuple(self._hooks.get(event, ()))

    def emit(self, event: str, payload: Mapping[str, Any]) -> list[str]:
        """Call all hooks for ``event``.

        Returns a list of error strings for hooks that raised. Later hooks still
        run after an earlier failure.
        """

        self._validate_event(event)
        errors: list[str] = []
        for fn in list(self._hooks.get(event, ())):
            try:
                fn(payload)
            except Exception as exc:  # noqa: BLE001 - isolate user hooks
                errors.append(
                    f"event_log:{event}:{getattr(fn, '__name__', repr(fn))}: "
                    f"{type(exc).__name__}: {exc}"
                )
        return errors

    def install_default_logging(self) -> None:
        """Register built-in logging hooks for all known events."""

        self.register(EVENT_BATCH_COMMITTED, log_batch_committed)
        self.register(EVENT_BATCH_SKIPPED, log_batch_skipped)
        self.register(EVENT_RUN_FAILED, log_run_failed)
        self.register(EVENT_SNAPSHOT, log_snapshot)
        self.register(EVENT_TRIGGER_EVALUATED, log_trigger_evaluated)

    def hook(self, event: str) -> Callable[[EventHook], EventHook]:
        """Decorator that registers ``fn`` on this ``EventLog``."""

        def decorator(fn: EventHook) -> EventHook:
            self.register(event, fn)
            return fn

        return decorator

    @staticmethod
    def _validate_event(event: str) -> None:
        if event not in KNOWN_EVENTS:
            raise EventLogError(
                f"unknown event {event!r}; expected one of {sorted(KNOWN_EVENTS)}"
            )


_DEFAULT_EVENT_LOG = EventLog()


def hook(event: str) -> Callable[[EventHook], EventHook]:
    """Register ``fn`` on the module-level default :class:`EventLog`."""

    return _DEFAULT_EVENT_LOG.hook(event)


def default_event_log() -> EventLog:
    """Return the process-wide default registry (used by ``@hook``)."""

    return _DEFAULT_EVENT_LOG


def create_event_log(*, default_logging: bool = True) -> EventLog:
    """Create a fresh :class:`EventLog`, optionally with built-in logging hooks."""

    event_log = EventLog()
    if default_logging:
        event_log.install_default_logging()
    return event_log


def _payload_text(payload: Mapping[str, Any]) -> str:
    try:
        return json.dumps(dict(payload), ensure_ascii=False, default=str, sort_keys=True)
    except TypeError:
        return repr(dict(payload))


def log_batch_committed(payload: Mapping[str, Any]) -> None:
    LOGGER.info("batch_committed %s", _payload_text(payload))


def log_batch_skipped(payload: Mapping[str, Any]) -> None:
    LOGGER.warning("batch_skipped %s", _payload_text(payload))


def log_run_failed(payload: Mapping[str, Any]) -> None:
    LOGGER.error("run_failed %s", _payload_text(payload))


def log_snapshot(payload: Mapping[str, Any]) -> None:
    LOGGER.info("snapshot %s", _payload_text(payload))


def log_trigger_evaluated(payload: Mapping[str, Any]) -> None:
    LOGGER.info("trigger_evaluated %s", _payload_text(payload))


def discover_event_hooks(
    directory: str | Path,
    *,
    event_log: EventLog | None = None,
    default_logging: bool = True,
) -> EventLog:
    """Load ``HOOKS`` from ``*.py`` modules under ``directory``.

    Each ``HOOKS`` item is ``{"event": "<name>", "entrypoint": "<fn_name>"}``.
    Modules without ``HOOKS`` are skipped. An empty directory (or no hooks)
    still returns an ``EventLog`` (with default logging when requested).
    """

    root = Path(directory).expanduser().resolve()
    if not root.is_dir():
        raise EventLogError(f"event hook directory does not exist: {root}")

    target = (
        event_log
        if event_log is not None
        else create_event_log(default_logging=default_logging)
    )

    for path in sorted(root.glob("*.py")):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue
        module = _load_module(path)
        raw_hooks = getattr(module, "HOOKS", None)
        if raw_hooks is None:
            continue
        if not isinstance(raw_hooks, Sequence) or isinstance(raw_hooks, (str, bytes)):
            raise EventLogError(f"{path} HOOKS must be a sequence")
        for index, item in enumerate(raw_hooks):
            if not isinstance(item, Mapping):
                raise EventLogError(f"{path} HOOKS[{index}] must be a mapping")
            event = item.get("event")
            entrypoint = item.get("entrypoint")
            if not isinstance(event, str) or not event:
                raise EventLogError(f"{path} HOOKS[{index}] requires string event")
            if not isinstance(entrypoint, str) or not entrypoint:
                raise EventLogError(
                    f"{path} HOOKS[{index}] requires string entrypoint"
                )
            fn = getattr(module, entrypoint, None)
            if not callable(fn):
                raise EventLogError(
                    f"{path} entrypoint {entrypoint!r} is missing or not callable"
                )
            target.register(event, fn)
    return target


def _load_module(path: Path) -> ModuleType:
    module_name = f"ray_dispatcher_event_hooks_{path.stem}_{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise EventLogError(f"unable to import event hook module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


__all__ = [
    "EVENT_BATCH_COMMITTED",
    "EVENT_BATCH_SKIPPED",
    "EVENT_RUN_FAILED",
    "EVENT_SNAPSHOT",
    "EVENT_TRIGGER_EVALUATED",
    "EVENT_WINDOW_OVER_CAPACITY",
    "KNOWN_EVENTS",
    "EventHook",
    "EventLog",
    "EventLogError",
    "create_event_log",
    "default_event_log",
    "discover_event_hooks",
    "hook",
    "log_batch_committed",
    "log_batch_skipped",
    "log_run_failed",
    "log_snapshot",
    "log_trigger_evaluated",
]
