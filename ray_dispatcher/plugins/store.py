"""Persistent plugin metadata and staging/active directory management."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ray_dispatcher.plugins.validate import (
    ValidationResult,
    sha256_file,
    validate_plugin_id,
    validate_worker_py,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _safe_plugin_filename(filename: str | None) -> str:
    raw = (filename or "worker.py").strip()
    if any(part in raw for part in ("/", "\\", "\x00")):
        raise PluginStoreError("uploaded plugin filename must not contain path parts")
    name = raw
    if not name:
        name = "worker.py"
    if name in {"", ".", ".."} or Path(name).suffix != ".py":
        raise PluginStoreError("uploaded plugin file must be a .py file")
    return name


@dataclass
class PluginRecord:
    plugin_id: str
    desired_enabled: bool = False
    effective_enabled: bool = False
    reload_pending: bool = False
    filename: str = "worker.py"
    sha256: str = ""
    validation: dict[str, Any] = field(
        default_factory=lambda: {"ok": False, "errors": [], "warnings": []}
    )
    handler_ids: list[str] = field(default_factory=list)
    source_ids: list[str] = field(default_factory=list)
    resource_ids: list[str] = field(default_factory=list)
    uploaded_at: str | None = None
    enabled_at: str | None = None
    disabled_at: str | None = None
    last_reload_at: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PluginRecord":
        validation = dict(raw.get("validation") or {})
        validation.setdefault("ok", False)
        validation.setdefault("errors", [])
        validation.setdefault("warnings", [])
        return cls(
            plugin_id=str(raw["plugin_id"]),
            desired_enabled=bool(raw.get("desired_enabled", False)),
            effective_enabled=bool(raw.get("effective_enabled", False)),
            reload_pending=bool(raw.get("reload_pending", False)),
            filename=str(raw.get("filename", "worker.py")),
            sha256=str(raw.get("sha256", "")),
            validation=validation,
            handler_ids=[str(item) for item in raw.get("handler_ids", [])],
            source_ids=[str(item) for item in raw.get("source_ids", [])],
            resource_ids=[str(item) for item in raw.get("resource_ids", [])],
            uploaded_at=raw.get("uploaded_at"),
            enabled_at=raw.get("enabled_at"),
            disabled_at=raw.get("disabled_at"),
            last_reload_at=raw.get("last_reload_at"),
            last_error=raw.get("last_error"),
        )


class PluginStoreError(RuntimeError):
    pass


class PluginConflictError(PluginStoreError):
    pass


class PluginNotFoundError(PluginStoreError):
    pass


class PluginStore:
    """JSON + directory store for uploaded worker plugins."""

    def __init__(self, root: str | Path, *, max_bytes: int = 1_048_576) -> None:
        self.root = Path(root).expanduser().resolve()
        self.uploads_dir = self.root / "plugin_uploads"
        self.active_dir = self.root / "plugin_active"
        self.state_path = self.root / "plugins.json"
        self.max_bytes = max_bytes
        self._lock = asyncio.Lock()
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.active_dir.mkdir(parents=True, exist_ok=True)
        if not self.state_path.exists():
            self._write_state({"plugins": {}})

    def plugin_file_name(self, plugin_id: str) -> str:
        record = self.get(plugin_id)
        return record.filename if record is not None else "worker.py"

    def staging_path(self, plugin_id: str) -> Path:
        return self.uploads_dir / plugin_id / self.plugin_file_name(plugin_id)

    def active_path(self, plugin_id: str) -> Path:
        return self.active_dir / plugin_id / self.plugin_file_name(plugin_id)

    def active_root(self, plugin_id: str) -> Path:
        return self.active_dir / plugin_id

    def list_active_roots(self) -> list[Path]:
        """Roots currently installed in the dispatcher (effective_enabled only)."""

        state = self._read_state()
        roots: list[Path] = []
        for plugin_id, raw in state.get("plugins", {}).items():
            record = PluginRecord.from_dict(raw)
            if not record.effective_enabled:
                continue
            if not record.validation.get("ok"):
                continue
            root = self.active_root(plugin_id)
            if root.is_dir() and self.active_path(plugin_id).is_file():
                roots.append(root)
        return sorted(roots)

    def list_pending_enable_roots(self) -> list[Path]:
        """Active dirs ready to install but not yet effective."""

        state = self._read_state()
        roots: list[Path] = []
        for plugin_id, raw in state.get("plugins", {}).items():
            record = PluginRecord.from_dict(raw)
            if not (
                record.desired_enabled
                and not record.effective_enabled
                and record.reload_pending
                and record.validation.get("ok")
            ):
                continue
            root = self.active_root(plugin_id)
            if root.is_dir() and self.active_path(plugin_id).is_file():
                roots.append(root)
        return sorted(roots)

    def list_candidate_plugin_roots(self) -> list[Path]:
        """Plugin roots that should be installed after the next successful reload.

        Equals desired effective roots (excludes pending-disable) plus
        ``list_pending_enable_roots()``. Never flips metadata.
        """

        state = self._read_state()
        roots: list[Path] = []
        seen: set[str] = set()

        def _add(root: Path) -> None:
            key = str(root.resolve())
            if key in seen:
                return
            seen.add(key)
            roots.append(root)

        for plugin_id, raw in state.get("plugins", {}).items():
            record = PluginRecord.from_dict(raw)
            if not record.desired_enabled or not record.validation.get("ok"):
                continue
            root = self.active_root(plugin_id)
            if root.is_dir() and self.active_path(plugin_id).is_file():
                _add(root)
        return sorted(roots)

    def finalize_after_reload(self, installed_handler_ids: set[str]) -> None:
        """Reconcile desired/effective/pending after a successful dispatcher swap."""

        state = self._read_state()
        plugins = state.setdefault("plugins", {})
        changed = False
        now = _utc_now()
        for plugin_id, raw in list(plugins.items()):
            record = PluginRecord.from_dict(raw)
            present = any(
                handler_id in installed_handler_ids for handler_id in record.handler_ids
            )
            if record.desired_enabled:
                if present:
                    if not record.effective_enabled or record.reload_pending:
                        record.effective_enabled = True
                        record.reload_pending = False
                        record.last_reload_at = now
                        record.last_error = None
                        plugins[plugin_id] = record.to_dict()
                        changed = True
            else:
                if present:
                    if not record.reload_pending:
                        record.reload_pending = True
                        plugins[plugin_id] = record.to_dict()
                        changed = True
                else:
                    if (
                        record.effective_enabled
                        or record.reload_pending
                        or self.active_path(plugin_id).exists()
                    ):
                        record.effective_enabled = False
                        record.reload_pending = False
                        record.last_reload_at = now
                        plugins[plugin_id] = record.to_dict()
                        changed = True
                        self._remove_path(self.active_root(plugin_id))
        if changed:
            self._write_state(state)

    def get(self, plugin_id: str) -> PluginRecord | None:
        raw = self._read_state().get("plugins", {}).get(plugin_id)
        if raw is None:
            return None
        return PluginRecord.from_dict(raw)

    def list_plugins(self) -> list[PluginRecord]:
        state = self._read_state()
        return [
            PluginRecord.from_dict(raw)
            for raw in state.get("plugins", {}).values()
        ]

    def plugin_store_fingerprint(self) -> str:
        from ray_dispatcher.registries import canonical_json

        rows = []
        for record in sorted(self.list_plugins(), key=lambda item: item.plugin_id):
            staging = self.staging_path(record.plugin_id)
            active = self.active_path(record.plugin_id)
            rows.append(
                {
                    "plugin_id": record.plugin_id,
                    "desired_enabled": record.desired_enabled,
                    "effective_enabled": record.effective_enabled,
                    "reload_pending": record.reload_pending,
                    "staging_sha256": (
                        sha256_file(staging) if staging.is_file() else None
                    ),
                    "active_sha256": (
                        sha256_file(active) if active.is_file() else None
                    ),
                    "validation_ok": bool(record.validation.get("ok")),
                    "sha256": record.sha256,
                }
            )
        return canonical_json({"plugins": rows})

    async def reconcile_on_startup(self) -> list[str]:
        async with self._lock:
            return self._reconcile_on_startup_unlocked()

    def _reconcile_on_startup_unlocked(self) -> list[str]:
        state = self._read_state()
        notes: list[str] = []
        plugins = state.setdefault("plugins", {})
        changed = False
        for plugin_id, raw in list(plugins.items()):
            record = PluginRecord.from_dict(raw)
            active = self.active_path(plugin_id)
            if record.desired_enabled and not active.is_file():
                record.effective_enabled = False
                record.reload_pending = True
                record.last_error = "active missing while desired_enabled=true"
                notes.append(record.last_error + f" ({plugin_id})")
                plugins[plugin_id] = record.to_dict()
                changed = True
                continue
            if active.is_file():
                digest = sha256_file(active)
                if record.sha256 and digest != record.sha256:
                    record.validation = {
                        "ok": False,
                        "errors": [
                            "active sha256 mismatch; refusing to schedule plugin"
                        ],
                        "warnings": [],
                    }
                    record.effective_enabled = False
                    record.reload_pending = False
                    record.last_error = "active sha256 mismatch"
                    notes.append(record.last_error + f" ({plugin_id})")
                    plugins[plugin_id] = record.to_dict()
                    changed = True
                    continue
            if not record.desired_enabled and active.exists():
                if not record.effective_enabled and not record.reload_pending:
                    self._remove_path(self.active_root(plugin_id))
                    notes.append(f"cleaned orphan active for {plugin_id}")
                    changed = True
            plugins[plugin_id] = record.to_dict()
        if changed:
            self._write_state(state)
        return notes

    async def upload(
        self,
        plugin_id: str,
        content: bytes,
        *,
        filename: str | None = None,
    ) -> PluginRecord:
        validate_plugin_id(plugin_id)
        safe_filename = _safe_plugin_filename(filename)
        if len(content) > self.max_bytes:
            raise PluginStoreError(
                f"upload exceeds size limit ({len(content)} > {self.max_bytes})"
            )
        async with self._lock:
            staging_dir = self.uploads_dir / plugin_id
            self._remove_path(staging_dir)
            staging_dir.mkdir(parents=True, exist_ok=True)
            staging = staging_dir / safe_filename
            tmp = staging.with_name(f".{safe_filename}.tmp")
            tmp.write_bytes(content)
            os.replace(tmp, staging)
            result = validate_worker_py(
                staging, plugin_id=plugin_id, max_bytes=self.max_bytes
            )
            state = self._read_state()
            existing = state.get("plugins", {}).get(plugin_id)
            record = PluginRecord(
                plugin_id=plugin_id,
                desired_enabled=False,
                effective_enabled=(
                    bool(existing.get("effective_enabled")) if existing else False
                ),
                reload_pending=False,
                filename=safe_filename,
                sha256=result.sha256,
                validation={
                    "ok": result.ok,
                    "errors": result.errors,
                    "warnings": result.warnings,
                },
                handler_ids=result.handler_ids,
                source_ids=result.source_ids,
                resource_ids=result.resource_ids,
                uploaded_at=_utc_now(),
                enabled_at=None,
                disabled_at=None,
                last_reload_at=existing.get("last_reload_at") if existing else None,
                last_error=None if result.ok else "; ".join(result.errors),
            )
            # Re-upload forces disabled desired state; clear effective if was on.
            if record.effective_enabled:
                record.reload_pending = True
            state.setdefault("plugins", {})[plugin_id] = record.to_dict()
            self._write_state(state)
            return record

    async def validate(self, plugin_id: str) -> ValidationResult:
        async with self._lock:
            staging = self.staging_path(plugin_id)
            if not staging.is_file():
                raise PluginNotFoundError(plugin_id)
            result = validate_worker_py(
                staging, plugin_id=plugin_id, max_bytes=self.max_bytes
            )
            state = self._read_state()
            raw = state.get("plugins", {}).get(plugin_id)
            if raw is None:
                raise PluginNotFoundError(plugin_id)
            record = PluginRecord.from_dict(raw)
            record.sha256 = result.sha256
            record.validation = {
                "ok": result.ok,
                "errors": result.errors,
                "warnings": result.warnings,
            }
            record.handler_ids = result.handler_ids
            record.source_ids = result.source_ids
            record.resource_ids = result.resource_ids
            record.last_error = None if result.ok else "; ".join(result.errors)
            state["plugins"][plugin_id] = record.to_dict()
            self._write_state(state)
            return result

    async def promote_active(self, plugin_id: str) -> PluginRecord:
        """Copy validated staging → active (atomic replace)."""

        async with self._lock:
            return self._promote_active_unlocked(plugin_id)

    def _promote_active_unlocked(self, plugin_id: str) -> PluginRecord:
        staging = self.staging_path(plugin_id)
        if not staging.is_file():
            raise PluginNotFoundError(plugin_id)
        result = validate_worker_py(
            staging, plugin_id=plugin_id, max_bytes=self.max_bytes
        )
        if not result.ok:
            state = self._read_state()
            raw = state["plugins"][plugin_id]
            record = PluginRecord.from_dict(raw)
            record.validation = {
                "ok": False,
                "errors": result.errors,
                "warnings": result.warnings,
            }
            record.last_error = "; ".join(result.errors)
            state["plugins"][plugin_id] = record.to_dict()
            self._write_state(state)
            raise PluginStoreError(f"validation failed: {record.last_error}")

        state = self._read_state()
        record = PluginRecord.from_dict(state["plugins"][plugin_id])
        active_root = self.active_root(plugin_id)
        tmp_root = self.active_dir / f".{plugin_id}.tmp-{os.getpid()}"
        if tmp_root.exists():
            shutil.rmtree(tmp_root)
        tmp_root.mkdir(parents=True, exist_ok=True)
        target = tmp_root / record.filename
        shutil.copy2(staging, target)
        # Atomic replace of directory: remove old then rename tmp.
        final = active_root
        backup = self.active_dir / f".{plugin_id}.bak-{os.getpid()}"
        if final.exists():
            os.replace(final, backup)
        try:
            os.replace(tmp_root, final)
        except Exception:
            if backup.exists() and not final.exists():
                os.replace(backup, final)
            raise
        finally:
            if backup.exists():
                shutil.rmtree(backup, ignore_errors=True)

        state = self._read_state()
        record = PluginRecord.from_dict(state["plugins"][plugin_id])
        record.sha256 = result.sha256
        record.validation = {
            "ok": True,
            "errors": [],
            "warnings": result.warnings,
        }
        record.handler_ids = result.handler_ids
        record.source_ids = result.source_ids
        record.resource_ids = result.resource_ids
        record.last_error = None
        state["plugins"][plugin_id] = record.to_dict()
        self._write_state(state)
        return record

    async def update_record(self, record: PluginRecord) -> PluginRecord:
        async with self._lock:
            state = self._read_state()
            if record.plugin_id not in state.get("plugins", {}):
                raise PluginNotFoundError(record.plugin_id)
            state["plugins"][record.plugin_id] = record.to_dict()
            self._write_state(state)
            return record

    async def remove_active(self, plugin_id: str) -> None:
        async with self._lock:
            self._remove_path(self.active_root(plugin_id))

    async def delete(self, plugin_id: str) -> None:
        async with self._lock:
            state = self._read_state()
            raw = state.get("plugins", {}).get(plugin_id)
            if raw is None:
                raise PluginNotFoundError(plugin_id)
            record = PluginRecord.from_dict(raw)
            if (
                record.desired_enabled
                or record.effective_enabled
                or record.reload_pending
            ):
                raise PluginConflictError(
                    "delete requires desired_enabled=false, "
                    "effective_enabled=false, reload_pending=false"
                )
            self._remove_path(self.uploads_dir / plugin_id)
            self._remove_path(self.active_root(plugin_id))
            del state["plugins"][plugin_id]
            self._write_state(state)

    def _read_state(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise PluginStoreError(
                f"plugins.json is corrupt at {self.state_path}: {exc}"
            ) from exc
        if not isinstance(payload, dict) or "plugins" not in payload:
            raise PluginStoreError("plugins.json missing top-level 'plugins'")
        return payload

    def _write_state(self, state: Mapping[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        data = json.dumps(state, indent=2, sort_keys=True) + "\n"
        fd, tmp_name = tempfile.mkstemp(
            prefix="plugins.", suffix=".json.tmp", dir=str(self.root)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.state_path)
        finally:
            if os.path.exists(tmp_name):
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    @staticmethod
    def _remove_path(path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink(missing_ok=True)


__all__ = [
    "PluginConflictError",
    "PluginNotFoundError",
    "PluginRecord",
    "PluginStore",
    "PluginStoreError",
]
