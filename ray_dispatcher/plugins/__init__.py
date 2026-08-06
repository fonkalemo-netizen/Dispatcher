"""Worker plugin upload, validation, and hot enable/disable."""

from __future__ import annotations

from ray_dispatcher.plugins.manager import (
    PluginActionResult,
    PluginManager,
    ReloadResult,
)
from ray_dispatcher.plugins.store import (
    PluginConflictError,
    PluginNotFoundError,
    PluginRecord,
    PluginStore,
    PluginStoreError,
)
from ray_dispatcher.plugins.validate import (
    ValidationResult,
    sha256_file,
    validate_plugin_id,
    validate_worker_py,
)

try:
    from ray_dispatcher.plugins.api import create_plugin_router
except Exception:  # pragma: no cover - fastapi optional at import time
    create_plugin_router = None  # type: ignore[assignment,misc]

__all__ = [
    "PluginActionResult",
    "PluginConflictError",
    "PluginManager",
    "PluginNotFoundError",
    "PluginRecord",
    "PluginStore",
    "PluginStoreError",
    "ReloadResult",
    "ValidationResult",
    "create_plugin_router",
    "sha256_file",
    "validate_plugin_id",
    "validate_worker_py",
]
