"""Orchestrate plugin upload / enable / disable against a RayDispatcher."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ray_dispatcher.dispatcher import RayDispatcher
from ray_dispatcher.plugins.store import (
    PluginNotFoundError,
    PluginRecord,
    PluginStore,
    PluginStoreError,
)
from ray_dispatcher.plugins.validate import ValidationResult


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


class ReloadResult(str, Enum):
    SWAPPED = "swapped"
    PENDING_INFLIGHT = "pending_inflight"
    UNCHANGED = "unchanged"
    FAILED_VALIDATION = "failed_validation"
    FAILED_PRELOAD = "failed_preload"
    FAILED_DISCOVER = "failed_discover"
    FAILED_CONFLICT = "failed_conflict"


@dataclass
class PluginActionResult:
    record: PluginRecord
    reload_result: ReloadResult | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = self.record.to_dict()
        if self.reload_result is not None:
            payload["reload_result"] = self.reload_result.value
        payload["plugin_store_fingerprint"] = None
        return payload


class PluginManager:
    """HTTP/RPC-facing facade over PluginStore + dispatcher.reload_workers."""

    def __init__(
        self,
        store: PluginStore,
        dispatcher: RayDispatcher,
    ) -> None:
        self.store = store
        self.dispatcher = dispatcher
        if dispatcher.plugin_store is None:
            dispatcher.plugin_store = store

    async def startup(self) -> list[str]:
        notes = await self.store.reconcile_on_startup()
        await self.apply_pending_reloads()
        return notes

    async def upload(self, plugin_id: str, content: bytes) -> PluginActionResult:
        record = await self.store.upload(plugin_id, content)
        return PluginActionResult(record=record)

    async def get(self, plugin_id: str) -> PluginRecord:
        record = self.store.get(plugin_id)
        if record is None:
            raise PluginNotFoundError(plugin_id)
        return record

    async def list_plugins(self) -> list[PluginRecord]:
        return self.store.list_plugins()

    async def validate(self, plugin_id: str) -> ValidationResult:
        return await self.store.validate(plugin_id)

    async def enable(self, plugin_id: str) -> PluginActionResult:
        record = self.store.get(plugin_id)
        if record is None:
            raise PluginNotFoundError(plugin_id)
        try:
            record = await self.store.promote_active(plugin_id)
        except PluginStoreError as exc:
            record = self.store.get(plugin_id)
            assert record is not None
            record.last_error = str(exc)
            await self.store.update_record(record)
            return PluginActionResult(
                record=record, reload_result=ReloadResult.FAILED_VALIDATION
            )

        conflict = self._probe_conflict(plugin_id)
        if conflict is not None:
            record.last_error = conflict
            record.desired_enabled = False
            record.effective_enabled = False
            record.reload_pending = False
            await self.store.update_record(record)
            return PluginActionResult(
                record=record, reload_result=ReloadResult.FAILED_CONFLICT
            )

        record.desired_enabled = True
        record.effective_enabled = False
        record.reload_pending = True
        record.enabled_at = _utc_now()
        record.disabled_at = None
        record.last_error = None
        await self.store.update_record(record)

        if self.dispatcher.has_inflight_work():
            return PluginActionResult(
                record=record, reload_result=ReloadResult.PENDING_INFLIGHT
            )

        return await self._reload_with_candidates(plugin_id)

    async def disable(self, plugin_id: str) -> PluginActionResult:
        record = self.store.get(plugin_id)
        if record is None:
            raise PluginNotFoundError(plugin_id)

        record.desired_enabled = False
        record.disabled_at = _utc_now()

        if not record.effective_enabled:
            record.reload_pending = False
            await self.store.update_record(record)
            await self.store.remove_active(plugin_id)
            record = self.store.get(plugin_id)
            assert record is not None
            return PluginActionResult(
                record=record, reload_result=ReloadResult.UNCHANGED
            )

        # Keep effective until swap removes the plugin from dispatcher.
        record.reload_pending = True
        await self.store.update_record(record)

        if self.dispatcher.has_inflight_work():
            return PluginActionResult(
                record=record, reload_result=ReloadResult.PENDING_INFLIGHT
            )

        return await self._reload_with_candidates(plugin_id)

    async def delete(self, plugin_id: str) -> None:
        await self.store.delete(plugin_id)

    async def apply_pending_reloads(self) -> list[PluginActionResult]:
        """Install desired candidate roots when the dispatcher is idle."""

        results: list[PluginActionResult] = []
        if self.dispatcher.has_inflight_work():
            return results

        # Orphan pending: desired=false, effective=false — just clean disk.
        for record in list(self.store.list_plugins()):
            if (
                record.reload_pending
                and not record.desired_enabled
                and not record.effective_enabled
            ):
                await self.store.remove_active(record.plugin_id)
                record.reload_pending = False
                await self.store.update_record(record)
                results.append(
                    PluginActionResult(
                        record=record, reload_result=ReloadResult.UNCHANGED
                    )
                )

        pending = [r for r in self.store.list_plugins() if r.reload_pending]
        if not pending:
            return results

        before = {r.plugin_id: r.to_dict() for r in pending}
        swapped = await self.dispatcher.reload_workers(
            candidate_plugin_roots=self.store.list_candidate_plugin_roots()
        )
        for plugin_id in before:
            record = self.store.get(plugin_id)
            assert record is not None
            if swapped and not record.reload_pending:
                result = ReloadResult.SWAPPED
            elif self.dispatcher.has_inflight_work():
                result = ReloadResult.PENDING_INFLIGHT
            elif any(
                err.startswith("workers_reload:preload:")
                for err in self.dispatcher.state.loop_errors[-5:]
            ):
                result = ReloadResult.FAILED_PRELOAD
            elif any(
                err.startswith("workers_reload:discover:")
                for err in self.dispatcher.state.loop_errors[-5:]
            ):
                result = ReloadResult.FAILED_DISCOVER
            elif not record.reload_pending:
                result = ReloadResult.UNCHANGED
            else:
                # Still pending after failed/no-op reload — metadata untouched.
                result = ReloadResult.PENDING_INFLIGHT
            results.append(PluginActionResult(record=record, reload_result=result))
        return results

    async def _reload_with_candidates(self, plugin_id: str) -> PluginActionResult:
        swapped = await self.dispatcher.reload_workers(
            candidate_plugin_roots=self.store.list_candidate_plugin_roots()
        )
        record = self.store.get(plugin_id)
        assert record is not None
        if swapped:
            return PluginActionResult(record=record, reload_result=ReloadResult.SWAPPED)
        if self.dispatcher.has_inflight_work():
            return PluginActionResult(
                record=record, reload_result=ReloadResult.PENDING_INFLIGHT
            )
        if any(
            err.startswith("workers_reload:preload:")
            for err in self.dispatcher.state.loop_errors[-5:]
        ):
            return PluginActionResult(
                record=record, reload_result=ReloadResult.FAILED_PRELOAD
            )
        if any(
            err.startswith("workers_reload:discover:")
            for err in self.dispatcher.state.loop_errors[-5:]
        ):
            return PluginActionResult(
                record=record, reload_result=ReloadResult.FAILED_DISCOVER
            )

        installed = any(
            name.startswith(f"{plugin_id}:") for name in self.dispatcher.workers
        )
        if record.desired_enabled and installed:
            record.effective_enabled = True
            record.reload_pending = False
            record.last_reload_at = _utc_now()
            await self.store.update_record(record)
            return PluginActionResult(
                record=record, reload_result=ReloadResult.UNCHANGED
            )
        if not record.desired_enabled and not installed:
            record.effective_enabled = False
            record.reload_pending = False
            record.last_reload_at = _utc_now()
            await self.store.update_record(record)
            await self.store.remove_active(plugin_id)
            record = self.store.get(plugin_id)
            assert record is not None
            return PluginActionResult(
                record=record, reload_result=ReloadResult.UNCHANGED
            )
        # Still pending; metadata unchanged for a later retry.
        return PluginActionResult(
            record=record, reload_result=ReloadResult.PENDING_INFLIGHT
        )

    def status_payload(
        self, record: PluginRecord, *, reload_result: ReloadResult | None = None
    ) -> dict[str, Any]:
        payload = record.to_dict()
        if reload_result is not None:
            payload["reload_result"] = reload_result.value
        payload["plugin_store_fingerprint"] = self.store.plugin_store_fingerprint()
        return payload

    def _probe_conflict(self, plugin_id: str) -> str | None:
        """Return error message if enabling would conflict with builtin/effective."""

        from ray_dispatcher.discovery import WorkerDiscoveryError, discover_workers

        builtin = self.dispatcher._workers_dir
        if builtin is None:
            return "dispatcher has no workers directory"
        # Probe desired install set including this plugin's active root.
        roots = [
            builtin,
            *self.store.list_candidate_plugin_roots(),
            self.store.active_root(plugin_id),
        ]
        seen: set[str] = set()
        unique_roots = []
        for root in roots:
            key = str(root.resolve())
            if key in seen:
                continue
            seen.add(key)
            unique_roots.append(root)
        try:
            discover_workers(
                unique_roots,
                ray_module=None,
                source_registry=self.dispatcher._injected_source_registry,
                resource_registry=self.dispatcher._injected_resource_registry,
            )
        except WorkerDiscoveryError as exc:
            return str(exc)
        return None


__all__ = [
    "PluginActionResult",
    "PluginManager",
    "ReloadResult",
]
