"""Optional FastAPI routes for plugin management."""

from __future__ import annotations

from typing import Any

from ray_dispatcher.plugins.manager import PluginManager, ReloadResult
from ray_dispatcher.plugins.store import (
    PluginConflictError,
    PluginNotFoundError,
    PluginStoreError,
)


def create_plugin_router(manager: PluginManager) -> Any:
    """Build a FastAPI ``APIRouter`` bound to ``manager``.

    Requires optional dependency: ``pip install fastapi python-multipart``.
    """

    try:
        from fastapi import APIRouter, File, Form, HTTPException, UploadFile
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "FastAPI plugin routes require: pip install fastapi python-multipart"
        ) from exc

    router = APIRouter(prefix="/api/plugins", tags=["plugins"])

    @router.post("")
    async def upload_plugin(
        plugin_id: str = Form(...),
        file: UploadFile = File(...),
    ) -> dict[str, Any]:
        content = await file.read()
        try:
            result = await manager.upload(plugin_id, content)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except PluginStoreError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return manager.status_payload(result.record)

    @router.get("")
    async def list_plugins() -> list[dict[str, Any]]:
        return [
            manager.status_payload(record)
            for record in await manager.list_plugins()
        ]

    @router.get("/{plugin_id}")
    async def get_plugin(plugin_id: str) -> dict[str, Any]:
        try:
            record = await manager.get(plugin_id)
        except PluginNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return manager.status_payload(record)

    @router.post("/{plugin_id}/enable")
    async def enable_plugin(plugin_id: str) -> dict[str, Any]:
        try:
            result = await manager.enable(plugin_id)
        except PluginNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        status = 202 if result.reload_result is ReloadResult.PENDING_INFLIGHT else 200
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=status,
            content=manager.status_payload(
                result.record, reload_result=result.reload_result
            ),
        )

    @router.post("/{plugin_id}/disable")
    async def disable_plugin(plugin_id: str) -> dict[str, Any]:
        try:
            result = await manager.disable(plugin_id)
        except PluginNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        status = 202 if result.reload_result is ReloadResult.PENDING_INFLIGHT else 200
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=status,
            content=manager.status_payload(
                result.record, reload_result=result.reload_result
            ),
        )

    @router.delete("/{plugin_id}")
    async def delete_plugin(plugin_id: str) -> dict[str, str]:
        try:
            await manager.delete(plugin_id)
        except PluginNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PluginConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"plugin_id": plugin_id, "status": "deleted"}

    return router


__all__ = ["create_plugin_router"]
