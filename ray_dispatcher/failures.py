"""Durable stores for permanently failed (skipped) batches."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Protocol, Sequence

from ray_dispatcher.models import FailureRecord, FailureRunDetail, PostgresCursor


class FailureStore(Protocol):
    """永久失败（毒区间）落盘接口。"""

    async def save_failure(self, record: FailureRecord) -> None:
        """写入跳过批次的区间元信息，并尽量附带可物化的 fetch payload。"""
        ...

    async def get_failure(self, failure_id: str) -> FailureRecord | None:
        """按 failure_id 查询单条失败记录。"""
        ...

    async def list_failures(self, *, limit: int = 100) -> Sequence[FailureRecord]:
        """列出最近的失败记录，供排查或事后重读。"""
        ...


class MemoryFailureStore:
    """In-memory failure store for tests and short-lived processes."""

    def __init__(self) -> None:
        self.records: dict[str, FailureRecord] = {}
        self._order: list[str] = []

    async def save_failure(self, record: FailureRecord) -> None:
        if record.failure_id not in self.records:
            self._order.append(record.failure_id)
        self.records[record.failure_id] = record

    async def get_failure(self, failure_id: str) -> FailureRecord | None:
        return self.records.get(failure_id)

    async def list_failures(self, *, limit: int = 100) -> Sequence[FailureRecord]:
        ids = self._order[-max(0, limit) :] if limit >= 0 else self._order
        return [self.records[failure_id] for failure_id in ids]


class SQLiteFailureStore:
    """Durable failure store for a single Dispatcher deployment."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(Path(path).expanduser().resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._lock:
            connection = self._connect()
            try:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ray_dispatcher_failures (
                        failure_id TEXT PRIMARY KEY,
                        record_json TEXT NOT NULL,
                        created_at REAL NOT NULL
                    )
                    """
                )
                connection.commit()
            finally:
                connection.close()

    @staticmethod
    def _serialize_bound(value: Any) -> Any:
        if isinstance(value, PostgresCursor):
            return {
                "timestamp": value.timestamp.isoformat(),
                "primary_key": value.primary_key,
            }
        return value

    @staticmethod
    def _deserialize_bound(value: Any) -> Any:
        if isinstance(value, dict) and "timestamp" in value and "primary_key" in value:
            from datetime import datetime

            return PostgresCursor(
                datetime.fromisoformat(value["timestamp"]),
                value["primary_key"],
            )
        return value

    def _serialize(self, record: FailureRecord) -> str:
        body = {
            "failure_id": record.failure_id,
            "batch_id": record.batch_id,
            "source_state_key": record.source_state_key,
            "start": self._serialize_bound(record.start),
            "end": self._serialize_bound(record.end),
            "item_count": record.item_count,
            "worker_names": list(record.worker_names),
            "runs": [
                {
                    "run_id": run.run_id,
                    "kind": run.kind,
                    "worker_name": run.worker_name,
                    "status": run.status,
                    "attempt": run.attempt,
                    "error": run.error,
                    "request": run.request,
                }
                for run in record.runs
            ],
            "payload": record.payload,
            "payload_error": record.payload_error,
            "created_at": record.created_at,
        }
        try:
            return json.dumps(body, ensure_ascii=False)
        except TypeError as exc:
            raise TypeError(
                "SQLiteFailureStore requires JSON-serializable failure payload "
                "and request fields"
            ) from exc

    def _deserialize(self, raw: str) -> FailureRecord:
        body = json.loads(raw)
        runs = tuple(
            FailureRunDetail(
                run_id=str(item["run_id"]),
                kind=item["kind"],
                worker_name=str(item["worker_name"]),
                status=str(item["status"]),
                attempt=int(item["attempt"]),
                error=item.get("error"),
                request=dict(item.get("request") or {}),
            )
            for item in body["runs"]
        )
        return FailureRecord(
            failure_id=str(body["failure_id"]),
            batch_id=str(body["batch_id"]),
            source_state_key=str(body["source_state_key"]),
            start=self._deserialize_bound(body["start"]),
            end=self._deserialize_bound(body["end"]),
            item_count=int(body["item_count"]),
            worker_names=tuple(body.get("worker_names") or ()),
            runs=runs,
            payload=body.get("payload"),
            payload_error=body.get("payload_error"),
            created_at=float(body.get("created_at") or time.time()),
        )

    def _save_sync(self, record: FailureRecord) -> None:
        payload = self._serialize(record)
        with self._lock:
            connection = self._connect()
            try:
                connection.execute(
                    """
                    INSERT INTO ray_dispatcher_failures (
                        failure_id, record_json, created_at
                    ) VALUES (?, ?, ?)
                    ON CONFLICT(failure_id) DO UPDATE SET
                        record_json = excluded.record_json,
                        created_at = excluded.created_at
                    """,
                    (record.failure_id, payload, record.created_at),
                )
                connection.commit()
            finally:
                connection.close()

    async def save_failure(self, record: FailureRecord) -> None:
        await asyncio.to_thread(self._save_sync, record)

    def _get_sync(self, failure_id: str) -> FailureRecord | None:
        with self._lock:
            connection = self._connect()
            try:
                row = connection.execute(
                    "SELECT record_json FROM ray_dispatcher_failures "
                    "WHERE failure_id = ?",
                    (failure_id,),
                ).fetchone()
            finally:
                connection.close()
        if row is None:
            return None
        return self._deserialize(str(row[0]))

    async def get_failure(self, failure_id: str) -> FailureRecord | None:
        return await asyncio.to_thread(self._get_sync, failure_id)

    def _list_sync(self, limit: int) -> list[FailureRecord]:
        with self._lock:
            connection = self._connect()
            try:
                rows = connection.execute(
                    """
                    SELECT record_json FROM ray_dispatcher_failures
                    ORDER BY created_at ASC
                    LIMIT ?
                    """,
                    (max(0, limit),),
                ).fetchall()
            finally:
                connection.close()
        return [self._deserialize(str(row[0])) for row in rows]

    async def list_failures(self, *, limit: int = 100) -> Sequence[FailureRecord]:
        return await asyncio.to_thread(self._list_sync, limit)


__all__ = ["FailureStore", "MemoryFailureStore", "SQLiteFailureStore"]
