"""Checkpoint store implementations."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol, Union

from ray_dispatcher.models import PostgresCursor

CheckpointValue = Union[int, PostgresCursor, datetime]


@dataclass(frozen=True)
class CheckpointDocument:
    """Durable checkpoint document: scheduling progress plus optional opaque state."""

    progress: CheckpointValue
    state: Any | None = None


CheckpointWrite = Union[CheckpointDocument, CheckpointValue]


def as_document(value: CheckpointWrite) -> CheckpointDocument:
    """Normalize a bare progress value or document into ``CheckpointDocument``."""

    if isinstance(value, CheckpointDocument):
        return value
    return CheckpointDocument(progress=value, state=None)


class CheckpointStore(Protocol):
    """Checkpoint document store (progress + optional opaque state)."""

    async def load(self, key: str) -> CheckpointDocument | None:
        """读取 ``shared:…`` / ``mswin:…`` / ``actor:…`` 等 key。"""
        ...

    async def save(self, key: str, value: CheckpointWrite) -> None:
        """写入单个 checkpoint 文档；裸 progress 视为 ``state=None``。"""
        ...

    async def save_many(self, items: Mapping[str, CheckpointWrite]) -> None:
        """在同一事务中写入多个 checkpoint 文档。"""
        ...


class MemoryCheckpointStore:
    """Useful for tests; production should use a durable atomic store."""

    def __init__(
        self,
        initial: Mapping[str, CheckpointWrite] | None = None,
    ) -> None:
        self.values: dict[str, CheckpointDocument] = {
            key: as_document(value) for key, value in dict(initial or {}).items()
        }

    async def load(self, key: str) -> CheckpointDocument | None:
        return self.values.get(key)

    async def save(self, key: str, value: CheckpointWrite) -> None:
        self.values[key] = as_document(value)

    async def save_many(self, items: Mapping[str, CheckpointWrite]) -> None:
        for key, value in items.items():
            self.values[key] = as_document(value)


class SQLiteCheckpointStore:
    """Durable checkpoint store for a single Dispatcher deployment.

    Each operation uses a short SQLite transaction and is moved off the event
    loop. Postgres primary keys must be JSON-serializable so their original
    comparison type survives a restart.

    On-disk rows use ``value_type='document'`` with an envelope JSON body. Legacy
    rows (``kafka_offset`` / ``event_time`` / ``postgres_cursor``) are still
    readable and rewritten as documents on the next save.
    """

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
                    CREATE TABLE IF NOT EXISTS ray_dispatcher_checkpoints (
                        checkpoint_key TEXT PRIMARY KEY,
                        value_type TEXT NOT NULL,
                        value_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                connection.commit()
            finally:
                connection.close()

    @staticmethod
    def _serialize_progress(value: CheckpointValue) -> dict[str, Any]:
        if isinstance(value, int):
            return {"type": "kafka_offset", "value": value}
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("datetime checkpoints must be timezone-aware")
            return {"type": "event_time", "value": value.isoformat()}
        if isinstance(value, PostgresCursor):
            try:
                json.dumps(value.primary_key)
            except TypeError as exc:
                raise TypeError(
                    "SQLiteCheckpointStore requires a JSON-serializable "
                    "Postgres primary key"
                ) from exc
            return {
                "type": "postgres_cursor",
                "value": {
                    "timestamp": value.timestamp.isoformat(),
                    "primary_key": value.primary_key,
                },
            }
        raise TypeError(f"unsupported checkpoint value: {type(value).__name__}")

    @staticmethod
    def _deserialize_progress(payload: Mapping[str, Any]) -> CheckpointValue:
        value_type = str(payload["type"])
        value = payload["value"]
        if value_type == "kafka_offset":
            if not isinstance(value, int):
                raise TypeError("stored Kafka checkpoint must be an integer")
            return value
        if value_type == "event_time":
            return datetime.fromisoformat(str(value))
        if value_type == "postgres_cursor":
            return PostgresCursor(
                datetime.fromisoformat(value["timestamp"]), value["primary_key"]
            )
        raise ValueError(f"unknown checkpoint progress type: {value_type}")

    @classmethod
    def _serialize_document(cls, document: CheckpointDocument) -> tuple[str, str]:
        try:
            encoded = json.dumps(
                {
                    "progress": cls._serialize_progress(document.progress),
                    "state": document.state,
                },
                ensure_ascii=False,
            )
        except TypeError as exc:
            raise TypeError(
                "SQLiteCheckpointStore requires JSON-serializable checkpoint state"
            ) from exc
        return "document", encoded

    @classmethod
    def _deserialize_row(cls, value_type: str, value_json: str) -> CheckpointDocument:
        if value_type == "document":
            payload = json.loads(value_json)
            return CheckpointDocument(
                progress=cls._deserialize_progress(payload["progress"]),
                state=payload.get("state"),
            )
        # Legacy flat progress rows.
        if value_type == "kafka_offset":
            value = json.loads(value_json)
            if not isinstance(value, int):
                raise TypeError("stored Kafka checkpoint must be an integer")
            return CheckpointDocument(progress=value, state=None)
        if value_type == "event_time":
            return CheckpointDocument(
                progress=datetime.fromisoformat(str(json.loads(value_json))),
                state=None,
            )
        if value_type == "postgres_cursor":
            value = json.loads(value_json)
            return CheckpointDocument(
                progress=PostgresCursor(
                    datetime.fromisoformat(value["timestamp"]),
                    value["primary_key"],
                ),
                state=None,
            )
        raise ValueError(f"unknown checkpoint value type: {value_type}")

    def _load_sync(self, key: str) -> CheckpointDocument | None:
        with self._lock:
            connection = self._connect()
            try:
                row = connection.execute(
                    "SELECT value_type, value_json "
                    "FROM ray_dispatcher_checkpoints WHERE checkpoint_key = ?",
                    (key,),
                ).fetchone()
            finally:
                connection.close()
        if row is None:
            return None
        return self._deserialize_row(str(row[0]), str(row[1]))

    async def load(self, key: str) -> CheckpointDocument | None:
        return await asyncio.to_thread(self._load_sync, key)

    def _upsert(self, connection: sqlite3.Connection, key: str, document: CheckpointDocument) -> None:
        value_type, value_json = self._serialize_document(document)
        connection.execute(
            """
            INSERT INTO ray_dispatcher_checkpoints (
                checkpoint_key, value_type, value_json, updated_at
            ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(checkpoint_key) DO UPDATE SET
                value_type = excluded.value_type,
                value_json = excluded.value_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value_type, value_json),
        )

    def _save_sync(self, key: str, value: CheckpointWrite) -> None:
        document = as_document(value)
        with self._lock:
            connection = self._connect()
            try:
                self._upsert(connection, key, document)
                connection.commit()
            finally:
                connection.close()

    async def save(self, key: str, value: CheckpointWrite) -> None:
        await asyncio.to_thread(self._save_sync, key, value)

    def _save_many_sync(self, items: Mapping[str, CheckpointWrite]) -> None:
        with self._lock:
            connection = self._connect()
            try:
                for key, value in items.items():
                    self._upsert(connection, key, as_document(value))
                connection.commit()
            finally:
                connection.close()

    async def save_many(self, items: Mapping[str, CheckpointWrite]) -> None:
        await asyncio.to_thread(self._save_many_sync, items)


__all__ = [
    "CheckpointDocument",
    "CheckpointStore",
    "CheckpointValue",
    "CheckpointWrite",
    "MemoryCheckpointStore",
    "SQLiteCheckpointStore",
    "as_document",
]
