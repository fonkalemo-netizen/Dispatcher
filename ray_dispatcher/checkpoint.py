"""Checkpoint store implementations."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Mapping

from ray_dispatcher.models import PostgresCursor

class MemoryCheckpointStore:
    """Useful for tests; production should use a durable atomic store."""

    def __init__(self, initial: Mapping[str, int | PostgresCursor] | None = None) -> None:
        self.values: dict[str, int | PostgresCursor] = dict(initial or {})

    async def load(self, key: str) -> int | PostgresCursor | None:
        return self.values.get(key)

    async def save(self, key: str, value: int | PostgresCursor) -> None:
        self.values[key] = value


class SQLiteCheckpointStore:
    """Durable checkpoint store for a single Dispatcher deployment.

    Each operation uses a short SQLite transaction and is moved off the event
    loop. Postgres primary keys must be JSON-serializable so their original
    comparison type survives a restart.
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
    def _serialize(value: int | PostgresCursor) -> tuple[str, str]:
        if isinstance(value, int):
            return "kafka_offset", json.dumps(value)
        if isinstance(value, PostgresCursor):
            try:
                encoded = json.dumps(
                    {
                        "timestamp": value.timestamp.isoformat(),
                        "primary_key": value.primary_key,
                    },
                    ensure_ascii=False,
                )
            except TypeError as exc:
                raise TypeError(
                    "SQLiteCheckpointStore requires a JSON-serializable "
                    "Postgres primary key"
                ) from exc
            return "postgres_cursor", encoded
        raise TypeError(f"unsupported checkpoint value: {type(value).__name__}")

    @staticmethod
    def _deserialize(value_type: str, value_json: str) -> int | PostgresCursor:
        value = json.loads(value_json)
        if value_type == "kafka_offset":
            if not isinstance(value, int):
                raise TypeError("stored Kafka checkpoint must be an integer")
            return value
        if value_type == "postgres_cursor":
            return PostgresCursor(
                datetime.fromisoformat(value["timestamp"]), value["primary_key"]
            )
        raise ValueError(f"unknown checkpoint value type: {value_type}")

    def _load_sync(self, key: str) -> int | PostgresCursor | None:
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
        return self._deserialize(str(row[0]), str(row[1]))

    async def load(self, key: str) -> int | PostgresCursor | None:
        return await asyncio.to_thread(self._load_sync, key)

    def _save_sync(self, key: str, value: int | PostgresCursor) -> None:
        value_type, value_json = self._serialize(value)
        with self._lock:
            connection = self._connect()
            try:
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
                connection.commit()
            finally:
                connection.close()

    async def save(self, key: str, value: int | PostgresCursor) -> None:
        await asyncio.to_thread(self._save_sync, key, value)
