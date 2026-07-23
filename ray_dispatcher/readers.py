"""Framework payload readers for DispatchRequest ranges."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from ray_dispatcher.models import DispatchRequest, KafkaSource, PostgresSource, SourceSpec


class PayloadReader(Protocol):
    """Reads the payload for one scheduled source range."""

    def fetch(self, request: DispatchRequest, source: SourceSpec) -> Any: ...


class KafkaPayloadReader:
    """Consume ``[start_offset, end_offset)`` and return message values only."""

    def __init__(self, *, timeout: float = 10.0) -> None:
        self.timeout = timeout

    def fetch(self, request: DispatchRequest, source: SourceSpec) -> list[Any]:
        if not isinstance(source, KafkaSource):
            raise TypeError("KafkaPayloadReader requires a KafkaSource")
        if (
            request.partition is None
            or request.start_offset is None
            or request.end_offset is None
        ):
            raise ValueError("Kafka fetch requires partition and offset bounds")
        try:
            from confluent_kafka import Consumer, KafkaError, TopicPartition
        except ImportError as exc:
            raise RuntimeError(
                "Kafka payload reading requires: pip install confluent-kafka"
            ) from exc

        consumer = Consumer(
            {
                "bootstrap.servers": ",".join(source.brokers),
                "group.id": f"ray-dispatcher-fetch-{request.dispatch_id}",
                "enable.auto.commit": False,
                "auto.offset.reset": "error",
                "enable.partition.eof": True,
            }
        )
        try:
            consumer.assign(
                [TopicPartition(source.topic, request.partition, request.start_offset)]
            )
            records: list[Any] = []
            cursor = request.start_offset
            while cursor < request.end_offset:
                message = consumer.poll(self.timeout)
                if message is None:
                    raise TimeoutError(
                        f"timed out reading {source.topic}/{request.partition} "
                        f"at offset {cursor}"
                    )
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        raise RuntimeError(
                            f"reached partition EOF for {source.topic}/"
                            f"{request.partition} at offset {cursor} before "
                            f"end_offset {request.end_offset}"
                        )
                    raise RuntimeError(message.error())
                cursor = message.offset() + 1
                records.append(message.value())
            return records
        finally:
            consumer.close()


class PostgresPayloadReader:
    """Read rows in ``start_cursor < row <= end_cursor`` from Postgres."""

    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout

    def fetch(self, request: DispatchRequest, source: SourceSpec) -> list[Any]:
        if not isinstance(source, PostgresSource):
            raise TypeError("PostgresPayloadReader requires a PostgresSource")
        if request.start_cursor is None or request.end_cursor is None:
            raise ValueError("Postgres fetch requires cursor bounds")
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "Postgres payload reading requires: pip install asyncpg"
            ) from exc

        from ray_dispatcher.sources import (
            _quote_identifier,
            _quote_qualified_identifier,
        )

        qualified = _quote_qualified_identifier(source.table)
        ts_q = _quote_identifier(source.timestamp_column)
        pk_q = _quote_identifier(source.primary_key_column)
        sql = (
            f"SELECT * FROM {qualified} "
            f"WHERE ({ts_q}, {pk_q}) > ($1, $2) "
            f"AND ({ts_q}, {pk_q}) <= ($3, $4) "
            f"ORDER BY {ts_q}, {pk_q}"
        )

        async def _load() -> list[Any]:
            connection = await asyncpg.connect(source.dsn, timeout=self.timeout)
            try:
                rows = await connection.fetch(
                    sql,
                    request.start_cursor.timestamp,
                    request.start_cursor.primary_key,
                    request.end_cursor.timestamp,
                    request.end_cursor.primary_key,
                    timeout=self.timeout,
                )
                return [dict(row) for row in rows]
            finally:
                await connection.close()

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(_load())
        raise RuntimeError(
            "PostgresPayloadReader.fetch cannot be called from a running event loop; "
            "invoke it from a worker thread or Ray task"
        )


class CompositePayloadReader:
    """Dispatch to Kafka or Postgres readers based on source kind."""

    def __init__(
        self,
        *,
        kafka: PayloadReader | None = None,
        postgres: PayloadReader | None = None,
    ) -> None:
        self.kafka = kafka or KafkaPayloadReader()
        self.postgres = postgres or PostgresPayloadReader()

    def fetch(self, request: DispatchRequest, source: SourceSpec) -> Any:
        if isinstance(source, KafkaSource):
            return self.kafka.fetch(request, source)
        if isinstance(source, PostgresSource):
            return self.postgres.fetch(request, source)
        raise TypeError(f"unsupported source type: {type(source).__name__}")


def merge_fetch_results(
    source_ids: tuple[str, ...], *payloads: Any
) -> dict[str, list[Any]]:
    """Merge per-partition fetch payloads into ``{source_id: records}``.

    ``source_ids`` may be longer than ``payloads`` when the dispatcher pads
    group sources that had no fetch (empty lists). Extra payloads beyond
    ``source_ids`` are rejected so data cannot be silently dropped.
    """

    if len(payloads) > len(source_ids):
        raise ValueError(
            f"merge_fetch_results got {len(payloads)} payloads for "
            f"{len(source_ids)} source_ids={source_ids!r}; refusing to drop data"
        )
    merged: dict[str, list[Any]] = {source_id: [] for source_id in source_ids}
    for source_id, payload in zip(source_ids, payloads):
        if payload is None:
            continue
        if not isinstance(payload, list):
            raise TypeError(
                f"fetch payload for {source_id!r} must be a list, "
                f"got {type(payload).__name__}"
            )
        merged.setdefault(source_id, []).extend(payload)
    return merged


__all__ = [
    "CompositePayloadReader",
    "KafkaPayloadReader",
    "PayloadReader",
    "PostgresPayloadReader",
    "merge_fetch_results",
]
