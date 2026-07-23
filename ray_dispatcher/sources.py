"""Production source observers for RayDispatcher.

Dependencies are imported lazily:

    pip install confluent-kafka asyncpg
"""

from __future__ import annotations

import asyncio
import re
import threading
from datetime import datetime, timezone
from typing import Any, Mapping

from ray_dispatcher.models import KafkaSource, PostgresCursor, PostgresSource


class SourceDependencyError(RuntimeError):
    pass


class EmptyPostgresSource(RuntimeError):
    pass


class SourceObserver:
    """Fixed watermark observer / range planner used by RayDispatcher.

    Not a payload reader: ``PayloadReader`` performs record I/O.
    This observer only answers "how far has the source progressed?" and optionally
    plans Postgres task slices. It is not a user-facing extension point.
    """

    def __init__(
        self,
        *,
        kafka_timeout: float = 10.0,
        postgres_timeout: float = 30.0,
        postgres_pool_size: int = 4,
    ) -> None:
        self.kafka_timeout = kafka_timeout
        self.postgres_timeout = postgres_timeout
        self.postgres_pool_size = postgres_pool_size
        self._kafka_consumers: dict[tuple[Any, ...], Any] = {}
        self._kafka_lock = threading.RLock()
        self._postgres_pools: dict[str, Any] = {}

    async def kafka_watermarks(
        self, source: KafkaSource
    ) -> Mapping[int, tuple[int, int]]:
        """Discover every partition and actively query its low/high offsets."""

        return await asyncio.to_thread(self._kafka_watermarks_sync, source)

    def _kafka_watermarks_sync(self, source: KafkaSource) -> dict[int, tuple[int, int]]:
        try:
            from confluent_kafka import KafkaException, TopicPartition
        except ImportError as exc:
            raise SourceDependencyError(
                "Kafka observation requires: pip install confluent-kafka"
            ) from exc

        with self._kafka_lock:
            consumer = self._kafka_consumer(source)
            metadata = consumer.list_topics(source.topic, timeout=self.kafka_timeout)
            topic_metadata = metadata.topics.get(source.topic)
            if topic_metadata is None:
                raise RuntimeError(f"Kafka topic not found: {source.topic}")
            if topic_metadata.error is not None:
                raise KafkaException(topic_metadata.error)

            result: dict[int, tuple[int, int]] = {}
            for partition in sorted(topic_metadata.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(source.topic, partition),
                    timeout=self.kafka_timeout,
                    cached=False,
                )
                result[int(partition)] = (int(low), int(high))
            return result

    async def kafka_event_time_highs(
        self,
        source: KafkaSource,
        watermarks: Mapping[int, tuple[int, int]] | None = None,
    ) -> Mapping[int, datetime]:
        """Return the record timestamp at ``high-1`` for each non-empty partition."""

        return await asyncio.to_thread(
            self._kafka_event_time_highs_sync, source, watermarks
        )

    def _kafka_event_time_highs_sync(
        self,
        source: KafkaSource,
        watermarks: Mapping[int, tuple[int, int]] | None,
    ) -> dict[int, datetime]:
        try:
            from confluent_kafka import KafkaError, TopicPartition
        except ImportError as exc:
            raise SourceDependencyError(
                "Kafka observation requires: pip install confluent-kafka"
            ) from exc

        if watermarks is None:
            watermarks = self._kafka_watermarks_sync(source)
        with self._kafka_lock:
            consumer = self._kafka_consumer(source)
            result: dict[int, datetime] = {}
            for partition, (low, high) in watermarks.items():
                if high <= low:
                    continue
                consumer.assign([TopicPartition(source.topic, partition, high - 1)])
                message = consumer.poll(self.kafka_timeout)
                if message is None:
                    raise TimeoutError(
                        f"timed out reading event-time high for "
                        f"{source.topic}/{partition} at offset {high - 1}"
                    )
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(message.error())
                _ts_type, ts_value = message.timestamp()
                if ts_value < 0:
                    raise RuntimeError(
                        f"missing Kafka timestamp for {source.topic}/{partition}"
                    )
                result[int(partition)] = datetime.fromtimestamp(
                    ts_value / 1000.0, tz=timezone.utc
                )
            return result

    async def kafka_offsets_for_times(
        self,
        source: KafkaSource,
        timestamps: Mapping[int, datetime],
    ) -> Mapping[int, int]:
        """Map event-times to the earliest offset at or after each timestamp."""

        return await asyncio.to_thread(
            self._kafka_offsets_for_times_sync, source, timestamps
        )

    def _kafka_offsets_for_times_sync(
        self,
        source: KafkaSource,
        timestamps: Mapping[int, datetime],
    ) -> dict[int, int]:
        try:
            from confluent_kafka import TopicPartition
        except ImportError as exc:
            raise SourceDependencyError(
                "Kafka observation requires: pip install confluent-kafka"
            ) from exc

        query = []
        for partition, moment in timestamps.items():
            if moment.tzinfo is None:
                raise ValueError("offsets_for_times requires timezone-aware datetimes")
            millis = int(moment.timestamp() * 1000)
            query.append(TopicPartition(source.topic, int(partition), millis))
        with self._kafka_lock:
            consumer = self._kafka_consumer(source)
            answered = consumer.offsets_for_times(query, timeout=self.kafka_timeout)
            result: dict[int, int] = {}
            for item in answered:
                if item is None or item.offset is None or item.offset < 0:
                    # No message at/after timestamp: treat as high watermark later.
                    continue
                result[int(item.partition)] = int(item.offset)
            return result

    def _kafka_consumer(self, source: KafkaSource) -> Any:
        """Return a shared metadata consumer. Caller must hold ``_kafka_lock``."""

        brokers = tuple(source.brokers)
        connection_key = (source.connection_id, *sorted(brokers))
        consumer = self._kafka_consumers.get(connection_key)
        if consumer is not None:
            return consumer
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise SourceDependencyError(
                "Kafka observation requires: pip install confluent-kafka"
            ) from exc
        consumer = Consumer(
            {
                "bootstrap.servers": ",".join(brokers),
                "group.id": "ray-dispatcher-metadata",
                "enable.auto.commit": False,
                "allow.auto.create.topics": False,
            }
        )
        self._kafka_consumers[connection_key] = consumer
        return consumer

    async def postgres_high_watermark(self, source: PostgresSource) -> PostgresCursor:
        """Return the largest ``(timestamp, primary_key)`` currently visible."""

        pool = await self._postgres_pool(source.dsn)
        table = _quote_qualified_identifier(source.table)
        timestamp_column = _quote_identifier(source.timestamp_column)
        primary_key_column = _quote_identifier(source.primary_key_column)
        sql = (
            f"SELECT {timestamp_column} AS cursor_timestamp, "
            f"{primary_key_column} AS cursor_primary_key "
            f"FROM {table} "
            f"ORDER BY {timestamp_column} DESC, {primary_key_column} DESC LIMIT 1"
        )
        row = await pool.fetchrow(sql, timeout=self.postgres_timeout)
        if row is None:
            if source.initial_cursor is None:
                raise EmptyPostgresSource(
                    f"{source.source_id} is empty; configure initial_cursor as its baseline"
                )
            return source.initial_cursor
        return PostgresCursor(row["cursor_timestamp"], row["cursor_primary_key"])

    async def postgres_count(
        self,
        source: PostgresSource,
        start_exclusive: PostgresCursor,
        end_inclusive: PostgresCursor,
    ) -> int:
        """Count rows in ``start < (timestamp, pk) <= end`` for task sizing."""

        pool = await self._postgres_pool(source.dsn)
        table = _quote_qualified_identifier(source.table)
        timestamp_column = _quote_identifier(source.timestamp_column)
        primary_key_column = _quote_identifier(source.primary_key_column)
        sql = (
            f"SELECT count(*) FROM {table} "
            f"WHERE ({timestamp_column}, {primary_key_column}) > ($1, $2) "
            f"AND ({timestamp_column}, {primary_key_column}) <= ($3, $4)"
        )
        count = await pool.fetchval(
            sql,
            start_exclusive.timestamp,
            start_exclusive.primary_key,
            end_inclusive.timestamp,
            end_inclusive.primary_key,
            timeout=self.postgres_timeout,
        )
        return int(count or 0)

    async def postgres_ranges(
        self,
        source: PostgresSource,
        start_exclusive: PostgresCursor,
        end_inclusive: PostgresCursor,
        max_ranges: int,
        batch_size: int,
    ) -> list[tuple[PostgresCursor, PostgresCursor, int]]:
        """Build bounded keyset ranges inside PostgreSQL.

        Only boundary rows are returned to the Dispatcher. Every resulting
        range is ordered, disjoint and contains no more than ``batch_size``
        rows, including when Ray capacity allows fewer tasks than the backlog
        would otherwise request.
        """

        if max_ranges < 1 or batch_size < 1:
            return []
        pool = await self._postgres_pool(source.dsn)
        table = _quote_qualified_identifier(source.table)
        timestamp_column = _quote_identifier(source.timestamp_column)
        primary_key_column = _quote_identifier(source.primary_key_column)
        row_limit = max_ranges * batch_size
        sql = f"""
            WITH windowed AS (
                SELECT
                    {timestamp_column} AS cursor_timestamp,
                    {primary_key_column} AS cursor_primary_key,
                    row_number() OVER (
                        ORDER BY {timestamp_column}, {primary_key_column}
                    ) AS row_number
                FROM {table}
                WHERE ({timestamp_column}, {primary_key_column}) > ($1, $2)
                  AND ({timestamp_column}, {primary_key_column}) <= ($3, $4)
                ORDER BY {timestamp_column}, {primary_key_column}
                LIMIT $5
            ), last_row AS (
                SELECT max(row_number) AS max_row_number FROM windowed
            )
            SELECT
                cursor_timestamp,
                cursor_primary_key,
                row_number
            FROM windowed, last_row
            WHERE mod(row_number, $6) = 0
               OR row_number = max_row_number
            ORDER BY row_number
        """
        rows = await pool.fetch(
            sql,
            start_exclusive.timestamp,
            start_exclusive.primary_key,
            end_inclusive.timestamp,
            end_inclusive.primary_key,
            row_limit,
            batch_size,
            timeout=self.postgres_timeout,
        )
        ranges: list[tuple[PostgresCursor, PostgresCursor, int]] = []
        range_start = start_exclusive
        previous_row_number = 0
        for row in rows:
            range_end = PostgresCursor(
                row["cursor_timestamp"], row["cursor_primary_key"]
            )
            row_number = int(row["row_number"])
            ranges.append(
                (range_start, range_end, row_number - previous_row_number)
            )
            range_start = range_end
            previous_row_number = row_number
        return ranges

    async def _postgres_pool(self, dsn: str) -> Any:
        pool = self._postgres_pools.get(dsn)
        if pool is not None:
            return pool
        try:
            import asyncpg
        except ImportError as exc:
            raise SourceDependencyError(
                "Postgres observation requires: pip install asyncpg"
            ) from exc
        pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=self.postgres_pool_size,
            command_timeout=self.postgres_timeout,
        )
        self._postgres_pools[dsn] = pool
        return pool

    async def close(self) -> None:
        with self._kafka_lock:
            consumers = list(self._kafka_consumers.values())
            self._kafka_consumers.clear()
        for consumer in consumers:
            await asyncio.to_thread(consumer.close)
        pools = list(self._postgres_pools.values())
        self._postgres_pools.clear()
        await asyncio.gather(*(pool.close() for pool in pools))


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe PostgreSQL identifier: {value!r}")
    return f'"{value}"'


def _quote_qualified_identifier(value: str) -> str:
    parts = value.split(".")
    if len(parts) not in (1, 2):
        raise ValueError(f"expected table or schema.table, got {value!r}")
    return ".".join(_quote_identifier(part) for part in parts)


__all__ = [
    "EmptyPostgresSource",
    "SourceDependencyError",
    "SourceObserver",
]
