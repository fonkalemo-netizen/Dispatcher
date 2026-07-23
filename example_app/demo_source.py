"""A deterministic source observer for the runnable local Ray example."""

from __future__ import annotations

from typing import Mapping

from ray_dispatcher import KafkaSource


class DemoSourceObserver:
    """Expose 25 synthetic Kafka offsets without requiring a broker."""

    def __init__(self, high_offset: int = 25) -> None:
        self.high_offset = high_offset
        self.kafka_queries = 0

    async def kafka_watermarks(
        self, source: KafkaSource
    ) -> Mapping[int, tuple[int, int]]:
        self.kafka_queries += 1
        return {0: (0, self.high_offset)}
