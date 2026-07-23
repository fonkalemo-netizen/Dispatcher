"""Demo payload reader used when no real Kafka broker is available."""

from __future__ import annotations

from typing import Any

from ray_dispatcher import DispatchRequest, KafkaSource, SourceSpec


class DemoPayloadReader:
    """Synthesize order row values for the assigned Kafka offset window."""

    def fetch(self, request: DispatchRequest, source: SourceSpec) -> list[dict[str, Any]]:
        if not isinstance(source, KafkaSource):
            raise TypeError("DemoPayloadReader expects a KafkaSource")
        if request.start_offset is None or request.end_offset is None:
            raise ValueError("this example expects a Kafka offset request")
        # Only business payload values — scheduling metadata stays on request.
        return [
            {
                "order_id": offset,
                "user_id": offset,
                "amount": round(10.0 + offset * 1.25, 2),
            }
            for offset in range(request.start_offset, request.end_offset)
        ]
