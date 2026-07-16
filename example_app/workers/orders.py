"""Two independently scheduled handlers reading one logical input source."""

from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
from typing import Any


OUTPUT_ROOT = Path(
    os.environ.get(
        "RAY_DISPATCHER_DEMO_OUTPUT",
        str(Path(__file__).resolve().parents[1] / "demo_output"),
    )
)


HANDLERS = [
    {
        "name": "orders_to_jsonl",
        "handler_id": "orders:orders_to_jsonl",
        "entrypoint": "orders_to_jsonl",
        "sources": [
            {
                "kind": "kafka",
                "source_id": "demo-orders",
                "connection_id": "demo-kafka",
                "brokers": ["demo-kafka:9092"],
                "topic": "orders",
                "initial_offset": "earliest",
            }
        ],
        "output": {
            "connection_id": "local-jsonl",
            "target": str(OUTPUT_ROOT / "jsonl"),
            "output_format": "jsonl",
            "max_parallelism": 3,
        },
        "batch_size": 10,
        "max_parallelism": 3,
        "cpus_per_task": 1,
        "shared_source_group": "orders-fanout",
        "data_fetcher": "fetch_orders",
        "fetcher_id": "orders-range-reader-v1",
    },
    {
        "name": "orders_to_csv",
        "handler_id": "orders:orders_to_csv",
        "entrypoint": "orders_to_csv",
        "sources": [
            {
                "kind": "kafka",
                "source_id": "demo-orders",
                "connection_id": "demo-kafka",
                "brokers": ["demo-kafka:9092"],
                "topic": "orders",
                "initial_offset": "earliest",
            }
        ],
        "output": {
            "connection_id": "local-csv",
            "target": str(OUTPUT_ROOT / "csv"),
            "output_format": "csv",
            "max_parallelism": 2,
        },
        "batch_size": 5,
        "max_parallelism": 4,
        "cpus_per_task": 1,
        "shared_source_group": "orders-fanout",
        "data_fetcher": "fetch_orders",
        "fetcher_id": "orders-range-reader-v1",
    },
]


def fetch_orders(request: Any) -> list[dict[str, Any]]:
    """Demo range reader; a real deployment consumes Kafka here once."""

    if request.start_offset is None or request.end_offset is None:
        raise ValueError("this example expects a Kafka offset request")
    return [
        {
            "order_id": offset,
            "topic": request.topic,
            "partition": request.partition,
            "amount": round(10.0 + offset * 1.25, 2),
            "dispatch_id": request.dispatch_id,
        }
        for offset in range(request.start_offset, request.end_offset)
    ]


def _atomic_write(target: Path, content: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, target)


def orders_to_jsonl(
    request: Any, records: list[dict[str, Any]]
) -> dict[str, Any]:
    output_dir = Path(request.output_target)
    target = output_dir / f"{request.dispatch_id}.jsonl"
    content = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in records)
    _atomic_write(target, content)
    return {
        "handler": request.handler_id,
        "processed": len(records),
        "output": str(target),
    }


def orders_to_csv(
    request: Any, records: list[dict[str, Any]]
) -> dict[str, Any]:
    output_dir = Path(request.output_target)
    target = output_dir / f"{request.dispatch_id}.csv"
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=("order_id", "topic", "partition", "amount", "dispatch_id"),
    )
    writer.writeheader()
    writer.writerows(records)
    _atomic_write(target, buffer.getvalue())
    return {
        "handler": request.handler_id,
        "processed": len(records),
        "output": str(target),
    }
