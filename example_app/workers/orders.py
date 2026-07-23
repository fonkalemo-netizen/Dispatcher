"""Two thin handlers that auto-share one colocated Kafka source."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


OUTPUT_ROOT = Path(
    os.environ.get(
        "RAY_DISPATCHER_DEMO_OUTPUT",
        str(Path(__file__).resolve().parents[1] / "demo_output"),
    )
)

SOURCES = {
    "demo-orders": {
        "kind": "kafka",
        "connection_id": "demo-kafka",
        "brokers": ["demo-kafka:9092"],
        "topic": "orders",
        "initial_offset": "earliest",
    }
}

# static is for small inline config only; large dims use file (or a future DB snapshot kind).
RESOURCES = {
    "user-dim-v1": {
        "kind": "static",
        "data": {offset: {"name": f"user-{offset}"} for offset in range(100)},
    }
}

HANDLERS = [
    {
        # Binding
        "entrypoint": "orders_to_jsonl",
        "sources": ["demo-orders"],
        "resources": ["user-dim-v1"],
        # Write-side passthrough (opaque to the dispatcher)
        "output": {
            "path": str(OUTPUT_ROOT / "jsonl"),
        },
        # Scheduling knobs (explicit for discoverability)
        "batch_size": 10,
        "cpus_per_task": 1,
        "max_retries": 2,
        "priority": 0,
    },
    {
        # Binding
        "entrypoint": "orders_to_csv",
        "sources": ["demo-orders"],
        # Write-side passthrough
        "output": {
            "path": str(OUTPUT_ROOT / "csv"),
        },
        # Scheduling knobs
        "batch_size": 5,
        "cpus_per_task": 1,
        "max_retries": 2,
        "priority": 0,
    },
]


def _atomic_write(target: Path, content: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, target)


def orders_to_jsonl(
    request: Any,
    records: list[dict[str, Any]],
    resources: dict[str, Any],
) -> dict[str, Any]:
    # Imports stay inside the function so drop-in workers survive Ray serialization.
    import json
    from pathlib import Path

    user_dim = resources.get("user-dim-v1") or {}
    enriched: list[dict[str, Any]] = []
    for row in records:
        item = dict(row)
        user_id = item.get("user_id")
        if user_id in user_dim:
            item["user"] = user_dim[user_id]
        enriched.append(item)

    output_dir = Path(request.output["path"])
    target = output_dir / f"{request.dispatch_id}.jsonl"
    content = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in enriched)
    _atomic_write(target, content)
    return {
        "handler": request.handler_id,
        "count": len(enriched),
        "path": str(target),
    }


def orders_to_csv(request: Any, records: list[dict[str, Any]]) -> dict[str, Any]:
    import csv
    import io
    from pathlib import Path

    output_dir = Path(request.output["path"])
    target = output_dir / f"{request.dispatch_id}.csv"
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=["order_id", "user_id", "amount"],
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in records:
        writer.writerow(row)
    _atomic_write(target, buffer.getvalue())
    return {
        "handler": request.handler_id,
        "count": len(records),
        "path": str(target),
    }
