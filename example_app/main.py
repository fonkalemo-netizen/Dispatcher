"""Run the multi-handler RayDispatcher example on a local Ray cluster."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path


EXAMPLE_ROOT = Path(__file__).resolve().parent
PACKAGE_ROOT = EXAMPLE_ROOT.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

# Avoid Ray's optional `uv run` parent-process probe. This also lets the example
# start in restricted containers where enumerating all host processes is denied.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

try:  # noqa: E402
    import ray
except ImportError:  # The complete demo can still run through its local fallback.
    ray = None

from demo_source import DemoSourceClient  # noqa: E402
from local_backend import LocalThreadBackend  # noqa: E402
from ray_dispatcher import (  # noqa: E402
    NativeRayBackend,
    RayDispatcher,
    SchedulingPolicy,
    SQLiteCheckpointStore,
)


async def wait_until_complete(dispatcher: RayDispatcher, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        states = list(dispatcher.state.sources.values())
        batches = list(dispatcher.state.batches.values())
        if (
            states
            and batches
            and all(state.committed == state.observed for state in states)
            and all(state.active_batch_id is None for state in states)
        ):
            return
        if dispatcher.state.loop_errors:
            raise RuntimeError(dispatcher.state.loop_errors[-1])
        await asyncio.sleep(0.05)
    raise TimeoutError("RayDispatcher example did not complete in time")


def clear_previous_demo_state() -> None:
    for pattern in ("demo_output/jsonl/*.jsonl", "demo_output/csv/*.csv"):
        for path in EXAMPLE_ROOT.glob(pattern):
            path.unlink()
    checkpoint = EXAMPLE_ROOT / "demo_state/checkpoints.sqlite3"
    for suffix in ("", "-wal", "-shm"):
        path = Path(f"{checkpoint}{suffix}")
        if path.exists():
            path.unlink()


async def main() -> None:
    # Reset only so every demo invocation deterministically processes 25 rows.
    # A production process must keep this file across restarts.
    clear_previous_demo_state()
    local_backend = None
    if ray is not None:
        try:
            ray.init(
                address="local",
                num_cpus=4,
                include_dashboard=False,
                ignore_reinit_error=True,
            )
            backend = NativeRayBackend(ray)
            print("runtime backend: Ray")
        except Exception as exc:
            if os.environ.get("DEMO_REQUIRE_RAY") == "1":
                raise
            ray.shutdown()
            print(f"Ray unavailable ({type(exc).__name__}: {exc}); using thread fallback")
            local_backend = LocalThreadBackend(max_workers=4)
            backend = local_backend
    else:
        local_backend = LocalThreadBackend(max_workers=4)
        backend = local_backend
        print("Ray is not installed; using thread fallback")

    source_client = DemoSourceClient(high_offset=25)
    dispatcher = RayDispatcher.from_worker_directory(
        EXAMPLE_ROOT / "workers",
        source_client=source_client,
        ray_backend=backend,
        checkpoint_store=SQLiteCheckpointStore(
            EXAMPLE_ROOT / "demo_state/checkpoints.sqlite3"
        ),
        policy=SchedulingPolicy(max_in_flight=8),
        listener_interval=0.1,
        trigger_interval=0.05,
        status_interval=0.05,
    )

    await dispatcher.start()
    try:
        await wait_until_complete(dispatcher)
    finally:
        await dispatcher.stop()
        if local_backend is not None:
            local_backend.close()
        elif ray is not None:
            ray.shutdown()

    print(f"physical Kafka watermark queries: {source_client.kafka_queries}")
    print(
        "shared source fetches: "
        f"{sum(run.kind == 'fetch' for run in dispatcher.state.runs.values())}"
    )
    print(json.dumps(dispatcher.snapshot(), indent=2, ensure_ascii=False))
    print(f"JSONL files: {len(list((EXAMPLE_ROOT / 'demo_output/jsonl').glob('*.jsonl')))}")
    print(f"CSV files: {len(list((EXAMPLE_ROOT / 'demo_output/csv').glob('*.csv')))}")


if __name__ == "__main__":
    asyncio.run(main())
