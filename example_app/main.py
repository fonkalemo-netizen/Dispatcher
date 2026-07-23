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
# Allow running without `pip install -e .` from a checkout.
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

# Avoid Ray's optional `uv run` parent-process probe. This also lets the example
# start in restricted containers where enumerating all host processes is denied.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

try:  # noqa: E402
    import ray
except ImportError:  # The complete demo can still run through its local fallback.
    ray = None

from demo_reader import DemoPayloadReader  # noqa: E402
from demo_source import DemoSourceObserver  # noqa: E402
from local_adapter import LocalThreadAdapter  # noqa: E402
from ray_dispatcher import (  # noqa: E402
    DispatcherConfig,
    NativeRayAdapter,
    RayDispatcher,
    SQLiteCheckpointStore,
    SQLiteFailureStore,
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
    for stem in ("checkpoints.sqlite3", "failures.sqlite3"):
        base = EXAMPLE_ROOT / "demo_state" / stem
        for suffix in ("", "-wal", "-shm"):
            path = Path(f"{base}{suffix}")
            if path.exists():
                path.unlink()


async def main() -> None:
    # Reset only so every demo invocation deterministically processes 25 rows.
    # A production process must keep this file across restarts.
    clear_previous_demo_state()
    payload_reader = DemoPayloadReader()
    local_adapter = None
    if ray is not None:
        try:
            ray.init(
                address="local",
                num_cpus=4,
                include_dashboard=False,
                ignore_reinit_error=True,
            )
            backend = NativeRayAdapter(ray)
            print("runtime adapter: Ray")
        except Exception as exc:
            if os.environ.get("DEMO_REQUIRE_RAY") == "1":
                raise
            ray.shutdown()
            print(f"Ray unavailable ({type(exc).__name__}: {exc}); using thread fallback")
            local_adapter = LocalThreadAdapter(
                max_workers=4, payload_reader=payload_reader
            )
            backend = local_adapter
    else:
        local_adapter = LocalThreadAdapter(
            max_workers=4, payload_reader=payload_reader
        )
        backend = local_adapter
        print("Ray is not installed; using thread fallback")

    source_observer = DemoSourceObserver(high_offset=25)
    dispatcher = RayDispatcher(
        EXAMPLE_ROOT / "workers",
        ray_adapter=backend,
        checkpoint_store=SQLiteCheckpointStore(
            EXAMPLE_ROOT / "demo_state/checkpoints.sqlite3"
        ),
        failure_store=SQLiteFailureStore(
            EXAMPLE_ROOT / "demo_state/failures.sqlite3"
        ),
        config=DispatcherConfig(max_in_flight=8),
        listener_interval=0.1,
        trigger_interval=0.05,
        status_interval=0.05,
        payload_reader=payload_reader,
    )
    dispatcher.source_observer = source_observer

    try:
        async with dispatcher:
            await wait_until_complete(dispatcher)
    finally:
        if local_adapter is not None:
            local_adapter.close()
        elif ray is not None:
            ray.shutdown()

    print(f"physical Kafka watermark queries: {source_observer.kafka_queries}")
    print(
        "shared source fetches: "
        f"{sum(run.kind == 'fetch' for run in dispatcher.state.runs.values())}"
    )
    print(json.dumps(await dispatcher.snapshot(), indent=2, ensure_ascii=False))
    print(f"JSONL files: {len(list((EXAMPLE_ROOT / 'demo_output/jsonl').glob('*.jsonl')))}")
    print(f"CSV files: {len(list((EXAMPLE_ROOT / 'demo_output/csv').glob('*.csv')))}")


if __name__ == "__main__":
    asyncio.run(main())
