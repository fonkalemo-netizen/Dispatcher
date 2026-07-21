"""Scheduling policy for progressive backlog-aware dispatch."""

from __future__ import annotations

import math
from dataclasses import dataclass

from ray_dispatcher.models import SourceState, WorkerSpec

@dataclass
class SchedulingPolicy:
    """Progressive backlog-aware policy bounded by capacity checks."""

    max_in_flight: int = 64
    target_batch_seconds: float = 10.0
    ewma_alpha: float = 0.3

    def task_count(
        self,
        worker: WorkerSpec,
        backlog: int,
        free_slots: int,
        available_cpus: float | None,
    ) -> int:
        count = min(worker.max_parallelism, math.ceil(backlog / worker.batch_size), free_slots)
        if available_cpus is not None:
            count = min(count, math.floor(available_cpus / worker.cpus_per_task))
        return max(0, count)

    def priority(
        self, worker: WorkerSpec, state: "SourceState", now: float
    ) -> tuple[int, int, float, float]:
        """Lexicographic schedule key; higher sorts first with ``reverse=True``.

        Order: static handler priority → largest backlog → slowest drain
        (highest backlog/processing_rate) → longest wait. Capacity (slots,
        CPU, output limits) is checked after ranking, not folded into the key.
        """

        throughput = max(state.processing_rate, 1e-6)
        backlog_seconds = (
            state.backlog / throughput if state.processing_rate else float(state.backlog)
        )
        age = max(0.0, now - state.last_scheduled_at)
        return (worker.priority, state.backlog, backlog_seconds, age)
