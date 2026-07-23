"""Trigger decision engine and dispatcher runtime knobs.

``TriggerPolicy`` owns *whether* to schedule (condition chain).
``DispatcherConfig`` owns non-decision knobs (capacity, fetch, windows, EWMA).
Handlers never configure trigger strategy; the host system injects both.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Mapping, Protocol, Sequence

from ray_dispatcher.models import (
    HandlerSpec,
    MultiSourceWindowState,
    SourceState,
)


class ConditionVerdict(str, Enum):
    PASS = "pass"
    FAIL_SKIP = "fail_skip"
    FAIL_BLOCK = "fail_block"
    FAIL_SOFT = "fail_soft"


class SoftAction(str, Enum):
    NONE = "none"
    URGENT = "urgent"
    THROTTLE = "throttle"
    ALERT_ONLY = "alert_only"


class Decision(str, Enum):
    TRIGGER = "trigger"
    DEGRADE = "degrade"
    SKIP = "skip"
    BLOCK = "block"


@dataclass
class DispatcherConfig:
    """Non-decision runtime knobs formerly on SchedulingPolicy."""

    max_in_flight: int = 64
    ewma_alpha: float = 0.3
    fetch_cpus: float = 0.25
    fetch_max_retries: int = 2
    max_window_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_in_flight < 1:
            raise ValueError("max_in_flight must be positive")
        if not 0.0 < self.ewma_alpha <= 1.0:
            raise ValueError("ewma_alpha must be in (0, 1]")
        if self.fetch_cpus <= 0 or self.fetch_max_retries < 0:
            raise ValueError("fetch_cpus must be positive and fetch_max_retries non-negative")
        if self.max_window_seconds <= 0:
            raise ValueError("max_window_seconds must be positive")


@dataclass(frozen=True)
class TriggerContext:
    """Inputs for one condition-chain evaluation."""

    handler: HandlerSpec
    source_state: SourceState | None
    free_slots: int
    available_cpus: float | None
    now: float
    config: DispatcherConfig
    window: MultiSourceWindowState | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None
    # Minimum slots one slice needs (fetch + N handlers); used by HasCapacity.
    slots_per_slice: int = 1
    phase_cpus: float = 1.0


@dataclass(frozen=True)
class ConditionResult:
    name: str
    verdict: ConditionVerdict
    reason: str
    soft_action: SoftAction = SoftAction.NONE
    concurrency_factor: float | None = None
    metrics: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class TriggerDecision:
    decision: Decision
    soft_action: SoftAction
    results: tuple[ConditionResult, ...]
    concurrency_factor: float | None = None

    def to_event_payload(
        self,
        *,
        source_key: str | None = None,
        group_key: str | None = None,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "decision": self.decision.value,
            "soft_action": self.soft_action.value,
            "conditions": [
                {
                    "name": result.name,
                    "verdict": result.verdict.value,
                    "reason": result.reason,
                    "soft_action": result.soft_action.value,
                }
                for result in self.results
            ],
        }
        if source_key is not None:
            payload["source_key"] = source_key
        if group_key is not None:
            payload["group_key"] = group_key
        if self.concurrency_factor is not None:
            payload["concurrency_factor"] = self.concurrency_factor
        return payload


class TriggerCondition(Protocol):
    """One pluggable check in the trigger chain."""

    name: str
    required: bool

    def evaluate(self, ctx: TriggerContext) -> ConditionResult: ...


@dataclass(frozen=True)
class HasBacklog:
    """Require positive backlog on the source state."""

    name: str = "has_backlog"
    required: bool = True

    def evaluate(self, ctx: TriggerContext) -> ConditionResult:
        state = ctx.source_state
        if state is None:
            return ConditionResult(
                self.name, ConditionVerdict.FAIL_SKIP, "no source state"
            )
        if state.backlog > 0:
            return ConditionResult(
                self.name,
                ConditionVerdict.PASS,
                f"backlog={state.backlog}",
                metrics={"backlog": float(state.backlog)},
            )
        return ConditionResult(
            self.name, ConditionVerdict.FAIL_SKIP, "backlog=0", metrics={"backlog": 0.0}
        )


@dataclass(frozen=True)
class SourceIdle:
    """Require no active batch and no retention gap."""

    name: str = "source_idle"
    required: bool = True

    def evaluate(self, ctx: TriggerContext) -> ConditionResult:
        state = ctx.source_state
        if state is None:
            return ConditionResult(
                self.name, ConditionVerdict.FAIL_SKIP, "no source state"
            )
        if state.active_batch_id is not None:
            return ConditionResult(
                self.name,
                ConditionVerdict.FAIL_SKIP,
                f"active_batch_id={state.active_batch_id}",
            )
        if state.retention_gap:
            return ConditionResult(
                self.name,
                ConditionVerdict.FAIL_SKIP,
                f"retention_gap={state.retention_gap}",
            )
        return ConditionResult(self.name, ConditionVerdict.PASS, "source idle")


@dataclass(frozen=True)
class HasTimeWindow:
    """Require a non-empty multi-source time window and idle window state."""

    name: str = "has_time_window"
    required: bool = True

    def evaluate(self, ctx: TriggerContext) -> ConditionResult:
        window = ctx.window
        if window is None:
            return ConditionResult(
                self.name, ConditionVerdict.FAIL_SKIP, "no multi-source window"
            )
        if window.active_batch_id is not None:
            return ConditionResult(
                self.name,
                ConditionVerdict.FAIL_SKIP,
                f"active_batch_id={window.active_batch_id}",
            )
        if ctx.window_start is None or ctx.window_end is None:
            return ConditionResult(
                self.name, ConditionVerdict.FAIL_SKIP, "window bounds unset"
            )
        if ctx.window_end <= ctx.window_start:
            return ConditionResult(
                self.name, ConditionVerdict.FAIL_SKIP, "empty time window"
            )
        return ConditionResult(
            self.name,
            ConditionVerdict.PASS,
            f"window=[{ctx.window_start.isoformat()}, {ctx.window_end.isoformat()})",
        )


@dataclass(frozen=True)
class HasCapacity:
    """Require global in-flight slots and (when known) CPU headroom."""

    name: str = "has_capacity"
    required: bool = True

    def evaluate(self, ctx: TriggerContext) -> ConditionResult:
        if ctx.free_slots < ctx.slots_per_slice:
            return ConditionResult(
                self.name,
                ConditionVerdict.FAIL_BLOCK,
                f"free_slots={ctx.free_slots} < slots_per_slice={ctx.slots_per_slice}",
                metrics={
                    "free_slots": float(ctx.free_slots),
                    "slots_per_slice": float(ctx.slots_per_slice),
                },
            )
        if ctx.available_cpus is not None and ctx.available_cpus < ctx.phase_cpus:
            return ConditionResult(
                self.name,
                ConditionVerdict.FAIL_BLOCK,
                f"available_cpus={ctx.available_cpus} < phase_cpus={ctx.phase_cpus}",
                metrics={
                    "available_cpus": float(ctx.available_cpus),
                    "phase_cpus": float(ctx.phase_cpus),
                },
            )
        return ConditionResult(
            self.name,
            ConditionVerdict.PASS,
            f"free_slots={ctx.free_slots}",
            metrics={"free_slots": float(ctx.free_slots)},
        )


@dataclass(frozen=True)
class TargetReached:
    """Require backlog to reach ``min_items`` before triggering."""

    min_items: int = 1
    name: str = "target_reached"
    required: bool = True

    def __post_init__(self) -> None:
        if self.min_items < 1:
            raise ValueError("min_items must be positive")

    def evaluate(self, ctx: TriggerContext) -> ConditionResult:
        state = ctx.source_state
        if state is None:
            return ConditionResult(
                self.name, ConditionVerdict.FAIL_SKIP, "no source state"
            )
        if state.backlog >= self.min_items:
            return ConditionResult(
                self.name,
                ConditionVerdict.PASS,
                f"backlog={state.backlog} >= min_items={self.min_items}",
                metrics={
                    "backlog": float(state.backlog),
                    "min_items": float(self.min_items),
                },
            )
        return ConditionResult(
            self.name,
            ConditionVerdict.FAIL_SKIP,
            f"backlog={state.backlog} < min_items={self.min_items}",
            metrics={
                "backlog": float(state.backlog),
                "min_items": float(self.min_items),
            },
        )


@dataclass(frozen=True)
class BacklogPressure:
    """Soft pressure check; failure suggests URGENT (or configured) degrade."""

    max_backlog: int | None = None
    max_backlog_seconds: float | None = None
    on_fail: SoftAction = SoftAction.URGENT
    name: str = "backlog_pressure"
    required: bool = False

    def __post_init__(self) -> None:
        if self.max_backlog is None and self.max_backlog_seconds is None:
            raise ValueError("set max_backlog and/or max_backlog_seconds")
        if self.max_backlog is not None and self.max_backlog < 1:
            raise ValueError("max_backlog must be positive")
        if self.max_backlog_seconds is not None and self.max_backlog_seconds <= 0:
            raise ValueError("max_backlog_seconds must be positive")
        if self.on_fail is SoftAction.NONE:
            raise ValueError("on_fail must be a soft action")

    def evaluate(self, ctx: TriggerContext) -> ConditionResult:
        state = ctx.source_state
        if state is None:
            return ConditionResult(
                self.name, ConditionVerdict.PASS, "no source state; treating as ok"
            )
        throughput = max(state.processing_rate, 1e-6)
        backlog_seconds = (
            state.backlog / throughput if state.processing_rate else float(state.backlog)
        )
        metrics = {
            "backlog": float(state.backlog),
            "backlog_seconds": float(backlog_seconds),
        }
        reasons: list[str] = []
        if self.max_backlog is not None and state.backlog > self.max_backlog:
            reasons.append(f"backlog={state.backlog} > max_backlog={self.max_backlog}")
        if (
            self.max_backlog_seconds is not None
            and backlog_seconds > self.max_backlog_seconds
        ):
            reasons.append(
                f"backlog_seconds={backlog_seconds:.3f} > "
                f"max_backlog_seconds={self.max_backlog_seconds}"
            )
        if reasons:
            return ConditionResult(
                self.name,
                ConditionVerdict.FAIL_SOFT,
                "; ".join(reasons),
                soft_action=self.on_fail,
                metrics=metrics,
            )
        return ConditionResult(
            self.name, ConditionVerdict.PASS, "backlog pressure ok", metrics=metrics
        )


def default_rank_key(
    worker: HandlerSpec, state: SourceState, now: float
) -> tuple[int, int, float, float]:
    """Lexicographic schedule key; higher sorts first with ``reverse=True``.

    Order: static handler priority → largest backlog → slowest drain
    (highest backlog/processing_rate) → longest wait.
    """

    throughput = max(state.processing_rate, 1e-6)
    backlog_seconds = (
        state.backlog / throughput if state.processing_rate else float(state.backlog)
    )
    age = max(0.0, now - state.last_scheduled_at)
    return (worker.priority, state.backlog, backlog_seconds, age)


@dataclass
class TriggerPolicy:
    """Ordered condition chain plus candidate ranking."""

    conditions: Sequence[TriggerCondition]
    # Optional alternate chain for multi-source window scheduling.
    multisource_conditions: Sequence[TriggerCondition] | None = None

    def __post_init__(self) -> None:
        if not self.conditions:
            raise ValueError("conditions must be non-empty")

    @classmethod
    def default(cls) -> "TriggerPolicy":
        """Chain matching legacy backlog + idle + capacity behavior."""

        return cls(
            conditions=(HasBacklog(), SourceIdle(), HasCapacity()),
            multisource_conditions=(HasTimeWindow(), HasCapacity()),
        )

    def rank_key(
        self, worker: HandlerSpec, state: SourceState, now: float
    ) -> tuple[int, int, float, float]:
        return default_rank_key(worker, state, now)

    def evaluate(
        self, ctx: TriggerContext, *, multisource: bool = False
    ) -> TriggerDecision:
        chain = (
            self.multisource_conditions
            if multisource and self.multisource_conditions is not None
            else self.conditions
        )
        results: list[ConditionResult] = []
        soft_action = SoftAction.NONE
        concurrency_factor: float | None = None
        soft_failed = False

        for condition in chain:
            result = condition.evaluate(ctx)
            results.append(result)
            if result.verdict is ConditionVerdict.PASS:
                continue
            if result.verdict is ConditionVerdict.FAIL_SKIP:
                return TriggerDecision(
                    Decision.SKIP, SoftAction.NONE, tuple(results)
                )
            if result.verdict is ConditionVerdict.FAIL_BLOCK:
                return TriggerDecision(
                    Decision.BLOCK, SoftAction.NONE, tuple(results)
                )
            if result.verdict is ConditionVerdict.FAIL_SOFT:
                soft_failed = True
                if result.soft_action is not SoftAction.NONE:
                    soft_action = result.soft_action
                if result.concurrency_factor is not None:
                    concurrency_factor = result.concurrency_factor
                if condition.required:
                    # Required + soft is unusual; treat as skip for safety.
                    return TriggerDecision(
                        Decision.SKIP, soft_action, tuple(results), concurrency_factor
                    )
                continue
            raise ValueError(f"unknown verdict {result.verdict!r}")

        if soft_failed:
            if soft_action is SoftAction.THROTTLE and concurrency_factor is None:
                concurrency_factor = 0.5
            return TriggerDecision(
                Decision.DEGRADE, soft_action, tuple(results), concurrency_factor
            )
        return TriggerDecision(
            Decision.TRIGGER, SoftAction.NONE, tuple(results), concurrency_factor
        )


def window_bounds(
    window: MultiSourceWindowState,
    *,
    max_window_seconds: float,
) -> tuple[datetime | None, datetime | None]:
    """Compute ``[t_left, t_right)`` for a multi-source window, or unset bounds."""

    if window.committed_time is None or window.observed_time is None:
        return None, None
    t_left = window.committed_time
    t_right = min(
        window.observed_time,
        t_left + timedelta(seconds=max_window_seconds),
    )
    return t_left, t_right


__all__ = [
    "BacklogPressure",
    "ConditionResult",
    "ConditionVerdict",
    "Decision",
    "DispatcherConfig",
    "HasBacklog",
    "HasCapacity",
    "HasTimeWindow",
    "SoftAction",
    "SourceIdle",
    "TargetReached",
    "TriggerCondition",
    "TriggerContext",
    "TriggerDecision",
    "TriggerPolicy",
    "default_rank_key",
    "window_bounds",
]
