"""Lightweight profiling primitives for agent-memory smoke workflows."""

from __future__ import annotations

import math
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Mapping


VALID_PROFILE_OUTPUT_MODES = frozenset({"artifact", "stdout", "both"})


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if p < 0.0 or p > 1.0:
        raise ValueError("percentile p must be within [0, 1]")
    ordered = sorted(values)
    index = int(math.ceil(p * len(ordered)) - 1)
    if index < 0:
        index = 0
    if index >= len(ordered):
        index = len(ordered) - 1
    return float(ordered[index])


@dataclass
class _StageStat:
    """Mutable stage-level counters."""

    count: int = 0
    total_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0

    def record(self, *, elapsed_ms: float) -> None:
        self.count += 1
        self.total_ms += elapsed_ms
        if self.count == 1:
            self.min_ms = elapsed_ms
            self.max_ms = elapsed_ms
            return
        self.min_ms = min(self.min_ms, elapsed_ms)
        self.max_ms = max(self.max_ms, elapsed_ms)

    def to_dict(self) -> Mapping[str, float | int]:
        avg_ms = (self.total_ms / self.count) if self.count > 0 else 0.0
        return {
            "count": self.count,
            "total_ms": round(self.total_ms, 3),
            "avg_ms": round(avg_ms, 3),
            "min_ms": round(self.min_ms, 3),
            "max_ms": round(self.max_ms, 3),
        }


@dataclass
class _LLMStat:
    """Mutable llm-call aggregate counters."""

    calls: int = 0
    retries: int = 0
    timeout_errors: int = 0
    payload_errors: int = 0
    other_errors: int = 0
    latencies_ms: list[float] | None = None

    def __post_init__(self) -> None:
        if self.latencies_ms is None:
            self.latencies_ms = []

    def record_success(self, *, latency_ms: float, attempts: int) -> None:
        self.calls += 1
        self.latencies_ms.append(latency_ms)
        self.retries += max(0, int(attempts) - 1)

    def record_error(self, *, error_type: str) -> None:
        if error_type == "timeout":
            self.timeout_errors += 1
            return
        if error_type == "payload":
            self.payload_errors += 1
            return
        self.other_errors += 1

    def to_dict(self) -> Mapping[str, float | int]:
        count = len(self.latencies_ms)
        total_ms = float(sum(self.latencies_ms))
        avg_ms = (total_ms / count) if count > 0 else 0.0
        p95_ms = _percentile(self.latencies_ms, 0.95)
        return {
            "calls": self.calls,
            "retries": self.retries,
            "timeout_errors": self.timeout_errors,
            "payload_errors": self.payload_errors,
            "other_errors": self.other_errors,
            "total_ms": round(total_ms, 3),
            "avg_ms": round(avg_ms, 3),
            "p95_ms": round(p95_ms, 3),
        }


class AgentMemoryProfiler:
    """Toggleable workflow profiler with stage + LLM aggregation."""

    def __init__(self, *, enabled: bool, output_mode: str) -> None:
        if output_mode not in VALID_PROFILE_OUTPUT_MODES:
            raise ValueError(
                "output_mode must be one of "
                f"{sorted(VALID_PROFILE_OUTPUT_MODES)!r}"
            )
        self.enabled = bool(enabled)
        self.output_mode = output_mode
        self._started_at_ms = time.perf_counter() * 1000.0
        self._stages: Dict[str, Dict[str, _StageStat]] = {}
        self._llm: Dict[str, _LLMStat] = {}

    @contextmanager
    def stage(self, *, workflow: str, stage: str) -> Iterator[None]:
        """Record one stage execution, no-op when disabled."""
        if not self.enabled:
            yield
            return
        start_ms = time.perf_counter() * 1000.0
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() * 1000.0) - start_ms
            workflow_row = self._stages.setdefault(workflow, {})
            stat = workflow_row.setdefault(stage, _StageStat())
            stat.record(elapsed_ms=elapsed_ms)

    def record_llm_success(
        self,
        *,
        workflow: str,
        step: str,
        latency_ms: float,
        attempts: int,
    ) -> None:
        """Aggregate one successful LLM call."""
        if not self.enabled:
            return
        key = f"{workflow}.{step}"
        stat = self._llm.setdefault(key, _LLMStat())
        stat.record_success(
            latency_ms=float(latency_ms),
            attempts=int(attempts),
        )

    def record_llm_error(
        self,
        *,
        workflow: str,
        step: str,
        error_type: str,
    ) -> None:
        """Aggregate one failed LLM call."""
        if not self.enabled:
            return
        key = f"{workflow}.{step}"
        stat = self._llm.setdefault(key, _LLMStat())
        stat.record_error(error_type=error_type)

    def should_emit_artifact(self) -> bool:
        """Whether profiler output should be written into artifact files."""
        if not self.enabled:
            return False
        return self.output_mode in {"artifact", "both"}

    def should_emit_stdout(self) -> bool:
        """Whether profiler output should be printed to stdout."""
        if not self.enabled:
            return False
        return self.output_mode in {"stdout", "both"}

    def build_output(self) -> Mapping[str, Any]:
        """Render final immutable profiling payload."""
        if not self.enabled:
            return {}
        stages = {
            workflow: {
                stage: stat.to_dict()
                for stage, stat in sorted(stage_rows.items())
            }
            for workflow, stage_rows in sorted(self._stages.items())
        }
        llm = {
            key: stat.to_dict()
            for key, stat in sorted(self._llm.items())
        }
        total_ms = (time.perf_counter() * 1000.0) - self._started_at_ms
        return {
            "stage_aggregate": stages,
            "llm_aggregate": llm,
            "total_ms": round(total_ms, 3),
        }
