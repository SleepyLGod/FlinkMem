"""Unit tests for agent-memory profiling primitives."""

from __future__ import annotations

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import os  # noqa: E401,E402
import pathlib  # noqa: E401,E402

import pyflink as _pf  # noqa: E401,E402

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

import pytest

from pyflink.semantic_runtime.runtime.workflows.agent_memory.runtime.profiling import (
    AgentMemoryProfiler,
)


def test_profiler_rejects_invalid_output_mode() -> None:
    with pytest.raises(ValueError):
        AgentMemoryProfiler(enabled=True, output_mode="invalid")


def test_profiler_disabled_is_noop() -> None:
    profiler = AgentMemoryProfiler(enabled=False, output_mode="artifact")
    with profiler.stage(workflow="mem0", stage="basic_add_event"):
        pass
    profiler.record_llm_success(
        workflow="mem0.basic",
        step="extract_facts",
        latency_ms=10.0,
        attempts=1,
    )
    profiler.record_llm_error(
        workflow="mem0.basic",
        step="extract_facts",
        error_type="timeout",
    )

    assert profiler.build_output() == {}
    assert profiler.should_emit_artifact() is False
    assert profiler.should_emit_stdout() is False


def test_profiler_records_stage_aggregate() -> None:
    profiler = AgentMemoryProfiler(enabled=True, output_mode="artifact")

    with profiler.stage(workflow="mem0", stage="basic_add_event"):
        pass
    with profiler.stage(workflow="mem0", stage="basic_add_event"):
        pass

    payload = profiler.build_output()
    stages = payload["stage_aggregate"]
    assert "mem0" in stages
    stat = stages["mem0"]["basic_add_event"]
    assert stat["count"] == 2
    assert stat["total_ms"] >= 0.0
    assert stat["max_ms"] >= stat["min_ms"]
    assert stat["avg_ms"] >= 0.0


def test_profiler_records_llm_aggregate() -> None:
    profiler = AgentMemoryProfiler(enabled=True, output_mode="both")

    profiler.record_llm_success(
        workflow="zep",
        step="extract_edges",
        latency_ms=100.0,
        attempts=3,
    )
    profiler.record_llm_success(
        workflow="zep",
        step="extract_edges",
        latency_ms=200.0,
        attempts=1,
    )
    profiler.record_llm_error(
        workflow="zep",
        step="extract_edges",
        error_type="timeout",
    )
    profiler.record_llm_error(
        workflow="zep",
        step="extract_edges",
        error_type="payload",
    )
    profiler.record_llm_error(
        workflow="zep",
        step="extract_edges",
        error_type="other",
    )

    payload = profiler.build_output()
    llm = payload["llm_aggregate"]["zep.extract_edges"]
    assert llm["calls"] == 2
    assert llm["retries"] == 2
    assert llm["timeout_errors"] == 1
    assert llm["payload_errors"] == 1
    assert llm["other_errors"] == 1
    assert llm["total_ms"] == 300.0
    assert llm["avg_ms"] == 150.0
    assert llm["p95_ms"] == 200.0
    assert profiler.should_emit_artifact() is True
    assert profiler.should_emit_stdout() is True
