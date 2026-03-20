#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""End-to-end integration checks for row-style semantic operators.

These checks validate the full call chain under strict fail-fast semantics.
The pipeline is:

source -> sem_map -> sem_filter -> sink

Scenarios:
1. Normal execution succeeds with valid structured outputs.
2. Flink timeout propagates as job failure.
3. Invalid model JSON propagates as job failure.
4. Sustained load succeeds without silent drops.
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from typing import Any, Dict, List

import os
import pathlib
import pyflink as _pf  # noqa: E401

from pyflink.common import Time, Types
from pyflink.datastream import AsyncDataStream, StreamExecutionEnvironment

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.sem_filter import SemFilterFunction
from pyflink.semantic_runtime.operators.sem_map import SemMapFunction

_SEM_RUNTIME_SRC = pathlib.Path(__file__).resolve().parents[1]
_SEM_RUNTIME_DST = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _SEM_RUNTIME_DST.exists():
    os.symlink(_SEM_RUNTIME_SRC, _SEM_RUNTIME_DST)


class SLOReport:
    """Collects latency and attempt metrics from successful output records."""

    def __init__(self, records: List[str]):
        self.raw = records
        self.parsed: List[Dict[str, Any]] = [json.loads(r) for r in records]
        self.total = len(records)
        self.latencies: List[float] = []
        self.attempts: List[int] = []

        for obj in self.parsed:
            metrics = obj.get("_metrics", {})
            if metrics.get("latency_ms"):
                self.latencies.append(float(metrics["latency_ms"]))
            if metrics.get("attempts"):
                self.attempts.append(int(metrics["attempts"]))

    @property
    def p95_latency_ms(self) -> float:
        """Return the p95 latency across successful records."""
        if not self.latencies:
            return 0.0
        ordered = sorted(self.latencies)
        index = int(len(ordered) * 0.95)
        return ordered[min(index, len(ordered) - 1)]

    @property
    def avg_attempts(self) -> float:
        """Return average LLM call attempts."""
        return statistics.mean(self.attempts) if self.attempts else 0.0

    @property
    def max_attempts(self) -> int:
        """Return maximum LLM call attempts."""
        return max(self.attempts) if self.attempts else 0


def collect_results(env: StreamExecutionEnvironment, result_stream, job_name: str) -> List[str]:
    """Execute the stream job and collect all output records."""
    results: List[str] = []
    with result_stream.execute_and_collect(job_name) as iterator:
        for row in iterator:
            results.append(str(row))
    return results


def assert_job_fails(env: StreamExecutionEnvironment, result_stream, job_name: str) -> None:
    """Assert that the stream job fails during execution."""
    try:
        collect_results(env, result_stream, job_name)
    except Exception:
        return
    raise AssertionError(f"Expected job {job_name!r} to fail")


def test_normal_pipeline() -> None:
    """Normal path should succeed with schema-valid outputs."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["great product", "amazing service", "wonderful experience"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.95})
    map_cfg = LLMClientConfig(backend="mock", mock_delay_s=0.05, mock_response=map_resp)
    map_fn = SemMapFunction(
        "Classify: {input}",
        {"sentiment": str, "confidence": float},
        map_cfg,
    )
    mapped = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(10), 5, Types.STRING())

    filter_resp = json.dumps(
        {"decision": True, "confidence": 0.9, "reason": "positive sentiment"}
    )
    filter_cfg = LLMClientConfig(
        backend="mock",
        mock_delay_s=0.05,
        mock_response=filter_resp,
    )
    filter_fn = SemFilterFunction("Keep positive? {input}", filter_cfg)
    filtered = AsyncDataStream.unordered_wait(
        mapped,
        filter_fn,
        Time.seconds(10),
        5,
        Types.STRING(),
    )

    kept = filtered.filter(lambda x: json.loads(x).get("decision", False))
    results = collect_results(env, kept, "test_normal_pipeline")
    report = SLOReport(results)

    assert report.total == len(inputs)
    for obj in report.parsed:
        assert "decision" in obj
        assert "confidence" in obj
        assert "reason" in obj


def test_timeout_pipeline_fails() -> None:
    """Timeouts should fail the job instead of emitting fallback records."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["item1", "item2"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.9})
    map_cfg = LLMClientConfig(backend="mock", mock_delay_s=10.0, mock_response=map_resp)
    map_fn = SemMapFunction(
        "Classify: {input}",
        {"sentiment": str, "confidence": float},
        map_cfg,
    )
    result = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(1), 2, Types.STRING())
    assert_job_fails(env, result, "test_timeout_pipeline_fails")


def test_invalid_json_pipeline_fails() -> None:
    """Invalid model JSON should fail the job instead of emitting fallback records."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["item1", "item2", "item3"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    map_cfg = LLMClientConfig(
        backend="mock",
        mock_delay_s=0.05,
        mock_response="this is not json at all",
    )
    map_fn = SemMapFunction(
        "Classify: {input}",
        {"sentiment": str, "confidence": float},
        map_cfg,
    )
    result = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(10), 5, Types.STRING())
    assert_job_fails(env, result, "test_invalid_json_pipeline_fails")


def test_backpressure_pipeline() -> None:
    """Sustained load should preserve all records without silent drops."""
    n_records = int(os.environ.get("BACKPRESSURE_RECORDS", "200"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = [f"record_{i}" for i in range(n_records)]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.9})
    map_cfg = LLMClientConfig(backend="mock", mock_delay_s=2.0, mock_response=map_resp)
    map_fn = SemMapFunction(
        "Classify: {input}",
        {"sentiment": str, "confidence": float},
        map_cfg,
    )
    result = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(30), 10, Types.STRING())

    start = time.time()
    results = collect_results(env, result, "test_backpressure_pipeline")
    elapsed = time.time() - start
    report = SLOReport(results)

    assert report.total == n_records
    assert elapsed >= 0.0


TESTS = {
    "normal": test_normal_pipeline,
    "timeout": test_timeout_pipeline_fails,
    "invalid_json": test_invalid_json_pipeline_fails,
    "backpressure": test_backpressure_pipeline,
}


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which == "all":
        passed = 0
        failed = 0
        for name, fn in TESTS.items():
            try:
                fn()
                passed += 1
            except Exception as exc:
                print(f"  ✗ {name}: {exc}")
                failed += 1
        print(f"\n{passed}/{passed + failed} E2E tests passed")
        if failed:
            sys.exit(1)
    elif which in TESTS:
        TESTS[which]()
    else:
        print(f"Unknown: {which}. Options: {list(TESTS.keys())} or 'all'")
        sys.exit(1)
