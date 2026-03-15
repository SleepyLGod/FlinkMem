#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""
End-to-End Integration Tests for CP semantic operators.

Pipeline: source → sem_map → sem_filter → filter → sink (MockLLMClient).

Test scenarios:
  1. Normal: verify output schema + no degraded records.
  2. Timeout: mock delay > Flink timeout, verify degraded fallback.
  3. Error: mock returns invalid JSON, verify parse failure handling.
  4. Retry: mock first N calls fail then succeed via Flink retry.
  5. Backpressure: (separate long-running test — see test_backpressure).

SLO metrics reported per test:
  - p95_e2e_latency_ms, timeout_rate, invalid_output_rate,
    avg_attempts, max_attempts, degraded_rate
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from typing import Any, Dict, List

# Bootstrap: ensure pyflink.semantic_runtime is importable via symlink
import os, pathlib, pyflink as _pf  # noqa: E401
_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)

from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment, AsyncDataStream

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.sem_map import SemMapFunction
from pyflink.semantic_runtime.operators.sem_filter import SemFilterFunction
from pyflink.semantic_runtime.retry import create_flink_retry_strategy


# ---------------------------------------------------------------------------
# SLO metrics collector
# ---------------------------------------------------------------------------

class SLOReport:
    """Collects and reports SLO metrics from test output records."""

    def __init__(self, records: List[str]):
        self.raw = records
        self.parsed: List[Dict[str, Any]] = []
        self.degraded_count = 0
        self.total = len(records)
        self.latencies: List[float] = []
        self.attempts: List[int] = []
        self.timeout_count = 0
        self.invalid_output_count = 0

        for r in records:
            try:
                obj = json.loads(r)
            except (json.JSONDecodeError, TypeError):
                self.degraded_count += 1
                continue
            self.parsed.append(obj)
            if obj.get("_degraded", False):
                self.degraded_count += 1
                err = str(obj.get("_error", ""))
                if "flink_timeout" in err:
                    self.timeout_count += 1
                if "json_parse_error" in err or "schema_validation" in err:
                    self.invalid_output_count += 1
            m = obj.get("_metrics", {})
            if m.get("latency_ms"):
                self.latencies.append(m["latency_ms"])
            if m.get("attempts"):
                self.attempts.append(m["attempts"])

    @property
    def p95_latency_ms(self) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        idx = int(len(s) * 0.95)
        return s[min(idx, len(s) - 1)]

    @property
    def timeout_rate(self) -> float:
        return self.timeout_count / max(self.total, 1)

    @property
    def invalid_output_rate(self) -> float:
        return self.invalid_output_count / max(self.total, 1)

    @property
    def degraded_rate(self) -> float:
        return self.degraded_count / max(self.total, 1)

    @property
    def avg_attempts(self) -> float:
        return statistics.mean(self.attempts) if self.attempts else 0.0

    @property
    def max_attempts(self) -> int:
        return max(self.attempts) if self.attempts else 0

    def print_report(self, test_name: str):
        print(f"\n--- SLO Report: {test_name} ---")
        print(f"  total_records:          {self.total}")
        print(f"  degraded_count:         {self.degraded_count}")
        print(f"  degraded_rate:          {self.degraded_rate:.2%}")
        print(f"  timeout_rate:           {self.timeout_rate:.2%}")
        print(f"  invalid_output_rate:    {self.invalid_output_rate:.2%}")
        print(f"  p95_e2e_latency_ms:     {self.p95_latency_ms:.1f}")
        print(f"  avg_attempts:           {self.avg_attempts:.2f}")
        print(f"  max_attempts:           {self.max_attempts}")
        print(f"---")


# ---------------------------------------------------------------------------
# Helper: collect results
# ---------------------------------------------------------------------------

def collect_results(env, result_stream, job_name: str) -> List[str]:
    """Execute and collect all output records as strings."""
    results = []
    with result_stream.execute_and_collect(job_name) as it:
        for row in it:
            results.append(str(row))
    return results


# ---------------------------------------------------------------------------
# Test 1: Normal path — full pipeline
# ---------------------------------------------------------------------------

def test_normal_pipeline():
    """source → sem_map → sem_filter → downstream filter → collect.

    All records should pass through with correct schema, zero degraded.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["great product", "amazing service", "wonderful experience"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    # sem_map: extract sentiment
    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.95})
    map_cfg = LLMClientConfig(backend="mock", mock_delay_s=0.05, mock_response=map_resp)
    map_fn = SemMapFunction("Classify: {input}",
                            {"sentiment": str, "confidence": float}, map_cfg)
    mapped = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(10), 5, Types.STRING())

    # sem_filter: decide keep/reject
    filter_resp = json.dumps({"decision": True, "confidence": 0.9, "reason": "positive sentiment"})
    filter_cfg = LLMClientConfig(backend="mock", mock_delay_s=0.05, mock_response=filter_resp)
    filter_fn = SemFilterFunction("Keep positive? {input}", filter_cfg)
    filtered = AsyncDataStream.unordered_wait(mapped, filter_fn, Time.seconds(10), 5, Types.STRING())

    # downstream filter: only keep decision=True
    kept = filtered.filter(lambda x: json.loads(x).get("decision", False))

    results = collect_results(env, kept, "test_normal_pipeline")
    report = SLOReport(results)
    report.print_report("normal_pipeline")

    # Assertions
    assert report.total == len(inputs), f"Expected {len(inputs)} records, got {report.total}"
    assert report.degraded_count == 0, f"Expected 0 degraded, got {report.degraded_count}"
    assert report.timeout_count == 0, f"Expected 0 timeouts, got {report.timeout_count}"
    for obj in report.parsed:
        assert "decision" in obj, f"Missing 'decision' in output: {obj}"
        assert "confidence" in obj, f"Missing 'confidence' in output: {obj}"
    print("  ✓ test_normal_pipeline PASSED")


# ---------------------------------------------------------------------------
# Test 2: Timeout path
# ---------------------------------------------------------------------------

def test_timeout_pipeline():
    """sem_map with mock delay >> Flink timeout → all records degraded."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["item1", "item2"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    # mock delay 10s but Flink timeout 1s → guaranteed timeout
    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.9})
    map_cfg = LLMClientConfig(backend="mock", mock_delay_s=10.0, mock_response=map_resp)
    map_fn = SemMapFunction("Classify: {input}",
                            {"sentiment": str, "confidence": float}, map_cfg)
    result = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(1), 2, Types.STRING())

    results = collect_results(env, result, "test_timeout_pipeline")
    report = SLOReport(results)
    report.print_report("timeout_pipeline")

    assert report.total == len(inputs), f"Expected {len(inputs)} records, got {report.total}"
    assert report.degraded_count == len(inputs), \
        f"Expected all {len(inputs)} degraded, got {report.degraded_count}"
    for obj in report.parsed:
        assert obj.get("_degraded", False), f"Expected _degraded=True: {obj}"
        assert "flink_timeout" in str(obj.get("_error", "")), f"Expected flink_timeout error: {obj}"
    print("  ✓ test_timeout_pipeline PASSED")


# ---------------------------------------------------------------------------
# Test 3: Error / invalid JSON path
# ---------------------------------------------------------------------------

def test_error_pipeline():
    """sem_map with mock returning non-JSON → parse failure → degraded."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["item1", "item2", "item3"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    # Mock returns plain text (not valid JSON for expected schema)
    map_cfg = LLMClientConfig(
        backend="mock", mock_delay_s=0.05,
        mock_response="this is not json at all",
    )
    map_fn = SemMapFunction("Classify: {input}",
                            {"sentiment": str, "confidence": float}, map_cfg)
    result = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(10), 5, Types.STRING())

    results = collect_results(env, result, "test_error_pipeline")
    report = SLOReport(results)
    report.print_report("error_pipeline")

    assert report.total == len(inputs), f"Expected {len(inputs)} records, got {report.total}"
    assert report.degraded_count == len(inputs), \
        f"Expected all {len(inputs)} degraded, got {report.degraded_count}"
    for obj in report.parsed:
        assert obj.get("_degraded", False), f"Expected _degraded=True: {obj}"
        assert "json_parse_error" in str(obj.get("_error", "")), \
            f"Expected json_parse_error: {obj}"
    print("  ✓ test_error_pipeline PASSED")



# ---------------------------------------------------------------------------
# Test 4: Retry — Flink AsyncRetryStrategy
# ---------------------------------------------------------------------------

def test_retry_pipeline():
    """sem_map with mock returning bad JSON first → Flink retry → success.

    Uses mock_bad_json_first_n to make the first call per record return
    invalid JSON (triggers json_parse_error → degraded).  Flink's retry
    strategy detects the degraded result and re-invokes async_invoke.
    On the second attempt the mock returns valid JSON.

    Verifies:
      - All records eventually succeed (0 degraded).
      - Retry ownership: only Flink layer retries json_parse_error.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["item1", "item2"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    # First 2 calls return bad JSON, then valid JSON on retry
    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.95})
    map_cfg = LLMClientConfig(
        backend="mock", mock_delay_s=0.05, mock_response=map_resp,
        mock_bad_json_first_n=2,  # first call per record returns bad JSON
    )
    map_fn = SemMapFunction("Classify: {input}",
                            {"sentiment": str, "confidence": float}, map_cfg)

    retry_strategy = create_flink_retry_strategy(
        max_attempts=3, backoff_time_millis=200)
    result = AsyncDataStream.unordered_wait_with_retry(
        ds, map_fn, Time.seconds(30), retry_strategy, 5, Types.STRING())

    results = collect_results(env, result, "test_retry_pipeline")
    report = SLOReport(results)
    report.print_report("retry_pipeline")

    assert report.total == len(inputs), f"Expected {len(inputs)} records, got {report.total}"
    assert report.degraded_count == 0, \
        f"Expected 0 degraded after retry, got {report.degraded_count}"
    for obj in report.parsed:
        assert "sentiment" in obj, f"Missing 'sentiment' in output: {obj}"
        assert obj.get("confidence") == 0.95, f"Unexpected confidence: {obj}"
    print("  ✓ test_retry_pipeline PASSED")


# ---------------------------------------------------------------------------
# Test 5: Backpressure stability (long-run)
# ---------------------------------------------------------------------------

def test_backpressure_pipeline():
    """Sustained load: QPS ~100, mock latency 2s, capacity=10.

    With capacity=10 and 2s latency, effective throughput is ~5 records/s,
    so backpressure will build up.  We generate enough records to run for
    several minutes and verify:
      - No task failures / restarts.
      - No silent record drops (output count == input count).
      - All records have valid schema or are properly degraded.

    Default: 200 records (~40s at 5 rec/s effective throughput).
    Set env BACKPRESSURE_RECORDS=3000 for 10+ min run.
    """
    n_records = int(os.environ.get("BACKPRESSURE_RECORDS", "200"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = [f"record_{i}" for i in range(n_records)]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    # 2s mock latency, capacity 10 → backpressure guaranteed
    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.9})
    map_cfg = LLMClientConfig(backend="mock", mock_delay_s=2.0, mock_response=map_resp)
    map_fn = SemMapFunction("Classify: {input}",
                            {"sentiment": str, "confidence": float}, map_cfg)
    result = AsyncDataStream.unordered_wait(
        ds, map_fn, Time.seconds(30), 10, Types.STRING())

    t0 = time.time()
    results = collect_results(env, result, "test_backpressure_pipeline")
    elapsed = time.time() - t0

    report = SLOReport(results)
    report.print_report("backpressure_pipeline")
    print(f"  wall_time_s:            {elapsed:.1f}")
    print(f"  effective_throughput:    {report.total / max(elapsed, 0.001):.1f} rec/s")

    # Assertions
    assert report.total == n_records, \
        f"Silent drops: expected {n_records}, got {report.total}"
    assert report.degraded_count == 0, \
        f"Expected 0 degraded under backpressure, got {report.degraded_count}"
    assert report.timeout_count == 0, \
        f"Expected 0 timeouts (timeout=30s >> latency=2s), got {report.timeout_count}"
    print("  ✓ test_backpressure_pipeline PASSED")


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

TESTS = {
    "normal": test_normal_pipeline,
    "timeout": test_timeout_pipeline,
    "error": test_error_pipeline,
    "retry": test_retry_pipeline,
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
            except (AssertionError, Exception) as e:
                print(f"  ✗ {name}: {e}")
                failed += 1
        print(f"\n{passed}/{passed + failed} E2E tests passed")
        if failed:
            sys.exit(1)
    elif which in TESTS:
        TESTS[which]()
    else:
        print(f"Unknown: {which}. Options: {list(TESTS.keys())} or 'all'")
        sys.exit(1)
