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
5. Optional real-provider path runs only when explicitly selected.

Recommended isolated-runtime invocation:

```bash
JAVA_HOME=/Users/von/Projects/FlinkMem/.isolation/jdk/jdk-17.0.18+8/Contents/Home \
PYFLINK_CLIENT_EXECUTABLE=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python \
PATH=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin:$PATH \
/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python \
flink-python/pyflink/semantic_runtime/tests/test_e2e_integration.py all
```

Do not set `PYTHONPATH=flink-python` for this runtime test. The test injects
`semantic_runtime` into the installed isolated `pyflink`, and mixing repo
Python code with the isolated JVM side causes version skew.
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
import pytest

from pyflink.common import Time, Types
from pyflink.datastream import AsyncDataStream, StreamExecutionEnvironment

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.row.sem_filter import SemFilterFunction
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction

_SEM_RUNTIME_SRC = pathlib.Path(__file__).resolve().parents[1]
_SEM_RUNTIME_DST = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _SEM_RUNTIME_DST.exists():
    os.symlink(_SEM_RUNTIME_SRC, _SEM_RUNTIME_DST)

pytestmark = pytest.mark.skipif(
    os.environ.get("SEM_RUNTIME_RUN_PYTEST_E2E", "0") != "1",
    reason="Run E2E via the isolated runtime command or set SEM_RUNTIME_RUN_PYTEST_E2E=1.",
)


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
    """Timeouts should fail the job instead of emitting substitute records."""
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
    """Invalid model JSON should fail the job instead of emitting substitute records."""
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


def _parse_dotenv_line(raw: str) -> tuple[str | None, str | None]:
    stripped = raw.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None, None
    key, val = stripped.split("=", 1)
    key = key.strip()
    val = val.strip()
    if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
        val = val[1:-1]
    return key or None, val


def _try_load_env_file(env_file: str = ".env") -> str | None:
    repo_root = pathlib.Path(__file__).resolve().parents[4]
    candidates = [pathlib.Path.cwd() / env_file, repo_root / env_file]
    for candidate in candidates:
        if not candidate.exists():
            continue
        loaded = 0
        with candidate.open("r", encoding="utf-8") as handle:
            for raw in handle:
                key, val = _parse_dotenv_line(raw)
                if not key or key in os.environ:
                    continue
                os.environ[key] = val or ""
                loaded += 1
        return f"{candidate} (loaded={loaded})"
    return None


def test_real_pipeline_if_enabled() -> None:
    """Run a minimal real-provider pipeline when explicitly enabled.

    This test is gated on environment and is intentionally excluded from the
    default ``all`` path to avoid accidental network usage and cost.
    """

    if os.environ.get("SEM_RUNTIME_LOAD_DOTENV", "0") == "1":
        _try_load_env_file(os.environ.get("SEM_RUNTIME_ENV_FILE", ".env"))

    api_key_env = os.environ.get("SEM_RUNTIME_API_KEY_ENV", "DEEPSEEK_API_KEY")
    if not os.environ.get(api_key_env):
        raise RuntimeError(
            f"Environment variable {api_key_env} is empty. "
            "Export it first or set SEM_RUNTIME_LOAD_DOTENV=1."
        )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["The service was excellent and fast."]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    llm_cfg = LLMClientConfig(
        backend="openai",
        model=os.environ.get("SEM_RUNTIME_MODEL", "deepseek-chat"),
        api_base=os.environ.get("SEM_RUNTIME_API_BASE", "https://api.deepseek.com/v1"),
        api_key_env=api_key_env,
        timeout_s=float(os.environ.get("SEM_RUNTIME_LLM_TIMEOUT_S", "30")),
        max_retries=int(os.environ.get("SEM_RUNTIME_LLM_MAX_RETRIES", "2")),
        retry_base_delay_s=float(os.environ.get("SEM_RUNTIME_LLM_RETRY_BASE_DELAY_S", "0.5")),
    )

    map_fn = SemMapFunction(
        (
            "Return strict JSON with keys sentiment and confidence. "
            "sentiment must be one of positive, neutral, negative. "
            "confidence must be a float in [0,1]. "
            "Input: {input}"
        ),
        {"sentiment": str, "confidence": float},
        llm_cfg,
    )
    mapped = AsyncDataStream.unordered_wait(ds, map_fn, Time.seconds(60), 2, Types.STRING())

    filter_fn = SemFilterFunction(
        (
            "Return strict JSON with keys decision, confidence, reason. "
            "decision should be true only for clearly positive sentiment. "
            "Input: {input}"
        ),
        llm_cfg,
    )
    filtered = AsyncDataStream.unordered_wait(
        mapped,
        filter_fn,
        Time.seconds(60),
        2,
        Types.STRING(),
    )

    results = collect_results(env, filtered, "test_real_pipeline_if_enabled")
    assert len(results) == 1
    parsed = json.loads(results[0])
    assert isinstance(parsed.get("decision"), bool)
    assert isinstance(parsed.get("confidence"), (int, float))
    assert isinstance(parsed.get("reason"), str)


TESTS = {
    "normal": test_normal_pipeline,
    "timeout": test_timeout_pipeline_fails,
    "invalid_json": test_invalid_json_pipeline_fails,
    "backpressure": test_backpressure_pipeline,
    "real": test_real_pipeline_if_enabled,
}


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which == "all":
        passed = 0
        failed = 0
        for name in ("normal", "timeout", "invalid_json", "backpressure"):
            fn = TESTS[name]
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
