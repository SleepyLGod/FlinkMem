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
import ast
import contextlib
import io
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

from pyflink.semantic_runtime.public_api import (
    context,
    sem_agg,
    sem_filter,
    sem_groupby,
    sem_join,
    sem_local_topk,
    sem_lookup_join,
    sem_map,
    sem_topk,
)
from pyflink.semantic_runtime.runtime import (
    apply_sem_agg_pushdown,
    apply_sem_filter_pushdown,
    apply_sem_groupby_pushdown,
    apply_sem_join_from_request,
    apply_sem_local_topk_pushdown,
    apply_sem_lookup_join_pushdown,
    apply_sem_map_pushdown,
    apply_sem_topk_pushdown,
)
from pyflink.semantic_runtime.runtime.external_search_backend import MockSearchBackend
from pyflink.semantic_runtime.runtime_config import RuntimeConfig

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


def parse_result_record(raw: str) -> Dict[str, Any]:
    """Parse one collected result record from JSON or Python dict repr."""
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        parsed = ast.literal_eval(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Expected collected result record to be a dict")
    return parsed


def assert_job_fails(env: StreamExecutionEnvironment, result_stream, job_name: str) -> None:
    """Assert that the stream job fails during execution."""
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            collect_results(env, result_stream, job_name)
    except Exception:
        return
    raise AssertionError(f"Expected job {job_name!r} to fail")


def _runtime_config_with_mock_rows(
    *,
    map_response: str,
    map_delay_s: float,
    filter_response: str,
    filter_delay_s: float,
) -> RuntimeConfig:
    return RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_map": {
                    "kernel": {
                        "mock_delay_s": map_delay_s,
                        "mock_response": map_response,
                    }
                },
                "sem_filter": {
                    "kernel": {
                        "mock_delay_s": filter_delay_s,
                        "mock_response": filter_response,
                    }
                },
            },
        }
    )


def test_normal_pipeline() -> None:
    """Normal path should succeed with schema-valid outputs."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["great product", "amazing service", "wonderful experience"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    map_resp = json.dumps({"sentiment": "positive", "confidence": 0.95})
    filter_resp = json.dumps(
        {"decision": True, "confidence": 0.9, "reason": "positive sentiment"}
    )
    runtime_config = _runtime_config_with_mock_rows(
        map_response=map_resp,
        map_delay_s=0.05,
        filter_response=filter_resp,
        filter_delay_s=0.05,
    )
    mapped = apply_sem_map_pushdown(
        ds,
        request=sem_map(
            intent="Classify sentiment",
            output_schema={"sentiment": str, "confidence": float},
        ),
        runtime_config=runtime_config,
    )

    filtered = apply_sem_filter_pushdown(
        mapped,
        request=sem_filter(intent="Keep positive sentiment only"),
        runtime_config=runtime_config,
    )
    results = collect_results(env, filtered, "test_normal_pipeline")
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

    runtime_config = _runtime_config_with_mock_rows(
        map_response=json.dumps({"sentiment": "positive", "confidence": 0.9}),
        map_delay_s=10.0,
        filter_response=json.dumps({"decision": True, "confidence": 0.9, "reason": "unused"}),
        filter_delay_s=0.05,
    )
    result = apply_sem_map_pushdown(
        ds,
        request=sem_map(
            intent="Classify sentiment",
            output_schema={"sentiment": str, "confidence": float},
        ),
        runtime_config=runtime_config,
        timeout_ms=1_000,
        async_capacity=2,
    )
    assert_job_fails(env, result, "test_timeout_pipeline_fails")


def test_invalid_json_pipeline_fails() -> None:
    """Invalid model JSON should fail the job instead of emitting substitute records."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["item1", "item2", "item3"]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    runtime_config = _runtime_config_with_mock_rows(
        map_response="this is not json at all",
        map_delay_s=0.05,
        filter_response=json.dumps({"decision": True, "confidence": 0.9, "reason": "unused"}),
        filter_delay_s=0.05,
    )
    result = apply_sem_map_pushdown(
        ds,
        request=sem_map(
            intent="Classify sentiment",
            output_schema={"sentiment": str, "confidence": float},
        ),
        runtime_config=runtime_config,
        timeout_ms=10_000,
        async_capacity=5,
    )
    assert_job_fails(env, result, "test_invalid_json_pipeline_fails")


def test_backpressure_pipeline() -> None:
    """Sustained load should preserve all records without silent drops."""
    n_records = int(os.environ.get("BACKPRESSURE_RECORDS", "200"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = [f"record_{i}" for i in range(n_records)]
    ds = env.from_collection(inputs, type_info=Types.STRING())

    runtime_config = _runtime_config_with_mock_rows(
        map_response=json.dumps({"sentiment": "positive", "confidence": 0.9}),
        map_delay_s=2.0,
        filter_response=json.dumps({"decision": True, "confidence": 0.9, "reason": "unused"}),
        filter_delay_s=0.05,
    )
    result = apply_sem_map_pushdown(
        ds,
        request=sem_map(
            intent="Classify sentiment",
            output_schema={"sentiment": str, "confidence": float},
        ),
        runtime_config=runtime_config,
        timeout_ms=30_000,
        async_capacity=10,
    )

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

    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {
                "backend": "openai",
                "model": os.environ.get("SEM_RUNTIME_MODEL", "deepseek-chat"),
                "endpoint": os.environ.get("SEM_RUNTIME_API_BASE", "https://api.deepseek.com/v1"),
                "extra": {
                    "api_key_env": api_key_env,
                    "timeout_s": float(os.environ.get("SEM_RUNTIME_LLM_TIMEOUT_S", "30")),
                    "max_retries": int(os.environ.get("SEM_RUNTIME_LLM_MAX_RETRIES", "2")),
                    "retry_base_delay_s": float(
                        os.environ.get("SEM_RUNTIME_LLM_RETRY_BASE_DELAY_S", "0.5")
                    ),
                },
            },
            "operators": {
                "sem_map": {"kernel": {}},
                "sem_filter": {"kernel": {}},
            },
        }
    )
    mapped = apply_sem_map_pushdown(
        ds,
        request=sem_map(
            intent="Classify sentiment as positive, neutral, or negative",
            output_schema={"sentiment": str, "confidence": float},
        ),
        runtime_config=runtime_config,
        timeout_ms=60_000,
        async_capacity=2,
    )

    filtered = apply_sem_filter_pushdown(
        mapped,
        request=sem_filter(intent="Keep only clearly positive sentiment"),
        runtime_config=runtime_config,
        timeout_ms=60_000,
        async_capacity=2,
    )

    results = collect_results(env, filtered, "test_real_pipeline_if_enabled")
    assert len(results) == 1
    parsed = json.loads(results[0])
    assert isinstance(parsed.get("decision"), bool)
    assert isinstance(parsed.get("confidence"), (int, float))
    assert isinstance(parsed.get("reason"), str)


def test_local_topk_pushdown_pipeline() -> None:
    """Positive pushdown path for sem_local_topk should succeed."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    records = [
        json.dumps({"query": "best restaurant", "items": ["A", "B", "C", "D"]}),
    ]
    ds = env.from_collection(records, type_info=Types.STRING())
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_local_topk": {
                    "kernel": {
                        "mock_delay_s": 0.05,
                        "mock_response": json.dumps(
                            {
                                "scores": [
                                    {"item_idx": 0, "score": 0.91, "confidence": 0.9, "reason": "good"},
                                    {"item_idx": 1, "score": 0.70, "confidence": 0.7, "reason": "weak"},
                                    {"item_idx": 2, "score": 0.98, "confidence": 0.95, "reason": "best"},
                                    {"item_idx": 3, "score": 0.8, "confidence": 0.8, "reason": "ok"},
                                ]
                            }
                        ),
                    }
                }
            },
        }
    )
    result = apply_sem_local_topk_pushdown(
        ds,
        request=sem_local_topk(intent="Rank candidates by relevance", k=2),
        runtime_config=runtime_config,
    )
    results = collect_results(env, result, "test_local_topk_pushdown_pipeline")
    assert len(results) == 1
    parsed = json.loads(results[0])
    assert parsed["top_k"][0]["item"] == "C"
    assert len(parsed["top_k"]) == 2


def test_lookup_join_pushdown_pipeline() -> None:
    """Positive pushdown path for sem_lookup_join should succeed."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    inputs = ["budget update"]
    ds = env.from_collection(inputs, type_info=Types.STRING())
    backend = MockSearchBackend(
        query_rules={
            "budget": [
                {"candidate_id": "c1", "text": "Budget plan", "score": 0.9},
                {"candidate_id": "c2", "text": "Budget risk", "score": 0.8},
            ]
        }
    )
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_lookup_join": {
                    "kernel": {
                        "mock_delay_s": 0.05,
                        "mock_response": json.dumps(
                            {
                                "matched": True,
                                "match_score": 0.93,
                                "selected_candidate": {"candidate_id": "c1"},
                                "reason": "best semantic match",
                            }
                        ),
                        "right_block_size": 1,
                    }
                }
            },
        }
    )
    result = apply_sem_lookup_join_pushdown(
        ds,
        request=sem_lookup_join(intent="Join with the most relevant budget memory", candidate_source=backend),
        runtime_config=runtime_config,
    )
    results = collect_results(env, result, "test_lookup_join_pushdown_pipeline")
    assert len(results) == 1
    parsed = json.loads(results[0])
    assert parsed["join_result"]["selected_candidate"]["candidate_id"] == "c1"


def test_window_groupby_pushdown_pipeline() -> None:
    """Positive pushdown path for window-owned sem_groupby should succeed."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    snapshots = [
        json.dumps(
            {
                "key": "user_1",
                "window_id": "w1",
                "trigger_reason": "close",
                "events": [
                    {"key": "user_1", "payload": "project budget", "seq_id": 1},
                    {"key": "user_1", "payload": "project risk", "seq_id": 2},
                    {"key": "user_1", "payload": "travel flight", "seq_id": 3},
                ],
            }
        )
    ]
    ds = env.from_collection(snapshots, type_info=Types.STRING())
    runtime_config = RuntimeConfig.from_dict(
        {
            "operators": {
                "sem_groupby": {
                    "query_spec": {},
                    "kernel": {"variant": "rule", "confidence_threshold": 0.5},
                }
            }
        }
    )
    result = apply_sem_groupby_pushdown(
        ds,
        request=sem_groupby(intent="Group by topic", context=context("window")),
        runtime_config=runtime_config,
    )
    results = collect_results(env, result, "test_window_groupby_pushdown_pipeline")
    assert len(results) == 3
    parsed = [parse_result_record(item) for item in results]
    assert parsed[0]["group_id"] == parsed[1]["group_id"]
    assert parsed[2]["group_id"] != parsed[0]["group_id"]


def test_stateful_topk_pushdown_pipeline() -> None:
    """Positive pushdown path for bounded pointwise sem_topk should succeed."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    pools = [
        json.dumps(
            {
                "key": "user_1",
                "query": "budget",
                "query_seq_id": 7,
                "candidates": [
                    {"candidate_id": "c1", "text": "Budget plan"},
                    {"candidate_id": "c2", "text": "Budget risk"},
                    {"candidate_id": "c3", "text": "Travel itinerary"},
                ],
            }
        )
    ]
    ds = env.from_collection(pools, type_info=Types.STRING())
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_topk": {
                    "query_spec": {},
                    "kernel": {
                        "scorer_backend": "llm",
                        "mock_delay_s": 0.05,
                        "mock_response": json.dumps(
                            {
                                "scores": [
                                    {"item_idx": 0, "score": 0.88, "confidence": 0.85, "reason": "good"},
                                    {"item_idx": 1, "score": 0.97, "confidence": 0.95, "reason": "best"},
                                    {"item_idx": 2, "score": 0.21, "confidence": 0.2, "reason": "low"},
                                ]
                            }
                        ),
                    },
                }
            },
        }
    )
    result = apply_sem_topk_pushdown(
        ds,
        request=sem_topk(intent="Rank relevant memories", k=2, context=context("window")),
        runtime_config=runtime_config,
    )
    results = collect_results(env, result, "test_stateful_topk_pushdown_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["top_ids"] == ["c2", "c1"]


def test_window_algebraic_agg_pushdown_pipeline() -> None:
    """Positive pushdown path for window-owned algebraic sem_agg should succeed."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    snapshots = [
        json.dumps(
            {
                "key": "user_1",
                "window_id": "w1",
                "trigger_reason": "close",
                "events": [
                    {"key": "user_1", "payload": "a", "seq_id": 1, "total": 3},
                    {"key": "user_1", "payload": "b", "seq_id": 2, "total": 5},
                ],
            }
        )
    ]
    ds = env.from_collection(snapshots, type_info=Types.STRING())

    def sum_reduce(acc, event):
        return {"key": acc.get("key", "user_1"), "total": acc.get("total", 0) + event.get("total", 0)}

    runtime_config = RuntimeConfig.from_dict(
        {
            "operators": {
                "sem_agg": {
                    "query_spec": {},
                    "kernel": {"mode": "algebraic", "reduce_fn": sum_reduce},
                }
            }
        }
    )
    result = apply_sem_agg_pushdown(
        ds,
        request=sem_agg(intent="Aggregate totals", mode="algebraic", context=context("window")),
        runtime_config=runtime_config,
    )
    results = collect_results(env, result, "test_window_algebraic_agg_pushdown_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["aggregate"]["total"] == 8


def test_true_two_input_sem_join_pipeline() -> None:
    """Positive path for true two-input sem_join should succeed."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_rows = [
        ("alice", "Alice lives in Beijing"),
    ]
    right_rows = [
        ("alice", "Alice lives in Shanghai"),
    ]
    left_ds = env.from_collection(left_rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING()]))
    right_ds = env.from_collection(
        right_rows,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING()]),
    )
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match contradictory facts",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match contradictory facts",
            context=context("stream"),
            right_input=right_ds,
            join_type="inner",
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: row[0],
        right_key_selector=lambda row: row[0],
    )
    results = collect_results(env, result, "test_true_two_input_sem_join_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["match_score"] >= 0.0
    assert parsed["left"][0] == "alice"
    assert parsed["right"][0] == "alice"


def test_true_two_input_left_sem_join_pipeline() -> None:
    """Positive path for left sem_join should emit matched rows on semantic match."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_rows = [
        ("alice", "Alice lives in Beijing"),
    ]
    right_rows = [
        ("alice", "Alice currently lives in Beijing"),
    ]
    left_ds = env.from_collection(left_rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING()]))
    right_ds = env.from_collection(
        right_rows,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING()]),
    )
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match same fact",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match same fact",
            context=context("stream"),
            right_input=right_ds,
            join_type="left",
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: row[0],
        right_key_selector=lambda row: row[0],
    )
    results = collect_results(env, result, "test_true_two_input_left_sem_join_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["join_type"] == "left"
    assert parsed["left"][0] == "alice"
    assert parsed["right"][0] == "alice"


def test_true_two_input_right_sem_join_pipeline() -> None:
    """Positive path for right sem_join should emit matched rows on semantic match."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_rows = [
        ("alice", "Alice lives in Beijing"),
    ]
    right_rows = [
        ("alice", "Alice currently lives in Beijing"),
    ]
    left_ds = env.from_collection(left_rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING()]))
    right_ds = env.from_collection(
        right_rows,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING()]),
    )
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match same fact",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match same fact",
            context=context("stream"),
            right_input=right_ds,
            join_type="right",
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: row[0],
        right_key_selector=lambda row: row[0],
    )
    results = collect_results(env, result, "test_true_two_input_right_sem_join_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["join_type"] == "right"
    assert parsed["left"][0] == "alice"
    assert parsed["right"][0] == "alice"


def test_true_two_input_full_sem_join_pipeline() -> None:
    """Positive path for full sem_join should emit matched rows on semantic match."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_rows = [
        ("alice", "Alice lives in Beijing"),
    ]
    right_rows = [
        ("alice", "Alice currently lives in Beijing"),
    ]
    left_ds = env.from_collection(left_rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING()]))
    right_ds = env.from_collection(
        right_rows,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING()]),
    )
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match same fact",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match same fact",
            context=context("stream"),
            right_input=right_ds,
            join_type="full",
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: row[0],
        right_key_selector=lambda row: row[0],
    )
    results = collect_results(env, result, "test_true_two_input_full_sem_join_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["join_type"] == "full"
    assert parsed["left"][0] == "alice"
    assert parsed["right"][0] == "alice"


def test_true_two_input_semi_sem_join_pipeline() -> None:
    """Positive path for semi sem_join should emit left side once on semantic match."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_rows = [
        ("alice", "Alice lives in Beijing"),
    ]
    right_rows = [
        ("alice", "Alice currently lives in Beijing"),
    ]
    left_ds = env.from_collection(left_rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING()]))
    right_ds = env.from_collection(
        right_rows,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING()]),
    )
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match same fact",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match same fact",
            context=context("stream"),
            right_input=right_ds,
            join_type="semi",
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: row[0],
        right_key_selector=lambda row: row[0],
    )
    results = collect_results(env, result, "test_true_two_input_semi_sem_join_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["join_type"] == "semi"
    assert parsed["left"][0] == "alice"
    assert parsed["right"] is None


def test_window_context_sem_join_pipeline() -> None:
    """Window-context sem_join should ingest snapshot deltas into the same continuous join runtime."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_snapshots = [
        json.dumps(
            {
                "key": "alice",
                "window_id": "w1",
                "events": [{"key": "alice", "payload": "Alice lives in Beijing", "seq_id": 1}],
                "trigger_reason": "close",
            }
        )
    ]
    right_snapshots = [
        json.dumps(
            {
                "key": "alice",
                "window_id": "w1",
                "events": [{"key": "alice", "payload": "Alice lives in Shanghai", "seq_id": 2}],
                "trigger_reason": "close",
            }
        )
    ]
    left_ds = env.from_collection(left_snapshots, type_info=Types.STRING())
    right_ds = env.from_collection(right_snapshots, type_info=Types.STRING())
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match contradictory facts",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match contradictory facts",
            context=context("window"),
            right_input=right_ds,
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: json.loads(row)["key"],
        right_key_selector=lambda row: json.loads(row)["key"],
    )
    results = collect_results(env, result, "test_window_context_sem_join_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["left"] == "Alice lives in Beijing"
    assert parsed["right"] == "Alice lives in Shanghai"


def test_mixed_scope_sem_join_pipeline() -> None:
    """Mixed scope join should work: left snapshot stream, right row stream."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_snapshots = [
        json.dumps(
            {
                "key": "alice",
                "window_id": "left-w1",
                "events": [{"key": "alice", "payload": "Alice lives in Beijing", "seq_id": 1}],
                "trigger_reason": "close",
            }
        )
    ]
    right_rows = [
        ("alice", "Alice currently lives in Beijing"),
    ]
    left_ds = env.from_collection(left_snapshots, type_info=Types.STRING())
    right_ds = env.from_collection(right_rows, type_info=Types.TUPLE([Types.STRING(), Types.STRING()]))

    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match same fact",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match same fact",
            context=context("stream"),
            right_input=right_ds,
            join_type="inner",
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: json.loads(row)["key"],
        right_key_selector=lambda row: row[0],
    )
    results = collect_results(env, result, "test_mixed_scope_sem_join_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["left"] == "Alice lives in Beijing"
    assert parsed["right"][0] == "alice"


def test_sem_join_cross_scope_seq_dedupe_pipeline() -> None:
    """Repeated seq_id across scopes should not produce duplicate join matches."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    left_snapshots = [
        json.dumps(
            {
                "key": "alice",
                "window_id": "left-w1",
                "events": [{"key": "alice", "payload": "L", "seq_id": 1}],
                "trigger_reason": "close",
            }
        )
    ]
    right_snapshots = [
        json.dumps(
            {
                "key": "alice",
                "window_id": "right-w1",
                "events": [{"key": "alice", "payload": "R", "seq_id": 2}],
                "trigger_reason": "close",
            }
        ),
        json.dumps(
            {
                "key": "alice",
                "window_id": "right-w2",
                "events": [{"key": "alice", "payload": "R", "seq_id": 2}],
                "trigger_reason": "close",
            }
        ),
    ]
    left_ds = env.from_collection(left_snapshots, type_info=Types.STRING())
    right_ds = env.from_collection(right_snapshots, type_info=Types.STRING())

    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "semantic": {
                            "instruction": "Match same fact",
                            "backend": "embedding",
                            "output_mode": "bool",
                            "threshold": 0.0,
                        }
                    },
                    "kernel": {
                        "pair_block_size": 1,
                    },
                }
            },
        }
    )
    result = apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match same fact",
            context=context("stream"),
            right_input=right_ds,
            join_type="inner",
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: json.loads(row)["key"],
        right_key_selector=lambda row: json.loads(row)["key"],
    )
    results = collect_results(env, result, "test_sem_join_cross_scope_seq_dedupe_pipeline")
    assert len(results) == 1
    parsed = parse_result_record(results[0])
    assert parsed["matched"] is True
    assert parsed["left"] == "L"
    assert parsed["right"] == "R"


TESTS = {
    "normal": test_normal_pipeline,
    "timeout": test_timeout_pipeline_fails,
    "invalid_json": test_invalid_json_pipeline_fails,
    "backpressure": test_backpressure_pipeline,
    "local_topk": test_local_topk_pushdown_pipeline,
    "lookup_join": test_lookup_join_pushdown_pipeline,
    "groupby": test_window_groupby_pushdown_pipeline,
    "stateful_topk": test_stateful_topk_pushdown_pipeline,
    "agg": test_window_algebraic_agg_pushdown_pipeline,
    "sem_join": test_true_two_input_sem_join_pipeline,
    "left_sem_join": test_true_two_input_left_sem_join_pipeline,
    "right_sem_join": test_true_two_input_right_sem_join_pipeline,
    "full_sem_join": test_true_two_input_full_sem_join_pipeline,
    "semi_sem_join": test_true_two_input_semi_sem_join_pipeline,
    "window_sem_join": test_window_context_sem_join_pipeline,
    "mixed_scope_sem_join": test_mixed_scope_sem_join_pipeline,
    "cross_scope_dedupe_sem_join": test_sem_join_cross_scope_seq_dedupe_pipeline,
    "real": test_real_pipeline_if_enabled,
}


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which == "all":
        passed = 0
        failed = 0
        for name in (
            "normal",
            "timeout",
            "invalid_json",
            "backpressure",
            "local_topk",
            "lookup_join",
            "groupby",
            "stateful_topk",
            "agg",
            "sem_join",
            "left_sem_join",
            "right_sem_join",
            "full_sem_join",
            "semi_sem_join",
            "window_sem_join",
            "mixed_scope_sem_join",
            "cross_scope_dedupe_sem_join",
        ):
            fn = TESTS[name]
            try:
                fn()
                print(f"  ✓ {name}")
                passed += 1
            except Exception as exc:
                print(f"  ✗ {name}: {exc}")
                failed += 1
        print(f"\n{passed}/{passed + failed} E2E tests passed")
        if failed:
            sys.exit(1)
    elif which in TESTS:
        TESTS[which]()
        print(f"✓ {which}")
    else:
        print(f"Unknown: {which}. Options: {list(TESTS.keys())} or 'all'")
        sys.exit(1)
