#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""
Smoke tests for all four public row-style semantic operators
(`sem_map`, `sem_filter`, `sem_local_topk`, `sem_lookup_join`).

Each sub-test verifies the normal path with MockLLMClient.
"""

import json
import sys

# Bootstrap: ensure pyflink.semantic_runtime is importable via symlink
import os, pathlib, pyflink as _pf  # noqa: E401
_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)

from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment, AsyncDataStream

from pyflink.semantic_runtime.operators import (
    build_sem_filter_operator,
    build_sem_local_topk_operator,
    build_sem_map_operator,
)
from pyflink.semantic_runtime.operators.row.sem_lookup_join import (
    SemLookupJoinFunction, SemLookupJoinConfig,
)
from pyflink.semantic_runtime.runtime_config import RuntimeConfig
from pyflink.semantic_runtime.semantic_spec import SemanticSpec


def _mock_runtime_config(operator_name: str, *, response: str, delay_s: float = 0.05) -> RuntimeConfig:
    return RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                operator_name: {
                    "kernel": {
                        "mock_delay_s": delay_s,
                        "mock_response": response,
                    }
                }
            },
        }
    )


def run_sem_map():
    """Verify sem_map produces structured output."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(["hello", "world"], type_info=Types.STRING())

    runtime_config = _mock_runtime_config(
        "sem_map",
        response=json.dumps({"sentiment": "positive", "confidence": 0.9}),
    )
    fn = build_sem_map_operator(
        SemanticSpec.for_sem_map(
            "Classify: {input}",
            output_schema={"sentiment": str, "confidence": float},
        ),
        runtime_config,
    )
    result = AsyncDataStream.unordered_wait(ds, fn, Time.seconds(10), 2, Types.STRING())
    result.print()
    env.execute("smoke_sem_map")


def run_sem_filter():
    """Verify sem_filter produces 1:1 records with decision/confidence/reason."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(
        ["great product", "terrible product", "okay product"],
        type_info=Types.STRING(),
    )

    mock_resp = json.dumps({"decision": True, "confidence": 0.85, "reason": "positive"})
    runtime_config = _mock_runtime_config("sem_filter", response=mock_resp)
    fn = build_sem_filter_operator(
        SemanticSpec.for_sem_filter("Is this positive? {input}"),
        runtime_config,
    )
    result = AsyncDataStream.unordered_wait(ds, fn, Time.seconds(10), 2, Types.STRING())
    result.print()
    env.execute("smoke_sem_filter")


def run_sem_local_topk():
    """Verify sem_local_topk reranks candidates and returns top-k."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    records = [
        json.dumps({"query": "best restaurant", "candidates": ["A", "B", "C", "D"]}),
        json.dumps({"query": "cheap hotel", "candidates": ["X", "Y", "Z"]}),
    ]
    ds = env.from_collection(records, type_info=Types.STRING())

    mock_resp = json.dumps(["C", "A", "D", "B"])
    runtime_config = _mock_runtime_config("sem_local_topk", response=mock_resp)
    fn = build_sem_local_topk_operator(
        SemanticSpec.for_sem_topk("Rank these for '{input}': {candidates}"),
        k=2,
        runtime_config=runtime_config,
    )
    result = AsyncDataStream.unordered_wait(ds, fn, Time.seconds(10), 2, Types.STRING())
    result.print()
    env.execute("smoke_sem_topk")


def run_sem_lookup_join():
    """Verify sem_lookup_join fetches candidates and runs LLM matching."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(["query_a", "query_b"], type_info=Types.STRING())

    mock_join_result = json.dumps({"matched": "candidate_1", "score": 0.92})
    from pyflink.semantic_runtime.llm_client import LLMClientConfig

    llm_config = LLMClientConfig(
        backend="mock", mock_delay_s=0.05, mock_response=mock_join_result)
    join_config = SemLookupJoinConfig(
        max_candidates_per_record=5,
        retrieve_timeout_ms=3000,
        mock_candidates=["candidate_1", "candidate_2", "candidate_3"],
        mock_retrieve_delay_s=0.02,
    )
    fn = SemLookupJoinFunction(
        "Match {input} with: {candidates}", llm_config, join_config)
    result = AsyncDataStream.unordered_wait(ds, fn, Time.seconds(10), 2, Types.STRING())
    result.print()
    env.execute("smoke_sem_join")


TESTS = {
    "sem_map": run_sem_map,
    "sem_filter": run_sem_filter,
    "sem_local_topk": run_sem_local_topk,
    "sem_lookup_join": run_sem_lookup_join,
}

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which == "all":
        for name, fn in TESTS.items():
            print(f"\n=== {name} ===")
            fn()
    elif which in TESTS:
        print(f"\n=== {which} ===")
        TESTS[which]()
    else:
        print(f"Unknown: {which}. Options: {list(TESTS.keys())} or 'all'")
        sys.exit(1)
