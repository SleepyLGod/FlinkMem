#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""
Smoke tests for all four semantic operators (sem_map, sem_filter, sem_topk,
sem_join_retrieve).

Each sub-test verifies the normal path with MockLLMClient.
"""

import json
import sys

# Bootstrap: ensure pyflink.cp is importable via symlink
import os, pathlib, pyflink as _pf  # noqa: E401
_cp_src = pathlib.Path(__file__).resolve().parents[1]
_cp_dst = pathlib.Path(_pf.__file__).parent / "cp"
if not _cp_dst.exists():
    os.symlink(_cp_src, _cp_dst)

from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment, AsyncDataStream

from pyflink.cp.llm_client import LLMClientConfig
from pyflink.cp.operators.sem_map import SemMapFunction
from pyflink.cp.operators.sem_filter import SemFilterFunction
from pyflink.cp.operators.sem_topk import SemTopKFunction
from pyflink.cp.operators.sem_join_retrieve import (
    SemJoinRetrieveFunction, SemJoinRetrieveConfig,
)


def run_sem_map():
    """Verify sem_map produces structured output."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(["hello", "world"], type_info=Types.STRING())

    config = LLMClientConfig(
        backend="mock", mock_delay_s=0.05,
        mock_response=json.dumps({"sentiment": "positive", "confidence": 0.9}),
    )
    fn = SemMapFunction("Classify: {input}",
                        {"sentiment": str, "confidence": float}, config)
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
    config = LLMClientConfig(backend="mock", mock_delay_s=0.05, mock_response=mock_resp)
    fn = SemFilterFunction("Is this positive? {input}", config)
    result = AsyncDataStream.unordered_wait(ds, fn, Time.seconds(10), 2, Types.STRING())
    result.print()
    env.execute("smoke_sem_filter")


def run_sem_topk():
    """Verify sem_topk reranks candidates and returns top-k."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    records = [
        json.dumps({"query": "best restaurant", "candidates": ["A", "B", "C", "D"]}),
        json.dumps({"query": "cheap hotel", "candidates": ["X", "Y", "Z"]}),
    ]
    ds = env.from_collection(records, type_info=Types.STRING())

    mock_resp = json.dumps(["C", "A", "D", "B"])
    config = LLMClientConfig(backend="mock", mock_delay_s=0.05, mock_response=mock_resp)
    fn = SemTopKFunction("Rank these for '{input}': {candidates}", k=2, llm_config=config)
    result = AsyncDataStream.unordered_wait(ds, fn, Time.seconds(10), 2, Types.STRING())
    result.print()
    env.execute("smoke_sem_topk")


def run_sem_join():
    """Verify sem_join_retrieve fetches candidates and runs LLM matching."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(["query_a", "query_b"], type_info=Types.STRING())

    mock_join_result = json.dumps({"matched": "candidate_1", "score": 0.92})
    llm_config = LLMClientConfig(
        backend="mock", mock_delay_s=0.05, mock_response=mock_join_result)
    join_config = SemJoinRetrieveConfig(
        max_candidates_per_record=5,
        retrieve_timeout_ms=3000,
        mock_candidates=["candidate_1", "candidate_2", "candidate_3"],
        mock_retrieve_delay_s=0.02,
    )
    fn = SemJoinRetrieveFunction(
        "Match {input} with: {candidates}", llm_config, join_config)
    result = AsyncDataStream.unordered_wait(ds, fn, Time.seconds(10), 2, Types.STRING())
    result.print()
    env.execute("smoke_sem_join")


TESTS = {
    "sem_map": run_sem_map,
    "sem_filter": run_sem_filter,
    "sem_topk": run_sem_topk,
    "sem_join": run_sem_join,
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

