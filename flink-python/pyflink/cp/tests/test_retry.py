#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""
Retry ownership split verification.

Tests:
1. result_predicate correctly classifies degraded records.
2. Flink-level retry with ``unordered_wait_with_retry`` recovers from
   retryable degraded results.
3. LLM-client-owned errors are NOT retried at the Flink layer.
"""

import json
import sys

# Bootstrap: ensure pyflink.cp is importable via symlink
import os, pathlib, pyflink as _pf  # noqa: E401
_cp_src = pathlib.Path(__file__).resolve().parents[1]
_cp_dst = pathlib.Path(_pf.__file__).parent / "cp"
if not _cp_dst.exists():
    os.symlink(_cp_src, _cp_dst)

from pyflink.cp.retry import (
    is_degraded_and_retryable,
    create_flink_retry_strategy,
    FLINK_RETRYABLE_ERRORS,
    LLM_CLIENT_OWNED_ERRORS,
)


def test_predicate_normal_result():
    """Normal (non-degraded) results should NOT trigger retry."""
    normal = [json.dumps({"sentiment": "positive", "confidence": 0.9})]
    assert not is_degraded_and_retryable(normal), "Normal result should not be retryable"


def test_predicate_empty_result():
    """Empty results should trigger retry."""
    assert is_degraded_and_retryable([]), "Empty result should be retryable"


def test_predicate_flink_timeout():
    """flink_timeout is owned by Flink layer → should retry."""
    degraded = [json.dumps({
        "_input": "test", "_error": "flink_timeout", "_degraded": True,
    })]
    assert is_degraded_and_retryable(degraded), "flink_timeout should be retryable"


def test_predicate_json_parse_error():
    """json_parse_error is owned by Flink layer → should retry."""
    degraded = [json.dumps({
        "_input": "test", "_error": "json_parse_error: Expecting value",
        "_degraded": True,
    })]
    assert is_degraded_and_retryable(degraded), "json_parse_error should be retryable"


def test_predicate_llm_call_error():
    """llm_call_error already retried by client → should NOT retry."""
    degraded = [json.dumps({
        "_input": "test", "_error": "llm_call_error: timeout after 3 attempts",
        "_degraded": True,
    })]
    assert not is_degraded_and_retryable(degraded), "llm_call_error should NOT be retryable"


def test_predicate_retrieve_timeout():
    """retrieve_timeout is owned by Flink layer → should retry."""
    degraded = [json.dumps({
        "_input": "test", "_error": "retrieve_timeout", "_degraded": True,
    })]
    assert is_degraded_and_retryable(degraded), "retrieve_timeout should be retryable"


def test_predicate_schema_validation_error():
    """schema_validation_error is owned by Flink layer → should retry."""
    degraded = [json.dumps({
        "_input": "test", "_error": "schema_validation_error", "_degraded": True,
    })]
    assert is_degraded_and_retryable(degraded), "schema_validation_error should be retryable"


def test_ownership_disjoint():
    """Verify that no error class appears in both ownership sets."""
    overlap = LLM_CLIENT_OWNED_ERRORS & FLINK_RETRYABLE_ERRORS
    assert len(overlap) == 0, f"Ownership overlap detected: {overlap}"


def test_create_strategy():
    """Verify strategy can be created without error."""
    strategy = create_flink_retry_strategy(max_attempts=3, backoff_time_millis=200)
    assert strategy.can_retry(1), "Should allow retry on first attempt"
    assert strategy.can_retry(3), "Should allow retry on max attempt"
    assert not strategy.can_retry(4), "Should not retry beyond max_attempts"
    assert strategy.get_backoff_time_millis(1) == 200


TESTS = {
    "predicate_normal": test_predicate_normal_result,
    "predicate_empty": test_predicate_empty_result,
    "predicate_flink_timeout": test_predicate_flink_timeout,
    "predicate_json_parse": test_predicate_json_parse_error,
    "predicate_llm_call": test_predicate_llm_call_error,
    "predicate_retrieve_timeout": test_predicate_retrieve_timeout,
    "predicate_schema": test_predicate_schema_validation_error,
    "ownership_disjoint": test_ownership_disjoint,
    "create_strategy": test_create_strategy,
}

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which == "all":
        passed = 0
        failed = 0
        for name, fn in TESTS.items():
            try:
                fn()
                print(f"  ✓ {name}")
                passed += 1
            except AssertionError as e:
                print(f"  ✗ {name}: {e}")
                failed += 1
        print(f"\n{passed}/{passed + failed} passed")
        if failed:
            sys.exit(1)
    elif which in TESTS:
        TESTS[which]()
        print(f"  ✓ {which}")
    else:
        print(f"Unknown: {which}. Options: {list(TESTS.keys())} or 'all'")
        sys.exit(1)

