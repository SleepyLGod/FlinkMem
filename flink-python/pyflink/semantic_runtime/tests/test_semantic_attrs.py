#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import asyncio
import json

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.row._semantic_attrs import (
    SemLabelFunction,
    SemMatchFunction,
    SemScoreFunction,
)


def _open(fn) -> None:
    fn.open(object())


def test_sem_score_function_parses_score_schema():
    fn = SemScoreFunction(
        "Score relevance: {input}",
        LLMClientConfig(
            backend="mock",
            mock_response=json.dumps(
                {"score": 0.9, "confidence": 0.8, "reason": "high overlap"}
            ),
        ),
    )
    _open(fn)
    out = asyncio.run(fn.async_invoke("weather memo"))
    parsed = json.loads(out[0])
    assert parsed["score"] == 0.9
    assert parsed["confidence"] == 0.8
    assert parsed["reason"] == "high overlap"
    assert "_metrics" in parsed
    fn.close()


def test_sem_label_function_parses_label_schema():
    fn = SemLabelFunction(
        "Label topic: {input}",
        LLMClientConfig(
            backend="mock",
            mock_response=json.dumps(
                {"label": "weather", "confidence": 0.7, "reason": "mentions rain"}
            ),
        ),
    )
    _open(fn)
    out = asyncio.run(fn.async_invoke("today will rain"))
    parsed = json.loads(out[0])
    assert parsed["label"] == "weather"
    assert parsed["confidence"] == 0.7
    assert parsed["reason"] == "mentions rain"
    fn.close()


def test_sem_match_function_parses_match_schema():
    fn = SemMatchFunction(
        "Do these match? {input}",
        LLMClientConfig(
            backend="mock",
            mock_response=json.dumps(
                {"matched": True, "match_score": 0.95, "reason": "same entity"}
            ),
        ),
    )
    _open(fn)
    out = asyncio.run(fn.async_invoke({"left": "A", "right": "A"}))
    parsed = json.loads(out[0])
    assert parsed["matched"] is True
    assert parsed["match_score"] == 0.95
    assert parsed["reason"] == "same entity"
    fn.close()


def test_sem_match_function_degrades_on_schema_mismatch():
    fn = SemMatchFunction(
        "Do these match? {input}",
        LLMClientConfig(
            backend="mock",
            mock_response=json.dumps(
                {"matched": True, "score": 0.95, "reason": "wrong field name"}
            ),
        ),
    )
    _open(fn)
    try:
        asyncio.run(fn.async_invoke({"left": "A", "right": "B"}))
    except ValueError as exc:
        assert "violates output_schema" in str(exc)
    else:
        raise AssertionError("Expected schema mismatch to fail fast")
    fn.close()
