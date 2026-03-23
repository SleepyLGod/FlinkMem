#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import asyncio
import json

from pyflink.semantic_runtime.llm_client import LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime.steps import (
    SemLabelFunction,
    SemMatchFunction,
    SemRerankFunction,
    SemScoreFunction,
    evaluate_sem_match_block_sync,
    evaluate_sem_rerank_block_sync,
    evaluate_sem_score_block_sync,
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


def test_sem_score_block_execution_parses_scores() -> None:
    llm_config = LLMClientConfig(
        backend="mock",
        mock_response=json.dumps(
            {
                "scores": [
                    {
                        "item_idx": 0,
                        "score": 0.88,
                        "confidence": 0.81,
                        "reason": "high relevance",
                    }
                ]
            }
        ),
    )
    client = create_llm_client(llm_config)
    try:
        out = evaluate_sem_score_block_sync(
            client=client,
            llm_config=llm_config,
            intent="Score these items",
            score_block=[{"item_idx": 0, "candidate": {"id": 1}}],
        )
    finally:
        client.close()
    assert out == [
        {
            "item_idx": 0,
            "score": 0.88,
            "confidence": 0.81,
            "reason": "high relevance",
        }
    ]


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


def test_sem_rerank_function_parses_rerank_schema() -> None:
    fn = SemRerankFunction(
        "Rerank these items: {input}",
        LLMClientConfig(
            backend="mock",
            mock_response=json.dumps({"ranked_item_ids": ["b", "a"]}),
        ),
    )
    _open(fn)
    out = asyncio.run(fn.async_invoke([{"item_id": "a"}, {"item_id": "b"}]))
    parsed = json.loads(out[0])
    assert parsed["ranked_item_ids"] == ["b", "a"]
    fn.close()


def test_sem_match_function_fails_fast_on_schema_mismatch():
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


def test_sem_match_block_execution_parses_matches() -> None:
    llm_config = LLMClientConfig(
        backend="mock",
        mock_response=json.dumps(
            {
                "matches": [
                    {
                        "pair_idx": 0,
                        "matched": True,
                        "match_score": 0.92,
                        "reason": "same issue",
                    }
                ]
            }
        ),
    )
    client = create_llm_client(llm_config)
    try:
        out = evaluate_sem_match_block_sync(
            client=client,
            llm_config=llm_config,
            intent="Do these pairs match?",
            pair_block=[{"left": {"id": 1}, "right": {"id": 1}}],
        )
    finally:
        client.close()
    assert out == [
        {
            "pair_idx": 0,
            "matched": True,
            "match_score": 0.92,
            "reason": "same issue",
        }
    ]


def test_sem_rerank_block_execution_parses_ranked_ids() -> None:
    llm_config = LLMClientConfig(
        backend="mock",
        mock_response=json.dumps({"ranked_item_ids": ["b", "a"]}),
    )
    client = create_llm_client(llm_config)
    try:
        out = evaluate_sem_rerank_block_sync(
            client=client,
            llm_config=llm_config,
            intent="Rerank these items",
            method="listwise",
            rerank_block=[
                {"item_id": "a", "candidate": {"id": "a"}},
                {"item_id": "b", "candidate": {"id": "b"}},
            ],
        )
    finally:
        client.close()
    assert out == ["b", "a"]
