"""Tests for the public semantic facade."""

from __future__ import annotations

import pytest

from pyflink.semantic_runtime import (
    context,
    sem_agg,
    sem_filter,
    sem_groupby,
    sem_local_topk,
    sem_lookup_join,
    sem_map,
    sem_topk,
    sem_window,
)
from pyflink.semantic_runtime import __all__ as public_all


def test_context_creation() -> None:
    ctx = context("window", size="conversation")
    assert ctx.kind == "window"
    assert ctx.metadata == {"size": "conversation"}


def test_context_rejects_invalid_kind() -> None:
    with pytest.raises(ValueError, match="Invalid context kind"):
        context("periodic")


def test_sem_map_request() -> None:
    req = sem_map(intent="Extract sentiment: {input}", output_schema={"sentiment": str})
    assert req.intent == "Extract sentiment: {input}"
    assert req.output_schema == {"sentiment": str}
    assert req.output_mode == "json"


def test_sem_filter_request() -> None:
    req = sem_filter(intent="Keep weather-related events")
    assert req.intent == "Keep weather-related events"


def test_sem_local_topk_requires_positive_k() -> None:
    with pytest.raises(ValueError, match="requires k > 0"):
        sem_local_topk(intent="Rank candidates", k=0)


def test_sem_lookup_join_request() -> None:
    source = object()
    req = sem_lookup_join(intent="Join with the most relevant memory", candidate_source=source)
    assert req.candidate_source is source


def test_sem_window_request() -> None:
    ctx = context("session")
    req = sem_window(context=ctx)
    assert req.context is ctx


def test_sem_topk_request() -> None:
    req = sem_topk(intent="Rank by relevance", k=3, context=context("window"))
    assert req.k == 3
    assert req.context.kind == "window"


def test_sem_groupby_request() -> None:
    req = sem_groupby(intent="Group by topic", context=context("semantic_segment"))
    assert req.context.kind == "semantic_segment"


def test_sem_agg_request_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError, match="Invalid sem_agg mode"):
        sem_agg(intent="Summarize session", mode="periodic", context=context("session"))


def test_top_level_public_surface_exposes_only_facade() -> None:
    assert "RuntimeConfig" not in public_all
    assert "TriggerPolicy" not in public_all
    assert "SemTopKConfig" not in public_all
    assert "build_sem_topk_pipeline" not in public_all
