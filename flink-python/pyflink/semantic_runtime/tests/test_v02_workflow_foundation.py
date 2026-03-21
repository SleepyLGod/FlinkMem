#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""Focused workflow foundation tests for the V0.2 continuous RAG surface."""

from __future__ import annotations

import os
import pathlib

import pyflink as _pf

_SEM_RUNTIME_SRC = pathlib.Path(__file__).resolve().parents[1]
_SEM_RUNTIME_DST = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _SEM_RUNTIME_DST.exists():
    os.symlink(_SEM_RUNTIME_SRC, _SEM_RUNTIME_DST)

from pyflink.semantic_runtime.runtime_config import RuntimeConfig
from pyflink.semantic_runtime.semantic_spec import GroupbyQuerySpec, TopKQuerySpec
from pyflink.semantic_runtime.runtime.continuous_rag_components import (
    MEMORY_EVENT_TAG,
    QUERY_REQUEST_TAG,
    _AnswerSynthesiser,
    _StreamRouter,
)
from pyflink.semantic_runtime.runtime.continuous_rag_workflow import (
    ContinuousRAGConfig,
    build_continuous_rag_workflow_from_runtime_config,
    validate_rag_config,
)
from pyflink.semantic_runtime.runtime.sem_search import SemSearchConfig
from pyflink.semantic_runtime.operators.stateful.sem_agg import SemAggConfig
from pyflink.semantic_runtime.operators.stateful.sem_groupby import SemGroupbyConfig
from pyflink.semantic_runtime.operators.stateful.sem_topk import SemTopKConfig
from pyflink.semantic_runtime.operators.stateful.sem_window import SemWindowConfig


class _FakeContext:
    """Minimal mock for KeyedProcessFunction.Context."""

    def __init__(self, key: str = "test_key") -> None:
        self._key = key

        class _TimerService:
            def register_processing_time_timer(self, ts) -> None:
                return None

            def register_event_time_timer(self, ts) -> None:
                return None

        self._timer = _TimerService()

    def get_current_key(self) -> str:
        return self._key

    def timer_service(self):
        return self._timer

    def output(self, tag, value) -> None:
        return None


class TestContinuousRAGConfig:
    def test_defaults(self) -> None:
        cfg = ContinuousRAGConfig()
        assert cfg.workflow_version == "v0.2.0"
        assert cfg.topk_config is None
        assert cfg.groupby_query_spec is None
        assert cfg.async_timeout_ms == 30_000
        assert isinstance(cfg.window_config, SemWindowConfig)
        assert isinstance(cfg.groupby_config, SemGroupbyConfig)
        assert isinstance(cfg.agg_config, SemAggConfig)
        assert isinstance(cfg.retrieve_config, SemSearchConfig)

    def test_custom_configs(self) -> None:
        cfg = ContinuousRAGConfig(
            window_config=SemWindowConfig(max_window_events=10),
            groupby_query_spec=GroupbyQuerySpec(),
            groupby_config=SemGroupbyConfig(confidence_threshold=0.8),
            topk_config=SemTopKConfig(max_candidates=50),
            topk_query_spec=TopKQuerySpec(k=5),
            workflow_version="v0.2.1",
        )
        assert cfg.window_config.max_window_events == 10
        assert cfg.groupby_config.confidence_threshold == 0.8
        assert cfg.topk_query_spec.k == 5
        assert cfg.topk_config.max_candidates == 50
        assert cfg.workflow_version == "v0.2.1"

    def test_from_runtime_config_hydrates_typed_subconfigs(self) -> None:
        runtime_cfg = RuntimeConfig.from_dict(
            {
                "defaults": {
                    "ttl_seconds": 900,
                    "async_timeout_ms": 12345,
                    "async_capacity": 17,
                },
                "llm": {"backend": "mock", "model": "test-model"},
                "embedding": {"backend": "local_hashing", "dimensions": 48},
                "operators": {
                    "sem_window": {
                        "kernel": {
                            "max_window_events": 9,
                            "window_timeout_ms": 1111,
                            "boundary_flag": "topic_shift",
                        },
                    },
                    "sem_groupby": {
                        "query_spec": {},
                        "kernel": {
                            "max_groups_per_key": 6,
                            "confidence_threshold": 0.85,
                        },
                    },
                    "sem_agg": {
                        "query_spec": {
                            "agg_method": "summarize",
                        },
                        "kernel": {"flush_interval_ms": 2222},
                    },
                    "sem_search": {
                        "kernel": {
                            "max_candidates_per_request": 8,
                        },
                    },
                    "sem_topk": {
                        "query_spec": {
                            "k": 4,
                            "ranking_method": "pointwise",
                        },
                        "kernel": {"max_candidates": 12},
                    },
                },
            }
        )

        cfg = ContinuousRAGConfig.from_runtime_config(runtime_cfg)

        assert cfg.window_config.max_window_events == 9
        assert cfg.window_config.window_timeout_ms == 1111
        assert cfg.window_config.ttl_seconds == 900
        assert cfg.groupby_query_spec is not None
        assert cfg.groupby_config.confidence_threshold == 0.85
        assert cfg.groupby_config.max_groups_per_key == 6
        assert cfg.agg_query_spec is not None
        assert cfg.agg_query_spec.agg_method == "summarize"
        assert cfg.agg_config.flush_interval_ms == 2222
        assert cfg.retrieve_config.max_candidates_per_request == 8
        assert cfg.topk_query_spec is not None
        assert cfg.topk_query_spec.k == 4
        assert cfg.topk_config is not None
        assert cfg.topk_config.max_candidates == 12
        assert cfg.topk_llm_config.model == "test-model"
        assert cfg.topk_embedding_config is not None
        assert cfg.topk_embedding_config.dimensions == 48
        assert cfg.async_timeout_ms == 12345
        assert cfg.async_capacity == 17

    def test_from_runtime_config_allows_explicit_overrides(self) -> None:
        runtime_cfg = RuntimeConfig.from_dict({})
        custom_window = SemWindowConfig(max_window_events=3)
        cfg = ContinuousRAGConfig.from_runtime_config(
            runtime_cfg,
            window_config=custom_window,
            answer_prompt_template="Q={query}",
            answer_output_schema={"answer": str},
            workflow_version="v0.2.9",
            config_version="test_override",
        )
        assert cfg.window_config is custom_window
        assert cfg.answer_prompt_template == "Q={query}"
        assert cfg.answer_output_schema == {"answer": str}
        assert cfg.workflow_version == "v0.2.9"
        assert cfg.config_version == "test_override"

    def test_runtime_config_builder_helper_uses_typed_conversion(self) -> None:
        import pyflink.semantic_runtime.runtime.continuous_rag_workflow as rag_workflow

        runtime_cfg = RuntimeConfig.from_dict({})
        sentinel = object()

        def fake_builder(input_ds, config=None):
            return {"config": config, "input": input_ds}

        original = rag_workflow.build_continuous_rag_workflow
        rag_workflow.build_continuous_rag_workflow = fake_builder
        try:
            out = build_continuous_rag_workflow_from_runtime_config(
                sentinel,
                runtime_cfg,
                config_version="typed_test",
            )
        finally:
            rag_workflow.build_continuous_rag_workflow = original

        assert out["input"] is sentinel
        built_cfg = out["config"]
        assert isinstance(built_cfg, ContinuousRAGConfig)
        assert built_cfg.config_version == "typed_test"


class TestStreamRouter:
    def _route(self, event_dict):
        router = _StreamRouter()
        return list(router.process_element(event_dict, _FakeContext()))

    def test_memory_event_routed(self) -> None:
        results = self._route({"key": "k1", "stream_type": "memory_event", "payload": "hello"})
        assert len(results) == 1
        tag, value = results[0]
        assert tag == MEMORY_EVENT_TAG
        assert value["payload"] == "hello"

    def test_query_request_routed(self) -> None:
        results = self._route({"key": "k1", "stream_type": "query_request", "query": "what?"})
        assert len(results) == 1
        tag, value = results[0]
        assert tag == QUERY_REQUEST_TAG
        assert value["query"] == "what?"

    def test_unknown_type_defaults_to_memory(self) -> None:
        results = self._route({"key": "k1", "stream_type": "unknown", "payload": "x"})
        assert len(results) == 1
        tag, _ = results[0]
        assert tag == MEMORY_EVENT_TAG

    def test_missing_type_defaults_to_memory(self) -> None:
        results = self._route({"key": "k1", "payload": "x"})
        assert len(results) == 1
        tag, _ = results[0]
        assert tag == MEMORY_EVENT_TAG

    def test_non_dict_dropped(self) -> None:
        router = _StreamRouter()
        results = list(router.process_element("not_a_dict", _FakeContext()))
        assert len(results) == 0


class TestAnswerSynthesiser:
    def _synthesise(self, value_dict, template=None):
        prompt_template = template or "Context:\n{context}\n\nQuery:\n{query}\n\nAnswer:"
        synthesiser = _AnswerSynthesiser(
            prompt_template=prompt_template,
            workflow_version="v0.2.0",
            config_version="test_v1",
        )
        return list(synthesiser.process_element(value_dict, _FakeContext("k1")))

    def test_basic_synthesis(self) -> None:
        results = self._synthesise(
            {
                "key": "k1",
                "query": "What is Flink?",
                "retrieved_context": [
                    {"candidate_id": "c1", "payload": "Flink is a stream processor"},
                    {"candidate_id": "c2", "payload": "Flink supports stateful ops"},
                ],
            }
        )
        assert len(results) == 1
        out = results[0]
        assert out["stream_type"] == "answer_request"
        assert "Flink is a stream processor" in out["prompt"]
        assert "What is Flink?" in out["prompt"]
        assert out["retrieved_ids"] == ["c1", "c2"]
        assert out["workflow_version"] == "v0.2.0"
        assert out["config_version"] == "test_v1"

    def test_empty_context(self) -> None:
        results = self._synthesise(
            {
                "key": "k1",
                "query": "test?",
                "retrieved_context": [],
            }
        )
        assert len(results) == 1
        assert results[0]["retrieved_ids"] == []

    def test_topk_format(self) -> None:
        results = self._synthesise(
            {
                "key": "k1",
                "payload": "What about X?",
                "topk": [
                    {"candidate_id": "t1", "content": "X is interesting"},
                ],
                "version": 42,
                "total_candidates": 100,
                "changed": True,
            }
        )
        assert len(results) == 1
        out = results[0]
        assert out["retrieved_ids"] == ["t1"]
        assert out["memory_version"] == 42
        assert out["total_candidates"] == 100
        assert out["retrieval_changed"] is True

    def test_non_dict_dropped(self) -> None:
        synthesiser = _AnswerSynthesiser(prompt_template="{context}\n{query}")
        results = list(synthesiser.process_element("not_a_dict", _FakeContext()))
        assert len(results) == 0


class TestValidateRAGConfig:
    def test_valid_defaults(self) -> None:
        cfg = ContinuousRAGConfig()
        warnings = validate_rag_config(cfg)
        assert len(warnings) == 0

    def test_window_exceeds_agg_buffer(self) -> None:
        cfg = ContinuousRAGConfig(
            window_config=SemWindowConfig(max_window_events=200),
            agg_config=SemAggConfig(max_buffer_events=50),
        )
        warnings = validate_rag_config(cfg)
        assert any("window max_events" in warning for warning in warnings)

    def test_retrieve_exceeds_topk(self) -> None:
        cfg = ContinuousRAGConfig(
            retrieve_config=SemSearchConfig(max_candidates_per_request=50),
            topk_config=SemTopKConfig(max_candidates=10),
        )
        warnings = validate_rag_config(cfg)
        assert any("retrieve max_candidates_per_request" in warning for warning in warnings)

    def test_ttl_spread_warning(self) -> None:
        cfg = ContinuousRAGConfig(
            window_config=SemWindowConfig(ttl_seconds=100),
            agg_config=SemAggConfig(ttl_seconds=100),
            groupby_config=SemGroupbyConfig(ttl_seconds=100),
            retrieve_config=SemSearchConfig(ttl_seconds=10000),
        )
        warnings = validate_rag_config(cfg)
        assert any("TTL spread" in warning for warning in warnings)

    def test_summarize_no_flush_warning(self) -> None:
        cfg = ContinuousRAGConfig(
            agg_config=SemAggConfig(mode="summarize", flush_interval_ms=0),
        )
        warnings = validate_rag_config(cfg)
        assert any("flush_interval_ms" in warning for warning in warnings)
