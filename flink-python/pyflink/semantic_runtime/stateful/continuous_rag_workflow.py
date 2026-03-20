# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
continuous_rag_workflow — Composed continuous RAG pipeline over stateful operators.

This module wires the V0.2 stateful operators into a complete continuous RAG
workflow, modeled as a **composed topology** rather than a single monolithic
operator.

Subflow A (Memory Build)::

    input events
      → key_by → sem_window → sem_groupby → sem_agg
      → memory entries (stored in downstream state / sink)

Subflow B (Query / Retrieval)::

    query requests
      → key_by → cts_retrieve → optional sem_topk rerank
      → retrieved context

Subflow C (Answer Synthesis)::

    (query request ⊕ retrieved context)
      → sem_map (V0.1 async) → answer with audit fields

Routing
-------
All input events share the same keyed stream.  The workflow uses a
``stream_type`` field in each event dict to route:

- ``"memory_event"`` → Subflow A
- ``"query_request"`` → Subflow B → Subflow C

This is implemented via Flink ``OutputTag`` side-output splitting from a
lightweight router ``KeyedProcessFunction``.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyflink.common import Time, Types
from pyflink.datastream import AsyncDataStream, DataStream, OutputTag
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    RuntimeContext,
)

from pyflink.semantic_runtime.stateful.event_model import (
    SemanticEvent,
    group_assignment_to_semantic_event,
    retrieve_to_answer_context,
    retrieve_to_topk_items,
    topk_to_answer_context,
    simple_key_selector,
)
from pyflink.semantic_runtime.stateful.semantic_window import (
    SemWindowConfig,
    SemWindowFunction,
)
from pyflink.semantic_runtime.stateful.sem_groupby_stateful import (
    SemGroupbyConfig,
)
from pyflink.semantic_runtime.stateful.sem_groupby_pipeline import (
    build_sem_groupby_operator,
)
from pyflink.semantic_runtime.stateful.sem_agg_stateful import (
    SemAggConfig,
    SemAggFunction,
)
from pyflink.semantic_runtime.stateful.sem_agg_pipeline import (
    build_sem_agg_operator,
)
from pyflink.semantic_runtime.stateful.cts_retrieve import (
    CtsRetrieveConfig,
    CtsRetrieveFunction,
)
from pyflink.semantic_runtime.stateful.sem_topk_continuous import (
    SemTopKConfig,
)
from pyflink.semantic_runtime.semantic_spec import AggQuerySpec, GroupbyQuerySpec, TopKQuerySpec
from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.runtime_config import EmbeddingBackendConfig, RuntimeConfig
from pyflink.semantic_runtime.stateful.sem_topk_pipeline import (
    build_sem_topk_pipeline,
)
from pyflink.semantic_runtime.stateful.external_search_backend import (
    SearchBackendAsyncFn,
)
from pyflink.semantic_runtime.stateful.async_bridge import (
    ASYNC_WORK_TAG,
    build_async_bridge,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Output tags for stream routing
# ============================================================================

MEMORY_EVENT_TAG = OutputTag("memory_events", Types.PICKLED_BYTE_ARRAY())
QUERY_REQUEST_TAG = OutputTag("query_requests", Types.PICKLED_BYTE_ARRAY())


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class ContinuousRAGConfig:
    """Top-level configuration for the continuous RAG workflow.

    Bundles sub-operator configs and workflow-level settings.
    """

    # Subflow A configs
    window_config: SemWindowConfig = field(default_factory=SemWindowConfig)
    groupby_config: SemGroupbyConfig = field(default_factory=SemGroupbyConfig)
    groupby_query_spec: Optional[GroupbyQuerySpec] = None
    agg_config: SemAggConfig = field(default_factory=SemAggConfig)
    agg_query_spec: Optional[AggQuerySpec] = None

    # Subflow B configs
    retrieve_config: CtsRetrieveConfig = field(default_factory=CtsRetrieveConfig)
    topk_config: Optional[SemTopKConfig] = None  # None = skip rerank
    topk_query_spec: Optional[TopKQuerySpec] = None  # query-level params (k, version, …)
    topk_llm_config: Optional[LLMClientConfig] = None
    topk_embedding_config: Optional[EmbeddingBackendConfig] = None

    # Subflow C configs
    answer_prompt_template: str = (
        "Based on the following context, answer the query.\n\n"
        "Context:\n{context}\n\nQuery:\n{query}\n\nAnswer:"
    )
    answer_output_schema: Dict[str, type] = field(
        default_factory=lambda: {"answer": str, "confidence": float}
    )

    # Workflow-level
    key_selector: Callable = field(default_factory=lambda: simple_key_selector)
    async_timeout_ms: int = 30_000
    async_capacity: int = 20

    # Async bridge workers (None = side outputs are discarded with warning)
    # These should be AsyncFunction instances that process AsyncWorkItem dicts
    # and return AsyncResult dicts.
    classify_async_fn: Optional[Any] = None    # For sem_groupby side output
    summarize_async_fn: Optional[Any] = None   # For sem_agg side output
    retrieve_async_fn: Optional[Any] = None    # For cts_retrieve side output

    # Audit
    workflow_version: str = "v0.2.0"
    config_version: str = "default"

    @classmethod
    def from_runtime_config(
        cls,
        runtime_config: RuntimeConfig,
        *,
        window_config: Optional[SemWindowConfig] = None,
        answer_prompt_template: Optional[str] = None,
        answer_output_schema: Optional[Dict[str, type]] = None,
        key_selector: Callable = simple_key_selector,
        classify_async_fn: Optional[Any] = None,
        summarize_async_fn: Optional[Any] = None,
        retrieve_async_fn: Optional[Any] = None,
        workflow_version: str = "v0.2.0",
        config_version: str = "runtime_config",
    ) -> "ContinuousRAGConfig":
        """Build a workflow config from typed runtime config bundles.

        This is the main bridge from the V0.2++ typed config shell into the
        composed continuous RAG workflow.
        """

        base = cls()
        topk_bundle = runtime_config.resolve_topk_runtime_bundle()
        groupby_bundle = runtime_config.resolve_groupby_runtime_bundle(
            input_kind="window_snapshot",
        )
        agg_bundle = runtime_config.resolve_agg_runtime_bundle(
            input_kind="event_stream",
        )

        return cls(
            window_config=window_config or runtime_config.get_window_config(),
            groupby_config=groupby_bundle.kernel_config,
            groupby_query_spec=groupby_bundle.query_spec,
            agg_config=agg_bundle.kernel_config,
            agg_query_spec=agg_bundle.query_spec,
            retrieve_config=runtime_config.get_search_config(),
            topk_config=topk_bundle.kernel_config,
            topk_query_spec=topk_bundle.query_spec,
            topk_llm_config=runtime_config.to_llm_client_config(),
            topk_embedding_config=runtime_config.to_embedding_backend_config(),
            answer_prompt_template=answer_prompt_template or base.answer_prompt_template,
            answer_output_schema=answer_output_schema or dict(base.answer_output_schema),
            key_selector=key_selector,
            async_timeout_ms=runtime_config.defaults.async_timeout_ms,
            async_capacity=runtime_config.defaults.async_capacity,
            classify_async_fn=classify_async_fn,
            summarize_async_fn=summarize_async_fn,
            retrieve_async_fn=retrieve_async_fn,
            workflow_version=workflow_version,
            config_version=config_version,
        )


def build_continuous_rag_workflow_from_runtime_config(
    input_ds: DataStream,
    runtime_config: RuntimeConfig,
    *,
    window_config: Optional[SemWindowConfig] = None,
    answer_prompt_template: Optional[str] = None,
    answer_output_schema: Optional[Dict[str, type]] = None,
    key_selector: Callable = simple_key_selector,
    classify_async_fn: Optional[Any] = None,
    summarize_async_fn: Optional[Any] = None,
    retrieve_async_fn: Optional[Any] = None,
    workflow_version: str = "v0.2.0",
    config_version: str = "runtime_config",
) -> Dict[str, DataStream]:
    """Typed RuntimeConfig entry point for the composed workflow."""

    return build_continuous_rag_workflow(
        input_ds,
        ContinuousRAGConfig.from_runtime_config(
            runtime_config,
            window_config=window_config,
            answer_prompt_template=answer_prompt_template,
            answer_output_schema=answer_output_schema,
            key_selector=key_selector,
            classify_async_fn=classify_async_fn,
            summarize_async_fn=summarize_async_fn,
            retrieve_async_fn=retrieve_async_fn,
            workflow_version=workflow_version,
            config_version=config_version,
        ),
    )


# ============================================================================
# Stream Router — splits input into memory events vs query requests
# ============================================================================

class _StreamRouter(KeyedProcessFunction):
    """Lightweight router that splits incoming events by ``stream_type``.

    - ``stream_type == "memory_event"`` → side output ``MEMORY_EVENT_TAG``
    - ``stream_type == "query_request"`` → side output ``QUERY_REQUEST_TAG``
    - Unknown types → dropped with warning.

    This is a pure routing function with no state.
    """

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            logger.warning("StreamRouter: non-dict input dropped: %s", type(value))
            return

        stream_type = value.get("stream_type", "")
        if stream_type == "memory_event":
            yield MEMORY_EVENT_TAG, value
        elif stream_type == "query_request":
            yield QUERY_REQUEST_TAG, value
        else:
            # Default: treat as memory event
            logger.debug("StreamRouter: unknown stream_type '%s', routing as memory_event", stream_type)
            yield MEMORY_EVENT_TAG, value


# ============================================================================
# Answer Synthesiser — wraps retrieved context + query for Subflow C
# ============================================================================

class _AnswerSynthesiser(KeyedProcessFunction):
    """Combines retrieved context with the original query for answer synthesis.

    Input: dicts with ``stream_type == "retrieval_result"`` (from Subflow B)
    containing ``query``, ``retrieved_context``, and audit fields.

    Output: dicts ready for V0.1 ``sem_map`` async answer generation, or
    direct passthrough if no LLM is configured.
    """

    def __init__(
        self,
        prompt_template: str,
        workflow_version: str = "v0.2.0",
        config_version: str = "default",
    ) -> None:
        self._prompt_template = prompt_template
        self._workflow_version = workflow_version
        self._config_version = config_version

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            return

        now_ms = int(time.time() * 1000)
        query = value.get("query", value.get("payload", ""))
        context_items = value.get(
            "retrieved_context",
            value.get("topk", value.get("candidates", [])),
        )

        # Format context
        if isinstance(context_items, list):
            context_str = "\n".join(
                str(item.get("payload", item.get("content", str(item))))
                for item in context_items
            )
        else:
            context_str = str(context_items)

        # Build prompt
        prompt = self._prompt_template.format(
            context=context_str,
            query=query,
        )

        # Build audit envelope
        retrieved_ids = []
        if isinstance(context_items, list):
            for item in context_items:
                cid = item.get("candidate_id", item.get("id", ""))
                if cid:
                    retrieved_ids.append(cid)

        yield {
            "key": value.get("key", str(ctx.get_current_key())),
            "stream_type": "answer_request",
            "prompt": prompt,
            "query": query,
            "context_str": context_str,
            "retrieved_ids": retrieved_ids,
            "memory_version": value.get("memory_version", value.get("version", 0)),
            "workflow_version": self._workflow_version,
            "config_version": self._config_version,
            "timestamp_ms": now_ms,
            # Carry forward upstream audit fields
            "total_candidates": value.get(
                "total_candidates",
                value.get("candidate_count", 0),
            ),
            "retrieval_changed": value.get(
                "retrieval_changed",
                value.get("changed", False),
            ),
            "source": value.get("source", ""),
            "error": value.get("error", ""),
        }


# ============================================================================
# Async merge functions — stage-aware merge-back behavior
# ============================================================================

class _ClassifyAsyncMergeFunction(KeyedProcessFunction):
    """Merge classify async results into normalized group assignment envelopes."""

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            return
        if value.get("task_type") != "classify":
            yield value
            return

        if not value.get("success", False):
            error = value.get("error", "classify_async_failed")
            raise RuntimeError(f"classify async bridge failed: {error}")

        result = value.get("result", {})
        yield {
            "key": value.get("key", str(ctx.get_current_key())),
            "group_id": result.get("group_id", "__unclassified__"),
            "confidence": float(result.get("confidence", 0.0)),
            "source": "async_classify",
            "event_seq_id": int(result.get("event_seq_id", 0)),
            "payload": result.get("payload", ""),
            "request_id": value.get("request_id", ""),
            "metadata": {"async_task_type": "classify", "async_success": True},
        }


class _SummarizeAsyncMergeFunction(KeyedProcessFunction):
    """Merge summarize async results into sem_agg-like envelopes."""

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            return
        if value.get("task_type") != "summarize":
            yield value
            return

        now_ms = int(time.time() * 1000)
        if not value.get("success", False):
            error = value.get("error", "summarize_async_failed")
            raise RuntimeError(f"summarize async bridge failed: {error}")

        result = value.get("result", {})
        summary = result.get("summary", "")
        version = int(result.get("version", 0))
        yield {
            "key": value.get("key", str(ctx.get_current_key())),
            "aggregate": {
                "summary": summary,
                "version": version,
                "updated_ms": now_ms,
                "source": "async_summary",
            },
            "version": version,
            "mode": "summarize_async",
            "event_count": int(result.get("event_count", 0)),
            "timestamp_ms": now_ms,
        }


class _RetrieveAsyncMergeFunction(KeyedProcessFunction):
    """Merge retrieve async results into normalized retrieval envelopes."""

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            return
        if value.get("task_type") != "retrieve":
            yield value
            return

        now_ms = int(time.time() * 1000)
        payload = value.get("payload", {})
        if not value.get("success", False):
            error = value.get("error", "retrieve_async_failed")
            raise RuntimeError(f"retrieve async bridge failed: {error}")

        result = value.get("result", {})
        candidates = result.get("candidates", [])
        yield {
            "key": value.get("key", str(ctx.get_current_key())),
            "query": result.get("query", payload.get("query", "")),
            "query_seq_id": int(result.get("event_seq_id", payload.get("event_seq_id", 0))),
            "candidates": candidates,
            "candidate_count": len(candidates),
            "truncated": bool(result.get("truncated", False)),
            "source": "async_retrieve",
            "timestamp_ms": now_ms,
        }


class _GroupbyToAggEnvelope(KeyedProcessFunction):
    """Normalize sem_groupby outputs into SemanticEvent envelopes for sem_agg."""

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            return
        if {"key", "payload", "seq_id"}.issubset(value.keys()):
            yield value
            return
        yield group_assignment_to_semantic_event(value)


class _RetrievalToAnswerEnvelope(KeyedProcessFunction):
    """Normalize retrieval/top-k outputs into answer-input envelopes."""

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            return
        if "retrieved_context" in value:
            yield value
            return

        if "topk" in value or "top_items" in value:
            out = topk_to_answer_context(value, query_payload=value.get("query", ""))
            out["memory_version"] = value.get("version", 0)
            out["error"] = value.get("error", "")
            yield out
            return

        out = retrieve_to_answer_context(value)
        out["memory_version"] = value.get("version", 0)
        out["error"] = value.get("error", "")
        yield out


class _RetrievalEnvelopeExpander(KeyedProcessFunction):
    """Expand retrieval envelopes into flat scored candidate dicts.

    This adapter sits between ``cts_retrieve`` (which emits
    ``{"candidates": [...], "query": ..., ...}``) and ``SemTopKFunction``
    (which only accepts individual scored candidate dicts).

    For inputs that are already flat candidate dicts (no ``"candidates"``
    key), they are yielded through unchanged.
    """

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            return
        if "candidates" in value and isinstance(value["candidates"], list):
            items = retrieve_to_topk_items(value)
            if items:
                for item in items:
                    yield item
            else:
                # Empty candidate pools are passed through for downstream
                # normalization and auditing.
                yield value
        else:
            # Already a flat candidate dict — pass through
            yield value


# ============================================================================
# Subflow builders — individually testable composition functions
# ============================================================================

def _wire_async_bridge_if_configured(
    operator_ds: DataStream,
    async_fn,
    merge_fn: KeyedProcessFunction,
    stage_name: str,
    config: ContinuousRAGConfig,
) -> DataStream:
    """Wire async bridge on an operator's output.

    Missing async workers are treated as configuration errors and fail fast.
    """
    if async_fn is None:
        raise ValueError(
            f"Async bridge not configured for stage {stage_name!r}"
        )

    return build_async_bridge(
        main_ds=operator_ds,
        async_fn=async_fn,
        merge_fn=merge_fn,
        key_selector=config.key_selector,
        timeout_ms=config.async_timeout_ms,
        capacity=config.async_capacity,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )


def _groupby_needs_classify_bridge(config: ContinuousRAGConfig) -> bool:
    """Return whether the configured sem_groupby path can emit classify work."""
    query_spec = config.groupby_query_spec
    if query_spec is not None:
        return query_spec.assignment_method in {"llm", "llm_refine"}
    return False


def _agg_needs_summarize_bridge(config: ContinuousRAGConfig) -> bool:
    """Return whether the configured sem_agg path can emit summarize work."""
    query_spec = config.agg_query_spec
    if query_spec is not None:
        return query_spec.agg_method in {"summarize", "compressive"}
    return config.agg_config.mode in {"summarize", "compressive"}


def _resolve_retrieve_async_fn(config: ContinuousRAGConfig):
    """Resolve the retrieve async worker from config."""
    if config.retrieve_async_fn is not None:
        return config.retrieve_async_fn

    backend = getattr(config.retrieve_config, "search_backend", None)
    if backend is not None:
        return SearchBackendAsyncFn(backend)
    return None


def build_memory_subflow(
    memory_ds: DataStream,
    config: ContinuousRAGConfig,
) -> DataStream:
    """Subflow A: memory build pipeline.

    ``memory_events → key_by → sem_window → sem_groupby (+ async bridge) → sem_agg (+ async bridge)``

    Parameters
    ----------
    memory_ds : DataStream
        Stream of memory events (dict-shaped, ``stream_type == "memory_event"``).
    config : ContinuousRAGConfig
        Workflow configuration.

    Returns
    -------
    DataStream
        Aggregated memory entries.
    """
    keyed = memory_ds.key_by(config.key_selector)

    # Step 1: Semantic windowing
    windowed = keyed.process(
        SemWindowFunction(config.window_config),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Step 2: Semantic grouping (re-key on window output)
    grouped_raw = windowed.key_by(config.key_selector).process(
        build_sem_groupby_operator(
            config.groupby_config,
            query_spec=config.groupby_query_spec,
            input_kind="window_snapshot",
        ),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Wire async bridge for sem_groupby classify side outputs
    if _groupby_needs_classify_bridge(config):
        grouped = _wire_async_bridge_if_configured(
            grouped_raw,
            config.classify_async_fn,
            _ClassifyAsyncMergeFunction(),
            "classify",
            config,
        )
    else:
        grouped = grouped_raw

    grouped_for_agg = grouped.key_by(config.key_selector).process(
        _GroupbyToAggEnvelope(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Step 3: Semantic aggregation (re-key on group output)
    aggregated_raw = grouped_for_agg.key_by(config.key_selector).process(
        build_sem_agg_operator(
            config.agg_config,
            query_spec=config.agg_query_spec,
            input_kind="event_stream",
        ),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Wire async bridge for sem_agg summarize side outputs
    if _agg_needs_summarize_bridge(config):
        aggregated = _wire_async_bridge_if_configured(
            aggregated_raw,
            config.summarize_async_fn,
            _SummarizeAsyncMergeFunction(),
            "summarize",
            config,
        )
    else:
        aggregated = aggregated_raw

    return aggregated


def build_retrieval_subflow(
    query_ds: DataStream,
    config: ContinuousRAGConfig,
) -> DataStream:
    """Subflow B: query/retrieval pipeline.

    ``query_requests → key_by → cts_retrieve (+ async bridge) → optional sem_topk``

    Parameters
    ----------
    query_ds : DataStream
        Stream of query requests (dict-shaped, ``stream_type == "query_request"``).
    config : ContinuousRAGConfig
        Workflow configuration.

    Returns
    -------
    DataStream
        Retrieved (and optionally re-ranked) context.
    """
    keyed = query_ds.key_by(config.key_selector)

    # Step 1: Continuous retrieval
    retrieved_raw = keyed.process(
        CtsRetrieveFunction(config.retrieve_config),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Wire async bridge for cts_retrieve external store fallback
    retrieved = _wire_async_bridge_if_configured(
        retrieved_raw,
        _resolve_retrieve_async_fn(config),
        _RetrieveAsyncMergeFunction(),
        "retrieve",
        config,
    )

    # Step 2: Optional top-k reranking
    if config.topk_config is not None:
        topk_query_spec = config.topk_query_spec or TopKQuerySpec()
        # Pointwise top-k consumes flat candidates. Bounded-pool pairwise/listwise
        # consumes retrieval envelopes directly so that the pool boundary remains
        # explicit. This keeps the pure kernel independent from workflow envelopes
        # while preserving a clear rerank boundary for contextual methods.
        if topk_query_spec.ranking_method == "pointwise":
            topk_input = retrieved.key_by(config.key_selector).process(
                _RetrievalEnvelopeExpander(),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
        else:
            topk_input = retrieved
        reranked_or_passthrough = build_sem_topk_pipeline(
            topk_input,
            key_selector=config.key_selector,
            topk_config=config.topk_config,
            query_spec=topk_query_spec,
            llm_config=config.topk_llm_config,
            embedding_config=config.topk_embedding_config,
            async_timeout_ms=config.async_timeout_ms,
            async_capacity=config.async_capacity,
        )
        normalized = reranked_or_passthrough.key_by(config.key_selector).process(
            _RetrievalToAnswerEnvelope(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        return normalized

    normalized = retrieved.key_by(config.key_selector).process(
        _RetrievalToAnswerEnvelope(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    return normalized


def build_answer_subflow(
    retrieval_ds: DataStream,
    config: ContinuousRAGConfig,
) -> DataStream:
    """Subflow C: answer synthesis with audit fields.

    ``retrieval_results → key_by → answer_synthesiser``

    The output contains the structured prompt, audit fields (``memory_version``,
    ``retrieved_ids``, ``workflow_version``, ``config_version``), and is ready
    for downstream V0.1 ``sem_map`` async LLM processing.

    Parameters
    ----------
    retrieval_ds : DataStream
        Stream of retrieval results from Subflow B.
    config : ContinuousRAGConfig
        Workflow configuration.

    Returns
    -------
    DataStream
        Answer requests with full audit envelope.
    """
    synthesiser = _AnswerSynthesiser(
        prompt_template=config.answer_prompt_template,
        workflow_version=config.workflow_version,
        config_version=config.config_version,
    )

    answer_ds = retrieval_ds.key_by(config.key_selector).process(
        synthesiser,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    return answer_ds


# ============================================================================
# Top-level workflow builder
# ============================================================================

def build_continuous_rag_workflow(
    input_ds: DataStream,
    config: Optional[ContinuousRAGConfig] = None,
) -> Dict[str, DataStream]:
    """Build the complete continuous RAG workflow topology.

    Wires Subflows A, B, and C from a single input stream.

    Parameters
    ----------
    input_ds : DataStream
        Mixed input stream containing both memory events and query requests.
        Each element must be a dict with a ``stream_type`` field.
    config : ContinuousRAGConfig, optional
        Full workflow config. Uses defaults if not provided.

    Returns
    -------
    dict[str, DataStream]
        A dict with keys:
        - ``"memory"``   : Subflow A output (aggregated memory entries)
        - ``"retrieval"`` : Subflow B output (retrieved context)
        - ``"answers"``  : Subflow C output (answer requests with audit)
        - ``"routed"``   : The raw router output (for debugging / metrics)
    """
    if config is None:
        config = ContinuousRAGConfig()

    # 1. Route input stream
    routed = input_ds.key_by(config.key_selector).process(
        _StreamRouter(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    memory_ds = routed.get_side_output(MEMORY_EVENT_TAG)
    query_ds = routed.get_side_output(QUERY_REQUEST_TAG)

    # 2. Subflow A: memory build
    memory_out = build_memory_subflow(memory_ds, config)

    # 3. Subflow B: query / retrieval
    retrieval_out = build_retrieval_subflow(query_ds, config)

    # 4. Subflow C: answer synthesis
    answer_out = build_answer_subflow(retrieval_out, config)

    return {
        "memory": memory_out,
        "retrieval": retrieval_out,
        "answers": answer_out,
        "routed": routed,
    }


# ============================================================================
# Utility: validate workflow config consistency
# ============================================================================

def validate_rag_config(config: ContinuousRAGConfig) -> List[str]:
    """Check a ContinuousRAGConfig for common misconfigurations.

    Returns a list of warning messages (empty if all OK).
    """
    warnings = []

    # Check agg mode consistency
    if config.agg_config.mode == "summarize" and config.agg_config.flush_interval_ms <= 0:
        warnings.append(
            "sem_agg in summarize mode with flush_interval_ms <= 0: "
            "summarization will only trigger on buffer overflow."
        )

    # Check window vs agg buffer sizing
    if config.window_config.max_window_events > config.agg_config.max_buffer_events:
        warnings.append(
            f"window max_events ({config.window_config.max_window_events}) > "
            f"agg max_buffer ({config.agg_config.max_buffer_events}): "
            "window snapshots may exceed agg buffer capacity."
        )

    # Check retrieve cache vs topk candidates
    if config.topk_config is not None:
        if config.retrieve_config.max_candidates_per_request > config.topk_config.max_candidates:
            warnings.append(
                f"retrieve max_candidates_per_request ({config.retrieve_config.max_candidates_per_request}) > "
                f"topk max_candidates ({config.topk_config.max_candidates}): "
                "retrieval results may exceed topk buffer capacity."
            )

    # Check TTL consistency
    ttls = {
        "window": config.window_config.ttl_seconds,
        "groupby": config.groupby_config.ttl_seconds,
        "agg": config.agg_config.ttl_seconds,
        "retrieve": config.retrieve_config.ttl_seconds,
    }
    if config.topk_config:
        ttls["topk"] = config.topk_config.ttl_seconds

    min_ttl = min(ttls.values())
    max_ttl = max(ttls.values())
    if max_ttl > 4 * min_ttl:
        warnings.append(
            f"TTL spread is wide ({min_ttl}s–{max_ttl}s): "
            "downstream operators may expire state while upstream still holds it."
        )

    return warnings
