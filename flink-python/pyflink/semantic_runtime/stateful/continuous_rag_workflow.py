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
    simple_key_selector,
)
from pyflink.semantic_runtime.stateful.semantic_window import (
    SemWindowConfig,
    SemWindowFunction,
)
from pyflink.semantic_runtime.stateful.sem_groupby_stateful import (
    SemGroupbyConfig,
    SemGroupbyFunction,
)
from pyflink.semantic_runtime.stateful.sem_agg_stateful import (
    SemAggConfig,
    SemAggFunction,
)
from pyflink.semantic_runtime.stateful.cts_retrieve import (
    CtsRetrieveConfig,
    CtsRetrieveFunction,
)
from pyflink.semantic_runtime.stateful.sem_topk_continuous import (
    SemTopKConfig,
    SemTopKFunction,
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
    agg_config: SemAggConfig = field(default_factory=SemAggConfig)

    # Subflow B configs
    retrieve_config: CtsRetrieveConfig = field(default_factory=CtsRetrieveConfig)
    topk_config: Optional[SemTopKConfig] = None  # None = skip rerank

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
            "memory_version": value.get("version", 0),
            "workflow_version": self._workflow_version,
            "config_version": self._config_version,
            "timestamp_ms": now_ms,
            # Carry forward upstream audit fields
            "total_candidates": value.get("total_candidates", 0),
            "retrieval_changed": value.get("changed", False),
        }


# ============================================================================
# Async merge functions — lightweight passthrough + async result forwarding
# ============================================================================

class _AsyncMergeFunction(KeyedProcessFunction):
    """Generic merge function for async bridge.

    Receives the union of an operator's main output and async results.
    Main output events are passed through.  Async results (identified by
    ``task_type``) are forwarded with the task_type preserved so the
    downstream operator can detect and handle them in its process_element.
    """

    def __init__(self, expected_task_types: Optional[List[str]] = None):
        self._expected_task_types = set(expected_task_types or [])

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if not isinstance(value, dict):
            yield value
            return

        task_type = value.get("task_type", "")

        if task_type and task_type in self._expected_task_types:
            # Async result — forward with task_type intact for downstream merge-back
            yield value
        else:
            # Regular main output — pass through
            yield value


# ============================================================================
# Subflow builders — individually testable composition functions
# ============================================================================

def _wire_async_bridge_if_configured(
    operator_ds: DataStream,
    async_fn,
    merge_task_types: List[str],
    config: ContinuousRAGConfig,
) -> DataStream:
    """Wire async bridge on an operator's output if an async function is provided.

    If ``async_fn`` is None, logs a warning about unhandled side outputs
    and returns the operator output as-is.
    """
    if async_fn is None:
        # Side outputs will be silently discarded by Flink if not captured.
        # Log warning for observability.
        logger.warning(
            "Async bridge not configured (no async_fn for task_types=%s). "
            "Side-output async work items will be discarded.",
            merge_task_types,
        )
        return operator_ds

    return build_async_bridge(
        main_ds=operator_ds,
        async_fn=async_fn,
        merge_fn=_AsyncMergeFunction(merge_task_types),
        key_selector=config.key_selector,
        timeout_ms=config.async_timeout_ms,
        capacity=config.async_capacity,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )


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
        SemGroupbyFunction(config.groupby_config),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Wire async bridge for sem_groupby classify side outputs
    grouped = _wire_async_bridge_if_configured(
        grouped_raw, config.classify_async_fn, ["classify"], config,
    )

    # Step 3: Semantic aggregation (re-key on group output)
    aggregated_raw = grouped.key_by(config.key_selector).process(
        SemAggFunction(config.agg_config),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Wire async bridge for sem_agg summarize side outputs
    aggregated = _wire_async_bridge_if_configured(
        aggregated_raw, config.summarize_async_fn, ["summarize"], config,
    )

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
        retrieved_raw, config.retrieve_async_fn, ["retrieve"], config,
    )

    # Step 2: Optional top-k reranking
    if config.topk_config is not None:
        reranked = retrieved.key_by(config.key_selector).process(
            SemTopKFunction(config.topk_config),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        return reranked

    return retrieved


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

