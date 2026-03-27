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
      → key_by → sem_search → optional sem_topk rerank
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

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyflink.common import Types
from pyflink.datastream import DataStream

from pyflink.semantic_runtime.runtime.event_model import (
    simple_key_selector,
)
from pyflink.semantic_runtime.runtime import continuous_rag_components as _components
from pyflink.semantic_runtime.operators.stateful.sem_window_kernel import (
    SemWindowConfig,
    SemWindowFunction,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_kernel import (
    SemGroupbyConfig,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_pipeline import (
    build_sem_groupby_operator,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_kernel import (
    SemAggConfig,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_pipeline import (
    build_sem_agg_operator,
)
from pyflink.semantic_runtime.runtime.steps.sem_search import (
    SemSearchConfig,
    SemSearchFunction,
)
from pyflink.semantic_runtime.operators.stateful.sem_topk_kernel import (
    SemTopKConfig,
)
from pyflink.semantic_runtime.sem_spec import AggQuerySpec, GroupbyQuerySpec, TopKQuerySpec
from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.runtime_config import EmbeddingBackendConfig, RuntimeConfig
from pyflink.semantic_runtime.operators.stateful.sem_topk_pipeline import (
    build_sem_topk_pipeline,
)


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
    groupby_llm_config: Optional[LLMClientConfig] = None
    agg_config: SemAggConfig = field(default_factory=SemAggConfig)
    agg_query_spec: Optional[AggQuerySpec] = None
    agg_llm_config: Optional[LLMClientConfig] = None

    # Subflow B configs
    retrieve_config: SemSearchConfig = field(default_factory=SemSearchConfig)
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

    retrieve_async_fn: Optional[Any] = None    # For sem_search side output

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
            agg_llm_config=runtime_config.to_llm_client_config(),
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
            groupby_llm_config=runtime_config.to_llm_client_config(),
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
            retrieve_async_fn=retrieve_async_fn,
            workflow_version=workflow_version,
            config_version=config_version,
        ),
    )


def build_memory_subflow(
    memory_ds: DataStream,
    config: ContinuousRAGConfig,
) -> DataStream:
    """Subflow A: memory build pipeline.

    ``memory_events → key_by → sem_window → sem_groupby → sem_agg``

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
            llm_config=config.groupby_llm_config,
        ),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    grouped = grouped_raw

    grouped_for_agg = grouped.key_by(config.key_selector).process(
        _components._GroupbyToAggEnvelope(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Step 3: Semantic aggregation (re-key on group output)
    aggregated_raw = grouped_for_agg.key_by(config.key_selector).process(
        build_sem_agg_operator(
            config.agg_config,
            query_spec=config.agg_query_spec,
            input_kind="event_stream",
            llm_config=config.agg_llm_config,
        ),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    return aggregated_raw


def build_retrieval_subflow(
    query_ds: DataStream,
    config: ContinuousRAGConfig,
) -> DataStream:
    """Subflow B: query/retrieval pipeline.

    ``query_requests → key_by → sem_search (+ async bridge) → optional sem_topk``

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
        SemSearchFunction(config.retrieve_config),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Wire async bridge for sem_search external store retrieval
    retrieved = _components._wire_async_bridge_if_configured(
        retrieved_raw,
        _components._resolve_retrieve_async_fn(config),
        _components._RetrieveAsyncMergeFunction(),
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
                _components._RetrievalEnvelopeExpander(),
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
            _components._RetrievalToAnswerEnvelope(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        return normalized

    normalized = retrieved.key_by(config.key_selector).process(
        _components._RetrievalToAnswerEnvelope(),
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
    synthesiser = _components._AnswerSynthesiser(
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
        _components._StreamRouter(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    memory_ds = routed.get_side_output(_components.MEMORY_EVENT_TAG)
    query_ds = routed.get_side_output(_components.QUERY_REQUEST_TAG)

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
