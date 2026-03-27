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

"""Planner/builder for ``sem_topk``.

This module keeps only orchestration logic. Worker classes, contextual plans,
and scoring helpers live in ``sem_topk_worker.py`` so the builder stays small
and path selection remains easy to audit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from pyflink.common import Types
from pyflink.datastream import AsyncDataStream, DataStream

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.runtime_config import EmbeddingBackendConfig
from pyflink.semantic_runtime.sem_spec import TopKQuerySpec
from pyflink.semantic_runtime.runtime.event_model import retrieve_to_topk_items
from pyflink.semantic_runtime.operators.stateful.sem_topk_kernel import (
    SemTopKConfig,
    SemTopKFunction,
    resolve_topk_persistence_policy,
)
from pyflink.semantic_runtime.operators.stateful.sem_topk_bounded import (
    ScopedPersistentSemTopKFunction,
)
from pyflink.semantic_runtime.operators.stateful.sem_topk_scope_runtime import SemTopKScopeSnapshotFunction
from pyflink.semantic_runtime.runtime.plans import (
    SemLoweringPlan,
    resolve_topk_lowering_plan,
)
from pyflink.semantic_runtime.operators.stateful.sem_topk_worker import (
    TopKContextualPlan,
    _BoundedPoolEmbeddingTopKSnapshotWorker,
    _BoundedPoolExternalScoreRerankerWorker,
    _BoundedPoolExternalTopKSnapshotWorker,
    _BoundedPoolLLMTopKSnapshotWorker,
    _EmbeddingScorerWorker,
    _PointwiseLLMScorerWorker,
    _raise_missing_external_score,
    _raise_topk_shape_mismatch,
    build_contextual_snapshot_query_spec,
    derive_topk_contextual_plan,
    is_topk_candidate_pool,
    is_topk_candidate_record,
    topk_candidate_has_score,
    topk_candidate_needs_scoring,
)


@dataclass(frozen=True)
class TopKExecutionPlan:
    """Resolved internal execution plan for ``sem_topk``."""

    scope_source: str = "internal_scope"
    persistence_policy: str = "persistent_across_scopes"
    input_kind: str = "event_stream"
    lowering_plan: Optional[SemLoweringPlan] = None


def resolve_topk_execution_plan(
    query_spec: Optional[TopKQuerySpec] = None,
    *,
    config: Optional[SemTopKConfig] = None,
    input_kind: str = "event_stream",
) -> TopKExecutionPlan:
    """Resolve scope source and persistence policy for ``sem_topk``."""
    spec = query_spec or TopKQuerySpec()
    kernel_config = config or SemTopKConfig()
    scope_source = "external_window" if input_kind == "window_snapshot" else "internal_scope"
    persistence_policy = resolve_topk_persistence_policy(
        kernel_config,
        scope_source=scope_source,
    )
    return TopKExecutionPlan(
        scope_source=scope_source,
        persistence_policy=persistence_policy,
        input_kind=input_kind,
        lowering_plan=resolve_topk_lowering_plan(
            spec,
            input_kind=input_kind,
            persistence_policy=persistence_policy,
        ),
    )


def _make_scoped_topk_snapshot_worker(
    *,
    query_spec: TopKQuerySpec,
    topk_config: SemTopKConfig,
    llm_config: Optional[LLMClientConfig],
    embedding_config: Optional[EmbeddingBackendConfig],
):
    """Build one scoped top-k snapshot worker for the active backend."""
    backend = topk_config.scorer_backend
    score_field = topk_config.score_field
    if backend == "llm":
        if llm_config is None:
            raise ValueError("llm_config is required for sem_topk backend='llm'")
        return _BoundedPoolLLMTopKSnapshotWorker(
            query_spec,
            llm_config,
            score_field,
            rerank_chunk_size=topk_config.rerank_chunk_size,
        )
    if backend == "embedding":
        return _BoundedPoolEmbeddingTopKSnapshotWorker(
            query_spec,
            embedding_config,
            score_field,
            rerank_chunk_size=topk_config.rerank_chunk_size,
        )
    if backend == "external_score":
        return _BoundedPoolExternalTopKSnapshotWorker(
            query_spec,
            score_field,
            rerank_chunk_size=topk_config.rerank_chunk_size,
        )
    raise ValueError(f"Unsupported sem_topk backend: {backend!r}")

def build_sem_topk_pipeline(
    input_ds: DataStream,
    *,
    key_selector: Callable,
    topk_config: SemTopKConfig,
    query_spec: TopKQuerySpec,
    llm_config: Optional[LLMClientConfig] = None,
    embedding_config: Optional[EmbeddingBackendConfig] = None,
    async_timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Build the scored-candidate pipeline around the pure top-k kernel.

    Parameters
    ----------
    input_ds : DataStream
        Stream of flat candidates and/or passthrough dicts.
    key_selector : callable
        Key selector for the final keyed top-k kernel.
    topk_config : SemTopKConfig
        Kernel configuration.
    query_spec : TopKQuerySpec
        Continuous top-k query definition.
    llm_config : LLMClientConfig, optional
        Required when ``topk_config.scorer_backend == "llm"``.
    embedding_config : EmbeddingBackendConfig, optional
        Optional embedding scorer config. Current implementation supports
        explicit local embedding-style backends such as
        ``backend="local_hashing"`` / ``"local_lexical"``, plus
        ``backend="mock"`` for tests.
    async_timeout_ms : int
        Async scoring timeout in milliseconds.
    async_capacity : int
        Max concurrent async scoring requests.
    """
    score_field = topk_config.score_field
    backend = topk_config.scorer_backend
    trigger_mode = query_spec.trigger_policy.mode
    supported_scope_close_kinds = {"session", "tumbling", "semantic"}

    if trigger_mode not in {"on_event", "on_scope_close", "periodic", "idle_flush", "count_threshold"}:
        raise ValueError(
            "sem_topk currently supports only trigger_policy.mode in "
            "{'on_event', 'on_scope_close', 'periodic', 'idle_flush', 'count_threshold'}"
        )

    pools = input_ds.filter(
        lambda v: is_topk_candidate_pool(v),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    nonempty_pools = pools.filter(
        lambda v: len(v.get("candidates", [])) > 0,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    empty_pools = pools.filter(
        lambda v: len(v.get("candidates", [])) == 0,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    flat_candidates = input_ds.filter(
        lambda v: is_topk_candidate_record(v),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    passthrough = input_ds.filter(
        lambda v: not is_topk_candidate_pool(v) and not is_topk_candidate_record(v),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    def _make_final_pool_worker():
        return _make_scoped_topk_snapshot_worker(
            query_spec=query_spec,
            topk_config=topk_config,
            llm_config=llm_config,
            embedding_config=embedding_config,
        )

    if query_spec.ranking_method in {"pairwise", "listwise"}:
        contextual_plan = derive_topk_contextual_plan(query_spec, topk_config)

        snapshot_worker = _make_scoped_topk_snapshot_worker(
            query_spec=query_spec,
            topk_config=topk_config,
            llm_config=llm_config,
            embedding_config=embedding_config,
        )
        snapshot_query_spec = build_contextual_snapshot_query_spec(
            query_spec, topk_config, contextual_plan
        )
        pool_emitter = flat_candidates.key_by(key_selector).process(
            SemTopKScopeSnapshotFunction(topk_config, snapshot_query_spec),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        flat_results = AsyncDataStream.unordered_wait(
            pool_emitter, snapshot_worker, async_timeout_ms, async_capacity,
        )

        if trigger_mode in {"periodic", "idle_flush", "count_threshold"}:
            incompatible_pools = pools.map(
                lambda v: _raise_topk_shape_mismatch(
                    "topk_contextual_timer_triggers_require_flat_candidates"
                ),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            return flat_results.union(passthrough.union(incompatible_pools))

        pool_results = AsyncDataStream.unordered_wait(
            nonempty_pools, snapshot_worker, async_timeout_ms, async_capacity,
        )
        return pool_results.union(flat_results).union(passthrough.union(empty_pools))
    else:
        pool_results = None
        passthrough_total = passthrough.union(empty_pools)
        if trigger_mode == "on_scope_close":
            final_worker = _make_final_pool_worker()
            pool_results = AsyncDataStream.unordered_wait(
                nonempty_pools, final_worker, async_timeout_ms, async_capacity,
            )
            if query_spec.scope_policy.window_kind not in supported_scope_close_kinds:
                incompatible_flat = flat_candidates.map(
                    lambda v: _raise_topk_shape_mismatch(
                        "topk_pointwise_scope_close_requires_close_capable_scope"
                    ),
                    output_type=Types.PICKLED_BYTE_ARRAY(),
                )
                return pool_results.union(passthrough_total).union(incompatible_flat)
            candidate_stream = flat_candidates
        elif trigger_mode in {"periodic", "idle_flush", "count_threshold"}:
            incompatible_pools = pools.map(
                lambda v: _raise_topk_shape_mismatch(
                    "topk_pointwise_timer_triggers_require_flat_candidates"
                ),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            candidate_stream = flat_candidates
            passthrough_total = passthrough.union(incompatible_pools)
        else:
            expanded_from_pools = nonempty_pools.flat_map(
                lambda v: retrieve_to_topk_items(v),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            candidate_stream = flat_candidates.union(expanded_from_pools)

        if backend == "external_score":
            ready = candidate_stream.filter(
                lambda v: topk_candidate_has_score(v, score_field),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            missing = candidate_stream.filter(
                lambda v: not topk_candidate_has_score(v, score_field),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            ).map(
                lambda v: _raise_missing_external_score(score_field),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            passthrough_total = passthrough_total.union(missing)
            kernel_input = ready
        else:
            ready = candidate_stream.filter(
                lambda v: not topk_candidate_needs_scoring(
                    v,
                    query_version=query_spec.query_version,
                    score_backend=backend,
                    score_field=score_field,
                ),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            to_score = candidate_stream.filter(
                lambda v: topk_candidate_needs_scoring(
                    v,
                    query_version=query_spec.query_version,
                    score_backend=backend,
                    score_field=score_field,
                ),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )

            if backend == "llm":
                if llm_config is None:
                    raise ValueError("llm_config is required for sem_topk backend='llm'")
                scorer_fn = _PointwiseLLMScorerWorker(query_spec, llm_config, score_field)
            elif backend == "embedding":
                scorer_fn = _EmbeddingScorerWorker(query_spec, embedding_config, score_field)
            else:
                raise ValueError(f"Unsupported sem_topk backend: {backend!r}")

            scored_ready = AsyncDataStream.unordered_wait(
                to_score, scorer_fn, async_timeout_ms, async_capacity,
            )
            kernel_input = ready.union(scored_ready)

    reranked = kernel_input.key_by(key_selector).process(
        SemTopKFunction(topk_config, query_spec),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    out = reranked.union(passthrough_total)
    if pool_results is not None:
        out = out.union(pool_results)
    return out


def build_scoped_persistent_topk_pipeline(
    input_ds: DataStream,
    *,
    key_selector: Callable,
    topk_config: SemTopKConfig,
    query_spec: TopKQuerySpec,
    llm_config: Optional[LLMClientConfig] = None,
    embedding_config: Optional[EmbeddingBackendConfig] = None,
    async_timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Build one continuous top-k from scope-level top-k contributions."""
    snapshot_worker = _make_scoped_topk_snapshot_worker(
        query_spec=query_spec,
        topk_config=topk_config,
        llm_config=llm_config,
        embedding_config=embedding_config,
    )
    scope_snapshots = AsyncDataStream.unordered_wait(
        input_ds,
        snapshot_worker,
        async_timeout_ms,
        async_capacity,
    )
    return scope_snapshots.key_by(key_selector).process(
        ScopedPersistentSemTopKFunction(topk_config, query_spec),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )


def build_external_window_persistent_topk_pipeline(
    input_ds: DataStream,
    *,
    key_selector: Callable,
    topk_config: SemTopKConfig,
    query_spec: TopKQuerySpec,
    llm_config: Optional[LLMClientConfig] = None,
    embedding_config: Optional[EmbeddingBackendConfig] = None,
    async_timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Build the external-window persistent top-k runtime.

    Input must be a bounded candidate pool stream with stable ``scope_id``.
    Each scope fire is reranked independently and then merged into one
    continuous cross-scope frontier by replacing that scope's prior
    contribution.
    """
    return build_scoped_persistent_topk_pipeline(
        input_ds,
        key_selector=key_selector,
        topk_config=topk_config,
        query_spec=query_spec,
        llm_config=llm_config,
        embedding_config=embedding_config,
        async_timeout_ms=async_timeout_ms,
        async_capacity=async_capacity,
    )


def build_internal_scope_persistent_contextual_topk_pipeline(
    input_ds: DataStream,
    *,
    key_selector: Callable,
    topk_config: SemTopKConfig,
    query_spec: TopKQuerySpec,
    llm_config: Optional[LLMClientConfig] = None,
    embedding_config: Optional[EmbeddingBackendConfig] = None,
    async_timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Build persistent contextual top-k over operator-owned scopes."""
    if query_spec.ranking_method not in {"pairwise", "listwise"}:
        raise ValueError(
            "internal_scope persistent contextual top-k requires ranking_method in "
            "{'pairwise', 'listwise'}"
        )

    contextual_plan = derive_topk_contextual_plan(query_spec, topk_config)
    snapshot_query_spec = build_contextual_snapshot_query_spec(
        query_spec,
        topk_config,
        contextual_plan,
    )
    scope_pools = input_ds.key_by(key_selector).process(
        SemTopKScopeSnapshotFunction(topk_config, snapshot_query_spec),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    return build_scoped_persistent_topk_pipeline(
        scope_pools,
        key_selector=key_selector,
        topk_config=topk_config,
        query_spec=query_spec,
        llm_config=llm_config,
        embedding_config=embedding_config,
        async_timeout_ms=async_timeout_ms,
        async_capacity=async_capacity,
    )
