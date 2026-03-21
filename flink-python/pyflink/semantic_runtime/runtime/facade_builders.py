"""Internal builders for public semantic facade requests.

This module lowers public request objects into internal operator plans and then
builds concrete runtime kernels. It is not part of the user-facing API.
"""

from __future__ import annotations

from typing import Callable

from pyflink.datastream import DataStream

from pyflink.semantic_runtime.public_api import (
    SemAggRequest,
    SemFilterRequest,
    SemGroupbyRequest,
    SemLookupJoinRequest,
    SemLocalTopKRequest,
    SemMapRequest,
    SemTopKRequest,
    SemWindowRequest,
)
from pyflink.semantic_runtime.runtime.plans import (
    lower_sem_agg_request,
    lower_sem_groupby_request,
    lower_sem_topk_request,
    lower_sem_window_request,
)
from pyflink.semantic_runtime.runtime.pushdown import (
    apply_sem_agg_pushdown,
    apply_sem_filter_pushdown,
    apply_sem_groupby_pushdown,
    apply_sem_local_topk_pushdown,
    apply_sem_lookup_join_pushdown,
    apply_sem_map_pushdown,
    apply_sem_topk_pushdown,
)
from pyflink.semantic_runtime.runtime_config import RuntimeConfig


def apply_sem_map_from_request(
    input_ds: DataStream,
    *,
    request: SemMapRequest,
    runtime_config: RuntimeConfig,
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply a row-level semantic map request through the pushdown path."""
    return apply_sem_map_pushdown(
        input_ds,
        request=request,
        runtime_config=runtime_config,
        timeout_ms=timeout_ms,
        async_capacity=async_capacity,
    )


def apply_sem_filter_from_request(
    input_ds: DataStream,
    *,
    request: SemFilterRequest,
    runtime_config: RuntimeConfig,
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply a row-level semantic filter request through the pushdown path."""
    return apply_sem_filter_pushdown(
        input_ds,
        request=request,
        runtime_config=runtime_config,
        timeout_ms=timeout_ms,
        async_capacity=async_capacity,
    )


def apply_sem_local_topk_from_request(
    input_ds: DataStream,
    *,
    request: SemLocalTopKRequest,
    runtime_config: RuntimeConfig,
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
    candidates_field: str = "candidates",
) -> DataStream:
    """Apply a row-level local semantic top-k request through the pushdown path."""
    return apply_sem_local_topk_pushdown(
        input_ds,
        request=request,
        runtime_config=runtime_config,
        timeout_ms=timeout_ms,
        async_capacity=async_capacity,
        candidates_field=candidates_field,
    )


def apply_sem_lookup_join_from_request(
    input_ds: DataStream,
    *,
    request: SemLookupJoinRequest,
    runtime_config: RuntimeConfig,
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply a row-level semantic lookup join request through the pushdown path."""
    return apply_sem_lookup_join_pushdown(
        input_ds,
        request=request,
        runtime_config=runtime_config,
        timeout_ms=timeout_ms,
        async_capacity=async_capacity,
    )


def build_sem_window_from_request(
    request: SemWindowRequest,
    runtime_config: RuntimeConfig,
):
    """Build a semantic window runtime from a public request."""
    from pyflink.semantic_runtime.operators.stateful.sem_window import SemWindowFunction

    plan = lower_sem_window_request(request, runtime_config)
    return SemWindowFunction(plan.kernel_config)


def build_sem_topk_from_request(
    input_ds: DataStream,
    *,
    key_selector: Callable,
    request: SemTopKRequest,
    runtime_config: RuntimeConfig,
    async_timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Build a stateful semantic top-k pipeline from a public request."""
    from pyflink.semantic_runtime.operators.stateful.sem_topk_pipeline import (
        build_sem_topk_pipeline,
    )

    plan = lower_sem_topk_request(request, runtime_config)
    if plan.context_kind == "window" and plan.query_spec.ranking_method == "pointwise":
        return apply_sem_topk_pushdown(
            input_ds,
            request=request,
            runtime_config=runtime_config,
            timeout_ms=async_timeout_ms,
            async_capacity=async_capacity,
        )

    llm_config = None
    if plan.kernel_config.scorer_backend == "llm":
        llm_config = runtime_config.get_operator_llm_client_config(
            "sem_topk",
            allow_query_spec=True,
        )

    embedding_config = runtime_config.to_embedding_backend_config()
    return build_sem_topk_pipeline(
        input_ds,
        key_selector=key_selector,
        topk_config=plan.kernel_config,
        query_spec=plan.query_spec,
        llm_config=llm_config,
        embedding_config=embedding_config,
        async_timeout_ms=async_timeout_ms,
        async_capacity=async_capacity,
    )


def apply_sem_groupby_from_request(
    input_ds: DataStream,
    *,
    request: SemGroupbyRequest,
    runtime_config: RuntimeConfig,
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply a stateful semantic groupby request to one input stream."""
    plan = lower_sem_groupby_request(request, runtime_config)
    if plan.context_kind == "window":
        return apply_sem_groupby_pushdown(
            input_ds,
            request=request,
            runtime_config=runtime_config,
            timeout_ms=timeout_ms,
            async_capacity=async_capacity,
        )
    op = build_sem_groupby_from_request(request, runtime_config)
    return input_ds.key_by(lambda value: value.get("key", "")).process(op)


def build_sem_groupby_from_request(
    request: SemGroupbyRequest,
    runtime_config: RuntimeConfig,
):
    """Build a stateful semantic groupby runtime from a public request."""
    from pyflink.semantic_runtime.operators.stateful.sem_groupby_pipeline import (
        build_sem_groupby_operator,
    )

    plan = lower_sem_groupby_request(request, runtime_config)
    return build_sem_groupby_operator(
        config=plan.kernel_config,
        query_spec=plan.query_spec,
        input_kind=plan.input_kind,
    )


def apply_sem_agg_from_request(
    input_ds: DataStream,
    *,
    request: SemAggRequest,
    runtime_config: RuntimeConfig,
) -> DataStream:
    """Apply a stateful semantic aggregation request to one input stream."""
    plan = lower_sem_agg_request(request, runtime_config)
    if plan.context_kind == "window" and plan.mode == "algebraic":
        return apply_sem_agg_pushdown(
            input_ds,
            request=request,
            runtime_config=runtime_config,
        )
    op = build_sem_agg_from_request(request, runtime_config)
    return input_ds.key_by(lambda value: value.get("key", "")).process(op)


def build_sem_agg_from_request(
    request: SemAggRequest,
    runtime_config: RuntimeConfig,
):
    """Build a stateful semantic aggregation runtime from a public request."""
    from pyflink.semantic_runtime.operators.stateful.sem_agg_pipeline import (
        build_sem_agg_operator,
    )

    plan = lower_sem_agg_request(request, runtime_config)
    return build_sem_agg_operator(
        config=plan.kernel_config,
        query_spec=plan.query_spec,
        input_kind=plan.input_kind,
    )
