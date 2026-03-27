"""Internal builders for public semantic facade requests.

This module lowers public request objects into internal operator plans and then
builds concrete runtime kernels. It is not part of the user-facing API.
"""

from __future__ import annotations

from typing import Callable

from pyflink.common import Types
from pyflink.datastream import DataStream

from pyflink.semantic_runtime.public_api import (
    SemAggRequest,
    SemFilterRequest,
    SemGroupbyRequest,
    SemJoinRequest,
    SemLookupJoinRequest,
    SemLocalTopKRequest,
    SemMapRequest,
    SemTopKRequest,
    SemWindowRequest,
)
from pyflink.semantic_runtime.runtime.event_model import window_snapshot_to_topk_pool
from pyflink.semantic_runtime.runtime.plans import (
    lower_sem_agg_request,
    lower_sem_groupby_request,
    lower_sem_join_request,
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
from pyflink.semantic_runtime.runtime.pushdown.common import parse_json_or_passthrough, parse_window_snapshot
from pyflink.semantic_runtime.runtime.window_materialization import materialize_window_stream
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
    items_field: str = "items",
) -> DataStream:
    """Apply a row-level local semantic top-k request through the pushdown path."""
    return apply_sem_local_topk_pushdown(
        input_ds,
        request=request,
        runtime_config=runtime_config,
        timeout_ms=timeout_ms,
        async_capacity=async_capacity,
        items_field=items_field,
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
    return SemWindowFunction(
        plan.kernel_config,
        llm_config=runtime_config.get_operator_llm_client_config(
            "sem_window",
            allow_query_spec=False,
        ),
        embedding_config=runtime_config.to_embedding_backend_config(),
    )


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
        build_external_window_persistent_topk_pipeline,
        build_internal_scope_persistent_contextual_topk_pipeline,
        build_sem_topk_pipeline,
        resolve_topk_execution_plan,
    )

    plan = lower_sem_topk_request(request, runtime_config)
    execution_plan = resolve_topk_execution_plan(
        plan.query_spec,
        config=plan.kernel_config,
        input_kind=plan.input_kind,
    )
    topk_input = input_ds
    if plan.context_kind == "window":
        snapshots = materialize_window_stream(
            input_ds,
            scope_policy=plan.query_spec.scope_policy,
            trigger_policy=plan.query_spec.trigger_policy,
            runtime_config=runtime_config,
            operator_name="sem_topk",
        )
        if execution_plan.persistence_policy == "reset_per_scope":
            topk_input = snapshots.map(
                lambda value: window_snapshot_to_topk_pool(
                    parse_window_snapshot(value, operator_name="sem_topk window materialization"),
                    ranking_text=plan.intent,
                ),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            if plan.query_spec.ranking_method == "pointwise":
                return apply_sem_topk_pushdown(
                    topk_input,
                    request=request,
                    runtime_config=runtime_config,
                    timeout_ms=async_timeout_ms,
                    async_capacity=async_capacity,
                )
        else:
            topk_input = snapshots.map(
                lambda value: window_snapshot_to_topk_pool(
                    parse_window_snapshot(value, operator_name="sem_topk window materialization"),
                    ranking_text=plan.intent,
                ),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
            llm_config = None
            if plan.kernel_config.scorer_backend == "llm":
                llm_config = runtime_config.get_operator_llm_client_config(
                    "sem_topk",
                    allow_query_spec=True,
                )
            return build_external_window_persistent_topk_pipeline(
                topk_input,
                key_selector=lambda value: str(
                    parse_json_or_passthrough(value, operator_name="sem_topk external window pool").get("key", "")
                ),
                topk_config=plan.kernel_config,
                query_spec=plan.query_spec,
                llm_config=llm_config,
                embedding_config=runtime_config.to_embedding_backend_config(),
                async_timeout_ms=async_timeout_ms,
                async_capacity=async_capacity,
            )

    llm_config = None
    if plan.kernel_config.scorer_backend == "llm":
        llm_config = runtime_config.get_operator_llm_client_config(
            "sem_topk",
            allow_query_spec=True,
        )

    embedding_config = runtime_config.to_embedding_backend_config()
    if (
        execution_plan.scope_source == "internal_scope"
        and execution_plan.persistence_policy == "persistent_across_scopes"
        and plan.query_spec.ranking_method in {"pairwise", "listwise"}
    ):
        return build_internal_scope_persistent_contextual_topk_pipeline(
            topk_input,
            key_selector=key_selector,
            topk_config=plan.kernel_config,
            query_spec=plan.query_spec,
            llm_config=llm_config,
            embedding_config=embedding_config,
            async_timeout_ms=async_timeout_ms,
            async_capacity=async_capacity,
        )
    effective_key_selector = key_selector
    if execution_plan.scope_source == "external_window":
        effective_key_selector = lambda value: str(
            parse_json_or_passthrough(value, operator_name="sem_topk window pool").get("key", "")
        )
    return build_sem_topk_pipeline(
        topk_input,
        key_selector=effective_key_selector,
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
    from pyflink.semantic_runtime.operators.stateful.sem_groupby_window import (
        WindowOwnedSemGroupbyFunction,
    )

    plan = lower_sem_groupby_request(request, runtime_config)
    groupby_input = input_ds
    if plan.context_kind == "window":
        groupby_input = materialize_window_stream(
            input_ds,
            scope_policy=plan.query_spec.scope_policy,
            trigger_policy=plan.query_spec.trigger_policy,
            runtime_config=runtime_config,
            operator_name="sem_groupby",
        )
    op = build_sem_groupby_from_request(request, runtime_config)
    if isinstance(op, WindowOwnedSemGroupbyFunction):
        return apply_sem_groupby_pushdown(
            groupby_input,
            request=request,
            runtime_config=runtime_config,
            timeout_ms=timeout_ms,
            async_capacity=async_capacity,
        )
    return groupby_input.key_by(lambda value: value.get("key", "")).process(op)


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
        llm_config=runtime_config.to_llm_client_config(),
    )


def apply_sem_agg_from_request(
    input_ds: DataStream,
    *,
    request: SemAggRequest,
    runtime_config: RuntimeConfig,
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply a stateful semantic aggregation request to one input stream."""
    from pyflink.semantic_runtime.operators.stateful.sem_agg_pipeline import (
        resolve_agg_execution_plan,
    )

    plan = lower_sem_agg_request(request, runtime_config)
    agg_input = input_ds
    execution_plan = resolve_agg_execution_plan(
        plan.query_spec,
        config=plan.kernel_config,
        input_kind=plan.input_kind,
    )
    if plan.context_kind == "window":
        agg_input = materialize_window_stream(
            input_ds,
            scope_policy=plan.query_spec.scope_policy,
            trigger_policy=plan.query_spec.trigger_policy,
            runtime_config=runtime_config,
            operator_name="sem_agg",
        )
    if (
        execution_plan.scope_source == "external_window"
        and execution_plan.persistence_policy == "reset_per_scope"
    ):
        return apply_sem_agg_pushdown(
            agg_input,
            request=request,
            runtime_config=runtime_config,
            timeout_ms=timeout_ms,
            async_capacity=async_capacity,
        )
    op = build_sem_agg_from_request(request, runtime_config)
    return agg_input.key_by(lambda value: value.get("key", "")).process(op)


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
        llm_config=runtime_config.to_llm_client_config(),
    )


def apply_sem_join_from_request(
    left_input_ds: DataStream,
    *,
    request: SemJoinRequest,
    runtime_config: RuntimeConfig,
    left_key_selector: Callable,
    right_key_selector: Callable,
) -> DataStream:
    """Apply a public semantic join request to two keyed streams."""
    from pyflink.semantic_runtime.operators.stateful.sem_join import (
        build_sem_join_operator,
        build_window_owned_sem_join_operator,
    )

    if request.context.kind not in {"stream", "window"}:
        raise ValueError(f"sem_join does not support context {request.context.kind!r}")
    if not hasattr(request.right_input, "key_by"):
        raise TypeError(
            "sem_join right_input must be a stream-like object with key_by(...) "
            "for true two-input runtime"
        )

    plan = lower_sem_join_request(request, runtime_config)
    left_stream = left_input_ds
    right_stream = request.right_input
    if request.context.kind == "window":
        if plan.query_spec.scope_policy.window_kind == "semantic":
            raise NotImplementedError(
                "window-owned sem_join does not support semantic-window pairing yet"
            )
        left_stream = materialize_window_stream(
            left_input_ds,
            scope_policy=plan.query_spec.scope_policy,
            trigger_policy=plan.query_spec.trigger_policy,
            runtime_config=runtime_config,
            operator_name="sem_join(left)",
        )
        right_stream = materialize_window_stream(
            request.right_input,
            scope_policy=plan.query_spec.scope_policy,
            trigger_policy=plan.query_spec.trigger_policy,
            runtime_config=runtime_config,
            operator_name="sem_join(right)",
        )
        left_key_selector = lambda value: str(
            parse_json_or_passthrough(value, operator_name="sem_join(left window)").get("key", "")
        )
        right_key_selector = lambda value: str(
            parse_json_or_passthrough(value, operator_name="sem_join(right window)").get("key", "")
        )
    llm_config = runtime_config.get_operator_llm_client_config(
        "sem_join",
        allow_query_spec=True,
    )
    if request.context.kind == "window":
        op = build_window_owned_sem_join_operator(
            query_spec=plan.query_spec,
            llm_config=llm_config,
            kernel_config=plan.kernel_config,
        )
    else:
        op = build_sem_join_operator(
            query_spec=plan.query_spec,
            llm_config=llm_config,
            kernel_config=plan.kernel_config,
        )
    return (
        left_stream.key_by(left_key_selector)
        .connect(right_stream.key_by(right_key_selector))
        .process(op)
    )
