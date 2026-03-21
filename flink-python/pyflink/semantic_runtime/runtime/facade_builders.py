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
from pyflink.semantic_runtime.runtime.operator_plans import (
    lower_sem_agg_request,
    lower_sem_filter_request,
    lower_sem_groupby_request,
    lower_sem_lookup_join_request,
    lower_sem_local_topk_request,
    lower_sem_map_request,
    lower_sem_topk_request,
    lower_sem_window_request,
)
from pyflink.semantic_runtime.runtime_config import RuntimeConfig


def build_sem_map_from_request(
    request: SemMapRequest,
    runtime_config: RuntimeConfig,
):
    """Build a row-level semantic map runtime from a public request."""
    from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction

    plan = lower_sem_map_request(request, runtime_config)
    return SemMapFunction(
        prompt_template=plan.intent,
        output_schema=plan.output_schema,
        llm_config=plan.llm_config,
        return_mode=plan.output_mode,
    )


def build_sem_filter_from_request(
    request: SemFilterRequest,
    runtime_config: RuntimeConfig,
):
    """Build a row-level semantic filter runtime from a public request."""
    from pyflink.semantic_runtime.operators.row.sem_filter import SemFilterFunction

    plan = lower_sem_filter_request(request, runtime_config)
    return SemFilterFunction(
        prompt_template=plan.intent,
        llm_config=plan.llm_config,
    )


def build_sem_local_topk_from_request(
    request: SemLocalTopKRequest,
    runtime_config: RuntimeConfig,
    *,
    candidates_field: str = "candidates",
):
    """Build a row-level local semantic top-k runtime from a public request."""
    from pyflink.semantic_runtime.operators.row.sem_local_topk import SemLocalTopKFunction

    plan = lower_sem_local_topk_request(
        request,
        runtime_config,
        candidates_field=candidates_field,
    )
    return SemLocalTopKFunction(
        prompt_template=plan.intent,
        k=plan.k,
        llm_config=plan.llm_config,
        candidates_field=plan.candidates_field,
    )


def build_sem_lookup_join_from_request(
    request: SemLookupJoinRequest,
    runtime_config: RuntimeConfig,
):
    """Build a row-level semantic lookup join runtime from a public request."""
    from pyflink.semantic_runtime.operators.row.sem_lookup_join import SemLookupJoinFunction

    plan = lower_sem_lookup_join_request(request, runtime_config)
    return SemLookupJoinFunction(
        prompt_template=plan.intent,
        llm_config=plan.llm_config,
        join_config=plan.join_config,
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
    llm_config = None
    if plan.kernel_config.scorer_backend == "llm":
        llm_config = runtime_config.to_llm_client_config()

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
