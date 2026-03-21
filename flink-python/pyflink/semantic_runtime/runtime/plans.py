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

"""Internal operator planning.

This module is the single internal planning entrypoint for semantic runtime.
It merges two responsibilities that used to live in separate files:

1. lowering public requests into per-operator runtime plans,
2. deciding whether an operator path lowers to `sem_xxx + Flink operator`
   or remains native runtime.

This module is not public API.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from pyflink.semantic_runtime.llm_client import LLMClientConfig
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
from pyflink.semantic_runtime.sem_spec import SemSpec
from pyflink.semantic_runtime.sem_spec import (
    AggQuerySpec,
    AggScopePolicy,
    GroupbyQuerySpec,
    GroupbyScopePolicy,
    JoinQuerySpec,
    TopKQuerySpec,
    TopKScopePolicy,
    TriggerPolicy,
)

if TYPE_CHECKING:
    from pyflink.semantic_runtime.operators.row.sem_lookup_join import SemLookupJoinConfig
    from pyflink.semantic_runtime.operators.stateful.sem_agg import SemAggConfig
    from pyflink.semantic_runtime.operators.stateful.sem_groupby import SemGroupbyConfig
    from pyflink.semantic_runtime.operators.stateful.sem_topk import SemTopKConfig
    from pyflink.semantic_runtime.operators.stateful.sem_window import SemWindowConfig
    from pyflink.semantic_runtime.runtime_config import RuntimeConfig


VALID_LOWERING_KINDS = {
    "derived_attribute_then_classical",
    "native_runtime",
}


@dataclass(frozen=True)
class SemDerivedAttrPlan:
    """Internal plan for one semantic derived attribute."""

    attribute_kind: str
    output_field: str
    backend: str
    stable_per_record: bool = True
    bounded_context: bool = False


@dataclass(frozen=True)
class SemLoweringPlan:
    """Logical lowering result for one semantic operator."""

    operator_name: str
    lowering_kind: str
    classical_operator: Optional[str] = None
    derived_attribute: Optional[SemDerivedAttrPlan] = None
    notes: Tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.lowering_kind not in VALID_LOWERING_KINDS:
            raise ValueError(
                f"Invalid lowering_kind={self.lowering_kind!r}. "
                f"Must be one of {VALID_LOWERING_KINDS}."
            )


@dataclass(frozen=True)
class SemMapPlan:
    """Merged internal plan for row-level semantic map."""

    intent: str
    output_schema: Optional[Dict[str, Any]]
    output_mode: str
    semantic: SemSpec
    llm_config: LLMClientConfig


@dataclass(frozen=True)
class SemFilterPlan:
    """Merged internal plan for row-level semantic filter."""

    intent: str
    semantic: SemSpec
    llm_config: LLMClientConfig


@dataclass(frozen=True)
class SemLocalTopKPlan:
    """Merged internal plan for row-level local semantic top-k."""

    intent: str
    k: int
    semantic: SemSpec
    llm_config: LLMClientConfig
    candidates_field: str


@dataclass(frozen=True)
class SemLookupJoinPlan:
    """Merged internal plan for row-level semantic lookup join."""

    intent: str
    candidate_source: Any
    left_block_size: int
    right_block_size: Optional[int]
    llm_config: LLMClientConfig
    join_config: "SemLookupJoinConfig"


@dataclass(frozen=True)
class SemWindowPlan:
    """Merged internal plan for semantic window materialization."""

    context_kind: str
    kernel_config: "SemWindowConfig"


@dataclass(frozen=True)
class SemTopKPlan:
    """Merged internal plan for stateful semantic top-k."""

    intent: str
    k: int
    context_kind: str
    input_kind: str
    query_spec: TopKQuerySpec
    kernel_config: "SemTopKConfig"


@dataclass(frozen=True)
class SemGroupbyPlan:
    """Merged internal plan for stateful semantic groupby."""

    intent: str
    context_kind: str
    input_kind: str
    query_spec: GroupbyQuerySpec
    kernel_config: "SemGroupbyConfig"


@dataclass(frozen=True)
class SemAggPlan:
    """Merged internal plan for stateful semantic aggregation."""

    intent: str
    mode: str
    context_kind: str
    input_kind: str
    query_spec: AggQuerySpec
    kernel_config: "SemAggConfig"


def lower_sem_map_request(
    request: SemMapRequest,
    runtime_config: "RuntimeConfig",
) -> SemMapPlan:
    """Lower a public semantic map request into one internal plan."""
    semantic = SemSpec.for_sem_map(
        request.intent,
        output_schema=request.output_schema,
        return_mode=request.output_mode,
    )
    return SemMapPlan(
        intent=request.intent,
        output_schema=request.output_schema,
        output_mode=request.output_mode,
        semantic=semantic,
        llm_config=runtime_config.get_row_llm_client_config("sem_map"),
    )


def lower_sem_filter_request(
    request: SemFilterRequest,
    runtime_config: "RuntimeConfig",
) -> SemFilterPlan:
    """Lower a public semantic filter request into one internal plan."""
    semantic = SemSpec.for_sem_filter(request.intent)
    return SemFilterPlan(
        intent=request.intent,
        semantic=semantic,
        llm_config=runtime_config.get_row_llm_client_config("sem_filter"),
    )


def lower_sem_local_topk_request(
    request: SemLocalTopKRequest,
    runtime_config: "RuntimeConfig",
    *,
    candidates_field: str = "candidates",
) -> SemLocalTopKPlan:
    """Lower a public local top-k request into one internal plan."""
    semantic = SemSpec.for_sem_topk(request.intent)
    return SemLocalTopKPlan(
        intent=request.intent,
        k=request.k,
        semantic=semantic,
        llm_config=runtime_config.get_row_llm_client_config("sem_local_topk"),
        candidates_field=candidates_field,
    )


def lower_sem_lookup_join_request(
    request: SemLookupJoinRequest,
    runtime_config: "RuntimeConfig",
) -> SemLookupJoinPlan:
    """Lower a public lookup join request into one internal plan."""
    join_config = runtime_config.get_lookup_join_config(
        candidate_source=request.candidate_source,
    )
    return SemLookupJoinPlan(
        intent=request.intent,
        candidate_source=request.candidate_source,
        left_block_size=1,
        right_block_size=join_config.right_block_size,
        llm_config=runtime_config.get_row_llm_client_config("sem_lookup_join"),
        join_config=join_config,
    )


def _context_to_input_kind(kind: str) -> str:
    if kind == "window":
        return "window_snapshot"
    if kind in {"session", "semantic_segment"}:
        return "event_stream"
    raise ValueError(f"Stateful semantic operators do not support context {kind!r}.")


def _topk_scope_policy_from_context(kind: str) -> TopKScopePolicy:
    if kind == "window":
        return TopKScopePolicy()
    if kind == "session":
        return TopKScopePolicy(window_kind="session")
    if kind == "semantic_segment":
        return TopKScopePolicy(window_kind="semantic")
    raise ValueError(f"Unsupported sem_topk context {kind!r}.")


def _groupby_scope_policy_from_context(kind: str) -> GroupbyScopePolicy:
    if kind == "window":
        return GroupbyScopePolicy()
    if kind == "session":
        return GroupbyScopePolicy(window_kind="session")
    if kind == "semantic_segment":
        return GroupbyScopePolicy(window_kind="semantic")
    raise ValueError(f"Unsupported sem_groupby context {kind!r}.")


def _agg_scope_policy_from_context(kind: str) -> AggScopePolicy:
    if kind == "window":
        return AggScopePolicy()
    if kind == "session":
        return AggScopePolicy(window_kind="session")
    if kind == "semantic_segment":
        return AggScopePolicy(window_kind="semantic")
    raise ValueError(f"Unsupported sem_agg context {kind!r}.")


def lower_sem_window_request(
    request: SemWindowRequest,
    runtime_config: "RuntimeConfig",
) -> SemWindowPlan:
    """Lower a public semantic window request into one internal plan."""
    if request.context.kind == "record":
        raise ValueError("sem_window does not support record context.")
    return SemWindowPlan(
        context_kind=request.context.kind,
        kernel_config=runtime_config.get_window_config(),
    )


def lower_sem_topk_request(
    request: SemTopKRequest,
    runtime_config: "RuntimeConfig",
) -> SemTopKPlan:
    """Lower a public stateful semantic top-k request into one internal plan."""
    input_kind = _context_to_input_kind(request.context.kind)
    query_spec = TopKQuerySpec.simple(request.intent, k=request.k)
    query_spec.trigger_policy = TriggerPolicy(mode="on_scope_close")
    query_spec.scope_policy = _topk_scope_policy_from_context(request.context.kind)
    return SemTopKPlan(
        intent=request.intent,
        k=request.k,
        context_kind=request.context.kind,
        input_kind=input_kind,
        query_spec=query_spec,
        kernel_config=runtime_config.get_topk_kernel_config(),
    )


def lower_sem_groupby_request(
    request: SemGroupbyRequest,
    runtime_config: "RuntimeConfig",
) -> SemGroupbyPlan:
    """Lower a public stateful semantic groupby request into one internal plan."""
    input_kind = _context_to_input_kind(request.context.kind)
    query_spec = GroupbyQuerySpec.simple(request.intent)
    query_spec.scope_policy = _groupby_scope_policy_from_context(request.context.kind)
    if request.context.kind == "window":
        query_spec.maintenance_trigger_policy = TriggerPolicy(mode="on_scope_close")
    return SemGroupbyPlan(
        intent=request.intent,
        context_kind=request.context.kind,
        input_kind=input_kind,
        query_spec=query_spec,
        kernel_config=runtime_config.get_groupby_kernel_config(),
    )


def lower_sem_agg_request(
    request: SemAggRequest,
    runtime_config: "RuntimeConfig",
) -> SemAggPlan:
    """Lower a public stateful semantic aggregation request into one internal plan."""
    input_kind = _context_to_input_kind(request.context.kind)
    query_spec = AggQuerySpec.simple(request.intent, agg_method=request.mode)
    if request.context.kind != "window":
        query_spec.trigger_policy = TriggerPolicy(mode="on_scope_close")
    query_spec.scope_policy = _agg_scope_policy_from_context(request.context.kind)
    return SemAggPlan(
        intent=request.intent,
        mode=request.mode,
        context_kind=request.context.kind,
        input_kind=input_kind,
        query_spec=query_spec,
        kernel_config=runtime_config.get_agg_kernel_config(),
    )


def resolve_topk_lowering_plan(query_spec: TopKQuerySpec) -> SemLoweringPlan:
    """Resolve the logical form for ``sem_topk``."""
    if query_spec.ranking_method == "pointwise":
        return SemLoweringPlan(
            operator_name="sem_topk",
            lowering_kind="derived_attribute_then_classical",
            classical_operator="topn",
            derived_attribute=SemDerivedAttrPlan(
                attribute_kind="score",
                output_field="score",
                backend="planner_selected",
                stable_per_record=True,
                bounded_context=False,
            ),
            notes=(
                "Pointwise top-k can be modeled as semantic score generation "
                "followed by classical Top-N maintenance.",
            ),
        )
    return SemLoweringPlan(
        operator_name="sem_topk",
        lowering_kind="native_runtime",
        notes=(
            "Contextual rerank depends on pool-level context and does not lower "
            "to one stable per-record score attribute.",
        ),
    )


def resolve_groupby_lowering_plan(
    query_spec: GroupbyQuerySpec,
    *,
    input_kind: str = "event_stream",
) -> SemLoweringPlan:
    """Resolve the logical form for ``sem_groupby``."""
    if input_kind == "window_snapshot":
        return SemLoweringPlan(
            operator_name="sem_groupby",
            lowering_kind="derived_attribute_then_classical",
            classical_operator="groupby",
            derived_attribute=SemDerivedAttrPlan(
                attribute_kind="label",
                output_field="group_id",
                backend="planner_selected",
                stable_per_record=True,
                bounded_context=False,
            ),
            notes=(
                "Bounded/window-owned grouping is modeled as semantic label "
                "generation followed by classical group-by.",
            ),
        )

    return SemLoweringPlan(
        operator_name="sem_groupby",
        lowering_kind="native_runtime",
        notes=(
            "Operator-owned grouping keeps evolving group state; group identity "
            "is not a static per-record label.",
        ),
    )


def resolve_agg_lowering_plan(
    query_spec: AggQuerySpec,
    *,
    input_kind: str = "event_stream",
) -> SemLoweringPlan:
    """Resolve the logical form for ``sem_agg``."""
    if input_kind == "window_snapshot" and query_spec.agg_method == "algebraic":
        return SemLoweringPlan(
            operator_name="sem_agg",
            lowering_kind="derived_attribute_then_classical",
            classical_operator="aggregate",
            notes=(
                "Window-owned algebraic aggregation can run as bounded native "
                "aggregation without operator-owned semantic state.",
            ),
        )
    return SemLoweringPlan(
        operator_name="sem_agg",
        lowering_kind="native_runtime",
        notes=(
            "Semantic summarize/compressive reduction remains native runtime.",
        ),
    )


def resolve_join_lowering_plan(query_spec: JoinQuerySpec) -> SemLoweringPlan:
    """Resolve the logical form for future ``sem_join``."""
    return SemLoweringPlan(
        operator_name="sem_join",
        lowering_kind="derived_attribute_then_classical",
        classical_operator="join/filter",
        derived_attribute=SemDerivedAttrPlan(
            attribute_kind="match",
            output_field="match_score",
            backend=query_spec.semantic.backend,
            stable_per_record=False,
            bounded_context=False,
        ),
        notes=(
            "Logical sem_join can be modeled as semantic match score/predicate "
            "plus classical join/filter even if runtime still needs native "
            "candidate generation and pruning.",
        ),
    )
