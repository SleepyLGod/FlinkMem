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

"""Internal semantic lowering plans.

This module captures the logical/operator-algebra view of semantic operators.
It deliberately sits below the public API:

- user-facing specs still live in ``semantic_spec.py``
- runtime kernels still live in the operator-specific modules

The plans here only tell the system whether one semantic operator can be
understood as:

1. semantic attribute generation + classical operator
2. native continuous runtime
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple

from pyflink.semantic_runtime.semantic_spec import (
    AggQuerySpec,
    GroupbyQuerySpec,
    JoinQuerySpec,
    TopKQuerySpec,
)


VALID_LOWERING_KINDS = {
    "derived_attribute_then_classical",
    "native_runtime",
}


@dataclass(frozen=True)
class SemanticDerivedAttributePlan:
    """Internal plan for one semantic derived attribute."""

    attribute_kind: str
    output_field: str
    backend: str
    stable_per_record: bool = True
    bounded_context: bool = False


@dataclass(frozen=True)
class SemanticLoweringPlan:
    """Logical lowering result for one semantic operator."""

    operator_name: str
    lowering_kind: str
    classical_operator: Optional[str] = None
    derived_attribute: Optional[SemanticDerivedAttributePlan] = None
    notes: Tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.lowering_kind not in VALID_LOWERING_KINDS:
            raise ValueError(
                f"Invalid lowering_kind={self.lowering_kind!r}. "
                f"Must be one of {VALID_LOWERING_KINDS}."
            )


def resolve_topk_lowering_plan(query_spec: TopKQuerySpec) -> SemanticLoweringPlan:
    """Resolve the logical form for ``sem_topk``."""
    if query_spec.ranking_method == "pointwise":
        return SemanticLoweringPlan(
            operator_name="sem_topk",
            lowering_kind="derived_attribute_then_classical",
            classical_operator="topn",
            derived_attribute=SemanticDerivedAttributePlan(
                attribute_kind="score",
                output_field="score",
                backend=query_spec.semantic.backend,
                stable_per_record=True,
                bounded_context=False,
            ),
            notes=(
                "Pointwise top-k can be modeled as semantic score generation "
                "followed by classical Top-N maintenance.",
            ),
        )
    return SemanticLoweringPlan(
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
) -> SemanticLoweringPlan:
    """Resolve the logical form for ``sem_groupby``."""
    execution_path = query_spec.execution_path
    if execution_path == "auto":
        execution_path = "window_owned" if input_kind == "window_snapshot" else "operator_owned"

    if execution_path == "window_owned":
        return SemanticLoweringPlan(
            operator_name="sem_groupby",
            lowering_kind="derived_attribute_then_classical",
            classical_operator="groupby",
            derived_attribute=SemanticDerivedAttributePlan(
                attribute_kind="label",
                output_field="group_id",
                backend=query_spec.semantic.backend,
                stable_per_record=True,
                bounded_context=False,
            ),
            notes=(
                "Bounded/window-owned grouping is modeled as semantic label "
                "generation followed by classical group-by.",
            ),
        )

    return SemanticLoweringPlan(
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
) -> SemanticLoweringPlan:
    """Resolve the logical form for ``sem_agg``.

    Current V0.2++ treatment stays conservative: sem_agg is still modeled as a
    native semantic reduction operator. Explicit semantic-attribute lowering
    can be added later for derived-aggregate cases.
    """
    _ = (query_spec, input_kind)
    return SemanticLoweringPlan(
        operator_name="sem_agg",
        lowering_kind="native_runtime",
        notes=(
            "Current sem_agg is treated as native semantic reduction. "
            "Derived-aggregate lowering remains future work.",
        ),
    )


def resolve_join_lowering_plan(query_spec: JoinQuerySpec) -> SemanticLoweringPlan:
    """Resolve the logical form for future ``sem_join``."""
    return SemanticLoweringPlan(
        operator_name="sem_join",
        lowering_kind="derived_attribute_then_classical",
        classical_operator="join/filter",
        derived_attribute=SemanticDerivedAttributePlan(
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
