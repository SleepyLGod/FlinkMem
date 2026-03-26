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

"""Planner and builder helpers for ``sem_agg``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.stateful.sem_agg import (
    SemAggConfig,
    SemAggFunction,
    ensure_agg_query_spec,
    resolve_agg_persistence_policy,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_window import (
    WindowOwnedSemAggFunction,
)
from pyflink.semantic_runtime.runtime.plans import (
    SemLoweringPlan,
    resolve_agg_lowering_plan,
)
from pyflink.semantic_runtime.sem_spec import AggQuerySpec


@dataclass(frozen=True)
class AggExecutionPlan:
    """Resolved internal execution plan for ``sem_agg``."""

    scope_source: str = "internal_scope"
    persistence_policy: str = "persistent_across_scopes"
    input_kind: str = "event_stream"
    lowering_plan: Optional[SemLoweringPlan] = None


def resolve_agg_execution_plan(
    query_spec: Optional[AggQuerySpec] = None,
    *,
    config: Optional[SemAggConfig] = None,
    input_kind: str = "event_stream",
) -> AggExecutionPlan:
    """Resolve scope source and persistence policy for ``sem_agg``."""
    kernel_config = config or SemAggConfig()
    spec = ensure_agg_query_spec(kernel_config, query_spec)
    scope_source = "external_window" if input_kind == "window_snapshot" else "internal_scope"
    persistence_policy = resolve_agg_persistence_policy(
        kernel_config,
        scope_source=scope_source,
    )
    return AggExecutionPlan(
        scope_source=scope_source,
        persistence_policy=persistence_policy,
        input_kind=input_kind,
        lowering_plan=resolve_agg_lowering_plan(
            spec,
            input_kind=input_kind,
            persistence_policy=persistence_policy,
        ),
    )


def build_sem_agg_operator(
    config: Optional[SemAggConfig] = None,
    query_spec: Optional[AggQuerySpec] = None,
    *,
    input_kind: str = "event_stream",
    llm_config: Optional[LLMClientConfig] = None,
):
    """Return the concrete runtime for the resolved ``sem_agg`` plan."""
    kernel_config = config or SemAggConfig()
    spec = ensure_agg_query_spec(kernel_config, query_spec)
    plan = resolve_agg_execution_plan(
        spec,
        config=kernel_config,
        input_kind=input_kind,
    )

    if plan.scope_source == "internal_scope":
        if (
            spec.trigger_policy.mode == "on_scope_close"
            and spec.scope_policy.window_kind not in {"session", "tumbling", "semantic"}
        ):
            raise NotImplementedError(
                "sem_agg internal_scope on_scope_close requires "
                "scope_policy.window_kind in {'session', 'tumbling', 'semantic'}"
            )
        return SemAggFunction(
            kernel_config,
            query_spec=spec,
            scope_source=plan.scope_source,
            llm_config=llm_config,
        )

    if plan.scope_source == "external_window":
        if input_kind != "window_snapshot":
            raise NotImplementedError(
                "sem_agg external_window path requires input_kind='window_snapshot'"
            )
        if plan.persistence_policy == "reset_per_scope":
            return WindowOwnedSemAggFunction(
                kernel_config,
                query_spec=spec,
            )
        return SemAggFunction(
            kernel_config,
            query_spec=spec,
            scope_source=plan.scope_source,
            llm_config=llm_config,
        )

    raise ValueError(f"Unsupported sem_agg scope_source={plan.scope_source!r}")
