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

"""Planner and builder helpers for ``sem_groupby``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.stateful.sem_groupby import (
    SemGroupbyConfig,
    SemGroupbyFunction,
    resolve_groupby_persistence_policy,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_window import (
    WindowOwnedSemGroupbyFunction,
)
from pyflink.semantic_runtime.runtime.plans import (
    SemLoweringPlan,
    resolve_groupby_lowering_plan,
)
from pyflink.semantic_runtime.sem_spec import GroupbyQuerySpec


@dataclass(frozen=True)
class GroupbyExecutionPlan:
    """Resolved internal execution plan for ``sem_groupby``."""

    scope_source: str = "internal_scope"
    persistence_policy: str = "persistent_across_scopes"
    input_kind: str = "event_stream"
    lowering_plan: Optional[SemLoweringPlan] = None


def resolve_groupby_execution_plan(
    query_spec: Optional[GroupbyQuerySpec] = None,
    *,
    config: Optional[SemGroupbyConfig] = None,
    input_kind: str = "event_stream",
) -> GroupbyExecutionPlan:
    """Resolve scope source and persistence policy for ``sem_groupby``."""
    spec = query_spec or GroupbyQuerySpec()
    kernel_config = config or SemGroupbyConfig()
    scope_source = "external_window" if input_kind == "window_snapshot" else "internal_scope"
    persistence_policy = resolve_groupby_persistence_policy(
        kernel_config,
        scope_source=scope_source,
    )
    return GroupbyExecutionPlan(
        scope_source=scope_source,
        persistence_policy=persistence_policy,
        input_kind=input_kind,
        lowering_plan=resolve_groupby_lowering_plan(
            spec,
            input_kind=input_kind,
            persistence_policy=persistence_policy,
        ),
    )


def build_sem_groupby_operator(
    config: Optional[SemGroupbyConfig] = None,
    query_spec: Optional[GroupbyQuerySpec] = None,
    *,
    input_kind: str = "event_stream",
    llm_config: Optional[LLMClientConfig] = None,
):
    """Return the concrete runtime for the resolved ``sem_groupby`` plan."""
    spec = query_spec or GroupbyQuerySpec()
    kernel_config = config or SemGroupbyConfig()
    plan = resolve_groupby_execution_plan(
        spec,
        config=kernel_config,
        input_kind=input_kind,
    )

    if plan.scope_source == "internal_scope":
        if spec.trigger_policy.mode != "on_event":
            raise NotImplementedError(
                "sem_groupby internal_scope path currently supports only "
                "trigger_policy.mode='on_event'."
            )
        if spec.maintenance_trigger_policy is not None:
            mode = spec.maintenance_trigger_policy.mode
            if mode == "periodic":
                pass
            elif mode == "on_scope_close":
                close_capable = {"session", "tumbling", "semantic"}
                if spec.scope_policy.window_kind not in close_capable:
                    raise NotImplementedError(
                        "sem_groupby internal_scope path supports "
                        "maintenance_trigger_policy.mode='on_scope_close' only "
                        "for close-capable scopes: session, tumbling, semantic."
                    )
            else:
                raise NotImplementedError(
                    "sem_groupby internal_scope path currently supports only "
                    "maintenance_trigger_policy.mode='periodic' or 'on_scope_close'."
                )
        return SemGroupbyFunction(
            kernel_config,
            query_spec=spec,
            llm_config=llm_config,
            scope_source=plan.scope_source,
        )

    if plan.scope_source == "external_window":
        if input_kind != "window_snapshot":
            raise NotImplementedError(
                "sem_groupby external_window path requires input_kind='window_snapshot'."
            )
        if (
            plan.persistence_policy == "reset_per_scope"
            and spec.maintenance_trigger_policy is not None
            and spec.maintenance_trigger_policy.mode != "on_scope_close"
        ):
            raise NotImplementedError(
                "sem_groupby reset_per_scope bounded specialization currently "
                "supports only maintenance_trigger_policy.mode='on_scope_close'."
            )
        if plan.persistence_policy == "reset_per_scope":
            return WindowOwnedSemGroupbyFunction(
                kernel_config,
                query_spec=spec,
                llm_config=llm_config,
            )
        return SemGroupbyFunction(
            kernel_config,
            query_spec=spec,
            llm_config=llm_config,
            scope_source=plan.scope_source,
        )

    raise ValueError(f"Unsupported sem_groupby scope_source={plan.scope_source!r}")
