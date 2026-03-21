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

"""Planner/builder helpers for ``sem_groupby``.

This planner resolves both:

- the internal physical path: window-owned vs operator-owned
- the logical lowering view: ``semantic label + classical group-by`` vs
  native continuous grouping
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyflink.semantic_runtime.sem_spec import GroupbyQuerySpec
from pyflink.semantic_runtime.runtime.plans import (
    SemLoweringPlan,
    resolve_groupby_lowering_plan,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby import (
    SemGroupbyConfig,
    SemGroupbyFunction,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_window import (
    WindowOwnedSemGroupbyFunction,
)


@dataclass(frozen=True)
class GroupbyExecutionPlan:
    """Internal execution plan for ``sem_groupby``."""

    execution_path: str = "operator_owned"
    input_kind: str = "event_stream"
    lowering_plan: Optional[SemLoweringPlan] = None


def resolve_groupby_execution_plan(
    query_spec: Optional[GroupbyQuerySpec] = None,
    *,
    input_kind: str = "event_stream",
) -> GroupbyExecutionPlan:
    """Resolve the internal physical path for ``sem_groupby``."""

    spec = query_spec or GroupbyQuerySpec()
    path = "window_owned" if input_kind == "window_snapshot" else "operator_owned"
    return GroupbyExecutionPlan(
        execution_path=path,
        input_kind=input_kind,
        lowering_plan=resolve_groupby_lowering_plan(spec, input_kind=input_kind),
    )


def build_sem_groupby_operator(
    config: Optional[SemGroupbyConfig] = None,
    query_spec: Optional[GroupbyQuerySpec] = None,
    *,
    input_kind: str = "event_stream",
):
    """Return the concrete groupby operator for the resolved path."""

    spec = query_spec or GroupbyQuerySpec()
    plan = resolve_groupby_execution_plan(spec, input_kind=input_kind)
    if plan.execution_path == "operator_owned":
        if spec.trigger_policy.mode != "on_event":
            raise NotImplementedError(
                "sem_groupby operator_owned path currently supports only "
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
                        "sem_groupby operator_owned path supports "
                        "maintenance_trigger_policy.mode='on_scope_close' only "
                        "for close-capable scopes: session, tumbling, semantic."
                    )
            else:
                raise NotImplementedError(
                    "sem_groupby operator_owned path currently supports only "
                    "maintenance_trigger_policy.mode='periodic' or "
                    "'on_scope_close'."
                )
        return SemGroupbyFunction(config, query_spec=spec)
    if plan.execution_path == "window_owned":
        if input_kind != "window_snapshot":
            raise NotImplementedError(
                "sem_groupby window_owned path requires input_kind='window_snapshot'."
            )
        if (
            spec.maintenance_trigger_policy is not None
            and spec.maintenance_trigger_policy.mode != "on_scope_close"
        ):
            raise NotImplementedError(
                "sem_groupby window_owned path currently supports only "
                "maintenance_trigger_policy.mode='on_scope_close'."
            )
        return WindowOwnedSemGroupbyFunction(config, query_spec=spec)
    raise ValueError(f"Unsupported sem_groupby execution_path={plan.execution_path!r}")
