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

"""Planner/builder helpers for ``sem_agg``.

The current lowering view for sem_agg remains conservative: the runtime is
still treated as a native semantic reduction operator. This planner records
that internal decision explicitly so future derived-aggregate lowering has a
stable insertion point.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyflink.semantic_runtime.semantic_spec import AggQuerySpec
from pyflink.semantic_runtime.runtime.semantic_lowering import (
    SemanticLoweringPlan,
    resolve_agg_lowering_plan,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg import SemAggConfig, SemAggFunction
from pyflink.semantic_runtime.operators.stateful.sem_agg_window import WindowOwnedSemAggFunction


@dataclass(frozen=True)
class AggExecutionPlan:
    execution_path: str = "operator_owned"
    input_kind: str = "event_stream"
    lowering_plan: Optional[SemanticLoweringPlan] = None


def resolve_agg_execution_plan(
    query_spec: Optional[AggQuerySpec] = None,
    *,
    input_kind: str = "event_stream",
) -> AggExecutionPlan:
    spec = query_spec or AggQuerySpec()
    path = "window_owned" if input_kind == "window_snapshot" else "operator_owned"
    return AggExecutionPlan(
        execution_path=path,
        input_kind=input_kind,
        lowering_plan=resolve_agg_lowering_plan(spec, input_kind=input_kind),
    )


def build_sem_agg_operator(
    config: Optional[SemAggConfig] = None,
    query_spec: Optional[AggQuerySpec] = None,
    *,
    input_kind: str = "event_stream",
):
    spec = query_spec or AggQuerySpec()
    plan = resolve_agg_execution_plan(spec, input_kind=input_kind)
    if plan.execution_path == "operator_owned":
        if (
            query_spec is not None
            and spec.trigger_policy.mode == "on_scope_close"
            and spec.scope_policy.window_kind not in {"session", "tumbling", "semantic"}
        ):
            raise NotImplementedError(
                "sem_agg operator_owned on_scope_close requires "
                "scope_policy.window_kind in {'session', 'tumbling', 'semantic'}"
            )
        return SemAggFunction(config, query_spec=query_spec)
    if plan.execution_path == "window_owned":
        if input_kind != "window_snapshot":
            raise NotImplementedError(
                "sem_agg window_owned path requires input_kind='window_snapshot'."
            )
        return WindowOwnedSemAggFunction(config, query_spec=query_spec)
    raise ValueError(f"Unsupported sem_agg execution_path={plan.execution_path!r}")
