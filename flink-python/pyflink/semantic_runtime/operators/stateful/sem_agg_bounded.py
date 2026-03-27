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

"""Window-owned bounded aggregation for ``sem_agg``."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction

from pyflink.semantic_runtime.sem_spec import AggQuerySpec
from pyflink.semantic_runtime.runtime.async_bridge import ASYNC_WORK_TAG, AsyncWorkItem
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    is_window_snapshot,
    window_snapshot_to_sem_events,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_kernel import (
    SemAggConfig,
    _aggregate_event_records,
    resolve_agg_runtime_params,
)

COMPRESSIVE_MIN_EVENTS_FOR_TRUNCATION = 2
COMPRESSIVE_KEEP_DIVISOR = 2


class WindowOwnedSemAggFunction(KeyedProcessFunction):
    """Bounded/window-owned runtime for ``sem_agg``.

    The input must already be one closed or early-fired ``WindowSnapshot``.
    No cross-scope aggregate state is preserved.
    """

    def __init__(
        self,
        config: Optional[SemAggConfig] = None,
        query_spec: Optional[AggQuerySpec] = None,
    ) -> None:
        self._config = config or SemAggConfig()
        self._query_spec = query_spec
        (
            self._resolved_mode,
            self._resolved_ttl_seconds,
            self._resolved_max_buffer_events,
            self._resolved_flush_interval_ms,
        ) = resolve_agg_runtime_params(self._config, self._query_spec)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return
        if not is_window_snapshot(value):
            return

        raw_events = list(window_snapshot_to_sem_events(value))
        if not raw_events:
            return
        events = [SemEvent.from_dict(e) for e in raw_events]
        now_ms = int(time.time() * 1000)

        if self._resolved_mode == "algebraic":
            aggregate = self._aggregate_algebraic(raw_events)
            yield {
                "key": str(value.get("key", str(ctx.get_current_key()))),
                "aggregate": aggregate,
                "version": 1,
                "mode": "algebraic_window",
                "event_count": len(events),
                "timestamp_ms": now_ms,
            }
            return

        bounded_events = raw_events
        if self._resolved_mode == "compressive":
            bounded_events = self._compress_events(bounded_events)
        yield ASYNC_WORK_TAG, AsyncWorkItem(
            key=str(value.get("key", str(ctx.get_current_key()))),
            task_type="summarize",
            payload={
                "events": bounded_events,
                "event_count": len(bounded_events),
                "current_version": 0,
                "agg_method": self._resolved_mode,
                "scope_kind": "window_snapshot",
            },
        ).to_dict()

    def _aggregate_algebraic(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        return _aggregate_event_records(events, reduce_fn=self._config.reduce_fn)

    def _compress_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if len(events) <= COMPRESSIVE_MIN_EVENTS_FOR_TRUNCATION:
            return events
        keep = max(
            1,
            min(len(events), self._resolved_max_buffer_events // COMPRESSIVE_KEEP_DIVISOR),
        )
        return events[-keep:]
