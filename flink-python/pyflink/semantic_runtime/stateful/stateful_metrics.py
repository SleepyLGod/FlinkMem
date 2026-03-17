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

"""
Keyed/stateful metrics for V0.2 semantic operators.

Extends the V0.1 ``OperatorMetrics`` pattern with counters and gauges
specific to stateful operators:

- **state_size**: current number of entries in keyed state (gauge)
- **timer_fire_count**: total timer callbacks fired (counter)
- **eviction_count**: total entries evicted by overflow policy (counter)
- **overflow_count**: total overflow events (counter)
- **async_queue_depth**: pending async work items (gauge)
- **stale_window_count**: windows closed by timeout (counter)
- **boundary_trigger_count**: windows closed by semantic boundary (counter)
- **recompute_count**: top-k recomputation triggers (counter)

Each stateful operator creates a ``StatefulOperatorMetrics`` in ``open()``
via ``StatefulOperatorMetrics.from_runtime_context(ctx, operator_name)``.
Falls back to local in-memory accumulators when Flink MetricGroup is
unavailable (unit tests).

Usage::

    def open(self, runtime_context):
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_window"
        )

    def process_element(self, value, ctx):
        self._metrics.record_event_processed()
        ...
        self._metrics.record_eviction(count=2)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from pyflink.datastream.functions import RuntimeContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Operator version/tag metadata
# ---------------------------------------------------------------------------

@dataclass
class OperatorTag:
    """Immutable metadata attached to every operator for audit/replay.

    These tags are embedded in emitted records and exposed as metric labels.
    """
    operator_name: str = ""
    operator_version: str = "v0.2.0"
    workflow_version: str = "v0.2.0"
    config_hash: str = ""          # hash of operator config for drift detection

    def to_dict(self) -> Dict[str, str]:
        return {
            "operator_name": self.operator_name,
            "operator_version": self.operator_version,
            "workflow_version": self.workflow_version,
            "config_hash": self.config_hash,
        }


# ---------------------------------------------------------------------------
# StatefulOperatorMetrics
# ---------------------------------------------------------------------------

class StatefulOperatorMetrics:
    """Metrics collector for V0.2 stateful semantic operators.

    Mirrors the V0.1 ``OperatorMetrics`` API pattern but tracks
    state-machine-specific counters and gauges.
    """

    def __init__(
        self,
        operator_tag: Optional[OperatorTag] = None,
        # Flink metric handles (None = local-only mode)
        _event_processed_counter=None,
        _timer_fire_counter=None,
        _eviction_counter=None,
        _overflow_counter=None,
        _stale_window_counter=None,
        _boundary_trigger_counter=None,
        _recompute_counter=None,
        _async_emit_counter=None,
        _state_size_gauge_fn=None,
        _async_queue_gauge_fn=None,
        _local: bool = False,
    ) -> None:
        self.tag = operator_tag or OperatorTag()
        self._local = _local

        # Flink handles
        self._event_processed_counter = _event_processed_counter
        self._timer_fire_counter = _timer_fire_counter
        self._eviction_counter = _eviction_counter
        self._overflow_counter = _overflow_counter
        self._stale_window_counter = _stale_window_counter
        self._boundary_trigger_counter = _boundary_trigger_counter
        self._recompute_counter = _recompute_counter
        self._async_emit_counter = _async_emit_counter

        # Local accumulators (always maintained)
        self.local_events_processed: int = 0
        self.local_timer_fires: int = 0
        self.local_evictions: int = 0
        self.local_overflows: int = 0
        self.local_stale_windows: int = 0
        self.local_boundary_triggers: int = 0
        self.local_recomputes: int = 0
        self.local_async_emits: int = 0

        # Gauge-like values (updated by operator, read by gauge callback)
        self.current_state_size: int = 0
        self.current_async_queue_depth: int = 0

    # -- factory -------------------------------------------------------------

    @classmethod
    def from_runtime_context(
        cls,
        runtime_context: RuntimeContext,
        operator_name: str,
        operator_tag: Optional[OperatorTag] = None,
    ) -> "StatefulOperatorMetrics":
        """Register Flink metrics; fall back to local-only on failure."""
        tag = operator_tag or OperatorTag(operator_name=operator_name)
        try:
            if hasattr(runtime_context, "get_metrics_group"):
                root_mg = runtime_context.get_metrics_group()
            elif hasattr(runtime_context, "get_metric_group"):
                root_mg = runtime_context.get_metric_group()
            else:
                raise AttributeError("No metric-group accessor")

            mg = root_mg.add_group("cp_stateful", operator_name)

            instance = cls(
                operator_tag=tag,
                _event_processed_counter=mg.counter("events_processed"),
                _timer_fire_counter=mg.counter("timer_fires"),
                _eviction_counter=mg.counter("evictions"),
                _overflow_counter=mg.counter("overflows"),
                _stale_window_counter=mg.counter("stale_windows"),
                _boundary_trigger_counter=mg.counter("boundary_triggers"),
                _recompute_counter=mg.counter("recomputes"),
                _async_emit_counter=mg.counter("async_emits"),
            )

            # Register gauges for state size and async queue depth
            mg.gauge("state_size", lambda: instance.current_state_size)
            mg.gauge("async_queue_depth", lambda: instance.current_async_queue_depth)

            logger.info("StatefulOperatorMetrics registered for %s", operator_name)
            return instance

        except Exception as e:
            logger.warning(
                "Could not register Flink metrics for %s: %s. "
                "Falling back to local accumulators.", operator_name, e,
            )
            return cls(operator_tag=tag, _local=True)

    @classmethod
    def noop(cls, operator_name: str = "") -> "StatefulOperatorMetrics":
        """Create a local-only metrics instance (for tests)."""
        return cls(
            operator_tag=OperatorTag(operator_name=operator_name),
            _local=True,
        )

    # -- recording methods ---------------------------------------------------

    def record_event_processed(self, count: int = 1) -> None:
        """Record one or more events processed."""
        self.local_events_processed += count
        if self._event_processed_counter is not None:
            self._event_processed_counter.inc(count)

    def record_timer_fire(self) -> None:
        """Record a timer callback invocation."""
        self.local_timer_fires += 1
        if self._timer_fire_counter is not None:
            self._timer_fire_counter.inc()

    def record_eviction(self, count: int = 1) -> None:
        """Record entries evicted by overflow policy."""
        self.local_evictions += count
        if self._eviction_counter is not None:
            self._eviction_counter.inc(count)

    def record_overflow(self) -> None:
        """Record an overflow event (buffer/state limit hit)."""
        self.local_overflows += 1
        if self._overflow_counter is not None:
            self._overflow_counter.inc()

    def record_stale_window(self) -> None:
        """Record a window closed by timeout (stale)."""
        self.local_stale_windows += 1
        if self._stale_window_counter is not None:
            self._stale_window_counter.inc()

    def record_boundary_trigger(self) -> None:
        """Record a semantic boundary trigger."""
        self.local_boundary_triggers += 1
        if self._boundary_trigger_counter is not None:
            self._boundary_trigger_counter.inc()

    def record_recompute(self) -> None:
        """Record a top-k / aggregation recomputation."""
        self.local_recomputes += 1
        if self._recompute_counter is not None:
            self._recompute_counter.inc()

    def record_async_emit(self) -> None:
        """Record an async work item emitted via side output."""
        self.local_async_emits += 1
        if self._async_emit_counter is not None:
            self._async_emit_counter.inc()

    def update_state_size(self, size: int) -> None:
        """Update the current state size gauge."""
        self.current_state_size = size

    def update_async_queue_depth(self, depth: int) -> None:
        """Update the current async queue depth gauge."""
        self.current_async_queue_depth = depth

    # -- snapshot for audit records ------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """Return a dict of all local counters + tag info for audit embedding."""
        return {
            **self.tag.to_dict(),
            "events_processed": self.local_events_processed,
            "timer_fires": self.local_timer_fires,
            "evictions": self.local_evictions,
            "overflows": self.local_overflows,
            "stale_windows": self.local_stale_windows,
            "boundary_triggers": self.local_boundary_triggers,
            "recomputes": self.local_recomputes,
            "async_emits": self.local_async_emits,
            "state_size": self.current_state_size,
            "async_queue_depth": self.current_async_queue_depth,
        }

