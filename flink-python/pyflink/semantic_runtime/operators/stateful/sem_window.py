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
sem_window — semantic boundary detection and window materialization.

Option B (V0.2 default): ``KeyedProcessFunction`` state machine that
accumulates events in ``ListState``, evaluates boundary conditions
(count / time / semantic flag), and emits ``WindowSnapshot`` when
a boundary is reached.

Boundary triggers
-----------------
1. **Count trigger**: window emits after ``max_window_events`` events.
2. **Time trigger**: window emits after ``window_timeout_ms`` since open.
3. **Semantic boundary**: window emits when an event carries a boundary flag
   (e.g. ``topic_shift=True``), produced by an upstream async pre-classifier.

Timer pattern
-------------
- A processing-time timer is registered when the window opens.
- ``on_timer`` flushes the window if it has not already been closed by a
  count or semantic trigger.
- Timer callbacks perform only local state operations (no LLM calls).
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, ValueState

from pyflink.semantic_runtime.runtime.state_descriptors import (
    StateSafetyConfig,
    OverflowPolicy,
    sem_window_event_buffer_descriptor,
    sem_window_meta_descriptor,
)
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    WindowSnapshot,
)
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    TimerPolicy,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SemWindowConfig:
    """Configuration for the semantic window operator."""
    max_window_events: int = 50        # count trigger threshold
    window_timeout_ms: int = 30_000    # time trigger (ms since window open)
    boundary_flag: str = "topic_shift" # which flag to check for semantic boundary
    ttl_seconds: int = 3600
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST


# ---------------------------------------------------------------------------
# Window metadata (stored in ValueState)
# ---------------------------------------------------------------------------

def _new_window_meta(open_time_ms: int) -> Dict[str, Any]:
    return {
        "window_id": uuid.uuid4().hex[:12],
        "open_time_ms": open_time_ms,
        "event_count": 0,
    }


# ---------------------------------------------------------------------------
# SemWindowFunction
# ---------------------------------------------------------------------------

class SemWindowFunction(KeyedProcessFunction):
    """Keyed semantic window state machine.

    Usage::

        ds = env.from_collection(...)
        keyed = ds.key_by(simple_key_selector)
        windowed = keyed.process(SemWindowFunction(SemWindowConfig(...)))
    """

    def __init__(self, config: Optional[SemWindowConfig] = None) -> None:
        self._config = config or SemWindowConfig()
        # State handles — initialised in open()
        self._event_buffer: Optional[ListState] = None
        self._window_meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._config.ttl_seconds
        self._event_buffer = runtime_context.get_list_state(
            sem_window_event_buffer_descriptor(ttl)
        )
        self._window_meta = runtime_context.get_state(
            sem_window_meta_descriptor(ttl)
        )
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_window",
        )
        logger.info(
            "SemWindowFunction opened (max_events=%d, timeout_ms=%d, boundary=%s)",
            self._config.max_window_events,
            self._config.window_timeout_ms,
            self._config.boundary_flag,
        )

    # -- core ----------------------------------------------------------------

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """Process one incoming event.

        The *value* is expected to be a dict (``SemEvent.to_dict()``
        or compatible) serialised by Flink's ``Types.PICKLED_BYTE_ARRAY``.

        Yields ``WindowSnapshot.to_dict()`` dicts on the main output.
        """
        # Materialise as SemEvent for convenience
        if isinstance(value, dict):
            event = SemEvent.from_dict(value)
            event_dict = value
        else:
            # Fallback: treat as raw payload string
            event = SemEvent(
                key=str(ctx.get_current_key()),
                payload=str(value),
                seq_id=0,
            )
            event_dict = event.to_dict()

        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        # --- ensure window is open ---
        meta = self._window_meta.value()
        if meta is None:
            meta = _new_window_meta(now_ms)
            meta["key"] = event.key
            # Register a processing-time flush timer using timer_policy
            register_timer(
                ctx.timer_service(), meta, TimerCategory.FLUSH,
                now_ms + self._config.window_timeout_ms,
            )

        # --- append event to buffer ---
        meta["event_count"] += 1
        count = meta["event_count"]
        self._event_buffer.add(event_dict)

        # --- enforce hard size limit ---
        if count > self._config.max_window_events:
            if self._config.overflow_policy == OverflowPolicy.DROP_OLDEST:
                self._trim_buffer_oldest(meta)
            elif self._config.overflow_policy == OverflowPolicy.DROP_NEWEST:
                # Undo: don't actually store the latest event
                meta["event_count"] -= 1
                self._rebuild_buffer_without_last()
                self._window_meta.update(meta)
                return

        self._window_meta.update(meta)

        # --- evaluate boundary triggers ---
        trigger_reason = self._check_triggers(event, meta)
        if trigger_reason:
            if self._metrics:
                self._metrics.record_boundary_trigger()
            yield from self._emit_snapshot(meta, trigger_reason, now_ms)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        """Timer-triggered window flush.

        This fires when ``window_timeout_ms`` elapses since the window opened.
        Only performs local state operations — no LLM calls.
        """
        meta = self._window_meta.value()
        if meta is None:
            return  # window already closed/emitted

        if self._metrics:
            self._metrics.record_timer_fire()

        # Resolve which timer category fired
        category = resolve_timer_category(meta, timestamp)
        if category is None:
            return  # stale or unknown timer

        if category == TimerCategory.FLUSH:
            clear_timer_registration(meta, TimerCategory.FLUSH)
            now_ms = int(time.time() * 1000)
            yield from self._emit_snapshot(meta, "time", now_ms)
        elif category == TimerCategory.EVICT:
            # Eviction: clear stale window state without emitting
            clear_timer_registration(meta, TimerCategory.EVICT)
            self._event_buffer.clear()
            self._window_meta.clear()
            logger.info("Evicted stale window state for key=%s", meta.get("key", "?"))

    # -- internals -----------------------------------------------------------

    def _check_triggers(
        self, event: SemEvent, meta: Dict[str, Any]
    ) -> Optional[str]:
        """Return trigger reason string or None."""
        # 1. Semantic boundary flag
        if event.has_boundary(self._config.boundary_flag):
            return "semantic_boundary"
        # 2. Count trigger
        if meta["event_count"] >= self._config.max_window_events:
            return "count"
        return None

    def _emit_snapshot(
        self, meta: Dict[str, Any], trigger_reason: str, close_time_ms: int
    ):
        """Build and yield a WindowSnapshot, then reset state."""
        events = list(self._event_buffer.get())
        snapshot = WindowSnapshot(
            key=meta.get("key", ""),
            window_id=meta["window_id"],
            events=events,
            event_count=len(events),
            open_time_ms=meta["open_time_ms"],
            close_time_ms=close_time_ms,
            trigger_reason=trigger_reason,
        )
        # Reset state for next window
        self._event_buffer.clear()
        self._window_meta.clear()

        yield snapshot.to_dict()

    def _trim_buffer_oldest(self, meta: Dict[str, Any]) -> None:
        """Drop the oldest event from the buffer (DROP_OLDEST policy)."""
        events = list(self._event_buffer.get())
        if len(events) > 1:
            events = events[1:]  # drop oldest
        self._event_buffer.clear()
        for e in events:
            self._event_buffer.add(e)
        meta["event_count"] = len(events)

    def _rebuild_buffer_without_last(self) -> None:
        """Remove the last-added event (DROP_NEWEST policy)."""
        events = list(self._event_buffer.get())
        if events:
            events = events[:-1]
        self._event_buffer.clear()
        for e in events:
            self._event_buffer.add(e)
