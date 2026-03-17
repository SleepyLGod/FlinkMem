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
sem_agg — stateful semantic aggregation with two modes.

Mode 1 — Algebraic aggregation:
  Incremental reduction over keyed events using a user-supplied ``reduce_fn``.
  State: ``ValueState[agg_value]``.  No async calls needed.

Mode 2 — Summarization aggregation:
  Accumulates events in bounded ``ListState``, then emits a side-output
  ``AsyncWorkItem(task_type="summarize")`` to an async LLM summariser.
  The summary result is merged back and stored in ``ValueState``.

Guardrails:
  - ``max_buffer_events``: hard cap on pending events before forced summarize.
  - TTL via ``StateTtlConfig`` on all state handles.
  - ``overflow_policy``: DROP_OLDEST / DROP_NEWEST / DEGRADE_TAG.

Reuses the async bridge from ``stateful/async_bridge.py``.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, ValueState

from pyflink.semantic_runtime.stateful.state_descriptors import (
    OverflowPolicy,
    sem_agg_buffer_descriptor,
    sem_agg_value_descriptor,
    sem_agg_meta_descriptor,
)
from pyflink.semantic_runtime.stateful.event_model import (
    SemanticEvent,
    is_window_snapshot,
    window_snapshot_to_semantic_events,
)
from pyflink.semantic_runtime.stateful.async_bridge import (
    ASYNC_WORK_TAG,
    AsyncWorkItem,
    AsyncResult,
)
from pyflink.semantic_runtime.stateful.timer_policy import (
    TimerCategory,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.stateful.stateful_metrics import StatefulOperatorMetrics

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SemAggConfig:
    """Configuration for the semantic aggregation operator."""
    mode: str = "algebraic"          # "algebraic" | "summarize"
    max_buffer_events: int = 100     # summarize path: max events before flush
    flush_interval_ms: int = 30_000  # timer-driven summarize flush
    ttl_seconds: int = 3600
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST
    # Algebraic mode: user provides a 2-arg reduce function
    reduce_fn: Optional[Callable[[Dict, Dict], Dict]] = None


# ---------------------------------------------------------------------------
# SemAggFunction
# ---------------------------------------------------------------------------

class SemAggFunction(KeyedProcessFunction):
    """Keyed semantic aggregation state machine.

    Usage (algebraic)::

        cfg = SemAggConfig(mode="algebraic", reduce_fn=my_reduce)
        keyed.process(SemAggFunction(cfg))

    Usage (summarize)::

        cfg = SemAggConfig(mode="summarize", max_buffer_events=50)
        main_ds = keyed.process(SemAggFunction(cfg))
        merged = build_async_bridge(main_ds, summarizer_fn, merge_fn, ...)
    """

    def __init__(self, config: Optional[SemAggConfig] = None) -> None:
        self._config = config or SemAggConfig()
        self._buffer: Optional[ListState] = None
        self._agg_value: Optional[ValueState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._config.ttl_seconds
        self._buffer = runtime_context.get_list_state(
            sem_agg_buffer_descriptor(ttl)
        )
        self._agg_value = runtime_context.get_state(
            sem_agg_value_descriptor(ttl)
        )
        self._meta = runtime_context.get_state(
            sem_agg_meta_descriptor(ttl)
        )
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_agg",
        )
        logger.info(
            "SemAggFunction opened (mode=%s, max_buffer=%d)",
            self._config.mode, self._config.max_buffer_events,
        )

    # -- core ----------------------------------------------------------------

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        # Detect async summarize result merge-back
        if isinstance(value, dict) and value.get("task_type") == "summarize":
            yield from self._handle_summarize_result(value, now_ms)
            return

        # Detect WindowSnapshot input → expand into individual events
        if isinstance(value, dict) and is_window_snapshot(value):
            for sub_event_dict in window_snapshot_to_semantic_events(value):
                yield from self._process_single_event(sub_event_dict, ctx, now_ms)
            return

        # Single event path
        yield from self._process_single_event(value, ctx, now_ms)

    def _process_single_event(self, value, ctx, now_ms: int):
        """Process a single SemanticEvent-shaped dict."""
        # Parse event
        if isinstance(value, dict):
            event = SemanticEvent.from_dict(value)
            event_dict = value
        else:
            event = SemanticEvent(
                key=str(ctx.get_current_key()), payload=str(value), seq_id=0,
            )
            event_dict = event.to_dict()

        meta = self._meta.value() or {
            "key": event.key, "event_count": 0,
            "version": 0, "pending_summarize": False,
        }

        # Register flush timer on first event (summarize mode)
        if (meta["event_count"] == 0
                and self._config.mode == "summarize"
                and self._config.flush_interval_ms > 0):
            register_timer(
                ctx.timer_service(), meta, TimerCategory.FLUSH,
                now_ms + self._config.flush_interval_ms,
            )

        meta["event_count"] += 1

        if self._config.mode == "algebraic":
            yield from self._algebraic_step(event_dict, meta, now_ms)
        else:
            yield from self._summarize_step(event_dict, meta, now_ms)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        """Timer-driven summarization flush."""
        meta = self._meta.value()
        if meta is None:
            return
        if self._metrics:
            self._metrics.record_timer_fire()

        category = resolve_timer_category(meta, timestamp)
        if category != TimerCategory.FLUSH:
            return

        clear_timer_registration(meta, TimerCategory.FLUSH)

        if self._config.mode == "summarize" and not meta.get("pending_summarize"):
            yield from self._emit_summarize_request(meta)

        # Re-register flush timer
        now_ms = int(time.time() * 1000)
        register_timer(
            ctx.timer_service(), meta, TimerCategory.FLUSH,
            now_ms + self._config.flush_interval_ms,
        )
        self._meta.update(meta)

    # -- algebraic path ------------------------------------------------------

    def _algebraic_step(
        self, event_dict: Dict[str, Any], meta: Dict[str, Any], now_ms: int
    ):
        """Incremental reduction using user-supplied reduce_fn."""
        reduce_fn = self._config.reduce_fn
        current = self._agg_value.value()

        if current is None or reduce_fn is None:
            # First event or no reduce_fn → store directly
            self._agg_value.update(event_dict)
        else:
            try:
                reduced = reduce_fn(current, event_dict)
                self._agg_value.update(reduced)
            except Exception as exc:
                logger.warning("Algebraic reduce failed: %s", exc)
                self._agg_value.update(event_dict)

        meta["version"] = meta.get("version", 0) + 1
        self._meta.update(meta)

        # Emit current aggregate snapshot
        agg = self._agg_value.value()
        yield {
            "key": meta.get("key", ""),
            "aggregate": agg,
            "version": meta["version"],
            "mode": "algebraic",
            "event_count": meta["event_count"],
            "timestamp_ms": now_ms,
        }

    # -- summarize path ------------------------------------------------------

    def _summarize_step(
        self, event_dict: Dict[str, Any], meta: Dict[str, Any], now_ms: int
    ):
        """Buffer events and trigger summarization when threshold reached."""
        self._buffer.add(event_dict)

        # Enforce buffer limit
        buf_count = meta["event_count"] - meta.get("_last_summarize_count", 0)
        if buf_count > self._config.max_buffer_events:
            overflow = self._config.overflow_policy
            if overflow == OverflowPolicy.DROP_OLDEST:
                self._trim_buffer_oldest(meta)
            elif overflow == OverflowPolicy.DROP_NEWEST:
                meta["event_count"] -= 1
                self._meta.update(meta)
                return
            # DEGRADE_TAG: accept, tagged at emit

        self._meta.update(meta)

        # Check if buffer is full → trigger summarize
        if buf_count >= self._config.max_buffer_events and not meta.get("pending_summarize"):
            yield from self._emit_summarize_request(meta)

    def _emit_summarize_request(self, meta: Dict[str, Any]):
        """Emit buffered events as an async summarize work item."""
        events = list(self._buffer.get())
        if not events:
            return

        meta["pending_summarize"] = True
        meta["_last_summarize_count"] = meta.get("event_count", 0)
        self._meta.update(meta)

        work = AsyncWorkItem(
            key=meta.get("key", ""),
            task_type="summarize",
            payload={
                "events": events,
                "event_count": len(events),
                "current_version": meta.get("version", 0),
            },
        )
        if self._metrics:
            self._metrics.record_async_emit()
        yield ASYNC_WORK_TAG, work.to_dict()

    def _handle_summarize_result(
        self, result_dict: Dict[str, Any], now_ms: int
    ):
        """Merge async summarize result into agg state."""
        meta = self._meta.value() or {}

        if not result_dict.get("success", False):
            meta["pending_summarize"] = False
            self._meta.update(meta)
            yield {
                "key": result_dict.get("key", ""),
                "aggregate": None,
                "version": meta.get("version", 0),
                "mode": "summarize_failed",
                "event_count": meta.get("event_count", 0),
                "timestamp_ms": now_ms,
            }
            return

        summary = result_dict.get("result", {}).get("summary", "")
        meta["version"] = meta.get("version", 0) + 1
        meta["pending_summarize"] = False

        agg = {
            "summary": summary,
            "version": meta["version"],
            "updated_ms": now_ms,
        }
        self._agg_value.update(agg)

        # Clear buffer after successful summarize
        self._buffer.clear()
        self._meta.update(meta)

        yield {
            "key": meta.get("key", ""),
            "aggregate": agg,
            "version": meta["version"],
            "mode": "summarize",
            "event_count": meta.get("event_count", 0),
            "timestamp_ms": now_ms,
        }

    def _trim_buffer_oldest(self, meta: Dict[str, Any]) -> None:
        """Drop the oldest event from the buffer."""
        events = list(self._buffer.get())
        if len(events) > 1:
            events = events[1:]
        self._buffer.clear()
        for e in events:
            self._buffer.add(e)

