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
  - ``overflow_policy``: DROP_OLDEST / DROP_NEWEST.

Reuses the async bridge from ``runtime/async_bridge.py``.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, ValueState

from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    sem_agg_buffer_descriptor,
    sem_agg_value_descriptor,
    sem_agg_meta_descriptor,
)
from pyflink.semantic_runtime.runtime.event_model import (
    SemanticEvent,
    is_window_snapshot,
    window_snapshot_to_semantic_events,
)
from pyflink.semantic_runtime.runtime.async_bridge import (
    ASYNC_WORK_TAG,
    AsyncWorkItem,
    AsyncResult,
)
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    encode_timer_key,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.semantic_spec import (
    AggQuerySpec,
    AggScopePolicy,
    SemanticSpec,
    TriggerPolicy,
)

logger = logging.getLogger(__name__)


@dataclass
class _AggScopeDecision:
    event_time_ms: int
    scope_bucket_id: Optional[int] = None
    pre_reset_reason: str = ""
    post_reset_reason: str = ""


class _AggScopeRuntime:
    """Pure scope-boundary logic for operator-owned agg kernels."""

    def __init__(self, query_spec: AggQuerySpec) -> None:
        self._query_spec = query_spec
        self._scope = query_spec.scope_policy

    def resolve_event_time_ms(self, value: Dict[str, Any], now_ms: int) -> int:
        for field in ("event_time_ms", "timestamp_ms", "proc_time_ms"):
            raw = value.get(field)
            if raw is not None:
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    continue
        return now_ms

    def _resolve_bucket_id(self, event_time_ms: int) -> Optional[int]:
        if self._scope.window_kind != "tumbling":
            return None
        if not self._scope.window_size_ms or self._scope.window_size_ms <= 0:
            return None
        return int(event_time_ms // self._scope.window_size_ms)

    def _has_semantic_boundary(self, value: Dict[str, Any]) -> bool:
        if self._scope.window_kind != "semantic":
            return False
        flags = value.get("boundary_flags")
        if not isinstance(flags, dict):
            return False
        return bool(flags.get(self._scope.boundary_flag, False))

    def plan(self, value: Dict[str, Any], meta: Dict[str, Any], now_ms: int) -> _AggScopeDecision:
        event_time_ms = self.resolve_event_time_ms(value, now_ms)
        decision = _AggScopeDecision(
            event_time_ms=event_time_ms,
            scope_bucket_id=self._resolve_bucket_id(event_time_ms),
        )
        kind = self._scope.window_kind
        prev_last_time_ms = int(meta.get("scope_last_time_ms", 0) or 0)
        prev_bucket_id = meta.get("scope_bucket_id")

        if kind == "session":
            gap_ms = self._scope.session_gap_ms
            if gap_ms and prev_last_time_ms > 0 and (event_time_ms - prev_last_time_ms) > gap_ms:
                decision.pre_reset_reason = "session_gap"
        elif kind == "tumbling":
            if (
                decision.scope_bucket_id is not None
                and prev_bucket_id is not None
                and decision.scope_bucket_id != prev_bucket_id
            ):
                decision.pre_reset_reason = "tumbling_rollover"
        elif kind == "semantic":
            if self._has_semantic_boundary(value):
                decision.post_reset_reason = "semantic_boundary"

        return decision


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


def resolve_agg_runtime_params(
    config: SemAggConfig,
    query_spec: AggQuerySpec,
):
    """Resolve runtime parameters from config + query spec."""
    resolved_mode = config.mode
    if query_spec.agg_method == "algebraic":
        resolved_mode = "algebraic"
    else:
        resolved_mode = query_spec.agg_method
    ttl_seconds = int(
        query_spec.scope_policy.ttl_seconds
        if query_spec.scope_policy.ttl_seconds is not None
        else config.ttl_seconds
    )
    max_buffer_events = int(
        query_spec.scope_policy.max_buffer_events
        if query_spec.scope_policy.max_buffer_events is not None
        else config.max_buffer_events
    )
    flush_interval_ms = int(
        query_spec.scope_policy.flush_interval_ms
        if query_spec.scope_policy.flush_interval_ms is not None
        else config.flush_interval_ms
    )
    return resolved_mode, ttl_seconds, max_buffer_events, flush_interval_ms


def _build_default_agg_query_spec(config: SemAggConfig) -> AggQuerySpec:
    """Translate kernel config into one canonical agg query spec.

    This keeps legacy constructor ergonomics without retaining a second trigger
    model inside the operator core.
    """
    if config.mode == "algebraic":
        trigger_policy = TriggerPolicy(mode="on_event")
    elif config.flush_interval_ms > 0:
        trigger_policy = TriggerPolicy(
            mode="periodic",
            interval_ms=int(config.flush_interval_ms),
        )
    else:
        trigger_policy = TriggerPolicy(
            mode="count_threshold",
            count_threshold=max(1, int(config.max_buffer_events)),
        )

    return AggQuerySpec(
        semantic=SemanticSpec(
            instruction="Aggregate semantic events into one result.",
            backend="rule",
            output_mode="summary" if config.mode != "algebraic" else "json",
        ),
        agg_method="algebraic" if config.mode == "algebraic" else config.mode,
        trigger_policy=trigger_policy,
        scope_policy=AggScopePolicy(
            ttl_seconds=int(config.ttl_seconds),
            max_buffer_events=int(config.max_buffer_events),
            flush_interval_ms=int(config.flush_interval_ms),
        ),
    )


def ensure_agg_query_spec(
    config: SemAggConfig,
    query_spec: Optional[AggQuerySpec],
) -> AggQuerySpec:
    """Return the canonical query spec used by the agg runtime."""
    return query_spec if query_spec is not None else _build_default_agg_query_spec(config)


def resolve_agg_trigger_runtime(query_spec: AggQuerySpec):
    """Resolve trigger runtime from the canonical agg query spec.

    Returns
    -------
    tuple
        ``(trigger_mode, periodic_ms, idle_ms, count_threshold)``
    """
    trigger = query_spec.trigger_policy
    periodic_ms = int(trigger.interval_ms or 0) if trigger.mode == "periodic" else 0
    idle_ms = int(trigger.idle_ms or 0) if trigger.mode == "idle_flush" else 0
    count_threshold = (
        int(trigger.count_threshold or 0)
        if trigger.mode == "count_threshold"
        else 0
    )
    return trigger.mode, periodic_ms, idle_ms, count_threshold


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

    def __init__(
        self,
        config: Optional[SemAggConfig] = None,
        query_spec: Optional[AggQuerySpec] = None,
    ) -> None:
        self._config = config or SemAggConfig()
        self._query_spec = ensure_agg_query_spec(self._config, query_spec)
        self._scope_runtime = _AggScopeRuntime(self._query_spec)
        self._buffer: Optional[ListState] = None
        self._agg_value: Optional[ValueState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None
        (
            self._resolved_mode,
            self._resolved_ttl_seconds,
            self._resolved_max_buffer_events,
            self._resolved_flush_interval_ms,
        ) = resolve_agg_runtime_params(self._config, self._query_spec)
        (
            self._trigger_mode,
            self._resolved_periodic_ms,
            self._resolved_idle_ms,
            self._resolved_count_threshold,
        ) = resolve_agg_trigger_runtime(self._query_spec)
        if self._trigger_mode == "on_scope_close":
            if not self._supports_operator_scope_close:
                raise NotImplementedError(
                    "sem_agg operator_owned on_scope_close requires "
                    "scope_policy.window_kind in {'session', 'tumbling', 'semantic'}"
                )

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._resolved_ttl_seconds
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
            self._resolved_mode, self._resolved_max_buffer_events,
        )

    @property
    def _supports_operator_scope_close(self) -> bool:
        return self._query_spec.scope_policy.window_kind in {"session", "tumbling", "semantic"}

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
            "_last_emit_count": 0,
            "_pending_buffer_count": 0,
            "scope_epoch": 0,
            "scope_last_time_ms": 0,
            "scope_bucket_id": None,
        }

        decision = self._scope_runtime.plan(event_dict, meta, now_ms)

        if decision.pre_reset_reason:
            yield from self._emit_scope_close_output(meta, now_ms, decision.pre_reset_reason)
            self._reset_scope_state(meta, reason=decision.pre_reset_reason)

        meta["event_count"] += 1
        meta["scope_last_time_ms"] = decision.event_time_ms
        if decision.scope_bucket_id is not None:
            meta["scope_bucket_id"] = decision.scope_bucket_id
        self._maybe_register_event_timers(ctx, meta, now_ms)

        if self._resolved_mode == "algebraic":
            yield from self._algebraic_step(event_dict, meta, now_ms)
        else:
            yield from self._summarize_step(event_dict, meta, now_ms)

        if self._trigger_mode == "on_scope_close":
            if decision.post_reset_reason:
                yield from self._emit_scope_close_output(meta, now_ms, decision.post_reset_reason)
                self._reset_scope_state(meta, reason=decision.post_reset_reason)
            else:
                self._register_scope_close_timer(ctx, meta, decision, now_ms)
            self._meta.update(meta)
            return

        if decision.post_reset_reason:
            yield from self._emit_scope_close_output(meta, now_ms, decision.post_reset_reason)
            self._reset_scope_state(meta, reason=decision.post_reset_reason)
            self._meta.update(meta)
            return

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

        now_ms = int(time.time() * 1000)
        if self._trigger_mode == "on_scope_close":
            reason = str(meta.pop("pending_scope_close_reason", "") or "scope_close")
            yield from self._emit_scope_close_output(meta, now_ms, reason)
            self._reset_scope_state(meta, reason=reason)
            self._meta.update(meta)
            return

        if self._resolved_mode == "algebraic":
            yield from self._emit_current_aggregate(meta, now_ms, reason=self._trigger_mode)
        elif not meta.get("pending_summarize"):
            yield from self._emit_summarize_request(meta)

        if self._trigger_mode == "periodic" and self._resolved_periodic_ms > 0:
            register_timer(
                ctx.timer_service(), meta, TimerCategory.FLUSH,
                now_ms + self._resolved_periodic_ms,
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

        if self._trigger_mode == "on_event":
            yield from self._emit_current_aggregate(meta, now_ms, reason="on_event")
            return

        if self._trigger_mode == "count_threshold":
            threshold = max(1, self._resolved_count_threshold)
            last_emit = int(meta.get("_last_emit_count", 0) or 0)
            if meta["event_count"] - last_emit >= threshold:
                yield from self._emit_current_aggregate(
                    meta, now_ms, reason="count_threshold"
                )

    # -- summarize path ------------------------------------------------------

    def _summarize_step(
        self, event_dict: Dict[str, Any], meta: Dict[str, Any], now_ms: int
    ):
        """Buffer events and trigger summarization when threshold reached."""
        self._buffer.add(event_dict)

        # Enforce buffer limit
        buf_count = len(list(self._buffer.get()))
        if buf_count > self._resolved_max_buffer_events:
            overflow = self._config.overflow_policy
            if overflow == OverflowPolicy.DROP_OLDEST:
                self._trim_buffer_oldest(meta)
            elif overflow == OverflowPolicy.DROP_NEWEST:
                self._trim_buffer_newest(meta)
                meta["event_count"] -= 1
                self._meta.update(meta)
                return

        self._meta.update(meta)

        if meta.get("pending_summarize"):
            return

        if len(list(self._buffer.get())) >= self._resolved_max_buffer_events:
            yield from self._emit_summarize_request(meta)
            return

        if self._trigger_mode == "on_event":
            yield from self._emit_summarize_request(meta)
            return

        if self._trigger_mode == "count_threshold":
            threshold = max(1, self._resolved_count_threshold)
            if len(list(self._buffer.get())) >= threshold:
                yield from self._emit_summarize_request(meta)

    def _emit_summarize_request(self, meta: Dict[str, Any]):
        """Emit buffered events as an async summarize work item."""
        events = list(self._buffer.get())
        if not events:
            return

        meta["pending_summarize"] = True
        meta["_last_summarize_count"] = meta.get("event_count", 0)
        meta["_pending_buffer_count"] = len(events)
        self._meta.update(meta)

        work = AsyncWorkItem(
            key=meta.get("key", ""),
            task_type="summarize",
            payload={
                "events": self._prepare_summary_events(events),
                "event_count": len(events),
                "current_version": meta.get("version", 0),
                "agg_method": self._resolved_mode,
                "scope_epoch": int(meta.get("scope_epoch", 0) or 0),
                "scope_close_pending": bool(meta.get("_scope_close_pending", False)),
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
            meta["_pending_buffer_count"] = 0
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
        result_epoch = int(
            result_dict.get("result", {}).get(
                "scope_epoch",
                result_dict.get("scope_epoch", meta.get("scope_epoch", 0)),
            ) or 0
        )
        result_scope_close = bool(
            result_dict.get("result", {}).get(
                "scope_close_pending",
                result_dict.get("scope_close_pending", False),
            )
        )
        meta["version"] = meta.get("version", 0) + 1
        meta["pending_summarize"] = False
        emitted_count = int(meta.get("_pending_buffer_count", 0) or 0)
        meta["_pending_buffer_count"] = 0

        agg = {
            "summary": summary,
            "version": meta["version"],
            "updated_ms": now_ms,
        }
        current_epoch = int(meta.get("scope_epoch", 0) or 0)
        should_update_current_state = not result_scope_close and result_epoch == current_epoch
        if should_update_current_state:
            self._agg_value.update(agg)

        # Preserve events that arrived while the summarize request was in flight.
        if should_update_current_state:
            self._drop_buffer_prefix(emitted_count)
        self._meta.update(meta)

        yield {
            "key": meta.get("key", ""),
            "aggregate": agg,
            "version": meta["version"],
            "mode": "summarize_scope_close" if result_scope_close else "summarize",
            "event_count": meta.get("event_count", 0),
            "timestamp_ms": now_ms,
            "scope_epoch": result_epoch,
        }

        if result_scope_close:
            return
        if meta.get("pending_summarize"):
            return
        if self._trigger_mode == "on_event" and list(self._buffer.get()):
            yield from self._emit_summarize_request(meta)
            return
        if self._trigger_mode == "count_threshold":
            threshold = max(1, self._resolved_count_threshold)
            if len(list(self._buffer.get())) >= threshold:
                yield from self._emit_summarize_request(meta)

    def _trim_buffer_oldest(self, meta: Dict[str, Any]) -> None:
        """Drop the oldest event from the buffer."""
        events = list(self._buffer.get())
        if len(events) > 1:
            events = events[1:]
        self._buffer.clear()
        for e in events:
            self._buffer.add(e)

    def _trim_buffer_newest(self, meta: Dict[str, Any]) -> None:
        """Drop the newest event from the buffer."""
        events = list(self._buffer.get())
        if len(events) > 1:
            events = events[:-1]
        else:
            events = []
        self._buffer.clear()
        for e in events:
            self._buffer.add(e)

    def _drop_buffer_prefix(self, count: int) -> None:
        """Drop the oldest ``count`` buffered events, preserving later arrivals."""
        if count <= 0:
            return
        events = list(self._buffer.get())
        events = events[count:]
        self._buffer.clear()
        for event in events:
            self._buffer.add(event)

    def _prepare_summary_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self._resolved_mode != "compressive":
            return events
        if len(events) <= 2:
            return events
        keep = max(1, min(len(events), self._resolved_max_buffer_events // 2))
        return events[-keep:]

    def _maybe_register_event_timers(self, ctx, meta: Dict[str, Any], now_ms: int) -> None:
        if self._trigger_mode == "periodic" and self._resolved_periodic_ms > 0:
            tkey = encode_timer_key(TimerCategory.FLUSH)
            if not meta.get(tkey):
                register_timer(
                    ctx.timer_service(), meta, TimerCategory.FLUSH,
                    now_ms + self._resolved_periodic_ms,
                )
            return

        if self._trigger_mode == "idle_flush" and self._resolved_idle_ms > 0:
            register_timer(
                ctx.timer_service(), meta, TimerCategory.FLUSH,
                now_ms + self._resolved_idle_ms,
            )
            return

        if self._trigger_mode == "on_scope_close":
            self._register_scope_close_timer(
                ctx,
                meta,
                _AggScopeDecision(
                    event_time_ms=int(meta.get("scope_last_time_ms", now_ms) or now_ms),
                    scope_bucket_id=meta.get("scope_bucket_id"),
                ),
                now_ms,
            )

    def _emit_current_aggregate(self, meta: Dict[str, Any], now_ms: int, *, reason: str):
        agg = self._agg_value.value()
        if agg is None:
            return
        meta["_last_emit_count"] = meta.get("event_count", 0)
        self._meta.update(meta)
        mode = "algebraic" if reason in {"algebraic", "on_event"} else f"algebraic_{reason}"
        yield {
            "key": meta.get("key", ""),
            "aggregate": agg,
            "version": meta.get("version", 0),
            "mode": mode,
            "event_count": meta.get("event_count", 0),
            "timestamp_ms": now_ms,
        }

    def _emit_scope_close_output(self, meta: Dict[str, Any], now_ms: int, reason: str):
        if self._resolved_mode == "algebraic":
            yield from self._emit_current_aggregate(meta, now_ms, reason=reason)
            return
        if meta.get("pending_summarize"):
            meta["_scope_close_pending"] = True
            self._meta.update(meta)
            return
        if not list(self._buffer.get()):
            return
        meta["_scope_close_pending"] = True
        yield from self._emit_summarize_request(meta)

    def _register_scope_close_timer(
        self,
        ctx,
        meta: Dict[str, Any],
        decision: _AggScopeDecision,
        now_ms: int,
    ) -> None:
        scope = self._query_spec.scope_policy
        kind = scope.window_kind
        fire_at_ms: Optional[int] = None
        use_event_time = False
        reason = ""

        if kind == "session":
            if scope.session_gap_ms and scope.session_gap_ms > 0:
                fire_at_ms = now_ms + scope.session_gap_ms
                reason = "session_gap"
        elif kind == "tumbling":
            if scope.window_size_ms and scope.window_size_ms > 0 and decision.scope_bucket_id is not None:
                fire_at_ms = int((decision.scope_bucket_id + 1) * scope.window_size_ms)
                use_event_time = True
                reason = "tumbling_rollover"
        elif kind == "semantic":
            return

        if fire_at_ms is None:
            return
        meta["pending_scope_close_reason"] = reason
        register_timer(
            ctx.timer_service(), meta, TimerCategory.FLUSH,
            fire_at_ms, use_event_time=use_event_time,
        )

    def _reset_scope_state(self, meta: Dict[str, Any], *, reason: str) -> None:
        self._buffer.clear()
        self._agg_value.clear()
        clear_timer_registration(meta, TimerCategory.FLUSH)
        meta.pop("pending_scope_close_reason", None)
        meta.pop("_scope_close_pending", None)
        meta["event_count"] = 0
        meta["_last_emit_count"] = 0
        meta["_last_summarize_count"] = 0
        meta["_pending_buffer_count"] = 0
        meta["pending_summarize"] = False
        meta["scope_epoch"] = int(meta.get("scope_epoch", 0) or 0) + 1
        meta["scope_last_time_ms"] = 0
        meta["scope_bucket_id"] = None
        meta["last_scope_reset_reason"] = reason
