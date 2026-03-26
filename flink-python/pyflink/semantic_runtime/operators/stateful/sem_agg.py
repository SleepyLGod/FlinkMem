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

"""Continuous keyed semantic aggregation.

Algebraic mode behaves like a keyed accumulator.

Summarize/compressive modes behave like append-only semantic folds:
``current_summary + added_events -> updated_summary``.
The canonical state owner stays inside ``SemAggFunction`` and LLM updates run
through an internal async executor plus timer-driven polling.
"""

from __future__ import annotations

import concurrent.futures
import time
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, MapState, ValueState

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    sem_agg_buffer_descriptor,
    sem_agg_scope_contributions_descriptor,
    sem_agg_scope_progress_descriptor,
    sem_agg_value_descriptor,
    sem_agg_meta_descriptor,
)
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    is_window_snapshot,
    window_snapshot_to_sem_events,
)
from pyflink.semantic_runtime.runtime.steps import (
    evaluate_sem_agg_summary_update_from_config_sync,
)
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    encode_timer_key,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.sem_spec import (
    AggQuerySpec,
    AggScopePolicy,
    SemSpec,
    TriggerPolicy,
)

logger = logging.getLogger(__name__)

_VALID_AGG_PERSISTENCE_POLICIES = {
    "persistent_across_scopes",
    "reset_per_scope",
    "hybrid",
}


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
    mode: str = "algebraic"
    max_buffer_events: int = 100
    flush_interval_ms: int = 30_000
    ttl_seconds: int = 3600
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST
    persistence_policy: Optional[str] = None
    reduce_fn: Optional[Callable[[Dict, Dict], Dict]] = None
    async_max_workers: int = 20
    async_poll_interval_ms: int = 200

    def __post_init__(self) -> None:
        if (
            self.persistence_policy is not None
            and self.persistence_policy not in _VALID_AGG_PERSISTENCE_POLICIES
        ):
            raise ValueError(
                f"Invalid sem_agg persistence_policy={self.persistence_policy!r}. "
                f"Must be one of {_VALID_AGG_PERSISTENCE_POLICIES}."
            )
        if self.async_max_workers <= 0:
            raise ValueError("sem_agg async_max_workers must be a positive integer.")
        if self.async_poll_interval_ms <= 0:
            raise ValueError("sem_agg async_poll_interval_ms must be a positive integer.")


def resolve_agg_persistence_policy(
    config: SemAggConfig,
    *,
    scope_source: str,
) -> str:
    """Resolve aggregate-state persistence independently from scope source."""
    _ = scope_source
    if config.persistence_policy is not None:
        return str(config.persistence_policy)
    return "persistent_across_scopes"


def _aggregate_event_records(
    events: List[Dict[str, Any]],
    *,
    reduce_fn: Optional[Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]],
) -> Dict[str, Any]:
    """Reduce one bounded event list into one algebraic aggregate."""
    if not events:
        raise ValueError("sem_agg algebraic aggregation requires non-empty events")
    current = events[0]
    if len(events) == 1:
        return current
    if reduce_fn is None:
        raise ValueError(
            "sem_agg algebraic aggregation over multiple events requires reduce_fn"
        )
    for event in events[1:]:
        current = reduce_fn(current, event)
    return current


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
        semantic=SemSpec(
            instruction="Aggregate semantic events into one result.",
            backend="hybrid",
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
        *,
        scope_source: str = "internal_scope",
        llm_config: Optional[LLMClientConfig] = None,
    ) -> None:
        self._config = config or SemAggConfig()
        self._query_spec = ensure_agg_query_spec(self._config, query_spec)
        self._scope_source = scope_source
        self._llm_config = llm_config or LLMClientConfig()
        self._scope_runtime = _AggScopeRuntime(self._query_spec)
        self._buffer: Optional[ListState] = None
        self._agg_value: Optional[ValueState] = None
        self._meta: Optional[ValueState] = None
        self._scope_contributions: Optional[MapState] = None
        self._scope_progress: Optional[MapState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._pending_futures: Dict[str, concurrent.futures.Future] = {}
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
        self._resolved_persistence_policy = resolve_agg_persistence_policy(
            self._config,
            scope_source=self._scope_source,
        )
        self._resolved_async_poll_interval_ms = int(self._config.async_poll_interval_ms)
        if (
            self._scope_source == "external_window"
            and self._resolved_persistence_policy == "persistent_across_scopes"
            and self._resolved_mode != "algebraic"
            and self._resolved_mode not in {"summarize", "compressive"}
        ):
            raise NotImplementedError("Unsupported sem_agg mode for external_window persistent path")
        if (
            self._scope_source == "external_window"
            and self._resolved_mode in {"summarize", "compressive"}
            and self._trigger_mode in {"periodic", "idle_flush"}
        ):
            raise NotImplementedError(
                "sem_agg external_window summarize/compressive path does not support "
                "trigger_policy.mode in {'periodic', 'idle_flush'}; use "
                "'on_event', 'count_threshold', or 'on_scope_close'."
            )
        if self._scope_source == "internal_scope" and self._trigger_mode == "on_scope_close":
            if not self._supports_operator_scope_close:
                raise NotImplementedError(
                    "sem_agg internal_scope on_scope_close requires "
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
        self._scope_contributions = runtime_context.get_map_state(
            sem_agg_scope_contributions_descriptor(ttl)
        )
        self._scope_progress = runtime_context.get_map_state(
            sem_agg_scope_progress_descriptor(ttl)
        )
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_agg",
        )
        if self._resolved_mode in {"summarize", "compressive"}:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._config.async_max_workers,
                thread_name_prefix="sem-agg",
            )
        logger.info(
            "SemAggFunction opened (mode=%s, max_buffer=%d)",
            self._resolved_mode, self._resolved_max_buffer_events,
        )

    @property
    def _supports_operator_scope_close(self) -> bool:
        return self._query_spec.scope_policy.window_kind in {"session", "tumbling", "semantic"}

    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None
        self._pending_futures.clear()

    # -- core ----------------------------------------------------------------

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        meta = self._meta.value()
        if meta is not None and self._resolved_mode in {"summarize", "compressive"}:
            yield from self._poll_pending_summary(meta, ctx, now_ms)

        if self._scope_source == "external_window":
            if not isinstance(value, dict) or not is_window_snapshot(value):
                raise ValueError(
                    "sem_agg external_window path requires WindowSnapshot input"
                )
            yield from self._process_external_scope_snapshot(value, ctx, now_ms)
            return

        # Single event path
        yield from self._process_single_event(value, ctx, now_ms)

    def _process_single_event(self, value, ctx, now_ms: int):
        """Process a single SemEvent-shaped dict."""
        # Parse event
        if isinstance(value, dict):
            event = SemEvent.from_dict(value)
            event_dict = value
        else:
            event = SemEvent(
                key=str(ctx.get_current_key()), payload=str(value), seq_id=0,
            )
            event_dict = event.to_dict()

        meta = self._meta.value() or {
            "key": event.key, "event_count": 0,
            "version": 0, "pending_summarize": False,
            "_last_emit_count": 0,
            "scope_epoch": 0,
            "scope_last_time_ms": 0,
            "scope_bucket_id": None,
        }

        decision = self._scope_runtime.plan(event_dict, meta, now_ms)

        if decision.pre_reset_reason:
            yield from self._emit_scope_close_output(
                meta,
                ctx,
                now_ms,
                decision.pre_reset_reason,
            )
            self._reset_scope_state(meta, reason=decision.pre_reset_reason)

        meta["event_count"] += 1
        meta["scope_last_time_ms"] = decision.event_time_ms
        if decision.scope_bucket_id is not None:
            meta["scope_bucket_id"] = decision.scope_bucket_id
        self._maybe_register_event_timers(ctx, meta, now_ms)

        if self._resolved_mode == "algebraic":
            yield from self._algebraic_step(event_dict, meta, now_ms)
        else:
            self._summarize_step(event_dict, meta, ctx, now_ms)

        if self._trigger_mode == "on_scope_close":
            if decision.post_reset_reason:
                yield from self._emit_scope_close_output(
                    meta,
                    ctx,
                    now_ms,
                    decision.post_reset_reason,
                )
                self._reset_scope_state(meta, reason=decision.post_reset_reason)
            else:
                self._register_scope_close_timer(ctx, meta, decision, now_ms)
            self._meta.update(meta)
            return

        if decision.post_reset_reason:
            yield from self._emit_scope_close_output(
                meta,
                ctx,
                now_ms,
                decision.post_reset_reason,
            )
            self._reset_scope_state(meta, reason=decision.post_reset_reason)
            self._meta.update(meta)
            return

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        """Timer-driven summarization flush."""
        if self._scope_source != "internal_scope":
            raise RuntimeError("sem_agg external_window path does not own internal timers")
        meta = self._meta.value()
        if meta is None:
            return
        if self._metrics:
            self._metrics.record_timer_fire()

        category = resolve_timer_category(meta, timestamp)
        if category == TimerCategory.RECOMPUTE:
            clear_timer_registration(meta, TimerCategory.RECOMPUTE)
            if self._resolved_mode not in {"summarize", "compressive"}:
                return
            yield from self._poll_pending_summary(meta, ctx, timestamp)
            self._meta.update(meta)
            return
        if category != TimerCategory.FLUSH:
            return

        clear_timer_registration(meta, TimerCategory.FLUSH)

        now_ms = int(time.time() * 1000)
        if self._trigger_mode == "on_scope_close":
            reason = str(meta.pop("pending_scope_close_reason", "") or "scope_close")
            yield from self._emit_scope_close_output(meta, ctx, now_ms, reason)
            self._reset_scope_state(meta, reason=reason)
            self._meta.update(meta)
            return

        if self._resolved_mode == "algebraic":
            yield from self._emit_current_aggregate(meta, now_ms, reason=self._trigger_mode)
        elif not meta.get("pending_summarize"):
            self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)

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
            reduced = reduce_fn(current, event_dict)
            self._agg_value.update(reduced)

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
        self,
        event_dict: Dict[str, Any],
        meta: Dict[str, Any],
        ctx: Any,
        now_ms: int,
    ):
        """Buffer events and dispatch one async summary update when triggered."""
        self._buffer.add(event_dict)
        buf_count = len(list(self._buffer.get()))
        if buf_count > self._resolved_max_buffer_events:
            overflow = self._config.overflow_policy
            if overflow == OverflowPolicy.DROP_OLDEST:
                self._trim_buffer_oldest()
            elif overflow == OverflowPolicy.DROP_NEWEST:
                self._trim_buffer_newest()
                meta["event_count"] -= 1
                self._meta.update(meta)
                return

        self._meta.update(meta)

        if meta.get("pending_summarize"):
            return
        if len(list(self._buffer.get())) >= self._resolved_max_buffer_events:
            self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)
            self._meta.update(meta)
            return
        if self._trigger_mode == "on_event":
            self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)
            self._meta.update(meta)
            return
        if self._trigger_mode == "count_threshold":
            threshold = max(1, self._resolved_count_threshold)
            if len(list(self._buffer.get())) >= threshold:
                self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)
                self._meta.update(meta)

    def _trim_buffer_oldest(self) -> None:
        """Drop the oldest event from the buffer."""
        events = list(self._buffer.get())
        if len(events) > 1:
            events = events[1:]
        self._buffer.clear()
        for e in events:
            self._buffer.add(e)

    def _trim_buffer_newest(self) -> None:
        """Drop the newest event from the buffer."""
        events = list(self._buffer.get())
        if len(events) > 1:
            events = events[:-1]
        else:
            events = []
        self._buffer.clear()
        for e in events:
            self._buffer.add(e)

    def _dispatch_summary_request(
        self,
        meta: Dict[str, Any],
        timer_service: Any,
        now_ms: int,
        *,
        scope_close_pending: bool = False,
        request_scope_epoch: Optional[int] = None,
    ) -> None:
        """Submit one async summary update from the current buffer contents."""
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._config.async_max_workers,
                thread_name_prefix="sem-agg",
            )
        if meta.get("pending_summarize"):
            return
        buffered_events = list(self._buffer.get())
        if not buffered_events:
            return
        current_summary = ""
        current_aggregate = self._agg_value.value()
        if isinstance(current_aggregate, dict):
            current_summary = str(current_aggregate.get("summary", "") or "")
        request_id = f"{meta.get('key', '')}:{int(meta.get('version', 0) or 0)}:{now_ms}"
        scope_epoch = int(
            meta.get("scope_epoch", 0) if request_scope_epoch is None else request_scope_epoch
        )
        self._buffer.clear()
        future = self._executor.submit(
            evaluate_sem_agg_summary_update_from_config_sync,
            llm_config=self._llm_config,
            mode=self._resolved_mode,
            current_summary=current_summary,
            added_events=buffered_events,
        )
        self._pending_futures[request_id] = future
        meta["pending_summarize"] = True
        meta["pending_request_id"] = request_id
        meta["pending_scope_epoch"] = scope_epoch
        meta["pending_scope_close"] = scope_close_pending
        meta["pending_added_event_count"] = len(buffered_events)
        if self._metrics:
            self._metrics.record_async_emit()
        register_timer(
            timer_service,
            meta,
            TimerCategory.RECOMPUTE,
            now_ms + self._resolved_async_poll_interval_ms,
        )

    def _poll_pending_summary(
        self,
        meta: Dict[str, Any],
        ctx: Any,
        now_ms: int,
    ):
        """Poll one pending summary future and merge it into canonical state."""
        request_id = str(meta.get("pending_request_id", "") or "")
        if not meta.get("pending_summarize") or not request_id:
            return
        future = self._pending_futures.get(request_id)
        if future is None:
            raise RuntimeError(f"sem_agg lost pending async request {request_id!r}")
        if not future.done():
            register_timer(
                ctx.timer_service(),
                meta,
                TimerCategory.RECOMPUTE,
                now_ms + self._resolved_async_poll_interval_ms,
            )
            return
        del self._pending_futures[request_id]
        result = future.result()
        summary = str(result["summary"])
        result_scope_epoch = int(meta.get("pending_scope_epoch", 0) or 0)
        result_scope_close = bool(meta.get("pending_scope_close", False))
        meta["pending_summarize"] = False
        meta.pop("pending_request_id", None)
        meta.pop("pending_scope_epoch", None)
        meta.pop("pending_scope_close", None)
        meta.pop("pending_added_event_count", None)
        meta["version"] = int(meta.get("version", 0) or 0) + 1
        aggregate = {
            "summary": summary,
            "version": meta["version"],
            "updated_ms": now_ms,
        }
        if not result_scope_close and result_scope_epoch == int(meta.get("scope_epoch", 0) or 0):
            self._agg_value.update(aggregate)
        self._meta.update(meta)
        yield {
            "key": meta.get("key", ""),
            "aggregate": aggregate,
            "version": meta["version"],
            "mode": f"{self._resolved_mode}_scope_close_async" if result_scope_close else f"{self._resolved_mode}_async",
            "event_count": int(meta.get("event_count", 0) or 0),
            "timestamp_ms": now_ms,
            "scope_epoch": result_scope_epoch,
        }
        if result_scope_close:
            return
        if not list(self._buffer.get()):
            return
        if self._trigger_mode == "on_event":
            self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)
            self._meta.update(meta)
            return
        if self._trigger_mode == "count_threshold":
            threshold = max(1, self._resolved_count_threshold)
            if len(list(self._buffer.get())) >= threshold:
                self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)
                self._meta.update(meta)

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

    def _emit_scope_close_output(
        self,
        meta: Dict[str, Any],
        ctx: Any,
        now_ms: int,
        reason: str,
    ):
        if self._resolved_mode == "algebraic":
            yield from self._emit_current_aggregate(meta, now_ms, reason=reason)
            return
        if meta.get("pending_summarize"):
            meta["pending_scope_close"] = True
            self._meta.update(meta)
            return
        current_aggregate = self._agg_value.value()
        if current_aggregate is not None and not list(self._buffer.get()):
            meta["version"] = int(meta.get("version", 0) or 0) + 1
            yield {
                "key": meta.get("key", ""),
                "aggregate": current_aggregate,
                "version": meta["version"],
                "mode": f"{self._resolved_mode}_scope_close",
                "event_count": int(meta.get("event_count", 0) or 0),
                "timestamp_ms": now_ms,
                "scope_epoch": int(meta.get("scope_epoch", 0) or 0),
            }
            self._meta.update(meta)
            return
        if not list(self._buffer.get()):
            return
        self._dispatch_summary_request(
            meta,
            ctx.timer_service(),
            now_ms,
            scope_close_pending=True,
            request_scope_epoch=int(meta.get("scope_epoch", 0) or 0),
        )
        self._meta.update(meta)

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
        meta["event_count"] = 0
        meta["_last_emit_count"] = 0
        meta["scope_epoch"] = int(meta.get("scope_epoch", 0) or 0) + 1
        meta["scope_last_time_ms"] = 0
        meta["scope_bucket_id"] = None
        meta["last_scope_reset_reason"] = reason

    def _process_external_scope_snapshot(
        self,
        value: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ):
        """Ingest one external scope update into the aggregate owner."""
        if self._resolved_persistence_policy != "persistent_across_scopes":
            raise RuntimeError(
                "sem_agg external_window path requires persistent_across_scopes"
            )
        scope_id = str(value.get("window_id", "") or value.get("scope_id", "")).strip()
        if not scope_id:
            raise ValueError("sem_agg external_window snapshot requires window_id")
        raw_events = list(window_snapshot_to_sem_events(value))
        if not raw_events:
            raise ValueError("sem_agg external_window snapshot requires non-empty events")
        if self._resolved_mode in {"summarize", "compressive"}:
            yield from self._process_external_scope_append_only(
                value=value,
                ctx=ctx,
                now_ms=now_ms,
                scope_id=scope_id,
                raw_events=raw_events,
            )
            return
        scoped_aggregate = _aggregate_event_records(raw_events, reduce_fn=self._config.reduce_fn)
        self._scope_contributions.put(
            scope_id,
            {
                "scope_id": scope_id,
                "aggregate": scoped_aggregate,
                "event_count": len(raw_events),
                "updated_ms": now_ms,
            },
        )
        global_aggregate, total_event_count = self._recompute_external_global_aggregate()
        self._agg_value.update(global_aggregate)
        meta = self._meta.value() or {
            "key": str(value.get("key", str(ctx.get_current_key()))),
            "event_count": 0,
            "version": 0,
        }
        meta["event_count"] = total_event_count
        meta["version"] = int(meta.get("version", 0) or 0) + 1
        meta["last_scope_id"] = scope_id
        meta["last_scope_update_ms"] = now_ms
        self._meta.update(meta)
        yield {
            "key": meta.get("key", ""),
            "aggregate": global_aggregate,
            "version": meta["version"],
            "mode": "algebraic_scope_fire",
            "event_count": total_event_count,
            "timestamp_ms": now_ms,
            "scope_id": scope_id,
        }

    def _process_external_scope_append_only(
        self,
        *,
        value: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
        scope_id: str,
        raw_events: List[Dict[str, Any]],
    ):
        """Append only newly visible scope events into the summary/compression fold."""
        meta = self._meta.value() or {
            "key": str(value.get("key", str(ctx.get_current_key()))),
            "event_count": 0,
            "version": 0,
            "scope_epoch": 0,
            "scope_last_time_ms": 0,
            "scope_bucket_id": None,
            "pending_summarize": False,
        }
        progress = self._scope_progress.get(scope_id) if self._scope_progress is not None else None
        seen_seq_ids = {
            int(seq_id)
            for seq_id in (progress or {}).get("seen_event_seq_ids", [])
        }
        new_events: List[Dict[str, Any]] = []
        new_seq_ids: List[int] = []
        for event in raw_events:
            seq_id = int(event.get("seq_id", event.get("event_seq_id", 0)) or 0)
            if seq_id in seen_seq_ids:
                continue
            seen_seq_ids.add(seq_id)
            new_seq_ids.append(seq_id)
            new_events.append(event)
        if self._scope_progress is not None:
            self._scope_progress.put(
                scope_id,
                {
                    "seen_event_seq_ids": sorted(seen_seq_ids),
                    "last_update_ms": now_ms,
                    "newly_seen_event_seq_ids": new_seq_ids,
                },
            )
        if not new_events:
            self._meta.update(meta)
            return
        for event in new_events:
            self._buffer.add(event)
        meta["event_count"] = int(meta.get("event_count", 0) or 0) + len(new_events)
        meta["last_scope_id"] = scope_id
        meta["last_scope_update_ms"] = now_ms
        self._meta.update(meta)
        if meta.get("pending_summarize"):
            return
        if self._trigger_mode in {"on_scope_close", "on_event"}:
            self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)
            self._meta.update(meta)
            return
        if self._trigger_mode == "count_threshold":
            threshold = max(1, self._resolved_count_threshold)
            if len(list(self._buffer.get())) >= threshold:
                self._dispatch_summary_request(meta, ctx.timer_service(), now_ms)
                self._meta.update(meta)

    def _recompute_external_global_aggregate(self) -> tuple[Dict[str, Any], int]:
        """Recompute one global aggregate from current external scope contributions."""
        contributions = [
            value
            for _, value in self._scope_contributions.items()
        ]
        if not contributions:
            raise ValueError(
                "sem_agg external_window persistent path requires at least one scope contribution"
            )
        aggregates = [dict(item["aggregate"]) for item in contributions]
        total_event_count = sum(int(item.get("event_count", 0) or 0) for item in contributions)
        return _aggregate_event_records(
            aggregates,
            reduce_fn=self._config.reduce_fn,
        ), total_event_count
