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

import concurrent.futures
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, MapState, ValueState

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime.embedding_runtime import (
    EmbeddingRuntime,
    create_embedding_runtime,
)
from pyflink.semantic_runtime.runtime.steps.sem_continuity import (
    evaluate_all_history_sem_continuity_sync,
    evaluate_pairwise_sem_continuity_sync,
    evaluate_summary_sem_continuity_sync,
)
from pyflink.semantic_runtime.runtime.steps.sem_window_summary import (
    update_sem_window_summary_sync,
)
from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    sem_window_active_windows_descriptor,
    sem_window_deferred_events_descriptor,
    sem_window_event_buffer_descriptor,
    sem_window_meta_descriptor,
    sem_window_pending_continuity_descriptor,
)
from pyflink.semantic_runtime.runtime.stateful_async_executor import (
    ensure_thread_pool_executor,
)
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    WindowSnapshot,
)
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics

logger = logging.getLogger(__name__)

_VALID_CONTINUITY_VARIANTS = {
    "boundary_flag",
    "pairwise",
    "embedding",
    "summary",
    "all_history",
}
_DEFAULT_PAIRWISE_CONTINUITY_THRESHOLD = 0.35
DEFAULT_SEM_WINDOW_ASYNC_MAX_WORKERS = 20
DEFAULT_SEM_WINDOW_ASYNC_POLL_INTERVAL_MS = 200


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SemWindowConfig:
    """Configuration for the semantic window operator."""
    max_window_events: int = 50        # count trigger threshold
    window_timeout_ms: int = 30_000    # time trigger (ms since window open)
    boundary_flag: str = "topic_shift" # which flag to check for semantic boundary
    continuity_variant: str = "boundary_flag"
    continuity_threshold: float = _DEFAULT_PAIRWISE_CONTINUITY_THRESHOLD
    llm_async_enabled: bool = True
    async_max_workers: int = DEFAULT_SEM_WINDOW_ASYNC_MAX_WORKERS
    async_poll_interval_ms: int = DEFAULT_SEM_WINDOW_ASYNC_POLL_INTERVAL_MS
    ttl_seconds: int = 3600
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST

    def __post_init__(self) -> None:
        if self.continuity_variant not in _VALID_CONTINUITY_VARIANTS:
            raise ValueError(
                f"Invalid continuity_variant={self.continuity_variant!r}. "
                f"Must be one of {_VALID_CONTINUITY_VARIANTS}."
            )
        if not isinstance(self.continuity_threshold, (int, float)):
            raise TypeError("continuity_threshold must be numeric")
        if self.async_max_workers <= 0:
            raise ValueError("sem_window async_max_workers must be > 0")
        if self.async_poll_interval_ms <= 0:
            raise ValueError("sem_window async_poll_interval_ms must be > 0")


# ---------------------------------------------------------------------------
# Window metadata (stored in ValueState)
# ---------------------------------------------------------------------------

def _new_window_meta(open_time_ms: int) -> Dict[str, Any]:
    return {
        "window_id": uuid.uuid4().hex[:12],
        "open_time_ms": open_time_ms,
        "event_count": 0,
        "window_summary": None,
    }


def _new_active_window_record(
    *,
    key: str,
    first_event: Dict[str, Any],
    now_ms: int,
) -> Dict[str, Any]:
    """Build one new active semantic-window record."""
    payload = str(first_event.get("payload", ""))
    return {
        "window_id": uuid.uuid4().hex[:12],
        "key": key,
        "events": [first_event],
        "event_count": 1,
        "open_time_ms": now_ms,
        "last_update_ms": now_ms,
        "window_summary": payload,
        "representative_text": payload,
    }


def _evaluate_summary_assignment_with_update(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    active_records: list[Dict[str, Any]],
    current_event: Dict[str, Any],
) -> Dict[str, Any]:
    """Evaluate summary-based assignment and compute updated summary in worker."""
    best_window_id: Optional[str] = None
    best_summary: Optional[str] = None
    best_confidence = float("-inf")
    for record in active_records:
        summary = str(record.get("window_summary") or "").strip()
        if not summary:
            continue
        result = evaluate_summary_sem_continuity_sync(
            client=client,
            llm_config=llm_config,
            current_summary=summary,
            current_event=current_event,
        )
        if not bool(result["continue_window"]):
            continue
        confidence = float(result["confidence"])
        if confidence <= best_confidence:
            continue
        best_confidence = confidence
        best_window_id = str(record["window_id"])
        best_summary = summary
    if best_window_id is None or best_summary is None:
        return {
            "matched_window_id": None,
            "updated_summary": None,
        }
    updated_summary = update_sem_window_summary_sync(
        client=client,
        llm_config=llm_config,
        current_summary=best_summary,
        current_event=current_event,
    )
    return {
        "matched_window_id": best_window_id,
        "updated_summary": str(updated_summary),
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

    def __init__(
        self,
        config: Optional[SemWindowConfig] = None,
        *,
        llm_config: Optional[LLMClientConfig] = None,
        embedding_config: Optional[Any] = None,
    ) -> None:
        self._config = config or SemWindowConfig()
        self._llm_config = llm_config
        self._embedding_config = embedding_config
        # State handles — initialised in open()
        self._event_buffer: Optional[ListState] = None
        self._window_meta: Optional[ValueState] = None
        self._active_windows: Optional[MapState] = None
        self._pending_continuity: Optional[ValueState] = None
        self._deferred_events: Optional[ListState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None
        self._client: Optional[LLMClient] = None
        self._embedding_runtime: Optional[EmbeddingRuntime] = None
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._pending_futures: Dict[str, concurrent.futures.Future] = {}

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._config.ttl_seconds
        self._event_buffer = runtime_context.get_list_state(
            sem_window_event_buffer_descriptor(ttl)
        )
        self._window_meta = runtime_context.get_state(
            sem_window_meta_descriptor(ttl)
        )
        self._active_windows = runtime_context.get_map_state(
            sem_window_active_windows_descriptor(ttl)
        )
        self._pending_continuity = runtime_context.get_state(
            sem_window_pending_continuity_descriptor(ttl)
        )
        self._deferred_events = runtime_context.get_list_state(
            sem_window_deferred_events_descriptor(ttl)
        )
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_window",
        )
        logger.info(
            "SemWindowFunction opened (max_events=%d, timeout_ms=%d, boundary=%s, variant=%s)",
            self._config.max_window_events,
            self._config.window_timeout_ms,
            self._config.boundary_flag,
            self._config.continuity_variant,
        )
        self._initialize_continuity_runtime()

    def close(self) -> None:
        """Release continuity runtime resources."""
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None
        self._pending_futures.clear()
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._embedding_runtime is not None:
            self._embedding_runtime.close()
            self._embedding_runtime = None

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
            raise TypeError("sem_window expects SemEvent-shaped dict input")

        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        if self._uses_async_continuity():
            if self._config.continuity_variant == "summary":
                yield from self._process_async_summary_event(
                    event=event,
                    event_dict=event_dict,
                    ctx=ctx,
                    now_ms=now_ms,
                )
            else:
                yield from self._process_async_single_window_event(
                    event=event,
                    event_dict=event_dict,
                    ctx=ctx,
                    now_ms=now_ms,
                )
            return

        if self._uses_multi_active_windows():
            yield from self._process_multi_active_event(
                event=event,
                event_dict=event_dict,
                ctx=ctx,
                now_ms=now_ms,
            )
            return

        # --- ensure window is open ---
        meta = self._window_meta.value()
        if meta is None:
            meta = self._open_window_meta(ctx, event, now_ms)
        elif self._should_roll_window_before_continuity_check():
            if self._metrics:
                self._metrics.record_boundary_trigger()
            yield from self._emit_snapshot(meta, "count", now_ms)
            meta = self._open_window_meta(ctx, event, now_ms)
        elif not self._should_continue_current_window(event):
            if self._metrics:
                self._metrics.record_boundary_trigger()
            yield from self._emit_snapshot(meta, "semantic_boundary", now_ms)
            meta = self._open_window_meta(ctx, event, now_ms)

        self._append_event(meta, event_dict)
        self._refresh_summary_after_buffer_change(meta)
        self._window_meta.update(meta)

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
        now_ms = int(time.time() * 1000)
        if self._uses_async_continuity():
            yield from self._poll_pending_continuity(
                ctx=ctx,
                now_ms=now_ms,
            )
            if self._has_pending_continuity():
                ctx.timer_service().register_processing_time_timer(
                    now_ms + self._config.async_poll_interval_ms
                )
                return

        if self._uses_multi_active_windows():
            yield from self._on_multi_active_timer(timestamp)
            return

        meta = self._window_meta.value()
        if meta is None:
            return  # window already closed/emitted

        if self._metrics:
            self._metrics.record_timer_fire()

        # Resolve which timer category fired
        category = resolve_timer_category(meta, timestamp, tolerance_ms=0)
        if category is None:
            return  # stale or unknown timer

        if category == TimerCategory.FLUSH:
            if self._uses_async_continuity() and self._has_pending_continuity():
                register_timer(
                    ctx.timer_service(),
                    meta,
                    TimerCategory.FLUSH,
                    now_ms + self._config.async_poll_interval_ms,
                )
                self._window_meta.update(meta)
                return
            clear_timer_registration(meta, TimerCategory.FLUSH)
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
        if (
            self._config.continuity_variant == "boundary_flag"
            and event.has_boundary(self._config.boundary_flag)
        ):
            return "semantic_boundary"
        # 2. Count trigger
        if meta["event_count"] >= self._config.max_window_events:
            return "count"
        return None

    def _uses_multi_active_windows(self) -> bool:
        """Return whether the variant uses CP-style multi-active windows."""
        return self._config.continuity_variant in {"summary", "embedding"}

    def _uses_async_continuity(self) -> bool:
        """Return whether current continuity variant uses async bridge."""
        if not self._config.llm_async_enabled:
            return False
        return self._config.continuity_variant in {"pairwise", "all_history", "summary"}

    def _uses_async_single_window_continuity(self) -> bool:
        """Return whether single-window continuity checks run via async bridge."""
        return self._uses_async_continuity() and self._config.continuity_variant in {
            "pairwise",
            "all_history",
        }

    def _has_pending_continuity(self) -> bool:
        """Return whether one continuity request is currently in-flight."""
        assert self._pending_continuity is not None
        pending = self._pending_continuity.value()
        return isinstance(pending, dict) and bool(pending.get("request_id"))

    def _enqueue_deferred_event(self, event_dict: Dict[str, Any]) -> None:
        """Buffer one event while waiting for an async continuity decision."""
        assert self._deferred_events is not None
        self._deferred_events.add(dict(event_dict))

    def _dequeue_deferred_event(self) -> Optional[Dict[str, Any]]:
        """Pop the oldest deferred event, preserving arrival order."""
        assert self._deferred_events is not None
        queued = list(self._deferred_events.get())
        if not queued:
            return None
        first = dict(queued[0])
        self._deferred_events.clear()
        for item in queued[1:]:
            self._deferred_events.add(item)
        return first

    def _process_async_single_window_event(
        self,
        *,
        event: SemEvent,
        event_dict: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ):
        """Process one event with non-blocking async continuity checks."""
        outputs: list[Dict[str, Any]] = []
        outputs.extend(self._poll_pending_continuity(ctx=ctx, now_ms=now_ms))
        if self._has_pending_continuity():
            self._enqueue_deferred_event(event_dict)
            ctx.timer_service().register_processing_time_timer(
                now_ms + self._config.async_poll_interval_ms
            )
            yield from outputs
            return

        outputs.extend(
            self._process_single_window_event_ready(
                event=event,
                event_dict=event_dict,
                ctx=ctx,
                now_ms=now_ms,
            )
        )
        while not self._has_pending_continuity():
            next_event_dict = self._dequeue_deferred_event()
            if next_event_dict is None:
                break
            outputs.extend(
                self._process_single_window_event_ready(
                    event=SemEvent.from_dict(next_event_dict),
                    event_dict=next_event_dict,
                    ctx=ctx,
                    now_ms=now_ms,
                )
            )

        if self._has_pending_continuity():
            ctx.timer_service().register_processing_time_timer(
                now_ms + self._config.async_poll_interval_ms
            )
        yield from outputs

    def _process_async_summary_event(
        self,
        *,
        event: SemEvent,
        event_dict: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ):
        """Process one summary-variant event with async non-blocking assignment."""
        outputs: list[Dict[str, Any]] = []
        outputs.extend(self._poll_pending_continuity(ctx=ctx, now_ms=now_ms))
        if self._has_pending_continuity():
            self._enqueue_deferred_event(event_dict)
            ctx.timer_service().register_processing_time_timer(
                now_ms + self._config.async_poll_interval_ms
            )
            yield from outputs
            return
        outputs.extend(
            self._process_multi_active_summary_event_ready(
                event=event,
                event_dict=event_dict,
                ctx=ctx,
                now_ms=now_ms,
            )
        )
        while not self._has_pending_continuity():
            next_event_dict = self._dequeue_deferred_event()
            if next_event_dict is None:
                break
            outputs.extend(
                self._process_multi_active_summary_event_ready(
                    event=SemEvent.from_dict(next_event_dict),
                    event_dict=next_event_dict,
                    ctx=ctx,
                    now_ms=now_ms,
                )
            )
        if self._has_pending_continuity():
            ctx.timer_service().register_processing_time_timer(
                now_ms + self._config.async_poll_interval_ms
            )
        yield from outputs

    def _process_multi_active_summary_event_ready(
        self,
        *,
        event: SemEvent,
        event_dict: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> list[Dict[str, Any]]:
        """Process one summary event when no async assignment is in-flight."""
        records = self._list_active_window_records()
        if not records:
            record = self._open_active_window_record(ctx, event, event_dict, now_ms)
            if int(record["event_count"]) >= self._config.max_window_events:
                if self._metrics:
                    self._metrics.record_boundary_trigger()
                snapshot = self._build_active_window_snapshot(
                    record=record,
                    trigger_reason="count",
                    close_time_ms=now_ms,
                )
                self._remove_active_window_record(str(record["window_id"]))
                return [snapshot.to_dict()]
            return []
        self._dispatch_summary_assignment_async(
            ctx=ctx,
            now_ms=now_ms,
            active_records=records,
            event_dict=event_dict,
        )
        return []

    def _process_single_window_event_ready(
        self,
        *,
        event: SemEvent,
        event_dict: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> list[Dict[str, Any]]:
        """Process one event when no continuity request is in-flight."""
        assert self._window_meta is not None
        assert self._event_buffer is not None
        outputs: list[Dict[str, Any]] = []
        meta = self._window_meta.value()
        if meta is None:
            meta = self._open_window_meta(ctx, event, now_ms)
        elif self._should_roll_window_before_continuity_check():
            if self._metrics:
                self._metrics.record_boundary_trigger()
            outputs.extend(list(self._emit_snapshot(meta, "count", now_ms)))
            meta = self._open_window_meta(ctx, event, now_ms)
        else:
            events = list(self._event_buffer.get())
            if events:
                self._dispatch_continuity_async(
                    ctx=ctx,
                    now_ms=now_ms,
                    previous_events=events,
                    event_dict=event_dict,
                )
                return outputs

        self._append_event(meta, event_dict)
        self._refresh_summary_after_buffer_change(meta)
        self._window_meta.update(meta)
        trigger_reason = self._check_triggers(event, meta)
        if trigger_reason:
            if self._metrics:
                self._metrics.record_boundary_trigger()
            outputs.extend(list(self._emit_snapshot(meta, trigger_reason, now_ms)))
        return outputs

    def _dispatch_continuity_async(
        self,
        *,
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
        previous_events: list[Dict[str, Any]],
        event_dict: Dict[str, Any],
    ) -> None:
        """Dispatch one non-blocking continuity request."""
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window LLM continuity runtime is not initialized")
        if self._has_pending_continuity():
            raise RuntimeError("sem_window continuity single-flight violated")
        if self._executor is None:
            self._executor = ensure_thread_pool_executor(
                self._executor,
                max_workers=self._config.async_max_workers,
                thread_name_prefix="sem-window",
            )
        variant = self._config.continuity_variant
        request_id = uuid.uuid4().hex
        if variant == "pairwise":
            future = self._executor.submit(
                evaluate_pairwise_sem_continuity_sync,
                client=self._client,
                llm_config=self._llm_config,
                previous_event=dict(previous_events[-1]),
                current_event=dict(event_dict),
            )
        elif variant == "all_history":
            future = self._executor.submit(
                evaluate_all_history_sem_continuity_sync,
                client=self._client,
                llm_config=self._llm_config,
                active_window_events=[dict(item) for item in previous_events],
                current_event=dict(event_dict),
            )
        else:
            raise ValueError(
                f"sem_window async continuity does not support variant {variant!r}"
            )
        self._pending_futures[request_id] = future
        assert self._pending_continuity is not None
        self._pending_continuity.update(
            {
                "request_id": request_id,
                "variant": variant,
                "event": dict(event_dict),
                "created_at_ms": int(now_ms),
            }
        )
        ctx.timer_service().register_processing_time_timer(
            now_ms + self._config.async_poll_interval_ms
        )

    def _dispatch_summary_assignment_async(
        self,
        *,
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
        active_records: list[Dict[str, Any]],
        event_dict: Dict[str, Any],
    ) -> None:
        """Dispatch one async summary assignment+update request."""
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window summary runtime is not initialized")
        if self._has_pending_continuity():
            raise RuntimeError("sem_window summary assignment single-flight violated")
        if self._executor is None:
            self._executor = ensure_thread_pool_executor(
                self._executor,
                max_workers=self._config.async_max_workers,
                thread_name_prefix="sem-window",
            )
        request_id = uuid.uuid4().hex
        future = self._executor.submit(
            _evaluate_summary_assignment_with_update,
            client=self._client,
            llm_config=self._llm_config,
            active_records=[
                {
                    "window_id": str(record["window_id"]),
                    "window_summary": str(record.get("window_summary") or ""),
                }
                for record in active_records
            ],
            current_event=dict(event_dict),
        )
        self._pending_futures[request_id] = future
        assert self._pending_continuity is not None
        self._pending_continuity.update(
            {
                "request_id": request_id,
                "variant": "summary",
                "event": dict(event_dict),
                "created_at_ms": int(now_ms),
            }
        )
        ctx.timer_service().register_processing_time_timer(
            now_ms + self._config.async_poll_interval_ms
        )

    def _poll_pending_continuity(
        self,
        *,
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> list[Dict[str, Any]]:
        """Apply ready continuity results and drain deferred queue."""
        assert self._pending_continuity is not None
        pending = self._pending_continuity.value()
        if not isinstance(pending, dict):
            return []
        request_id = str(pending.get("request_id", ""))
        if not request_id:
            return []
        future = self._pending_futures.get(request_id)
        if future is None:
            raise RuntimeError(f"sem_window lost pending continuity future {request_id!r}")
        if not future.done():
            return []
        result = future.result()
        if not isinstance(result, dict):
            raise RuntimeError("sem_window continuity result must be a dict")
        del self._pending_futures[request_id]
        self._pending_continuity.clear()

        variant = str(pending.get("variant", ""))
        if variant in {"pairwise", "all_history"}:
            outputs = self._apply_pending_continuity_result(
                pending=pending,
                result=result,
                ctx=ctx,
                now_ms=now_ms,
            )
        elif variant == "summary":
            outputs = self._apply_pending_summary_assignment_result(
                pending=pending,
                result=result,
                ctx=ctx,
                now_ms=now_ms,
            )
        else:
            raise RuntimeError(
                f"sem_window pending continuity has unsupported variant {variant!r}"
            )
        while not self._has_pending_continuity():
            next_event_dict = self._dequeue_deferred_event()
            if next_event_dict is None:
                break
            if self._config.continuity_variant == "summary":
                outputs.extend(
                    self._process_multi_active_summary_event_ready(
                        event=SemEvent.from_dict(next_event_dict),
                        event_dict=next_event_dict,
                        ctx=ctx,
                        now_ms=now_ms,
                    )
                )
            else:
                outputs.extend(
                    self._process_single_window_event_ready(
                        event=SemEvent.from_dict(next_event_dict),
                        event_dict=next_event_dict,
                        ctx=ctx,
                        now_ms=now_ms,
                    )
                )
        return outputs

    def _apply_pending_continuity_result(
        self,
        *,
        pending: Dict[str, Any],
        result: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> list[Dict[str, Any]]:
        """Apply one completed continuity decision in keyed owner context."""
        assert self._window_meta is not None
        meta = self._window_meta.value()
        if meta is None:
            raise RuntimeError("sem_window pending continuity resolved without active window")
        event_dict = pending.get("event")
        if not isinstance(event_dict, dict):
            raise RuntimeError("sem_window pending continuity payload missing event")
        event = SemEvent.from_dict(event_dict)
        outputs: list[Dict[str, Any]] = []
        if not bool(result.get("continue_window")):
            if self._metrics:
                self._metrics.record_boundary_trigger()
            outputs.extend(list(self._emit_snapshot(meta, "semantic_boundary", now_ms)))
            meta = self._open_window_meta(ctx, event, now_ms)
        self._append_event(meta, event_dict)
        self._refresh_summary_after_buffer_change(meta)
        self._window_meta.update(meta)
        trigger_reason = self._check_triggers(event, meta)
        if trigger_reason:
            if self._metrics:
                self._metrics.record_boundary_trigger()
            outputs.extend(list(self._emit_snapshot(meta, trigger_reason, now_ms)))
        return outputs

    def _apply_pending_summary_assignment_result(
        self,
        *,
        pending: Dict[str, Any],
        result: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> list[Dict[str, Any]]:
        """Apply one completed summary assignment decision in owner context."""
        event_dict = pending.get("event")
        if not isinstance(event_dict, dict):
            raise RuntimeError("sem_window pending summary payload missing event")
        event = SemEvent.from_dict(event_dict)
        matched_window_id = result.get("matched_window_id")
        outputs: list[Dict[str, Any]] = []
        if matched_window_id is None:
            record = self._open_active_window_record(ctx, event, event_dict, now_ms)
        else:
            window_id = str(matched_window_id)
            assert self._active_windows is not None
            record = self._active_windows.get(window_id)
            if record is None:
                raise RuntimeError(
                    f"sem_window summary target window {window_id!r} no longer exists"
                )
            events = list(record.get("events", []))
            events.append(event_dict)
            record["events"] = events
            record["event_count"] = len(events)
            record["last_update_ms"] = now_ms
            updated_summary = result.get("updated_summary")
            if not isinstance(updated_summary, str) or not updated_summary.strip():
                raise RuntimeError("sem_window summary assignment missing updated_summary")
            record["window_summary"] = updated_summary
            record["representative_text"] = updated_summary
            self._register_active_window_flush_timer(
                ctx=ctx,
                record=record,
                base_time_ms=now_ms,
            )
            self._put_active_window_record(record)
        if int(record["event_count"]) >= self._config.max_window_events:
            if self._metrics:
                self._metrics.record_boundary_trigger()
            outputs.append(
                self._build_active_window_snapshot(
                    record=record,
                    trigger_reason="count",
                    close_time_ms=now_ms,
                ).to_dict()
            )
            self._remove_active_window_record(str(record["window_id"]))
        return outputs

    def _emit_snapshot(
        self, meta: Dict[str, Any], trigger_reason: str, close_time_ms: int
    ):
        """Build and yield a WindowSnapshot, then reset state."""
        assert self._event_buffer is not None
        assert self._window_meta is not None
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

    def _open_window_meta(
        self,
        ctx: "KeyedProcessFunction.Context",
        event: SemEvent,
        now_ms: int,
    ) -> Dict[str, Any]:
        """Open one new window and register its flush timer."""
        meta = _new_window_meta(now_ms)
        meta["key"] = event.key
        register_timer(
            ctx.timer_service(),
            meta,
            TimerCategory.FLUSH,
            now_ms + self._config.window_timeout_ms,
        )
        return meta

    def _open_active_window_record(
        self,
        ctx: "KeyedProcessFunction.Context",
        event: SemEvent,
        event_dict: Dict[str, Any],
        now_ms: int,
    ) -> Dict[str, Any]:
        """Create and register one new active semantic window."""
        record = _new_active_window_record(
            key=event.key,
            first_event=event_dict,
            now_ms=now_ms,
        )
        self._register_active_window_flush_timer(
            ctx=ctx,
            record=record,
            base_time_ms=now_ms,
        )
        self._put_active_window_record(record)
        return record

    def _append_event(self, meta: Dict[str, Any], event_dict: Dict[str, Any]) -> None:
        """Append one event to the active buffer and enforce overflow policy."""
        assert self._event_buffer is not None
        meta["event_count"] += 1
        self._event_buffer.add(event_dict)
        count = meta["event_count"]
        if count <= self._config.max_window_events:
            return
        if self._config.overflow_policy == OverflowPolicy.DROP_OLDEST:
            self._trim_buffer_oldest(meta)
            self._refresh_summary_after_buffer_change(meta)
            return
        if self._config.overflow_policy == OverflowPolicy.DROP_NEWEST:
            meta["event_count"] -= 1
            self._rebuild_buffer_without_last()
            self._refresh_summary_after_buffer_change(meta)
            return
        raise ValueError(f"Unsupported overflow policy: {self._config.overflow_policy!r}")

    def _initialize_continuity_runtime(self) -> None:
        """Initialize internal runtime objects needed by the chosen continuity variant."""
        variant = self._config.continuity_variant
        if variant == "boundary_flag":
            return
        if variant == "pairwise":
            if self._llm_config is None:
                raise ValueError("sem_window pairwise continuity requires llm_config")
            self._client = create_llm_client(self._llm_config)
            return
        if variant == "embedding":
            if self._embedding_config is None:
                raise ValueError("sem_window embedding continuity requires embedding_config")
            self._embedding_runtime = create_embedding_runtime(self._embedding_config)
            return
        if variant == "summary":
            if self._llm_config is None:
                raise ValueError("sem_window summary continuity requires llm_config")
            self._client = create_llm_client(self._llm_config)
            return
        if variant == "all_history":
            if self._llm_config is None:
                raise ValueError("sem_window all_history continuity requires llm_config")
            self._client = create_llm_client(self._llm_config)
            return
        raise ValueError(f"Unsupported continuity variant {variant!r}")

    def _process_multi_active_event(
        self,
        *,
        event: SemEvent,
        event_dict: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ):
        """Process one event with a CP-style multi-active window assignment model."""
        variant = self._config.continuity_variant
        if variant == "summary":
            matched_window = self._select_summary_window(current_event=event_dict)
        elif variant == "embedding":
            matched_window = self._select_embedding_window(current_event=event_dict)
        else:
            raise ValueError(
                f"Multi-active processing does not support variant {variant!r}"
            )

        if matched_window is None:
            record = self._open_active_window_record(ctx, event, event_dict, now_ms)
        else:
            record = matched_window
            self._append_event_to_active_window(record, event_dict, now_ms)
            self._register_active_window_flush_timer(
                ctx=ctx,
                record=record,
                base_time_ms=now_ms,
            )
            self._put_active_window_record(record)

        if int(record["event_count"]) >= self._config.max_window_events:
            if self._metrics:
                self._metrics.record_boundary_trigger()
            yield self._build_active_window_snapshot(
                record=record,
                trigger_reason="count",
                close_time_ms=now_ms,
            ).to_dict()
            self._remove_active_window_record(str(record["window_id"]))

    def _on_multi_active_timer(self, timestamp: int):
        """Flush any active semantic windows whose inactivity timer has expired."""
        if self._metrics:
            self._metrics.record_timer_fire()
        for record in self._list_active_window_records():
            category = resolve_timer_category(record, timestamp, tolerance_ms=0)
            if category != TimerCategory.FLUSH:
                continue
            clear_timer_registration(record, TimerCategory.FLUSH)
            close_time_ms = int(time.time() * 1000)
            yield self._build_active_window_snapshot(
                record=record,
                trigger_reason="time",
                close_time_ms=close_time_ms,
            ).to_dict()
            self._remove_active_window_record(str(record["window_id"]))

    def _list_active_window_records(self) -> list[Dict[str, Any]]:
        """Return all active semantic windows for the current keyed partition."""
        assert self._active_windows is not None
        records: list[Dict[str, Any]] = []
        for window_id in self._active_windows.keys():
            record = self._active_windows.get(window_id)
            if record is None:
                continue
            records.append(record)
        return records

    def _put_active_window_record(self, record: Dict[str, Any]) -> None:
        """Persist one active semantic-window record."""
        assert self._active_windows is not None
        self._active_windows.put(str(record["window_id"]), record)

    def _remove_active_window_record(self, window_id: str) -> None:
        """Remove one active semantic window."""
        assert self._active_windows is not None
        self._active_windows.remove(window_id)

    def _register_active_window_flush_timer(
        self,
        *,
        ctx: "KeyedProcessFunction.Context",
        record: Dict[str, Any],
        base_time_ms: int,
    ) -> None:
        """Register one inactivity-based flush timer for an active semantic window."""
        register_timer(
            ctx.timer_service(),
            record,
            TimerCategory.FLUSH,
            base_time_ms + self._config.window_timeout_ms,
        )

    def _append_event_to_active_window(
        self,
        record: Dict[str, Any],
        event_dict: Dict[str, Any],
        now_ms: int,
    ) -> None:
        """Append one event to one active semantic window record."""
        events = list(record.get("events", []))
        events.append(event_dict)
        record["events"] = events
        record["event_count"] = len(events)
        record["last_update_ms"] = now_ms
        if self._config.continuity_variant == "summary":
            self._update_active_window_summary(record, event_dict)
            record["representative_text"] = str(record.get("window_summary") or "")
            return
        if self._config.continuity_variant == "embedding":
            record["representative_text"] = self._build_embedding_representative(record)
            return
        raise ValueError(
            f"Active-window append does not support variant {self._config.continuity_variant!r}"
        )

    def _build_active_window_snapshot(
        self,
        *,
        record: Dict[str, Any],
        trigger_reason: str,
        close_time_ms: int,
    ) -> WindowSnapshot:
        """Convert one active semantic-window record into a WindowSnapshot."""
        events = list(record.get("events", []))
        return WindowSnapshot(
            key=str(record.get("key", "")),
            window_id=str(record["window_id"]),
            events=events,
            event_count=len(events),
            open_time_ms=int(record.get("open_time_ms", 0)),
            close_time_ms=close_time_ms,
            trigger_reason=trigger_reason,
        )

    def _select_summary_window(
        self,
        *,
        current_event: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Select the best active window for one event using summary continuity."""
        best_record: Optional[Dict[str, Any]] = None
        best_confidence = float("-inf")
        for record in self._list_active_window_records():
            summary = str(record.get("window_summary") or "").strip()
            if not summary:
                continue
            result = self._evaluate_summary_continuity(
                meta=record,
                current_event=current_event,
            )
            if not result["continue_window"]:
                continue
            if result["confidence"] <= best_confidence:
                continue
            best_record = record
            best_confidence = result["confidence"]
        return best_record

    def _select_embedding_window(
        self,
        *,
        current_event: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Select the best active window for one event using embedding similarity."""
        if self._embedding_runtime is None:
            raise RuntimeError("sem_window embedding continuity runtime is not initialized")
        current_text = str(current_event.get("payload", ""))
        best_record: Optional[Dict[str, Any]] = None
        best_score = float("-inf")
        threshold = float(self._config.continuity_threshold)
        for record in self._list_active_window_records():
            representative = str(
                record.get("representative_text")
                or self._build_embedding_representative(record)
            )
            score = self._embedding_runtime.similarity(representative, current_text)
            if score < threshold:
                continue
            if score <= best_score:
                continue
            best_record = record
            best_score = score
        return best_record

    def _build_embedding_representative(self, record: Dict[str, Any]) -> str:
        """Build one deterministic representative text for an active window."""
        events = list(record.get("events", []))
        payloads = [str(event.get("payload", "")) for event in events]
        return "\n".join(payloads).strip()

    def _should_roll_window_before_continuity_check(self) -> bool:
        """Return whether local hard limits require a split before LLM continuity."""
        if self._config.continuity_variant != "all_history":
            return False
        meta = self._window_meta.value()
        if meta is None:
            return False
        return int(meta.get("event_count", 0)) >= self._config.max_window_events

    def _should_continue_current_window(self, event: SemEvent) -> bool:
        """Return whether the current event should remain in the active window."""
        assert self._event_buffer is not None
        events = list(self._event_buffer.get())
        if not events:
            return True
        variant = self._config.continuity_variant
        if variant == "boundary_flag":
            return True
        if variant == "pairwise":
            return self._evaluate_pairwise_continuity(events[-1], event.to_dict())
        if variant == "embedding":
            return self._evaluate_embedding_continuity(events[-1], event.to_dict())
        if variant == "summary":
            result = self._evaluate_summary_continuity(
                meta=self._window_meta.value(),
                current_event=event.to_dict(),
            )
            return bool(result["continue_window"])
        if variant == "all_history":
            return self._evaluate_all_history_continuity(
                active_window_events=events,
                current_event=event.to_dict(),
            )
        raise ValueError(f"Unsupported continuity variant {variant!r}")

    def _evaluate_pairwise_continuity(
        self,
        previous_event: Dict[str, Any],
        current_event: Dict[str, Any],
    ) -> bool:
        """Evaluate pairwise continuity using the internal sem_continuity step."""
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window pairwise continuity runtime is not initialized")
        result = evaluate_pairwise_sem_continuity_sync(
            client=self._client,
            llm_config=self._llm_config,
            previous_event=previous_event,
            current_event=current_event,
        )
        return bool(result["continue_window"])

    def _evaluate_embedding_continuity(
        self,
        previous_event: Dict[str, Any],
        current_event: Dict[str, Any],
    ) -> bool:
        """Evaluate pairwise continuity using a local hashing encoder."""
        if self._embedding_runtime is None:
            raise RuntimeError("sem_window embedding continuity runtime is not initialized")
        previous_text = str(previous_event.get("payload", ""))
        current_text = str(current_event.get("payload", ""))
        score = self._embedding_runtime.similarity(previous_text, current_text)
        return score >= float(self._config.continuity_threshold)

    def _evaluate_summary_continuity(
        self,
        *,
        meta: Optional[Dict[str, Any]],
        current_event: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Evaluate summary-based continuity using the internal sem_continuity step."""
        if meta is None:
            return {
                "continue_window": True,
                "confidence": 1.0,
                "reason": "no active summary",
            }
        current_summary = str(meta.get("window_summary") or "").strip()
        if not current_summary:
            return {
                "continue_window": True,
                "confidence": 1.0,
                "reason": "empty active summary",
            }
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window summary continuity runtime is not initialized")
        return evaluate_summary_sem_continuity_sync(
            client=self._client,
            llm_config=self._llm_config,
            current_summary=current_summary,
            current_event=current_event,
        )

    def _evaluate_all_history_continuity(
        self,
        *,
        active_window_events: list[Dict[str, Any]],
        current_event: Dict[str, Any],
    ) -> bool:
        """Evaluate all-history continuity against the full active window."""
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window all_history continuity runtime is not initialized")
        result = evaluate_all_history_sem_continuity_sync(
            client=self._client,
            llm_config=self._llm_config,
            active_window_events=active_window_events,
            current_event=current_event,
        )
        return bool(result["continue_window"])

    def _refresh_summary_after_buffer_change(self, meta: Dict[str, Any]) -> None:
        """Refresh the internal summary after the active buffer changes."""
        if self._config.continuity_variant != "summary":
            return
        assert self._event_buffer is not None
        events = list(self._event_buffer.get())
        if not events:
            meta["window_summary"] = None
            return
        if len(events) == 1:
            meta["window_summary"] = str(events[0].get("payload", ""))
            return
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window summary continuity runtime is not initialized")
        latest_event = events[-1]
        current_summary = str(meta.get("window_summary") or events[-2].get("payload", ""))
        meta["window_summary"] = update_sem_window_summary_sync(
            client=self._client,
            llm_config=self._llm_config,
            current_summary=current_summary,
            current_event=latest_event,
        )

    def _update_active_window_summary(
        self,
        record: Dict[str, Any],
        latest_event: Dict[str, Any],
    ) -> None:
        """Refresh one active semantic-window summary after an append."""
        current_summary = str(record.get("window_summary") or "").strip()
        if not current_summary:
            record["window_summary"] = str(latest_event.get("payload", ""))
            return
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window summary continuity runtime is not initialized")
        record["window_summary"] = update_sem_window_summary_sync(
            client=self._client,
            llm_config=self._llm_config,
            current_summary=current_summary,
            current_event=latest_event,
        )

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
