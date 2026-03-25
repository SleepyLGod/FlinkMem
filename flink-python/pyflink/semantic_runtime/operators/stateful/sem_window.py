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
from dataclasses import dataclass
from typing import Any, Dict, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, ValueState

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
    sem_window_event_buffer_descriptor,
    sem_window_meta_descriptor,
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
        self._metrics: Optional[StatefulOperatorMetrics] = None
        self._client: Optional[LLMClient] = None
        self._embedding_runtime: Optional[EmbeddingRuntime] = None

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
            "SemWindowFunction opened (max_events=%d, timeout_ms=%d, boundary=%s, variant=%s)",
            self._config.max_window_events,
            self._config.window_timeout_ms,
            self._config.boundary_flag,
            self._config.continuity_variant,
        )
        self._initialize_continuity_runtime()

    def close(self) -> None:
        """Release continuity runtime resources."""
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
        if (
            self._config.continuity_variant == "boundary_flag"
            and event.has_boundary(self._config.boundary_flag)
        ):
            return "semantic_boundary"
        # 2. Count trigger
        if meta["event_count"] >= self._config.max_window_events:
            return "count"
        return None

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
            return self._evaluate_summary_continuity(
                meta=self._window_meta.value(),
                current_event=event.to_dict(),
            )
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
    ) -> bool:
        """Evaluate summary-based continuity using the internal sem_continuity step."""
        if meta is None:
            return True
        current_summary = str(meta.get("window_summary") or "").strip()
        if not current_summary:
            return True
        if self._client is None or self._llm_config is None:
            raise RuntimeError("sem_window summary continuity runtime is not initialized")
        result = evaluate_summary_sem_continuity_sync(
            client=self._client,
            llm_config=self._llm_config,
            current_summary=current_summary,
            current_event=current_event,
        )
        return bool(result["continue_window"])

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
