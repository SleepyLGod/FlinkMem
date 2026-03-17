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
sem_groupby — dynamic semantic category assignment over keyed state.

Input: keyed ``SemanticEvent`` dicts or ``WindowSnapshot`` dicts.

State model:
  - ``MapState[group_id -> group_profile]`` for semantic bucket summaries.
  - ``ValueState[meta]`` for counters and eviction bookkeeping.

Assignment flow:
  1. Local candidate-group proposal from current state (keyword / embedding match).
  2. If confidence is below threshold → emit side-output ``AsyncWorkItem``
     for async semantic classification.
  3. On async result merge-back → update group profile.

Guardrails:
  - ``max_groups_per_key``: hard cap on distinct groups per key.
  - Per-group TTL via ``StateTtlConfig``.
  - Overflow evicts least-recently-updated group.

V0.2 boundary:
  - No retroactive full reassignment over historical records.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapState, ValueState

from pyflink.semantic_runtime.stateful.state_descriptors import (
    OverflowPolicy,
    sem_groupby_profiles_descriptor,
    sem_window_meta_descriptor,
    build_ttl_config,
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
class SemGroupbyConfig:
    """Configuration for the semantic groupby operator."""
    max_groups_per_key: int = 50
    confidence_threshold: float = 0.7   # below this → async classify
    ttl_seconds: int = 3600
    evict_interval_ms: int = 60_000     # timer-driven stale group eviction
    new_group_creation_threshold: float = 0.3  # min similarity to reuse a group
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST


# ---------------------------------------------------------------------------
# Group profile schema
# ---------------------------------------------------------------------------

def _new_group_profile(group_id: str, label: str, now_ms: int) -> Dict[str, Any]:
    return {
        "group_id": group_id,
        "label": label,
        "event_count": 0,
        "created_ms": now_ms,
        "last_update_ms": now_ms,
        "summary": "",
    }


# ---------------------------------------------------------------------------
# SemGroupbyFunction
# ---------------------------------------------------------------------------

class SemGroupbyFunction(KeyedProcessFunction):
    """Keyed semantic grouping state machine.

    Usage::

        keyed = ds.key_by(simple_key_selector)
        grouped = keyed.process(SemGroupbyFunction(SemGroupbyConfig(...)))
        # Wire async bridge for ambiguous assignments:
        merged = build_async_bridge(grouped, classifier_fn, merge_fn, ...)
    """

    def __init__(self, config: Optional[SemGroupbyConfig] = None) -> None:
        self._config = config or SemGroupbyConfig()
        self._group_profiles: Optional[MapState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._config.ttl_seconds
        self._group_profiles = runtime_context.get_map_state(
            sem_groupby_profiles_descriptor(ttl)
        )
        # Reuse a generic ValueState for counters/eviction bookkeeping
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import ValueStateDescriptor
        desc = ValueStateDescriptor("sem_groupby_meta", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(build_ttl_config(ttl))
        self._meta = runtime_context.get_state(desc)
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_groupby",
        )
        logger.info(
            "SemGroupbyFunction opened (max_groups=%d, threshold=%.2f)",
            self._config.max_groups_per_key,
            self._config.confidence_threshold,
        )

    # -- core ----------------------------------------------------------------

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """Process one incoming event or async merge-back result.

        Yields assignment dicts on main output.  Yields side-output
        ``AsyncWorkItem`` dicts for ambiguous assignments.
        """
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        # Detect async merge-back result
        if isinstance(value, dict) and value.get("task_type") == "classify":
            yield from self._handle_async_result(value, now_ms)
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
        # Parse as SemanticEvent
        if isinstance(value, dict):
            event = SemanticEvent.from_dict(value)
            event_dict = value
        else:
            event = SemanticEvent(
                key=str(ctx.get_current_key()), payload=str(value), seq_id=0,
            )
            event_dict = event.to_dict()

        # Ensure meta exists
        meta = self._meta.value() or {"total_assigned": 0, "key": event.key}

        # Register eviction timer on first event
        if meta.get("total_assigned", 0) == 0 and self._config.evict_interval_ms > 0:
            register_timer(
                ctx.timer_service(), meta, TimerCategory.EVICT,
                now_ms + self._config.evict_interval_ms,
            )

        # Local assignment: find best matching group
        best_group_id, confidence = self._local_assign(event)

        if confidence >= self._config.confidence_threshold and best_group_id:
            # High confidence → direct assignment
            self._update_group(best_group_id, event, now_ms)
            meta["total_assigned"] = meta.get("total_assigned", 0) + 1
            self._meta.update(meta)
            yield {
                "key": event.key, "group_id": best_group_id,
                "confidence": confidence, "source": "local",
                "event_seq_id": event.seq_id,
                "payload": event.payload,
                "event_time_ms": event.event_time_ms,
                "metadata": dict(event.metadata),
                "boundary_flags": dict(event.boundary_flags),
            }
        elif confidence >= self._config.new_group_creation_threshold and best_group_id:
            # Medium confidence → assign but also emit for async verification
            self._update_group(best_group_id, event, now_ms)
            meta["total_assigned"] = meta.get("total_assigned", 0) + 1
            self._meta.update(meta)
            yield {
                "key": event.key, "group_id": best_group_id,
                "confidence": confidence, "source": "tentative",
                "event_seq_id": event.seq_id,
                "payload": event.payload,
                "event_time_ms": event.event_time_ms,
                "metadata": dict(event.metadata),
                "boundary_flags": dict(event.boundary_flags),
            }
            # Emit side-output async classification request
            work = AsyncWorkItem(
                key=event.key, task_type="classify",
                payload={"event": event_dict, "tentative_group": best_group_id},
            )
            if self._metrics:
                self._metrics.record_async_emit()
            yield ASYNC_WORK_TAG, work.to_dict()
        else:
            # Low confidence → create new group or emit async
            new_group_id = self._maybe_create_group(event, now_ms)
            if new_group_id:
                meta["total_assigned"] = meta.get("total_assigned", 0) + 1
                self._meta.update(meta)
                yield {
                    "key": event.key, "group_id": new_group_id,
                    "confidence": 0.0, "source": "new_group",
                    "event_seq_id": event.seq_id,
                    "payload": event.payload,
                    "event_time_ms": event.event_time_ms,
                    "metadata": dict(event.metadata),
                    "boundary_flags": dict(event.boundary_flags),
                }
            else:
                # At group limit → emit async for best-effort classification
                self._meta.update(meta)
                work = AsyncWorkItem(
                    key=event.key, task_type="classify",
                    payload={"event": event_dict, "tentative_group": None},
                )
                if self._metrics:
                    self._metrics.record_async_emit()
                yield ASYNC_WORK_TAG, work.to_dict()

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        """Timer-driven stale group eviction."""
        meta = self._meta.value()
        if meta is None:
            return
        if self._metrics:
            self._metrics.record_timer_fire()

        category = resolve_timer_category(meta, timestamp)
        if category != TimerCategory.EVICT:
            return

        clear_timer_registration(meta, TimerCategory.EVICT)
        evicted = self._evict_stale_groups(meta)
        if evicted > 0:
            if self._metrics:
                self._metrics.record_eviction(evicted)
            logger.info("Evicted %d stale groups for key=%s", evicted, meta.get("key", "?"))

        # Re-register eviction timer
        now_ms = int(time.time() * 1000)
        register_timer(
            ctx.timer_service(), meta, TimerCategory.EVICT,
            now_ms + self._config.evict_interval_ms,
        )
        self._meta.update(meta)

    # -- internals -----------------------------------------------------------

    def _local_assign(self, event: SemanticEvent) -> tuple:
        """Find best matching group by simple keyword overlap.

        Returns (group_id, confidence) or (None, 0.0).
        """
        best_id, best_score = None, 0.0
        payload_words = set(event.payload.lower().split())

        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is None:
                continue
            label_words = set(profile.get("label", "").lower().split())
            if not label_words:
                continue
            overlap = len(payload_words & label_words)
            score = overlap / max(len(label_words), 1)
            if score > best_score:
                best_score = score
                best_id = group_id

        return best_id, best_score

    def _update_group(
        self, group_id: str, event: SemanticEvent, now_ms: int
    ) -> None:
        """Increment group counters and update timestamp."""
        profile = self._group_profiles.get(group_id)
        if profile is None:
            return
        profile["event_count"] = profile.get("event_count", 0) + 1
        profile["last_update_ms"] = now_ms
        self._group_profiles.put(group_id, profile)

    def _maybe_create_group(
        self, event: SemanticEvent, now_ms: int
    ) -> Optional[str]:
        """Create a new group if under the limit. Returns group_id or None.

        Overflow behaviour depends on ``overflow_policy``:
        - DROP_OLDEST: evict the least-recently-updated group to make room.
        - DROP_NEWEST: refuse to create the group (return None).
        - DEGRADE_TAG: create the group but tag it as ``_degraded``.
        """
        count = sum(1 for _ in self._group_profiles.keys())
        if count >= self._config.max_groups_per_key:
            policy = self._config.overflow_policy
            if policy == OverflowPolicy.DROP_OLDEST:
                # Make room by evicting one oldest group
                self._evict_n_oldest(1)
            elif policy == OverflowPolicy.DROP_NEWEST:
                return None
            # DEGRADE_TAG falls through — we create but mark degraded

        import uuid
        group_id = uuid.uuid4().hex[:8]
        label = " ".join(event.payload.split()[:5])
        profile = _new_group_profile(group_id, label, now_ms)
        profile["event_count"] = 1
        if count >= self._config.max_groups_per_key:
            profile["_degraded"] = True
        self._group_profiles.put(group_id, profile)
        return group_id

    def _handle_async_result(
        self, result_dict: Dict[str, Any], now_ms: int
    ):
        """Merge an async classification result back into state."""
        result = AsyncResult.from_dict(result_dict) if "success" in result_dict else None
        if result is None or not result_dict.get("success", False):
            return
        group_id = result_dict.get("result", {}).get("group_id")
        if group_id and self._group_profiles.contains(group_id):
            profile = self._group_profiles.get(group_id)
            profile["last_update_ms"] = now_ms
            # Update label if provided
            new_label = result_dict.get("result", {}).get("label")
            if new_label:
                profile["label"] = new_label
            self._group_profiles.put(group_id, profile)
        yield {
            "key": result_dict.get("key", ""),
            "group_id": group_id,
            "source": "async_classify",
            "request_id": result_dict.get("request_id", ""),
        }

    def _evict_n_oldest(self, n: int) -> int:
        """Evict the *n* least-recently-updated groups. Returns count evicted."""
        groups = []
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile:
                groups.append((group_id, profile.get("last_update_ms", 0)))
        groups.sort(key=lambda x: x[1])  # oldest first
        evicted = 0
        for i in range(min(n, len(groups))):
            self._group_profiles.remove(groups[i][0])
            evicted += 1
        return evicted

    def _evict_stale_groups(self, meta: Dict[str, Any]) -> int:
        """Remove oldest groups if exceeding limit. Returns count evicted."""
        count = sum(1 for _ in self._group_profiles.keys())
        if count <= self._config.max_groups_per_key:
            return 0
        return self._evict_n_oldest(count - self._config.max_groups_per_key)
