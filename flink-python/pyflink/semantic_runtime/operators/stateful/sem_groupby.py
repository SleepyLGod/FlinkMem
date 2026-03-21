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

"""Stateful semantic grouping over keyed state.

``sem_groupby`` maintains a set of semantic groups for one keyed scope.
For each incoming event, the operator decides one of two outcomes:

1. assign the event to one existing group
2. create one new group

The decision backend is an internal execution concern. Local methods such as
keyword overlap or embedding similarity can assign synchronously. Expensive
semantic methods such as LLM-based assignment can emit async work and only
produce the final assignment after merge-back.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, MapState, ValueState

from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    sem_groupby_pending_events_descriptor,
    sem_groupby_profiles_descriptor,
    sem_window_meta_descriptor,
    build_ttl_config,
)
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    is_window_snapshot,
    window_snapshot_to_sem_events,
)
from pyflink.semantic_runtime.runtime.async_bridge import (
    ASYNC_WORK_TAG,
    AsyncWorkItem,
    AsyncResult,
)
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.sem_spec import GroupbyQuerySpec
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder

logger = logging.getLogger(__name__)

_LOCAL_ASSIGNMENT_METHODS = {"rule", "embedding"}
_ASYNC_ASSIGNMENT_METHODS = {"llm"}
_VALID_INTERNAL_ASSIGNMENT_METHODS = _LOCAL_ASSIGNMENT_METHODS | _ASYNC_ASSIGNMENT_METHODS


# ---------------------------------------------------------------------------
# Operator-owned scope runtime
# ---------------------------------------------------------------------------


@dataclass
class _GroupbyScopeDecision:
    event_time_ms: int
    scope_bucket_id: Optional[int] = None
    pre_reset_reason: str = ""
    post_reset_reason: str = ""


class _GroupbyScopeRuntime:
    """Pure scope-boundary logic for operator-owned groupby kernels."""

    def __init__(self, query_spec: GroupbyQuerySpec) -> None:
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

    def plan(
        self,
        value: Dict[str, Any],
        meta: Dict[str, Any],
        now_ms: int,
    ) -> _GroupbyScopeDecision:
        event_time_ms = self.resolve_event_time_ms(value, now_ms)
        decision = _GroupbyScopeDecision(
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
class SemGroupbyConfig:
    """Internal configuration for the semantic groupby operator."""

    max_groups_per_key: int = 50
    assignment_method: str = "rule"
    scope_chunk_size: int = 1
    confidence_threshold: float = 0.7
    ttl_seconds: int = 3600
    evict_interval_ms: int = 60_000
    new_group_creation_threshold: float = 0.3
    refresh_labels_during_maintenance: bool = False
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST

    def __post_init__(self) -> None:
        if self.assignment_method not in _VALID_INTERNAL_ASSIGNMENT_METHODS:
            raise ValueError(
                f"Invalid internal groupby assignment_method={self.assignment_method!r}. "
                f"Must be one of {_VALID_INTERNAL_ASSIGNMENT_METHODS}."
            )
        if self.scope_chunk_size <= 0:
            raise ValueError("scope_chunk_size must be a positive integer.")


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


def resolve_groupby_assignment_method(
    config: SemGroupbyConfig,
    query_spec: Optional[GroupbyQuerySpec] = None,
) -> str:
    """Resolve the internal assignment strategy."""
    _ = query_spec
    return str(config.assignment_method or "rule")


def _group_profile_text(profile: Dict[str, Any]) -> str:
    label = str(profile.get("label", "") or "")
    summary = str(profile.get("summary", "") or "")
    return f"{label}\n{summary}".strip()


def derive_group_label(profile: Dict[str, Any]) -> str:
    """Derive a compact local label from a group profile.

    This is a local relabel helper used by maintenance when label refresh is
    enabled. No separate LLM-driven relabel worker exists in the current
    runtime.
    """
    text = _group_profile_text(profile).strip()
    if not text:
        return str(profile.get("label", "") or "")
    tokens: List[str] = []
    seen = set()
    for token in text.replace("\n", " ").split():
        normalized = token.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        tokens.append(token.strip(".,;:!?"))
        if len(tokens) >= 5:
            break
    return " ".join(tokens).strip() or str(profile.get("label", "") or "")


def _keyword_overlap_score(event_text: str, profile_text: str) -> float:
    payload_words = set(event_text.lower().split())
    profile_words = set(profile_text.lower().split())
    if not payload_words or not profile_words:
        return 0.0
    overlap = len(payload_words & profile_words)
    return overlap / max(len(profile_words), 1)


def score_group_profile(
    event_text: str,
    profile: Dict[str, Any],
    *,
    assignment_method: str,
    encoder: Optional[HashingTextEncoder] = None,
) -> float:
    """Score an event against one group profile using the chosen local method."""
    profile_text = _group_profile_text(profile)
    if not profile_text:
        return 0.0
    if assignment_method == "embedding":
        local_encoder = encoder or HashingTextEncoder()
        return float(local_encoder.similarity(event_text, profile_text))
    return float(_keyword_overlap_score(event_text, profile_text))


def group_profile_similarity(
    left_profile: Dict[str, Any],
    right_profile: Dict[str, Any],
    *,
    assignment_method: str,
    encoder: Optional[HashingTextEncoder] = None,
) -> float:
    """Return a symmetric similarity score between two group profiles."""
    left_text = _group_profile_text(left_profile)
    right_text = _group_profile_text(right_profile)
    if not left_text or not right_text:
        return 0.0
    if assignment_method == "embedding":
        local_encoder = encoder or HashingTextEncoder()
        return float(local_encoder.similarity(left_text, right_text))
    left_to_right = _keyword_overlap_score(left_text, right_text)
    right_to_left = _keyword_overlap_score(right_text, left_text)
    return float((left_to_right + right_to_left) / 2.0)


def resolve_groupby_maintenance_merge_threshold(
    *,
    assignment_method: str,
    assign_threshold: float,
    new_group_threshold: float,
) -> float:
    """Return the local similarity threshold used by maintenance/refinement."""
    if assignment_method == "embedding":
        return max(0.8, float(assign_threshold))
    return max(0.65, float(new_group_threshold))


def merge_similar_group_profiles(
    groups: Dict[str, Dict[str, Any]],
    *,
    assignment_method: str,
    encoder: Optional[HashingTextEncoder],
    assign_threshold: float,
    new_group_threshold: float,
    now_ms: int,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], int]:
    """Greedily merge highly similar groups in a plain in-memory mapping.

    Returns
    -------
    merged_groups : dict
        Updated group mapping after local greedy merges.
    merged_into : dict
        Mapping of removed group_id -> survivor group_id.
    merge_count : int
        Number of merges applied.
    """
    if len(groups) < 2:
        return dict(groups), {}, 0

    threshold = resolve_groupby_maintenance_merge_threshold(
        assignment_method=assignment_method,
        assign_threshold=assign_threshold,
        new_group_threshold=new_group_threshold,
    )
    working = {gid: dict(profile) for gid, profile in groups.items()}
    candidates: List[Tuple[float, str, str]] = []
    items = list(working.items())
    for i in range(len(items)):
        left_id, left_profile = items[i]
        for j in range(i + 1, len(items)):
            right_id, right_profile = items[j]
            score = group_profile_similarity(
                left_profile,
                right_profile,
                assignment_method=assignment_method,
                encoder=encoder,
            )
            if score >= threshold:
                candidates.append((score, left_id, right_id))

    if not candidates:
        return working, {}, 0

    candidates.sort(key=lambda item: item[0], reverse=True)
    merged_into: Dict[str, str] = {}
    merged_ids = set()
    merge_count = 0

    for _score, left_id, right_id in candidates:
        if left_id in merged_ids or right_id in merged_ids:
            continue
        left_profile = working.get(left_id)
        right_profile = working.get(right_id)
        if left_profile is None or right_profile is None:
            continue
        survivor_id, merged_id = choose_group_merge_survivor(
            left_id, left_profile, right_id, right_profile
        )
        apply_group_merge(working, survivor_id, merged_id, now_ms)
        merged_ids.add(merged_id)
        merged_into[merged_id] = survivor_id
        merge_count += 1

    return working, merged_into, merge_count


def relabel_group_profiles(groups: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Return a copy with locally refreshed labels."""
    relabeled: Dict[str, Dict[str, Any]] = {}
    for group_id, profile in groups.items():
        updated = dict(profile)
        updated["label"] = derive_group_label(updated)
        relabeled[group_id] = updated
    return relabeled


def choose_group_merge_survivor(
    left_id: str,
    left_profile: Dict[str, Any],
    right_id: str,
    right_profile: Dict[str, Any],
) -> Tuple[str, str]:
    """Choose which group survives a merge."""
    left_count = int(left_profile.get("event_count", 0))
    right_count = int(right_profile.get("event_count", 0))
    if left_count > right_count:
        return left_id, right_id
    if right_count > left_count:
        return right_id, left_id
    left_created = int(left_profile.get("created_ms", 0))
    right_created = int(right_profile.get("created_ms", 0))
    if left_created <= right_created:
        return left_id, right_id
    return right_id, left_id


def apply_group_merge(
    groups: Dict[str, Dict[str, Any]],
    survivor_id: str,
    merged_id: str,
    now_ms: int,
) -> None:
    """Apply one in-memory group merge."""
    survivor = groups.get(survivor_id)
    merged = groups.get(merged_id)
    if survivor is None or merged is None:
        return

    survivor["event_count"] = int(survivor.get("event_count", 0)) + int(
        merged.get("event_count", 0)
    )
    survivor["created_ms"] = min(
        int(survivor.get("created_ms", now_ms)),
        int(merged.get("created_ms", now_ms)),
    )
    survivor["last_update_ms"] = now_ms
    merged_from = list(survivor.get("_merged_from", []))
    merged_from.append(merged_id)
    survivor["_merged_from"] = merged_from

    merged_text = _group_profile_text(merged)
    if merged_text:
        existing_summary = str(survivor.get("summary", "") or "")
        if merged_text not in existing_summary:
            survivor["summary"] = (
                f"{existing_summary}\n{merged_text}".strip() if existing_summary else merged_text
            )

    groups[survivor_id] = survivor
    groups.pop(merged_id, None)


def resolve_groupby_runtime_params(
    config: SemGroupbyConfig,
    query_spec: Optional[GroupbyQuerySpec] = None,
) -> Tuple[int, int, float, float]:
    """Resolve runtime parameters from config + query spec."""
    ttl_seconds = int(
        query_spec.scope_policy.ttl_seconds
        if query_spec and query_spec.scope_policy.ttl_seconds is not None
        else config.ttl_seconds
    )
    max_groups_per_key = int(
        query_spec.scope_policy.max_groups_per_key
        if query_spec and query_spec.scope_policy.max_groups_per_key is not None
        else config.max_groups_per_key
    )
    return (
        ttl_seconds,
        max_groups_per_key,
        float(config.confidence_threshold),
        float(config.new_group_creation_threshold),
    )


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

    def __init__(
        self,
        config: Optional[SemGroupbyConfig] = None,
        query_spec: Optional[GroupbyQuerySpec] = None,
    ) -> None:
        self._config = config or SemGroupbyConfig()
        self._query_spec = query_spec
        self._group_profiles: Optional[MapState] = None
        self._pending_events: Optional[ListState] = None
        self._pending_events_buffer: List[Dict[str, Any]] = []
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None
        self._resolved_assignment_method = resolve_groupby_assignment_method(
            self._config,
            query_spec,
        )
        self._maintenance_trigger_policy = (
            query_spec.maintenance_trigger_policy if query_spec is not None else None
        )
        self._scope_runtime = (
            _GroupbyScopeRuntime(query_spec)
            if query_spec is not None
            and self._maintenance_trigger_policy is not None
            and self._maintenance_trigger_policy.mode == "on_scope_close"
            else None
        )
        self._encoder = HashingTextEncoder(dim=128)
        (
            self._resolved_ttl_seconds,
            self._resolved_max_groups_per_key,
            self._resolved_assign_threshold,
            self._resolved_new_group_threshold,
        ) = resolve_groupby_runtime_params(self._config, self._query_spec)
        self._resolved_scope_chunk_size = int(self._config.scope_chunk_size)

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._resolved_ttl_seconds
        self._group_profiles = runtime_context.get_map_state(
            sem_groupby_profiles_descriptor(ttl)
        )
        self._pending_events = runtime_context.get_list_state(
            sem_groupby_pending_events_descriptor(ttl)
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
            "SemGroupbyFunction opened (max_groups=%d, reuse_threshold=%.2f, maintenance_merge_threshold=%.2f, assignment_method=%s)",
            self._resolved_max_groups_per_key,
            self._resolved_assign_threshold,
            self._resolved_new_group_threshold,
            self._resolved_assignment_method,
        )

    # -- core ----------------------------------------------------------------

    def process_element(
        self,
        value: Any,
        ctx: "KeyedProcessFunction.Context",
    ) -> Iterable[Any]:
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
            for sub_event_dict in window_snapshot_to_sem_events(value):
                yield from self._process_single_event(sub_event_dict, ctx, now_ms)
            return

        # Single event path
        yield from self._process_single_event(value, ctx, now_ms)

    def _process_single_event(
        self,
        value: Any,
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> Iterable[Any]:
        """Process a single SemEvent-shaped dict."""
        # Parse as SemEvent
        if isinstance(value, dict):
            event = SemEvent.from_dict(value)
            event_dict = value
        else:
            event = SemEvent(
                key=str(ctx.get_current_key()), payload=str(value), seq_id=0,
            )
            event_dict = event.to_dict()

        # Ensure meta exists
        meta = self._meta.value() or {
            "total_assigned": 0,
            "key": event.key,
            "scope_epoch": 0,
            "scope_last_time_ms": 0,
            "scope_bucket_id": None,
        }

        decision = (
            self._scope_runtime.plan(event_dict, meta, now_ms)
            if self._scope_runtime is not None
            else _GroupbyScopeDecision(event_time_ms=event.effective_time_ms)
        )

        if decision.pre_reset_reason:
            yield from self._close_scope(
                meta,
                now_ms=now_ms,
                reason=decision.pre_reset_reason,
                key=event.key,
            )

        # Register eviction timer on first event
        if meta.get("total_assigned", 0) == 0 and self._config.evict_interval_ms > 0:
            register_timer(
                ctx.timer_service(), meta, TimerCategory.EVICT,
                now_ms + self._config.evict_interval_ms,
            )
        if (
            meta.get("total_assigned", 0) == 0
            and self._maintenance_trigger_policy is not None
            and self._maintenance_trigger_policy.mode == "periodic"
        ):
            register_timer(
                ctx.timer_service(),
                meta,
                TimerCategory.RECOMPUTE,
                now_ms + int(self._maintenance_trigger_policy.interval_ms),
            )

        meta["scope_last_time_ms"] = decision.event_time_ms
        if decision.scope_bucket_id is not None:
            meta["scope_bucket_id"] = decision.scope_bucket_id

        if self._resolved_assignment_method in _LOCAL_ASSIGNMENT_METHODS:
            assignment_row = self._assign_locally(event, now_ms)
            meta["total_assigned"] = int(meta.get("total_assigned", 0)) + 1
            self._meta.update(meta)
            yield assignment_row
        elif self._resolved_assignment_method in _ASYNC_ASSIGNMENT_METHODS:
            yield from self._enqueue_async_assignment(
                event_dict=event_dict,
                event=event,
                meta=meta,
            )
        else:
            raise ValueError(
                f"Unsupported internal groupby assignment_method={self._resolved_assignment_method!r}."
            )

        if (
            self._maintenance_trigger_policy is not None
            and self._maintenance_trigger_policy.mode == "on_scope_close"
        ):
            if decision.post_reset_reason:
                yield from self._close_scope(
                    meta,
                    now_ms=now_ms,
                    reason=decision.post_reset_reason,
                    key=event.key,
                )
            else:
                self._register_scope_close_timer(ctx, meta, decision, now_ms)
            self._meta.update(meta)
            return

    def on_timer(
        self,
        timestamp: int,
        ctx: "KeyedProcessFunction.OnTimerContext",
    ) -> List[Any]:
        """Timer-driven stale group eviction."""
        outputs: List[Any] = []
        meta = self._meta.value()
        if meta is None:
            return outputs
        if self._metrics:
            self._metrics.record_timer_fire()

        category = resolve_timer_category(meta, timestamp)
        if category == TimerCategory.FLUSH:
            clear_timer_registration(meta, TimerCategory.FLUSH)
            reason = str(meta.pop("pending_scope_close_reason", "") or "scope_close")
            outputs.extend(
                self._close_scope(
                    meta,
                    now_ms=timestamp,
                    reason=reason,
                    key=str(ctx.get_current_key()),
                )
            )
            return outputs
        if category == TimerCategory.RECOMPUTE:
            clear_timer_registration(meta, TimerCategory.RECOMPUTE)
            self._run_maintenance(meta, timestamp)
            if (
                self._maintenance_trigger_policy is not None
                and self._maintenance_trigger_policy.mode == "periodic"
            ):
                register_timer(
                    ctx.timer_service(),
                    meta,
                    TimerCategory.RECOMPUTE,
                    int(time.time() * 1000) + int(self._maintenance_trigger_policy.interval_ms),
                )
            self._meta.update(meta)
            return outputs
        if category != TimerCategory.EVICT:
            return outputs

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
        return outputs

    # -- internals -----------------------------------------------------------

    def _local_assign(self, event: SemEvent) -> Tuple[Optional[str], float]:
        """Return the best local candidate group and its score."""
        best_id, best_score = None, 0.0

        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is None:
                continue
            score = score_group_profile(
                event.payload,
                profile,
                assignment_method=self._resolved_assignment_method,
                encoder=self._encoder,
            )
            if score > best_score:
                best_score = score
                best_id = group_id

        return best_id, best_score

    def _pending_event_values(self) -> List[Dict[str, Any]]:
        """Return the current pending async chunk as plain event dicts."""
        if self._pending_events is None:
            return list(self._pending_events_buffer)
        return list(self._pending_events.get())

    def _replace_pending_events(self, events: List[Dict[str, Any]]) -> None:
        """Replace the pending async chunk."""
        normalized = list(events)
        self._pending_events_buffer = normalized
        if self._pending_events is not None:
            self._pending_events.update(normalized)

    def _clear_pending_events(self) -> None:
        """Clear the pending async chunk."""
        self._pending_events_buffer = []
        if self._pending_events is not None:
            self._pending_events.clear()

    def _append_pending_event(self, event_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Append one event to the pending async chunk and return the chunk."""
        pending = self._pending_event_values()
        pending.append(dict(event_dict))
        self._replace_pending_events(pending)
        return pending

    def _build_async_assignment_payload(
        self,
        events: List[Dict[str, Any]],
        *,
        scope_epoch: int,
        scope_close_pending: bool,
        scope_close_reason: str = "",
    ) -> Dict[str, Any]:
        """Build one async assignment payload for one event chunk."""
        existing_groups = self._existing_groups_payload()
        if len(events) == 1:
            event = dict(events[0])
            semantic_event = SemEvent.from_dict(event)
            suggested_group_id, suggested_score = self._local_assign(semantic_event)
            return {
                "event": event,
                "existing_groups": existing_groups,
                "scope_chunk_size": self._resolved_scope_chunk_size,
                "scope_epoch": scope_epoch,
                "scope_close_pending": scope_close_pending,
                "scope_close_reason": scope_close_reason,
                "planner_hints": {
                    "suggested_group_id": suggested_group_id,
                    "suggested_score": suggested_score,
                },
            }
        return {
            "events": [dict(item) for item in events],
            "existing_groups": existing_groups,
            "scope_chunk_size": self._resolved_scope_chunk_size,
            "scope_epoch": scope_epoch,
            "scope_close_pending": scope_close_pending,
            "scope_close_reason": scope_close_reason,
        }

    def _emit_async_assignment_work(
        self,
        *,
        key: str,
        events: List[Dict[str, Any]],
        meta: Dict[str, Any],
        scope_close_pending: bool,
        scope_close_reason: str = "",
    ) -> Tuple[Any, Dict[str, Any]]:
        """Build one async assignment work item for the current chunk."""
        work = AsyncWorkItem(
            key=key,
            task_type="classify",
            payload=self._build_async_assignment_payload(
                events,
                scope_epoch=int(meta.get("scope_epoch", 0) or 0),
                scope_close_pending=scope_close_pending,
                scope_close_reason=scope_close_reason,
            ),
        )
        if self._metrics:
            self._metrics.record_async_emit()
        return ASYNC_WORK_TAG, work.to_dict()

    def _enqueue_async_assignment(
        self,
        *,
        event_dict: Dict[str, Any],
        event: SemEvent,
        meta: Dict[str, Any],
    ) -> Iterable[Any]:
        """Append one event to the async chunk and emit work when full."""
        pending = self._append_pending_event(event_dict)
        self._meta.update(meta)
        if len(pending) < self._resolved_scope_chunk_size:
            return
        chunk = list(pending)
        self._clear_pending_events()
        yield self._emit_async_assignment_work(
            key=event.key,
            events=chunk,
            meta=meta,
            scope_close_pending=False,
        )

    def _existing_groups_payload(self) -> List[Dict[str, Any]]:
        """Return a serializable view of current groups for async assignment."""
        return [
            {
                "group_id": group_id,
                "label": profile.get("label", ""),
                "summary": profile.get("summary", ""),
                "event_count": int(profile.get("event_count", 0)),
            }
            for group_id in self._group_profiles.keys()
            for profile in [self._group_profiles.get(group_id)]
            if profile is not None
        ]

    def _assign_locally(self, event: SemEvent, now_ms: int) -> Dict[str, Any]:
        """Assign one event using the configured local method."""
        best_group_id, confidence = self._local_assign(event)
        if best_group_id and confidence >= self._resolved_assign_threshold:
            self._update_group(best_group_id, event, now_ms)
            return self._assignment_row(event, best_group_id, confidence, "local")

        new_group_id = self._create_group_or_raise(event, now_ms)
        return self._assignment_row(event, new_group_id, confidence, "new_group")

    @staticmethod
    def _assignment_row(
        event: SemEvent,
        group_id: str,
        confidence: float,
        source: str,
    ) -> Dict[str, Any]:
        """Build one normalized group assignment row."""
        return {
            "key": event.key,
            "group_id": group_id,
            "confidence": confidence,
            "source": source,
            "event_seq_id": event.seq_id,
            "payload": event.payload,
            "event_time_ms": event.event_time_ms,
            "metadata": dict(event.metadata),
            "boundary_flags": dict(event.boundary_flags),
        }

    def _update_group(
        self, group_id: str, event: SemEvent, now_ms: int
    ) -> None:
        """Increment group counters and update timestamp."""
        profile = self._group_profiles.get(group_id)
        if profile is None:
            return
        profile["event_count"] = profile.get("event_count", 0) + 1
        profile["last_update_ms"] = now_ms
        self._group_profiles.put(group_id, profile)

    def _maybe_create_group(
        self, event: SemEvent, now_ms: int
    ) -> Optional[str]:
        """Create a new group if under the limit. Returns group_id or None.

        Overflow behaviour depends on ``overflow_policy``:
        - DROP_OLDEST: evict the least-recently-updated group to make room.
        - DROP_NEWEST: refuse to create the group (return None).
        """
        count = sum(1 for _ in self._group_profiles.keys())
        if count >= self._resolved_max_groups_per_key:
            policy = self._config.overflow_policy
            if policy == OverflowPolicy.DROP_OLDEST:
                # Make room by evicting one oldest group
                self._evict_n_oldest(1)
            elif policy == OverflowPolicy.DROP_NEWEST:
                return None

        import uuid
        group_id = uuid.uuid4().hex[:8]
        label = " ".join(event.payload.split()[:5])
        profile = _new_group_profile(group_id, label, now_ms)
        profile["event_count"] = 1
        self._group_profiles.put(group_id, profile)
        return group_id

    def _create_group_or_raise(self, event: SemEvent, now_ms: int) -> str:
        """Create a new group or fail when policy forbids it."""
        group_id = self._maybe_create_group(event, now_ms)
        if group_id is None:
            raise RuntimeError(
                "sem_groupby could not create a new group under the current overflow policy."
            )
        return group_id

    def _merge_one_async_assignment(
        self,
        payload: Dict[str, Any],
        *,
        now_ms: int,
        scope_close_pending: bool,
        stale_scope_result: bool,
    ) -> Dict[str, Any]:
        """Merge one async assignment or emit it without state mutation."""
        group_id = str(payload.get("group_id", "") or "")
        if not group_id:
            raise RuntimeError("sem_groupby async classify returned no group_id")

        event = SemEvent(
            key=str(payload.get("key", "") or ""),
            payload=str(payload.get("payload", "") or ""),
            seq_id=int(payload.get("event_seq_id", 0)),
            event_time_ms=payload.get("event_time_ms"),
            metadata=dict(payload.get("metadata", {}) or {}),
            boundary_flags=dict(payload.get("boundary_flags", {}) or {}),
        )
        confidence = float(payload.get("confidence", 0.0))
        label = str(payload.get("label", "") or "")

        meta = self._meta.value() or {}
        meta["total_assigned"] = int(meta.get("total_assigned", 0)) + 1
        self._meta.update(meta)

        if not scope_close_pending and not stale_scope_result:
            if self._group_profiles.contains(group_id):
                self._update_group(group_id, event, now_ms)
                if label:
                    profile = self._group_profiles.get(group_id)
                    if profile is not None:
                        profile["label"] = label
                        self._group_profiles.put(group_id, profile)
            else:
                profile = _new_group_profile(
                    group_id,
                    label or " ".join(event.payload.split()[:5]),
                    now_ms,
                )
                profile["event_count"] = 1
                self._group_profiles.put(group_id, profile)

        return self._assignment_row(event, group_id, confidence, "async_assign")

    def _handle_async_result(
        self, result_dict: Dict[str, Any], now_ms: int
    ) -> Iterable[Dict[str, Any]]:
        """Merge one async assignment result back into state."""
        result = AsyncResult.from_dict(result_dict) if "success" in result_dict else None
        if result is None or not result.success:
            error = result_dict.get("error", "async_classify_failed")
            raise RuntimeError(f"sem_groupby async classify failed: {error}")

        payload = result.result or {}
        scope_close_pending = bool(payload.get("scope_close_pending", False))
        result_scope_epoch = int(payload.get("scope_epoch", 0) or 0)
        current_scope_epoch = int((self._meta.value() or {}).get("scope_epoch", 0) or 0)
        stale_scope_result = result_scope_epoch != current_scope_epoch

        assignments = payload.get("assignments")
        if isinstance(assignments, list):
            for assignment in assignments:
                assignment_payload = dict(assignment)
                assignment_payload.setdefault("key", str(result_dict.get("key", "")))
                yield self._merge_one_async_assignment(
                    assignment_payload,
                    now_ms=now_ms,
                    scope_close_pending=scope_close_pending,
                    stale_scope_result=stale_scope_result,
                )
            return

        single_payload = dict(payload)
        single_payload.setdefault("key", str(result_dict.get("key", "")))
        yield self._merge_one_async_assignment(
            single_payload,
            now_ms=now_ms,
            scope_close_pending=scope_close_pending,
            stale_scope_result=stale_scope_result,
        )

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
        if count <= self._resolved_max_groups_per_key:
            return 0
        return self._evict_n_oldest(count - self._resolved_max_groups_per_key)

    def _run_maintenance(self, meta: Dict[str, Any], now_ms: int) -> None:
        """Run local maintenance for operator-owned grouping.

        Current behaviour is intentionally narrow:
        - perform a local greedy merge of highly similar groups
        - record maintenance heartbeat metadata
        - do not reassign historical events
        """
        merge_count = self._merge_similar_groups(now_ms)
        if self._config.refresh_labels_during_maintenance:
            self._refresh_group_labels()
        meta["last_refine_ms"] = now_ms
        meta["refine_count"] = int(meta.get("refine_count", 0)) + 1
        meta["last_merge_count"] = merge_count
        if self._metrics:
            self._metrics.record_recompute()

    def _close_scope(
        self,
        meta: Dict[str, Any],
        *,
        now_ms: int,
        reason: str,
        key: str,
    ) -> Iterable[Any]:
        """Finalize one scope and optionally flush pending async assignments."""
        self._run_maintenance(meta, now_ms)
        pending = self._pending_event_values()
        if pending:
            output = self._emit_async_assignment_work(
                key=key,
                events=pending,
                meta=meta,
                scope_close_pending=True,
                scope_close_reason=reason,
            )
            self._clear_pending_events()
            self._reset_scope_state(meta, reason=reason)
            self._meta.update(meta)
            yield output
            return
        self._reset_scope_state(meta, reason=reason)
        self._meta.update(meta)

    def _maintenance_merge_threshold(self) -> float:
        return resolve_groupby_maintenance_merge_threshold(
            assignment_method=self._resolved_assignment_method,
            assign_threshold=self._resolved_assign_threshold,
            new_group_threshold=self._resolved_new_group_threshold,
        )

    def _merge_similar_groups(self, now_ms: int) -> int:
        groups: Dict[str, Dict[str, Any]] = {}
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is not None:
                groups[group_id] = dict(profile)
        merged_groups, _merged_into, merge_count = merge_similar_group_profiles(
            groups,
            assignment_method=self._resolved_assignment_method,
            encoder=self._encoder,
            assign_threshold=self._resolved_assign_threshold,
            new_group_threshold=self._resolved_new_group_threshold,
            now_ms=now_ms,
        )
        for group_id in list(self._group_profiles.keys()):
            if group_id not in merged_groups:
                self._group_profiles.remove(group_id)
        for group_id, profile in merged_groups.items():
            self._group_profiles.put(group_id, profile)
        return merge_count

    def _refresh_group_labels(self) -> None:
        groups: Dict[str, Dict[str, Any]] = {}
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is not None:
                groups[group_id] = dict(profile)
        relabeled = relabel_group_profiles(groups)
        for group_id, profile in relabeled.items():
            self._group_profiles.put(group_id, profile)

    def _register_scope_close_timer(
        self,
        ctx,
        meta: Dict[str, Any],
        decision: _GroupbyScopeDecision,
        now_ms: int,
    ) -> None:
        if self._query_spec is None:
            return
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
        for group_id in list(self._group_profiles.keys()):
            self._group_profiles.remove(group_id)
        clear_timer_registration(meta, TimerCategory.FLUSH)
        meta.pop("pending_scope_close_reason", None)
        meta["scope_epoch"] = int(meta.get("scope_epoch", 0) or 0) + 1
        meta["scope_last_time_ms"] = 0
        meta["scope_bucket_id"] = None
        meta["last_scope_reset_reason"] = reason
