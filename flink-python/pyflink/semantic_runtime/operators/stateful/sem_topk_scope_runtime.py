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

"""Operator-owned scope runtime for ``sem_topk``.

This module isolates the scope-boundary state machine and bounded-pool snapshot
emitter used by contextual ``sem_topk`` execution. The pure top-k kernel stays
in ``sem_topk_continuous.py``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapState, ValueState, ValueStateDescriptor

from pyflink.semantic_runtime.sem_spec import TopKQuerySpec
from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    build_ttl_config,
    sem_topk_candidates_descriptor,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    clear_timer_registration,
    encode_timer_key,
    register_timer,
    resolve_timer_category,
)


@dataclass
class _TopKScopeDecision:
    event_time_ms: int
    scope_bucket_id: Optional[int] = None
    sliding_cutoff_ms: Optional[int] = None
    pre_reset_reason: str = ""
    post_reset_reason: str = ""


class _TopKScopeRuntime:
    """Pure scope-boundary logic for operator-owned top-k kernels."""

    def __init__(self, query_spec: TopKQuerySpec) -> None:
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

    def plan(self, value: Dict[str, Any], meta: Dict[str, Any], now_ms: int) -> _TopKScopeDecision:
        event_time_ms = self.resolve_event_time_ms(value, now_ms)
        decision = _TopKScopeDecision(
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
        elif kind == "sliding":
            if self._scope.window_size_ms and self._scope.window_size_ms > 0:
                decision.sliding_cutoff_ms = event_time_ms - self._scope.window_size_ms
        elif kind == "semantic":
            if self._has_semantic_boundary(value):
                decision.post_reset_reason = "semantic_boundary"

        return decision


class SemTopKScopeSnapshotFunction(KeyedProcessFunction):
    """Operator-owned bounded-pool snapshot emitter for contextual top-k."""

    def __init__(
        self,
        config=None,
        query_spec: Optional[TopKQuerySpec] = None,
    ) -> None:
        if config is None:
            from pyflink.semantic_runtime.operators.stateful.sem_topk import SemTopKConfig

            config = SemTopKConfig()
        self._config = config
        self._query_spec = query_spec or TopKQuerySpec()
        self._scope_runtime = _TopKScopeRuntime(self._query_spec)
        self._candidates: Optional[MapState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None

    @property
    def _trigger_mode(self) -> str:
        return self._query_spec.trigger_policy.mode

    @property
    def _resolved_recompute_interval_ms(self) -> int:
        if self._trigger_mode == "periodic":
            if self._query_spec.trigger_policy.interval_ms is not None:
                return max(0, int(self._query_spec.trigger_policy.interval_ms))
        return self._config.recompute_interval_ms

    @property
    def _resolved_idle_ms(self) -> int:
        if self._trigger_mode == "idle_flush":
            if self._query_spec.trigger_policy.idle_ms is not None:
                return max(0, int(self._query_spec.trigger_policy.idle_ms))
        return 0

    @property
    def _resolved_count_threshold(self) -> int:
        if self._trigger_mode == "count_threshold":
            if self._query_spec.trigger_policy.count_threshold is not None:
                return max(0, int(self._query_spec.trigger_policy.count_threshold))
        return 0

    @property
    def _resolved_max_candidates(self) -> int:
        sp = self._query_spec.scope_policy
        if sp.max_candidates is not None:
            return sp.max_candidates
        return self._config.max_candidates

    @property
    def _resolved_ttl_seconds(self) -> int:
        sp = self._query_spec.scope_policy
        if sp.ttl_seconds is not None:
            return sp.ttl_seconds
        return self._config.ttl_seconds

    @property
    def _supports_operator_scope_close(self) -> bool:
        return self._query_spec.scope_policy.window_kind in {"session", "tumbling", "semantic"}

    def open(self, runtime_context: RuntimeContext) -> None:
        if self._trigger_mode not in {
            "on_event",
            "periodic",
            "idle_flush",
            "count_threshold",
            "on_scope_close",
        }:
            raise ValueError(
                "SemTopKScopeSnapshotFunction supports only trigger_policy.mode in "
                "{'on_event', 'periodic', 'idle_flush', 'count_threshold', 'on_scope_close'}"
            )
        if self._trigger_mode == "on_scope_close" and not self._supports_operator_scope_close:
            raise ValueError(
                "SemTopKScopeSnapshotFunction operator-owned on_scope_close requires "
                "scope_policy.window_kind in {'session', 'tumbling', 'semantic'}"
            )

        ttl = self._resolved_ttl_seconds
        self._candidates = runtime_context.get_map_state(
            sem_topk_candidates_descriptor(ttl)
        )
        desc = ValueStateDescriptor("sem_topk_scope_snapshot_meta", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(build_ttl_config(ttl))
        self._meta = runtime_context.get_state(desc)
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_topk_scope_snapshot",
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()
        if not isinstance(value, dict):
            return

        meta = self._meta.value() or {
            "key": str(ctx.get_current_key()),
            "update_count": 0,
            "accepted_count": 0,
            "scope_epoch": 0,
            "scope_last_time_ms": 0,
            "scope_bucket_id": None,
            "last_ranking_text": "",
            "last_query_seq_id": 0,
            "last_source": "",
            "last_error": "",
        }

        decision = self._scope_runtime.plan(value, meta, now_ms)

        if decision.pre_reset_reason:
            if self._query_spec.trigger_policy.emit_final_on_scope_close:
                yield from self._emit_pool_snapshot(meta, now_ms, scope_close_reason=decision.pre_reset_reason)
            self._reset_scope_state(meta, reason=decision.pre_reset_reason)

        meta["update_count"] += 1
        meta["last_ranking_text"] = value.get("query", meta.get("last_ranking_text", ""))
        meta["last_query_seq_id"] = int(
            value.get("query_seq_id", meta.get("last_query_seq_id", 0))
        )
        meta["last_source"] = value.get("source", meta.get("last_source", ""))
        meta["last_error"] = value.get("error", "")
        meta["scope_last_time_ms"] = decision.event_time_ms
        if decision.scope_bucket_id is not None:
            meta["scope_bucket_id"] = decision.scope_bucket_id

        if not self._upsert_candidate(value, now_ms, decision.event_time_ms):
            self._meta.update(meta)
            return

        meta["accepted_count"] = int(meta.get("accepted_count", 0) or 0) + 1

        if decision.sliding_cutoff_ms is not None:
            self._evict_scope_stale_candidates(decision.sliding_cutoff_ms)
        self._enforce_candidate_limit()

        if self._trigger_mode == "periodic":
            self._ensure_periodic_timer(ctx, meta, now_ms)
            if decision.post_reset_reason:
                if self._query_spec.trigger_policy.emit_final_on_scope_close:
                    yield from self._emit_pool_snapshot(meta, now_ms, scope_close_reason=decision.post_reset_reason)
                self._reset_scope_state(meta, reason=decision.post_reset_reason)
            self._meta.update(meta)
            return

        if self._trigger_mode == "idle_flush":
            if decision.post_reset_reason:
                if self._query_spec.trigger_policy.emit_final_on_scope_close:
                    yield from self._emit_pool_snapshot(meta, now_ms, scope_close_reason=decision.post_reset_reason)
                self._reset_scope_state(meta, reason=decision.post_reset_reason)
            else:
                self._register_idle_flush_timer(ctx, meta, now_ms)
            self._meta.update(meta)
            return

        if self._trigger_mode == "count_threshold":
            if decision.post_reset_reason:
                if self._query_spec.trigger_policy.emit_final_on_scope_close:
                    yield from self._emit_pool_snapshot(meta, now_ms, scope_close_reason=decision.post_reset_reason)
                self._reset_scope_state(meta, reason=decision.post_reset_reason)
                self._meta.update(meta)
                return
            threshold = self._resolved_count_threshold
            if threshold > 0 and (meta["accepted_count"] % threshold) == 0:
                yield from self._emit_pool_snapshot(meta, now_ms)
            self._meta.update(meta)
            return

        if self._trigger_mode == "on_scope_close":
            if decision.post_reset_reason:
                if self._query_spec.trigger_policy.emit_final_on_scope_close:
                    yield from self._emit_pool_snapshot(meta, now_ms, scope_close_reason=decision.post_reset_reason)
                self._reset_scope_state(meta, reason=decision.post_reset_reason)
            else:
                self._register_scope_close_timer(ctx, meta, decision, now_ms)
            self._meta.update(meta)
            return

        yield from self._emit_pool_snapshot(meta, now_ms)
        if decision.post_reset_reason:
            self._reset_scope_state(meta, reason=decision.post_reset_reason)
            self._meta.update(meta)

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):
        meta = self._meta.value()
        if meta is None:
            return
        if self._metrics:
            self._metrics.record_timer_fire()

        category = resolve_timer_category(meta, timestamp)
        if category == TimerCategory.FLUSH:
            clear_timer_registration(meta, TimerCategory.FLUSH)
            now_ms = int(time.time() * 1000)
            if self._trigger_mode == "on_scope_close":
                reason = str(meta.pop("pending_scope_close_reason", "") or "scope_close")
                if self._query_spec.trigger_policy.emit_final_on_scope_close:
                    yield from self._emit_pool_snapshot(meta, now_ms, scope_close_reason=reason)
                self._reset_scope_state(meta, reason=reason)
            else:
                yield from self._emit_pool_snapshot(meta, now_ms)
            self._meta.update(meta)
            return

        if category != TimerCategory.RECOMPUTE:
            return

        clear_timer_registration(meta, TimerCategory.RECOMPUTE)
        now_ms = int(time.time() * 1000)
        yield from self._emit_pool_snapshot(meta, now_ms)

        recompute_interval_ms = self._resolved_recompute_interval_ms
        if recompute_interval_ms > 0:
            register_timer(
                ctx.timer_service(), meta, TimerCategory.RECOMPUTE,
                now_ms + recompute_interval_ms,
            )
        self._meta.update(meta)

    def _upsert_candidate(self, value: Dict[str, Any], now_ms: int, scope_time_ms: Optional[int] = None) -> bool:
        cid = value.get("candidate_id", "")
        if not cid:
            return False
        out = dict(value)
        out["_updated_ms"] = now_ms
        out["_scope_time_ms"] = int(scope_time_ms if scope_time_ms is not None else now_ms)
        self._candidates.put(cid, out)
        return True

    def _emit_pool_snapshot(
        self,
        meta: Dict[str, Any],
        now_ms: int,
        *,
        scope_close_reason: str = "",
    ):
        candidates: List[Dict[str, Any]] = []
        for cid in self._candidates.keys():
            record = self._candidates.get(cid)
            if record is not None:
                candidates.append(record)
        if not candidates:
            return
        candidates.sort(
            key=lambda rec: int(rec.get("_scope_time_ms", rec.get("_updated_ms", 0)) or 0)
        )
        out = {
            "key": meta.get("key", ""),
            "query": meta.get("last_ranking_text", ""),
            "query_seq_id": meta.get("last_query_seq_id", 0),
            "candidates": candidates,
            "candidate_count": len(candidates),
            "source": meta.get("last_source", ""),
            "error": str(meta.get("last_error", "")),
            "timestamp_ms": now_ms,
            "scope_epoch": meta.get("scope_epoch", 0),
            "scope_id": str(meta.get("scope_epoch", 0)),
            "emission_policy": "scope_close_final" if scope_close_reason else "snapshot",
        }
        if scope_close_reason:
            out["scope_close_reason"] = scope_close_reason
        yield out

    def _reset_scope_state(self, meta: Dict[str, Any], *, reason: str) -> None:
        if hasattr(self._candidates, "clear"):
            self._candidates.clear()
        else:
            for cid in list(self._candidates.keys()):
                self._candidates.remove(cid)
        clear_timer_registration(meta, TimerCategory.RECOMPUTE)
        clear_timer_registration(meta, TimerCategory.FLUSH)
        meta.pop("pending_scope_close_reason", None)
        meta["accepted_count"] = 0
        meta["scope_epoch"] = int(meta.get("scope_epoch", 0) or 0) + 1
        meta["scope_last_time_ms"] = 0
        meta["scope_bucket_id"] = None
        meta["last_scope_reset_reason"] = reason

    def _evict_scope_stale_candidates(self, cutoff_ms: int) -> int:
        evicted = 0
        for cid in list(self._candidates.keys()):
            record = self._candidates.get(cid)
            if record is None:
                continue
            scope_time_ms = int(record.get("_scope_time_ms", record.get("_updated_ms", 0)) or 0)
            if scope_time_ms < cutoff_ms:
                self._candidates.remove(cid)
                evicted += 1
        return evicted

    def _ensure_periodic_timer(
        self,
        ctx: "KeyedProcessFunction.Context",
        meta: Dict[str, Any],
        now_ms: int,
    ) -> None:
        recompute_interval_ms = self._resolved_recompute_interval_ms
        if recompute_interval_ms <= 0:
            return
        tkey = encode_timer_key(TimerCategory.RECOMPUTE)
        if meta.get(tkey):
            return
        register_timer(
            ctx.timer_service(), meta, TimerCategory.RECOMPUTE,
            now_ms + recompute_interval_ms,
        )

    def _register_idle_flush_timer(
        self,
        ctx: "KeyedProcessFunction.Context",
        meta: Dict[str, Any],
        now_ms: int,
    ) -> None:
        idle_ms = self._resolved_idle_ms
        if idle_ms <= 0:
            return
        register_timer(
            ctx.timer_service(), meta, TimerCategory.FLUSH,
            now_ms + idle_ms,
        )

    def _register_scope_close_timer(
        self,
        ctx: "KeyedProcessFunction.Context",
        meta: Dict[str, Any],
        decision: _TopKScopeDecision,
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

    def _enforce_candidate_limit(self) -> int:
        """Enforce max_candidates limit per overflow_policy. Returns evicted count."""
        max_cand = self._resolved_max_candidates
        entries = []
        for cid in self._candidates.keys():
            record = self._candidates.get(cid)
            if record:
                entries.append((cid, record.get("_updated_ms", 0), record.get(self._config.score_field, 0.0)))

        if len(entries) <= max_cand:
            return 0

        policy = self._config.overflow_policy
        to_evict = len(entries) - max_cand
        if policy == OverflowPolicy.DROP_NEWEST:
            entries.sort(key=lambda x: x[1], reverse=True)
        else:
            entries.sort(key=lambda x: x[1])

        for i in range(to_evict):
            self._candidates.remove(entries[i][0])
        return to_evict
