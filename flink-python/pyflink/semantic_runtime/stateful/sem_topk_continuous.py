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
sem_topk — continuous keyed semantic top-k with delta emissions.

Pure state-machine kernel: maintains a per-key candidate buffer and a
current top-k frontier.  Recomputes top-k on new evidence or timer
trigger.  Emits only when the frontier changes (delta policy) or on
every update (snapshot policy).

**This operator only consumes already-scored candidates.**  Scoring
orchestration (LLM, embedding, external) is handled upstream by the
pipeline builder (``build_sem_topk_pipeline``).

State model:
  - ``MapState[candidate_id -> candidate_record]`` for the candidate pool.
    Each record carries versioning metadata:
    ``score_version``, ``query_version``, ``score_backend``, ``_updated_ms``.
  - ``ValueState[snapshot]`` for the current top-k frontier.

Guardrails:
  - ``max_candidates``: hard cap on candidate buffer size.
  - ``k`` (via :class:`TopKQuerySpec`): number of top items to maintain.
  - TTL via ``StateTtlConfig``.
  - Overflow policy on candidate buffer.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapState, ValueState

from pyflink.semantic_runtime.stateful.state_descriptors import (
    OverflowPolicy,
    sem_topk_candidates_descriptor,
    sem_topk_snapshot_descriptor,
    build_ttl_config,
)
from pyflink.semantic_runtime.stateful.event_model import SemanticEvent
from pyflink.semantic_runtime.stateful.timer_policy import (
    TimerCategory,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.stateful.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.semantic_spec import TopKQuerySpec, TopKScopePolicy

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class SemTopKConfig:
    """Kernel-level configuration for the continuous top-k operator.

    This holds execution-level parameters for the state machine.
    Query-level parameters (``k``, ``query_version``, ``ranking_method``,
    ``scope_policy``) live in :class:`TopKQuerySpec`.

    Parameters
    ----------
    max_candidates : int
        Hard cap on the candidate pool size.
    recompute_interval_ms : int
        Timer-driven recompute interval in milliseconds.
    ttl_seconds : int
        Time-to-live for Flink state entries.
    overflow_policy : OverflowPolicy
        What to do when the candidate pool exceeds ``max_candidates``.
    emission_policy : str
        ``"delta"`` (emit only on change) or ``"snapshot"`` (emit every update).
    score_field : str
        Name of the score field in candidate records.
    """
    max_candidates: int = 100
    recompute_interval_ms: int = 10_000  # timer-driven recompute
    ttl_seconds: int = 3600
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST
    emission_policy: str = "delta"       # "delta" | "snapshot"
    score_field: str = "score"           # field name in candidate record


# ---------------------------------------------------------------------------
# SemTopKFunction
# ---------------------------------------------------------------------------

class SemTopKFunction(KeyedProcessFunction):
    """Keyed continuous top-k state machine (pure kernel).

    Consumes **already-scored** candidate records and maintains the top-k
    frontier per key.  Scoring orchestration (LLM / embedding / external)
    is the responsibility of the upstream pipeline builder, not this operator.

    Input
    -----
    Candidate dicts with at least ``candidate_id`` and a score field
    (default ``"score"``).  Each candidate may also carry versioning
    metadata (``score_version``, ``query_version``, ``score_backend``).

    Output
    ------
    Top-k snapshot dicts emitted on change (delta) or every update
    (snapshot policy).
    """

    def __init__(
        self,
        config: Optional[SemTopKConfig] = None,
        query_spec: Optional[TopKQuerySpec] = None,
    ) -> None:
        self._config = config or SemTopKConfig()
        self._query_spec = query_spec or TopKQuerySpec()
        self._candidates: Optional[MapState] = None
        self._snapshot: Optional[ValueState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None

    # -- resolved config (scope_policy > kernel config) ----------------------

    @property
    def _resolved_max_candidates(self) -> int:
        """``TopKScopePolicy.max_candidates`` wins; fall back to kernel config."""
        sp = self._query_spec.scope_policy
        if sp.max_candidates is not None:
            return sp.max_candidates
        return self._config.max_candidates

    @property
    def _resolved_ttl_seconds(self) -> int:
        """``TopKScopePolicy.ttl_seconds`` wins; fall back to kernel config."""
        sp = self._query_spec.scope_policy
        if sp.ttl_seconds is not None:
            return sp.ttl_seconds
        return self._config.ttl_seconds

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._resolved_ttl_seconds
        self._candidates = runtime_context.get_map_state(
            sem_topk_candidates_descriptor(ttl)
        )
        self._snapshot = runtime_context.get_state(
            sem_topk_snapshot_descriptor(ttl)
        )
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import ValueStateDescriptor
        desc = ValueStateDescriptor("sem_topk_meta", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(build_ttl_config(ttl))
        self._meta = runtime_context.get_state(desc)
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_topk",
        )
        logger.info(
            "SemTopKFunction opened (k=%d, max_candidates=%d, ttl=%d)",
            self._query_spec.k, self._resolved_max_candidates,
            self._resolved_ttl_seconds,
        )

    # -- core ----------------------------------------------------------------

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        if not isinstance(value, dict):
            return

        meta = self._meta.value() or {
            "key": str(ctx.get_current_key()),
            "update_count": 0,
            "last_query": "",
            "last_query_seq_id": 0,
            "last_source": "",
            "last_degraded": False,
            "last_error": "",
        }

        # Register recompute timer on first element
        if meta["update_count"] == 0 and self._config.recompute_interval_ms > 0:
            register_timer(
                ctx.timer_service(), meta, TimerCategory.RECOMPUTE,
                now_ms + self._config.recompute_interval_ms,
            )

        meta["update_count"] += 1

        # Propagate query + envelope metadata from the flat candidate into
        # meta so downstream snapshot emissions retain the query context and
        # degraded/source markers from upstream retrieval/scoring stages.
        meta["last_query"] = value.get("query", meta.get("last_query", ""))
        meta["last_query_seq_id"] = int(
            value.get("query_seq_id", meta.get("last_query_seq_id", 0))
        )
        meta["last_source"] = value.get("source", meta.get("last_source", ""))
        meta["last_degraded"] = value.get("degraded", False)
        meta["last_error"] = value.get("error", "")

        # Pure kernel: only accepts flat scored candidate dicts.
        # Retrieval envelope expansion ({"candidates": [...]}) must be done
        # upstream by _RetrievalEnvelopeExpander or equivalent adapter.
        if not self._upsert_candidate(value, now_ms):
            return

        # Enforce candidate buffer limit
        self._enforce_candidate_limit()

        # Recompute top-k and possibly emit
        yield from self._recompute_and_emit(meta, now_ms)

    # -- candidate management ------------------------------------------------

    def _upsert_candidate(self, value: Dict[str, Any], now_ms: int) -> bool:
        """Insert or update a single scored candidate in the MapState.

        Attaches versioning metadata alongside the candidate record:
        ``_updated_ms``, ``_score_version``, ``_query_version``,
        ``_score_backend``.

        Returns ``True`` when the candidate is accepted into the kernel and
        ``False`` when it is rejected (missing ``candidate_id`` or score).
        """
        cid = value.get("candidate_id", "")
        if not cid:
            return False
        if self._config.score_field not in value or value[self._config.score_field] is None:
            return False
        # Attach versioning metadata
        value["_updated_ms"] = now_ms
        value.setdefault("_score_version", 1)
        value.setdefault("_query_version", self._query_spec.query_version)
        value.setdefault("_score_backend", self._query_spec.semantic.backend)
        self._candidates.put(cid, value)
        return True

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        """Timer-driven top-k recomputation."""
        meta = self._meta.value()
        if meta is None:
            return
        if self._metrics:
            self._metrics.record_timer_fire()

        category = resolve_timer_category(meta, timestamp)
        if category != TimerCategory.RECOMPUTE:
            return

        clear_timer_registration(meta, TimerCategory.RECOMPUTE)
        now_ms = int(time.time() * 1000)
        yield from self._recompute_and_emit(meta, now_ms, force_emit=True)

        # Re-register recompute timer
        register_timer(
            ctx.timer_service(), meta, TimerCategory.RECOMPUTE,
            now_ms + self._config.recompute_interval_ms,
        )
        self._meta.update(meta)

    # -- internals -----------------------------------------------------------

    def _recompute_and_emit(
        self, meta: Dict[str, Any], now_ms: int, force_emit: bool = False,
    ):
        """Recompute top-k from candidate pool and emit if changed.

        Only candidates whose ``_query_version`` matches the current
        ``TopKQuerySpec.query_version`` participate in the frontier.
        Stale candidates remain in state (they may be re-scored later)
        but are excluded from ranking.
        """
        if self._metrics:
            self._metrics.record_recompute()
        score_field = self._config.score_field
        k = self._query_spec.k
        current_qv = self._query_spec.query_version

        # Collect eligible candidates (query_version match + has score)
        scored = []
        stale_count = 0
        for cid in self._candidates.keys():
            record = self._candidates.get(cid)
            if record is None:
                continue
            # Query-version staleness filter: skip candidates scored
            # under an older query version.
            record_qv = record.get("_query_version")
            if record_qv is not None and record_qv != current_qv:
                stale_count += 1
                continue
            score = record.get(score_field, 0.0)
            scored.append((cid, score, record))

        # Sort descending by score
        scored.sort(key=lambda x: x[1], reverse=True)
        new_topk_ids = [cid for cid, _, _ in scored[:k]]

        # Check if changed vs previous snapshot
        prev_snapshot = self._snapshot.value()
        prev_ids = prev_snapshot.get("top_ids", []) if prev_snapshot else []

        changed = new_topk_ids != prev_ids

        # Build new snapshot
        new_snapshot = {
            "top_ids": new_topk_ids,
            "top_records": [rec for _, _, rec in scored[:k]],
            "total_candidates": len(scored),
            "stale_candidates": stale_count,
            "version": meta.get("update_count", 0),
            "timestamp_ms": now_ms,
        }
        self._snapshot.update(new_snapshot)
        self._meta.update(meta)

        # Emission policy
        should_emit = force_emit or changed or self._config.emission_policy == "snapshot"
        if should_emit:
            yield {
                "key": meta.get("key", ""),
                "topk": new_snapshot["top_records"],
                "top_ids": new_topk_ids,
                "query": meta.get("last_query", ""),
                "query_seq_id": meta.get("last_query_seq_id", 0),
                "source": meta.get("last_source", ""),
                "total_candidates": len(scored),
                "stale_candidates": stale_count,
                "version": new_snapshot["version"],
                "changed": changed,
                "emission_policy": self._config.emission_policy,
                "degraded": bool(meta.get("last_degraded", False)),
                "error": str(meta.get("last_error", "")),
                "timestamp_ms": now_ms,
            }

    def _enforce_candidate_limit(self) -> int:
        """Enforce max_candidates limit per overflow_policy. Returns evicted count.

        Uses ``_resolved_max_candidates`` (scope_policy > kernel config).
        """
        max_cand = self._resolved_max_candidates
        entries = []
        for cid in self._candidates.keys():
            record = self._candidates.get(cid)
            if record:
                entries.append((cid, record.get("_updated_ms", 0),
                                record.get(self._config.score_field, 0.0)))

        if len(entries) <= max_cand:
            return 0

        policy = self._config.overflow_policy
        if policy == OverflowPolicy.DEGRADE_TAG:
            return 0

        to_evict = len(entries) - max_cand
        if policy == OverflowPolicy.DROP_NEWEST:
            entries.sort(key=lambda x: x[1], reverse=True)  # newest first
        else:
            # DROP_OLDEST: evict by oldest update time
            entries.sort(key=lambda x: x[1])

        for i in range(to_evict):
            self._candidates.remove(entries[i][0])
        return to_evict
