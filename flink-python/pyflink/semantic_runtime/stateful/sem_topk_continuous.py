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

Maintains a per-key candidate buffer and a current top-k frontier.
Recomputes top-k on new evidence or timer trigger.  Emits only when
the frontier changes (delta policy) or on every update (snapshot policy).

Scorer backends
~~~~~~~~~~~~~~~
``scorer_backend`` in :class:`SemTopKConfig` selects how candidate scores
are obtained:

- ``"external_score"`` (default): candidates already carry a numeric score
  field.  No async work is needed — the operator reads the score directly.
- ``"llm"``: the operator emits unscored candidates as side-output work
  items via the async bridge.  An LLM-based async worker scores them and
  the merge function feeds scored candidates back.
- ``"embedding"``: same async bridge path as ``llm``, but the async worker
  uses an embedding-based similarity scorer instead.

State model:
  - ``MapState[candidate_id -> candidate_record]`` for the candidate pool.
  - ``ValueState[snapshot]`` for the current top-k frontier.

Guardrails:
  - ``max_candidates``: hard cap on candidate buffer size.
  - ``k``: number of top items to maintain.
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
from pyflink.semantic_runtime.stateful.async_bridge import (
    ASYNC_WORK_TAG,
    AsyncWorkItem,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VALID_SCORER_BACKENDS = {"external_score", "llm", "embedding"}


@dataclass
class SemTopKConfig:
    """Configuration for the continuous top-k operator.

    Parameters
    ----------
    scorer_backend : str
        ``"external_score"`` (default) — use the score field already present
        on each candidate record (no async I/O).
        ``"llm"`` — emit unscored candidates as side-output for LLM scoring
        via the async bridge.
        ``"embedding"`` — same async bridge path but for embedding scoring.
    """
    k: int = 10
    max_candidates: int = 100
    recompute_interval_ms: int = 10_000  # timer-driven recompute
    ttl_seconds: int = 3600
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST
    emission_policy: str = "delta"       # "delta" | "snapshot"
    score_field: str = "score"           # field name in candidate record
    scorer_backend: str = "external_score"  # "external_score" | "llm" | "embedding"

    def __post_init__(self):
        if self.scorer_backend not in VALID_SCORER_BACKENDS:
            raise ValueError(
                f"Invalid scorer_backend={self.scorer_backend!r}. "
                f"Must be one of {VALID_SCORER_BACKENDS}."
            )


# ---------------------------------------------------------------------------
# SemTopKFunction
# ---------------------------------------------------------------------------

class SemTopKFunction(KeyedProcessFunction):
    """Keyed continuous top-k state machine.

    Input: candidate records as dicts with at least ``candidate_id`` and
    a score field (default ``"score"``).

    Output: top-k snapshot dicts emitted on change (delta) or every update.
    """

    def __init__(self, config: Optional[SemTopKConfig] = None) -> None:
        self._config = config or SemTopKConfig()
        self._candidates: Optional[MapState] = None
        self._snapshot: Optional[ValueState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._config.ttl_seconds
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
            "SemTopKFunction opened (k=%d, max_candidates=%d)",
            self._config.k, self._config.max_candidates,
        )

    # -- core ----------------------------------------------------------------

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        if not isinstance(value, dict):
            return

        # ── Handle async scorer merge-back ────────────────────────────
        if value.get("task_type") in ("score_llm", "score_embedding"):
            yield from self._handle_scorer_result(value, now_ms)
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

        # Accept retrieval envelope (from cts_retrieve) — expand candidates
        if "candidates" in value and isinstance(value["candidates"], list):
            from pyflink.semantic_runtime.stateful.event_model import (
                retrieve_to_topk_items,
            )
            items = retrieve_to_topk_items(value)
            meta["last_query"] = str(value.get("query", meta.get("last_query", "")))
            meta["last_query_seq_id"] = int(
                value.get("query_seq_id", meta.get("last_query_seq_id", 0))
            )
            meta["last_source"] = str(value.get("source", meta.get("last_source", "")))
            meta["last_degraded"] = bool(value.get("degraded", False))
            meta["last_error"] = str(value.get("error", ""))
            for item in items:
                yield from self._ingest_candidate(item, meta, now_ms)
        else:
            # Single candidate upsert
            yield from self._ingest_candidate(value, meta, now_ms)

        # Enforce candidate buffer limit
        self._enforce_candidate_limit()

        # Recompute top-k and possibly emit
        yield from self._recompute_and_emit(meta, now_ms)

    # -- scorer backend routing -----------------------------------------------

    def _ingest_candidate(
        self, item: Dict[str, Any], meta: Dict[str, Any], now_ms: int,
    ):
        """Route a single candidate through the configured scorer backend.

        - ``external_score``: upsert directly (score already present).
        - ``llm`` / ``embedding``: if the candidate lacks a score, emit a
          side-output work item for async scoring; otherwise upsert directly.
        """
        backend = self._config.scorer_backend
        score_field = self._config.score_field

        if backend == "external_score":
            self._upsert_candidate(item, now_ms)
            return

        # llm / embedding: check if score is already present
        has_score = (
            score_field in item
            and item[score_field] is not None
            and item[score_field] != 0.0
        )
        if has_score:
            self._upsert_candidate(item, now_ms)
            return

        # No score → emit async scoring request via side output
        task_type = f"score_{backend}"  # "score_llm" or "score_embedding"
        work = AsyncWorkItem(
            key=meta.get("key", ""),
            task_type=task_type,
            payload={
                "candidate": item,
                "query": meta.get("last_query", ""),
                "scorer_backend": backend,
            },
        )
        if self._metrics:
            self._metrics.record_async_emit()
        # Yield side-output; the caller wires ASYNC_WORK_TAG in the topology
        yield ASYNC_WORK_TAG, work.to_dict()

    def _handle_scorer_result(
        self, value: Dict[str, Any], now_ms: int,
    ):
        """Process a merge-back result from an async scorer (llm/embedding)."""
        meta = self._meta.value()
        if meta is None:
            return

        if not value.get("success", False):
            logger.warning(
                "sem_topk async scorer failed: %s", value.get("error", "unknown")
            )
            return

        result = value.get("result", {})
        candidate = result.get("candidate", {})
        if candidate and candidate.get("candidate_id"):
            self._upsert_candidate(candidate, now_ms)
            self._enforce_candidate_limit()
            yield from self._recompute_and_emit(meta, now_ms)

    def _upsert_candidate(self, value: Dict[str, Any], now_ms: int) -> None:
        """Insert or update a single candidate in the MapState."""
        cid = value.get("candidate_id", "")
        if not cid:
            return
        value["_updated_ms"] = now_ms
        self._candidates.put(cid, value)

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
        """Recompute top-k from candidate pool and emit if changed."""
        if self._metrics:
            self._metrics.record_recompute()
        score_field = self._config.score_field
        k = self._config.k

        # Collect all candidates with scores
        scored = []
        for cid in self._candidates.keys():
            record = self._candidates.get(cid)
            if record is None:
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
                "version": new_snapshot["version"],
                "changed": changed,
                "emission_policy": self._config.emission_policy,
                "degraded": bool(meta.get("last_degraded", False)),
                "error": str(meta.get("last_error", "")),
                "timestamp_ms": now_ms,
            }

    def _enforce_candidate_limit(self) -> int:
        """Enforce max_candidates limit per overflow_policy. Returns evicted count."""
        entries = []
        for cid in self._candidates.keys():
            record = self._candidates.get(cid)
            if record:
                entries.append((cid, record.get("_updated_ms", 0),
                                record.get(self._config.score_field, 0.0)))

        if len(entries) <= self._config.max_candidates:
            return 0

        policy = self._config.overflow_policy
        if policy == OverflowPolicy.DEGRADE_TAG:
            return 0

        to_evict = len(entries) - self._config.max_candidates
        if policy == OverflowPolicy.DROP_NEWEST:
            entries.sort(key=lambda x: x[1], reverse=True)  # newest first
        else:
            # DROP_OLDEST: evict by oldest update time
            entries.sort(key=lambda x: x[1])

        for i in range(to_evict):
            self._candidates.remove(entries[i][0])
        return to_evict
