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
sem_search — continuous stateful retrieval over keyed state.

Input: keyed ``SemEvent`` dicts (query/request records).

Output: bounded retrieved candidate sets with stable ordering metadata.

State model:
  - ``MapState[candidate_id -> candidate_record]`` for recent retrieval
    cache / index hints per key.
  - ``ValueState[meta]`` for retrieval counters, version markers, eviction
    bookkeeping.

Retrieval flow:
  1. Check keyed cache for locally matching candidates.
  2. Invoke external store retrieval (via side-output async) for cache misses.
  3. Merge results, truncate to ``max_candidates_per_request``, emit.

Guardrails:
  - ``max_candidates_per_request``: hard limit on output set size.
  - ``max_cache_entries_per_key``: bounds keyed cache growth.
  - Strict retrieval timeout budget via async bridge.
  - Deterministic overflow handling.

Relationship to V0.1 ``sem_lookup_join``:
  - ``sem_search`` is the stateful evolution (keyed cache, continuous).
  - ``sem_lookup_join`` remains valid for stateless / simple workloads.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapState, ValueState

from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    sem_search_cache_descriptor,
    build_ttl_config,
)
from pyflink.semantic_runtime.runtime.event_model import SemEvent
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
from pyflink.semantic_runtime.runtime.external_search_backend import (
    ExternalSearchBackend,
)
from pyflink.semantic_runtime.runtime.simple_text_encoder import (
    HashingTextEncoder,
    tokenize_text,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SemSearchConfig:
    """Configuration for the continuous semantic search operator."""
    max_candidates_per_request: int = 20
    max_cache_entries_per_key: int = 200
    ttl_seconds: int = 1800              # 30 min default for retrieval cache
    evict_interval_ms: int = 120_000     # 2 min eviction sweep
    cache_match_fn_name: str = "keyword" # "keyword" | "embedding" (extensible)
    cache_embedding_dim: int = 128
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST
    min_relevance_score: float = 0.0     # minimum score to include in results
    search_backend: Optional[ExternalSearchBackend] = None


# ---------------------------------------------------------------------------
# SemSearchFunction
# ---------------------------------------------------------------------------

class SemSearchFunction(KeyedProcessFunction):
    """Keyed continuous semantic search state machine.

    Usage::

        keyed = ds.key_by(simple_key_selector)
        retrieved = keyed.process(SemSearchFunction(SemSearchConfig(...)))
        # Wire async bridge for external store retrieval:
        merged = build_async_bridge(retrieved, store_fn, merge_fn, ...)
    """

    def __init__(self, config: Optional[SemSearchConfig] = None) -> None:
        self._config = config or SemSearchConfig()
        self._cache: Optional[MapState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None
        self._encoder = HashingTextEncoder(dim=self._config.cache_embedding_dim)

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._config.ttl_seconds
        self._cache = runtime_context.get_map_state(
            sem_search_cache_descriptor(ttl)
        )
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import ValueStateDescriptor
        desc = ValueStateDescriptor("sem_search_meta", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(build_ttl_config(ttl))
        self._meta = runtime_context.get_state(desc)
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context, "sem_search",
        )
        logger.info(
            "SemSearchFunction opened (max_candidates=%d, cache_limit=%d)",
            self._config.max_candidates_per_request,
            self._config.max_cache_entries_per_key,
        )

    # -- core ----------------------------------------------------------------

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """Process one query event or async retrieval result.

        Yields retrieval result dicts on main output.  Yields side-output
        ``AsyncWorkItem`` dicts for external store retrieval on cache miss.
        """
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()

        # Detect async merge-back result (from external store)
        if isinstance(value, dict) and value.get("task_type") == "retrieve":
            yield from self._handle_async_result(value, now_ms)
            return

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
            "total_queries": 0, "cache_hits": 0,
            "cache_misses": 0, "key": event.key,
        }

        # Register eviction timer on first query
        if meta.get("total_queries", 0) == 0 and self._config.evict_interval_ms > 0:
            register_timer(
                ctx.timer_service(), meta, TimerCategory.EVICT,
                now_ms + self._config.evict_interval_ms,
            )

        meta["total_queries"] = meta.get("total_queries", 0) + 1

        # Step 1: Local cache retrieval
        local_candidates = self._local_retrieve(event)

        if local_candidates:
            meta["cache_hits"] = meta.get("cache_hits", 0) + 1
            self._meta.update(meta)

            # Truncate to max_candidates_per_request
            truncated = local_candidates[:self._config.max_candidates_per_request]
            yield {
                "key": event.key,
                "query": event.payload,
                "query_seq_id": event.seq_id,
                "candidates": truncated,
                "candidate_count": len(truncated),
                "truncated": len(local_candidates) > self._config.max_candidates_per_request,
                "source": "cache",
                "timestamp_ms": now_ms,
            }
        else:
            # Step 2: Cache miss → emit async retrieval request
            meta["cache_misses"] = meta.get("cache_misses", 0) + 1
            self._meta.update(meta)

            work = AsyncWorkItem(
                key=event.key, task_type="retrieve",
                payload={
                    "query": event.payload,
                    "event_seq_id": event.seq_id,
                    "max_candidates": self._config.max_candidates_per_request,
                },
            )
            if self._metrics:
                self._metrics.record_async_emit()
            yield ASYNC_WORK_TAG, work.to_dict()

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        """Timer-driven stale cache eviction."""
        meta = self._meta.value()
        if meta is None:
            return
        if self._metrics:
            self._metrics.record_timer_fire()

        category = resolve_timer_category(meta, timestamp)
        if category != TimerCategory.EVICT:
            return

        clear_timer_registration(meta, TimerCategory.EVICT)
        evicted = self._evict_cache(meta)
        if evicted > 0:
            if self._metrics:
                self._metrics.record_eviction(evicted)
            logger.info("Evicted %d stale cache entries for key=%s",
                        evicted, meta.get("key", "?"))

        # Re-register eviction timer
        now_ms = int(time.time() * 1000)
        register_timer(
            ctx.timer_service(), meta, TimerCategory.EVICT,
            now_ms + self._config.evict_interval_ms,
        )
        self._meta.update(meta)

    # -- internals -----------------------------------------------------------

    def _local_retrieve(self, event: SemEvent) -> List[Dict[str, Any]]:
        """Retrieve matching candidates from keyed cache.

        Supports a default keyword overlap path and a lightweight local
        hashing-vector similarity path for demos.
        """
        query = str(event.payload or "")
        if not query.strip():
            return []

        scored = []
        match_fn = str(self._config.cache_match_fn_name or "keyword").lower()
        for cid in self._cache.keys():
            record = self._cache.get(cid)
            if record is None:
                continue
            content = str(record.get("content", record.get("text", "")))
            if not content:
                continue

            if match_fn == "embedding":
                score = self._encoder.similarity(query, content)
            else:
                query_words = set(tokenize_text(query))
                if not query_words:
                    continue
                content_words = set(tokenize_text(content))
                if not content_words:
                    continue
                overlap = len(query_words & content_words)
                score = overlap / max(len(query_words), 1)

            if score > self._config.min_relevance_score:
                scored.append({**record, "_score": score, "candidate_id": cid})

        # Sort by score descending
        scored.sort(key=lambda x: x.get("_score", 0), reverse=True)
        return scored

    def _handle_async_result(
        self, result_dict: Dict[str, Any], now_ms: int
    ):
        """Merge async retrieval results into cache and emit."""
        if not result_dict.get("success", False):
            error = result_dict.get("error", "retrieve_async_failed")
            raise RuntimeError(f"sem_search async retrieval failed: {error}")

        candidates = result_dict.get("result", {}).get("candidates", [])

        # Update cache with retrieved candidates
        for candidate in candidates:
            cid = candidate.get("candidate_id", candidate.get("id", ""))
            if cid:
                candidate["_cached_at_ms"] = now_ms
                self._cache.put(cid, candidate)

        # Enforce cache size limit
        self._enforce_cache_limit()

        # Truncate and emit
        truncated = candidates[:self._config.max_candidates_per_request]
        yield {
            "key": result_dict.get("key", ""),
            "query": result_dict.get("result", {}).get(
                "query",
                result_dict.get("payload", {}).get("query", ""),
            ),
            "query_seq_id": result_dict.get("payload", {}).get("event_seq_id", 0),
            "candidates": truncated,
            "candidate_count": len(truncated),
            "truncated": len(candidates) > self._config.max_candidates_per_request,
            "source": "async_store",
            "timestamp_ms": now_ms,
        }

    def _enforce_cache_limit(self) -> int:
        """Evict entries if cache exceeds limit per overflow_policy.

        - DROP_OLDEST: evict oldest entries beyond the limit.
        - DROP_NEWEST: evict newest entries beyond the limit.

        Returns count evicted.
        """
        entries = []
        for cid in self._cache.keys():
            record = self._cache.get(cid)
            if record:
                entries.append((cid, record.get("_cached_at_ms", 0)))

        if len(entries) <= self._config.max_cache_entries_per_key:
            return 0

        policy = self._config.overflow_policy
        to_evict = len(entries) - self._config.max_cache_entries_per_key
        if policy == OverflowPolicy.DROP_NEWEST:
            entries.sort(key=lambda x: x[1], reverse=True)  # newest first
        else:
            entries.sort(key=lambda x: x[1])  # oldest first (default)

        for i in range(to_evict):
            self._cache.remove(entries[i][0])
        return to_evict

    def _evict_cache(self, meta: Dict[str, Any]) -> int:
        """Timer-driven eviction of oldest cache entries beyond limit."""
        return self._enforce_cache_limit()
