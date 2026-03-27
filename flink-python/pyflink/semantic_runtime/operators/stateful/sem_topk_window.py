"""Scoped persistent runtime for ``sem_topk``.

This physical path keeps ``sem_topk`` as one continuous keyed operator while
using scope fires only as scope updates. Each ``scope_id`` contributes one
scoped top-k snapshot; repeated fires replace that contribution. The global
frontier is recomputed from the current contribution set.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapState, ValueState, ValueStateDescriptor

from pyflink.semantic_runtime.operators.stateful.sem_topk import SemTopKConfig
from pyflink.semantic_runtime.runtime.state_descriptors import (
    build_ttl_config,
    sem_topk_scope_contributions_descriptor,
    sem_topk_snapshot_descriptor,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.sem_spec import TopKQuerySpec


class ScopedPersistentSemTopKFunction(KeyedProcessFunction):
    """Continuous top-k that consumes scoped top-k contributions."""

    def __init__(
        self,
        config: Optional[SemTopKConfig] = None,
        query_spec: Optional[TopKQuerySpec] = None,
    ) -> None:
        self._config = config or SemTopKConfig()
        self._query_spec = query_spec or TopKQuerySpec()
        self._scope_contributions: Optional[MapState] = None
        self._snapshot: Optional[ValueState] = None
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None

    @property
    def _resolved_ttl_seconds(self) -> int:
        scope = self._query_spec.scope_policy
        if scope.ttl_seconds is not None:
            return int(scope.ttl_seconds)
        return int(self._config.ttl_seconds)

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl_seconds = self._resolved_ttl_seconds
        self._scope_contributions = runtime_context.get_map_state(
            sem_topk_scope_contributions_descriptor(ttl_seconds),
        )
        self._snapshot = runtime_context.get_state(
            sem_topk_snapshot_descriptor(ttl_seconds),
        )
        desc = ValueStateDescriptor("sem_topk_external_window_meta", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(build_ttl_config(ttl_seconds))
        self._meta = runtime_context.get_state(desc)
        self._metrics = StatefulOperatorMetrics.from_runtime_context(
            runtime_context,
            "sem_topk_external_window",
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()
            self._metrics.record_recompute()
        if not isinstance(value, dict):
            return

        scope_id = str(value.get("scope_id", "") or "")
        if not scope_id:
            raise ValueError(
                "sem_topk persistent scoped path requires non-empty scope_id"
            )
        top_items = value.get("top_items", value.get("topk", []))
        if not isinstance(top_items, list):
            raise ValueError(
                "sem_topk external_window persistent path requires top_items/topk list"
            )
        incoming_scope_epoch = int(value.get("scope_epoch", 0) or 0)
        incoming_scope_version = self._resolve_scope_version(value, now_ms)

        meta = self._meta.value() or {
            "key": str(ctx.get_current_key()),
            "update_count": 0,
            "last_ranking_text": "",
            "last_query_seq_id": 0,
            "last_source": "",
            "last_error": "",
        }
        meta["update_count"] = int(meta.get("update_count", 0)) + 1
        meta["last_ranking_text"] = str(value.get("query", meta.get("last_ranking_text", "")) or "")
        meta["last_query_seq_id"] = int(value.get("query_seq_id", meta.get("last_query_seq_id", 0)) or 0)
        meta["last_source"] = str(value.get("source", meta.get("last_source", "")) or "")
        meta["last_error"] = str(value.get("error", "") or "")

        assert self._scope_contributions is not None
        assert self._snapshot is not None
        assert self._meta is not None
        existing = self._scope_contributions.get(scope_id)
        if existing is not None:
            existing_scope_epoch = int(existing.get("scope_epoch", 0) or 0)
            existing_scope_version = int(existing.get("scope_version", 0) or 0)
            if (
                incoming_scope_epoch < existing_scope_epoch
                or (
                    incoming_scope_epoch == existing_scope_epoch
                    and incoming_scope_version < existing_scope_version
                )
            ):
                if self._metrics:
                    self._metrics.record_stale_window()
                self._meta.update(meta)
                return
        self._scope_contributions.put(
            scope_id,
            {
                "scope_id": scope_id,
                "scope_epoch": incoming_scope_epoch,
                "scope_version": incoming_scope_version,
                "top_items": [dict(item) for item in top_items],
                "query": meta["last_ranking_text"],
                "query_seq_id": meta["last_query_seq_id"],
                "source": meta["last_source"],
                "error": meta["last_error"],
                "timestamp_ms": now_ms,
            },
        )

        yield from self._recompute_and_emit(meta, now_ms)
        self._meta.update(meta)

    @staticmethod
    def _resolve_scope_version(value: Dict[str, Any], now_ms: int) -> int:
        """Resolve one monotonic scope contribution version."""
        for field in ("scope_version", "version", "timestamp_ms"):
            raw = value.get(field)
            if raw is None:
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
        return int(now_ms)

    def _recompute_and_emit(self, meta: Dict[str, Any], now_ms: int):
        assert self._scope_contributions is not None
        assert self._snapshot is not None

        best_by_candidate: Dict[str, Dict[str, Any]] = {}
        for scope_id in self._scope_contributions.keys():
            contribution = self._scope_contributions.get(scope_id)
            if contribution is None:
                continue
            for row in contribution.get("top_items", []):
                if not isinstance(row, dict):
                    continue
                candidate_id = str(row.get("candidate_id", "") or "")
                if not candidate_id:
                    raise ValueError(
                        "sem_topk external_window persistent contribution requires candidate_id"
                    )
                score = row.get(self._config.score_field)
                if not isinstance(score, (int, float)):
                    raise ValueError(
                        f"sem_topk external_window persistent contribution requires numeric {self._config.score_field!r}"
                    )
                current = best_by_candidate.get(candidate_id)
                if current is None or float(score) > float(current[self._config.score_field]):
                    best_by_candidate[candidate_id] = dict(row)

        ranked = sorted(
            best_by_candidate.values(),
            key=lambda row: float(row[self._config.score_field]),
            reverse=True,
        )
        top_rows = ranked[: self._query_spec.k]
        top_ids = [str(row["candidate_id"]) for row in top_rows]
        previous_snapshot = self._snapshot.value()
        previous_ids = previous_snapshot.get("top_ids", []) if previous_snapshot else []
        changed = top_ids != previous_ids
        new_snapshot = {
            "top_ids": top_ids,
            "top_records": top_rows,
            "total_candidates": len(best_by_candidate),
            "stale_candidates": 0,
            "version": int(meta.get("update_count", 0)),
            "timestamp_ms": now_ms,
        }
        self._snapshot.update(new_snapshot)

        should_emit = changed or self._config.emission_policy == "snapshot"
        if not should_emit:
            return
        yield {
            "key": meta.get("key", ""),
            "topk": top_rows,
            "top_items": top_rows,
            "top_ids": top_ids,
            "query": meta.get("last_ranking_text", ""),
            "query_seq_id": meta.get("last_query_seq_id", 0),
            "source": meta.get("last_source", ""),
            "total_candidates": len(best_by_candidate),
            "stale_candidates": 0,
            "version": new_snapshot["version"],
            "changed": changed,
            "emission_policy": self._config.emission_policy,
            "error": str(meta.get("last_error", "")),
            "timestamp_ms": now_ms,
        }
