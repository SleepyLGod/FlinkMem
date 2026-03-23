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

"""Internal kernel helpers for stateful ``sem_topk``.

These helpers operate on Flink state objects but are otherwise pure kernel
logic: candidate upsert, scope reset, stale-eviction, candidate-cap
enforcement, and frontier recomputation.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Optional

from pyflink.semantic_runtime.runtime.state_descriptors import OverflowPolicy
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    clear_timer_registration,
)


def upsert_topk_candidate(
    candidates_state: Any,
    value: Dict[str, Any],
    *,
    score_field: str,
    query_version: int,
    score_backend: str,
    now_ms: int,
    scope_time_ms: Optional[int] = None,
) -> bool:
    """Insert or update a single scored candidate in keyed state."""
    candidate_id = value.get("candidate_id", "")
    if not candidate_id:
        return False
    if score_field not in value or value[score_field] is None:
        return False

    value["_updated_ms"] = now_ms
    value["_scope_time_ms"] = int(scope_time_ms if scope_time_ms is not None else now_ms)
    value.setdefault("_score_version", 1)
    value.setdefault("_query_version", query_version)
    value.setdefault("_score_backend", score_backend)
    candidates_state.put(candidate_id, value)
    return True


def recompute_topk_snapshot(
    candidates_state: Any,
    snapshot_state: Any,
    meta: Dict[str, Any],
    *,
    score_field: str,
    k: int,
    query_version: int,
    emission_policy: str,
    now_ms: int,
    force_emit: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Recompute top-k from candidate state and yield snapshot outputs."""
    scored = []
    stale_count = 0
    for candidate_id in candidates_state.keys():
        record = candidates_state.get(candidate_id)
        if record is None:
            continue
        record_qv = record.get("_query_version")
        if record_qv is not None and record_qv != query_version:
            stale_count += 1
            continue
        score = record.get(score_field, 0.0)
        scored.append((candidate_id, score, record))

    scored.sort(key=lambda item: item[1], reverse=True)
    new_topk_ids = [candidate_id for candidate_id, _, _ in scored[:k]]

    previous_snapshot = snapshot_state.value()
    previous_ids = previous_snapshot.get("top_ids", []) if previous_snapshot else []
    changed = new_topk_ids != previous_ids

    new_snapshot = {
        "top_ids": new_topk_ids,
        "top_records": [record for _, _, record in scored[:k]],
        "total_candidates": len(scored),
        "stale_candidates": stale_count,
        "version": meta.get("update_count", 0),
        "timestamp_ms": now_ms,
    }
    snapshot_state.update(new_snapshot)

    should_emit = force_emit or changed or emission_policy == "snapshot"
    if not should_emit:
        return

    yield {
        "key": meta.get("key", ""),
        "topk": new_snapshot["top_records"],
        "top_ids": new_topk_ids,
        "query": meta.get("last_ranking_text", ""),
        "query_seq_id": meta.get("last_query_seq_id", 0),
        "source": meta.get("last_source", ""),
        "total_candidates": len(scored),
        "stale_candidates": stale_count,
        "version": new_snapshot["version"],
        "changed": changed,
        "emission_policy": emission_policy,
        "error": str(meta.get("last_error", "")),
        "timestamp_ms": now_ms,
    }


def emit_scope_close_snapshot(
    candidates_state: Any,
    snapshot_state: Any,
    meta: Dict[str, Any],
    *,
    score_field: str,
    k: int,
    query_version: int,
    emission_policy: str,
    now_ms: int,
    reason: str,
) -> Iterator[Dict[str, Any]]:
    """Emit the current frontier before a scope reset, if there is state."""
    if not list(candidates_state.keys()):
        return
    for output in recompute_topk_snapshot(
        candidates_state,
        snapshot_state,
        meta,
        score_field=score_field,
        k=k,
        query_version=query_version,
        emission_policy=emission_policy,
        now_ms=now_ms,
        force_emit=True,
    ):
        output["scope_close_reason"] = reason
        output["scope_epoch"] = meta.get("scope_epoch", 0)
        yield output


def reset_topk_scope_state(
    candidates_state: Any,
    snapshot_state: Any,
    meta: Dict[str, Any],
    *,
    reason: str,
) -> None:
    """Clear current scope state and advance scope epoch."""
    if hasattr(candidates_state, "clear"):
        candidates_state.clear()
    else:
        for candidate_id in list(candidates_state.keys()):
            candidates_state.remove(candidate_id)

    if hasattr(snapshot_state, "clear"):
        snapshot_state.clear()
    else:
        snapshot_state.update(None)

    clear_timer_registration(meta, TimerCategory.RECOMPUTE)
    clear_timer_registration(meta, TimerCategory.FLUSH)
    meta.pop("pending_scope_close_reason", None)
    meta["accepted_count"] = 0
    meta["scope_epoch"] = int(meta.get("scope_epoch", 0) or 0) + 1
    meta["scope_last_time_ms"] = 0
    meta["scope_bucket_id"] = None
    meta["last_scope_reset_reason"] = reason


def evict_scope_stale_candidates(candidates_state: Any, cutoff_ms: int) -> int:
    """Evict candidates outside an operator-owned sliding scope."""
    evicted = 0
    for candidate_id in list(candidates_state.keys()):
        record = candidates_state.get(candidate_id)
        if record is None:
            continue
        scope_time_ms = int(record.get("_scope_time_ms", record.get("_updated_ms", 0)) or 0)
        if scope_time_ms < cutoff_ms:
            candidates_state.remove(candidate_id)
            evicted += 1
    return evicted


def enforce_candidate_limit(
    candidates_state: Any,
    *,
    max_candidates: int,
    overflow_policy: OverflowPolicy,
) -> int:
    """Enforce max candidate count by update time."""
    entries = []
    for candidate_id in candidates_state.keys():
        record = candidates_state.get(candidate_id)
        if record:
            entries.append((candidate_id, int(record.get("_updated_ms", 0) or 0)))

    if len(entries) <= max_candidates:
        return 0

    to_evict = len(entries) - max_candidates
    if overflow_policy == OverflowPolicy.DROP_NEWEST:
        entries.sort(key=lambda item: item[1], reverse=True)
    else:
        entries.sort(key=lambda item: item[1])

    for index in range(to_evict):
        candidates_state.remove(entries[index][0])
    return to_evict
