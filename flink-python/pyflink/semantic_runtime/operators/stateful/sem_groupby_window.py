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

"""Window-owned bounded semantic grouping.

This runtime processes one closed ``WindowSnapshot`` as one grouping scope.
It does not keep cross-scope state. Local assignment methods group events
inside the snapshot directly. Async semantic methods emit one scope-level
classification request and only output final assignments after merge-back.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyflink.datastream.functions import KeyedProcessFunction

from pyflink.semantic_runtime.sem_spec import GroupbyQuerySpec
from pyflink.semantic_runtime.runtime.async_bridge import ASYNC_WORK_TAG, AsyncWorkItem
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    is_window_snapshot,
    window_snapshot_to_sem_events,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby import (
    _ASYNC_ASSIGNMENT_METHODS,
    _LOCAL_ASSIGNMENT_METHODS,
    SemGroupbyConfig,
    _new_group_profile,
    relabel_group_profiles,
    merge_similar_group_profiles,
    resolve_groupby_runtime_params,
    resolve_groupby_assignment_method,
    score_group_profile,
)
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder


class WindowOwnedSemGroupbyFunction(KeyedProcessFunction):
    """Bounded/window-owned grouping runtime for ``sem_groupby``."""

    def __init__(
        self,
        config: Optional[SemGroupbyConfig] = None,
        query_spec: Optional[GroupbyQuerySpec] = None,
    ) -> None:
        self._config = config or SemGroupbyConfig()
        self._query_spec = query_spec
        (
            _resolved_ttl_seconds,
            self._resolved_max_groups_per_key,
            self._resolved_assign_threshold,
            self._resolved_new_group_threshold,
        ) = resolve_groupby_runtime_params(self._config, self._query_spec)
        self._resolved_assignment_method = resolve_groupby_assignment_method(
            self._config,
            query_spec,
        )
        self._maintenance_trigger_policy = (
            query_spec.maintenance_trigger_policy if query_spec is not None else None
        )
        self._encoder = HashingTextEncoder(dim=128)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return
        if not is_window_snapshot(value):
            return

        events = [SemEvent.from_dict(e) for e in window_snapshot_to_sem_events(value)]
        if not events:
            return

        if self._resolved_assignment_method in _ASYNC_ASSIGNMENT_METHODS:
            yield (
                ASYNC_WORK_TAG,
                AsyncWorkItem(
                    key=events[0].key,
                    task_type="classify",
                    payload=self._build_scope_async_payload(events),
                ).to_dict(),
            )
            return

        groups: Dict[str, Dict[str, Any]] = {}
        assignment_rows: List[Dict[str, Any]] = []
        for event in events:
            assignment_rows.extend(self._assign_within_scope(groups, event))

        if (
            self._maintenance_trigger_policy is not None
            and self._maintenance_trigger_policy.mode == "on_scope_close"
        ):
            groups, merged_into, _merge_count = merge_similar_group_profiles(
                groups,
                assignment_method=self._resolved_assignment_method,
                encoder=self._encoder,
                assign_threshold=self._resolved_assign_threshold,
                new_group_threshold=self._resolved_new_group_threshold,
                now_ms=int(time.time() * 1000),
            )
            if self._config.refresh_labels_during_maintenance:
                groups = relabel_group_profiles(groups)
            if merged_into:
                for row in assignment_rows:
                    row["group_id"] = self._resolve_merged_group_id(
                        str(row.get("group_id", "")),
                        merged_into,
                    )

        for row in assignment_rows:
            yield row

    def _assign_within_scope(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
    ) -> List[Dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        if self._resolved_assignment_method not in _LOCAL_ASSIGNMENT_METHODS:
            raise ValueError(
                f"WindowOwnedSemGroupbyFunction received unsupported local assignment_method={self._resolved_assignment_method!r}."
            )

        best_group_id, confidence = self._local_assign(groups, event)
        if best_group_id and confidence >= self._resolved_assign_threshold:
            self._update_group(groups, best_group_id, event, now_ms)
            return [self._assignment_row(event, best_group_id, confidence, "local")]

        created_group_id = self._create_group_or_raise(groups, event, now_ms)
        return [self._assignment_row(event, created_group_id, confidence, "new_group")]

    def _local_assign(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
    ) -> Tuple[Optional[str], float]:
        best_id: Optional[str] = None
        best_score = 0.0
        for group_id, profile in groups.items():
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

    def _update_group(
        self,
        groups: Dict[str, Dict[str, Any]],
        group_id: str,
        event: SemEvent,
        now_ms: int,
    ) -> None:
        profile = groups.get(group_id)
        if profile is None:
            return
        profile["event_count"] = profile.get("event_count", 0) + 1
        profile["last_update_ms"] = now_ms

    def _maybe_create_group(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
        now_ms: int,
    ) -> Optional[str]:
        count = len(groups)
        if count >= self._resolved_max_groups_per_key:
            policy = self._config.overflow_policy
            if policy.name == "DROP_OLDEST":
                oldest_group_id = min(
                    groups.items(),
                    key=lambda item: item[1].get("last_update_ms", 0),
                )[0]
                groups.pop(oldest_group_id, None)
            elif policy.name == "DROP_NEWEST":
                return None

        group_id = uuid.uuid4().hex[:8]
        label = " ".join(event.payload.split()[:5])
        profile = _new_group_profile(group_id, label, now_ms)
        profile["event_count"] = 1
        groups[group_id] = profile
        return group_id

    def _create_group_or_raise(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
        now_ms: int,
    ) -> str:
        """Create a new group or fail when policy forbids it."""
        group_id = self._maybe_create_group(groups, event, now_ms)
        if group_id is None:
            raise RuntimeError(
                "sem_groupby window-owned path could not create a new group under the current overflow policy."
            )
        return group_id

    @staticmethod
    def _assignment_row(
        event: SemEvent,
        group_id: str,
        confidence: float,
        source: str,
    ) -> Dict[str, Any]:
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

    def _build_scope_async_payload(self, events: List[SemEvent]) -> Dict[str, Any]:
        """Build one scope-level async assignment payload."""
        return {
            "events": [event.to_dict() for event in events],
            "existing_groups": [],
            "scope_chunk_size": int(self._config.scope_chunk_size),
            "scope_epoch": 0,
            "scope_close_pending": True,
        }

    @staticmethod
    def _resolve_merged_group_id(group_id: str, merged_into: Dict[str, str]) -> str:
        current = group_id
        seen = set()
        while current in merged_into and current not in seen:
            seen.add(current)
            current = merged_into[current]
        return current
