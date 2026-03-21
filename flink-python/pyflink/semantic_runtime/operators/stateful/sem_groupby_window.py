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

This A-path runtime processes a closed/bounded ``WindowSnapshot`` as a single
grouping scope. It does not keep cross-scope group state. Assignments emitted
from one snapshot do not influence later snapshots.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyflink.datastream.functions import KeyedProcessFunction

from pyflink.semantic_runtime.semantic_spec import GroupbyQuerySpec
from pyflink.semantic_runtime.runtime.async_bridge import ASYNC_WORK_TAG, AsyncWorkItem
from pyflink.semantic_runtime.runtime.event_model import (
    SemanticEvent,
    is_window_snapshot,
    window_snapshot_to_semantic_events,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby import (
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
        self._resolved_assignment_method = resolve_groupby_assignment_method(query_spec)
        self._maintenance_trigger_policy = (
            query_spec.maintenance_trigger_policy if query_spec is not None else None
        )
        self._encoder = HashingTextEncoder(dim=128)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return
        if not is_window_snapshot(value):
            return

        events = [SemanticEvent.from_dict(e) for e in window_snapshot_to_semantic_events(value)]
        if not events:
            return

        groups: Dict[str, Dict[str, Any]] = {}
        assignment_rows: List[Dict[str, Any]] = []
        side_outputs: List[Any] = []
        for event in events:
            outs = self._assign_within_scope(groups, event)
            for out in outs:
                if isinstance(out, tuple):
                    side_outputs.append(out)
                else:
                    assignment_rows.append(out)

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
            if self._resolved_assignment_method == "llm_verify_local_refine":
                groups = relabel_group_profiles(groups)
            if merged_into:
                for row in assignment_rows:
                    row["group_id"] = self._resolve_merged_group_id(
                        str(row.get("group_id", "")),
                        merged_into,
                    )
                remapped_side_outputs: List[Any] = []
                for item in side_outputs:
                    tag, payload = item
                    if isinstance(payload, dict):
                        work_payload = dict(payload.get("payload", {}))
                        tentative_group = work_payload.get("tentative_group")
                        if tentative_group:
                            work_payload["tentative_group"] = self._resolve_merged_group_id(
                                str(tentative_group), merged_into
                            )
                            payload = dict(payload)
                            payload["payload"] = work_payload
                    remapped_side_outputs.append((tag, payload))
                side_outputs = remapped_side_outputs

        for row in assignment_rows:
            yield row
        for item in side_outputs:
            yield item

    def _assign_within_scope(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemanticEvent,
    ) -> List[Any]:
        now_ms = int(time.time() * 1000)
        best_group_id, confidence = self._local_assign(groups, event)

        if confidence >= self._resolved_assign_threshold and best_group_id:
            self._update_group(groups, best_group_id, event, now_ms)
            return [self._assignment_row(event, best_group_id, confidence, "local")]

        if confidence >= self._resolved_new_group_threshold and best_group_id:
            self._update_group(groups, best_group_id, event, now_ms)
            return [
                self._assignment_row(event, best_group_id, confidence, "tentative"),
                (
                    ASYNC_WORK_TAG,
                    AsyncWorkItem(
                        key=event.key,
                        task_type="classify",
                        payload=self._classify_payload(groups, event.to_dict(), best_group_id),
                    ).to_dict(),
                ),
            ]

        created_group_id = self._maybe_create_group(groups, event, now_ms)
        if created_group_id is not None:
            outputs: List[Any] = [
                self._assignment_row(event, created_group_id, 0.0, "new_group")
            ]
            if self._resolved_assignment_method in {"llm", "llm_verify_local_refine"}:
                outputs.append(
                    (
                        ASYNC_WORK_TAG,
                        AsyncWorkItem(
                            key=event.key,
                            task_type="classify",
                            payload=self._classify_payload(groups, event.to_dict(), created_group_id),
                        ).to_dict(),
                    )
                )
            return outputs

        return [
            (
                ASYNC_WORK_TAG,
                AsyncWorkItem(
                    key=event.key,
                    task_type="classify",
                    payload=self._classify_payload(groups, event.to_dict(), None),
                ).to_dict(),
            )
        ]

    def _local_assign(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemanticEvent,
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
        event: SemanticEvent,
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
        event: SemanticEvent,
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

    @staticmethod
    def _assignment_row(
        event: SemanticEvent,
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

    def _classify_payload(
        self,
        groups: Dict[str, Dict[str, Any]],
        event_dict: Dict[str, Any],
        tentative_group: Optional[str],
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "event": event_dict,
            "tentative_group": tentative_group,
        }
        if self._resolved_assignment_method in {"llm", "llm_verify_local_refine"}:
            payload["candidate_groups"] = [
                {
                    "group_id": group_id,
                    "label": profile.get("label", ""),
                    "summary": profile.get("summary", ""),
                    "event_count": int(profile.get("event_count", 0)),
                }
                for group_id, profile in groups.items()
            ]
        return payload

    @staticmethod
    def _resolve_merged_group_id(group_id: str, merged_into: Dict[str, str]) -> str:
        current = group_id
        seen = set()
        while current in merged_into and current not in seen:
            seen.add(current)
            current = merged_into[current]
        return current
