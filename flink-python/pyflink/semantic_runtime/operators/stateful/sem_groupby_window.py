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

"""Bounded-scope specialization for ``sem_groupby``."""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyflink.datastream.functions import KeyedProcessFunction

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.operators.stateful.sem_groupby import (
    _LOCAL_GROUPBY_VARIANTS,
    _LLM_GROUPBY_VARIANTS,
    GROUPBY_DERIVED_LABEL_TOKEN_LIMIT,
    GROUPBY_ID_HEX_CHARS,
    GROUPBY_LOCAL_ENCODER_DIM,
    SemGroupbyConfig,
    _append_profile_example,
    _profile_summary_from_examples,
    _new_group_profile,
    apply_group_merge,
    derive_group_label,
    merge_similar_group_profiles,
    resolve_groupby_runtime_params,
    resolve_groupby_variant,
    score_group_profile,
    split_group_profiles,
)
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    is_window_snapshot,
    window_snapshot_to_sem_events,
)
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder
from pyflink.semantic_runtime.runtime.steps import (
    evaluate_sem_group_assignment_chunks_sync,
    evaluate_sem_group_assignments_sync,
    evaluate_sem_group_refine_sync,
)
from pyflink.semantic_runtime.sem_spec import GroupbyQuerySpec


class WindowOwnedSemGroupbyFunction(KeyedProcessFunction):
    """Run one bounded grouping pass over one closed ``WindowSnapshot``."""

    def __init__(
        self,
        config: Optional[SemGroupbyConfig] = None,
        query_spec: Optional[GroupbyQuerySpec] = None,
        *,
        llm_config: Optional[LLMClientConfig] = None,
    ) -> None:
        self._config = config or SemGroupbyConfig()
        self._query_spec = query_spec
        self._llm_config = llm_config
        (
            _resolved_ttl_seconds,
            self._resolved_max_groups_per_key,
            self._resolved_assign_threshold,
            self._resolved_new_group_threshold,
        ) = resolve_groupby_runtime_params(self._config, self._query_spec)
        self._resolved_variant = resolve_groupby_variant(self._config, query_spec)
        if self._resolved_variant in _LLM_GROUPBY_VARIANTS and query_spec is None:
            raise ValueError(
                f"sem_groupby variant={self._resolved_variant!r} requires query_spec"
            )
        self._resolved_assignment_batch_size = int(self._config.assignment_batch_size)
        self._resolved_max_group_examples = int(self._config.max_group_examples)
        self._maintenance_trigger_policy = (
            query_spec.maintenance_trigger_policy if query_spec is not None else None
        )
        self._encoder = HashingTextEncoder(dim=GROUPBY_LOCAL_ENCODER_DIM)
        self._client: Optional[LLMClient] = None

    def open(self, runtime_context) -> None:
        if self._resolved_variant in _LLM_GROUPBY_VARIANTS:
            if self._llm_config is None:
                raise ValueError(
                    f"sem_groupby variant={self._resolved_variant!r} requires llm_config"
                )
            self._client = create_llm_client(self._llm_config)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def process_element(self, value: Any, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict) or not is_window_snapshot(value):
            return

        events = [SemEvent.from_dict(item) for item in window_snapshot_to_sem_events(value)]
        if not events:
            return

        groups: Dict[str, Dict[str, Any]] = {}
        assignment_rows: List[Dict[str, Any]] = []

        if self._resolved_variant in _LOCAL_GROUPBY_VARIANTS:
            for event in events:
                assignment_rows.extend(self._assign_locally(groups, event))
        elif self._resolved_variant in _LLM_GROUPBY_VARIANTS:
            assignment_rows.extend(self._assign_with_llm(groups, events))
        else:
            raise ValueError(
                f"Unsupported internal groupby variant={self._resolved_variant!r}."
            )

        if (
            self._maintenance_trigger_policy is not None
            and self._maintenance_trigger_policy.mode == "on_scope_close"
        ):
            assignment_rows = self._run_scope_close_maintenance(
                groups=groups,
                assignment_rows=assignment_rows,
            )

        for row in assignment_rows:
            yield row

    def _assign_locally(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
    ) -> List[Dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        best_group_id, confidence = self._local_assign(groups, event)
        if best_group_id is not None and confidence >= self._resolved_assign_threshold:
            self._update_group(groups, best_group_id, event, now_ms)
            return [self._assignment_row(event, best_group_id, confidence, "local")]

        created_group_id = self._create_group_or_raise(groups, event, now_ms)
        return [self._assignment_row(event, created_group_id, confidence, "new_group")]

    def _assign_with_llm(
        self,
        groups: Dict[str, Dict[str, Any]],
        events: List[SemEvent],
    ) -> List[Dict[str, Any]]:
        if self._client is None:
            raise RuntimeError("sem_groupby LLM runtime is not initialized")

        now_ms = int(time.time() * 1000)
        assignment_rows: List[Dict[str, Any]] = []
        event_chunks: List[List[SemEvent]] = [
            events[index : index + self._resolved_assignment_batch_size]
            for index in range(0, len(events), self._resolved_assignment_batch_size)
        ]
        existing_groups = [
            {
                "group_id": group_id,
                "label": profile.get("label", ""),
                "summary": profile.get("summary", ""),
                "event_count": int(profile.get("event_count", 0)),
                "examples": list(profile.get("examples", [])),
            }
            for group_id, profile in groups.items()
        ]
        if len(event_chunks) == 1:
            assignment_chunks = [
                evaluate_sem_group_assignments_sync(
                    client=self._client,
                    intent=self._resolve_intent(),
                    existing_groups=existing_groups,
                    events=[event.to_dict() for event in event_chunks[0]],
                )
            ]
        else:
            assignment_chunks = evaluate_sem_group_assignment_chunks_sync(
                client=self._client,
                intent=self._resolve_intent(),
                existing_groups=existing_groups,
                event_chunks=[[event.to_dict() for event in chunk] for chunk in event_chunks],
            )

        for chunk, assignments in zip(event_chunks, assignment_chunks):
            events_by_seq_id = {event.seq_id: event for event in chunk}
            seen_seq_ids = set()
            for assignment in assignments:
                event_seq_id = int(assignment["event_seq_id"])
                event = events_by_seq_id.get(event_seq_id)
                if event is None:
                    raise ValueError(
                        f"sem_group_assign returned unknown event_seq_id={event_seq_id}"
                    )
                if event_seq_id in seen_seq_ids:
                    raise ValueError(
                        f"sem_group_assign returned duplicate event_seq_id={event_seq_id}"
                    )
                seen_seq_ids.add(event_seq_id)
                decision = str(assignment["decision"])
                confidence = float(assignment["confidence"])
                if decision == "existing":
                    group_id = str(assignment["group_id"])
                    self._update_group(groups, group_id, event, now_ms)
                    assignment_rows.append(
                        self._assignment_row(event, group_id, confidence, self._resolved_variant)
                    )
                    continue
                label = str(assignment["label"])
                group_id = self._create_group_or_raise(groups, event, now_ms, label=label)
                assignment_rows.append(
                    self._assignment_row(event, group_id, confidence, self._resolved_variant)
                )
            if seen_seq_ids != set(events_by_seq_id.keys()):
                missing = sorted(set(events_by_seq_id.keys()) - seen_seq_ids)
                raise ValueError(
                    f"sem_group_assign did not return assignments for event_seq_ids={missing!r}"
                )
        return assignment_rows

    def _run_scope_close_maintenance(
        self,
        *,
        groups: Dict[str, Dict[str, Any]],
        assignment_rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if self._resolved_variant == "llm_refine":
            self._apply_llm_refine(groups)
            return assignment_rows

        groups_after_split, _split_count = split_group_profiles(
            groups,
            variant=self._resolved_variant,
            encoder=self._encoder,
            now_ms=int(time.time() * 1000),
            max_examples=self._resolved_max_group_examples,
            rule_threshold=self._config.local_rule_split_seed_similarity_threshold,
            embedding_threshold=self._config.local_embedding_split_seed_similarity_threshold,
        )
        groups.clear()
        groups.update(groups_after_split)

        groups_after_merge, merged_into, _merge_count = merge_similar_group_profiles(
            groups,
            variant=self._resolved_variant,
            encoder=self._encoder,
            assign_threshold=self._resolved_assign_threshold,
            new_group_threshold=self._resolved_new_group_threshold,
            now_ms=int(time.time() * 1000),
            max_examples=self._resolved_max_group_examples,
        )
        groups.clear()
        groups.update(groups_after_merge)
        if self._config.refresh_labels_during_maintenance:
            for profile in groups.values():
                profile["label"] = derive_group_label(profile)

        if not merged_into:
            return assignment_rows
        rewritten_rows: List[Dict[str, Any]] = []
        for row in assignment_rows:
            updated = dict(row)
            updated["group_id"] = self._resolve_merged_group_id(
                str(row.get("group_id", "")),
                merged_into,
            )
            rewritten_rows.append(updated)
        return rewritten_rows

    def _apply_llm_refine(self, groups: Dict[str, Dict[str, Any]]) -> None:
        if self._client is None:
            raise RuntimeError("sem_groupby llm_refine runtime is not initialized")
        plan = evaluate_sem_group_refine_sync(
            client=self._client,
            intent=self._resolve_intent(),
            groups=[
                {
                    "group_id": group_id,
                    "label": profile.get("label", ""),
                    "summary": profile.get("summary", ""),
                    "event_count": int(profile.get("event_count", 0)),
                    "examples": list(profile.get("examples", [])),
                }
                for group_id, profile in groups.items()
            ],
        )
        now_ms = int(time.time() * 1000)
        self._apply_llm_splits(groups, plan["splits"], now_ms)
        self._apply_llm_merges(groups, plan["merges"], now_ms)
        self._apply_llm_renames(groups, plan["renames"])

    def _apply_llm_splits(
        self,
        groups: Dict[str, Dict[str, Any]],
        splits: List[Dict[str, Any]],
        now_ms: int,
    ) -> None:
        for split in splits:
            group_id = str(split["group_id"])
            profile = groups.get(group_id)
            if profile is None:
                raise RuntimeError(
                    f"sem_groupby llm_refine split referenced missing group_id={group_id!r}"
                )
            children = list(split["children"])
            total_count = max(int(profile.get("event_count", 0)), len(children))
            source_created_ms = int(profile.get("created_ms", now_ms))
            source_examples = [str(item) for item in profile.get("examples", []) if str(item).strip()]
            groups.pop(group_id, None)
            allocated = 0
            for index, child in enumerate(children):
                child_label = str(child["label"])
                child_examples = [str(item) for item in child["examples"] if str(item).strip()]
                child_group_id = group_id if index == 0 else self._new_group_id()
                child_profile = _new_group_profile(child_group_id, child_label, now_ms)
                child_profile["created_ms"] = source_created_ms
                child_profile["last_update_ms"] = now_ms
                child_profile["examples"] = child_examples[-self._resolved_max_group_examples :]
                child_profile["summary"] = _profile_summary_from_examples(
                    child_profile["examples"],
                    max_examples=self._resolved_max_group_examples,
                )
                if index == len(children) - 1:
                    child_count = max(1, total_count - allocated)
                else:
                    proportion = len(child_examples) / max(len(source_examples), 1)
                    child_count = max(1, int(round(total_count * proportion)))
                    allocated += child_count
                child_profile["event_count"] = child_count
                groups[child_group_id] = child_profile

    def _apply_llm_merges(
        self,
        groups: Dict[str, Dict[str, Any]],
        merges: List[Dict[str, Any]],
        now_ms: int,
    ) -> None:
        for merge in merges:
            target_group_id = str(merge["target_group_id"])
            source_group_ids = [str(group_id) for group_id in merge["source_group_ids"]]
            if target_group_id not in groups:
                raise RuntimeError(
                    f"sem_groupby llm_refine merge referenced missing target_group_id={target_group_id!r}"
                )
            for source_group_id in source_group_ids:
                if source_group_id == target_group_id:
                    continue
                if source_group_id not in groups:
                    raise RuntimeError(
                        f"sem_groupby llm_refine merge referenced missing source_group_id={source_group_id!r}"
                    )
                apply_group_merge(
                    groups,
                    target_group_id,
                    source_group_id,
                    now_ms,
                    max_examples=self._resolved_max_group_examples,
                )
            label = str(merge.get("label", "") or "")
            if label:
                groups[target_group_id]["label"] = label

    def _apply_llm_renames(
        self,
        groups: Dict[str, Dict[str, Any]],
        renames: List[Dict[str, Any]],
    ) -> None:
        for rename in renames:
            group_id = str(rename["group_id"])
            if group_id not in groups:
                raise RuntimeError(
                    f"sem_groupby llm_refine rename referenced missing group_id={group_id!r}"
                )
            groups[group_id]["label"] = str(rename["label"])

    def _local_assign(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
    ) -> Tuple[Optional[str], float]:
        best_group_id: Optional[str] = None
        best_score = 0.0
        for group_id, profile in groups.items():
            score = score_group_profile(
                event.payload,
                profile,
                variant=self._resolved_variant,
                encoder=self._encoder,
            )
            if score > best_score:
                best_group_id = group_id
                best_score = score
        return best_group_id, best_score

    def _update_group(
        self,
        groups: Dict[str, Dict[str, Any]],
        group_id: str,
        event: SemEvent,
        now_ms: int,
    ) -> None:
        profile = groups.get(group_id)
        if profile is None:
            raise RuntimeError(f"sem_groupby update referenced missing group_id={group_id!r}")
        profile["event_count"] = int(profile.get("event_count", 0)) + 1
        profile["last_update_ms"] = now_ms
        _append_profile_example(
            profile,
            event.payload,
            max_examples=self._resolved_max_group_examples,
        )

    def _maybe_create_group(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
        now_ms: int,
        *,
        label: Optional[str] = None,
    ) -> Optional[str]:
        if len(groups) >= self._resolved_max_groups_per_key:
            policy = self._config.overflow_policy
            if policy.name == "DROP_OLDEST":
                oldest_group_id = min(
                    groups.items(),
                    key=lambda item: int(item[1].get("last_update_ms", 0)),
                )[0]
                groups.pop(oldest_group_id, None)
            elif policy.name == "DROP_NEWEST":
                return None

        group_id = uuid.uuid4().hex[:GROUPBY_ID_HEX_CHARS]
        profile = _new_group_profile(
            group_id,
            label or self._derive_local_group_label(event.payload),
            now_ms,
        )
        profile["event_count"] = 1
        _append_profile_example(
            profile,
            event.payload,
            max_examples=self._resolved_max_group_examples,
        )
        groups[group_id] = profile
        return group_id

    def _create_group_or_raise(
        self,
        groups: Dict[str, Dict[str, Any]],
        event: SemEvent,
        now_ms: int,
        *,
        label: Optional[str] = None,
    ) -> str:
        group_id = self._maybe_create_group(groups, event, now_ms, label=label)
        if group_id is None:
            raise RuntimeError(
                "sem_groupby window-owned path could not create a new group under the current overflow policy."
            )
        return group_id

    def _derive_local_group_label(self, payload: str) -> str:
        return " ".join(str(payload).split()[:GROUPBY_DERIVED_LABEL_TOKEN_LIMIT]).strip()

    def _resolve_intent(self) -> str:
        if self._query_spec is None:
            raise RuntimeError("sem_groupby LLM runtime requires query_spec semantic intent")
        return str(self._query_spec.semantic.instruction)

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
            "confidence": float(confidence),
            "source": source,
            "event_seq_id": event.seq_id,
            "payload": event.payload,
            "event_time_ms": event.event_time_ms,
            "metadata": dict(event.metadata),
            "boundary_flags": dict(event.boundary_flags),
        }

    @staticmethod
    def _resolve_merged_group_id(group_id: str, merged_into: Dict[str, str]) -> str:
        current = group_id
        seen = set()
        while current in merged_into and current not in seen:
            seen.add(current)
            current = merged_into[current]
        return current

    @staticmethod
    def _new_group_id() -> str:
        return uuid.uuid4().hex[:GROUPBY_ID_HEX_CHARS]
