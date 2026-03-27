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
keyword overlap or embedding similarity assign synchronously. LLM-backed
variants keep one canonical keyed-state owner; persistent paths may dispatch
LLM calls asynchronously and apply results serially in the owner.
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListState, MapState, ValueState

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    sem_groupby_pending_events_descriptor,
    sem_groupby_profiles_descriptor,
    sem_groupby_scope_progress_descriptor,
    sem_window_meta_descriptor,
    build_ttl_config,
)
from pyflink.semantic_runtime.runtime.event_model import (
    SemEvent,
    is_window_snapshot,
    window_snapshot_to_sem_events,
)
from pyflink.semantic_runtime.runtime.steps import (
    evaluate_sem_group_assignment_chunks_sync,
    evaluate_sem_group_assignments_sync,
    evaluate_sem_group_refine_sync,
)
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.runtime.stateful_async_primitives import (
    AsyncApplyGuard,
    AsyncRequestBasis,
    single_flight_begin,
    single_flight_complete,
    single_flight_get_basis,
    single_flight_is_in_flight,
)
from pyflink.semantic_runtime.runtime.stateful_async_executor import (
    ensure_thread_pool_executor,
)
from pyflink.semantic_runtime.sem_spec import GroupbyQuerySpec
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder

logger = logging.getLogger(__name__)

_LOCAL_GROUPBY_VARIANTS = {"rule", "embedding"}
_LLM_GROUPBY_VARIANTS = {"llm_basic", "llm_refine"}
_VALID_GROUPBY_VARIANTS = _LOCAL_GROUPBY_VARIANTS | _LLM_GROUPBY_VARIANTS
_VALID_GROUPBY_PERSISTENCE_POLICIES = {
    "reset_per_scope",
    "persistent_across_scopes",
}
DEFAULT_GROUPBY_MAX_GROUPS_PER_KEY = 50
DEFAULT_GROUPBY_ASSIGNMENT_BATCH_SIZE = 1
DEFAULT_GROUPBY_CONFIDENCE_THRESHOLD = 0.7
DEFAULT_GROUPBY_TTL_SECONDS = 3600
DEFAULT_GROUPBY_EVICT_INTERVAL_MS = 60_000
DEFAULT_GROUPBY_NEW_GROUP_CREATION_THRESHOLD = 0.3
DEFAULT_GROUPBY_MAX_GROUP_EXAMPLES = 8
DEFAULT_GROUPBY_RULE_SPLIT_SEED_SIMILARITY_THRESHOLD = 0.2
DEFAULT_GROUPBY_EMBEDDING_SPLIT_SEED_SIMILARITY_THRESHOLD = 0.5
DEFAULT_GROUPBY_ASYNC_MAX_WORKERS = 20
DEFAULT_GROUPBY_ASYNC_POLL_INTERVAL_MS = 200
GROUPBY_MERGE_THRESHOLD_EMBEDDING_FLOOR = 0.8
GROUPBY_MERGE_THRESHOLD_GENERIC_FLOOR = 0.65
GROUPBY_MIN_EXAMPLES_FOR_SPLIT = 4
GROUPBY_LOCAL_ENCODER_DIM = 128
GROUPBY_DERIVED_LABEL_TOKEN_LIMIT = 5
GROUPBY_ID_HEX_CHARS = 8
GROUPBY_META_ASYNC_POLL_DUE_MS = "_async_poll_due_ms"
GROUPBY_META_MAINTENANCE_DUE_MS = "_maintenance_due_ms"


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
# Async client pool
# ---------------------------------------------------------------------------


class _SemGroupbyLLMClientPool:
    """Thread-local LLM client pool for sem_groupby async workers."""

    def __init__(self, llm_config: LLMClientConfig) -> None:
        self._llm_config = llm_config
        self._local = threading.local()
        self._lock = threading.Lock()
        self._clients: Dict[int, LLMClient] = {}

    def _get_client(self) -> LLMClient:
        client = getattr(self._local, "client", None)
        if client is not None:
            return client
        created = create_llm_client(self._llm_config)
        self._local.client = created
        with self._lock:
            self._clients[threading.get_ident()] = created
        return created

    def evaluate_assignments(
        self,
        *,
        intent: str,
        existing_groups: List[Dict[str, Any]],
        event_chunks: List[List[Dict[str, Any]]],
    ) -> List[List[Dict[str, Any]]]:
        """Evaluate assignment chunks with the worker-thread client."""
        client = self._get_client()
        if len(event_chunks) == 1:
            return [
                evaluate_sem_group_assignments_sync(
                    client=client,
                    intent=intent,
                    existing_groups=existing_groups,
                    events=event_chunks[0],
                )
            ]
        return evaluate_sem_group_assignment_chunks_sync(
            client=client,
            intent=intent,
            existing_groups=existing_groups,
            event_chunks=event_chunks,
        )

    def evaluate_refine(
        self,
        *,
        intent: str,
        groups: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Evaluate one semantic refine pass with the worker-thread client."""
        client = self._get_client()
        return evaluate_sem_group_refine_sync(
            client=client,
            intent=intent,
            groups=groups,
        )

    def close(self) -> None:
        """Close all materialized worker clients."""
        with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for client in clients:
            client.close()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SemGroupbyConfig:
    """Internal configuration for the semantic groupby operator."""

    max_groups_per_key: int = DEFAULT_GROUPBY_MAX_GROUPS_PER_KEY
    variant: str = "llm_basic"
    persistence_policy: Optional[str] = None
    assignment_batch_size: int = DEFAULT_GROUPBY_ASSIGNMENT_BATCH_SIZE
    confidence_threshold: float = DEFAULT_GROUPBY_CONFIDENCE_THRESHOLD
    ttl_seconds: int = DEFAULT_GROUPBY_TTL_SECONDS
    evict_interval_ms: int = DEFAULT_GROUPBY_EVICT_INTERVAL_MS
    new_group_creation_threshold: float = DEFAULT_GROUPBY_NEW_GROUP_CREATION_THRESHOLD
    refresh_labels_during_maintenance: bool = False
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST
    max_group_examples: int = DEFAULT_GROUPBY_MAX_GROUP_EXAMPLES
    local_rule_split_seed_similarity_threshold: float = (
        DEFAULT_GROUPBY_RULE_SPLIT_SEED_SIMILARITY_THRESHOLD
    )
    local_embedding_split_seed_similarity_threshold: float = (
        DEFAULT_GROUPBY_EMBEDDING_SPLIT_SEED_SIMILARITY_THRESHOLD
    )
    async_max_workers: int = DEFAULT_GROUPBY_ASYNC_MAX_WORKERS
    async_poll_interval_ms: int = DEFAULT_GROUPBY_ASYNC_POLL_INTERVAL_MS

    def __post_init__(self) -> None:
        if self.variant not in _VALID_GROUPBY_VARIANTS:
            raise ValueError(
                f"Invalid internal groupby variant={self.variant!r}. "
                f"Must be one of {_VALID_GROUPBY_VARIANTS}."
            )
        if (
            self.persistence_policy is not None
            and self.persistence_policy not in _VALID_GROUPBY_PERSISTENCE_POLICIES
        ):
            raise ValueError(
                f"Invalid sem_groupby persistence_policy={self.persistence_policy!r}. "
                f"Must be one of {_VALID_GROUPBY_PERSISTENCE_POLICIES}."
            )
        if self.assignment_batch_size <= 0:
            raise ValueError("assignment_batch_size must be a positive integer.")
        if self.max_group_examples <= 0:
            raise ValueError("max_group_examples must be a positive integer.")
        if self.async_max_workers <= 0:
            raise ValueError("async_max_workers must be a positive integer.")
        if self.async_poll_interval_ms <= 0:
            raise ValueError("async_poll_interval_ms must be a positive integer.")


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
        "examples": [],
    }


def resolve_groupby_variant(
    config: SemGroupbyConfig,
    query_spec: Optional[GroupbyQuerySpec] = None,
) -> str:
    """Resolve the internal grouping variant."""
    _ = query_spec
    return str(config.variant or "llm_basic")


def resolve_groupby_persistence_policy(
    config: SemGroupbyConfig,
    *,
    scope_source: str,
) -> str:
    """Resolve group-state persistence independently from scope source."""
    _ = scope_source
    if config.persistence_policy is not None:
        policy = str(config.persistence_policy)
        if policy not in _VALID_GROUPBY_PERSISTENCE_POLICIES:
            raise ValueError(
                f"Invalid sem_groupby persistence_policy={policy!r}. "
                f"Must be one of {_VALID_GROUPBY_PERSISTENCE_POLICIES}."
            )
        return policy
    return "persistent_across_scopes"


def _group_profile_text(profile: Dict[str, Any]) -> str:
    label = str(profile.get("label", "") or "")
    summary = str(profile.get("summary", "") or "")
    return f"{label}\n{summary}".strip()


def _profile_summary_from_examples(
    examples: Iterable[str],
    *,
    max_examples: int,
) -> str:
    """Build a compact local summary from retained examples."""
    normalized = [str(item).strip() for item in examples if str(item).strip()]
    return "\n".join(normalized[-max_examples:])


def _append_profile_example(
    profile: Dict[str, Any],
    payload: str,
    *,
    max_examples: int,
) -> None:
    """Append one payload example and refresh the local summary."""
    cleaned = str(payload).strip()
    if not cleaned:
        return
    examples = [str(item) for item in profile.get("examples", []) if str(item).strip()]
    if cleaned not in examples:
        examples.append(cleaned)
    profile["examples"] = examples[-max_examples:]
    profile["summary"] = _profile_summary_from_examples(
        profile["examples"],
        max_examples=max_examples,
    )


def derive_group_label(profile: Dict[str, Any]) -> str:
    """Derive a compact local label from a group profile.

    This is a local relabel helper used by maintenance when label refresh is
    enabled.
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
    variant: str,
    encoder: Optional[HashingTextEncoder] = None,
) -> float:
    """Score an event against one group profile using the chosen local method."""
    profile_text = _group_profile_text(profile)
    if not profile_text:
        return 0.0
    if variant == "embedding":
        local_encoder = encoder or HashingTextEncoder()
        return float(local_encoder.similarity(event_text, profile_text))
    return float(_keyword_overlap_score(event_text, profile_text))


def group_profile_similarity(
    left_profile: Dict[str, Any],
    right_profile: Dict[str, Any],
    *,
    variant: str,
    encoder: Optional[HashingTextEncoder] = None,
) -> float:
    """Return a symmetric similarity score between two group profiles."""
    left_text = _group_profile_text(left_profile)
    right_text = _group_profile_text(right_profile)
    if not left_text or not right_text:
        return 0.0
    if variant == "embedding":
        local_encoder = encoder or HashingTextEncoder()
        return float(local_encoder.similarity(left_text, right_text))
    left_to_right = _keyword_overlap_score(left_text, right_text)
    right_to_left = _keyword_overlap_score(right_text, left_text)
    return float((left_to_right + right_to_left) / 2.0)


def resolve_groupby_maintenance_merge_threshold(
    *,
    variant: str,
    assign_threshold: float,
    new_group_threshold: float,
) -> float:
    """Return the local similarity threshold used by maintenance/refinement."""
    if variant == "embedding":
        return max(GROUPBY_MERGE_THRESHOLD_EMBEDDING_FLOOR, float(assign_threshold))
    return max(GROUPBY_MERGE_THRESHOLD_GENERIC_FLOOR, float(new_group_threshold))


def merge_similar_group_profiles(
    groups: Dict[str, Dict[str, Any]],
    *,
    variant: str,
    encoder: Optional[HashingTextEncoder],
    assign_threshold: float,
    new_group_threshold: float,
    now_ms: int,
    max_examples: int,
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
        variant=variant,
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
                variant=variant,
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
        apply_group_merge(
            working,
            survivor_id,
            merged_id,
            now_ms,
            max_examples=max_examples,
        )
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
    *,
    max_examples: int,
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

    merged_examples = list(survivor.get("examples", []))
    for example in list(merged.get("examples", [])):
        if example not in merged_examples:
            merged_examples.append(example)
    survivor["examples"] = merged_examples[-max_examples:]
    survivor["summary"] = _profile_summary_from_examples(
        survivor["examples"],
        max_examples=max_examples,
    )


def _split_profile_examples(
    profile: Dict[str, Any],
    *,
    variant: str,
    encoder: Optional[HashingTextEncoder],
    rule_threshold: float,
    embedding_threshold: float,
) -> Optional[Tuple[list[str], list[str]]]:
    """Split one profile's examples into two semantic clusters when possible."""
    examples = [str(item) for item in profile.get("examples", []) if str(item).strip()]
    if len(examples) < GROUPBY_MIN_EXAMPLES_FOR_SPLIT:
        return None

    seed_left_text = examples[0]
    seed_right_text = None
    lowest_seed_similarity = 1.0
    for candidate in examples[1:]:
        score = score_group_profile(
            candidate,
            {"label": seed_left_text, "summary": seed_left_text},
            variant=variant,
            encoder=encoder,
        )
        if score < lowest_seed_similarity:
            seed_right_text = candidate
            lowest_seed_similarity = score
    if seed_right_text is None:
        return None

    split_similarity_threshold = (
        embedding_threshold if variant == "embedding" else rule_threshold
    )
    if lowest_seed_similarity > split_similarity_threshold:
        return None

    left_cluster = [seed_left_text]
    right_cluster = [seed_right_text]
    for sample in examples[1:]:
        if sample == seed_right_text:
            continue
        left_similarity = score_group_profile(
            sample,
            {"label": seed_left_text, "summary": seed_left_text},
            variant=variant,
            encoder=encoder,
        )
        right_similarity = score_group_profile(
            sample,
            {"label": seed_right_text, "summary": seed_right_text},
            variant=variant,
            encoder=encoder,
        )
        if right_similarity > left_similarity:
            right_cluster.append(sample)
        else:
            left_cluster.append(sample)
    if not left_cluster or not right_cluster:
        return None
    return left_cluster, right_cluster


def split_group_profiles(
    groups: Dict[str, Dict[str, Any]],
    *,
    variant: str,
    encoder: Optional[HashingTextEncoder],
    now_ms: int,
    max_examples: int,
    rule_threshold: float,
    embedding_threshold: float,
) -> Tuple[Dict[str, Dict[str, Any]], int]:
    """Split semantically mixed groups using retained example payloads."""
    working = {gid: dict(profile) for gid, profile in groups.items()}
    split_count = 0
    next_suffix = 0

    for group_id in list(working.keys()):
        profile = working.get(group_id)
        if profile is None:
            continue
        split = _split_profile_examples(
            profile,
            variant=variant,
            encoder=encoder,
            rule_threshold=rule_threshold,
            embedding_threshold=embedding_threshold,
        )
        if split is None:
            continue
        cluster_a, cluster_b = split
        next_suffix += 1
        new_group_id = f"{group_id}_split{next_suffix}"

        left = dict(profile)
        right = dict(profile)
        left["examples"] = cluster_a[-max_examples:]
        right["examples"] = cluster_b[-max_examples:]
        left["summary"] = _profile_summary_from_examples(left["examples"], max_examples=max_examples)
        right["summary"] = _profile_summary_from_examples(right["examples"], max_examples=max_examples)
        left["label"] = derive_group_label(left)
        right["label"] = derive_group_label(right)
        total = max(int(profile.get("event_count", len(cluster_a) + len(cluster_b))), 2)
        left_count = max(1, int(round(total * (len(cluster_a) / (len(cluster_a) + len(cluster_b))))))
        right_count = max(1, total - left_count)
        left["event_count"] = left_count
        right["event_count"] = right_count
        left["last_update_ms"] = now_ms
        right["last_update_ms"] = now_ms
        right["group_id"] = new_group_id
        working[group_id] = left
        working[new_group_id] = right
        split_count += 1

    return working, split_count


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
    """

    def __init__(
        self,
        config: Optional[SemGroupbyConfig] = None,
        query_spec: Optional[GroupbyQuerySpec] = None,
        *,
        llm_config: Optional[LLMClientConfig] = None,
        scope_source: str = "internal_scope",
    ) -> None:
        self._config = config or SemGroupbyConfig()
        self._query_spec = query_spec
        self._llm_config = llm_config
        self._scope_source = scope_source
        self._group_profiles: Optional[MapState] = None
        self._pending_events: Optional[ListState] = None
        self._scope_progress: Optional[MapState] = None
        self._pending_events_buffer: List[Dict[str, Any]] = []
        self._meta: Optional[ValueState] = None
        self._metrics: Optional[StatefulOperatorMetrics] = None
        self._client: Optional[LLMClient] = None
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._llm_client_pool: Optional[_SemGroupbyLLMClientPool] = None
        self._pending_llm_futures: Dict[str, concurrent.futures.Future] = {}
        self._pending_llm_payloads: Dict[str, Dict[str, Any]] = {}
        self._resolved_variant = resolve_groupby_variant(
            self._config,
            query_spec,
        )
        if self._resolved_variant in _LLM_GROUPBY_VARIANTS and query_spec is None:
            raise ValueError(
                f"sem_groupby variant={self._resolved_variant!r} requires query_spec"
            )
        self._resolved_persistence_policy = resolve_groupby_persistence_policy(
            self._config,
            scope_source=self._scope_source,
        )
        self._maintenance_trigger_policy = (
            query_spec.maintenance_trigger_policy if query_spec is not None else None
        )
        self._scope_runtime = (
            _GroupbyScopeRuntime(query_spec)
            if (
                self._scope_source == "internal_scope"
                and query_spec is not None
                and self._maintenance_trigger_policy is not None
                and self._maintenance_trigger_policy.mode == "on_scope_close"
            )
            else None
        )
        self._encoder = HashingTextEncoder(dim=GROUPBY_LOCAL_ENCODER_DIM)
        (
            self._resolved_ttl_seconds,
            self._resolved_max_groups_per_key,
            self._resolved_assign_threshold,
            self._resolved_new_group_threshold,
        ) = resolve_groupby_runtime_params(self._config, self._query_spec)
        self._resolved_assignment_batch_size = int(self._config.assignment_batch_size)
        self._resolved_max_group_examples = int(self._config.max_group_examples)
        self._resolved_async_poll_interval_ms = int(self._config.async_poll_interval_ms)
        self._resolved_async_max_workers = int(self._config.async_max_workers)
        self._enable_async_llm_runtime = (
            self._resolved_variant in _LLM_GROUPBY_VARIANTS
            and self._resolved_persistence_policy == "persistent_across_scopes"
            and self._llm_config is not None
            and not (
                self._maintenance_trigger_policy is not None
                and self._maintenance_trigger_policy.mode == "on_scope_close"
            )
        )

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl = self._resolved_ttl_seconds
        self._group_profiles = runtime_context.get_map_state(
            sem_groupby_profiles_descriptor(ttl)
        )
        self._pending_events = runtime_context.get_list_state(
            sem_groupby_pending_events_descriptor(ttl)
        )
        self._scope_progress = runtime_context.get_map_state(
            sem_groupby_scope_progress_descriptor(ttl)
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
        if self._resolved_variant in _LLM_GROUPBY_VARIANTS:
            if self._llm_config is None:
                raise ValueError(
                    f"sem_groupby variant={self._resolved_variant!r} requires llm_config"
                )
            if self._enable_async_llm_runtime:
                self._executor = ensure_thread_pool_executor(
                    self._executor,
                    max_workers=self._resolved_async_max_workers,
                    thread_name_prefix="sem-groupby",
                )
                self._llm_client_pool = _SemGroupbyLLMClientPool(self._llm_config)
            else:
                self._client = create_llm_client(self._llm_config)
        logger.info(
            "SemGroupbyFunction opened (max_groups=%d, reuse_threshold=%.2f, maintenance_merge_threshold=%.2f, variant=%s, persistence=%s)",
            self._resolved_max_groups_per_key,
            self._resolved_assign_threshold,
            self._resolved_new_group_threshold,
            self._resolved_variant,
            self._resolved_persistence_policy,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None
        if self._llm_client_pool is not None:
            self._llm_client_pool.close()
            self._llm_client_pool = None
        self._pending_llm_futures.clear()
        self._pending_llm_payloads.clear()

    # -- core ----------------------------------------------------------------

    def process_element(
        self,
        value: Any,
        ctx: "KeyedProcessFunction.Context",
    ) -> Iterable[Any]:
        """Process one incoming event."""
        now_ms = int(time.time() * 1000)
        if self._metrics:
            self._metrics.record_event_processed()
        meta = self._meta.value()
        if meta is not None and self._enable_async_llm_runtime:
            yield from self._poll_pending_llm(meta, ctx, now_ms)

        # Detect WindowSnapshot input → expand into individual events
        if isinstance(value, dict) and is_window_snapshot(value):
            yield from self._process_window_snapshot(value, ctx, now_ms)
            return

        # Single event path
        yield from self._process_single_event(value, ctx, now_ms)

    def _process_window_snapshot(
        self,
        snapshot: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> Iterable[Any]:
        """Process one external cumulative window snapshot."""
        meta = self._meta.value() or {
            "total_assigned": 0,
            "key": str(ctx.get_current_key()),
            "scope_epoch": 0,
            "scope_last_time_ms": now_ms,
            "scope_bucket_id": None,
        }
        self._maybe_initialize_maintenance_due(meta, now_ms)
        scope_events = list(window_snapshot_to_sem_events(snapshot))
        if self._resolved_persistence_policy == "reset_per_scope":
            if self._resolved_variant in _LOCAL_GROUPBY_VARIANTS:
                for sub_event_dict in scope_events:
                    yield from self._process_single_event(sub_event_dict, ctx, now_ms)
            else:
                yield from self._assign_with_llm_events(scope_events, now_ms, meta=meta)
            return

        window_id = str(snapshot.get("window_id", "") or "")
        if not window_id:
            raise ValueError("sem_groupby external_window path requires non-empty window_id")

        progress = self._scope_progress.get(window_id) if self._scope_progress is not None else None
        seen_seq_ids = {
            int(seq_id)
            for seq_id in (progress or {}).get("seen_event_seq_ids", [])
        }
        unseen_scope_events: List[Dict[str, Any]] = []
        newly_seen_seq_ids: List[int] = []
        for sub_event_dict in scope_events:
            seq_id = int(sub_event_dict.get("seq_id", 0))
            if seq_id in seen_seq_ids:
                continue
            unseen_scope_events.append(sub_event_dict)
            seen_seq_ids.add(seq_id)
            newly_seen_seq_ids.append(seq_id)

        if self._resolved_variant in _LOCAL_GROUPBY_VARIANTS:
            for sub_event_dict in unseen_scope_events:
                yield from self._process_single_event(sub_event_dict, ctx, now_ms)
        else:
            if self._enable_async_llm_runtime:
                for sub_event_dict in unseen_scope_events:
                    event = SemEvent.from_dict(sub_event_dict)
                    yield from self._enqueue_llm_assignment(
                        event_dict=sub_event_dict,
                        event=event,
                        meta=meta,
                        now_ms=now_ms,
                        ctx=ctx,
                    )
            else:
                yield from self._assign_with_llm_events(unseen_scope_events, now_ms, meta=meta)

        if self._scope_progress is not None:
            self._scope_progress.put(
                window_id,
                {
                    "seen_event_seq_ids": sorted(seen_seq_ids),
                    "last_update_ms": now_ms,
                    "newly_seen_event_seq_ids": newly_seen_seq_ids,
                },
            )
        if (
            self._maintenance_trigger_policy is not None
            and self._maintenance_trigger_policy.mode == "on_scope_close"
        ):
            meta["scope_bucket_id"] = window_id
            self._run_maintenance(meta, now_ms)
            self._meta.update(meta)

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
            self._maybe_initialize_maintenance_due(meta, now_ms)
            if self._enable_async_llm_runtime:
                self._schedule_recompute_timer(meta, ctx.timer_service())
            else:
                register_timer(
                    ctx.timer_service(),
                    meta,
                    TimerCategory.RECOMPUTE,
                    now_ms + int(self._maintenance_trigger_policy.interval_ms),
                )

        meta["scope_last_time_ms"] = decision.event_time_ms
        if decision.scope_bucket_id is not None:
            meta["scope_bucket_id"] = decision.scope_bucket_id

        if self._resolved_variant in _LOCAL_GROUPBY_VARIANTS:
            assignment_row = self._assign_locally(event, now_ms)
            meta["total_assigned"] = int(meta.get("total_assigned", 0)) + 1
            self._meta.update(meta)
            yield assignment_row
        elif self._resolved_variant in _LLM_GROUPBY_VARIANTS:
            if self._enable_async_llm_runtime:
                yield from self._enqueue_llm_assignment(
                    event_dict=event_dict,
                    event=event,
                    meta=meta,
                    now_ms=now_ms,
                    ctx=ctx,
                )
            else:
                yield from self._enqueue_llm_assignment_sync(
                    event_dict=event_dict,
                    event=event,
                    meta=meta,
                    now_ms=now_ms,
                )
        else:
            raise ValueError(
                f"Unsupported internal groupby variant={self._resolved_variant!r}."
            )

        if (
            self._scope_source == "internal_scope"
            and self._maintenance_trigger_policy is not None
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
            if self._enable_async_llm_runtime:
                outputs.extend(self._on_recompute_timer_async(meta, ctx, timestamp))
                self._meta.update(meta)
                return outputs
            self._run_maintenance(meta, timestamp)
            if self._maintenance_trigger_policy is not None and self._maintenance_trigger_policy.mode == "periodic":
                meta[GROUPBY_META_MAINTENANCE_DUE_MS] = (
                    int(time.time() * 1000) + int(self._maintenance_trigger_policy.interval_ms)
                )
                self._schedule_recompute_timer(meta, ctx.timer_service())
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
                variant=self._resolved_variant,
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
        """Append one event to the pending assignment batch and return the batch."""
        pending = self._pending_event_values()
        pending.append(dict(event_dict))
        self._replace_pending_events(pending)
        return pending

    def _enqueue_llm_assignment(
        self,
        *,
        event_dict: Dict[str, Any],
        event: SemEvent,
        meta: Dict[str, Any],
        now_ms: int,
        ctx: "KeyedProcessFunction.Context",
    ) -> Iterable[Any]:
        """Append one event to async LLM assignment queue and dispatch if ready."""
        _ = event
        pending = self._append_pending_event(event_dict)
        self._maybe_initialize_maintenance_due(meta, now_ms)
        self._meta.update(meta)
        if len(pending) < self._resolved_assignment_batch_size:
            return
        yield from self._maybe_dispatch_assignment(meta, ctx, now_ms)

    def _enqueue_llm_assignment_sync(
        self,
        *,
        event_dict: Dict[str, Any],
        event: SemEvent,
        meta: Dict[str, Any],
        now_ms: int,
    ) -> Iterable[Any]:
        """Append one event to synchronous LLM assignment batch and flush when full."""
        _ = event
        pending = self._append_pending_event(event_dict)
        self._maybe_initialize_maintenance_due(meta, now_ms)
        self._meta.update(meta)
        if len(pending) < self._resolved_assignment_batch_size:
            return
        chunk = list(pending)
        self._clear_pending_events()
        yield from self._assign_with_llm_events(chunk, now_ms, meta=meta)

    def _existing_groups_payload(self) -> List[Dict[str, Any]]:
        """Return a serializable view of current groups for semantic assignment."""
        return [
            {
                "group_id": group_id,
                "label": profile.get("label", ""),
                "summary": profile.get("summary", ""),
                "event_count": int(profile.get("event_count", 0)),
                "examples": list(profile.get("examples", [])),
            }
            for group_id in self._group_profiles.keys()
            for profile in [self._group_profiles.get(group_id)]
            if profile is not None
        ]

    def _derive_local_group_label(self, payload: str) -> str:
        """Derive a local label for one newly created group."""
        return " ".join(str(payload).split()[:GROUPBY_DERIVED_LABEL_TOKEN_LIMIT]).strip()

    def _split_assignment_chunks(
        self,
        event_dicts: List[Dict[str, Any]],
    ) -> List[List[Dict[str, Any]]]:
        """Split one event list into canonical assignment chunks."""
        chunk_size = self._resolved_assignment_batch_size
        return [
            list(event_dicts[index:index + chunk_size])
            for index in range(0, len(event_dicts), chunk_size)
        ]

    def _apply_assignment_rows(
        self,
        *,
        event_dicts: List[Dict[str, Any]],
        assignments: List[Dict[str, Any]],
        now_ms: int,
    ) -> Iterable[Dict[str, Any]]:
        """Apply one semantic assignment result list to canonical state."""
        events_by_seq_id = {
            int(item["seq_id"]): SemEvent.from_dict(item)
            for item in event_dicts
        }
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
                self._update_group(group_id, event, now_ms)
                yield self._assignment_row(event, group_id, confidence, self._resolved_variant)
                continue
            label = str(assignment["label"])
            group_id = self._create_group_or_raise(event, now_ms, label=label)
            yield self._assignment_row(event, group_id, confidence, self._resolved_variant)
        if seen_seq_ids != set(events_by_seq_id.keys()):
            missing = sorted(set(events_by_seq_id.keys()) - seen_seq_ids)
            raise ValueError(
                f"sem_group_assign did not return assignments for event_seq_ids={missing!r}"
            )

    def _assign_with_llm_events(
        self,
        event_dicts: List[Dict[str, Any]],
        now_ms: int,
        *,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Assign one event list through canonical semantic grouping."""
        if self._client is None:
            raise RuntimeError("sem_groupby LLM runtime is not initialized")
        if not event_dicts:
            return
        request_basis: Optional[AsyncRequestBasis] = None
        if meta is not None:
            request_basis = self._begin_llm_lane(
                meta=meta,
                lane="assignment",
                now_ms=now_ms,
                trigger_reason="assignment",
            )
        existing_groups = self._existing_groups_payload()
        event_chunks = self._split_assignment_chunks(event_dicts)
        intent = self._query_spec.semantic.instruction
        try:
            if len(event_chunks) == 1:
                assignments = evaluate_sem_group_assignments_sync(
                    client=self._client,
                    intent=intent,
                    existing_groups=existing_groups,
                    events=event_chunks[0],
                )
                yield from self._apply_assignment_rows(
                    event_dicts=event_chunks[0],
                    assignments=assignments,
                    now_ms=now_ms,
                )
                if meta is not None:
                    meta["total_assigned"] = int(meta.get("total_assigned", 0)) + len(event_chunks[0])
                    self._meta.update(meta)
                return

            assignment_chunks = evaluate_sem_group_assignment_chunks_sync(
                client=self._client,
                intent=intent,
                existing_groups=existing_groups,
                event_chunks=event_chunks,
            )
            for event_chunk, assignment_chunk in zip(event_chunks, assignment_chunks):
                yield from self._apply_assignment_rows(
                    event_dicts=event_chunk,
                    assignments=assignment_chunk,
                    now_ms=now_ms,
                )
            if meta is not None:
                meta["total_assigned"] = int(meta.get("total_assigned", 0)) + len(event_dicts)
                self._meta.update(meta)
        finally:
            if meta is not None and request_basis is not None:
                single_flight_complete(meta, "assignment", request_basis.request_id)
                self._meta.update(meta)

    def _evaluate_assignment_request(
        self,
        *,
        intent: str,
        existing_groups: List[Dict[str, Any]],
        event_chunks: List[List[Dict[str, Any]]],
    ) -> List[List[Dict[str, Any]]]:
        """Worker task: evaluate assignment chunks."""
        if self._llm_client_pool is not None:
            return self._llm_client_pool.evaluate_assignments(
                intent=intent,
                existing_groups=existing_groups,
                event_chunks=event_chunks,
            )
        if self._client is not None:
            if len(event_chunks) == 1:
                return [
                    evaluate_sem_group_assignments_sync(
                        client=self._client,
                        intent=intent,
                        existing_groups=existing_groups,
                        events=event_chunks[0],
                    )
                ]
            return evaluate_sem_group_assignment_chunks_sync(
                client=self._client,
                intent=intent,
                existing_groups=existing_groups,
                event_chunks=event_chunks,
            )
        raise RuntimeError("sem_groupby assignment worker has no LLM runtime")

    def _evaluate_refine_request(
        self,
        *,
        intent: str,
        groups: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Worker task: evaluate semantic refinement."""
        if self._llm_client_pool is not None:
            return self._llm_client_pool.evaluate_refine(
                intent=intent,
                groups=groups,
            )
        if self._client is not None:
            return evaluate_sem_group_refine_sync(
                client=self._client,
                intent=intent,
                groups=groups,
            )
        raise RuntimeError("sem_groupby refine worker has no LLM runtime")

    def _maybe_dispatch_assignment(
        self,
        meta: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
    ) -> Iterable[Dict[str, Any]]:
        """Dispatch pending assignment chunk when lane is free."""
        outputs: List[Dict[str, Any]] = []
        while True:
            if single_flight_is_in_flight(meta, "assignment") or single_flight_is_in_flight(meta, "refine"):
                break
            pending = self._pending_event_values()
            if len(pending) < self._resolved_assignment_batch_size:
                break
            chunk = list(pending[: self._resolved_assignment_batch_size])
            self._replace_pending_events(list(pending[self._resolved_assignment_batch_size :]))
            outputs.extend(self._dispatch_assignment_request(meta, ctx, now_ms, chunk))
            if single_flight_is_in_flight(meta, "assignment"):
                break
        return outputs

    def _dispatch_assignment_request(
        self,
        meta: Dict[str, Any],
        ctx: "KeyedProcessFunction.Context",
        now_ms: int,
        event_dicts: List[Dict[str, Any]],
    ) -> Iterable[Dict[str, Any]]:
        """Dispatch one async assignment request."""
        if self._executor is None:
            self._executor = ensure_thread_pool_executor(
                self._executor,
                max_workers=self._resolved_async_max_workers,
                thread_name_prefix="sem-groupby",
            )
        if not event_dicts:
            return []
        request_basis = self._begin_llm_lane(
            meta=meta,
            lane="assignment",
            now_ms=now_ms,
            trigger_reason="assignment",
        )
        event_chunks = self._split_assignment_chunks(event_dicts)
        future = self._executor.submit(
            self._evaluate_assignment_request,
            intent=self._query_spec.semantic.instruction,
            existing_groups=self._existing_groups_payload(),
            event_chunks=event_chunks,
        )
        self._pending_llm_futures[request_basis.request_id] = future
        self._pending_llm_payloads[request_basis.request_id] = {
            "lane": "assignment",
            "event_chunks": event_chunks,
            "event_count": sum(len(chunk) for chunk in event_chunks),
        }
        self._set_async_poll_due(meta, now_ms + self._resolved_async_poll_interval_ms)
        self._schedule_recompute_timer(meta, ctx.timer_service())
        self._meta.update(meta)
        return list(self._poll_pending_llm(meta, ctx, now_ms))

    def _dispatch_refine_request(
        self,
        meta: Dict[str, Any],
        ctx: "KeyedProcessFunction.OnTimerContext",
        now_ms: int,
    ) -> Iterable[Dict[str, Any]]:
        """Dispatch one async refine request for llm_refine."""
        if self._resolved_variant != "llm_refine":
            return []
        if self._executor is None:
            self._executor = ensure_thread_pool_executor(
                self._executor,
                max_workers=self._resolved_async_max_workers,
                thread_name_prefix="sem-groupby",
            )
        if single_flight_is_in_flight(meta, "assignment") or single_flight_is_in_flight(meta, "refine"):
            return []
        groups: List[Dict[str, Any]] = []
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is None:
                continue
            groups.append(
                {
                    "group_id": group_id,
                    "label": str(profile.get("label", "") or ""),
                    "summary": str(profile.get("summary", "") or ""),
                    "event_count": int(profile.get("event_count", 0)),
                    "examples": list(profile.get("examples", [])),
                }
            )
        if not groups:
            return []
        request_basis = self._begin_llm_lane(
            meta=meta,
            lane="refine",
            now_ms=now_ms,
            trigger_reason="maintenance_refine",
        )
        future = self._executor.submit(
            self._evaluate_refine_request,
            intent=self._query_spec.semantic.instruction,
            groups=groups,
        )
        self._pending_llm_futures[request_basis.request_id] = future
        self._pending_llm_payloads[request_basis.request_id] = {
            "lane": "refine",
        }
        if self._maintenance_trigger_policy is not None and self._maintenance_trigger_policy.mode == "periodic":
            meta[GROUPBY_META_MAINTENANCE_DUE_MS] = now_ms + int(self._maintenance_trigger_policy.interval_ms)
        self._set_async_poll_due(meta, now_ms + self._resolved_async_poll_interval_ms)
        self._schedule_recompute_timer(meta, ctx.timer_service())
        self._meta.update(meta)
        return list(self._poll_pending_llm(meta, ctx, now_ms))

    def _poll_pending_llm(
        self,
        meta: Dict[str, Any],
        ctx: Any,
        now_ms: int,
    ) -> Iterable[Dict[str, Any]]:
        """Poll one in-flight async LLM request and apply if completed."""
        basis = single_flight_get_basis(meta, "assignment")
        lane = "assignment"
        if basis is None:
            basis = single_flight_get_basis(meta, "refine")
            lane = "refine"
        if basis is None:
            self._set_async_poll_due(meta, None)
            self._meta.update(meta)
            return []
        future = self._pending_llm_futures.get(basis.request_id)
        if future is None:
            raise RuntimeError(f"sem_groupby lost pending async request {basis.request_id!r}")
        if not future.done():
            self._set_async_poll_due(meta, now_ms + self._resolved_async_poll_interval_ms)
            self._schedule_recompute_timer(meta, ctx.timer_service())
            self._meta.update(meta)
            return []

        request_basis = single_flight_complete(meta, lane, basis.request_id)
        payload = self._pending_llm_payloads.pop(basis.request_id, None)
        if payload is None:
            raise RuntimeError(f"sem_groupby lost pending async payload {basis.request_id!r}")
        del self._pending_llm_futures[basis.request_id]
        self._set_async_poll_due(meta, None)
        stale = AsyncApplyGuard.is_stale(
            basis=request_basis,
            current_scope_epoch=int(meta.get("scope_epoch", 0) or 0),
            current_state_version=int(meta.get("total_assigned", 0) or 0),
            enforce_state_version=False,
        )
        outputs: List[Dict[str, Any]] = []
        if lane == "assignment":
            assignment_chunks = future.result()
            if not stale:
                event_chunks = payload.get("event_chunks")
                if not isinstance(event_chunks, list) or len(event_chunks) != len(assignment_chunks):
                    raise RuntimeError("sem_groupby assignment async payload mismatch")
                for event_chunk, assignment_chunk in zip(event_chunks, assignment_chunks):
                    outputs.extend(
                        self._apply_assignment_rows(
                            event_dicts=list(event_chunk),
                            assignments=list(assignment_chunk),
                            now_ms=now_ms,
                        )
                    )
                meta["total_assigned"] = int(meta.get("total_assigned", 0)) + int(payload.get("event_count", 0))
            outputs.extend(self._maybe_dispatch_assignment(meta, ctx, now_ms))
        else:
            refine_plan = future.result()
            if not stale:
                split_count = self._apply_llm_splits(refine_plan["splits"], now_ms)
                merge_count = self._apply_llm_merges(refine_plan["merges"], now_ms)
                rename_count = self._apply_llm_renames(refine_plan["renames"])
                meta["last_refine_ms"] = now_ms
                meta["refine_count"] = int(meta.get("refine_count", 0)) + 1
                meta["last_split_count"] = split_count
                meta["last_merge_count"] = merge_count
                meta["last_rename_count"] = rename_count
                if self._metrics:
                    self._metrics.record_recompute()
        self._schedule_recompute_timer(meta, ctx.timer_service())
        self._meta.update(meta)
        return outputs

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
            raise RuntimeError(
                f"sem_groupby assignment referenced missing group_id={group_id!r}"
            )
        profile["event_count"] = profile.get("event_count", 0) + 1
        profile["last_update_ms"] = now_ms
        _append_profile_example(
            profile,
            event.payload,
            max_examples=self._resolved_max_group_examples,
        )
        self._group_profiles.put(group_id, profile)

    def _maybe_create_group(
        self,
        event: SemEvent,
        now_ms: int,
        *,
        label: Optional[str] = None,
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
        self._group_profiles.put(group_id, profile)
        return group_id

    def _create_group_or_raise(
        self,
        event: SemEvent,
        now_ms: int,
        *,
        label: Optional[str] = None,
    ) -> str:
        """Create a new group or fail when policy forbids it."""
        group_id = self._maybe_create_group(event, now_ms, label=label)
        if group_id is None:
            raise RuntimeError(
                "sem_groupby could not create a new group under the current overflow policy."
            )
        return group_id

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

        Rule/embedding variants use local maintenance.
        llm_refine uses a real semantic refinement pass.
        """
        rename_count = 0
        if self._resolved_variant == "llm_refine":
            split_count, merge_count, rename_count = self._refine_groups_with_llm(
                now_ms,
                meta=meta,
            )
        else:
            split_count = self._split_mixed_groups(now_ms)
            merge_count = self._merge_similar_groups(now_ms)
            if self._config.refresh_labels_during_maintenance:
                rename_count = self._refresh_group_labels()
        meta["last_refine_ms"] = now_ms
        meta["refine_count"] = int(meta.get("refine_count", 0)) + 1
        meta["last_split_count"] = split_count
        meta["last_merge_count"] = merge_count
        meta["last_rename_count"] = rename_count
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
        pending = self._pending_event_values()
        if pending:
            self._clear_pending_events()
            yield from self._assign_with_llm_events(pending, now_ms, meta=meta)
        self._run_maintenance(meta, now_ms)
        self._reset_scope_state(meta, reason=reason)
        self._meta.update(meta)

    def _maybe_initialize_maintenance_due(self, meta: Dict[str, Any], now_ms: int) -> None:
        """Initialize periodic maintenance due timestamp once for this key."""
        if self._maintenance_trigger_policy is None or self._maintenance_trigger_policy.mode != "periodic":
            return
        if int(meta.get(GROUPBY_META_MAINTENANCE_DUE_MS, 0) or 0) > 0:
            return
        meta[GROUPBY_META_MAINTENANCE_DUE_MS] = now_ms + int(self._maintenance_trigger_policy.interval_ms)

    def _set_async_poll_due(self, meta: Dict[str, Any], due_ms: Optional[int]) -> None:
        """Set or clear async poll due timestamp."""
        if due_ms is None:
            meta.pop(GROUPBY_META_ASYNC_POLL_DUE_MS, None)
            return
        meta[GROUPBY_META_ASYNC_POLL_DUE_MS] = int(due_ms)

    def _schedule_recompute_timer(self, meta: Dict[str, Any], timer_service: Any) -> None:
        """Schedule the next recompute timer for async poll and maintenance."""
        due_values: List[int] = []
        async_due = int(meta.get(GROUPBY_META_ASYNC_POLL_DUE_MS, 0) or 0)
        maintenance_due = int(meta.get(GROUPBY_META_MAINTENANCE_DUE_MS, 0) or 0)
        if async_due > 0:
            due_values.append(async_due)
        if maintenance_due > 0:
            due_values.append(maintenance_due)
        if not due_values:
            clear_timer_registration(meta, TimerCategory.RECOMPUTE)
            return
        next_due = min(due_values)
        register_timer(
            timer_service,
            meta,
            TimerCategory.RECOMPUTE,
            next_due,
        )

    def _on_recompute_timer_async(
        self,
        meta: Dict[str, Any],
        ctx: "KeyedProcessFunction.OnTimerContext",
        timestamp: int,
    ) -> List[Any]:
        """Handle recompute timer in async groupby runtime."""
        outputs: List[Any] = []
        outputs.extend(self._poll_pending_llm(meta, ctx, timestamp))
        if self._maintenance_trigger_policy is None or self._maintenance_trigger_policy.mode != "periodic":
            self._schedule_recompute_timer(meta, ctx.timer_service())
            return outputs
        due = int(meta.get(GROUPBY_META_MAINTENANCE_DUE_MS, 0) or 0)
        if due <= 0 or timestamp < due:
            self._schedule_recompute_timer(meta, ctx.timer_service())
            return outputs
        if single_flight_is_in_flight(meta, "assignment") or single_flight_is_in_flight(meta, "refine"):
            self._schedule_recompute_timer(meta, ctx.timer_service())
            return outputs
        if self._pending_event_values():
            self._schedule_recompute_timer(meta, ctx.timer_service())
            return outputs
        if self._resolved_variant == "llm_refine":
            outputs.extend(self._dispatch_refine_request(meta, ctx, timestamp))
        else:
            self._run_maintenance(meta, timestamp)
            meta[GROUPBY_META_MAINTENANCE_DUE_MS] = timestamp + int(self._maintenance_trigger_policy.interval_ms)
        self._schedule_recompute_timer(meta, ctx.timer_service())
        return outputs

    def _maintenance_merge_threshold(self) -> float:
        return resolve_groupby_maintenance_merge_threshold(
            variant=self._resolved_variant,
            assign_threshold=self._resolved_assign_threshold,
            new_group_threshold=self._resolved_new_group_threshold,
        )

    def _split_mixed_groups(self, now_ms: int) -> int:
        groups: Dict[str, Dict[str, Any]] = {}
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is not None:
                groups[group_id] = dict(profile)
        split_groups, split_count = split_group_profiles(
            groups,
            variant=self._resolved_variant,
            encoder=self._encoder,
            now_ms=now_ms,
            max_examples=self._resolved_max_group_examples,
            rule_threshold=self._config.local_rule_split_seed_similarity_threshold,
            embedding_threshold=self._config.local_embedding_split_seed_similarity_threshold,
        )
        for group_id in list(self._group_profiles.keys()):
            if group_id not in split_groups:
                self._group_profiles.remove(group_id)
        for group_id, profile in split_groups.items():
            self._group_profiles.put(group_id, profile)
        return split_count

    def _merge_similar_groups(self, now_ms: int) -> int:
        groups: Dict[str, Dict[str, Any]] = {}
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is not None:
                groups[group_id] = dict(profile)
        merged_groups, _merged_into, merge_count = merge_similar_group_profiles(
            groups,
            variant=self._resolved_variant,
            encoder=self._encoder,
            assign_threshold=self._resolved_assign_threshold,
            new_group_threshold=self._resolved_new_group_threshold,
            now_ms=now_ms,
            max_examples=self._resolved_max_group_examples,
        )
        for group_id in list(self._group_profiles.keys()):
            if group_id not in merged_groups:
                self._group_profiles.remove(group_id)
        for group_id, profile in merged_groups.items():
            self._group_profiles.put(group_id, profile)
        return merge_count

    def _refresh_group_labels(self) -> int:
        groups: Dict[str, Dict[str, Any]] = {}
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is not None:
                groups[group_id] = dict(profile)
        relabeled = relabel_group_profiles(groups)
        rename_count = 0
        for group_id, profile in relabeled.items():
            previous = self._group_profiles.get(group_id)
            if previous is not None and str(previous.get("label", "")) != str(profile.get("label", "")):
                rename_count += 1
            self._group_profiles.put(group_id, profile)
        return rename_count

    def _refine_groups_with_llm(
        self,
        now_ms: int,
        *,
        meta: Dict[str, Any],
    ) -> Tuple[int, int, int]:
        """Run one true semantic refinement pass over current groups."""
        if self._client is None:
            raise RuntimeError("sem_groupby llm_refine runtime is not initialized")
        request_basis = self._begin_llm_lane(
            meta=meta,
            lane="refine",
            now_ms=now_ms,
            trigger_reason="maintenance_refine",
        )
        groups: List[Dict[str, Any]] = []
        for group_id in self._group_profiles.keys():
            profile = self._group_profiles.get(group_id)
            if profile is None:
                continue
            groups.append(
                {
                    "group_id": group_id,
                    "label": str(profile.get("label", "") or ""),
                    "summary": str(profile.get("summary", "") or ""),
                    "event_count": int(profile.get("event_count", 0)),
                    "examples": list(profile.get("examples", [])),
                }
            )
        try:
            if not groups:
                return 0, 0, 0
            refine_plan = evaluate_sem_group_refine_sync(
                client=self._client,
                intent=self._query_spec.semantic.instruction,
                groups=groups,
            )
            split_count = self._apply_llm_splits(refine_plan["splits"], now_ms)
            merge_count = self._apply_llm_merges(refine_plan["merges"], now_ms)
            rename_count = self._apply_llm_renames(refine_plan["renames"])
            return split_count, merge_count, rename_count
        finally:
            single_flight_complete(meta, "refine", request_basis.request_id)

    def _begin_llm_lane(
        self,
        *,
        meta: Dict[str, Any],
        lane: str,
        now_ms: int,
        trigger_reason: str,
    ) -> AsyncRequestBasis:
        """Start one controlled LLM lane request for this key."""
        if lane not in {"assignment", "refine"}:
            raise ValueError(f"Unsupported sem_groupby lane={lane!r}")
        other_lane = "refine" if lane == "assignment" else "assignment"
        if single_flight_is_in_flight(meta, other_lane):
            raise RuntimeError(
                f"sem_groupby {lane} cannot start while {other_lane} is in flight for the same key"
            )
        request_basis = AsyncRequestBasis(
            request_id=(
                f"{lane}:{meta.get('key', '')}:"
                f"{int(meta.get('scope_epoch', 0) or 0)}:"
                f"{int(meta.get('total_assigned', 0) or 0)}:"
                f"{now_ms}"
            ),
            key=str(meta.get("key", "")),
            scope_epoch=int(meta.get("scope_epoch", 0) or 0),
            state_version=int(meta.get("total_assigned", 0) or 0),
            trigger_reason=trigger_reason,
        )
        single_flight_begin(meta, lane, request_basis)
        return request_basis

    def _apply_llm_splits(self, splits: List[Dict[str, Any]], now_ms: int) -> int:
        """Apply semantic split operations using retained examples."""
        split_count = 0
        for split in splits:
            group_id = str(split["group_id"])
            profile = self._group_profiles.get(group_id)
            if profile is None:
                raise RuntimeError(f"sem_groupby llm_refine split referenced missing group_id={group_id!r}")
            children = list(split["children"])
            total_count = max(int(profile.get("event_count", 0)), len(children))
            source_created_ms = int(profile.get("created_ms", now_ms))
            source_examples = [str(item) for item in profile.get("examples", []) if str(item).strip()]
            self._group_profiles.remove(group_id)
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
                self._group_profiles.put(child_group_id, child_profile)
            split_count += 1
        return split_count

    def _apply_llm_merges(self, merges: List[Dict[str, Any]], now_ms: int) -> int:
        """Apply semantic merge operations."""
        merge_count = 0
        for merge in merges:
            target_group_id = str(merge["target_group_id"])
            source_group_ids = [str(group_id) for group_id in merge["source_group_ids"]]
            if not self._group_profiles.contains(target_group_id):
                raise RuntimeError(
                    f"sem_groupby llm_refine merge referenced missing target_group_id={target_group_id!r}"
                )
            for source_group_id in source_group_ids:
                if source_group_id == target_group_id:
                    continue
                if not self._group_profiles.contains(source_group_id):
                    raise RuntimeError(
                        f"sem_groupby llm_refine merge referenced missing source_group_id={source_group_id!r}"
                    )
                groups = {
                    group_id: dict(self._group_profiles.get(group_id))
                    for group_id in list(self._group_profiles.keys())
                    if self._group_profiles.get(group_id) is not None
                }
                apply_group_merge(
                    groups,
                    target_group_id,
                    source_group_id,
                    now_ms,
                    max_examples=self._resolved_max_group_examples,
                )
                for group_id in list(self._group_profiles.keys()):
                    if group_id not in groups:
                        self._group_profiles.remove(group_id)
                for group_id, profile in groups.items():
                    self._group_profiles.put(group_id, profile)
                merge_count += 1
            label = str(merge.get("label", "") or "")
            if label:
                target_profile = self._group_profiles.get(target_group_id)
                if target_profile is not None:
                    target_profile["label"] = label
                    self._group_profiles.put(target_group_id, target_profile)
        return merge_count

    def _apply_llm_renames(self, renames: List[Dict[str, Any]]) -> int:
        """Apply semantic rename operations."""
        rename_count = 0
        for rename in renames:
            group_id = str(rename["group_id"])
            profile = self._group_profiles.get(group_id)
            if profile is None:
                raise RuntimeError(f"sem_groupby llm_refine rename referenced missing group_id={group_id!r}")
            label = str(rename["label"])
            if str(profile.get("label", "")) != label:
                rename_count += 1
            profile["label"] = label
            self._group_profiles.put(group_id, profile)
        return rename_count

    def _new_group_id(self) -> str:
        """Allocate one compact group identifier."""
        import uuid

        return uuid.uuid4().hex[:GROUPBY_ID_HEX_CHARS]

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
        if self._resolved_persistence_policy == "reset_per_scope":
            for group_id in list(self._group_profiles.keys()):
                self._group_profiles.remove(group_id)
        clear_timer_registration(meta, TimerCategory.FLUSH)
        meta.pop("pending_scope_close_reason", None)
        meta["scope_epoch"] = int(meta.get("scope_epoch", 0) or 0) + 1
        meta["scope_last_time_ms"] = 0
        meta["scope_bucket_id"] = None
        meta["last_scope_reset_reason"] = reason
