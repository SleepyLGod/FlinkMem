"""Stateful internal physical pushdown builders."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from pyflink.common import Time, Types
from pyflink.datastream import AsyncDataStream, DataStream
from pyflink.datastream.functions import AsyncFunction, FlatMapFunction, MapFunction, RuntimeContext

from pyflink.semantic_runtime.public_api import SemAggRequest, SemGroupbyRequest, SemTopKRequest
from pyflink.semantic_runtime.runtime.event_model import SemEvent, window_snapshot_to_sem_events
from pyflink.semantic_runtime.runtime.plans import (
    lower_sem_agg_request,
    lower_sem_groupby_request,
    lower_sem_topk_request,
)
from pyflink.semantic_runtime.runtime.prompt_templates import (
    build_sem_group_assign_prompt,
)
from pyflink.semantic_runtime.runtime.pushdown.common import (
    parse_candidate_pool,
    parse_json_or_passthrough,
    parse_window_snapshot,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_kernel import (
    _LLM_GROUPBY_VARIANTS,
    _LOCAL_GROUPBY_VARIANTS,
    _append_profile_example,
    _new_group_profile,
    merge_similar_group_profiles,
    relabel_group_profiles,
    resolve_groupby_variant,
    resolve_groupby_runtime_params,
    score_group_profile,
)
from pyflink.semantic_runtime.operators.stateful.sem_topk_worker import (
    extract_topk_candidate_text,
    lexical_similarity,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_bounded import (
    COMPRESSIVE_KEEP_DIVISOR,
    COMPRESSIVE_MIN_EVENTS_FOR_TRUNCATION,
)
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder
from pyflink.semantic_runtime.llm_client import LLMClient, create_llm_client

if TYPE_CHECKING:
    from pyflink.semantic_runtime.llm_client import LLMClientConfig
    from pyflink.semantic_runtime.operators.stateful.sem_agg_kernel import SemAggConfig
    from pyflink.semantic_runtime.operators.stateful.sem_groupby_kernel import SemGroupbyConfig
    from pyflink.semantic_runtime.runtime_config import RuntimeConfig


def chunk_items(items: List[SemEvent], chunk_size: int) -> Iterable[List[SemEvent]]:
    """Yield contiguous chunks from one bounded scope."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    for index in range(0, len(items), chunk_size):
        yield items[index:index + chunk_size]


def groupby_assignment_row(
    event: SemEvent,
    *,
    group_id: str,
    confidence: float,
    source: str,
) -> Dict[str, Any]:
    """Build one normalized group assignment row."""
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


def apply_groupby_maintenance(
    *,
    groups: Dict[str, Dict[str, Any]],
    assignment_rows: List[Dict[str, Any]],
    variant: str,
    encoder: HashingTextEncoder,
    assign_threshold: float,
    new_group_threshold: float,
    refresh_labels_during_maintenance: bool,
    max_examples: int,
) -> List[Dict[str, Any]]:
    """Apply bounded-scope group maintenance and rewrite merged group ids."""
    now_ms = int(time.time() * 1000)
    groups, merged_into, _merge_count = merge_similar_group_profiles(
        groups,
        variant=variant,
        encoder=encoder,
        assign_threshold=assign_threshold,
        new_group_threshold=new_group_threshold,
        now_ms=now_ms,
        max_examples=max_examples,
    )
    if refresh_labels_during_maintenance:
        groups = relabel_group_profiles(groups)
    if not merged_into:
        return assignment_rows

    def resolve_merged_group_id(group_id: str) -> str:
        current = group_id
        seen: set[str] = set()
        while current in merged_into and current not in seen:
            seen.add(current)
            current = merged_into[current]
        return current

    rewritten: List[Dict[str, Any]] = []
    for row in assignment_rows:
        updated = dict(row)
        updated["group_id"] = resolve_merged_group_id(str(row.get("group_id", "")))
        rewritten.append(updated)
    return rewritten


class WindowOwnedLocalGroupbyLabeler(MapFunction):
    """Local semantic label assignment for one bounded groupby scope."""

    def __init__(self, *, config: "SemGroupbyConfig", query_spec: Any) -> None:
        self._config = config
        self._query_spec = query_spec
        (
            _resolved_ttl_seconds,
            self._max_groups_per_key,
            self._assign_threshold,
            self._new_group_threshold,
        ) = resolve_groupby_runtime_params(config, query_spec)
        self._variant = resolve_groupby_variant(config, query_spec)
        self._encoder = HashingTextEncoder(dim=128)
        if self._variant not in _LOCAL_GROUPBY_VARIANTS:
            raise ValueError(
                f"window-owned sem_groupby pushdown local path requires local variant, got {self._variant!r}"
            )

    def map(self, value: Any) -> Dict[str, Any]:
        """Assign all events inside one bounded scope."""
        snapshot = parse_window_snapshot(value, operator_name="sem_groupby pushdown")
        events = [SemEvent.from_dict(item) for item in window_snapshot_to_sem_events(snapshot)]
        groups: Dict[str, Dict[str, Any]] = {}
        rows: List[Dict[str, Any]] = []
        now_ms = int(time.time() * 1000)

        for event in events:
            best_group_id: Optional[str] = None
            best_score = 0.0
            for group_id, profile in groups.items():
                score = score_group_profile(
                    event.payload,
                    profile,
                    variant=self._variant,
                    encoder=self._encoder,
                )
                if score > best_score:
                    best_score = score
                    best_group_id = group_id

            if best_group_id is not None and best_score >= self._assign_threshold:
                profile = groups[best_group_id]
                profile["event_count"] = int(profile.get("event_count", 0)) + 1
                profile["last_update_ms"] = now_ms
                _append_profile_example(profile, event.payload, max_examples=int(self._config.max_group_examples))
                rows.append(
                    groupby_assignment_row(
                        event,
                        group_id=best_group_id,
                        confidence=best_score,
                        source="local",
                    )
                )
                continue

            if len(groups) >= self._max_groups_per_key:
                if self._config.overflow_policy.name == "DROP_OLDEST":
                    oldest_group_id = min(
                        groups.items(),
                        key=lambda item: int(item[1].get("last_update_ms", 0)),
                    )[0]
                    groups.pop(oldest_group_id, None)
                elif self._config.overflow_policy.name == "DROP_NEWEST":
                    raise RuntimeError(
                        "sem_groupby pushdown could not create a new group under DROP_NEWEST overflow policy."
                    )

            group_id = uuid4_hex()
            profile = _new_group_profile(group_id, " ".join(event.payload.split()[:5]), now_ms)
            profile["event_count"] = 1
            _append_profile_example(profile, event.payload, max_examples=int(self._config.max_group_examples))
            groups[group_id] = profile
            rows.append(
                groupby_assignment_row(
                    event,
                    group_id=group_id,
                    confidence=best_score,
                    source="new_group",
                )
            )

        if (
            self._query_spec.maintenance_trigger_policy is not None
            and self._query_spec.maintenance_trigger_policy.mode == "on_scope_close"
        ):
            rows = apply_groupby_maintenance(
                groups=groups,
                assignment_rows=rows,
                variant=self._variant,
                encoder=self._encoder,
                assign_threshold=self._assign_threshold,
                new_group_threshold=self._new_group_threshold,
                refresh_labels_during_maintenance=self._config.refresh_labels_during_maintenance,
                max_examples=int(self._config.max_group_examples),
            )
        return {"assignments": rows}


def uuid4_hex() -> str:
    """Return one compact group id."""
    import uuid

    return uuid.uuid4().hex[:8]


class WindowOwnedAsyncGroupbyLabeler(AsyncFunction):
    """Async LLM-driven scope labeling for one bounded groupby scope."""

    def __init__(
        self,
        *,
        intent: str,
        llm_config: "LLMClientConfig",
        config: "SemGroupbyConfig",
        query_spec: Any,
    ) -> None:
        self._intent = intent
        self._llm_config = llm_config
        self._config = config
        self._query_spec = query_spec
        self._client: Optional[LLMClient] = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    async def async_invoke(self, value: Any) -> List[Dict[str, Any]]:
        """Assign one bounded scope through chunked LLM grouping."""
        assert self._client is not None, "open() was not called"
        snapshot = parse_window_snapshot(value, operator_name="sem_groupby pushdown")
        events = [SemEvent.from_dict(item) for item in window_snapshot_to_sem_events(snapshot)]
        if not events:
            return [{"assignments": []}]

        groups: Dict[str, Dict[str, Any]] = {}
        rows: List[Dict[str, Any]] = []
        chunk_size = max(1, int(self._config.assignment_batch_size))
        prompt_template = build_sem_group_assign_prompt(self._intent)

        for chunk in chunk_items(events, chunk_size):
            prompt = prompt_template.format(
                existing_groups=json.dumps(
                    [
                        {"group_id": gid, "label": profile.get("label", ""), "summary": profile.get("summary", "")}
                        for gid, profile in groups.items()
                    ],
                    ensure_ascii=False,
                ),
                events=json.dumps([event.to_dict() for event in chunk], ensure_ascii=False),
            )
            text, _metrics = await self._client.call(prompt)
            chunk_result = self._parse_groupby_chunk_result(
                text,
                chunk,
                existing_group_ids=list(groups.keys()),
            )
            now_ms = int(time.time() * 1000)
            for item in chunk_result:
                event = item["event"]
                group_id = item["group_id"]
                if item["decision"] == "new":
                    group_id = uuid4_hex()
                    profile = _new_group_profile(group_id, item["label"], now_ms)
                    profile["event_count"] = 1
                    _append_profile_example(
                        profile,
                        event.payload,
                        max_examples=int(self._config.max_group_examples),
                    )
                    groups[group_id] = profile
                else:
                    profile = groups[group_id]
                    profile["event_count"] = int(profile.get("event_count", 0)) + 1
                    profile["last_update_ms"] = now_ms
                    _append_profile_example(
                        profile,
                        event.payload,
                        max_examples=int(self._config.max_group_examples),
                    )
                rows.append(
                    groupby_assignment_row(
                        event,
                        group_id=group_id,
                        confidence=item["confidence"],
                        source="llm_scope",
                    )
                )

        return [{"assignments": rows}]

    def timeout(self, value: Any) -> List[Dict[str, Any]]:
        """Fail fast on Flink-level timeout."""
        raise TimeoutError("sem_groupby pushdown timed out")

    @staticmethod
    def _parse_groupby_chunk_result(
        text: str,
        chunk: List[SemEvent],
        *,
        existing_group_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Parse and validate one chunk assignment result."""
        from pyflink.semantic_runtime.runtime.steps.sem_group_assign import (
            parse_sem_group_assignments,
        )

        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("sem_groupby pushdown expected valid JSON output") from exc
        by_seq = {event.seq_id: event for event in chunk}
        normalized_assignments = parse_sem_group_assignments(
            parsed,
            existing_group_ids=existing_group_ids,
        )
        seen: set[int] = set()
        normalized: List[Dict[str, Any]] = []
        for raw in normalized_assignments:
            seq_id = int(raw["event_seq_id"])
            if seq_id not in by_seq:
                raise ValueError("sem_groupby pushdown returned assignment for unknown event_seq_id")
            if seq_id in seen:
                raise ValueError("sem_groupby pushdown returned duplicate event assignment")
            seen.add(seq_id)
            normalized.append(
                {
                    "event": by_seq[seq_id],
                    "decision": str(raw["decision"]),
                    "group_id": str(raw["group_id"]).strip(),
                    "confidence": float(raw["confidence"]),
                    "label": str(raw.get("label", "")).strip(),
                }
            )
        if seen != set(by_seq.keys()):
            raise ValueError("sem_groupby pushdown did not assign every event in the chunk")
        return normalized


class GroupbyAssignmentsEmitter(FlatMapFunction):
    """Emit normalized assignment rows from one bounded assignment envelope."""

    def flat_map(self, value: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Emit assignment rows one by one."""
        if not isinstance(value, dict) or not isinstance(value.get("assignments"), list):
            raise ValueError("sem_groupby pushdown expected assignments envelope")
        return list(value["assignments"])


class TopKExternalScoreEnvelopeBuilder(MapFunction):
    """Build one scored-candidate envelope from an externally scored pool."""

    def __init__(self, *, score_field: str) -> None:
        self._score_field = score_field

    def map(self, value: Any) -> Dict[str, Any]:
        """Normalize one bounded candidate pool into scored_items."""
        record = parse_candidate_pool(value, operator_name="sem_topk pushdown")
        scored_items: List[Dict[str, Any]] = []
        for candidate in record["candidates"]:
            if not isinstance(candidate, dict):
                raise ValueError("sem_topk pushdown expected candidate dicts")
            if self._score_field not in candidate or candidate[self._score_field] is None:
                raise ValueError(f"sem_topk pushdown expected external score field {self._score_field!r}")
            scored_items.append(
                {
                    "item": dict(candidate),
                    "score": float(candidate[self._score_field]),
                    "reason": str(candidate.get("reason", "")),
                }
            )
        return {"scored_items": scored_items, "original_count": len(scored_items)}


class TopKEmbeddingEnvelopeBuilder(MapFunction):
    """Build one scored-candidate envelope using local embedding-style similarity."""

    def __init__(self, *, score_field: str) -> None:
        self._score_field = score_field

    def map(self, value: Any) -> Dict[str, Any]:
        """Score one bounded candidate pool locally."""
        record = parse_candidate_pool(value, operator_name="sem_topk pushdown")
        query_text = str(record.get("query", ""))
        scored_items: List[Dict[str, Any]] = []
        for candidate in record["candidates"]:
            if not isinstance(candidate, dict):
                raise ValueError("sem_topk pushdown expected candidate dicts")
            candidate_text = extract_topk_candidate_text(candidate)
            scored_items.append(
                {
                    "item": dict(candidate),
                    "score": float(lexical_similarity(query_text, candidate_text)),
                    "reason": "embedding_similarity",
                }
            )
        return {"scored_items": scored_items, "original_count": len(scored_items)}


class StatefulTopKProjector(MapFunction):
    """Project one scored bounded pool into a stateful-style top-k snapshot."""

    def __init__(self, *, k: int) -> None:
        self._k = k

    def map(self, value: Any) -> Dict[str, Any]:
        """Sort one scored pool and emit one top-k snapshot envelope."""
        payload = parse_json_or_passthrough(value, operator_name="sem_topk pushdown")
        if not isinstance(payload, dict) or not isinstance(payload.get("scored_items"), list):
            raise ValueError("sem_topk pushdown expected scored_items envelope")
        scored_items = payload["scored_items"]
        normalized: List[Tuple[Dict[str, Any], float]] = []
        for item in scored_items:
            if not isinstance(item, dict):
                raise ValueError("sem_topk pushdown expected scored item objects")
            candidate = item.get("item")
            if not isinstance(candidate, dict) or not candidate.get("candidate_id"):
                raise ValueError("sem_topk pushdown expected item dicts with candidate_id")
            normalized.append((dict(candidate), float(item.get("score", 0.0))))
        normalized.sort(key=lambda entry: entry[1], reverse=True)
        top_records = [candidate for candidate, _score in normalized[: self._k]]
        return {
            "key": str(payload.get("key", "")),
            "topk": top_records,
            "top_ids": [str(candidate["candidate_id"]) for candidate in top_records],
            "query": str(payload.get("query", "")),
            "query_seq_id": int(payload.get("query_seq_id", 0) or 0),
            "source": str(payload.get("source", "pushdown")),
            "total_candidates": int(payload.get("original_count", len(normalized))),
            "stale_candidates": 0,
            "version": 1,
            "changed": True,
            "emission_policy": "snapshot",
            "error": "",
            "timestamp_ms": int(time.time() * 1000),
        }


class WindowAlgebraicAggProjector(MapFunction):
    """Project one bounded window snapshot into one algebraic aggregate."""

    def __init__(self, *, config: "SemAggConfig") -> None:
        self._config = config

    def map(self, value: Any) -> Dict[str, Any]:
        """Aggregate one closed snapshot with the configured reduce function."""
        snapshot = parse_window_snapshot(value, operator_name="sem_agg pushdown")
        raw_events = list(window_snapshot_to_sem_events(snapshot))
        if not raw_events:
            raise ValueError("sem_agg pushdown requires non-empty window snapshot")
        reduce_fn = self._config.reduce_fn
        aggregate = raw_events[0]
        if reduce_fn is not None:
            for event in raw_events[1:]:
                aggregate = reduce_fn(aggregate, event)
        return {
            "key": str(snapshot.get("key", "")),
            "aggregate": aggregate,
            "version": 1,
            "mode": "algebraic_window",
            "event_count": len(raw_events),
            "timestamp_ms": int(time.time() * 1000),
        }


class WindowOwnedAsyncAggSummarizer(AsyncFunction):
    """Async semantic summarizer for one bounded agg snapshot."""

    def __init__(
        self,
        *,
        mode: str,
        max_buffer_events: int,
        llm_config: "LLMClientConfig",
    ) -> None:
        self._mode = mode
        self._max_buffer_events = int(max_buffer_events)
        self._llm_config = llm_config
        self._client: Optional[LLMClient] = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    async def async_invoke(self, value: Any) -> List[Dict[str, Any]]:
        """Summarize one window snapshot with one async LLM call."""
        if self._client is None:
            raise RuntimeError("sem_agg pushdown async runtime is not initialized")
        from pyflink.semantic_runtime.runtime.steps.sem_agg_summary import (
            evaluate_sem_agg_summary_update,
        )

        snapshot = parse_window_snapshot(value, operator_name="sem_agg pushdown")
        raw_events = list(window_snapshot_to_sem_events(snapshot))
        if not raw_events:
            raise ValueError("sem_agg pushdown requires non-empty window snapshot")
        events_for_summary = self._compress_events(raw_events)
        result = await evaluate_sem_agg_summary_update(
            client=self._client,
            mode=self._mode,
            current_summary="",
            added_events=events_for_summary,
        )
        now_ms = int(time.time() * 1000)
        return [
            {
                "key": str(snapshot.get("key", "")),
                "aggregate": {
                    "summary": str(result["summary"]),
                    "version": 1,
                    "updated_ms": now_ms,
                },
                "version": 1,
                "mode": f"{self._mode}_async",
                "event_count": len(events_for_summary),
                "timestamp_ms": now_ms,
                "scope_id": str(snapshot.get("window_id", "")),
            }
        ]

    def timeout(self, value: Any) -> List[Dict[str, Any]]:
        """Fail fast when one async agg pushdown request times out."""
        raise TimeoutError("sem_agg pushdown async summarization timed out")

    def _compress_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self._mode != "compressive":
            return events
        if len(events) <= COMPRESSIVE_MIN_EVENTS_FOR_TRUNCATION:
            return events
        keep = max(
            1,
            min(len(events), self._max_buffer_events // COMPRESSIVE_KEEP_DIVISOR),
        )
        return events[-keep:]


def apply_sem_groupby_pushdown(
    input_stream: DataStream,
    *,
    request: SemGroupbyRequest,
    runtime_config: "RuntimeConfig",
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply window-owned semantic grouping as label assignment plus native projection."""
    if timeout_ms <= 0:
        raise ValueError("sem_groupby pushdown requires timeout_ms > 0")
    if async_capacity <= 0:
        raise ValueError("sem_groupby pushdown requires async_capacity > 0")

    plan = lower_sem_groupby_request(request, runtime_config)
    if plan.input_kind != "window_snapshot":
        raise ValueError("sem_groupby pushdown requires window context")

    variant = resolve_groupby_variant(plan.kernel_config, plan.query_spec)
    if variant in _LOCAL_GROUPBY_VARIANTS:
        envelopes = input_stream.map(
            WindowOwnedLocalGroupbyLabeler(config=plan.kernel_config, query_spec=plan.query_spec),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    elif variant in _LLM_GROUPBY_VARIANTS:
        envelopes = AsyncDataStream.unordered_wait(
            input_stream,
            WindowOwnedAsyncGroupbyLabeler(
                intent=plan.intent,
                llm_config=runtime_config.get_operator_llm_client_config(
                    "sem_groupby",
                    allow_query_spec=True,
                ),
                config=plan.kernel_config,
                query_spec=plan.query_spec,
            ),
            Time.milliseconds(timeout_ms),
            async_capacity,
            Types.PICKLED_BYTE_ARRAY(),
        )
    else:
        raise ValueError(f"Unsupported sem_groupby pushdown variant {variant!r}")

    return envelopes.flat_map(
        GroupbyAssignmentsEmitter(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )


def apply_sem_topk_pushdown(
    input_stream: DataStream,
    *,
    request: SemTopKRequest,
    runtime_config: "RuntimeConfig",
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply bounded pointwise semantic top-k as score generation plus native top-k projection."""
    if timeout_ms <= 0:
        raise ValueError("sem_topk pushdown requires timeout_ms > 0")
    if async_capacity <= 0:
        raise ValueError("sem_topk pushdown requires async_capacity > 0")

    plan = lower_sem_topk_request(request, runtime_config)
    if plan.context_kind != "window":
        raise ValueError("sem_topk pushdown requires window context")
    if plan.query_spec.ranking_method != "pointwise":
        raise ValueError("sem_topk pushdown requires pointwise ranking_method")

    backend = plan.kernel_config.scorer_backend
    if backend == "llm":
        from pyflink.semantic_runtime.operators.row.sem_local_topk import SemLocalTopKScoringFunction

        scored = AsyncDataStream.unordered_wait(
            input_stream,
            SemLocalTopKScoringFunction(
                score_intent=plan.intent,
                llm_config=runtime_config.get_operator_llm_client_config(
                    "sem_topk",
                    allow_query_spec=True,
                ),
                items_field="candidates",
            ),
            Time.milliseconds(timeout_ms),
            async_capacity,
            Types.STRING(),
        )
    elif backend == "embedding":
        scored = input_stream.map(
            TopKEmbeddingEnvelopeBuilder(score_field=plan.kernel_config.score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    elif backend == "external_score":
        scored = input_stream.map(
            TopKExternalScoreEnvelopeBuilder(score_field=plan.kernel_config.score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    else:
        raise ValueError(f"Unsupported sem_topk pushdown backend {backend!r}")

    return scored.map(
        StatefulTopKProjector(k=plan.k),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )


def apply_sem_agg_pushdown(
    input_stream: DataStream,
    *,
    request: SemAggRequest,
    runtime_config: "RuntimeConfig",
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply bounded agg pushdown over window snapshots."""
    if timeout_ms <= 0:
        raise ValueError("sem_agg pushdown requires timeout_ms > 0")
    if async_capacity <= 0:
        raise ValueError("sem_agg pushdown requires async_capacity > 0")
    plan = lower_sem_agg_request(request, runtime_config)
    if plan.context_kind != "window":
        raise ValueError("sem_agg pushdown requires window context")
    if plan.mode == "algebraic":
        return input_stream.map(
            WindowAlgebraicAggProjector(config=plan.kernel_config),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    if plan.mode in {"summarize", "compressive"}:
        return AsyncDataStream.unordered_wait(
            input_stream,
            WindowOwnedAsyncAggSummarizer(
                mode=plan.mode,
                max_buffer_events=int(plan.kernel_config.max_buffer_events),
                llm_config=runtime_config.get_operator_llm_client_config(
                    "sem_agg",
                    allow_query_spec=True,
                ),
            ),
            Time.milliseconds(timeout_ms),
            async_capacity,
            Types.PICKLED_BYTE_ARRAY(),
        )
    raise ValueError(f"Unsupported sem_agg pushdown mode {plan.mode!r}")


__all__ = [
    "WindowOwnedLocalGroupbyLabeler",
    "WindowOwnedAsyncGroupbyLabeler",
    "GroupbyAssignmentsEmitter",
    "TopKExternalScoreEnvelopeBuilder",
    "TopKEmbeddingEnvelopeBuilder",
    "StatefulTopKProjector",
    "WindowAlgebraicAggProjector",
    "WindowOwnedAsyncAggSummarizer",
    "apply_sem_groupby_pushdown",
    "apply_sem_topk_pushdown",
    "apply_sem_agg_pushdown",
]
