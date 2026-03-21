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

"""Internal workers and helper plans for ``sem_topk``.

This module contains:
- pointwise scorer workers
- bounded-pool contextual workers
- contextual planning helpers
- record-shape utilities

The public builder lives in ``sem_topk_pipeline.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.semantic_runtime.llm_client import LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime_config import EmbeddingBackendConfig
from pyflink.semantic_runtime.semantic_spec import TopKQuerySpec, TriggerPolicy
from pyflink.semantic_runtime.runtime.simple_text_encoder import (
    HashingTextEncoder,
    tokenize_text,
)
from pyflink.semantic_runtime.operators.stateful.sem_topk import (
    SemTopKConfig,
)

logger = logging.getLogger(__name__)


DEFAULT_CANDIDATE_TEXT_FIELDS: tuple[str, ...] = ("text", "payload", "content")

VALID_CONTEXTUAL_MERGE_STRATEGIES = {"tournament", "global_rank"}
VALID_CONTEXTUAL_CLOSE_SURROGATES = {
    "none",
    "natural_close",
    "periodic_snapshot",
    "idle_flush",
    "count_threshold_snapshot",
    "epoch_close",
}


@dataclass(frozen=True)
class TopKContextualPlan:
    """Internal execution plan for contextual top-k reranking.

    This is intentionally not part of the public API. It captures execution
    details that may later be chosen by a planner/CBO:
    - `context_chunk_size`: per-call candidate context size
    - `merge_strategy`: how local rankings are merged
    - `close_surrogate`: how operator-owned scopes without a natural close are
      snapshot-triggered for contextual rerank
    """

    context_chunk_size: Optional[int]
    merge_strategy: str
    close_surrogate: str = "none"
    epoch_ms: Optional[int] = None

    def __post_init__(self):
        if self.merge_strategy not in VALID_CONTEXTUAL_MERGE_STRATEGIES:
            raise ValueError(
                f"Invalid merge_strategy={self.merge_strategy!r}. "
                f"Must be one of {VALID_CONTEXTUAL_MERGE_STRATEGIES}."
            )
        if self.close_surrogate not in VALID_CONTEXTUAL_CLOSE_SURROGATES:
            raise ValueError(
                f"Invalid close_surrogate={self.close_surrogate!r}. "
                f"Must be one of {VALID_CONTEXTUAL_CLOSE_SURROGATES}."
            )
        if self.close_surrogate == "epoch_close":
            if self.epoch_ms is None or int(self.epoch_ms) <= 0:
                raise ValueError("epoch_ms must be > 0 when close_surrogate='epoch_close'")


def derive_topk_contextual_plan(
    query_spec: TopKQuerySpec,
    topk_config: SemTopKConfig,
) -> TopKContextualPlan:
    """Derive the internal contextual rerank plan.

    Public semantics stay in `TopKQuerySpec`; this plan only captures execution
    details and close-surrogate choices.
    """
    method = query_spec.ranking_method
    if method == "pairwise":
        chunk_size = 2
        merge_strategy = "tournament"
    elif method == "listwise":
        chunk_size = None
        merge_strategy = "global_rank"
    else:
        raise ValueError("TopKContextualPlan applies only to pairwise/listwise methods")

    trigger = query_spec.trigger_policy.mode
    scope = query_spec.scope_policy
    surrogate = "none"
    epoch_ms: Optional[int] = None

    if trigger == "on_scope_close":
        if scope.window_kind in {"session", "tumbling", "semantic"}:
            surrogate = "natural_close"
        elif scope.window_kind == "sliding" and scope.window_size_ms and scope.window_size_ms > 0:
            surrogate = "epoch_close"
            epoch_ms = int(scope.window_size_ms)
        elif scope.ttl_seconds:
            surrogate = "periodic_snapshot"
        else:
            surrogate = "idle_flush"
    elif trigger == "periodic":
        surrogate = "periodic_snapshot"
    elif trigger == "idle_flush":
        surrogate = "idle_flush"
    elif trigger == "count_threshold":
        surrogate = "count_threshold_snapshot"

    return TopKContextualPlan(
        context_chunk_size=chunk_size,
        merge_strategy=merge_strategy,
        close_surrogate=surrogate,
        epoch_ms=epoch_ms,
    )


def build_contextual_snapshot_query_spec(
    query_spec: TopKQuerySpec,
    topk_config: SemTopKConfig,
    plan: TopKContextualPlan,
) -> TopKQuerySpec:
    """Map contextual close surrogates to an executable operator-owned trigger."""
    trigger = query_spec.trigger_policy
    if plan.close_surrogate in {"none", "natural_close"}:
        return query_spec
    if plan.close_surrogate == "epoch_close":
        eff_trigger = TriggerPolicy(
            mode="periodic",
            interval_ms=int(plan.epoch_ms or topk_config.recompute_interval_ms or 1),
            emit_intermediate=trigger.emit_intermediate,
            emit_final_on_scope_close=trigger.emit_final_on_scope_close,
        )
        return replace(query_spec, trigger_policy=eff_trigger)
    if plan.close_surrogate == "periodic_snapshot":
        interval_ms = topk_config.recompute_interval_ms
        if interval_ms <= 0:
            interval_ms = int((query_spec.scope_policy.ttl_seconds or 1) * 1000)
        eff_trigger = TriggerPolicy(
            mode="periodic",
            interval_ms=max(1, int(interval_ms)),
            emit_intermediate=trigger.emit_intermediate,
            emit_final_on_scope_close=trigger.emit_final_on_scope_close,
        )
        return replace(query_spec, trigger_policy=eff_trigger)
    if plan.close_surrogate == "idle_flush":
        idle_ms = trigger.idle_ms or topk_config.recompute_interval_ms or 1000
        eff_trigger = TriggerPolicy(
            mode="idle_flush",
            idle_ms=max(1, int(idle_ms)),
            emit_intermediate=trigger.emit_intermediate,
            emit_final_on_scope_close=trigger.emit_final_on_scope_close,
        )
        return replace(query_spec, trigger_policy=eff_trigger)
    if plan.close_surrogate == "count_threshold_snapshot":
        count_threshold = trigger.count_threshold or max(1, query_spec.k)
        eff_trigger = TriggerPolicy(
            mode="count_threshold",
            count_threshold=max(1, int(count_threshold)),
            emit_intermediate=trigger.emit_intermediate,
            emit_final_on_scope_close=trigger.emit_final_on_scope_close,
        )
        return replace(query_spec, trigger_policy=eff_trigger)
    return query_spec


def is_topk_candidate_record(value: Any) -> bool:
    """Return True when *value* is a flat candidate record for top-k."""
    return isinstance(value, dict) and bool(value.get("candidate_id"))


def is_topk_candidate_pool(value: Any) -> bool:
    """Return True when *value* is a bounded pool envelope for top-k reranking."""
    return isinstance(value, dict) and isinstance(value.get("candidates"), list)


def topk_candidate_has_score(value: Dict[str, Any], score_field: str = "score") -> bool:
    """Return True when the candidate already carries a usable score."""
    return is_topk_candidate_record(value) and score_field in value and value[score_field] is not None


def topk_candidate_needs_scoring(
    value: Dict[str, Any],
    *,
    query_version: int,
    score_backend: str,
    score_field: str = "score",
) -> bool:
    """Decide whether the candidate must be re-scored for the active query."""
    if not is_topk_candidate_record(value):
        return False

    if score_backend == "external_score":
        return not topk_candidate_has_score(value, score_field)

    if not topk_candidate_has_score(value, score_field):
        return True

    return (
        value.get("_query_version") != query_version
        or value.get("_score_backend") != score_backend
    )


def extract_topk_candidate_text(
    value: Dict[str, Any],
    fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
) -> str:
    """Extract the candidate text used by pointwise scorers."""
    for field in fields:
        candidate_text = value.get(field)
        if candidate_text not in (None, ""):
            return str(candidate_text)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def resolve_topk_query_text(value: Dict[str, Any], query_spec: TopKQuerySpec) -> str:
    """Resolve the active query text for scoring a candidate."""
    query_text = value.get("query")
    if query_text not in (None, ""):
        return str(query_text)
    return str(query_spec.semantic.instruction or "")


def build_scored_topk_candidate(
    value: Dict[str, Any],
    *,
    score: float,
    query_spec: TopKQuerySpec,
    score_backend: str,
    score_field: str,
    source: str,
) -> Dict[str, Any]:
    """Attach score + version metadata to a candidate record."""
    now_ms = int(time.time() * 1000)
    out = dict(value)
    prev_score_version = int(out.get("_score_version", 0) or 0)
    out[score_field] = float(score)
    out["_score_version"] = prev_score_version + 1
    out["_query_version"] = query_spec.query_version
    out["_score_backend"] = score_backend
    out["_updated_ms"] = now_ms
    out["source"] = source
    out["error"] = ""
    return out


def _raise_topk_pipeline_error(message: str) -> None:
    """Raise a strict runtime error for invalid sem_topk execution paths."""
    raise ValueError(message)


def _raise_topk_shape_mismatch(message: str) -> None:
    """Raise a strict runtime error for top-k input-shape mismatches."""
    _raise_topk_pipeline_error(message)


def _raise_missing_external_score(score_field: str) -> None:
    """Fail fast when external-score top-k receives an unscored candidate."""
    _raise_topk_pipeline_error(f"topk_missing_external_score:{score_field}")


def lexical_similarity(query_text: str, candidate_text: str) -> float:
    """Cheap deterministic text similarity used for embedding/mock scoring."""
    query_tokens = set(tokenize_text(query_text))
    candidate_tokens = set(tokenize_text(candidate_text))
    if not query_tokens or not candidate_tokens:
        return 0.0

    overlap = query_tokens & candidate_tokens
    denom = (len(query_tokens) * len(candidate_tokens)) ** 0.5
    if denom <= 0:
        return 0.0
    return min(1.0, max(0.0, len(overlap) / denom))


def _pairwise_rank(
    scored: List[Tuple[Dict[str, Any], float]],
) -> List[Tuple[Dict[str, Any], float]]:
    """Rank a bounded pool by pairwise tournament wins."""
    if len(scored) <= 1:
        return scored

    wins = {idx: 0 for idx in range(len(scored))}
    for left_idx in range(len(scored)):
        for right_idx in range(left_idx + 1, len(scored)):
            left_score = scored[left_idx][1]
            right_score = scored[right_idx][1]
            if left_score >= right_score:
                wins[left_idx] += 1
            else:
                wins[right_idx] += 1

    ranked_indices = sorted(
        range(len(scored)),
        key=lambda idx: (wins[idx], scored[idx][1]),
        reverse=True,
    )
    return [scored[idx] for idx in ranked_indices]


def _position_score(rank_index: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return max(0.0, min(1.0, (total - rank_index) / total))


class _BaseTopKScorerWorker(AsyncFunction):
    """Shared utilities for async candidate scorers."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        score_backend: str,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        self._query_spec = query_spec
        self._score_backend = score_backend
        self._score_field = score_field
        self._candidate_text_fields = tuple(candidate_text_fields)

    def _extract_candidate_text(self, value: Dict[str, Any]) -> str:
        return extract_topk_candidate_text(value, self._candidate_text_fields)

    def _query_text(self, value: Dict[str, Any]) -> str:
        return resolve_topk_query_text(value, self._query_spec)


class _BaseBoundedPoolRerankerWorker(AsyncFunction):
    """Shared utilities for bounded-pool pairwise/listwise reranking."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        score_backend: str,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        self._query_spec = query_spec
        self._score_backend = score_backend
        self._score_field = score_field
        self._candidate_text_fields = tuple(candidate_text_fields)

    def _query_text(self, value: Dict[str, Any]) -> str:
        return resolve_topk_query_text(value, self._query_spec)

    def _extract_pool(self, value: Dict[str, Any]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for idx, cand in enumerate(value.get("candidates", [])):
            out = dict(cand)
            if "candidate_id" not in out:
                out["candidate_id"] = out.get("id", f"{value.get('key', '')}_{idx}")
            out["key"] = value.get("key", "")
            out["query"] = value.get("query", "")
            out["query_seq_id"] = int(value.get("query_seq_id", 0))
            out.setdefault("source", value.get("source", ""))
            out.setdefault("error", value.get("error", ""))
            items.append(out)
        return items

    def _finalise_ranked(
        self,
        original_value: Dict[str, Any],
        ranked: List[Tuple[Dict[str, Any], float]],
        *,
        source: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        total = len(ranked)
        for idx, (candidate, score) in enumerate(ranked):
            final_score = float(score)
            if self._score_backend == "llm":
                final_score = _position_score(idx, total)
            out.append(
                build_scored_topk_candidate(
                    candidate,
                    score=final_score,
                    query_spec=self._query_spec,
                    score_backend=self._score_backend,
                    score_field=self._score_field,
                    source=source,
                )
            )
        if not out:
            raise ValueError("topk_empty_bounded_pool")
        return out

    def _build_snapshot(
        self,
        original_value: Dict[str, Any],
        ranked: List[Tuple[Dict[str, Any], float]],
        *,
        source: str,
        emission_policy: str,
    ) -> List[Dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        scored_rows = []
        for candidate, score in ranked:
            scored_rows.append(
                build_scored_topk_candidate(
                    candidate,
                    score=float(score),
                    query_spec=self._query_spec,
                    score_backend=self._score_backend,
                    score_field=self._score_field,
                    source=source,
                )
            )
        top_rows = scored_rows[: self._query_spec.k]
        return [
            {
                "key": original_value.get("key", ""),
                "topk": top_rows,
                "top_items": top_rows,
                "top_ids": [row.get("candidate_id", "") for row in top_rows],
                "query": original_value.get("query", ""),
                "query_seq_id": int(original_value.get("query_seq_id", 0)),
                "source": source,
                "total_candidates": len(scored_rows),
                "stale_candidates": 0,
                "version": 1,
                "changed": True,
                "emission_policy": emission_policy,
                "error": str(original_value.get("error", "")),
                "timestamp_ms": now_ms,
            }
        ]


class _PointwiseLLMScorerWorker(_BaseTopKScorerWorker):
    """Pointwise LLM scorer for continuous top-k."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        llm_config: LLMClientConfig,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "llm", score_field, candidate_text_fields)
        self._llm_config = llm_config
        self._client = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @staticmethod
    def _parse_json_object(text: str) -> Dict[str, Any]:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise
            parsed = json.loads(text[start:end + 1])
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Expected JSON object, got {type(parsed).__name__}")
        return parsed

    def _build_prompt(self, value: Dict[str, Any]) -> str:
        criterion = self._query_spec.semantic.instruction or "Score candidate relevance."
        query_text = self._query_text(value)
        candidate_text = self._extract_candidate_text(value)
        return (
            "You are scoring one candidate for a continuous semantic top-k query.\n"
            "Return JSON only with key score and a float in [0, 1].\n"
            f"Criterion: {criterion}\n"
            f"Query: {query_text}\n"
            f"Candidate: {candidate_text}\n"
        )

    async def _ensure_client(self):
        if self._client is None:
            self._client = create_llm_client(self._llm_config)

    async def async_invoke(self, value):
        if not is_topk_candidate_record(value):
            raise ValueError("topk_pointwise_scorer_requires_flat_candidate_record")

        try:
            if self._llm_config.backend == "mock":
                score = lexical_similarity(
                    self._query_text(value),
                    self._extract_candidate_text(value),
                )
            else:
                await self._ensure_client()
                assert self._client is not None
                prompt = self._build_prompt(value)
                text, _ = await self._client.call(prompt)
                parsed = self._parse_json_object(text)
                score = float(parsed.get("score", 0.0))

            return [
                build_scored_topk_candidate(
                    value,
                    score=max(0.0, min(1.0, score)),
                    query_spec=self._query_spec,
                    score_backend=self._score_backend,
                    score_field=self._score_field,
                    source="topk_llm_pointwise",
                )
            ]
        except Exception as exc:
            raise RuntimeError(f"topk_llm_score_error: {exc}") from exc

    def timeout(self, value):
        raise TimeoutError("topk_llm_score_timeout")


class _EmbeddingScorerWorker(_BaseTopKScorerWorker):
    """Deterministic local embedding-style scorer for continuous top-k.

    Current status:
    - ``backend == "mock"`` → lexical similarity surrogate for tests
    - ``backend == "local_lexical"`` / ``"local_hashing"`` → local hashing encoder
    - real external embedding services are not implemented yet
    """

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        embedding_config: Optional[EmbeddingBackendConfig] = None,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "embedding", score_field, candidate_text_fields)
        self._embedding_config = embedding_config or EmbeddingBackendConfig()
        dim = int(self._embedding_config.dimensions or 128)
        self._encoder = HashingTextEncoder(dim=max(dim, 1))

    async def async_invoke(self, value):
        if not is_topk_candidate_record(value):
            raise ValueError("topk_embedding_pointwise_requires_flat_candidate_record")

        backend = self._embedding_config.backend or "mock"
        if backend not in {"mock", "local_lexical", "local_hashing"}:
            raise ValueError(f"topk_embedding_backend_not_implemented: {backend}")

        if backend == "mock":
            score = lexical_similarity(
                self._query_text(value),
                self._extract_candidate_text(value),
            )
        else:
            score = self._encoder.similarity(
                self._query_text(value),
                self._extract_candidate_text(value),
            )
        await asyncio.sleep(0)
        return [
            build_scored_topk_candidate(
                value,
                score=score,
                query_spec=self._query_spec,
                score_backend=self._score_backend,
                score_field=self._score_field,
                source="topk_embedding_pointwise",
            )
        ]

    def timeout(self, value):
        raise TimeoutError("topk_embedding_score_timeout")


class _BoundedPoolLLMRerankerWorker(_BaseBoundedPoolRerankerWorker):
    """Bounded-pool pairwise/listwise reranker for LLM backends."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        llm_config: LLMClientConfig,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "llm", score_field, candidate_text_fields)
        self._llm_config = llm_config
        self._client = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _candidate_text(self, value: Dict[str, Any]) -> str:
        return extract_topk_candidate_text(value, self._candidate_text_fields)

    def _mock_rank(self, value: Dict[str, Any]) -> List[Tuple[Dict[str, Any], float]]:
        query_text = self._query_text(value)
        pool = self._extract_pool(value)
        scored = [(cand, lexical_similarity(query_text, self._candidate_text(cand))) for cand in pool]
        if self._query_spec.ranking_method == "pairwise":
            return _pairwise_rank(scored)
        return sorted(scored, key=lambda item: item[1], reverse=True)

    async def _ensure_client(self):
        if self._client is None:
            self._client = create_llm_client(self._llm_config)

    def _build_prompt(self, value: Dict[str, Any]) -> str:
        query_text = self._query_text(value)
        criterion = self._query_spec.semantic.instruction or "Rerank bounded candidate pool."
        lines = []
        for cand in self._extract_pool(value):
            lines.append(f"- {cand['candidate_id']}: {self._candidate_text(cand)}")
        return (
            "You are reranking a bounded candidate pool for a continuous semantic top-k query.\n"
            "Return JSON only with key ranked_candidate_ids and a ranked list of candidate ids.\n"
            f"Method: {self._query_spec.ranking_method}\n"
            f"Criterion: {criterion}\n"
            f"Query: {query_text}\n"
            "Candidates:\n"
            + "\n".join(lines)
        )

    async def async_invoke(self, value):
        if not is_topk_candidate_pool(value):
            raise ValueError("topk_contextual_reranker_requires_bounded_pool")
        try:
            if self._llm_config.backend == "mock":
                ranked = self._mock_rank(value)
            else:
                await self._ensure_client()
                assert self._client is not None
                prompt = self._build_prompt(value)
                text, _ = await self._client.call(prompt)
                parsed = json.loads(text)
                ranked_ids = parsed.get("ranked_candidate_ids", []) if isinstance(parsed, dict) else []
                pool = {cand["candidate_id"]: cand for cand in self._extract_pool(value)}
                ranked = []
                for cid in ranked_ids:
                    if cid in pool:
                        ranked.append((pool.pop(cid), 0.0))
                for cand in pool.values():
                    ranked.append((cand, 0.0))
            return self._finalise_ranked(
                value,
                ranked,
                source=f"topk_llm_{self._query_spec.ranking_method}",
            )
        except Exception as exc:
            raise RuntimeError(
                f"topk_llm_{self._query_spec.ranking_method}_error: {exc}"
            ) from exc

    def timeout(self, value):
        raise TimeoutError(f"topk_llm_{self._query_spec.ranking_method}_timeout")


class _BoundedPoolEmbeddingRerankerWorker(_BaseBoundedPoolRerankerWorker):
    """Bounded-pool pairwise/listwise reranker for local embedding-style backends."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        embedding_config: Optional[EmbeddingBackendConfig] = None,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "embedding", score_field, candidate_text_fields)
        self._embedding_config = embedding_config or EmbeddingBackendConfig()
        dim = int(self._embedding_config.dimensions or 128)
        self._encoder = HashingTextEncoder(dim=max(dim, 1))

    def _candidate_text(self, value: Dict[str, Any]) -> str:
        return extract_topk_candidate_text(value, self._candidate_text_fields)

    async def async_invoke(self, value):
        if not is_topk_candidate_pool(value):
            raise ValueError("topk_contextual_reranker_requires_bounded_pool")

        backend = self._embedding_config.backend or "mock"
        if backend not in {"mock", "local_lexical", "local_hashing"}:
            raise ValueError(f"topk_embedding_backend_not_implemented: {backend}")

        query_text = self._query_text(value)
        pool = self._extract_pool(value)
        scored: List[Tuple[Dict[str, Any], float]] = []
        for cand in pool:
            candidate_text = self._candidate_text(cand)
            if backend == "mock":
                score = lexical_similarity(query_text, candidate_text)
            else:
                score = self._encoder.similarity(query_text, candidate_text)
            scored.append((cand, score))

        if self._query_spec.ranking_method == "pairwise":
            ranked = _pairwise_rank(scored)
        else:
            ranked = sorted(scored, key=lambda item: item[1], reverse=True)

        await asyncio.sleep(0)
        return self._finalise_ranked(
            value,
            ranked,
            source=f"topk_embedding_{self._query_spec.ranking_method}",
        )

    def timeout(self, value):
        raise TimeoutError(f"topk_embedding_{self._query_spec.ranking_method}_timeout")


class _BoundedPoolExternalScoreRerankerWorker(_BaseBoundedPoolRerankerWorker):
    """Bounded-pool reranker using existing candidate scores."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "external_score", score_field, candidate_text_fields)

    async def async_invoke(self, value):
        if not is_topk_candidate_pool(value):
            raise ValueError("topk_contextual_reranker_requires_bounded_pool")
        pool = self._extract_pool(value)
        scored = []
        for cand in pool:
            if self._score_field not in cand or cand[self._score_field] is None:
                raise ValueError(f"topk_missing_external_score:{self._score_field}")
            scored.append((cand, float(cand[self._score_field])))
        if self._query_spec.ranking_method == "pairwise":
            ranked = _pairwise_rank(scored)
        else:
            ranked = sorted(scored, key=lambda item: item[1], reverse=True)
        await asyncio.sleep(0)
        return self._finalise_ranked(
            value,
            ranked,
            source=f"topk_external_{self._query_spec.ranking_method}",
        )


class _BoundedPoolLLMTopKSnapshotWorker(_BaseBoundedPoolRerankerWorker):
    """Window-owned bounded-pool scorer/reranker that emits one final top-k snapshot."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        llm_config: LLMClientConfig,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "llm", score_field, candidate_text_fields)
        self._llm_config = llm_config
        self._client = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _candidate_text(self, value: Dict[str, Any]) -> str:
        return extract_topk_candidate_text(value, self._candidate_text_fields)

    async def _ensure_client(self):
        if self._client is None:
            self._client = create_llm_client(self._llm_config)

    async def _rank_pointwise(self, value: Dict[str, Any]) -> List[Tuple[Dict[str, Any], float]]:
        query_text = self._query_text(value)
        ranked: List[Tuple[Dict[str, Any], float]] = []
        for cand in self._extract_pool(value):
            candidate_text = self._candidate_text(cand)
            if self._llm_config.backend == "mock":
                score = lexical_similarity(query_text, candidate_text)
            else:
                await self._ensure_client()
                assert self._client is not None
                prompt = (
                    "You are scoring one candidate for a continuous semantic top-k query.\n"
                    "Return JSON only with key score and a float in [0, 1].\n"
                    f"Criterion: {self._query_spec.semantic.instruction or 'Score candidate relevance.'}\n"
                    f"Query: {query_text}\n"
                    f"Candidate: {candidate_text}\n"
                )
                text, _ = await self._client.call(prompt)
                parsed = _PointwiseLLMScorerWorker._parse_json_object(text)
                score = float(parsed.get("score", 0.0))
            ranked.append((cand, max(0.0, min(1.0, score))))
        return sorted(ranked, key=lambda item: item[1], reverse=True)

    async def _rank_contextual(self, value: Dict[str, Any]) -> List[Tuple[Dict[str, Any], float]]:
        query_text = self._query_text(value)
        pool = self._extract_pool(value)
        scored = [(cand, lexical_similarity(query_text, self._candidate_text(cand))) for cand in pool]
        if self._llm_config.backend != "mock":
            await self._ensure_client()
            assert self._client is not None
            lines = [f"- {cand['candidate_id']}: {self._candidate_text(cand)}" for cand in pool]
            prompt = (
                "You are reranking a bounded candidate pool for a continuous semantic top-k query.\n"
                "Return JSON only with key ranked_candidate_ids and a ranked list of candidate ids.\n"
                f"Method: {self._query_spec.ranking_method}\n"
                f"Criterion: {self._query_spec.semantic.instruction or 'Rerank bounded candidate pool.'}\n"
                f"Query: {query_text}\n"
                "Candidates:\n" + "\n".join(lines)
            )
            text, _ = await self._client.call(prompt)
            parsed = json.loads(text)
            ranked_ids = parsed.get("ranked_candidate_ids", []) if isinstance(parsed, dict) else []
            scored_map = {cand["candidate_id"]: (cand, score) for cand, score in scored}
            ranked = [scored_map.pop(cid) for cid in ranked_ids if cid in scored_map]
            ranked.extend(scored_map.values())
            return ranked
        if self._query_spec.ranking_method == "pairwise":
            return _pairwise_rank(scored)
        return sorted(scored, key=lambda item: item[1], reverse=True)

    async def async_invoke(self, value):
        if not is_topk_candidate_pool(value):
            raise ValueError("topk_snapshot_worker_requires_bounded_pool")
        try:
            if self._query_spec.ranking_method == "pointwise":
                ranked = await self._rank_pointwise(value)
            else:
                ranked = await self._rank_contextual(value)
                if self._score_backend == "llm":
                    total = len(ranked)
                    ranked = [
                        (candidate, _position_score(idx, total))
                        for idx, (candidate, _) in enumerate(ranked)
                    ]
            if not ranked:
                raise ValueError("topk_empty_bounded_pool")
            return self._build_snapshot(
                value,
                ranked,
                source=f"topk_llm_{self._query_spec.ranking_method}",
                emission_policy="scope_close_final",
            )
        except Exception as exc:
            raise RuntimeError(
                f"topk_llm_{self._query_spec.ranking_method}_error: {exc}"
            ) from exc

    def timeout(self, value):
        raise TimeoutError(f"topk_llm_{self._query_spec.ranking_method}_timeout")


class _BoundedPoolEmbeddingTopKSnapshotWorker(_BaseBoundedPoolRerankerWorker):
    """Window-owned bounded-pool embedding scorer/reranker with final-only emission."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        embedding_config: Optional[EmbeddingBackendConfig] = None,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "embedding", score_field, candidate_text_fields)
        self._embedding_config = embedding_config or EmbeddingBackendConfig()
        dim = int(self._embedding_config.dimensions or 128)
        self._encoder = HashingTextEncoder(dim=max(dim, 1))

    def _candidate_text(self, value: Dict[str, Any]) -> str:
        return extract_topk_candidate_text(value, self._candidate_text_fields)

    async def async_invoke(self, value):
        if not is_topk_candidate_pool(value):
            raise ValueError("topk_snapshot_worker_requires_bounded_pool")

        backend = self._embedding_config.backend or "mock"
        if backend not in {"mock", "local_lexical", "local_hashing"}:
            raise ValueError(f"topk_embedding_backend_not_implemented: {backend}")

        query_text = self._query_text(value)
        scored: List[Tuple[Dict[str, Any], float]] = []
        for cand in self._extract_pool(value):
            candidate_text = self._candidate_text(cand)
            if backend == "mock":
                score = lexical_similarity(query_text, candidate_text)
            else:
                score = self._encoder.similarity(query_text, candidate_text)
            scored.append((cand, score))

        if self._query_spec.ranking_method == "pairwise":
            ranked = _pairwise_rank(scored)
        else:
            ranked = sorted(scored, key=lambda item: item[1], reverse=True)
        await asyncio.sleep(0)
        if not ranked:
            raise ValueError("topk_empty_bounded_pool")
        return self._build_snapshot(
            value,
            ranked,
            source=f"topk_embedding_{self._query_spec.ranking_method}",
            emission_policy="scope_close_final",
        )

    def timeout(self, value):
        raise TimeoutError(f"topk_embedding_{self._query_spec.ranking_method}_timeout")


class _BoundedPoolExternalTopKSnapshotWorker(_BaseBoundedPoolRerankerWorker):
    """Window-owned bounded-pool external-score executor with final-only emission."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, "external_score", score_field, candidate_text_fields)

    async def async_invoke(self, value):
        if not is_topk_candidate_pool(value):
            raise ValueError("topk_snapshot_worker_requires_bounded_pool")
        scored = []
        for cand in self._extract_pool(value):
            if self._score_field not in cand or cand[self._score_field] is None:
                raise ValueError(f"topk_missing_external_score:{self._score_field}")
            scored.append((cand, float(cand[self._score_field])))

        if self._query_spec.ranking_method == "pairwise":
            ranked = _pairwise_rank(scored)
        else:
            ranked = sorted(scored, key=lambda item: item[1], reverse=True)
        await asyncio.sleep(0)
        if not ranked:
            raise ValueError("topk_empty_bounded_pool")
        return self._build_snapshot(
            value,
            ranked,
            source=f"topk_external_{self._query_spec.ranking_method}",
            emission_policy="scope_close_final",
        )
