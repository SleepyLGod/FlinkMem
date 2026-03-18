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
sem_topk pipeline builder — scoring orchestration around the pure top-k kernel.

This module keeps :class:`SemTopKFunction` pure: the kernel only consumes
already-scored candidate records and maintains the keyed top-k frontier.
Scoring orchestration lives here.

Input contract
--------------
The builder accepts a mixed stream of dict-shaped records:

- scored or unscored flat candidate dicts (must carry ``candidate_id``)
- passthrough dicts that are not candidates (for example degraded retrieval
  envelopes with zero candidates)

Output contract
---------------
The returned stream is a union of:

- top-k snapshot records emitted by :class:`SemTopKFunction`
- passthrough/degraded records that bypass the kernel

This keeps workflow-level error propagation outside the kernel boundary.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, Iterable, Optional, Sequence

from pyflink.common import Types
from pyflink.datastream import AsyncDataStream, DataStream
from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.semantic_runtime.llm_client import LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime_config import EmbeddingBackendConfig
from pyflink.semantic_runtime.semantic_spec import TopKQuerySpec
from pyflink.semantic_runtime.stateful.simple_text_encoder import (
    HashingTextEncoder,
    tokenize_text,
)
from pyflink.semantic_runtime.stateful.sem_topk_continuous import (
    SemTopKConfig,
    SemTopKFunction,
)

logger = logging.getLogger(__name__)


DEFAULT_CANDIDATE_TEXT_FIELDS: tuple[str, ...] = ("text", "payload", "content")


def is_topk_candidate_record(value: Any) -> bool:
    """Return True when *value* is a flat candidate record for top-k."""
    return isinstance(value, dict) and bool(value.get("candidate_id"))


def topk_candidate_has_score(value: Dict[str, Any], score_field: str = "score") -> bool:
    """Return True when the candidate already carries a usable score."""
    return is_topk_candidate_record(value) and score_field in value and value[score_field] is not None


def topk_candidate_needs_scoring(
    value: Dict[str, Any],
    query_spec: TopKQuerySpec,
    score_field: str = "score",
) -> bool:
    """Decide whether the candidate must be re-scored for the active query."""
    if not is_topk_candidate_record(value):
        return False

    backend = query_spec.semantic.backend
    if backend == "external_score":
        return not topk_candidate_has_score(value, score_field)

    if not topk_candidate_has_score(value, score_field):
        return True

    return (
        value.get("_query_version") != query_spec.query_version
        or value.get("_score_backend") != backend
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
    out["_score_backend"] = query_spec.semantic.backend
    out["_updated_ms"] = now_ms
    out["source"] = source
    out["degraded"] = False
    out["error"] = ""
    return out


def build_topk_passthrough_record(
    value: Dict[str, Any],
    *,
    source: str,
    error: str,
) -> Dict[str, Any]:
    """Wrap a failed/missing-score candidate as a degraded passthrough record."""
    return {
        "key": value.get("key", ""),
        "query": value.get("query", ""),
        "query_seq_id": int(value.get("query_seq_id", 0)),
        "candidates": [],
        "candidate_count": 0,
        "source": source,
        "degraded": True,
        "error": error,
        "timestamp_ms": int(time.time() * 1000),
    }


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


class _BaseTopKScorerWorker(AsyncFunction):
    """Shared utilities for async candidate scorers."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        self._query_spec = query_spec
        self._score_field = score_field
        self._candidate_text_fields = tuple(candidate_text_fields)

    def _extract_candidate_text(self, value: Dict[str, Any]) -> str:
        return extract_topk_candidate_text(value, self._candidate_text_fields)

    def _query_text(self, value: Dict[str, Any]) -> str:
        return resolve_topk_query_text(value, self._query_spec)


class _PointwiseLLMScorerWorker(_BaseTopKScorerWorker):
    """Pointwise LLM scorer for continuous top-k."""

    def __init__(
        self,
        query_spec: TopKQuerySpec,
        llm_config: LLMClientConfig,
        score_field: str = "score",
        candidate_text_fields: Sequence[str] = DEFAULT_CANDIDATE_TEXT_FIELDS,
    ) -> None:
        super().__init__(query_spec, score_field, candidate_text_fields)
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
            return [value]

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
                    score_field=self._score_field,
                    source="topk_llm_pointwise",
                )
            ]
        except Exception as exc:
            return [
                build_topk_passthrough_record(
                    value,
                    source="topk_llm_pointwise",
                    error=f"topk_llm_score_error: {exc}",
                )
            ]

    def timeout(self, value):
        return [
            build_topk_passthrough_record(
                value,
                source="topk_llm_pointwise",
                error="topk_llm_score_timeout",
            )
        ]


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
        super().__init__(query_spec, score_field, candidate_text_fields)
        self._embedding_config = embedding_config or EmbeddingBackendConfig()
        dim = int(self._embedding_config.dimensions or 128)
        self._encoder = HashingTextEncoder(dim=max(dim, 1))

    async def async_invoke(self, value):
        if not is_topk_candidate_record(value):
            return [value]

        backend = self._embedding_config.backend or "mock"
        if backend not in {"mock", "local_lexical", "local_hashing"}:
            return [
                build_topk_passthrough_record(
                    value,
                    source="topk_embedding_pointwise",
                    error=f"topk_embedding_backend_not_implemented: {backend}",
                )
            ]

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
                score_field=self._score_field,
                source="topk_embedding_pointwise",
            )
        ]

    def timeout(self, value):
        return [
            build_topk_passthrough_record(
                value,
                source="topk_embedding_pointwise",
                error="topk_embedding_score_timeout",
            )
        ]


def _mark_missing_external_score(value: Dict[str, Any], score_field: str) -> Dict[str, Any]:
    return build_topk_passthrough_record(
        value,
        source="topk_external_score",
        error=f"topk_missing_external_score:{score_field}",
    )


def build_sem_topk_pipeline(
    input_ds: DataStream,
    *,
    key_selector: Callable,
    topk_config: SemTopKConfig,
    query_spec: TopKQuerySpec,
    llm_config: Optional[LLMClientConfig] = None,
    embedding_config: Optional[EmbeddingBackendConfig] = None,
    async_timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Build the scored-candidate pipeline around the pure top-k kernel.

    Parameters
    ----------
    input_ds : DataStream
        Stream of flat candidates and/or passthrough dicts.
    key_selector : callable
        Key selector for the final keyed top-k kernel.
    topk_config : SemTopKConfig
        Kernel configuration.
    query_spec : TopKQuerySpec
        Continuous top-k query definition.
    llm_config : LLMClientConfig, optional
        Required when ``query_spec.semantic.backend == "llm"``.
    embedding_config : EmbeddingBackendConfig, optional
        Optional embedding scorer config. Current implementation supports
        only ``backend="mock"`` / ``"local_lexical"``.
    async_timeout_ms : int
        Async scoring timeout in milliseconds.
    async_capacity : int
        Max concurrent async scoring requests.
    """
    score_field = topk_config.score_field
    backend = query_spec.semantic.backend

    candidates = input_ds.filter(
        lambda v: is_topk_candidate_record(v),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    passthrough = input_ds.filter(
        lambda v: not is_topk_candidate_record(v),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    if backend == "external_score":
        ready = candidates.filter(
            lambda v: topk_candidate_has_score(v, score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        missing = candidates.filter(
            lambda v: not topk_candidate_has_score(v, score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        ).map(
            lambda v: _mark_missing_external_score(v, score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        passthrough_total = passthrough.union(missing)
        kernel_input = ready
    else:
        ready = candidates.filter(
            lambda v: not topk_candidate_needs_scoring(v, query_spec, score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        to_score = candidates.filter(
            lambda v: topk_candidate_needs_scoring(v, query_spec, score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )

        if backend == "llm":
            if llm_config is None:
                raise ValueError("llm_config is required for sem_topk backend='llm'")
            scorer_fn = _PointwiseLLMScorerWorker(query_spec, llm_config, score_field)
        elif backend == "embedding":
            scorer_fn = _EmbeddingScorerWorker(query_spec, embedding_config, score_field)
        else:
            raise ValueError(f"Unsupported sem_topk backend: {backend!r}")

        scored_or_passthrough = AsyncDataStream.unordered_wait(
            to_score, scorer_fn, async_timeout_ms, async_capacity,
        )

        scored_ready = scored_or_passthrough.filter(
            lambda v: is_topk_candidate_record(v) and topk_candidate_has_score(v, score_field),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        async_passthrough = scored_or_passthrough.filter(
            lambda v: not (is_topk_candidate_record(v) and topk_candidate_has_score(v, score_field)),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        passthrough_total = passthrough.union(async_passthrough)
        kernel_input = ready.union(scored_ready)

    reranked = kernel_input.key_by(key_selector).process(
        SemTopKFunction(topk_config, query_spec),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    return reranked.union(passthrough_total)
