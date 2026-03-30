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
sem_lookup_join — retrieve-backed semantic lookup join (V0.1 local variant).

For each input record the operator:
1. Calls an external retriever (vector store / DB) to fetch candidate records.
2. Sends the input + candidates to the LLM for semantic matching / joining.

Hard bounds enforced at this layer:
- ``max_candidates_per_record`` — cap on retriever results.
- ``retrieve_timeout_ms`` — strict budget for the retrieval call.
- Overflow policy: ``truncate`` with explicit metric tag.

True two-input ``sem_join`` (dual-side state) is deferred to V0.3a.

"""

from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.metrics import OperatorMetrics
from pyflink.semantic_runtime.operators.row._common import attach_metrics
from pyflink.semantic_runtime.runtime.json_output import parse_llm_json_object
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder
from pyflink.semantic_runtime.runtime.external_search_backend import (
    ExternalSearchBackend,
    SearchResult,
)

logger = logging.getLogger(__name__)

_VALID_MATCH_BACKENDS = {"llm", "embedding_only"}
DEFAULT_LOOKUP_JOIN_EMBEDDING_THRESHOLD = 0.7
DEFAULT_LOOKUP_JOIN_EMBEDDING_DIM = 128


# ---------------------------------------------------------------------------
# Retriever abstraction
# ---------------------------------------------------------------------------

class CandidateRetriever(ABC):
    """Interface for candidate retrieval (vector DB, search index, etc.)."""

    def open(self) -> None:
        """Optional lifecycle hook."""

    def close(self) -> None:
        """Optional lifecycle hook."""

    @abstractmethod
    async def retrieve(self, query: str, max_results: int) -> List[Any]:
        """Return up to *max_results* candidate records for *query*."""
        ...


class MockCandidateRetriever(CandidateRetriever):
    """Deterministic mock retriever for dev/test."""

    def __init__(self, fixed_candidates: List[Any], delay_s: float = 0.05):
        self._candidates = fixed_candidates
        self._delay = delay_s

    async def retrieve(self, query: str, max_results: int) -> List[Any]:
        await asyncio.sleep(self._delay)
        return self._candidates[:max_results]


class CandidateRetrieverFromSearchBackend(CandidateRetriever):
    """Compatibility adapter: reuse ExternalSearchBackend inside V0.1 lookup join."""

    def __init__(self, backend: ExternalSearchBackend) -> None:
        self._backend = backend

    def open(self) -> None:
        self._backend.open()

    def close(self) -> None:
        self._backend.close()

    async def retrieve(self, query: str, max_results: int) -> List[Any]:
        results = await self._backend.search(query, top_k=max_results)
        converted: List[Any] = []
        for result in results:
            sr = SearchResult.from_any(result)
            converted.append(
                {
                    "candidate_id": sr.candidate_id,
                    "content": sr.text,
                    "text": sr.text,
                    "score": float(sr.score),
                    "metadata": dict(sr.metadata),
                }
            )
        return converted


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class SemLookupJoinConfig:
    """Picklable configuration for SemLookupJoinFunction."""
    max_candidates_per_record: int = 20
    retrieve_timeout_ms: float = 5000.0
    left_block_size: int = 1
    right_block_size: Optional[int] = None
    match_backend: str = "llm"
    embedding_similarity_threshold: float = DEFAULT_LOOKUP_JOIN_EMBEDDING_THRESHOLD
    embedding_dim: int = DEFAULT_LOOKUP_JOIN_EMBEDDING_DIM
    search_backend: Optional[ExternalSearchBackend] = None
    # mock-specific
    mock_candidates: Optional[List[Any]] = None
    mock_retrieve_delay_s: float = 0.05


# ---------------------------------------------------------------------------
# SemLookupJoinFunction
# ---------------------------------------------------------------------------

class SemLookupJoinFunction(AsyncFunction):
    """Async retrieve-backed semantic lookup join operator.

    Parameters
    ----------
    prompt_template : str
        Format-string with ``{input}`` and ``{candidates}`` placeholders.
    llm_config : LLMClientConfig
        Picklable LLM backend configuration.
    join_config : SemLookupJoinConfig
        Retrieval bounds and retriever settings.
    """

    def __init__(
        self,
        prompt_template: str,
        llm_config: LLMClientConfig,
        join_config: SemLookupJoinConfig,
    ) -> None:
        self._prompt_template = prompt_template
        self._llm_config = llm_config
        self._join_config = join_config
        self._client: Optional[LLMClient] = None
        self._retriever: Optional[CandidateRetriever] = None
        self._op_metrics: Optional[OperatorMetrics] = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)
        self._op_metrics = OperatorMetrics.from_runtime_context(runtime_context, "sem_lookup_join")
        cfg = self._join_config
        if cfg.left_block_size != 1:
            raise ValueError("row-level sem_lookup_join requires left_block_size == 1")
        if cfg.right_block_size is not None and cfg.right_block_size <= 0:
            raise ValueError("sem_lookup_join requires right_block_size > 0 when set")
        if cfg.match_backend not in _VALID_MATCH_BACKENDS:
            raise ValueError(
                f"sem_lookup_join requires match_backend in {_VALID_MATCH_BACKENDS}, "
                f"got {cfg.match_backend!r}"
            )
        if not 0.0 <= float(cfg.embedding_similarity_threshold) <= 1.0:
            raise ValueError(
                "sem_lookup_join requires embedding_similarity_threshold in [0.0, 1.0]"
            )
        if int(cfg.embedding_dim) <= 0:
            raise ValueError("sem_lookup_join requires embedding_dim > 0")
        if cfg.search_backend is not None:
            self._retriever = CandidateRetrieverFromSearchBackend(cfg.search_backend)
        elif cfg.mock_candidates is not None:
            self._retriever = MockCandidateRetriever(
                cfg.mock_candidates, cfg.mock_retrieve_delay_s)
        else:
            raise ValueError(
                "sem_lookup_join requires either search_backend or mock_candidates"
            )
        self._retriever.open()
        logger.info(
            "SemLookupJoinFunction opened (max_cand=%d, timeout=%dms, right_block_size=%s)",
            cfg.max_candidates_per_record,
            cfg.retrieve_timeout_ms,
            cfg.right_block_size,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._retriever is not None:
            self._retriever.close()

    # -- core ----------------------------------------------------------------

    async def async_invoke(self, value) -> List[str]:
        assert self._client is not None, "open() was not called"
        assert self._retriever is not None
        om = self._op_metrics

        cfg = self._join_config
        truncated = False

        # 1. Retrieve candidates with strict timeout
        try:
            candidates = await asyncio.wait_for(
                self._retriever.retrieve(
                    str(value), cfg.max_candidates_per_record),
                timeout=cfg.retrieve_timeout_ms / 1000.0,
            )
        except asyncio.TimeoutError:
            logger.warning("sem_lookup_join: retrieval timed out for %s", value)
            if om:
                om.record_timeout()
            raise TimeoutError(f"sem_lookup_join retrieval timed out for input: {value!r}")
        except Exception as e:
            logger.warning("sem_lookup_join: retrieval error: %s", e)
            if om:
                om.record_error()
            raise RuntimeError(f"sem_lookup_join retrieval failed: {e}") from e

        # 2. Enforce hard cap + truncate overflow
        if len(candidates) > cfg.max_candidates_per_record:
            candidates = candidates[: cfg.max_candidates_per_record]
            truncated = True
            logger.info("sem_lookup_join: truncated to %d candidates",
                        cfg.max_candidates_per_record)

        # 3. Pair-block semantic matching
        if cfg.match_backend == "embedding_only":
            parsed = self._evaluate_candidate_blocks_with_embedding(value, candidates)
        elif cfg.match_backend == "llm":
            parsed = await self._evaluate_candidate_blocks(value, candidates, om)
        else:
            raise RuntimeError(
                f"sem_lookup_join received unsupported match_backend={cfg.match_backend!r}"
            )

        result = {
            "_input": value,
            "join_result": parsed,
            "candidate_count": len(candidates),
            "truncated": truncated,
        }
        return [json.dumps(result)]

    def timeout(self, value) -> List[str]:
        """Fail fast on Flink-level timeout."""
        if self._op_metrics:
            self._op_metrics.record_timeout()
        raise TimeoutError(f"sem_lookup_join timed out for input: {value!r}")

    async def _evaluate_candidate_blocks(
        self,
        value: Any,
        candidates: List[Any],
        metrics_sink: Optional[OperatorMetrics],
    ) -> Dict[str, Any]:
        """Evaluate bounded candidate blocks and return the best join result."""
        best_result: Optional[Dict[str, Any]] = None
        for block in self._candidate_blocks(candidates):
            block_result = await self._evaluate_one_block(value, block, metrics_sink)
            if best_result is None or float(block_result["match_score"]) > float(best_result["match_score"]):
                best_result = block_result

        if best_result is None:
            return {
                "matched": False,
                "match_score": 0.0,
                "selected_candidate": None,
                "reason": "no candidates",
            }
        return best_result

    def _candidate_blocks(self, candidates: List[Any]) -> List[List[Any]]:
        """Split candidates into right-side evaluation blocks."""
        block_size = self._join_config.right_block_size or len(candidates) or 1
        return [
            candidates[start : start + block_size]
            for start in range(0, len(candidates), block_size)
        ]

    def _evaluate_candidate_blocks_with_embedding(
        self,
        value: Any,
        candidates: List[Any],
    ) -> Dict[str, Any]:
        """Evaluate candidate blocks using local embedding-style similarity only."""
        best_result: Optional[Dict[str, Any]] = None
        for block in self._candidate_blocks(candidates):
            block_result = self._evaluate_one_block_with_embedding(value, block)
            if best_result is None or float(block_result["match_score"]) > float(best_result["match_score"]):
                best_result = block_result
        if best_result is None:
            return {
                "matched": False,
                "match_score": 0.0,
                "selected_candidate": None,
                "reason": "no candidates",
            }
        return best_result

    def _evaluate_one_block_with_embedding(
        self,
        value: Any,
        candidates: List[Any],
    ) -> Dict[str, Any]:
        """Evaluate one candidate block using local hashing-embedding similarity."""
        if not candidates:
            return {
                "matched": False,
                "match_score": 0.0,
                "selected_candidate": None,
                "reason": "no candidates",
            }
        encoder = HashingTextEncoder(dim=int(self._join_config.embedding_dim))
        query_text = str(value)
        threshold = float(self._join_config.embedding_similarity_threshold)
        best_candidate: Optional[Any] = None
        best_score = float("-inf")
        for candidate in candidates:
            candidate_text = self._candidate_text(candidate)
            score = float(encoder.similarity(query_text, candidate_text))
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if best_candidate is None:
            raise RuntimeError("sem_lookup_join embedding backend failed to score candidates")
        return {
            "matched": bool(best_score >= threshold),
            "match_score": float(best_score),
            "selected_candidate": best_candidate,
            "reason": "embedding_similarity",
        }

    def _candidate_text(self, candidate: Any) -> str:
        """Extract candidate text used by embedding-only matching."""
        if isinstance(candidate, str):
            return candidate
        if isinstance(candidate, dict):
            for field in ("content", "text", "payload"):
                if field in candidate:
                    return str(candidate[field])
            raise ValueError(
                "sem_lookup_join embedding backend requires candidate dict to "
                "contain one of {'content', 'text', 'payload'}"
            )
        if hasattr(candidate, "text"):
            return str(candidate.text)
        raise TypeError(
            "sem_lookup_join embedding backend requires candidate to be str/dict/text-like object"
        )

    async def _evaluate_one_block(
        self,
        value: Any,
        candidates: List[Any],
        metrics_sink: Optional[OperatorMetrics],
    ) -> Dict[str, Any]:
        """Evaluate one candidate block with the semantic match backend."""
        assert self._client is not None, "open() was not called"

        prompt = self._prompt_template.format(
            input=value,
            candidates=json.dumps(candidates),
        )
        try:
            text, metrics = await self._client.call(prompt)
        except Exception as exc:
            logger.warning("LLM call failed for sem_lookup_join: %s", exc)
            if metrics_sink:
                metrics_sink.record_error()
            raise RuntimeError(f"sem_lookup_join LLM call failed: {exc}") from exc

        if metrics_sink:
            metrics_sink.record_call(
                metrics.latency_ms,
                metrics.input_tokens,
                metrics.output_tokens,
                metrics.attempts,
            )

        try:
            parsed = parse_llm_json_object(text, operator_name="sem_lookup_join")
        except ValueError as exc:
            logger.warning("sem_lookup_join JSON parse failed: %s", exc)
            if metrics_sink:
                metrics_sink.record_invalid_output()
            raise ValueError(str(exc)) from exc

        return self._validate_block_result(parsed)

    def _validate_block_result(self, parsed: Any) -> Dict[str, Any]:
        """Validate one pair-block result."""
        if not isinstance(parsed, dict):
            raise ValueError("sem_lookup_join expected JSON object output")
        required_fields = {"matched", "match_score", "selected_candidate", "reason"}
        if not required_fields.issubset(parsed):
            raise ValueError("sem_lookup_join response violates required block schema")
        return {
            "matched": bool(parsed["matched"]),
            "match_score": float(parsed["match_score"]),
            "selected_candidate": parsed["selected_candidate"],
            "reason": str(parsed["reason"]),
        }
