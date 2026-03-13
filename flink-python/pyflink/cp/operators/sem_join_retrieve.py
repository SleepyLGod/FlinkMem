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
sem_join_retrieve — retrieve-backed semantic join (V0.1 local variant).

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
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.cp.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.cp.metrics import OperatorMetrics
from pyflink.cp.operators._common import attach_metrics, make_degraded_json

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Retriever abstraction
# ---------------------------------------------------------------------------

class CandidateRetriever(ABC):
    """Interface for candidate retrieval (vector DB, search index, etc.)."""

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


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class SemJoinRetrieveConfig:
    """Picklable configuration for SemJoinRetrieveFunction."""
    max_candidates_per_record: int = 20
    retrieve_timeout_ms: float = 5000.0
    # mock-specific
    mock_candidates: Optional[List[Any]] = None
    mock_retrieve_delay_s: float = 0.05


# ---------------------------------------------------------------------------
# SemJoinRetrieveFunction
# ---------------------------------------------------------------------------

class SemJoinRetrieveFunction(AsyncFunction):
    """Async retrieve-backed semantic join operator.

    Parameters
    ----------
    prompt_template : str
        Format-string with ``{input}`` and ``{candidates}`` placeholders.
    llm_config : LLMClientConfig
        Picklable LLM backend configuration.
    join_config : SemJoinRetrieveConfig
        Retrieval bounds and retriever settings.
    """

    def __init__(
        self,
        prompt_template: str,
        llm_config: LLMClientConfig,
        join_config: SemJoinRetrieveConfig,
    ) -> None:
        self._prompt_template = prompt_template
        self._llm_config = llm_config
        self._join_config = join_config
        self._client: Optional[LLMClient] = None
        self._retriever: Optional[CandidateRetriever] = None
        self._op_metrics: Optional[OperatorMetrics] = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)
        self._op_metrics = OperatorMetrics.from_runtime_context(runtime_context, "sem_join_retrieve")
        cfg = self._join_config
        # V0.1: only mock retriever; real implementations plugged in later.
        if cfg.mock_candidates is not None:
            self._retriever = MockCandidateRetriever(
                cfg.mock_candidates, cfg.mock_retrieve_delay_s)
        else:
            self._retriever = MockCandidateRetriever([], 0.0)
        logger.info("SemJoinRetrieveFunction opened (max_cand=%d, timeout=%dms)",
                     cfg.max_candidates_per_record, cfg.retrieve_timeout_ms)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

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
            logger.warning("sem_join_retrieve: retrieval timed out for %s", value)
            if om:
                om.record_timeout()
            return [make_degraded_json(value, "retrieve_timeout")]
        except Exception as e:
            logger.warning("sem_join_retrieve: retrieval error: %s", e)
            if om:
                om.record_error()
            return [make_degraded_json(value, f"retrieve_error: {e}")]

        # 2. Enforce hard cap + truncate overflow
        if len(candidates) > cfg.max_candidates_per_record:
            candidates = candidates[: cfg.max_candidates_per_record]
            truncated = True
            logger.info("sem_join_retrieve: truncated to %d candidates",
                        cfg.max_candidates_per_record)

        # 3. LLM semantic matching
        prompt = self._prompt_template.format(
            input=value,
            candidates=json.dumps(candidates),
        )

        try:
            text, metrics = await self._client.call(prompt)
        except Exception as e:
            logger.warning("LLM call failed for sem_join_retrieve: %s", e)
            if om:
                om.record_error()
            return [make_degraded_json(value, f"llm_call_error: {e}")]

        if om:
            om.record_call(metrics.latency_ms, metrics.input_tokens,
                           metrics.output_tokens, metrics.attempts)

        # 4. Parse response
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("sem_join_retrieve JSON parse failed: %s", e)
            if om:
                om.record_invalid_output()
            return [make_degraded_json(value, f"json_parse_error: {e}")]

        result = {
            "_input": value,
            "join_result": parsed,
            "candidate_count": len(candidates),
            "truncated": truncated,
        }
        attach_metrics(result, metrics)
        return [json.dumps(result)]

    def timeout(self, value) -> List[str]:
        """Return degraded record on Flink-level timeout — never throw."""
        if self._op_metrics:
            self._op_metrics.record_timeout()
        return [make_degraded_json(value, "flink_timeout")]

