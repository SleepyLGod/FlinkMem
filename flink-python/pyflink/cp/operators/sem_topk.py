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
sem_topk — local (row-level) LLM-based reranking of a candidate list.

The input record is expected to be a JSON string carrying a ``candidates``
list.  The LLM reranks these candidates and returns the top-k.

This is the **local** V0.1 variant (no keyed state).  A continuous,
incremental ``sem_topk`` using ``KeyedProcessFunction`` is deferred to V0.2.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.cp.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.cp.metrics import OperatorMetrics
from pyflink.cp.operators._common import attach_metrics, make_degraded_json

logger = logging.getLogger(__name__)


class SemTopKFunction(AsyncFunction):
    """Async local semantic top-k reranker.

    Parameters
    ----------
    prompt_template : str
        Format-string with ``{input}`` and ``{candidates}`` placeholders.
    k : int
        Number of top results to return.
    llm_config : LLMClientConfig
        Picklable LLM backend configuration.
    candidates_field : str
        JSON key in the input record that holds the candidate list
        (default ``"candidates"``).
    """

    def __init__(
        self,
        prompt_template: str,
        k: int,
        llm_config: LLMClientConfig,
        candidates_field: str = "candidates",
    ) -> None:
        self._prompt_template = prompt_template
        self._k = k
        self._llm_config = llm_config
        self._candidates_field = candidates_field
        self._client: Optional[LLMClient] = None
        self._op_metrics: Optional[OperatorMetrics] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)
        self._op_metrics = OperatorMetrics.from_runtime_context(runtime_context, "sem_topk")
        logger.info("SemTopKFunction opened (backend=%s, k=%d)",
                     self._llm_config.backend, self._k)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- core ----------------------------------------------------------------

    async def async_invoke(self, value) -> List[str]:
        assert self._client is not None, "open() was not called"
        om = self._op_metrics

        # Parse input to extract candidates
        try:
            record = json.loads(value) if isinstance(value, str) else value
            candidates = record[self._candidates_field]
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            logger.warning("sem_topk input parse failed: %s", e)
            if om:
                om.record_invalid_output()
            return [make_degraded_json(value, f"input_parse_error: {e}")]

        prompt = self._prompt_template.format(
            input=json.dumps(record),
            candidates=json.dumps(candidates),
        )

        try:
            text, metrics = await self._client.call(prompt)
        except Exception as e:
            logger.warning("LLM call failed for sem_topk: %s", e)
            if om:
                om.record_error()
            return [make_degraded_json(value, f"llm_call_error: {e}")]

        if om:
            om.record_call(metrics.latency_ms, metrics.input_tokens,
                           metrics.output_tokens, metrics.attempts)

        # Parse LLM response — expect a JSON list of ranked items
        try:
            ranked = json.loads(text)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("sem_topk JSON parse failed: %s", e)
            if om:
                om.record_invalid_output()
            return [make_degraded_json(value, f"json_parse_error: {e}")]

        if not isinstance(ranked, list):
            logger.warning("sem_topk expected list, got %s", type(ranked).__name__)
            if om:
                om.record_invalid_output()
            return [make_degraded_json(value, "expected_list_response")]

        # Truncate to k
        top = ranked[: self._k]

        result = {
            "_input": value,
            "top_k": top,
            "k": self._k,
            "original_count": len(candidates),
        }
        attach_metrics(result, metrics)
        return [json.dumps(result)]

    def timeout(self, value) -> List[str]:
        """Return degraded record on Flink-level timeout — never throw."""
        if self._op_metrics:
            self._op_metrics.record_timeout()
        return [make_degraded_json(value, "flink_timeout")]

