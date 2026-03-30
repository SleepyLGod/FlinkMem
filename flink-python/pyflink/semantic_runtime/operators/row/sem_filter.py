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
sem_filter — row-wise semantic filtering with confidence and reason.

The operator itself is **1:1**: every input produces exactly one output record
enriched with ``{decision, confidence, reason}``.  Actual filtering is done
downstream via ``DataStream.filter(lambda x: json.loads(x)["decision"])``.

This keeps the async operator output count deterministic and filtered-out
records remain available for quality auditing.

Public callers should use ``build_sem_filter_operator(...)`` so semantic
intent stays separate from internal backend configuration.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.metrics import OperatorMetrics
from pyflink.semantic_runtime.operators.row._common import attach_metrics, validate_generic_sem_spec
from pyflink.semantic_runtime.runtime.json_output import parse_llm_json_object
from pyflink.semantic_runtime.runtime.prompt_templates import build_sem_filter_prompt
from pyflink.semantic_runtime.sem_spec import SemSpec

if TYPE_CHECKING:
    from pyflink.semantic_runtime.runtime_config import RuntimeConfig

logger = logging.getLogger(__name__)

# Expected LLM response schema for filter decisions.
_FILTER_SCHEMA_KEYS = {"decision", "confidence", "reason"}


class SemFilterFunction(AsyncFunction):
    """Async semantic filter operator.

    Parameters
    ----------
    prompt_template : str
        Format-string with ``{input}`` placeholder.  The prompt should instruct
        the LLM to return JSON ``{decision: bool, confidence: float, reason: str}``.
    llm_config : LLMClientConfig
        Picklable LLM backend configuration.
    """

    def __init__(
        self,
        prompt_template: str,
        llm_config: LLMClientConfig,
    ) -> None:
        self._prompt_template = prompt_template
        self._llm_config = llm_config
        self._client: Optional[LLMClient] = None
        self._op_metrics: Optional[OperatorMetrics] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)
        self._op_metrics = OperatorMetrics.from_runtime_context(runtime_context, "sem_filter")
        logger.info("SemFilterFunction opened (backend=%s)", self._llm_config.backend)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- core ----------------------------------------------------------------

    async def async_invoke(self, value) -> List[str]:
        assert self._client is not None, "open() was not called"
        om = self._op_metrics

        prompt = self._prompt_template.format(input=value)

        try:
            text, metrics = await self._client.call(prompt)
        except Exception as e:
            logger.warning("LLM call failed for sem_filter: %s", e)
            if om:
                om.record_error()
            raise RuntimeError(f"sem_filter LLM call failed: {e}") from e

        if om:
            om.record_call(metrics.latency_ms, metrics.input_tokens,
                           metrics.output_tokens, metrics.attempts)

        # Parse JSON
        try:
            parsed = parse_llm_json_object(text, operator_name="sem_filter")
        except ValueError as e:
            logger.warning("sem_filter JSON parse failed: %s", e)
            if om:
                om.record_invalid_output()
            raise ValueError(str(e)) from e

        # Validate required keys
        if not isinstance(parsed, dict) or not _FILTER_SCHEMA_KEYS.issubset(parsed):
            logger.warning("sem_filter schema validation failed: %s", parsed)
            if om:
                om.record_invalid_output()
            raise ValueError("sem_filter response violates required schema")

        # Normalise types
        parsed["decision"] = bool(parsed["decision"])
        parsed["confidence"] = float(parsed["confidence"])
        parsed["reason"] = str(parsed["reason"])
        parsed["_input"] = value

        attach_metrics(parsed, metrics)
        return [json.dumps(parsed)]

    def timeout(self, value) -> List[str]:
        """Fail fast on Flink-level timeout."""
        if self._op_metrics:
            self._op_metrics.record_timeout()
        raise TimeoutError(f"sem_filter timed out for input: {value!r}")


def build_sem_filter_operator(
    semantic: SemSpec,
    runtime_config: "RuntimeConfig",
) -> SemFilterFunction:
    """Build a public row-style semantic filter operator."""
    validate_generic_sem_spec(
        semantic,
        operator_name="sem_filter",
        allowed_output_modes={"bool"},
    )
    from pyflink.semantic_runtime.public_api import sem_filter
    from pyflink.semantic_runtime.runtime.plans import lower_sem_filter_request

    plan = lower_sem_filter_request(
        sem_filter(intent=semantic.instruction),
        runtime_config,
    )
    return SemFilterFunction(
        prompt_template=build_sem_filter_prompt(plan.intent),
        llm_config=plan.llm_config,
    )
