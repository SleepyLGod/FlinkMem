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
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.cp.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.cp.operators._common import attach_metrics, make_degraded_json

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
    default_decision : bool
        Decision used in degraded / timeout records (default ``False`` = reject).
    """

    def __init__(
        self,
        prompt_template: str,
        llm_config: LLMClientConfig,
        default_decision: bool = False,
    ) -> None:
        self._prompt_template = prompt_template
        self._llm_config = llm_config
        self._default_decision = default_decision
        self._client: Optional[LLMClient] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)
        logger.info("SemFilterFunction opened (backend=%s)", self._llm_config.backend)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- helpers -------------------------------------------------------------

    def _degraded_result(self, value, error: str) -> str:
        """Degraded record preserving 1:1 contract with default decision."""
        out = {
            "_input": value,
            "_error": error,
            "_degraded": True,
            "decision": self._default_decision,
            "confidence": 0.0,
            "reason": error,
        }
        return json.dumps(out)

    # -- core ----------------------------------------------------------------

    async def async_invoke(self, value) -> List[str]:
        assert self._client is not None, "open() was not called"

        prompt = self._prompt_template.format(input=value)

        try:
            text, metrics = await self._client.call(prompt)
        except Exception as e:
            logger.warning("LLM call failed for sem_filter: %s", e)
            return [self._degraded_result(value, f"llm_call_error: {e}")]

        # Parse JSON
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("sem_filter JSON parse failed: %s", e)
            return [self._degraded_result(value, f"json_parse_error: {e}")]

        # Validate required keys
        if not isinstance(parsed, dict) or not _FILTER_SCHEMA_KEYS.issubset(parsed):
            logger.warning("sem_filter schema validation failed: %s", parsed)
            return [self._degraded_result(value, "schema_validation_error")]

        # Normalise types
        parsed["decision"] = bool(parsed["decision"])
        parsed["confidence"] = float(parsed["confidence"])
        parsed["reason"] = str(parsed["reason"])
        parsed["_input"] = value

        attach_metrics(parsed, metrics)
        return [json.dumps(parsed)]

    def timeout(self, value) -> List[str]:
        """Return degraded record on Flink-level timeout — never throw."""
        return [self._degraded_result(value, "flink_timeout")]

