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
sem_map — row-wise semantic extraction / classification / transformation.

Maps each input record to a structured output by calling an LLM with a
prompt template and parsing the response against an expected JSON schema.

Design notes
------------
* ``__init__`` stores only picklable config (no live objects).
* ``LLMClient`` is created in ``open()`` to survive cloudpickle serialisation.
* ``timeout()`` returns a degraded record with an ``_error`` tag — never throws.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.cp.llm_client import LLMCallMetrics, LLMClient, LLMClientConfig, create_llm_client
from pyflink.cp.metrics import OperatorMetrics
from pyflink.cp.operators._common import (
    attach_metrics, make_degraded_json, validate_schema,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SemMapFunction
# ---------------------------------------------------------------------------

class SemMapFunction(AsyncFunction):
    """Async semantic map operator.

    Parameters
    ----------
    prompt_template : str
        A Python format-string with a ``{input}`` placeholder.
        Example: ``"Extract the sentiment from: {input}"``
    output_schema : dict[str, type]
        Expected keys and their Python types in the parsed LLM response.
        Example: ``{"sentiment": str, "confidence": float}``
    llm_config : LLMClientConfig
        Picklable LLM backend configuration.
    """

    def __init__(
        self,
        prompt_template: str,
        output_schema: Dict[str, type],
        llm_config: LLMClientConfig,
    ) -> None:
        # Everything here must be picklable.
        self._prompt_template = prompt_template
        self._output_schema = output_schema
        self._llm_config = llm_config
        # Initialised in open().
        self._client: Optional[LLMClient] = None
        self._op_metrics: Optional[OperatorMetrics] = None

    # -- lifecycle -----------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)
        self._op_metrics = OperatorMetrics.from_runtime_context(runtime_context, "sem_map")
        logger.info("SemMapFunction opened (backend=%s)", self._llm_config.backend)

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
            logger.warning("LLM call failed for sem_map: %s", e)
            if om:
                om.record_error()
            return [make_degraded_json(value, f"llm_call_error: {e}")]

        if om:
            om.record_call(metrics.latency_ms, metrics.input_tokens,
                           metrics.output_tokens, metrics.attempts)

        # Parse JSON response
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("sem_map JSON parse failed: %s", e)
            if om:
                om.record_invalid_output()
            return [make_degraded_json(value, f"json_parse_error: {e}")]

        # Validate schema
        if not validate_schema(parsed, self._output_schema):
            logger.warning("sem_map schema validation failed for: %s", parsed)
            if om:
                om.record_invalid_output()
            return [make_degraded_json(value, "schema_validation_error")]

        attach_metrics(parsed, metrics)
        return [json.dumps(parsed)]

    def timeout(self, value) -> List[str]:
        """Return degraded record on Flink-level timeout — never throw."""
        if self._op_metrics:
            self._op_metrics.record_timeout()
        return [make_degraded_json(value, "flink_timeout")]

