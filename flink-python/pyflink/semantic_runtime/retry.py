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
Retry ownership split for CP semantic operators.

Two retry layers exist, each owning **disjoint** error classes:

Layer 1 — LLMClient (inside ``async_invoke``)
    Retries: HTTP 429/503, connection timeout, network errors.
    Scope:   transient API/network failures only.
    Config:  ``LLMClientConfig.max_retries``, ``retry_base_delay_s``.

Layer 2 — Flink AsyncRetryStrategy (outside ``async_invoke``)
    Retries: operator-level degraded results that are NOT caused by
             errors already exhausted at Layer 1.
    Scope:   ``flink_timeout`` (Flink-level timeout, independent of LLM
             client timeout) and optionally ``json_parse_error`` /
             ``schema_validation_error`` (non-deterministic LLM output).
    Config:  ``AsyncRetryStrategy`` passed to ``AsyncDataStream.*_wait_with_retry``.

**Invariant**: No error class is retried by both layers.
- ``llm_call_error`` = LLM client already retried → Layer 2 does NOT retry.
- ``flink_timeout`` = Flink-level → Layer 1 never sees it → Layer 2 owns it.
- ``json_parse_error`` / ``schema_validation_error`` = semantic failures →
  Layer 1 explicitly does NOT retry these → Layer 2 may retry.
- ``retrieve_timeout`` / ``retrieve_error`` = retriever failures →
  NOT retried by Layer 1 → Layer 2 may retry.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional, Set

from pyflink.datastream.functions import AsyncRetryStrategy

logger = logging.getLogger(__name__)

# Error tags produced by operators — categorised by retry ownership.
# Layer 1 (LLM Client) already retried these; Layer 2 must NOT retry.
LLM_CLIENT_OWNED_ERRORS: Set[str] = frozenset({
    "llm_call_error",  # LLM client exhausted its retries
})

# Layer 2 (Flink) owns retry for these — Layer 1 never sees them.
FLINK_RETRYABLE_ERRORS: Set[str] = frozenset({
    "flink_timeout",
    "json_parse_error",
    "schema_validation_error",
    "retrieve_timeout",
    "retrieve_error",
    "input_parse_error",
    "expected_list_response",
})


def is_degraded_and_retryable(result: List[str]) -> bool:
    """Result predicate for Flink AsyncRetryStrategy.

    Returns ``True`` if the result contains a degraded record whose error
    is owned by the Flink retry layer (Layer 2).  Returns ``False`` for
    normal results or for errors already retried by the LLM client.
    """
    if not result:
        return True  # empty result → retry

    for item in result:
        try:
            parsed = json.loads(item)
        except (json.JSONDecodeError, TypeError):
            continue

        if not parsed.get("_degraded", False):
            continue

        error = str(parsed.get("_error", ""))
        # Check if the error tag starts with any retryable prefix
        for tag in FLINK_RETRYABLE_ERRORS:
            if error.startswith(tag):
                return True

    return False


def create_flink_retry_strategy(
    max_attempts: int = 2,
    backoff_time_millis: int = 500,
    retry_on_empty: bool = True,
) -> AsyncRetryStrategy:
    """Create a Flink-level retry strategy respecting the ownership split.

    Parameters
    ----------
    max_attempts : int
        Maximum retry attempts at the Flink layer (default 2, meaning
        up to 1 re-attempt after the initial try).
    backoff_time_millis : int
        Fixed delay between retries in milliseconds.
    retry_on_empty : bool
        Whether to retry on empty results.

    Returns
    -------
    AsyncRetryStrategy
        Ready to pass to ``AsyncDataStream.unordered_wait_with_retry()``.
    """
    def result_predicate(results: List[str]) -> bool:
        if retry_on_empty and not results:
            return True
        return is_degraded_and_retryable(results)

    # No exception_predicate — our operators never throw; they return degraded.
    return AsyncRetryStrategy.fixed_delay(
        max_attempts=max_attempts,
        backoff_time_millis=backoff_time_millis,
        result_predicate=result_predicate,
        exception_predicate=None,
    )

