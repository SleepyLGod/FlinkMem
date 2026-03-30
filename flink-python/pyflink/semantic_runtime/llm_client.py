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
LLM Client abstraction for CP semantic operators.

Retry at this layer is scoped to **API/network transient failures only**
(HTTP 429, 503, connection timeout).  Semantic-level retries (e.g. bad JSON
from the model) are NOT retried here — they surface as parse failures in the
operator layer.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Call metadata (returned alongside every response)
# ---------------------------------------------------------------------------

@dataclass
class LLMCallMetrics:
    """Per-call metrics recorded by every LLMClient implementation."""
    latency_ms: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    success: bool = True
    attempts: int = 1
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Config (must be fully picklable — no live objects)
# ---------------------------------------------------------------------------

@dataclass
class LLMClientConfig:
    """Serialisable configuration carried through cloudpickle."""
    backend: str = "openai"                 # "mock" | "openai"
    model: str = "gpt-4o-mini"
    api_base: Optional[str] = None          # None → default endpoint
    api_key_env: str = "OPENAI_API_KEY"     # env-var name (not the key itself)
    timeout_s: float = 30.0
    max_retries: int = 3                    # retries for API/network errors only
    retry_base_delay_s: float = 0.5
    # mock-specific
    mock_delay_s: float = 0.1
    mock_response: Optional[str] = None     # fixed response; None → echo prompt
    mock_fail_first_n: int = 0              # simulate N transient failures
    mock_bad_json_first_n: int = 0          # return invalid JSON for first N calls (Flink retry test)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class LLMClient(ABC):
    """Async LLM client interface used by all semantic operators."""

    @abstractmethod
    async def call(self, prompt: str) -> tuple[str, LLMCallMetrics]:
        """Send *prompt* and return (response_text, metrics)."""
        ...

    def close(self) -> None:  # noqa: B027
        """Release resources (e.g. HTTP session).  Optional override."""


# ---------------------------------------------------------------------------
# Mock backend
# ---------------------------------------------------------------------------

class MockLLMClient(LLMClient):
    """Deterministic mock for dev/test.  No network dependency."""

    def __init__(self, config: LLMClientConfig) -> None:
        self._delay = config.mock_delay_s
        self._response = config.mock_response
        self._fail_first_n = config.mock_fail_first_n
        self._bad_json_first_n = config.mock_bad_json_first_n
        self._call_count = 0

    async def call(self, prompt: str) -> tuple[str, LLMCallMetrics]:
        t0 = time.monotonic()
        attempts = 0
        last_err: Optional[str] = None

        while True:
            attempts += 1
            self._call_count += 1

            # simulate transient failures (Layer 1 retry)
            if self._call_count <= self._fail_first_n:
                last_err = "mock transient failure"
                await asyncio.sleep(self._delay)
                continue

            await asyncio.sleep(self._delay)

            # simulate bad JSON responses (triggers Flink Layer 2 retry)
            if self._call_count <= self._fail_first_n + self._bad_json_first_n:
                return "INVALID_JSON_FOR_RETRY_TEST", LLMCallMetrics(
                    latency_ms=(time.monotonic() - t0) * 1000,
                    attempts=attempts,
                )

            text = self._response if self._response is not None else prompt
            return text, LLMCallMetrics(
                latency_ms=(time.monotonic() - t0) * 1000,
                attempts=attempts,
            )


# ---------------------------------------------------------------------------
# OpenAI-compatible backend
# ---------------------------------------------------------------------------

class OpenAILLMClient(LLMClient):
    """Async client for OpenAI / compatible API."""

    def __init__(self, config: LLMClientConfig) -> None:
        self._config = config
        self._session = None  # lazily created

    async def _ensure_session(self):
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession()

    async def call(self, prompt: str) -> tuple[str, LLMCallMetrics]:
        import aiohttp
        import os  # noqa: E401
        await self._ensure_session()
        assert self._session is not None

        api_key = os.environ.get(self._config.api_key_env, "")
        if not api_key:
            raise RuntimeError(
                f"LLM API key environment variable {self._config.api_key_env!r} is empty"
            )
        base = self._config.api_base or "https://api.openai.com/v1"
        url = f"{base}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": self._config.model,
            "messages": [{"role": "user", "content": prompt}],
        }

        t0 = time.monotonic()
        attempts = 0
        last_err: Optional[str] = None
        retryable_statuses = {429, 500, 502, 503, 504}

        for attempt in range(1, self._config.max_retries + 1):
            attempts = attempt
            delay = self._config.retry_base_delay_s * (2 ** (attempt - 1))
            try:
                async with self._session.post(
                    url, headers=headers, json=body,
                    timeout=aiohttp.ClientTimeout(total=self._config.timeout_s),
                ) as resp:
                    if resp.status in retryable_statuses:
                        last_err = f"HTTP {resp.status}"
                        logger.warning("LLM API %s, retry %d after %.1fs",
                                       last_err, attempt, delay)
                        await asyncio.sleep(delay)
                        continue
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)

                choice = data["choices"][0]["message"]["content"]
                usage = data.get("usage", {})
                return choice, LLMCallMetrics(
                    latency_ms=(time.monotonic() - t0) * 1000,
                    input_tokens=usage.get("prompt_tokens", 0),
                    output_tokens=usage.get("completion_tokens", 0),
                    attempts=attempts,
                )
            except asyncio.TimeoutError:
                last_err = "timeout"
                logger.warning("LLM call timeout, retry %d after %.1fs",
                               attempt, delay)
                await asyncio.sleep(delay)
            except aiohttp.ClientResponseError as e:
                if e.status in retryable_statuses:
                    last_err = str(e)
                    logger.warning("LLM response error, retry %d after %.1fs: %s",
                                   attempt, delay, e)
                    await asyncio.sleep(delay)
                    continue
                raise
            except (
                aiohttp.ClientConnectionError,
                aiohttp.ClientPayloadError,
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientOSError,
            ) as e:
                last_err = str(e)
                logger.warning("LLM connection/payload error, retry %d after %.1fs: %s",
                               attempt, delay, e)
                await asyncio.sleep(delay)
            except aiohttp.ClientError as e:
                # Other aiohttp transport errors are treated as transient.
                last_err = str(e)
                logger.warning("LLM client error, retry %d after %.1fs: %s",
                               attempt, delay, e)
                await asyncio.sleep(delay)

        # all retries exhausted
        elapsed = (time.monotonic() - t0) * 1000
        raise RuntimeError(
            f"LLM call failed after {attempts} attempts: {last_err}"
        )

    def close(self) -> None:
        if self._session is not None:
            # aiohttp session close is async; best-effort sync close
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._session.close())
                else:
                    loop.run_until_complete(self._session.close())
            except Exception:
                pass
            self._session = None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_llm_client(config: LLMClientConfig) -> LLMClient:
    """Instantiate the correct backend from a picklable config."""
    if config.backend == "mock":
        return MockLLMClient(config)
    elif config.backend == "openai":
        return OpenAILLMClient(config)
    else:
        raise ValueError(f"Unknown LLM backend: {config.backend!r}")
