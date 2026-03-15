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
Flink-native metrics integration for CP semantic operators.

Registers counters, distributions, and meters with Flink's MetricGroup
when available.  Falls back to local in-memory accumulators when the
metric group is not accessible (e.g. in unit tests or unsupported
runtime contexts).

Usage in an ``AsyncFunction.open()``:

    self._metrics = OperatorMetrics.from_runtime_context(runtime_context, "sem_map")

Then in ``async_invoke`` / ``timeout``:

    self._metrics.record_call(latency_ms, input_tokens, output_tokens, attempts)
    self._metrics.record_error()
    self._metrics.record_timeout()
    self._metrics.record_invalid_output()
"""

from __future__ import annotations

import logging
from typing import Optional

from pyflink.datastream.functions import RuntimeContext

logger = logging.getLogger(__name__)


class OperatorMetrics:
    """Wrapper around Flink's MetricGroup for CP operator instrumentation.

    All methods are safe to call even when Flink metrics are unavailable —
    they fall back to silent local counters.
    """

    def __init__(
        self,
        call_counter=None,
        error_counter=None,
        timeout_counter=None,
        invalid_output_counter=None,
        retry_exhaustion_counter=None,
        latency_distribution=None,
        input_token_counter=None,
        output_token_counter=None,
        call_meter=None,
        # local fallback accumulators
        _local: bool = False,
    ):
        self._call_counter = call_counter
        self._error_counter = error_counter
        self._timeout_counter = timeout_counter
        self._invalid_output_counter = invalid_output_counter
        self._retry_exhaustion_counter = retry_exhaustion_counter
        self._latency_distribution = latency_distribution
        self._input_token_counter = input_token_counter
        self._output_token_counter = output_token_counter
        self._call_meter = call_meter
        self._local = _local

        # Local accumulators (always maintained for in-record _metrics and tests)
        self.local_call_count: int = 0
        self.local_error_count: int = 0
        self.local_timeout_count: int = 0
        self.local_invalid_output_count: int = 0
        self.local_retry_exhaustion_count: int = 0
        self.local_total_latency_ms: float = 0.0
        self.local_total_input_tokens: int = 0
        self.local_total_output_tokens: int = 0

    @classmethod
    def from_runtime_context(
        cls, runtime_context: RuntimeContext, operator_name: str,
    ) -> "OperatorMetrics":
        """Try to register Flink metrics; fall back to local-only on failure."""
        try:
            # DataStream RuntimeContext exposes get_metrics_group(); some other contexts may expose
            # get_metric_group(). Support both for compatibility.
            if hasattr(runtime_context, "get_metrics_group"):
                root_mg = runtime_context.get_metrics_group()
            elif hasattr(runtime_context, "get_metric_group"):
                root_mg = runtime_context.get_metric_group()
            else:
                raise AttributeError("RuntimeContext has no metric-group accessor")

            mg = root_mg.add_group("cp", operator_name)
            return cls(
                call_counter=mg.counter("call_count"),
                error_counter=mg.counter("error_count"),
                timeout_counter=mg.counter("timeout_count"),
                invalid_output_counter=mg.counter("invalid_output_count"),
                retry_exhaustion_counter=mg.counter("retry_exhaustion_count"),
                latency_distribution=mg.distribution("call_latency_ms"),
                input_token_counter=mg.counter("input_tokens_total"),
                output_token_counter=mg.counter("output_tokens_total"),
                call_meter=mg.meter("call_rate", time_span_in_seconds=60),
            )
        except Exception as e:
            logger.warning(
                "Could not register Flink metrics for %s: %s. "
                "Falling back to local accumulators.", operator_name, e)
            return cls(_local=True)

    @classmethod
    def noop(cls) -> "OperatorMetrics":
        """Create a local-only metrics instance (for tests)."""
        return cls(_local=True)

    # ---- recording methods --------------------------------------------------

    def record_call(
        self,
        latency_ms: float,
        input_tokens: int = 0,
        output_tokens: int = 0,
        attempts: int = 1,
    ) -> None:
        """Record a successful (or attempted) LLM call."""
        self.local_call_count += 1
        self.local_total_latency_ms += latency_ms
        self.local_total_input_tokens += input_tokens
        self.local_total_output_tokens += output_tokens

        if self._call_counter is not None:
            self._call_counter.inc()
        if self._latency_distribution is not None:
            self._latency_distribution.update(int(latency_ms))
        if self._input_token_counter is not None and input_tokens > 0:
            self._input_token_counter.inc(input_tokens)
        if self._output_token_counter is not None and output_tokens > 0:
            self._output_token_counter.inc(output_tokens)
        if self._call_meter is not None:
            self._call_meter.mark_event()

    def record_error(self) -> None:
        self.local_error_count += 1
        if self._error_counter is not None:
            self._error_counter.inc()

    def record_timeout(self) -> None:
        self.local_timeout_count += 1
        if self._timeout_counter is not None:
            self._timeout_counter.inc()

    def record_invalid_output(self) -> None:
        self.local_invalid_output_count += 1
        if self._invalid_output_counter is not None:
            self._invalid_output_counter.inc()

    def record_retry_exhaustion(self) -> None:
        self.local_retry_exhaustion_count += 1
        if self._retry_exhaustion_counter is not None:
            self._retry_exhaustion_counter.inc()
