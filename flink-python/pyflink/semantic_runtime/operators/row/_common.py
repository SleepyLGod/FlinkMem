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

"""Shared helpers for row-style semantic operators."""

from __future__ import annotations

from typing import Any, Dict

from pyflink.semantic_runtime.sem_spec import SemSpec


def validate_schema(obj: Any, schema: Dict[str, type]) -> bool:
    """Shallow check: *obj* is a dict with the expected keys and value types."""
    if not isinstance(obj, dict):
        return False
    for key, expected_type in schema.items():
        if key not in obj:
            return False
        if not isinstance(obj[key], expected_type):
            return False
    return True

def attach_metrics(parsed: dict, metrics) -> dict:
    """Attach LLM call metrics to the parsed result dict."""
    parsed["_metrics"] = {
        "latency_ms": metrics.latency_ms,
        "input_tokens": metrics.input_tokens,
        "output_tokens": metrics.output_tokens,
        "attempts": metrics.attempts,
    }
    return parsed


def validate_generic_sem_spec(
    spec: SemSpec,
    *,
    operator_name: str,
    allowed_output_modes: set[str],
) -> None:
    """Validate a public generic semantic spec.

    Generic semantic operators expose semantic intent only. Backend selection
    remains internal and must not be set on the public spec.
    """
    if spec.backend != "hybrid":
        raise ValueError(
            f"{operator_name} does not expose backend selection. "
            "Use internal runtime/kernel config for backend planning."
        )
    if spec.output_mode not in allowed_output_modes:
        raise ValueError(
            f"{operator_name} requires output_mode in {sorted(allowed_output_modes)!r}; "
            f"got {spec.output_mode!r}."
        )
