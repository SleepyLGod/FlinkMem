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

"""Shared helpers for CP semantic operators."""

from __future__ import annotations

import json
from typing import Any, Dict


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


def make_degraded(value: Any, error: str) -> dict:
    """Wrap *value* in a degraded-output envelope."""
    return {
        "_input": value,
        "_error": error,
        "_degraded": True,
    }


def make_degraded_json(value: Any, error: str) -> str:
    """JSON-serialised degraded envelope (ready for Types.STRING() output)."""
    return json.dumps(make_degraded(value, error))


def attach_metrics(parsed: dict, metrics) -> dict:
    """Attach LLM call metrics to the parsed result dict."""
    parsed["_metrics"] = {
        "latency_ms": metrics.latency_ms,
        "input_tokens": metrics.input_tokens,
        "output_tokens": metrics.output_tokens,
        "attempts": metrics.attempts,
    }
    return parsed

