#!/usr/bin/env python
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
SemMapFunction smoke test — through AsyncDataStream.

Uses MockLLMClient configured to return valid JSON matching the expected schema.
Verifies:
  1. Normal path: structured output parsed correctly.
  2. Timeout path: degraded record emitted (via short Flink timeout + long mock delay).
"""

import json
import sys

# Bootstrap: ensure pyflink.cp is importable via symlink
import os, pathlib, pyflink as _pf  # noqa: E401
_cp_src = pathlib.Path(__file__).resolve().parents[1]
_cp_dst = pathlib.Path(_pf.__file__).parent / "cp"
if not _cp_dst.exists():
    os.symlink(_cp_src, _cp_dst)

from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment, AsyncDataStream

from pyflink.cp.llm_client import LLMClientConfig
from pyflink.cp.operators.sem_map import SemMapFunction


def run_normal():
    """Normal path: mock returns valid JSON, sem_map parses it."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.from_collection(
        ["The food was great", "Terrible service", "Average experience"],
        type_info=Types.STRING(),
    )

    mock_response = json.dumps({"sentiment": "positive", "confidence": 0.95})
    config = LLMClientConfig(
        backend="mock",
        mock_delay_s=0.05,
        mock_response=mock_response,
    )

    sem_map_fn = SemMapFunction(
        prompt_template="Classify sentiment: {input}",
        output_schema={"sentiment": str, "confidence": float},
        llm_config=config,
    )

    result = AsyncDataStream.unordered_wait(
        ds, sem_map_fn, Time.seconds(10), 2, Types.STRING()
    )
    result.print()
    env.execute("sem_map_normal_test")


def run_timeout():
    """Timeout path: mock delay > Flink timeout -> degraded records."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.from_collection(
        ["input_a", "input_b"],
        type_info=Types.STRING(),
    )

    config = LLMClientConfig(
        backend="mock",
        mock_delay_s=30.0,  # will exceed Flink timeout
        mock_response=json.dumps({"sentiment": "x", "confidence": 0.0}),
    )

    sem_map_fn = SemMapFunction(
        prompt_template="Classify: {input}",
        output_schema={"sentiment": str, "confidence": float},
        llm_config=config,
    )

    result = AsyncDataStream.unordered_wait(
        ds, sem_map_fn, Time.seconds(2), 2, Types.STRING()
    )
    result.print()
    env.execute("sem_map_timeout_test")


if __name__ == "__main__":
    test = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if test == "normal":
        print("=== Running normal path test ===")
        run_normal()
    elif test == "timeout":
        print("=== Running timeout path test ===")
        run_timeout()
    else:
        print(f"Unknown test: {test}. Use 'normal' or 'timeout'.")
        sys.exit(1)

