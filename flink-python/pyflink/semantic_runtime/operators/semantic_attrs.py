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

"""Public semantic derived-attribute operators.

These operators make the lowering view explicit at the public API layer:

- ``sem_score``  -> semantic score attribute
- ``sem_label``  -> semantic label attribute
- ``sem_match``  -> semantic match predicate/score attribute

They intentionally reuse ``SemMapFunction`` so the parsing, schema
validation, timeout handling, and metric behavior stay identical to the rest
of the V0.1 async operator family.
"""

from __future__ import annotations

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.sem_map import SemMapFunction


class SemScoreFunction(SemMapFunction):
    """Public semantic score attribute operator.

    Expected LLM response schema:

    - ``score``: float
    - ``confidence``: float
    - ``reason``: str
    """

    def __init__(self, prompt_template: str, llm_config: LLMClientConfig) -> None:
        super().__init__(
            prompt_template=prompt_template,
            output_schema={"score": float, "confidence": float, "reason": str},
            llm_config=llm_config,
        )


class SemLabelFunction(SemMapFunction):
    """Public semantic label attribute operator.

    Expected LLM response schema:

    - ``label``: str
    - ``confidence``: float
    - ``reason``: str
    """

    def __init__(self, prompt_template: str, llm_config: LLMClientConfig) -> None:
        super().__init__(
            prompt_template=prompt_template,
            output_schema={"label": str, "confidence": float, "reason": str},
            llm_config=llm_config,
        )


class SemMatchFunction(SemMapFunction):
    """Public semantic match attribute operator.

    Expected LLM response schema:

    - ``matched``: bool
    - ``match_score``: float
    - ``reason``: str
    """

    def __init__(self, prompt_template: str, llm_config: LLMClientConfig) -> None:
        super().__init__(
            prompt_template=prompt_template,
            output_schema={"matched": bool, "match_score": float, "reason": str},
            llm_config=llm_config,
        )
