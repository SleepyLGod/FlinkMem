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
SemanticSpec — unified semantic criterion specification.

This is the minimal V0.2+ dataclass that captures the semantic "what to do"
across operators, decoupled from how each operator manages state or topology.

Currently serves:
  - ``sem_map``: instruction + backend + output_mode + schema
  - ``sem_topk``: instruction + backend (scorer) + output_mode(score) + threshold

Later phases may extend this to ``sem_filter``, ``sem_groupby``, etc.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Valid enums
# ---------------------------------------------------------------------------

VALID_BACKENDS = {"llm", "embedding", "hybrid", "rule", "external_score"}
VALID_OUTPUT_MODES = {"bool", "label", "score", "json", "text", "summary"}


# ---------------------------------------------------------------------------
# SemanticSpec
# ---------------------------------------------------------------------------

@dataclass
class SemanticSpec:
    """Unified semantic criterion specification.

    Parameters
    ----------
    instruction : str
        The prompt, predicate, or scoring criterion in natural language.
        For ``sem_map`` this is the prompt template; for ``sem_topk`` this
        is the reranking instruction.
    backend : str
        Processing backend.  One of ``"llm"``, ``"embedding"``,
        ``"hybrid"``, ``"rule"``, ``"external_score"``.
    output_mode : str
        Expected output shape.  One of ``"bool"``, ``"label"``, ``"score"``,
        ``"json"``, ``"text"``, ``"summary"``.
    schema : dict or None
        When ``output_mode="json"``, the expected key→type mapping.
        Ignored for other output modes.
    threshold : float or None
        Optional confidence / score threshold used by operators that need
        a decision boundary (e.g. filter, scorer).
    examples : list[dict]
        Optional few-shot examples for the LLM backend.
    metadata : dict
        Arbitrary operator-specific metadata.
    """

    instruction: str = ""
    backend: str = "llm"
    output_mode: str = "json"
    schema: Optional[Dict[str, Any]] = None
    threshold: Optional[float] = None
    examples: list = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.backend not in VALID_BACKENDS:
            raise ValueError(
                f"Invalid backend={self.backend!r}. "
                f"Must be one of {VALID_BACKENDS}."
            )
        if self.output_mode not in VALID_OUTPUT_MODES:
            raise ValueError(
                f"Invalid output_mode={self.output_mode!r}. "
                f"Must be one of {VALID_OUTPUT_MODES}."
            )

    # -- convenience constructors ---------------------------------------------

    @classmethod
    def for_sem_map(
        cls,
        instruction: str,
        *,
        output_schema: Optional[Dict[str, Any]] = None,
        return_mode: str = "json",
        backend: str = "llm",
    ) -> "SemanticSpec":
        """Build a SemanticSpec suited for ``sem_map``."""
        output_mode = return_mode  # "json" or "text"
        return cls(
            instruction=instruction,
            backend=backend,
            output_mode=output_mode,
            schema=output_schema,
        )

    @classmethod
    def for_sem_topk(
        cls,
        instruction: str = "",
        *,
        scorer_backend: str = "external_score",
        threshold: Optional[float] = None,
    ) -> "SemanticSpec":
        """Build a SemanticSpec suited for ``sem_topk`` scoring/reranking."""
        return cls(
            instruction=instruction,
            backend=scorer_backend,
            output_mode="score",
            threshold=threshold,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict (JSON-safe)."""
        return {
            "instruction": self.instruction,
            "backend": self.backend,
            "output_mode": self.output_mode,
            "schema": self.schema,
            "threshold": self.threshold,
            "examples": list(self.examples),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SemanticSpec":
        """Deserialize from a plain dict."""
        return cls(
            instruction=d.get("instruction", ""),
            backend=d.get("backend", "llm"),
            output_mode=d.get("output_mode", "json"),
            schema=d.get("schema"),
            threshold=d.get("threshold"),
            examples=d.get("examples", []),
            metadata=d.get("metadata", {}),
        )

