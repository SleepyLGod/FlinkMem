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
RuntimeConfig — unified top-level configuration entry point.

This is the minimal V0.2+ config shell that gives users one clean entry
point while keeping per-operator configs strongly typed internally.

Usage::

    cfg = RuntimeConfig.from_dict({
        "defaults": {"ttl_seconds": 7200},
        "llm": {"backend": "openai", "model": "gpt-4"},
        "operators": {
            "sem_topk": {"k": 5, "scorer_backend": "llm"},
        },
    })

    # Access typed operator configs:
    topk_cfg = cfg.get_operator_config("sem_topk")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

@dataclass
class DefaultsConfig:
    """Common runtime defaults shared across operators."""
    ttl_seconds: int = 3600
    overflow_policy: str = "drop_oldest"
    async_timeout_ms: int = 30_000
    async_capacity: int = 100
    metrics_enabled: bool = True

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DefaultsConfig":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class LLMBackendConfig:
    """LLM backend connection configuration."""
    backend: str = "mock"
    model: str = ""
    api_key: str = ""
    endpoint: str = ""
    temperature: float = 0.0
    max_tokens: int = 1024
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LLMBackendConfig":
        known = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        extra = {k: v for k, v in d.items() if k not in cls.__dataclass_fields__}
        if extra:
            known.setdefault("extra", {}).update(extra)
        return cls(**known)


@dataclass
class EmbeddingBackendConfig:
    """Embedding backend configuration."""
    backend: str = "mock"
    model: str = ""
    endpoint: str = ""
    dimensions: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "EmbeddingBackendConfig":
        known = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        extra = {k: v for k, v in d.items() if k not in cls.__dataclass_fields__}
        if extra:
            known.setdefault("extra", {}).update(extra)
        return cls(**known)


# ---------------------------------------------------------------------------
# RuntimeConfig — top-level shell
# ---------------------------------------------------------------------------

@dataclass
class RuntimeConfig:
    """Unified top-level runtime configuration.

    Holds backend configs, common defaults, and raw per-operator config
    dicts.  Per-operator configs are stored as raw dicts; callers use
    ``get_operator_raw()`` to feed them into typed operator config
    constructors.
    """
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    llm: LLMBackendConfig = field(default_factory=LLMBackendConfig)
    embedding: EmbeddingBackendConfig = field(default_factory=EmbeddingBackendConfig)
    operators: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    workflow: Dict[str, Any] = field(default_factory=dict)

    # -- loader ---------------------------------------------------------------

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RuntimeConfig":
        """Build a RuntimeConfig from a plain dict (e.g. parsed YAML/JSON)."""
        defaults = DefaultsConfig.from_dict(d.get("defaults", {}))
        llm = LLMBackendConfig.from_dict(d.get("llm", {}))
        embedding = EmbeddingBackendConfig.from_dict(d.get("embedding", {}))
        operators = d.get("operators", {})
        workflow = d.get("workflow", {})
        return cls(
            defaults=defaults,
            llm=llm,
            embedding=embedding,
            operators=operators,
            workflow=workflow,
        )

    # -- accessors ------------------------------------------------------------

    def get_operator_raw(self, operator_name: str) -> Dict[str, Any]:
        """Return the raw config dict for an operator, with defaults merged."""
        base = {
            "ttl_seconds": self.defaults.ttl_seconds,
            "overflow_policy": self.defaults.overflow_policy,
        }
        base.update(self.operators.get(operator_name, {}))
        return base

