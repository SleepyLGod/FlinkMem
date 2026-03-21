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

This module now does more than hold raw dicts:

- backend defaults remain typed (`DefaultsConfig`, `LLMBackendConfig`,
  `EmbeddingBackendConfig`)
- operator sections can be hydrated into typed query specs and typed kernel
  configs
- lowering/runtime decisions can be resolved into internal runtime bundles

Operator sections use one canonical shape:

- semantic operators: nested `query_spec` + `kernel`
- runtime helpers without query semantics: nested `kernel`
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.semantic_spec import (
    AggQuerySpec,
    GroupbyQuerySpec,
    JoinQuerySpec,
    SemanticSpec,
    TopKQuerySpec,
)
if TYPE_CHECKING:
    from pyflink.semantic_runtime.runtime.semantic_lowering import SemanticLoweringPlan
    from pyflink.semantic_runtime.operators.stateful.sem_agg import SemAggConfig
    from pyflink.semantic_runtime.operators.stateful.sem_groupby import SemGroupbyConfig
    from pyflink.semantic_runtime.operators.stateful.sem_topk import SemTopKConfig
    from pyflink.semantic_runtime.operators.stateful.sem_window import SemWindowConfig
    from pyflink.semantic_runtime.runtime.sem_search import SemSearchConfig
    from pyflink.semantic_runtime.runtime.state_descriptors import OverflowPolicy


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
# Typed runtime bundles
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TopKRuntimeBundle:
    query_spec: TopKQuerySpec
    kernel_config: "SemTopKConfig"
    lowering_plan: "SemanticLoweringPlan"


@dataclass(frozen=True)
class GroupbyRuntimeBundle:
    query_spec: GroupbyQuerySpec
    kernel_config: "SemGroupbyConfig"
    lowering_plan: "SemanticLoweringPlan"


@dataclass(frozen=True)
class AggRuntimeBundle:
    query_spec: AggQuerySpec
    kernel_config: "SemAggConfig"
    lowering_plan: "SemanticLoweringPlan"


@dataclass(frozen=True)
class JoinRuntimeBundle:
    query_spec: JoinQuerySpec
    lowering_plan: "SemanticLoweringPlan"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _coerce_overflow_policy(value: Any) -> OverflowPolicy:
    from pyflink.semantic_runtime.runtime.state_descriptors import OverflowPolicy

    if isinstance(value, OverflowPolicy):
        return value
    if isinstance(value, str):
        return OverflowPolicy(value)
    raise ValueError(f"Unsupported overflow_policy value: {value!r}")


def _split_operator_section(
    raw: Dict[str, Any],
    *,
    operator_name: str,
    allow_query_spec: bool,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if not raw:
        return {}, {}
    if not allow_query_spec and "query_spec" in raw:
        raise ValueError(f"{operator_name} does not accept query_spec")
    allowed_keys = {"kernel"}
    if allow_query_spec:
        allowed_keys.add("query_spec")
    unknown_keys = set(raw.keys()) - allowed_keys
    if unknown_keys:
        expected = "query_spec/kernel" if allow_query_spec else "kernel"
        raise ValueError(
            f"{operator_name} config must use nested {expected} layout only; "
            f"unexpected top-level keys: {sorted(unknown_keys)!r}"
        )
    if "kernel" in raw and not isinstance(raw["kernel"], dict):
        raise ValueError(f"{operator_name}.kernel must be a dict")
    if allow_query_spec and "query_spec" in raw and not isinstance(raw["query_spec"], dict):
        raise ValueError(f"{operator_name}.query_spec must be a dict")
    return dict(raw.get("query_spec", {})), dict(raw.get("kernel", {}))


def _inject_scope_defaults(scope: Dict[str, Any], *, defaults_ttl_seconds: int) -> Dict[str, Any]:
    out = dict(scope)
    if "ttl_seconds" not in out:
        out["ttl_seconds"] = defaults_ttl_seconds
    return out


def _reject_execution_path(raw: Dict[str, Any], operator_name: str) -> None:
    """Reject public execution-path configuration.

    Execution-path selection is an internal planning decision. The public
    runtime config must not carry physical planning directives.
    """
    if "execution_path" in raw:
        raise ValueError(
            f"{operator_name} query_spec no longer accepts public execution_path. "
            "Path selection is internal."
        )


def _normalize_topk_query_raw(raw: Dict[str, Any], *, defaults_ttl_seconds: int) -> Dict[str, Any]:
    _reject_execution_path(raw, "sem_topk")
    if "semantic" in raw:
        out = dict(raw)
        out["scope_policy"] = _inject_scope_defaults(
            dict(out.get("scope_policy", {})),
            defaults_ttl_seconds=defaults_ttl_seconds,
        )
        return out

    backend = raw.get("backend", raw.get("scorer_backend", "external_score"))
    scope = _inject_scope_defaults(
        dict(raw.get("scope_policy", {})),
        defaults_ttl_seconds=defaults_ttl_seconds,
    )
    if "max_candidates" in raw and "max_candidates" not in scope:
        scope["max_candidates"] = raw["max_candidates"]

    out: Dict[str, Any] = {
        "semantic": SemanticSpec.for_sem_topk(
            raw.get("instruction", ""),
            scorer_backend=backend,
            threshold=raw.get("threshold"),
        ).to_dict(),
        "k": raw.get("k", 10),
        "query_id": raw.get("query_id", "default"),
        "query_version": raw.get("query_version", 1),
        "ranking_method": raw.get("ranking_method", "pointwise"),
        "trigger_policy": dict(raw.get("trigger_policy", {})),
        "scope_policy": scope,
    }
    return out


def _normalize_groupby_query_raw(raw: Dict[str, Any], *, defaults_ttl_seconds: int) -> Dict[str, Any]:
    _reject_execution_path(raw, "sem_groupby")
    if "semantic" in raw:
        out = dict(raw)
        out["scope_policy"] = _inject_scope_defaults(
            dict(out.get("scope_policy", {})),
            defaults_ttl_seconds=defaults_ttl_seconds,
        )
        return out

    scope = _inject_scope_defaults(
        dict(raw.get("scope_policy", {})),
        defaults_ttl_seconds=defaults_ttl_seconds,
    )
    if "max_groups_per_key" in raw and "max_groups_per_key" not in scope:
        scope["max_groups_per_key"] = raw["max_groups_per_key"]

    return {
        "semantic": SemanticSpec(
            instruction=raw.get("instruction", "Assign tuples to semantic groups."),
            backend=raw.get("backend", "llm"),
            output_mode="label",
        ).to_dict(),
        "query_id": raw.get("query_id", "default"),
        "query_version": raw.get("query_version", 1),
        "assignment_method": raw.get("assignment_method", "llm"),
        "trigger_policy": dict(raw.get("trigger_policy", {})),
        "maintenance_trigger_policy": raw.get("maintenance_trigger_policy"),
        "scope_policy": scope,
        "new_group_threshold": raw.get("new_group_threshold", 0.3),
        "assign_threshold": raw.get("assign_threshold", 0.7),
    }


def _normalize_agg_query_raw(raw: Dict[str, Any], *, defaults_ttl_seconds: int) -> Dict[str, Any]:
    _reject_execution_path(raw, "sem_agg")
    if "semantic" in raw:
        out = dict(raw)
        out["scope_policy"] = _inject_scope_defaults(
            dict(out.get("scope_policy", {})),
            defaults_ttl_seconds=defaults_ttl_seconds,
        )
        return out

    scope = _inject_scope_defaults(
        dict(raw.get("scope_policy", {})),
        defaults_ttl_seconds=defaults_ttl_seconds,
    )
    if "max_buffer_events" in raw and "max_buffer_events" not in scope:
        scope["max_buffer_events"] = raw["max_buffer_events"]
    if "flush_interval_ms" in raw and "flush_interval_ms" not in scope:
        scope["flush_interval_ms"] = raw["flush_interval_ms"]

    return {
        "semantic": SemanticSpec(
            instruction=raw.get("instruction", "Aggregate semantic state over a keyed stream."),
            backend=raw.get("backend", "rule"),
            output_mode="summary",
        ).to_dict(),
        "query_id": raw.get("query_id", "default"),
        "query_version": raw.get("query_version", 1),
        "agg_method": raw.get("agg_method", raw.get("mode", "algebraic")),
        "trigger_policy": dict(raw.get("trigger_policy", {})),
        "scope_policy": scope,
    }


def _normalize_join_query_raw(raw: Dict[str, Any], *, defaults_ttl_seconds: int) -> Dict[str, Any]:
    if "semantic" in raw:
        out = dict(raw)
        scope = dict(out.get("scope_policy", {}))
        if "ttl_seconds" not in scope:
            scope["ttl_seconds"] = defaults_ttl_seconds
        out["scope_policy"] = scope
        return out

    scope = dict(raw.get("scope_policy", {}))
    if "ttl_seconds" not in scope:
        scope["ttl_seconds"] = defaults_ttl_seconds
    for field in ("max_left_buffer", "max_right_buffer", "window_kind", "window_size_ms"):
        if field in raw and field not in scope:
            scope[field] = raw[field]

    return {
        "semantic": SemanticSpec(
            instruction=raw.get("instruction", "Decide whether left and right tuples semantically join."),
            backend=raw.get("backend", "llm"),
            output_mode="bool",
        ).to_dict(),
        "query_id": raw.get("query_id", "default"),
        "query_version": raw.get("query_version", 1),
        "pairing_method": raw.get("pairing_method", "candidate_pruned"),
        "trigger_policy": dict(raw.get("trigger_policy", {})),
        "scope_policy": scope,
    }


def _sem_search_operator_name(operators: Dict[str, Dict[str, Any]]) -> str:
    if "sem_search" in operators:
        return "sem_search"
    return "sem_search"


# ---------------------------------------------------------------------------
# RuntimeConfig — top-level shell
# ---------------------------------------------------------------------------


@dataclass
class RuntimeConfig:
    """Unified top-level runtime configuration.

    The shell exposes typed hydration helpers so planner/builder code can resolve:

    - operator query semantics (`QuerySpec`)
    - kernel/runtime config (`Sem*Config`)
    - lowering view (`SemanticLoweringPlan`)
    """

    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    llm: LLMBackendConfig = field(default_factory=LLMBackendConfig)
    embedding: EmbeddingBackendConfig = field(default_factory=EmbeddingBackendConfig)
    operators: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    workflow: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RuntimeConfig":
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

    # -- raw access ----------------------------------------------------------

    def _get_operator_sections(
        self,
        operator_name: str,
        *,
        allow_query_spec: bool,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        raw = self.operators.get(operator_name, {})
        if not isinstance(raw, dict):
            raise ValueError(f"{operator_name} config must be a dict")
        return _split_operator_section(
            raw,
            operator_name=operator_name,
            allow_query_spec=allow_query_spec,
        )

    def to_llm_client_config(self) -> LLMClientConfig:
        """Best-effort adapter from backend shell config to async client config.

        This remains conservative:
        - `backend`, `model`, and `endpoint/api_base` are mapped directly
        - retry / timeout / mock fields can be passed through `llm.extra`
        - the direct `api_key` string is intentionally not threaded into the
          client config because the current client contract uses `api_key_env`
        """

        extra = dict(self.llm.extra)
        return LLMClientConfig(
            backend=self.llm.backend or "mock",
            model=self.llm.model or "gpt-4o-mini",
            api_base=self.llm.endpoint or None,
            api_key_env=str(extra.get("api_key_env", "OPENAI_API_KEY")),
            timeout_s=float(extra.get("timeout_s", 30.0)),
            max_retries=int(extra.get("max_retries", 3)),
            retry_base_delay_s=float(extra.get("retry_base_delay_s", 0.5)),
            mock_delay_s=float(extra.get("mock_delay_s", 0.1)),
            mock_response=extra.get("mock_response"),
            mock_fail_first_n=int(extra.get("mock_fail_first_n", 0)),
            mock_bad_json_first_n=int(extra.get("mock_bad_json_first_n", 0)),
        )

    def to_embedding_backend_config(self) -> EmbeddingBackendConfig:
        return EmbeddingBackendConfig(
            backend=self.embedding.backend,
            model=self.embedding.model,
            endpoint=self.embedding.endpoint,
            dimensions=self.embedding.dimensions,
            extra=dict(self.embedding.extra),
        )

    # -- typed query specs ---------------------------------------------------

    def get_topk_query_spec(self) -> TopKQuerySpec:
        query_raw, _kernel_raw = self._get_operator_sections(
            "sem_topk",
            allow_query_spec=True,
        )
        return TopKQuerySpec.from_dict(
            _normalize_topk_query_raw(
                query_raw,
                defaults_ttl_seconds=self.defaults.ttl_seconds,
            )
        )

    def get_groupby_query_spec(self) -> GroupbyQuerySpec:
        query_raw, _kernel_raw = self._get_operator_sections(
            "sem_groupby",
            allow_query_spec=True,
        )
        return GroupbyQuerySpec.from_dict(
            _normalize_groupby_query_raw(
                query_raw,
                defaults_ttl_seconds=self.defaults.ttl_seconds,
            )
        )

    def get_agg_query_spec(self) -> AggQuerySpec:
        query_raw, _kernel_raw = self._get_operator_sections(
            "sem_agg",
            allow_query_spec=True,
        )
        return AggQuerySpec.from_dict(
            _normalize_agg_query_raw(
                query_raw,
                defaults_ttl_seconds=self.defaults.ttl_seconds,
            )
        )

    def get_join_query_spec(self) -> JoinQuerySpec:
        query_raw, _kernel_raw = self._get_operator_sections(
            "sem_join",
            allow_query_spec=True,
        )
        return JoinQuerySpec.from_dict(
            _normalize_join_query_raw(
                query_raw,
                defaults_ttl_seconds=self.defaults.ttl_seconds,
            )
        )

    # -- typed kernel configs ------------------------------------------------

    def get_topk_kernel_config(self) -> SemTopKConfig:
        from pyflink.semantic_runtime.operators.stateful.sem_topk import SemTopKConfig

        _query_raw, kernel_raw = self._get_operator_sections(
            "sem_topk",
            allow_query_spec=True,
        )
        kwargs: Dict[str, Any] = {}
        for field_name in SemTopKConfig.__dataclass_fields__:
            if field_name in kernel_raw:
                kwargs[field_name] = kernel_raw[field_name]
        if "overflow_policy" in kwargs:
            kwargs["overflow_policy"] = _coerce_overflow_policy(kwargs["overflow_policy"])
        return SemTopKConfig(**kwargs)

    def get_groupby_kernel_config(self) -> SemGroupbyConfig:
        from pyflink.semantic_runtime.operators.stateful.sem_groupby import SemGroupbyConfig

        _query_raw, kernel_raw = self._get_operator_sections(
            "sem_groupby",
            allow_query_spec=True,
        )
        kwargs: Dict[str, Any] = {}
        for field_name in SemGroupbyConfig.__dataclass_fields__:
            if field_name in kernel_raw:
                kwargs[field_name] = kernel_raw[field_name]
        if "overflow_policy" in kwargs:
            kwargs["overflow_policy"] = _coerce_overflow_policy(kwargs["overflow_policy"])
        return SemGroupbyConfig(**kwargs)

    def get_agg_kernel_config(self) -> SemAggConfig:
        from pyflink.semantic_runtime.operators.stateful.sem_agg import SemAggConfig

        _query_raw, kernel_raw = self._get_operator_sections(
            "sem_agg",
            allow_query_spec=True,
        )
        kwargs: Dict[str, Any] = {}
        for field_name in SemAggConfig.__dataclass_fields__:
            if field_name in kernel_raw:
                kwargs[field_name] = kernel_raw[field_name]
        if "overflow_policy" in kwargs:
            kwargs["overflow_policy"] = _coerce_overflow_policy(kwargs["overflow_policy"])
        return SemAggConfig(**kwargs)

    def get_search_config(self) -> "SemSearchConfig":
        from pyflink.semantic_runtime.runtime.sem_search import SemSearchConfig

        operator_name = _sem_search_operator_name(self.operators)
        _query_raw, kernel_raw = self._get_operator_sections(
            operator_name,
            allow_query_spec=False,
        )
        kwargs: Dict[str, Any] = {}
        for field_name in SemSearchConfig.__dataclass_fields__:
            if field_name in kernel_raw:
                kwargs[field_name] = kernel_raw[field_name]
        if "ttl_seconds" not in kwargs:
            kwargs["ttl_seconds"] = self.defaults.ttl_seconds
        if "overflow_policy" not in kwargs:
            kwargs["overflow_policy"] = self.defaults.overflow_policy
        if "overflow_policy" in kwargs:
            kwargs["overflow_policy"] = _coerce_overflow_policy(kwargs["overflow_policy"])
        return SemSearchConfig(**kwargs)

    def get_window_config(self) -> "SemWindowConfig":
        from pyflink.semantic_runtime.operators.stateful.sem_window import SemWindowConfig

        _query_raw, kernel_raw = self._get_operator_sections(
            "sem_window",
            allow_query_spec=False,
        )
        kwargs: Dict[str, Any] = {}
        for field_name in SemWindowConfig.__dataclass_fields__:
            if field_name in kernel_raw:
                kwargs[field_name] = kernel_raw[field_name]
        if "ttl_seconds" not in kwargs:
            kwargs["ttl_seconds"] = self.defaults.ttl_seconds
        if "overflow_policy" not in kwargs:
            kwargs["overflow_policy"] = self.defaults.overflow_policy
        if "overflow_policy" in kwargs:
            kwargs["overflow_policy"] = _coerce_overflow_policy(kwargs["overflow_policy"])
        return SemWindowConfig(**kwargs)

    # -- resolved bundles ----------------------------------------------------

    def resolve_topk_runtime_bundle(self) -> TopKRuntimeBundle:
        from pyflink.semantic_runtime.runtime.semantic_lowering import (
            resolve_topk_lowering_plan,
        )

        query_spec = self.get_topk_query_spec()
        return TopKRuntimeBundle(
            query_spec=query_spec,
            kernel_config=self.get_topk_kernel_config(),
            lowering_plan=resolve_topk_lowering_plan(query_spec),
        )

    def resolve_groupby_runtime_bundle(self, *, input_kind: str = "event_stream") -> GroupbyRuntimeBundle:
        from pyflink.semantic_runtime.runtime.semantic_lowering import (
            resolve_groupby_lowering_plan,
        )

        query_spec = self.get_groupby_query_spec()
        return GroupbyRuntimeBundle(
            query_spec=query_spec,
            kernel_config=self.get_groupby_kernel_config(),
            lowering_plan=resolve_groupby_lowering_plan(query_spec, input_kind=input_kind),
        )

    def resolve_agg_runtime_bundle(self, *, input_kind: str = "event_stream") -> AggRuntimeBundle:
        from pyflink.semantic_runtime.runtime.semantic_lowering import (
            resolve_agg_lowering_plan,
        )

        query_spec = self.get_agg_query_spec()
        return AggRuntimeBundle(
            query_spec=query_spec,
            kernel_config=self.get_agg_kernel_config(),
            lowering_plan=resolve_agg_lowering_plan(query_spec, input_kind=input_kind),
        )

    def resolve_join_runtime_bundle(self) -> JoinRuntimeBundle:
        from pyflink.semantic_runtime.runtime.semantic_lowering import (
            resolve_join_lowering_plan,
        )

        query_spec = self.get_join_query_spec()
        return JoinRuntimeBundle(
            query_spec=query_spec,
            lowering_plan=resolve_join_lowering_plan(query_spec),
        )
