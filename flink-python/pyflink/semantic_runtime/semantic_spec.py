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
Operator-specific query specs — continuous semantic query definitions.

SemanticSpec captures the semantic "what to do" across operators, decoupled
from how each operator manages state or topology.

The operator-specific query specs wrap SemanticSpec and add operator-level
continuous semantics such as scope policy, trigger policy, method selection,
and versioning.

Currently serves:
  - ``sem_map``: instruction + backend + output_mode + schema
  - ``sem_topk``: instruction + backend (scorer) + output_mode(score) + threshold + scope

V0.2+ extends this pattern to ``sem_groupby``, ``sem_agg``, and future ``sem_join``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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



# ---------------------------------------------------------------------------
# TopKScopePolicy — candidate scope boundary for continuous top-k
# ---------------------------------------------------------------------------

VALID_WINDOW_KINDS = {"tumbling", "sliding", "semantic", "session", None}


@dataclass
class TopKScopePolicy:
    """Defines which tuples are eligible for ranking in a continuous top-k query.

    The candidate scope can be bounded by TTL, a maximum pool size, or a
    window.  These boundaries are evaluated *before* scoring — a tuple that
    falls outside the active scope is evicted from the candidate pool
    regardless of its score.

    Parameters
    ----------
    ttl_seconds : int or None
        Time-to-live for candidates in the pool.  ``None`` means no TTL
        (candidates are only evicted by ``max_candidates``).
    max_candidates : int or None
        Hard cap on the candidate pool size.  When exceeded, the overflow
        policy (configured on the kernel) decides which candidates to evict.
    window_kind : str or None
        Optional window type that feeds candidates into the scope.
        One of ``"tumbling"``, ``"sliding"``, ``"semantic"``, ``"session"``,
        or ``None`` (no windowing).
    window_size_ms : int or None
        Window size in milliseconds (only meaningful when ``window_kind``
        is set).
    session_gap_ms : int or None
        Idle-gap boundary for operator-owned session scopes.  Only meaningful
        when ``window_kind="session"``.
    boundary_flag : str
        Semantic boundary flag name used by operator-owned semantic scopes.
        Only meaningful when ``window_kind="semantic"``.
    """

    ttl_seconds: Optional[int] = None
    max_candidates: Optional[int] = None
    window_kind: Optional[str] = None
    window_size_ms: Optional[int] = None
    session_gap_ms: Optional[int] = None
    boundary_flag: str = "topic_shift"

    def __post_init__(self):
        if self.window_kind is not None and self.window_kind not in VALID_WINDOW_KINDS:
            raise ValueError(
                f"Invalid window_kind={self.window_kind!r}. "
                f"Must be one of {VALID_WINDOW_KINDS}."
            )
        if self.window_kind == "session" and self.session_gap_ms is not None and self.session_gap_ms <= 0:
            raise ValueError("session_gap_ms must be > 0 when provided")
        if self.window_kind == "semantic" and not self.boundary_flag:
            raise ValueError("boundary_flag must be non-empty for semantic scopes")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ttl_seconds": self.ttl_seconds,
            "max_candidates": self.max_candidates,
            "window_kind": self.window_kind,
            "window_size_ms": self.window_size_ms,
            "session_gap_ms": self.session_gap_ms,
            "boundary_flag": self.boundary_flag,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TopKScopePolicy":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# TopKQuerySpec — continuous query definition for sem_topk
# ---------------------------------------------------------------------------

VALID_RANKING_METHODS = {"pointwise", "pairwise", "listwise"}
VALID_ASSIGNMENT_METHODS = {"rule", "embedding", "llm", "llm_refine"}
VALID_AGG_METHODS = {"algebraic", "summarize", "compressive"}
VALID_JOIN_PAIRING_METHODS = {
    "candidate_pruned",
    "embedding_prefilter",
    "blocking",
    "brute_force",
}
VALID_TRIGGER_MODES = {
    "on_event",
    "on_scope_close",
    "periodic",
    "idle_flush",
    "count_threshold",
}
VALID_TOPK_EXECUTION_PATHS = {"auto", "window_owned", "operator_owned"}
VALID_GROUPBY_EXECUTION_PATHS = {"auto", "window_owned", "operator_owned"}


@dataclass
class TriggerPolicy:
    """Defines when an operator computes, refreshes, or emits results.

    Scope answers "who participates"; trigger answers "when to act on the
    current active set".
    """

    mode: str = "on_event"
    interval_ms: Optional[int] = None
    idle_ms: Optional[int] = None
    count_threshold: Optional[int] = None
    emit_intermediate: bool = True
    emit_final_on_scope_close: bool = True

    def __post_init__(self):
        if self.mode not in VALID_TRIGGER_MODES:
            raise ValueError(
                f"Invalid trigger mode={self.mode!r}. "
                f"Must be one of {VALID_TRIGGER_MODES}."
            )
        if self.mode == "periodic":
            if self.interval_ms is None or int(self.interval_ms) <= 0:
                raise ValueError("interval_ms must be > 0 when mode='periodic'")
        if self.mode == "idle_flush":
            if self.idle_ms is None or int(self.idle_ms) <= 0:
                raise ValueError("idle_ms must be > 0 when mode='idle_flush'")
        if self.mode == "count_threshold":
            if self.count_threshold is None or int(self.count_threshold) <= 0:
                raise ValueError(
                    "count_threshold must be > 0 when mode='count_threshold'"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "interval_ms": self.interval_ms,
            "idle_ms": self.idle_ms,
            "count_threshold": self.count_threshold,
            "emit_intermediate": self.emit_intermediate,
            "emit_final_on_scope_close": self.emit_final_on_scope_close,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TriggerPolicy":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class TopKQuerySpec:
    """Complete specification for a continuous top-k query.

    Wraps a :class:`SemanticSpec` (criterion / backend / prompt) and adds
    the top-k-specific parameters: *k*, query versioning, scope policy,
    ranking method, and trigger policy.

    Parameters
    ----------
    semantic : SemanticSpec
        The semantic criterion (instruction, backend, output_mode, …).
        ``semantic.instruction`` is the ranking prompt / predicate.
        ``semantic.backend`` is the scorer backend
        (``"external_score"``, ``"llm"``, ``"embedding"``).
    k : int
        Number of top items to maintain.
    query_id : str
        Logical identifier for this continuous query.  Useful when the
        same key space hosts multiple concurrent top-k queries.
    query_version : int
        Monotonically increasing version.  When the instruction or backend
        changes, bump this to invalidate cached scores in the state.
    ranking_method : str
        Execution strategy for scoring.  ``"pointwise"`` (default) asks the
        backend to score each candidate independently.  ``"pairwise"`` and
        ``"listwise"`` are contextual reranking strategies evaluated on
        bounded candidate pools (Phase C).
    execution_path : str
        Execution path selector. ``"window_owned"`` expects bounded pools or
        window-bounded snapshots. ``"operator_owned"`` expects continuous flat
        candidate updates. ``"auto"`` chooses the most suitable path for the
        currently supported semantics.
    trigger_policy : TriggerPolicy
        Defines when the current active candidate set is ranked/refreshed.
    scope_policy : TopKScopePolicy
        Defines the active candidate scope (TTL, pool cap, window).
    """

    semantic: SemanticSpec = field(default_factory=lambda: SemanticSpec.for_sem_topk())
    k: int = 10
    query_id: str = "default"
    query_version: int = 1
    ranking_method: str = "pointwise"
    execution_path: str = "auto"
    trigger_policy: TriggerPolicy = field(default_factory=TriggerPolicy)
    scope_policy: TopKScopePolicy = field(default_factory=TopKScopePolicy)

    def __post_init__(self):
        if self.ranking_method not in VALID_RANKING_METHODS:
            raise ValueError(
                f"Invalid ranking_method={self.ranking_method!r}. "
                f"Must be one of {VALID_RANKING_METHODS}."
            )
        if self.execution_path not in VALID_TOPK_EXECUTION_PATHS:
            raise ValueError(
                f"Invalid execution_path={self.execution_path!r}. "
                f"Must be one of {VALID_TOPK_EXECUTION_PATHS}."
            )
        if self.k < 1:
            raise ValueError(f"k must be >= 1, got {self.k}")

    # -- convenience constructors ---------------------------------------------

    @classmethod
    def simple(
        cls,
        instruction: str = "",
        *,
        k: int = 10,
        backend: str = "external_score",
        ttl_seconds: Optional[int] = None,
        max_candidates: Optional[int] = None,
        execution_path: str = "auto",
    ) -> "TopKQuerySpec":
        """Quick builder for the common case.

        Example::

            spec = TopKQuerySpec.simple(
                "Rank by relevance to user interests",
                k=5,
                backend="llm",
                ttl_seconds=3600,
                max_candidates=100,
            )
        """
        return cls(
            semantic=SemanticSpec.for_sem_topk(instruction, scorer_backend=backend),
            k=k,
            execution_path=execution_path,
            trigger_policy=TriggerPolicy(),
            scope_policy=TopKScopePolicy(
                ttl_seconds=ttl_seconds,
                max_candidates=max_candidates,
            ),
        )

    # -- serde ----------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "semantic": self.semantic.to_dict(),
            "k": self.k,
            "query_id": self.query_id,
            "query_version": self.query_version,
            "ranking_method": self.ranking_method,
            "execution_path": self.execution_path,
            "trigger_policy": self.trigger_policy.to_dict(),
            "scope_policy": self.scope_policy.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TopKQuerySpec":
        return cls(
            semantic=SemanticSpec.from_dict(d.get("semantic", {})),
            k=d.get("k", 10),
            query_id=d.get("query_id", "default"),
            query_version=d.get("query_version", 1),
            ranking_method=d.get("ranking_method", "pointwise"),
            execution_path=d.get("execution_path", "auto"),
            trigger_policy=TriggerPolicy.from_dict(d.get("trigger_policy", {})),
            scope_policy=TopKScopePolicy.from_dict(d.get("scope_policy", {})),
        )


# ---------------------------------------------------------------------------
# Groupby query spec
# ---------------------------------------------------------------------------


@dataclass
class GroupbyScopePolicy:
    """Scope boundary for continuous semantic grouping."""

    ttl_seconds: Optional[int] = None
    max_groups_per_key: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ttl_seconds": self.ttl_seconds,
            "max_groups_per_key": self.max_groups_per_key,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GroupbyScopePolicy":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class GroupbyQuerySpec:
    """Continuous query definition for ``sem_groupby``."""

    semantic: SemanticSpec = field(
        default_factory=lambda: SemanticSpec(
            instruction="Assign tuples to semantic groups.",
            backend="llm",
            output_mode="label",
        )
    )
    query_id: str = "default"
    query_version: int = 1
    assignment_method: str = "llm"
    execution_path: str = "auto"
    trigger_policy: TriggerPolicy = field(default_factory=TriggerPolicy)
    maintenance_trigger_policy: Optional[TriggerPolicy] = None
    scope_policy: GroupbyScopePolicy = field(default_factory=GroupbyScopePolicy)
    new_group_threshold: float = 0.3
    assign_threshold: float = 0.7

    def __post_init__(self):
        if self.assignment_method not in VALID_ASSIGNMENT_METHODS:
            raise ValueError(
                f"Invalid assignment_method={self.assignment_method!r}. "
                f"Must be one of {VALID_ASSIGNMENT_METHODS}."
            )
        if self.execution_path not in VALID_GROUPBY_EXECUTION_PATHS:
            raise ValueError(
                f"Invalid execution_path={self.execution_path!r}. "
                f"Must be one of {VALID_GROUPBY_EXECUTION_PATHS}."
            )

    @classmethod
    def simple(
        cls,
        instruction: str = "Assign tuples to semantic groups.",
        *,
        backend: str = "llm",
        assignment_method: str = "llm",
        execution_path: str = "auto",
        ttl_seconds: Optional[int] = None,
        max_groups_per_key: Optional[int] = None,
    ) -> "GroupbyQuerySpec":
        return cls(
            semantic=SemanticSpec(
                instruction=instruction,
                backend=backend,
                output_mode="label",
            ),
            assignment_method=assignment_method,
            execution_path=execution_path,
            trigger_policy=TriggerPolicy(),
            scope_policy=GroupbyScopePolicy(
                ttl_seconds=ttl_seconds,
                max_groups_per_key=max_groups_per_key,
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "semantic": self.semantic.to_dict(),
            "query_id": self.query_id,
            "query_version": self.query_version,
            "assignment_method": self.assignment_method,
            "execution_path": self.execution_path,
            "trigger_policy": self.trigger_policy.to_dict(),
            "maintenance_trigger_policy": (
                self.maintenance_trigger_policy.to_dict()
                if self.maintenance_trigger_policy is not None
                else None
            ),
            "scope_policy": self.scope_policy.to_dict(),
            "new_group_threshold": self.new_group_threshold,
            "assign_threshold": self.assign_threshold,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GroupbyQuerySpec":
        return cls(
            semantic=SemanticSpec.from_dict(d.get("semantic", {})),
            query_id=d.get("query_id", "default"),
            query_version=d.get("query_version", 1),
            assignment_method=d.get("assignment_method", "llm"),
            execution_path=d.get("execution_path", "auto"),
            trigger_policy=TriggerPolicy.from_dict(d.get("trigger_policy", {})),
            maintenance_trigger_policy=(
                TriggerPolicy.from_dict(d["maintenance_trigger_policy"])
                if d.get("maintenance_trigger_policy") is not None
                else None
            ),
            scope_policy=GroupbyScopePolicy.from_dict(d.get("scope_policy", {})),
            new_group_threshold=d.get("new_group_threshold", 0.3),
            assign_threshold=d.get("assign_threshold", 0.7),
        )


# ---------------------------------------------------------------------------
# Agg query spec
# ---------------------------------------------------------------------------


@dataclass
class AggScopePolicy:
    """Scope boundary for continuous semantic aggregation."""

    ttl_seconds: Optional[int] = None
    max_buffer_events: Optional[int] = None
    flush_interval_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ttl_seconds": self.ttl_seconds,
            "max_buffer_events": self.max_buffer_events,
            "flush_interval_ms": self.flush_interval_ms,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AggScopePolicy":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class AggQuerySpec:
    """Continuous query definition for ``sem_agg``."""

    semantic: SemanticSpec = field(
        default_factory=lambda: SemanticSpec(
            instruction="Aggregate semantic state over a keyed stream.",
            backend="rule",
            output_mode="summary",
        )
    )
    query_id: str = "default"
    query_version: int = 1
    agg_method: str = "algebraic"
    trigger_policy: TriggerPolicy = field(default_factory=TriggerPolicy)
    scope_policy: AggScopePolicy = field(default_factory=AggScopePolicy)

    def __post_init__(self):
        if self.agg_method not in VALID_AGG_METHODS:
            raise ValueError(
                f"Invalid agg_method={self.agg_method!r}. "
                f"Must be one of {VALID_AGG_METHODS}."
            )

    @classmethod
    def simple(
        cls,
        instruction: str = "Aggregate semantic state over a keyed stream.",
        *,
        backend: str = "rule",
        agg_method: str = "algebraic",
        ttl_seconds: Optional[int] = None,
        max_buffer_events: Optional[int] = None,
        flush_interval_ms: Optional[int] = None,
    ) -> "AggQuerySpec":
        return cls(
            semantic=SemanticSpec(
                instruction=instruction,
                backend=backend,
                output_mode="summary",
            ),
            agg_method=agg_method,
            trigger_policy=TriggerPolicy(),
            scope_policy=AggScopePolicy(
                ttl_seconds=ttl_seconds,
                max_buffer_events=max_buffer_events,
                flush_interval_ms=flush_interval_ms,
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "semantic": self.semantic.to_dict(),
            "query_id": self.query_id,
            "query_version": self.query_version,
            "agg_method": self.agg_method,
            "trigger_policy": self.trigger_policy.to_dict(),
            "scope_policy": self.scope_policy.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AggQuerySpec":
        return cls(
            semantic=SemanticSpec.from_dict(d.get("semantic", {})),
            query_id=d.get("query_id", "default"),
            query_version=d.get("query_version", 1),
            agg_method=d.get("agg_method", "algebraic"),
            trigger_policy=TriggerPolicy.from_dict(d.get("trigger_policy", {})),
            scope_policy=AggScopePolicy.from_dict(d.get("scope_policy", {})),
        )


# ---------------------------------------------------------------------------
# Join query spec (design shell for V0.3)
# ---------------------------------------------------------------------------


@dataclass
class JoinScopePolicy:
    """Scope boundary for future true two-input semantic join."""

    ttl_seconds: Optional[int] = None
    max_left_buffer: Optional[int] = None
    max_right_buffer: Optional[int] = None
    window_kind: Optional[str] = None
    window_size_ms: Optional[int] = None

    def __post_init__(self):
        if self.window_kind is not None and self.window_kind not in VALID_WINDOW_KINDS:
            raise ValueError(
                f"Invalid window_kind={self.window_kind!r}. "
                f"Must be one of {VALID_WINDOW_KINDS}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ttl_seconds": self.ttl_seconds,
            "max_left_buffer": self.max_left_buffer,
            "max_right_buffer": self.max_right_buffer,
            "window_kind": self.window_kind,
            "window_size_ms": self.window_size_ms,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "JoinScopePolicy":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class JoinQuerySpec:
    """Continuous query definition for future true two-input ``sem_join``."""

    semantic: SemanticSpec = field(
        default_factory=lambda: SemanticSpec(
            instruction="Decide whether left and right tuples semantically join.",
            backend="llm",
            output_mode="bool",
        )
    )
    query_id: str = "default"
    query_version: int = 1
    pairing_method: str = "candidate_pruned"
    trigger_policy: TriggerPolicy = field(default_factory=TriggerPolicy)
    scope_policy: JoinScopePolicy = field(default_factory=JoinScopePolicy)

    def __post_init__(self):
        if self.pairing_method not in VALID_JOIN_PAIRING_METHODS:
            raise ValueError(
                f"Invalid pairing_method={self.pairing_method!r}. "
                f"Must be one of {VALID_JOIN_PAIRING_METHODS}."
            )

    @classmethod
    def simple(
        cls,
        instruction: str = "Decide whether left and right tuples semantically join.",
        *,
        backend: str = "llm",
        pairing_method: str = "candidate_pruned",
        ttl_seconds: Optional[int] = None,
        max_left_buffer: Optional[int] = None,
        max_right_buffer: Optional[int] = None,
    ) -> "JoinQuerySpec":
        return cls(
            semantic=SemanticSpec(
                instruction=instruction,
                backend=backend,
                output_mode="bool",
            ),
            pairing_method=pairing_method,
            trigger_policy=TriggerPolicy(),
            scope_policy=JoinScopePolicy(
                ttl_seconds=ttl_seconds,
                max_left_buffer=max_left_buffer,
                max_right_buffer=max_right_buffer,
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "semantic": self.semantic.to_dict(),
            "query_id": self.query_id,
            "query_version": self.query_version,
            "pairing_method": self.pairing_method,
            "trigger_policy": self.trigger_policy.to_dict(),
            "scope_policy": self.scope_policy.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "JoinQuerySpec":
        return cls(
            semantic=SemanticSpec.from_dict(d.get("semantic", {})),
            query_id=d.get("query_id", "default"),
            query_version=d.get("query_version", 1),
            pairing_method=d.get("pairing_method", "candidate_pruned"),
            trigger_policy=TriggerPolicy.from_dict(d.get("trigger_policy", {})),
            scope_policy=JoinScopePolicy.from_dict(d.get("scope_policy", {})),
        )
