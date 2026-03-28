"""Configuration for EverMemOS-style workflow reconstruction."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet


DEFAULT_FORCE_SPLIT_TOKEN_THRESHOLD = 8192
DEFAULT_FORCE_SPLIT_MESSAGE_THRESHOLD = 50
DEFAULT_PROFILE_MIN_MEMCELLS = 1
DEFAULT_SUPPORTED_SCENES: FrozenSet[str] = frozenset({"assistant", "group_chat"})

DEFAULT_KEYWORD_RECALL_TOP_K = 20
DEFAULT_VECTOR_RECALL_TOP_K = 20
DEFAULT_OUTPUT_TOP_K = 10
DEFAULT_RRF_K = 60
DEFAULT_HYBRID_KEYWORD_WEIGHT = 0.5
DEFAULT_HYBRID_VECTOR_WEIGHT = 0.5


@dataclass(frozen=True)
class EverMemOSWorkflowConfig:
    """Workflow-level config for EverMemOS insertion/retrieval orchestration."""

    force_split_token_threshold: int = DEFAULT_FORCE_SPLIT_TOKEN_THRESHOLD
    force_split_message_threshold: int = DEFAULT_FORCE_SPLIT_MESSAGE_THRESHOLD
    profile_min_memcells: int = DEFAULT_PROFILE_MIN_MEMCELLS
    supported_scenes: FrozenSet[str] = field(default_factory=lambda: DEFAULT_SUPPORTED_SCENES)

    def __post_init__(self) -> None:
        if self.force_split_token_threshold <= 0:
            raise ValueError("force_split_token_threshold must be > 0")
        if self.force_split_message_threshold <= 0:
            raise ValueError("force_split_message_threshold must be > 0")
        if self.profile_min_memcells <= 0:
            raise ValueError("profile_min_memcells must be > 0")
        if not self.supported_scenes:
            raise ValueError("supported_scenes must not be empty")


@dataclass(frozen=True)
class EverMemOSRetrievalConfig:
    """Workflow-level config for EverMemOS retrieval orchestration."""

    keyword_recall_top_k: int = DEFAULT_KEYWORD_RECALL_TOP_K
    vector_recall_top_k: int = DEFAULT_VECTOR_RECALL_TOP_K
    output_top_k: int = DEFAULT_OUTPUT_TOP_K
    rrf_k: int = DEFAULT_RRF_K
    hybrid_keyword_weight: float = DEFAULT_HYBRID_KEYWORD_WEIGHT
    hybrid_vector_weight: float = DEFAULT_HYBRID_VECTOR_WEIGHT
    enable_rerank: bool = True
    supported_modes: FrozenSet[str] = field(
        default_factory=lambda: frozenset(
            {"keyword", "vector", "hybrid", "rrf", "agentic"}
        )
    )
    supported_scenes: FrozenSet[str] = field(default_factory=lambda: DEFAULT_SUPPORTED_SCENES)

    def __post_init__(self) -> None:
        if self.keyword_recall_top_k <= 0:
            raise ValueError("keyword_recall_top_k must be > 0")
        if self.vector_recall_top_k <= 0:
            raise ValueError("vector_recall_top_k must be > 0")
        if self.output_top_k <= 0:
            raise ValueError("output_top_k must be > 0")
        if self.rrf_k <= 0:
            raise ValueError("rrf_k must be > 0")
        if self.hybrid_keyword_weight < 0.0:
            raise ValueError("hybrid_keyword_weight must be >= 0")
        if self.hybrid_vector_weight < 0.0:
            raise ValueError("hybrid_vector_weight must be >= 0")
        if (
            self.hybrid_keyword_weight + self.hybrid_vector_weight
            <= 0.0
        ):
            raise ValueError(
                "hybrid_keyword_weight + hybrid_vector_weight must be > 0"
            )
        if not self.supported_modes:
            raise ValueError("supported_modes must not be empty")
        if not self.supported_scenes:
            raise ValueError("supported_scenes must not be empty")
