"""Configuration for Zep/Graphiti workflow reconstruction."""

from __future__ import annotations

from dataclasses import dataclass


DEFAULT_ZEP_RECENT_EPISODE_LIMIT = 10
DEFAULT_ZEP_ENTITY_CANDIDATE_TOP_K = 4
DEFAULT_ZEP_EDGE_CANDIDATE_TOP_K = 4
DEFAULT_ZEP_DEDUP_SCORE_THRESHOLD = 0.6
DEFAULT_ZEP_ENTITY_RESOLVE_CONCURRENCY = 8
DEFAULT_ZEP_ENTITY_UPSERT_GROUP_CONCURRENCY = 8
DEFAULT_ZEP_ENTITY_SUMMARY_CONCURRENCY = 8
DEFAULT_ZEP_EDGE_RESOLVE_CONCURRENCY = 8
DEFAULT_ZEP_EDGE_WRITE_GROUP_CONCURRENCY = 8

DEFAULT_ZEP_ENTITY_EXTRACTION_PROMPT = (
    "Extract entity nodes mentioned explicitly or implicitly in CURRENT MESSAGE. "
    "Return structured entities with stable names and type_id."
)
DEFAULT_ZEP_ENTITY_DEDUP_PROMPT = (
    "Given one extracted entity and candidate graph entities, decide if they refer to "
    "the same real-world object or concept. Return EXISTING or NEW."
)
DEFAULT_ZEP_EDGE_EXTRACTION_PROMPT = (
    "Extract all factual relationships between resolved entities based on CURRENT MESSAGE. "
    "Return source_entity_name, destination_entity_name, relation, and fact."
)
DEFAULT_ZEP_EDGE_RESOLUTION_PROMPT = (
    "Given one new fact edge and candidate existing edges, decide exactly one action from "
    "{ADD, DUPLICATE, CONTRADICTS}. If CONTRADICTS, provide target_edge_id."
)
DEFAULT_ZEP_ENTITY_SUMMARY_PROMPT = (
    "Update summary combining relevant information about ENTITY from CURRENT MESSAGE and "
    "recent episodic context."
)


@dataclass(frozen=True)
class ZepWorkflowConfig:
    """Workflow-level config for Zep/Graphiti `add_episode` flow."""

    recent_episode_limit: int = DEFAULT_ZEP_RECENT_EPISODE_LIMIT
    entity_candidate_top_k: int = DEFAULT_ZEP_ENTITY_CANDIDATE_TOP_K
    edge_candidate_top_k: int = DEFAULT_ZEP_EDGE_CANDIDATE_TOP_K
    dedup_score_threshold: float = DEFAULT_ZEP_DEDUP_SCORE_THRESHOLD
    entity_resolve_concurrency: int = DEFAULT_ZEP_ENTITY_RESOLVE_CONCURRENCY
    entity_upsert_group_concurrency: int = (
        DEFAULT_ZEP_ENTITY_UPSERT_GROUP_CONCURRENCY
    )
    entity_summary_concurrency: int = DEFAULT_ZEP_ENTITY_SUMMARY_CONCURRENCY
    edge_resolve_concurrency: int = DEFAULT_ZEP_EDGE_RESOLVE_CONCURRENCY
    edge_write_group_concurrency: int = DEFAULT_ZEP_EDGE_WRITE_GROUP_CONCURRENCY

    entity_extraction_prompt: str = DEFAULT_ZEP_ENTITY_EXTRACTION_PROMPT
    entity_dedup_prompt: str = DEFAULT_ZEP_ENTITY_DEDUP_PROMPT
    edge_extraction_prompt: str = DEFAULT_ZEP_EDGE_EXTRACTION_PROMPT
    edge_resolution_prompt: str = DEFAULT_ZEP_EDGE_RESOLUTION_PROMPT
    entity_summary_prompt: str = DEFAULT_ZEP_ENTITY_SUMMARY_PROMPT

    def __post_init__(self) -> None:
        if int(self.recent_episode_limit) <= 0:
            raise ValueError("recent_episode_limit must be > 0")
        if int(self.entity_candidate_top_k) <= 0:
            raise ValueError("entity_candidate_top_k must be > 0")
        if int(self.edge_candidate_top_k) <= 0:
            raise ValueError("edge_candidate_top_k must be > 0")
        if not (0.0 <= float(self.dedup_score_threshold) <= 1.0):
            raise ValueError("dedup_score_threshold must be within [0, 1]")
        if int(self.entity_resolve_concurrency) <= 0:
            raise ValueError("entity_resolve_concurrency must be > 0")
        if int(self.entity_upsert_group_concurrency) <= 0:
            raise ValueError("entity_upsert_group_concurrency must be > 0")
        if int(self.entity_summary_concurrency) <= 0:
            raise ValueError("entity_summary_concurrency must be > 0")
        if int(self.edge_resolve_concurrency) <= 0:
            raise ValueError("edge_resolve_concurrency must be > 0")
        if int(self.edge_write_group_concurrency) <= 0:
            raise ValueError("edge_write_group_concurrency must be > 0")
        if not self.entity_extraction_prompt.strip():
            raise ValueError("entity_extraction_prompt must be non-empty")
        if not self.entity_dedup_prompt.strip():
            raise ValueError("entity_dedup_prompt must be non-empty")
        if not self.edge_extraction_prompt.strip():
            raise ValueError("edge_extraction_prompt must be non-empty")
        if not self.edge_resolution_prompt.strip():
            raise ValueError("edge_resolution_prompt must be non-empty")
        if not self.entity_summary_prompt.strip():
            raise ValueError("entity_summary_prompt must be non-empty")
