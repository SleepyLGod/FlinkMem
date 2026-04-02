"""Configuration for Mem0 workflow reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet


DEFAULT_MEM0_BASIC_SIMILAR_TOP_K = 5
DEFAULT_MEM0_BASIC_SEARCH_TOP_K = 10
DEFAULT_MEM0_BASIC_MEMORY_TYPES = frozenset({"fact"})
DEFAULT_MEM0_BASIC_FACT_RESOLVE_CONCURRENCY = 8
DEFAULT_MEM0_BASIC_FACT_WRITE_GROUP_CONCURRENCY = 8
VALID_MEM0_RECALL_BACKENDS = frozenset({"embedding", "llm"})
DEFAULT_MEM0_RECALL_BACKEND = "embedding"
DEFAULT_MEM0_GRAPH_ENTITY_RECALL_TOP_K = 1
DEFAULT_MEM0_GRAPH_RELATION_RECALL_TOP_K = 5
DEFAULT_MEM0_GRAPH_SEARCH_TOP_K = 10
DEFAULT_MEM0_GRAPH_ENTITY_RECALL_BACKEND = "embedding"
DEFAULT_MEM0_GRAPH_RELATION_RECALL_BACKEND = "embedding"
DEFAULT_MEM0_GRAPH_ENTITY_RESOLVE_CONCURRENCY = 8
DEFAULT_MEM0_GRAPH_ENTITY_UPSERT_GROUP_CONCURRENCY = 8
DEFAULT_MEM0_GRAPH_RELATION_RESOLVE_CONCURRENCY = 8
DEFAULT_MEM0_GRAPH_RELATION_WRITE_GROUP_CONCURRENCY = 8
DEFAULT_MEM0_GRAPH_UPSTREAM_PLACEHOLDER_ENTITY_TYPE = "unknown"

# Source prompt references:
# - https://raw.githubusercontent.com/mem0ai/mem0/main/mem0/configs/prompts.py
# - FACT_RETRIEVAL_PROMPT / DEFAULT_UPDATE_MEMORY_PROMPT
DEFAULT_MEM0_BASIC_FACT_EXTRACTION_PROMPT = (
    "Extract standalone, self-contained facts from the conversation."
)
DEFAULT_MEM0_BASIC_FACT_RESOLUTION_PROMPT = (
    "Given one new fact and candidate old memories, output exactly one action from "
    "{ADD, UPDATE, DELETE, NONE}. For UPDATE/DELETE, provide target_memory_id. "
    "For ADD/UPDATE, provide content."
)
DEFAULT_MEM0_GRAPH_ENTITY_EXTRACTION_PROMPT = (
    "Extract entities and their types from the input text. "
    "Return exactly one JSON object following mem0 graph tool `extract_entities` schema: "
    "{'entities': [{'entity': '<entity_name>', 'entity_type': '<entity_type>'}]}. "
    "Use only entities explicitly present in the input text."
)
DEFAULT_MEM0_GRAPH_RELATION_EXTRACTION_PROMPT = (
    "Establish relationships among extracted entities based on the input text. "
    "Return exactly one JSON object following mem0 graph relation tool schema "
    "(`establish_relationships`/`establish_relations`): "
    "{'entities': [{'source': '<entity_name>', "
    "'relationship': '<relationship>', "
    "'destination': '<entity_name>'}]}. "
    "Prefer entities from allowed_entity_names when possible."
)
DEFAULT_MEM0_GRAPH_ENTITY_IDENTITY_PROMPT = (
    "Given one extracted entity and one candidate graph entity, decide whether they refer "
    "to the same real-world entity. Return exactly one JSON object with keys: "
    "{'decision': 'SAME|DIFFERENT', 'target_entity_id': '<id_or_null>', "
    "'reason': '<short_reason>', 'confidence': <0_to_1_float>}."
)
DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT = (
    "Given one new relation and candidate existing graph relations, choose exactly one action "
    "label from {CONTRADICTS, AUGMENTS, NEW}. "
    "Action mapping to mem0 graph tools must be preserved: "
    "CONTRADICTS -> delete_graph_memory(source, destination, relationship), "
    "AUGMENTS -> update_graph_memory(source, destination, relationship), "
    "NEW -> add_graph_memory(source, destination, relationship, source_type, destination_type). "
    "Return exactly one JSON object with keys: "
    "{'action': 'CONTRADICTS|AUGMENTS|NEW', "
    "'target_relation_id': '<id_or_null>', "
    "'source': '<entity_name>', "
    "'destination': '<entity_name>', "
    "'relationship': '<relationship>', "
    "'source_type': '<entity_type_or_null>', "
    "'destination_type': '<entity_type_or_null>', "
    "'reason': '<short_reason>', "
    "'confidence': <0_to_1_float>}. "
    "When action is CONTRADICTS or AUGMENTS, target_relation_id is required."
)


@dataclass(frozen=True)
class Mem0BasicConfig:
    """Workflow-level config for Mem0 Basic insertion/retrieval."""

    fact_extraction_prompt: str = DEFAULT_MEM0_BASIC_FACT_EXTRACTION_PROMPT
    fact_resolution_prompt: str = DEFAULT_MEM0_BASIC_FACT_RESOLUTION_PROMPT
    similar_top_k: int = DEFAULT_MEM0_BASIC_SIMILAR_TOP_K
    search_top_k: int = DEFAULT_MEM0_BASIC_SEARCH_TOP_K
    memory_types: FrozenSet[str] = DEFAULT_MEM0_BASIC_MEMORY_TYPES
    recall_backend: str = DEFAULT_MEM0_RECALL_BACKEND
    fact_resolve_concurrency: int = DEFAULT_MEM0_BASIC_FACT_RESOLVE_CONCURRENCY
    fact_write_group_concurrency: int = (
        DEFAULT_MEM0_BASIC_FACT_WRITE_GROUP_CONCURRENCY
    )

    def __post_init__(self) -> None:
        if not self.fact_extraction_prompt.strip():
            raise ValueError("fact_extraction_prompt must be non-empty")
        if not self.fact_resolution_prompt.strip():
            raise ValueError("fact_resolution_prompt must be non-empty")
        if int(self.similar_top_k) <= 0:
            raise ValueError("similar_top_k must be > 0")
        if int(self.search_top_k) <= 0:
            raise ValueError("search_top_k must be > 0")
        if not self.memory_types:
            raise ValueError("memory_types must not be empty")
        if self.recall_backend not in VALID_MEM0_RECALL_BACKENDS:
            raise ValueError(
                f"recall_backend must be one of {sorted(VALID_MEM0_RECALL_BACKENDS)!r}"
            )
        if int(self.fact_resolve_concurrency) <= 0:
            raise ValueError("fact_resolve_concurrency must be > 0")
        if int(self.fact_write_group_concurrency) <= 0:
            raise ValueError("fact_write_group_concurrency must be > 0")


@dataclass(frozen=True)
class Mem0GraphConfig:
    """Workflow-level config for Mem0 Graph insertion/retrieval."""

    entity_extraction_prompt: str = DEFAULT_MEM0_GRAPH_ENTITY_EXTRACTION_PROMPT
    relation_extraction_prompt: str = DEFAULT_MEM0_GRAPH_RELATION_EXTRACTION_PROMPT
    entity_identity_prompt: str = DEFAULT_MEM0_GRAPH_ENTITY_IDENTITY_PROMPT
    relation_resolution_prompt: str = DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT
    entity_recall_top_k: int = DEFAULT_MEM0_GRAPH_ENTITY_RECALL_TOP_K
    relation_recall_top_k: int = DEFAULT_MEM0_GRAPH_RELATION_RECALL_TOP_K
    search_top_k: int = DEFAULT_MEM0_GRAPH_SEARCH_TOP_K
    entity_recall_backend: str = DEFAULT_MEM0_GRAPH_ENTITY_RECALL_BACKEND
    relation_recall_backend: str = DEFAULT_MEM0_GRAPH_RELATION_RECALL_BACKEND
    entity_resolve_concurrency: int = DEFAULT_MEM0_GRAPH_ENTITY_RESOLVE_CONCURRENCY
    entity_upsert_group_concurrency: int = (
        DEFAULT_MEM0_GRAPH_ENTITY_UPSERT_GROUP_CONCURRENCY
    )
    relation_resolve_concurrency: int = DEFAULT_MEM0_GRAPH_RELATION_RESOLVE_CONCURRENCY
    relation_write_group_concurrency: int = (
        DEFAULT_MEM0_GRAPH_RELATION_WRITE_GROUP_CONCURRENCY
    )
    upstream_placeholder_entity_type: str = (
        DEFAULT_MEM0_GRAPH_UPSTREAM_PLACEHOLDER_ENTITY_TYPE
    )

    def __post_init__(self) -> None:
        if not self.entity_extraction_prompt.strip():
            raise ValueError("entity_extraction_prompt must be non-empty")
        if not self.relation_extraction_prompt.strip():
            raise ValueError("relation_extraction_prompt must be non-empty")
        if not self.entity_identity_prompt.strip():
            raise ValueError("entity_identity_prompt must be non-empty")
        if not self.relation_resolution_prompt.strip():
            raise ValueError("relation_resolution_prompt must be non-empty")
        if int(self.entity_recall_top_k) <= 0:
            raise ValueError("entity_recall_top_k must be > 0")
        if int(self.relation_recall_top_k) <= 0:
            raise ValueError("relation_recall_top_k must be > 0")
        if int(self.search_top_k) <= 0:
            raise ValueError("search_top_k must be > 0")
        if self.entity_recall_backend not in VALID_MEM0_RECALL_BACKENDS:
            raise ValueError(
                "entity_recall_backend must be one of "
                f"{sorted(VALID_MEM0_RECALL_BACKENDS)!r}"
            )
        if self.relation_recall_backend not in VALID_MEM0_RECALL_BACKENDS:
            raise ValueError(
                "relation_recall_backend must be one of "
                f"{sorted(VALID_MEM0_RECALL_BACKENDS)!r}"
            )
        if int(self.entity_resolve_concurrency) <= 0:
            raise ValueError("entity_resolve_concurrency must be > 0")
        if int(self.entity_upsert_group_concurrency) <= 0:
            raise ValueError("entity_upsert_group_concurrency must be > 0")
        if int(self.relation_resolve_concurrency) <= 0:
            raise ValueError("relation_resolve_concurrency must be > 0")
        if int(self.relation_write_group_concurrency) <= 0:
            raise ValueError("relation_write_group_concurrency must be > 0")
        if not str(self.upstream_placeholder_entity_type).strip():
            raise ValueError("upstream_placeholder_entity_type must be non-empty")
