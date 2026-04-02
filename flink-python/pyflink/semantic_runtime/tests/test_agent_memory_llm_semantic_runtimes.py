"""Unit tests for agent-memory LLM semantic runtime adapters."""

from __future__ import annotations

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import asyncio
import json
import os  # noqa: E401,E402
import pathlib  # noqa: E401,E402

import pyflink as _pf  # noqa: E401,E402

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

from typing import Sequence

from pyflink.semantic_runtime.llm_client import LLMCallMetrics, LLMClient
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0GraphExtractedEntity,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.runtime.llm_semantic_runtimes import (
    Mem0GraphLLMSemanticRuntime,
    ZepLLMSemanticRuntime,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.entity_reference_mode import (
    DRIFT_POLICY_FAIL_FAST,
    DRIFT_POLICY_UPSTREAM_COMPATIBLE,
    ENTITY_REFERENCE_MODE_INDEX,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.contracts import (
    ZepEpisodeCandidate,
    ZepExtractedEntity,
    ZepResolvedEntity,
)


class _ScriptedLLMClient(LLMClient):
    """Deterministic in-memory LLM client used by tests."""

    def __init__(self, *, responses: Sequence[str]) -> None:
        self._responses = list(responses)
        self._index = 0

    async def call(self, prompt: str) -> tuple[str, LLMCallMetrics]:
        _ = prompt
        if self._index >= len(self._responses):
            raise RuntimeError("scripted llm response exhausted")
        response = self._responses[self._index]
        self._index += 1
        return response, LLMCallMetrics(latency_ms=1.0, attempts=1)


def test_mem0_extract_relations_uses_allowed_entity_names_by_default() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "entities": [
                            {
                                "source": "Alice",
                                "relationship": "knows",
                                "destination": "Bob",
                            }
                        ]
                    }
                )
            ]
        )
    )
    result = asyncio.run(
        runtime.extract_relations(
            messages=["Alice knows Bob"],
            entities=[
                Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
            ],
            allowed_entity_names=["Alice", "Bob"],
            prompt="extract relations",
        )
    )
    assert len(result) == 1
    assert result[0].source_entity_name == "Alice"
    assert result[0].destination_entity_name == "Bob"
    assert result[0].relationship == "knows"


def test_mem0_extract_relations_rejects_unknown_name_in_strict_name_mode() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "entities": [
                            {
                                "source": "Carol",
                                "relationship": "knows",
                                "destination": "Bob",
                            }
                        ]
                    }
                )
            ]
        ),
        relation_entity_reference_mode="name",
        drift_policy=DRIFT_POLICY_FAIL_FAST,
    )
    try:
        asyncio.run(
            runtime.extract_relations(
                messages=["Alice knows Bob"],
                entities=[
                    Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                    Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
                ],
                allowed_entity_names=["Alice", "Bob"],
                prompt="extract relations",
            )
        )
    except ValueError as exc:
        assert "source not in allowed_entity_names" in str(exc)
        return
    raise AssertionError("Expected ValueError for unknown source entity name")


def test_mem0_extract_relations_uses_allowed_entity_indexes() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "entities": [
                            {
                                "source_index": 0,
                                "relationship": "knows",
                                "destination_index": 1,
                            }
                        ]
                    }
                )
            ]
        ),
        relation_entity_reference_mode=ENTITY_REFERENCE_MODE_INDEX,
    )
    result = asyncio.run(
        runtime.extract_relations(
            messages=["Alice knows Bob"],
            entities=[
                Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
            ],
            allowed_entity_names=["Alice", "Bob"],
            prompt="extract relations",
        )
    )
    assert len(result) == 1
    assert result[0].source_entity_name == "Alice"
    assert result[0].destination_entity_name == "Bob"
    assert result[0].relationship == "knows"


def test_mem0_extract_relations_rejects_out_of_range_index() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "entities": [
                            {
                                "source_index": 2,
                                "relationship": "knows",
                                "destination_index": 1,
                            }
                        ]
                    }
                )
            ]
        ),
        relation_entity_reference_mode=ENTITY_REFERENCE_MODE_INDEX,
        drift_policy=DRIFT_POLICY_FAIL_FAST,
    )
    try:
        asyncio.run(
            runtime.extract_relations(
                messages=["Alice knows Bob"],
                entities=[
                    Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                    Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
                ],
                allowed_entity_names=["Alice", "Bob"],
                prompt="extract relations",
            )
        )
    except ValueError as exc:
        assert "source_index out of range" in str(exc)
        return
    raise AssertionError("Expected ValueError for out-of-range source_index")


def test_mem0_extract_relations_allows_unknown_name_in_default_mode() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "entities": [
                            {
                                "source": "Carol",
                                "relationship": "knows",
                                "destination": "Bob",
                            }
                        ]
                    }
                )
            ]
        )
    )
    result = asyncio.run(
        runtime.extract_relations(
            messages=["Alice knows Bob"],
            entities=[
                Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
            ],
            allowed_entity_names=["Alice", "Bob"],
            prompt="extract relations",
        )
    )
    assert len(result) == 1
    assert result[0].source_entity_name == "Carol"
    assert result[0].destination_entity_name == "Bob"


def test_mem0_extract_relations_skips_out_of_range_index_in_upstream_mode() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "entities": [
                            {
                                "source_index": 8,
                                "relationship": "knows",
                                "destination_index": 1,
                            }
                        ]
                    }
                )
            ]
        ),
        relation_entity_reference_mode=ENTITY_REFERENCE_MODE_INDEX,
        drift_policy=DRIFT_POLICY_UPSTREAM_COMPATIBLE,
    )
    result = asyncio.run(
        runtime.extract_relations(
            messages=["Alice knows Bob"],
            entities=[
                Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
            ],
            allowed_entity_names=["Alice", "Bob"],
            prompt="extract relations",
        )
    )
    assert result == []


def test_mem0_resolve_entity_upstream_mode_downgrades_same_without_target() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "SAME",
                        "entity_name": "Alice",
                        "target_entity_id": None,
                    }
                )
            ]
        )
    )
    result = asyncio.run(
        runtime.resolve_entity(
            entity=Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
            candidates=[],
            prompt="resolve entity",
        )
    )
    assert result.decision == "DIFFERENT"
    assert result.target_entity_id is None


def test_mem0_resolve_entity_fail_fast_requires_target_for_same() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "SAME",
                        "entity_name": "Alice",
                        "target_entity_id": None,
                    }
                )
            ]
        ),
        drift_policy=DRIFT_POLICY_FAIL_FAST,
    )
    try:
        asyncio.run(
            runtime.resolve_entity(
                entity=Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                candidates=[],
                prompt="resolve entity",
            )
        )
    except ValueError as exc:
        assert "decision=SAME requires non-empty target_entity_id" in str(exc)
        return
    raise AssertionError("Expected ValueError for SAME without target_entity_id")


def test_mem0_resolve_entity_upstream_mode_ignores_target_for_different() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "DIFFERENT",
                        "entity_name": "Alice",
                        "target_entity_id": "e1",
                    }
                )
            ]
        )
    )
    result = asyncio.run(
        runtime.resolve_entity(
            entity=Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
            candidates=[],
            prompt="resolve entity",
        )
    )
    assert result.decision == "DIFFERENT"
    assert result.target_entity_id is None


def test_mem0_resolve_entity_fail_fast_rejects_target_for_different() -> None:
    runtime = Mem0GraphLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "DIFFERENT",
                        "entity_name": "Alice",
                        "target_entity_id": "e1",
                    }
                )
            ]
        ),
        drift_policy=DRIFT_POLICY_FAIL_FAST,
    )
    try:
        asyncio.run(
            runtime.resolve_entity(
                entity=Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
                candidates=[],
                prompt="resolve entity",
            )
        )
    except ValueError as exc:
        assert "decision=DIFFERENT requires null target_entity_id" in str(exc)
        return
    raise AssertionError("Expected ValueError for DIFFERENT with non-null target_entity_id")


def test_zep_resolve_entity_upstream_mode_downgrades_missing_existing_target() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "EXISTING",
                        "entity_name": "Alice",
                        "target_entity_id": None,
                    }
                )
            ]
        )
    )
    result = asyncio.run(
        runtime.resolve_entity(
            extracted_entity=ZepExtractedEntity(entity_name="Alice", type_id="person"),
            candidates=[],
            message="Alice likes tea",
            recent_episodes=[],
            prompt="resolve entity",
        )
    )
    assert result.decision == "NEW"
    assert result.target_entity_id is None


def test_zep_resolve_entity_fail_fast_requires_target_for_existing() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "EXISTING",
                        "entity_name": "Alice",
                        "target_entity_id": None,
                    }
                )
            ]
        ),
        drift_policy=DRIFT_POLICY_FAIL_FAST,
    )
    try:
        asyncio.run(
            runtime.resolve_entity(
                extracted_entity=ZepExtractedEntity(entity_name="Alice", type_id="person"),
                candidates=[],
                message="Alice likes tea",
                recent_episodes=[],
                prompt="resolve entity",
            )
        )
    except ValueError as exc:
        assert "decision=EXISTING requires non-empty target_entity_id" in str(exc)
        return
    raise AssertionError("Expected ValueError for EXISTING without target_entity_id")


def test_zep_resolve_entity_upstream_mode_ignores_target_for_new() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "NEW",
                        "entity_name": "Alice",
                        "target_entity_id": "e1",
                    }
                )
            ]
        )
    )
    result = asyncio.run(
        runtime.resolve_entity(
            extracted_entity=ZepExtractedEntity(entity_name="Alice", type_id="person"),
            candidates=[],
            message="Alice likes tea",
            recent_episodes=[],
            prompt="resolve entity",
        )
    )
    assert result.decision == "NEW"
    assert result.target_entity_id is None


def test_zep_resolve_entity_fail_fast_rejects_target_for_new() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "decision": "NEW",
                        "entity_name": "Alice",
                        "target_entity_id": "e1",
                    }
                )
            ]
        ),
        drift_policy=DRIFT_POLICY_FAIL_FAST,
    )
    try:
        asyncio.run(
            runtime.resolve_entity(
                extracted_entity=ZepExtractedEntity(entity_name="Alice", type_id="person"),
                candidates=[],
                message="Alice likes tea",
                recent_episodes=[],
                prompt="resolve entity",
            )
        )
    except ValueError as exc:
        assert "decision=NEW requires null target_entity_id" in str(exc)
        return
    raise AssertionError("Expected ValueError for NEW with non-null target_entity_id")


def test_zep_extract_edges_uses_allowed_entity_indexes() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "edges": [
                            {
                                "source_index": 0,
                                "destination_index": 1,
                                "relation": "knows",
                                "fact": "Alice knows Bob",
                            }
                        ]
                    }
                )
            ]
        ),
        edge_entity_reference_mode=ENTITY_REFERENCE_MODE_INDEX,
    )
    result = asyncio.run(
        runtime.extract_edges(
            message="Alice knows Bob",
            resolved_entities=[
                ZepResolvedEntity(
                    entity_id="e1",
                    entity_name="Alice",
                    type_id="person",
                    summary="Alice summary",
                ),
                ZepResolvedEntity(
                    entity_id="e2",
                    entity_name="Bob",
                    type_id="person",
                    summary="Bob summary",
                ),
            ],
            allowed_entity_names=["Alice", "Bob"],
            recent_episodes=[
                ZepEpisodeCandidate(
                    episode_id="ep1",
                    content="Earlier Alice and Bob met",
                    created_at_ms=1,
                )
            ],
            prompt="extract edges",
        )
    )
    assert len(result) == 1
    assert result[0].source_entity_name == "Alice"
    assert result[0].destination_entity_name == "Bob"
    assert result[0].relation == "knows"


def test_zep_extract_edges_rejects_out_of_range_index() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "edges": [
                            {
                                "source_index": 3,
                                "destination_index": 0,
                                "relation": "knows",
                                "fact": "Alice knows Bob",
                            }
                        ]
                    }
                )
            ]
        ),
        edge_entity_reference_mode=ENTITY_REFERENCE_MODE_INDEX,
        drift_policy=DRIFT_POLICY_FAIL_FAST,
    )
    try:
        asyncio.run(
            runtime.extract_edges(
                message="Alice knows Bob",
                resolved_entities=[
                    ZepResolvedEntity(
                        entity_id="e1",
                        entity_name="Alice",
                        type_id="person",
                        summary="Alice summary",
                    ),
                    ZepResolvedEntity(
                        entity_id="e2",
                        entity_name="Bob",
                        type_id="person",
                        summary="Bob summary",
                    ),
                ],
                allowed_entity_names=["Alice", "Bob"],
                recent_episodes=[],
                prompt="extract edges",
            )
        )
    except ValueError as exc:
        assert "source_index out of range" in str(exc)
        return
    raise AssertionError("Expected ValueError for out-of-range source_index")


def test_zep_extract_edges_allows_unknown_entity_name_in_default_mode() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "edges": [
                            {
                                "source_entity_name": "Carol",
                                "destination_entity_name": "Bob",
                                "relation": "knows",
                                "fact": "Carol knows Bob",
                            }
                        ]
                    }
                )
            ]
        )
    )
    result = asyncio.run(
        runtime.extract_edges(
            message="Carol knows Bob",
            resolved_entities=[
                ZepResolvedEntity(
                    entity_id="e1",
                    entity_name="Alice",
                    type_id="person",
                    summary="Alice summary",
                ),
                ZepResolvedEntity(
                    entity_id="e2",
                    entity_name="Bob",
                    type_id="person",
                    summary="Bob summary",
                ),
            ],
            allowed_entity_names=["Alice", "Bob"],
            recent_episodes=[],
            prompt="extract edges",
        )
    )
    assert len(result) == 1
    assert result[0].source_entity_name == "Carol"
    assert result[0].destination_entity_name == "Bob"


def test_zep_extract_edges_skips_out_of_range_index_in_upstream_mode() -> None:
    runtime = ZepLLMSemanticRuntime(
        client=_ScriptedLLMClient(
            responses=[
                json.dumps(
                    {
                        "edges": [
                            {
                                "source_index": 9,
                                "destination_index": 0,
                                "relation": "knows",
                                "fact": "Alice knows Bob",
                            }
                        ]
                    }
                )
            ]
        ),
        edge_entity_reference_mode=ENTITY_REFERENCE_MODE_INDEX,
        drift_policy=DRIFT_POLICY_UPSTREAM_COMPATIBLE,
    )
    result = asyncio.run(
        runtime.extract_edges(
            message="Alice knows Bob",
            resolved_entities=[
                ZepResolvedEntity(
                    entity_id="e1",
                    entity_name="Alice",
                    type_id="person",
                    summary="Alice summary",
                ),
                ZepResolvedEntity(
                    entity_id="e2",
                    entity_name="Bob",
                    type_id="person",
                    summary="Bob summary",
                ),
            ],
            allowed_entity_names=["Alice", "Bob"],
            recent_episodes=[],
            prompt="extract edges",
        )
    )
    assert result == []
