#!/usr/bin/env python3
"""LLM-backed semantic runtime adapters for Mem0 and Zep workflows."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping, Sequence

from pyflink.semantic_runtime.llm_client import LLMClient
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    RetrievedMemory,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0FactResolution,
    Mem0GraphEntityCandidate,
    Mem0GraphEntityResolution,
    Mem0GraphExtractedEntity,
    Mem0GraphExtractedRelation,
    Mem0GraphRelationCandidate,
    Mem0GraphRelationResolution,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.contracts import (
    ZepEdgeCandidate,
    ZepEdgeResolution,
    ZepEntityCandidate,
    ZepEntityResolution,
    ZepEpisodeCandidate,
    ZepExtractedEdge,
    ZepExtractedEntity,
    ZepResolvedEntity,
)


def _format_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _as_text(value: Any, *, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def _as_float(value: Any, *, field_name: str) -> float:
    if not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be numeric")
    return float(value)


def _as_str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


class _JSONLLMHelper:
    """Shared strict JSON-calling helper."""

    def __init__(self, *, client: LLMClient) -> None:
        self._client = client

    async def call_json(self, *, prompt: str) -> Any:
        response_text, _ = await self._client.call(prompt)
        return json.loads(response_text)


class Mem0BasicLLMSemanticRuntime:
    """LLM-backed Mem0 Basic semantic runtime."""

    def __init__(self, *, client: LLMClient) -> None:
        self._helper = _JSONLLMHelper(client=client)

    async def extract_facts(
        self,
        *,
        messages: Sequence[str],
        prompt: str,
    ) -> Sequence[str]:
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON with schema:\n"
            '{"facts":["fact 1","fact 2"]}\n'
            f"messages={_format_json(list(messages))}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("extract_facts payload must be object")
        facts = payload.get("facts")
        if not isinstance(facts, list):
            raise ValueError("extract_facts payload.facts must be list")
        output: list[str] = []
        for item in facts:
            output.append(_as_text(item, field_name="fact"))
        return output

    async def resolve_fact(
        self,
        *,
        fact: str,
        candidates: Sequence[RetrievedMemory],
        prompt: str,
    ) -> Mem0FactResolution:
        candidates_payload = [
            {
                "memory_id": candidate.memory_id,
                "content": candidate.content,
                "memory_type": candidate.memory_type,
                "score": candidate.score,
            }
            for candidate in candidates
        ]
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"action":"ADD|UPDATE|DELETE|NONE","fact":"...","target_memory_id":"...|null",'
            '"content":"...|null","reason":"...","confidence":0.0}\n'
            f"fact={_format_json(fact)}\n"
            f"candidates={_format_json(candidates_payload)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("resolve_fact payload must be object")
        return Mem0FactResolution(
            action=_as_text(payload.get("action"), field_name="action"),
            fact=_as_text(payload.get("fact"), field_name="fact"),
            target_memory_id=_as_str_or_none(payload.get("target_memory_id")),
            content=_as_str_or_none(payload.get("content")),
            reason=str(payload.get("reason", "")),
            confidence=_as_float(payload.get("confidence", 0.0), field_name="confidence"),
        )


class Mem0GraphLLMSemanticRuntime:
    """LLM-backed Mem0 Graph semantic runtime."""

    def __init__(self, *, client: LLMClient) -> None:
        self._helper = _JSONLLMHelper(client=client)

    async def extract_entities(
        self,
        *,
        messages: Sequence[str],
        prompt: str,
    ) -> Sequence[Mem0GraphExtractedEntity]:
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"entities":[{"entity":"...","entity_type":"..."}]}\n'
            f"messages={_format_json(list(messages))}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("extract_entities payload must be object")
        entities = payload.get("entities")
        if not isinstance(entities, list):
            raise ValueError("extract_entities payload.entities must be list")
        output: list[Mem0GraphExtractedEntity] = []
        for item in entities:
            if not isinstance(item, Mapping):
                raise ValueError("entity row must be object")
            output.append(
                Mem0GraphExtractedEntity(
                    entity_name=_as_text(item.get("entity"), field_name="entity"),
                    entity_type=_as_text(item.get("entity_type"), field_name="entity_type"),
                )
            )
        return output

    async def extract_relations(
        self,
        *,
        messages: Sequence[str],
        entities: Sequence[Mem0GraphExtractedEntity],
        prompt: str,
    ) -> Sequence[Mem0GraphExtractedRelation]:
        entities_payload = [
            {"entity": entity.entity_name, "entity_type": entity.entity_type}
            for entity in entities
        ]
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"entities":[{"source":"...","relationship":"...","destination":"..."}]}\n'
            f"messages={_format_json(list(messages))}\n"
            f"entities={_format_json(entities_payload)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("extract_relations payload must be object")
        relations = payload.get("entities")
        if not isinstance(relations, list):
            raise ValueError("extract_relations payload.entities must be list")
        output: list[Mem0GraphExtractedRelation] = []
        for item in relations:
            if not isinstance(item, Mapping):
                raise ValueError("relation row must be object")
            output.append(
                Mem0GraphExtractedRelation(
                    source_entity_name=_as_text(item.get("source"), field_name="source"),
                    relationship=_as_text(item.get("relationship"), field_name="relationship"),
                    destination_entity_name=_as_text(
                        item.get("destination"), field_name="destination"
                    ),
                )
            )
        return output

    async def resolve_entity(
        self,
        *,
        entity: Mem0GraphExtractedEntity,
        candidates: Sequence[Mem0GraphEntityCandidate],
        prompt: str,
    ) -> Mem0GraphEntityResolution:
        candidates_payload = [
            {
                "entity_id": candidate.entity_id,
                "entity_name": candidate.entity_name,
                "entity_type": candidate.entity_type,
                "score": candidate.score,
            }
            for candidate in candidates
        ]
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"decision":"SAME|DIFFERENT","entity_name":"...","target_entity_id":"...|null",'
            '"reason":"...","confidence":0.0}\n'
            f"entity={_format_json({'entity_name': entity.entity_name, 'entity_type': entity.entity_type})}\n"
            f"candidates={_format_json(candidates_payload)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("resolve_entity payload must be object")
        return Mem0GraphEntityResolution(
            decision=_as_text(payload.get("decision"), field_name="decision"),
            entity_name=_as_text(payload.get("entity_name"), field_name="entity_name"),
            target_entity_id=_as_str_or_none(payload.get("target_entity_id")),
            reason=str(payload.get("reason", "")),
            confidence=_as_float(payload.get("confidence", 0.0), field_name="confidence"),
        )

    async def resolve_relation(
        self,
        *,
        relation: Mem0GraphExtractedRelation,
        candidates: Sequence[Mem0GraphRelationCandidate],
        prompt: str,
    ) -> Mem0GraphRelationResolution:
        candidates_payload = [
            {
                "relation_id": candidate.relation_id,
                "source_entity_id": candidate.source_entity_id,
                "destination_entity_id": candidate.destination_entity_id,
                "relationship": candidate.relationship,
                "score": candidate.score,
            }
            for candidate in candidates
        ]
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"action":"CONTRADICTS|AUGMENTS|NEW","target_relation_id":"...|null",'
            '"source":"...","destination":"...","relationship":"...","reason":"...",'
            '"confidence":0.0}\n'
            f"relation={_format_json({'source': relation.source_entity_name, 'destination': relation.destination_entity_name, 'relationship': relation.relationship})}\n"
            f"candidates={_format_json(candidates_payload)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("resolve_relation payload must be object")
        return Mem0GraphRelationResolution(
            action=_as_text(payload.get("action"), field_name="action"),
            source_entity_name=_as_text(payload.get("source"), field_name="source"),
            destination_entity_name=_as_text(
                payload.get("destination"),
                field_name="destination",
            ),
            relationship=_as_text(payload.get("relationship"), field_name="relationship"),
            target_relation_id=_as_str_or_none(payload.get("target_relation_id")),
            reason=str(payload.get("reason", "")),
            confidence=_as_float(payload.get("confidence", 0.0), field_name="confidence"),
        )


class ZepLLMSemanticRuntime:
    """LLM-backed Zep semantic runtime."""

    def __init__(self, *, client: LLMClient) -> None:
        self._helper = _JSONLLMHelper(client=client)

    async def extract_entities(
        self,
        *,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEntity]:
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"entities":[{"entity_name":"...","type_id":"..."}]}\n'
            f"message={_format_json(message)}\n"
            f"recent_episodes={_format_json([{'content': e.content} for e in recent_episodes])}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("extract_entities payload must be object")
        entities = payload.get("entities")
        if not isinstance(entities, list):
            raise ValueError("extract_entities payload.entities must be list")
        output: list[ZepExtractedEntity] = []
        for item in entities:
            if not isinstance(item, Mapping):
                raise ValueError("entity row must be object")
            output.append(
                ZepExtractedEntity(
                    entity_name=_as_text(item.get("entity_name"), field_name="entity_name"),
                    type_id=_as_text(item.get("type_id"), field_name="type_id"),
                )
            )
        return output

    async def resolve_entity(
        self,
        *,
        extracted_entity: ZepExtractedEntity,
        candidates: Sequence[ZepEntityCandidate],
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> ZepEntityResolution:
        candidates_payload = [
            {
                "entity_id": candidate.entity_id,
                "entity_name": candidate.entity_name,
                "summary": candidate.summary,
                "score": candidate.score,
            }
            for candidate in candidates
        ]
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"decision":"EXISTING|NEW","entity_name":"...","target_entity_id":"...|null"}\n'
            f"entity={_format_json({'entity_name': extracted_entity.entity_name, 'type_id': extracted_entity.type_id})}\n"
            f"message={_format_json(message)}\n"
            f"recent_episodes={_format_json([{'content': e.content} for e in recent_episodes])}\n"
            f"candidates={_format_json(candidates_payload)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("resolve_entity payload must be object")
        return ZepEntityResolution(
            decision=_as_text(payload.get("decision"), field_name="decision"),
            entity_name=_as_text(payload.get("entity_name"), field_name="entity_name"),
            target_entity_id=_as_str_or_none(payload.get("target_entity_id")),
        )

    async def summarize_entity(
        self,
        *,
        extracted_entity: ZepExtractedEntity,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> str:
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"summary":"..."}\n'
            f"entity={_format_json({'entity_name': extracted_entity.entity_name, 'type_id': extracted_entity.type_id})}\n"
            f"message={_format_json(message)}\n"
            f"recent_episodes={_format_json([{'content': e.content} for e in recent_episodes])}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("summarize_entity payload must be object")
        return _as_text(payload.get("summary"), field_name="summary")

    async def extract_edges(
        self,
        *,
        message: str,
        resolved_entities: Sequence[ZepResolvedEntity],
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEdge]:
        entities_payload = [
            {
                "entity_id": entity.entity_id,
                "entity_name": entity.entity_name,
                "type_id": entity.type_id,
                "summary": entity.summary,
            }
            for entity in resolved_entities
        ]
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"edges":[{"source_entity_name":"...","destination_entity_name":"...","relation":"...","fact":"..."}]}\n'
            f"message={_format_json(message)}\n"
            f"entities={_format_json(entities_payload)}\n"
            f"recent_episodes={_format_json([{'content': e.content} for e in recent_episodes])}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("extract_edges payload must be object")
        edges = payload.get("edges")
        if not isinstance(edges, list):
            raise ValueError("extract_edges payload.edges must be list")
        output: list[ZepExtractedEdge] = []
        for item in edges:
            if not isinstance(item, Mapping):
                raise ValueError("edge row must be object")
            output.append(
                ZepExtractedEdge(
                    source_entity_name=_as_text(
                        item.get("source_entity_name"),
                        field_name="source_entity_name",
                    ),
                    destination_entity_name=_as_text(
                        item.get("destination_entity_name"),
                        field_name="destination_entity_name",
                    ),
                    relation=_as_text(item.get("relation"), field_name="relation"),
                    fact=_as_text(item.get("fact"), field_name="fact"),
                )
            )
        return output

    async def resolve_edge(
        self,
        *,
        extracted_edge: ZepExtractedEdge,
        candidates: Sequence[ZepEdgeCandidate],
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> ZepEdgeResolution:
        candidates_payload = [
            {
                "edge_id": candidate.edge_id,
                "relation": candidate.relation,
                "fact": candidate.fact,
                "score": candidate.score,
            }
            for candidate in candidates
        ]
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"action":"ADD|DUPLICATE|CONTRADICTS","source_entity_name":"...","destination_entity_name":"...",'
            '"relation":"...","fact":"...","target_edge_id":"...|null"}\n'
            f"edge={_format_json({'source_entity_name': extracted_edge.source_entity_name, 'destination_entity_name': extracted_edge.destination_entity_name, 'relation': extracted_edge.relation, 'fact': extracted_edge.fact})}\n"
            f"message={_format_json(message)}\n"
            f"recent_episodes={_format_json([{'content': e.content} for e in recent_episodes])}\n"
            f"candidates={_format_json(candidates_payload)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("resolve_edge payload must be object")
        return ZepEdgeResolution(
            action=_as_text(payload.get("action"), field_name="action"),
            source_entity_name=_as_text(
                payload.get("source_entity_name"),
                field_name="source_entity_name",
            ),
            destination_entity_name=_as_text(
                payload.get("destination_entity_name"),
                field_name="destination_entity_name",
            ),
            relation=_as_text(payload.get("relation"), field_name="relation"),
            fact=_as_text(payload.get("fact"), field_name="fact"),
            target_edge_id=_as_str_or_none(payload.get("target_edge_id")),
        )
