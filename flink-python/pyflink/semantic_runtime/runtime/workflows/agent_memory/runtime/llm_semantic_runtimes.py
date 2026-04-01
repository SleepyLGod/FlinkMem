#!/usr/bin/env python3
"""LLM-backed semantic runtime adapters for Mem0 and Zep workflows."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping, Sequence

from pyflink.semantic_runtime.llm_client import LLMClient
from pyflink.semantic_runtime.runtime.json_output import parse_llm_json_object
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


def _as_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be integer")
    if isinstance(value, int):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name} must be non-empty integer string")
        try:
            return int(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be integer") from exc
    raise TypeError(f"{field_name} must be integer")


class _JSONLLMHelper:
    """Shared strict JSON-calling helper."""

    def __init__(self, *, client: LLMClient) -> None:
        self._client = client

    async def call_json(self, *, prompt: str) -> Any:
        response_text, _ = await self._client.call(prompt)
        return parse_llm_json_object(
            response_text,
            operator_name="agent_memory_llm_runtime",
        )


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

    async def resolve_facts(
        self,
        *,
        facts: Sequence[str],
        candidates_by_fact: Sequence[Sequence[RetrievedMemory]],
        prompt: str,
    ) -> Sequence[Mem0FactResolution]:
        if len(facts) != len(candidates_by_fact):
            raise ValueError(
                "resolve_facts requires facts and candidates_by_fact with equal lengths"
            )
        fact_rows = []
        for index, (fact, candidates) in enumerate(
            zip(facts, candidates_by_fact, strict=True)
        ):
            fact_rows.append(
                {
                    "fact_index": index,
                    "fact": fact,
                    "candidates": [
                        {
                            "memory_id": candidate.memory_id,
                            "content": candidate.content,
                            "memory_type": candidate.memory_type,
                            "score": candidate.score,
                        }
                        for candidate in candidates
                    ],
                }
            )
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"decisions":[{"fact_index":0,"action":"ADD|UPDATE|DELETE|NONE",'
            '"target_memory_id":"...|null","content":"...|null","reason":"...",'
            '"confidence":0.0}]}\n'
            f"facts={_format_json(fact_rows)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("resolve_facts payload must be object")
        decisions = payload.get("decisions")
        if not isinstance(decisions, list):
            raise ValueError("resolve_facts payload.decisions must be list")

        resolutions: list[Mem0FactResolution | None] = [None] * len(facts)
        seen_indexes: set[int] = set()
        for item in decisions:
            if not isinstance(item, Mapping):
                raise ValueError("resolve_facts decision row must be object")
            fact_index = _as_int(item.get("fact_index"), field_name="fact_index")
            if fact_index < 0 or fact_index >= len(facts):
                raise ValueError(
                    f"resolve_facts returned out-of-range fact_index={fact_index}"
                )
            if fact_index in seen_indexes:
                raise ValueError(
                    f"resolve_facts returned duplicate fact_index={fact_index}"
                )
            seen_indexes.add(fact_index)
            expected_fact = _as_text(facts[fact_index], field_name="facts[]")
            resolutions[fact_index] = Mem0FactResolution(
                action=_as_text(item.get("action"), field_name="action"),
                fact=expected_fact,
                target_memory_id=_as_str_or_none(item.get("target_memory_id")),
                content=_as_str_or_none(item.get("content")),
                reason=str(item.get("reason", "")),
                confidence=_as_float(item.get("confidence", 0.0), field_name="confidence"),
            )
        if len(seen_indexes) != len(facts):
            raise ValueError(
                "resolve_facts must return exactly one decision per input fact"
            )
        output: list[Mem0FactResolution] = []
        for index, resolution in enumerate(resolutions):
            if resolution is None:
                raise RuntimeError(
                    f"resolve_facts produced no resolution for fact_index={index}"
                )
            output.append(resolution)
        return output


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
        allowed_entity_names: Sequence[str],
        prompt: str,
    ) -> Sequence[Mem0GraphExtractedRelation]:
        entities_payload = [
            {"entity": entity.entity_name, "entity_type": entity.entity_type}
            for entity in entities
        ]
        allowed_names = [
            _as_text(name, field_name="allowed_entity_names[]")
            for name in allowed_entity_names
        ]
        if not allowed_names:
            raise ValueError("extract_relations requires non-empty allowed_entity_names")
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"entities":[{"source_index":0,"relationship":"...","destination_index":1}]}\n'
            "source_index and destination_index MUST be valid indexes into allowed_entity_names.\n"
            f"messages={_format_json(list(messages))}\n"
            f"entities={_format_json(entities_payload)}\n"
            f"allowed_entity_names={_format_json(allowed_names)}"
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
            source_index = _as_int(item.get("source_index"), field_name="source_index")
            destination_index = _as_int(
                item.get("destination_index"),
                field_name="destination_index",
            )
            if source_index < 0 or source_index >= len(allowed_names):
                raise ValueError(
                    "extract_relations returned source_index out of range: "
                    f"{source_index}"
                )
            if destination_index < 0 or destination_index >= len(allowed_names):
                raise ValueError(
                    "extract_relations returned destination_index out of range: "
                    f"{destination_index}"
                )
            source_canonical = allowed_names[source_index]
            destination_canonical = allowed_names[destination_index]
            output.append(
                Mem0GraphExtractedRelation(
                    source_entity_name=source_canonical,
                    relationship=_as_text(item.get("relationship"), field_name="relationship"),
                    destination_entity_name=destination_canonical,
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

    def __init__(
        self,
        *,
        client: LLMClient,
        summary_client: LLMClient | None = None,
        max_message_chars: int | None = None,
        max_recent_episodes: int | None = None,
        max_recent_episode_chars: int | None = None,
        max_edge_entities: int | None = None,
    ) -> None:
        if max_message_chars is not None and int(max_message_chars) <= 0:
            raise ValueError("max_message_chars must be > 0 when provided")
        if max_recent_episodes is not None and int(max_recent_episodes) <= 0:
            raise ValueError("max_recent_episodes must be > 0 when provided")
        if max_recent_episode_chars is not None and int(max_recent_episode_chars) <= 0:
            raise ValueError("max_recent_episode_chars must be > 0 when provided")
        if max_edge_entities is not None and int(max_edge_entities) <= 0:
            raise ValueError("max_edge_entities must be > 0 when provided")
        self._helper = _JSONLLMHelper(client=client)
        self._summary_helper = (
            _JSONLLMHelper(client=summary_client)
            if summary_client is not None
            else self._helper
        )
        self._max_message_chars = (
            int(max_message_chars) if max_message_chars is not None else None
        )
        self._max_recent_episodes = (
            int(max_recent_episodes) if max_recent_episodes is not None else None
        )
        self._max_recent_episode_chars = (
            int(max_recent_episode_chars)
            if max_recent_episode_chars is not None
            else None
        )
        self._max_edge_entities = (
            int(max_edge_entities) if max_edge_entities is not None else None
        )

    def _bounded_text(self, text: str, *, max_chars: int | None) -> str:
        normalized = _as_text(text, field_name="text")
        if max_chars is None or len(normalized) <= max_chars:
            return normalized
        return normalized[:max_chars]

    def _recent_episodes_payload(
        self,
        recent_episodes: Sequence[ZepEpisodeCandidate],
    ) -> Sequence[Mapping[str, str]]:
        episodes = list(recent_episodes)
        if self._max_recent_episodes is not None:
            episodes = episodes[: self._max_recent_episodes]
        payload: list[Mapping[str, str]] = []
        for episode in episodes:
            payload.append(
                {
                    "content": self._bounded_text(
                        episode.content,
                        max_chars=self._max_recent_episode_chars,
                    )
                }
            )
        return payload

    async def extract_entities(
        self,
        *,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEntity]:
        bounded_message = self._bounded_text(
            message,
            max_chars=self._max_message_chars,
        )
        recent_payload = self._recent_episodes_payload(recent_episodes)
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"entities":[{"entity_name":"...","type_id":"..."}]}\n'
            f"message={_format_json(bounded_message)}\n"
            f"recent_episodes={_format_json(recent_payload)}"
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
        bounded_message = self._bounded_text(
            message,
            max_chars=self._max_message_chars,
        )
        recent_payload = self._recent_episodes_payload(recent_episodes)
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
            f"message={_format_json(bounded_message)}\n"
            f"recent_episodes={_format_json(recent_payload)}\n"
            f"candidates={_format_json(candidates_payload)}"
        )
        payload = await self._helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("resolve_entity payload must be object")
        decision = _as_text(payload.get("decision"), field_name="decision")
        entity_name = _as_text(payload.get("entity_name"), field_name="entity_name")
        target_entity_id = _as_str_or_none(payload.get("target_entity_id"))
        if decision == "EXISTING":
            if target_entity_id is None:
                raise ValueError(
                    "resolve_entity decision=EXISTING requires non-empty target_entity_id"
                )
        elif decision == "NEW":
            if target_entity_id is not None:
                raise ValueError(
                    "resolve_entity decision=NEW requires null target_entity_id"
                )
        else:
            raise ValueError(
                "resolve_entity decision must be one of {'EXISTING','NEW'}"
            )
        return ZepEntityResolution(
            decision=decision,
            entity_name=entity_name,
            target_entity_id=target_entity_id,
        )

    async def summarize_entity(
        self,
        *,
        extracted_entity: ZepExtractedEntity,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> str:
        bounded_message = self._bounded_text(
            message,
            max_chars=self._max_message_chars,
        )
        recent_payload = self._recent_episodes_payload(recent_episodes)
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"summary":"..."}\n'
            f"entity={_format_json({'entity_name': extracted_entity.entity_name, 'type_id': extracted_entity.type_id})}\n"
            f"message={_format_json(bounded_message)}\n"
            f"recent_episodes={_format_json(recent_payload)}"
        )
        payload = await self._summary_helper.call_json(prompt=request_prompt)
        if not isinstance(payload, Mapping):
            raise ValueError("summarize_entity payload must be object")
        return _as_text(payload.get("summary"), field_name="summary")

    async def extract_edges(
        self,
        *,
        message: str,
        resolved_entities: Sequence[ZepResolvedEntity],
        allowed_entity_names: Sequence[str],
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEdge]:
        bounded_message = self._bounded_text(
            message,
            max_chars=self._max_message_chars,
        )
        recent_payload = self._recent_episodes_payload(recent_episodes)
        entities = list(resolved_entities)
        if self._max_edge_entities is not None:
            entities = entities[: self._max_edge_entities]
        entities_payload = [
            {
                "entity_name": entity.entity_name,
                "type_id": entity.type_id,
            }
            for entity in entities
        ]
        allowed_names = [
            _as_text(name, field_name="allowed_entity_names[]")
            for name in allowed_entity_names
        ]
        allowed_name_set = set(allowed_names)
        if not allowed_name_set:
            raise ValueError("extract_edges requires non-empty allowed_entity_names")
        request_prompt = (
            f"{prompt}\n"
            "Return ONLY JSON schema:\n"
            '{"edges":[{"source_entity_name":"...","destination_entity_name":"...","relation":"...","fact":"..."}]}\n'
            "source_entity_name and destination_entity_name MUST be exact members of allowed_entity_names.\n"
            f"message={_format_json(bounded_message)}\n"
            f"entities={_format_json(entities_payload)}\n"
            f"allowed_entity_names={_format_json(allowed_names)}\n"
            f"recent_episodes={_format_json(recent_payload)}"
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
            source_entity_name = _as_text(
                item.get("source_entity_name"),
                field_name="source_entity_name",
            )
            destination_entity_name = _as_text(
                item.get("destination_entity_name"),
                field_name="destination_entity_name",
            )
            if source_entity_name not in allowed_name_set:
                raise ValueError(
                    "extract_edges produced source_entity_name not in allowed_entity_names: "
                    f"{source_entity_name!r}"
                )
            if destination_entity_name not in allowed_name_set:
                raise ValueError(
                    "extract_edges produced destination_entity_name not in allowed_entity_names: "
                    f"{destination_entity_name!r}"
                )
            output.append(
                ZepExtractedEdge(
                    source_entity_name=source_entity_name,
                    destination_entity_name=destination_entity_name,
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
        bounded_message = self._bounded_text(
            message,
            max_chars=self._max_message_chars,
        )
        recent_payload = self._recent_episodes_payload(recent_episodes)
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
            f"message={_format_json(bounded_message)}\n"
            f"recent_episodes={_format_json(recent_payload)}\n"
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
