"""Mem0 Basic workflow implementation."""

from __future__ import annotations

from typing import List, Optional, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    RetrievedMemory,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.config import (
    Mem0BasicConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0BasicAddResult,
    Mem0BasicOperation,
    Mem0BasicSearchResult,
    Mem0FactResolution,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.interfaces import (
    Mem0BasicSemanticRuntime,
    Mem0FactSearcher,
    Mem0FactStore,
    Mem0LLMFactSearcher,
)


class Mem0BasicWorkflow:
    """Reconstruct Mem0 Basic flow with strict dependency injection."""

    def __init__(
        self,
        *,
        config: Mem0BasicConfig,
        semantic_runtime: Mem0BasicSemanticRuntime,
        fact_store: Mem0FactStore,
        fact_searcher: Mem0FactSearcher,
        llm_fact_searcher: Optional[Mem0LLMFactSearcher] = None,
    ) -> None:
        self._config = config
        self._semantic_runtime = semantic_runtime
        self._fact_store = fact_store
        self._fact_searcher = fact_searcher
        self._llm_fact_searcher = llm_fact_searcher

    async def add(
        self,
        *,
        group_id: str,
        messages: Sequence[str],
    ) -> Mem0BasicAddResult:
        """Process one Mem0 Basic ``add(messages)`` request."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not messages:
            raise ValueError("messages must be non-empty")
        for message in messages:
            if not str(message).strip():
                raise ValueError("messages must contain non-empty strings")

        facts = list(
            await self._semantic_runtime.extract_facts(
                messages=list(messages),
                prompt=self._config.fact_extraction_prompt,
            )
        )
        operations: List[Mem0BasicOperation] = []
        added = 0
        updated = 0
        deleted = 0
        noop = 0

        for fact in facts:
            candidates = await self._search_candidates(
                group_id=group_id,
                query=fact,
                top_k=self._config.similar_top_k,
                memory_types=sorted(self._config.memory_types),
            )
            resolution = await self._semantic_runtime.resolve_fact(
                fact=fact,
                candidates=candidates,
                prompt=self._config.fact_resolution_prompt,
            )
            operation = await self._apply_resolution(
                group_id=group_id,
                fact=fact,
                resolution=resolution,
            )
            operations.append(operation)
            if operation.action == "ADD":
                added += 1
            elif operation.action == "UPDATE":
                updated += 1
            elif operation.action == "DELETE":
                deleted += 1
            elif operation.action == "NONE":
                noop += 1
            else:
                raise RuntimeError(f"unsupported operation action={operation.action!r}")

        return Mem0BasicAddResult(
            extracted_fact_count=len(facts),
            added=added,
            updated=updated,
            deleted=deleted,
            noop=noop,
            operations=operations,
        )

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: Optional[int] = None,
    ) -> Mem0BasicSearchResult:
        """Process one Mem0 Basic ``search(query)`` request."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not query:
            raise ValueError("query must be non-empty")
        resolved_top_k = int(top_k) if top_k is not None else self._config.search_top_k
        if resolved_top_k <= 0:
            raise ValueError("top_k must be > 0")
        memories = await self._search_candidates(
            group_id=group_id,
            query=query,
            top_k=resolved_top_k,
            memory_types=sorted(self._config.memory_types),
        )
        return Mem0BasicSearchResult(
            query=query,
            top_k=resolved_top_k,
            memories=list(memories),
            metadata={"workflow": "mem0_basic"},
        )

    async def _search_candidates(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Sequence[str],
    ) -> Sequence[RetrievedMemory]:
        if self._config.recall_backend == "embedding":
            return await self._fact_searcher.search(
                group_id=group_id,
                query=query,
                top_k=top_k,
                memory_types=memory_types,
            )
        if self._llm_fact_searcher is None:
            raise ValueError(
                "llm_fact_searcher is required when recall_backend='llm'"
            )
        return await self._llm_fact_searcher.search(
            group_id=group_id,
            query=query,
            top_k=top_k,
            memory_types=memory_types,
        )

    async def _apply_resolution(
        self,
        *,
        group_id: str,
        fact: str,
        resolution: Mem0FactResolution,
    ) -> Mem0BasicOperation:
        action = str(resolution.action)
        if action == "ADD":
            memory_id = await self._fact_store.add_fact(
                group_id=group_id,
                content=str(resolution.content),
            )
            return Mem0BasicOperation(
                action=action,
                fact=fact,
                memory_id=memory_id,
                content=str(resolution.content),
                reason=str(resolution.reason),
                confidence=float(resolution.confidence),
            )
        if action == "UPDATE":
            memory_id = str(resolution.target_memory_id)
            await self._fact_store.update_fact(
                group_id=group_id,
                memory_id=memory_id,
                content=str(resolution.content),
            )
            return Mem0BasicOperation(
                action=action,
                fact=fact,
                memory_id=memory_id,
                content=str(resolution.content),
                reason=str(resolution.reason),
                confidence=float(resolution.confidence),
            )
        if action == "DELETE":
            memory_id = str(resolution.target_memory_id)
            await self._fact_store.delete_fact(
                group_id=group_id,
                memory_id=memory_id,
            )
            return Mem0BasicOperation(
                action=action,
                fact=fact,
                memory_id=memory_id,
                content=None,
                reason=str(resolution.reason),
                confidence=float(resolution.confidence),
            )
        if action == "NONE":
            return Mem0BasicOperation(
                action=action,
                fact=fact,
                memory_id=None,
                content=None,
                reason=str(resolution.reason),
                confidence=float(resolution.confidence),
            )
        raise ValueError(f"unsupported resolution action={action!r}")
