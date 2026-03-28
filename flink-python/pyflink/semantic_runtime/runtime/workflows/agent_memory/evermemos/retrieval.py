"""EverMemOS-style retrieval workflow engine."""

from __future__ import annotations

import asyncio
from typing import Dict, List, Optional, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    RetrievalResult,
    RetrievedMemory,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.interfaces import (
    RetrievalPlanner,
    RetrievalReranker,
    RetrievalSearcher,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.config import (
    EverMemOSRetrievalConfig,
)


class EverMemOSRetrievalWorkflow:
    """Reconstruct EverMemOS retrieval orchestration with strict DI contracts."""

    def __init__(
        self,
        *,
        config: EverMemOSRetrievalConfig,
        keyword_searcher: RetrievalSearcher,
        vector_searcher: RetrievalSearcher,
        reranker: Optional[RetrievalReranker] = None,
        planner: Optional[RetrievalPlanner] = None,
    ) -> None:
        self._config = config
        self._keyword_searcher = keyword_searcher
        self._vector_searcher = vector_searcher
        self._reranker = reranker
        self._planner = planner

    async def retrieve(
        self,
        *,
        group_id: str,
        query: str,
        scene: str,
        mode: str = "hybrid",
        top_k: Optional[int] = None,
        memory_types: Optional[Sequence[str]] = None,
    ) -> RetrievalResult:
        """Retrieve memories for one query request."""
        if not group_id:
            raise ValueError("retrieve requires non-empty group_id")
        if not query:
            raise ValueError("retrieve requires non-empty query")
        if scene not in self._config.supported_scenes:
            raise ValueError(
                f"retrieve received unsupported scene={scene!r}; "
                f"supported={sorted(self._config.supported_scenes)!r}"
            )
        if mode not in self._config.supported_modes:
            raise ValueError(
                f"retrieve received unsupported mode={mode!r}; "
                f"supported={sorted(self._config.supported_modes)!r}"
            )
        resolved_top_k = int(top_k) if top_k is not None else self._config.output_top_k
        if resolved_top_k <= 0:
            raise ValueError("retrieve requires top_k > 0")

        selected_mode = mode
        if mode == "agentic":
            if self._planner is None:
                raise ValueError("retrieve(mode='agentic') requires planner")
            selected_mode = await self._planner.choose_mode(query=query, scene=scene)
            if selected_mode not in {"keyword", "vector", "hybrid", "rrf"}:
                raise ValueError(
                    "planner returned invalid mode; expected one of "
                    "{'keyword', 'vector', 'hybrid', 'rrf'}"
                )

        candidates = await self._retrieve_candidates(
            group_id=group_id,
            query=query,
            selected_mode=selected_mode,
            memory_types=memory_types,
        )
        ranked = self._sort_desc(candidates)
        reranked = False
        output_memories: List[RetrievedMemory]
        if self._config.enable_rerank:
            if self._reranker is None:
                raise ValueError("retrieve requires reranker when enable_rerank=True")
            reranked_candidates = await self._reranker.rerank(
                query=query,
                candidates=ranked,
                top_k=resolved_top_k,
                scene=scene,
            )
            output_memories = list(reranked_candidates)[:resolved_top_k]
            reranked = True
        else:
            output_memories = ranked[:resolved_top_k]

        return RetrievalResult(
            mode=mode,
            selected_mode=selected_mode,
            memories=output_memories,
            candidate_count=len(candidates),
            reranked=reranked,
        )

    async def _retrieve_candidates(
        self,
        *,
        group_id: str,
        query: str,
        selected_mode: str,
        memory_types: Optional[Sequence[str]],
    ) -> List[RetrievedMemory]:
        if selected_mode == "keyword":
            return list(
                await self._keyword_searcher.search(
                    group_id=group_id,
                    query=query,
                    top_k=self._config.keyword_recall_top_k,
                    memory_types=memory_types,
                )
            )
        if selected_mode == "vector":
            return list(
                await self._vector_searcher.search(
                    group_id=group_id,
                    query=query,
                    top_k=self._config.vector_recall_top_k,
                    memory_types=memory_types,
                )
            )
        if selected_mode in {"hybrid", "rrf"}:
            keyword_candidates, vector_candidates = await asyncio.gather(
                self._keyword_searcher.search(
                    group_id=group_id,
                    query=query,
                    top_k=self._config.keyword_recall_top_k,
                    memory_types=memory_types,
                ),
                self._vector_searcher.search(
                    group_id=group_id,
                    query=query,
                    top_k=self._config.vector_recall_top_k,
                    memory_types=memory_types,
                ),
            )
            if selected_mode == "hybrid":
                return self._hybrid_fuse(keyword_candidates, vector_candidates)
            return self._rrf_fuse(keyword_candidates, vector_candidates)
        raise RuntimeError(f"unsupported selected_mode={selected_mode!r}")

    def _hybrid_fuse(
        self,
        keyword_candidates: Sequence[RetrievedMemory],
        vector_candidates: Sequence[RetrievedMemory],
    ) -> List[RetrievedMemory]:
        keyword_rank_score = self._reciprocal_rank_score(keyword_candidates)
        vector_rank_score = self._reciprocal_rank_score(vector_candidates)
        by_id: Dict[str, RetrievedMemory] = {}

        for candidate in keyword_candidates:
            by_id[candidate.memory_id] = candidate
        for candidate in vector_candidates:
            if candidate.memory_id not in by_id:
                by_id[candidate.memory_id] = candidate

        keyword_weight = self._config.hybrid_keyword_weight
        vector_weight = self._config.hybrid_vector_weight
        fused: List[RetrievedMemory] = []
        for memory_id, candidate in by_id.items():
            score = (
                keyword_weight * keyword_rank_score.get(memory_id, 0.0)
                + vector_weight * vector_rank_score.get(memory_id, 0.0)
            )
            fused.append(
                RetrievedMemory(
                    memory_id=candidate.memory_id,
                    memory_type=candidate.memory_type,
                    content=candidate.content,
                    score=float(score),
                    source="hybrid",
                    timestamp_ms=candidate.timestamp_ms,
                    metadata=candidate.metadata,
                )
            )
        return fused

    def _rrf_fuse(
        self,
        keyword_candidates: Sequence[RetrievedMemory],
        vector_candidates: Sequence[RetrievedMemory],
    ) -> List[RetrievedMemory]:
        keyword_rank = self._rank_by_id(keyword_candidates)
        vector_rank = self._rank_by_id(vector_candidates)
        by_id: Dict[str, RetrievedMemory] = {}
        for candidate in keyword_candidates:
            by_id[candidate.memory_id] = candidate
        for candidate in vector_candidates:
            if candidate.memory_id not in by_id:
                by_id[candidate.memory_id] = candidate

        fused: List[RetrievedMemory] = []
        for memory_id, candidate in by_id.items():
            score = 0.0
            if memory_id in keyword_rank:
                score += 1.0 / (self._config.rrf_k + keyword_rank[memory_id])
            if memory_id in vector_rank:
                score += 1.0 / (self._config.rrf_k + vector_rank[memory_id])
            fused.append(
                RetrievedMemory(
                    memory_id=candidate.memory_id,
                    memory_type=candidate.memory_type,
                    content=candidate.content,
                    score=float(score),
                    source="rrf",
                    timestamp_ms=candidate.timestamp_ms,
                    metadata=candidate.metadata,
                )
            )
        return fused

    def _rank_by_id(self, candidates: Sequence[RetrievedMemory]) -> Dict[str, int]:
        return {candidate.memory_id: rank for rank, candidate in enumerate(candidates, 1)}

    def _reciprocal_rank_score(
        self,
        candidates: Sequence[RetrievedMemory],
    ) -> Dict[str, float]:
        return {
            candidate.memory_id: float(1.0 / rank)
            for rank, candidate in enumerate(candidates, 1)
        }

    def _sort_desc(self, candidates: Sequence[RetrievedMemory]) -> List[RetrievedMemory]:
        return sorted(candidates, key=lambda candidate: float(candidate.score), reverse=True)
