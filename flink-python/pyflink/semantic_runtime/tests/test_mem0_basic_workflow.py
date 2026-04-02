"""Unit tests for Mem0 Basic workflow."""

from __future__ import annotations

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import asyncio
import os  # noqa: E401,E402
import pathlib  # noqa: E401,E402

import pyflink as _pf  # noqa: E401,E402

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

from typing import Dict, List, Optional, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    RetrievedMemory,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.entity_reference_mode import (
    DRIFT_POLICY_UPSTREAM_COMPATIBLE,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.basic import (
    Mem0BasicWorkflow,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.config import (
    Mem0BasicConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0FactResolution,
)


class _InMemoryFactStore:
    def __init__(self) -> None:
        self._rows: Dict[str, Dict[str, str]] = {}
        self._counter = 0

    async def add_fact(self, *, group_id: str, content: str) -> str:
        self._counter += 1
        memory_id = f"m{self._counter}"
        self._rows[memory_id] = {"group_id": group_id, "content": content}
        return memory_id

    async def update_fact(self, *, group_id: str, memory_id: str, content: str) -> None:
        if memory_id not in self._rows:
            raise KeyError(f"unknown memory_id={memory_id}")
        if self._rows[memory_id]["group_id"] != group_id:
            raise ValueError("group_id mismatch during update_fact")
        self._rows[memory_id]["content"] = content

    async def delete_fact(self, *, group_id: str, memory_id: str) -> None:
        if memory_id not in self._rows:
            raise KeyError(f"unknown memory_id={memory_id}")
        if self._rows[memory_id]["group_id"] != group_id:
            raise ValueError("group_id mismatch during delete_fact")
        del self._rows[memory_id]


class _ScriptedSearcher:
    def __init__(self, rows: Dict[str, Sequence[RetrievedMemory]]) -> None:
        self._rows = rows
        self.calls = 0

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        _ = (group_id, top_k, memory_types)
        self.calls += 1
        return list(self._rows.get(query, []))


class _ScriptedSemanticRuntime:
    def __init__(
        self,
        *,
        facts: Sequence[str],
        resolutions: Dict[str, Mem0FactResolution],
    ) -> None:
        self._facts = list(facts)
        self._resolutions = dict(resolutions)
        self.extract_calls = 0
        self.resolve_calls = 0
        self.extract_prompts: List[str] = []
        self.resolve_prompts: List[str] = []

    async def extract_facts(self, *, messages: Sequence[str], prompt: str) -> Sequence[str]:
        _ = messages
        self.extract_calls += 1
        self.extract_prompts.append(prompt)
        return list(self._facts)

    async def resolve_facts(
        self,
        *,
        facts: Sequence[str],
        candidates_by_fact: Sequence[Sequence[RetrievedMemory]],
        prompt: str,
    ) -> Sequence[Mem0FactResolution]:
        _ = candidates_by_fact
        self.resolve_calls += 1
        self.resolve_prompts.append(prompt)
        output: list[Mem0FactResolution] = []
        for fact in facts:
            if fact not in self._resolutions:
                raise KeyError(f"no scripted resolution for fact={fact!r}")
            output.append(self._resolutions[fact])
        return output


def test_mem0_basic_add_executes_all_actions() -> None:
    store = _InMemoryFactStore()
    asyncio.run(store.add_fact(group_id="g1", content="old fact"))
    searcher = _ScriptedSearcher(
        {
            "f_update": [
                RetrievedMemory(
                    memory_id="m1",
                    memory_type="fact",
                    content="old fact",
                    score=0.9,
                    source="vector",
                )
            ],
            "f_delete": [
                RetrievedMemory(
                    memory_id="m1",
                    memory_type="fact",
                    content="old fact",
                    score=0.8,
                    source="vector",
                )
            ],
        }
    )
    runtime = _ScriptedSemanticRuntime(
        facts=["f_add", "f_update", "f_none", "f_delete"],
        resolutions={
            "f_add": Mem0FactResolution(
                action="ADD",
                fact="f_add",
                content="new fact",
                reason="new",
                confidence=0.9,
            ),
            "f_update": Mem0FactResolution(
                action="UPDATE",
                fact="f_update",
                target_memory_id="m1",
                content="updated fact",
                reason="augment",
                confidence=0.8,
            ),
            "f_none": Mem0FactResolution(
                action="NONE",
                fact="f_none",
                reason="redundant",
                confidence=0.7,
            ),
            "f_delete": Mem0FactResolution(
                action="DELETE",
                fact="f_delete",
                target_memory_id="m1",
                reason="contradiction",
                confidence=0.6,
            ),
        },
    )
    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(similar_top_k=5),
        semantic_runtime=runtime,
        fact_store=store,
        fact_searcher=searcher,
    )
    result = asyncio.run(
        workflow.add(
            group_id="g1",
            messages=["message a", "message b"],
        )
    )

    assert result.extracted_fact_count == 4
    assert result.added == 1
    assert result.updated == 1
    assert result.deleted == 1
    assert result.noop == 1
    assert runtime.extract_calls == 1
    assert runtime.resolve_calls == 1
    assert runtime.extract_prompts == [Mem0BasicConfig().fact_extraction_prompt]
    assert runtime.resolve_prompts == [Mem0BasicConfig().fact_resolution_prompt]
    assert searcher.calls == 4


def test_mem0_basic_search_uses_searcher() -> None:
    row = RetrievedMemory(
        memory_id="m1",
        memory_type="fact",
        content="plan trip",
        score=0.9,
        source="vector",
    )
    searcher = _ScriptedSearcher({"travel": [row]})
    runtime = _ScriptedSemanticRuntime(facts=[], resolutions={})
    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(search_top_k=3),
        semantic_runtime=runtime,
        fact_store=_InMemoryFactStore(),
        fact_searcher=searcher,
    )
    result = asyncio.run(
        workflow.search(
            group_id="g1",
            query="travel",
            top_k=1,
        )
    )
    assert result.top_k == 1
    assert len(result.memories) == 1
    assert result.memories[0].memory_id == "m1"
    assert searcher.calls == 1


def test_mem0_basic_add_no_facts_short_circuits() -> None:
    runtime = _ScriptedSemanticRuntime(facts=[], resolutions={})
    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(),
        semantic_runtime=runtime,
        fact_store=_InMemoryFactStore(),
        fact_searcher=_ScriptedSearcher({}),
    )
    result = asyncio.run(
        workflow.add(
            group_id="g1",
            messages=["message"],
        )
    )
    assert result.extracted_fact_count == 0
    assert result.added == 0
    assert result.updated == 0
    assert result.deleted == 0
    assert result.noop == 0
    assert len(result.operations) == 0
    assert runtime.resolve_calls == 0


def test_mem0_basic_add_rejects_fact_mismatch_from_resolution() -> None:
    class _MismatchedRuntime(_ScriptedSemanticRuntime):
        async def resolve_facts(
            self,
            *,
            facts: Sequence[str],
            candidates_by_fact: Sequence[Sequence[RetrievedMemory]],
            prompt: str,
        ) -> Sequence[Mem0FactResolution]:
            _ = (facts, candidates_by_fact, prompt)
            return [
                Mem0FactResolution(
                    action="NONE",
                    fact="unexpected",
                    reason="bad",
                    confidence=0.1,
                )
            ]

    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(),
        semantic_runtime=_MismatchedRuntime(
            facts=["expected"],
            resolutions={
                "expected": Mem0FactResolution(
                    action="NONE",
                    fact="expected",
                    reason="ok",
                    confidence=0.9,
                )
            },
        ),
        fact_store=_InMemoryFactStore(),
        fact_searcher=_ScriptedSearcher({}),
    )
    try:
        asyncio.run(
            workflow.add(
                group_id="g1",
                messages=["message"],
            )
        )
        raise AssertionError("Expected ValueError for fact mismatch")
    except ValueError as exc:
        assert "fact mismatch" in str(exc)


def test_mem0_basic_llm_recall_backend_uses_llm_searcher() -> None:
    row = RetrievedMemory(
        memory_id="m1",
        memory_type="fact",
        content="llm recalled memory",
        score=0.99,
        source="llm",
    )
    embedding_searcher = _ScriptedSearcher({"travel": []})
    llm_searcher = _ScriptedSearcher({"travel": [row]})
    runtime = _ScriptedSemanticRuntime(
        facts=["travel"],
        resolutions={
            "travel": Mem0FactResolution(
                action="NONE",
                fact="travel",
                reason="no-op",
                confidence=0.8,
            )
        },
    )
    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(recall_backend="llm"),
        semantic_runtime=runtime,
        fact_store=_InMemoryFactStore(),
        fact_searcher=embedding_searcher,
        llm_fact_searcher=llm_searcher,
    )

    add_result = asyncio.run(
        workflow.add(
            group_id="g1",
            messages=["user asks about travel plan"],
        )
    )
    search_result = asyncio.run(
        workflow.search(
            group_id="g1",
            query="travel",
            top_k=1,
        )
    )

    assert add_result.noop == 1
    assert llm_searcher.calls == 2
    assert embedding_searcher.calls == 0
    assert len(search_result.memories) == 1
    assert search_result.memories[0].source == "llm"


def test_mem0_basic_llm_recall_backend_requires_llm_searcher() -> None:
    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(recall_backend="llm"),
        semantic_runtime=_ScriptedSemanticRuntime(
            facts=["f1"],
            resolutions={
                "f1": Mem0FactResolution(
                    action="NONE",
                    fact="f1",
                    reason="redundant",
                    confidence=0.9,
                )
            },
        ),
        fact_store=_InMemoryFactStore(),
        fact_searcher=_ScriptedSearcher({}),
    )
    try:
        asyncio.run(
            workflow.add(
                group_id="g1",
                messages=["hello"],
            )
        )
    except ValueError as exc:
        assert "llm_fact_searcher is required" in str(exc)
        return
    raise AssertionError("llm recall backend should require llm_fact_searcher")


def test_mem0_basic_upstream_mode_tolerates_short_resolution_list() -> None:
    class _ShortResolutionRuntime(_ScriptedSemanticRuntime):
        def __init__(self) -> None:
            super().__init__(
                facts=["f1", "f2"],
                resolutions={
                    "f1": Mem0FactResolution(
                        action="NONE",
                        fact="f1",
                        reason="ok",
                        confidence=1.0,
                    )
                },
            )
            self.drift_policy = DRIFT_POLICY_UPSTREAM_COMPATIBLE

        async def resolve_facts(
            self,
            *,
            facts: Sequence[str],
            candidates_by_fact: Sequence[Sequence[RetrievedMemory]],
            prompt: str,
        ) -> Sequence[Mem0FactResolution]:
            _ = (facts, candidates_by_fact, prompt)
            return [
                Mem0FactResolution(
                    action="NONE",
                    fact="f1",
                    reason="only one",
                    confidence=1.0,
                )
            ]

    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(),
        semantic_runtime=_ShortResolutionRuntime(),
        fact_store=_InMemoryFactStore(),
        fact_searcher=_ScriptedSearcher({}),
    )
    result = asyncio.run(
        workflow.add(
            group_id="g1",
            messages=["message"],
        )
    )
    assert result.extracted_fact_count == 2
    assert result.noop == 2


def test_mem0_basic_upstream_mode_tolerates_fact_mismatch() -> None:
    class _MismatchedRuntime(_ScriptedSemanticRuntime):
        def __init__(self) -> None:
            super().__init__(
                facts=["expected"],
                resolutions={
                    "expected": Mem0FactResolution(
                        action="NONE",
                        fact="expected",
                        reason="ok",
                        confidence=1.0,
                    )
                },
            )
            self.drift_policy = DRIFT_POLICY_UPSTREAM_COMPATIBLE

        async def resolve_facts(
            self,
            *,
            facts: Sequence[str],
            candidates_by_fact: Sequence[Sequence[RetrievedMemory]],
            prompt: str,
        ) -> Sequence[Mem0FactResolution]:
            _ = (facts, candidates_by_fact, prompt)
            return [
                Mem0FactResolution(
                    action="NONE",
                    fact="other",
                    reason="mismatch",
                    confidence=0.2,
                )
            ]

    workflow = Mem0BasicWorkflow(
        config=Mem0BasicConfig(),
        semantic_runtime=_MismatchedRuntime(),
        fact_store=_InMemoryFactStore(),
        fact_searcher=_ScriptedSearcher({}),
    )
    result = asyncio.run(
        workflow.add(
            group_id="g1",
            messages=["message"],
        )
    )
    assert result.extracted_fact_count == 1
    assert result.noop == 1
