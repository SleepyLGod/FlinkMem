#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""
V0.2 workflow-aligned use-case tests (LOTUS-inspired, stream-first, API-optional).

Run:
  pytest -q flink-python/pyflink/semantic_runtime/tests/test_v02_evermemos_workflow.py
  python flink-python/pyflink/semantic_runtime/tests/test_v02_evermemos_workflow.py --mode real
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Bootstrap: ensure pyflink.semantic_runtime import path is visible in tests.
import pyflink as _pf

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)

from pyflink.datastream.functions import AsyncFunction

from pyflink.semantic_runtime.llm_client import LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.stateful.async_bridge import AsyncResult, AsyncWorkItem
from pyflink.semantic_runtime.stateful.continuous_rag_workflow import (
    _AnswerSynthesiser,
    _ClassifyAsyncMergeFunction,
    _GroupbyToAggEnvelope,
    _MissingAsyncFallbackMapper,
    _RetrievalEnvelopeExpander,
    _RetrievalToAnswerEnvelope,
    _RetrieveAsyncMergeFunction,
    _SummarizeAsyncMergeFunction,
)
from pyflink.semantic_runtime.stateful.cts_retrieve import CtsRetrieveConfig, CtsRetrieveFunction
from pyflink.semantic_runtime.stateful.sem_agg_stateful import SemAggConfig, SemAggFunction
from pyflink.semantic_runtime.stateful.sem_groupby_stateful import SemGroupbyConfig, SemGroupbyFunction
from pyflink.semantic_runtime.stateful.sem_topk_continuous import SemTopKConfig, SemTopKFunction
from pyflink.semantic_runtime.stateful.external_search_backend import (
    MockSearchBackend,
    SearchBackendAsyncFn,
)
from pyflink.semantic_runtime.semantic_spec import TopKQuerySpec
from pyflink.semantic_runtime.stateful.semantic_window import SemWindowConfig, SemWindowFunction


# ---------------------------------------------------------------------------
# Minimal fake runtime helpers (no Java gateway / no Flink cluster required)
# ---------------------------------------------------------------------------


class _FakeTimerService:
    def register_processing_time_timer(self, timestamp: int) -> None:
        pass

    def register_event_time_timer(self, timestamp: int) -> None:
        pass


class _FakeContext:
    def __init__(self, key: str):
        self._key = key
        self._timer = _FakeTimerService()

    def get_current_key(self):
        return self._key

    def timer_service(self):
        return self._timer

    def output(self, tag, value):
        pass


class _FakeMapState:
    def __init__(self, initial: Optional[Dict[str, Any]] = None):
        self._data = dict(initial or {})

    def get(self, key):
        return self._data.get(key)

    def put(self, key, value):
        self._data[key] = value

    def remove(self, key):
        if key in self._data:
            del self._data[key]

    def contains(self, key):
        return key in self._data

    def keys(self):
        return list(self._data.keys())


class _FakeListState:
    def __init__(self, initial: Optional[List[Any]] = None):
        self._data = list(initial or [])

    def add(self, value):
        self._data.append(value)

    def get(self):
        return iter(self._data)

    def clear(self):
        self._data.clear()


class _FakeValueState:
    def __init__(self, initial=None):
        self._value = initial

    def value(self):
        return self._value

    def update(self, val):
        self._value = val

    def clear(self):
        self._value = None


# ---------------------------------------------------------------------------
# Deterministic async workers
# ---------------------------------------------------------------------------


class _DeterministicClassifyAsyncFn(AsyncFunction):
    async def async_invoke(self, value):
        work = AsyncWorkItem.from_dict(value)
        payload = work.payload or {}
        event = payload.get("event", {})
        text = str(event.get("payload", "")).lower()
        if "travel" in text or "flight" in text or "hotel" in text:
            group_id = "topic_travel"
        elif "budget" in text or "project" in text or "risk" in text:
            group_id = "topic_project"
        else:
            group_id = payload.get("tentative_group") or "topic_general"
        return [
            AsyncResult(
                key=work.key,
                task_type="classify",
                request_id=work.request_id,
                success=True,
                result={
                    "group_id": group_id,
                    "confidence": 0.88,
                    "event_seq_id": int(event.get("seq_id", 0)),
                    "payload": str(event.get("payload", "")),
                },
            ).to_dict()
        ]

    def timeout(self, value):
        work = AsyncWorkItem.from_dict(value)
        return [
            AsyncResult(
                key=work.key,
                task_type="classify",
                request_id=work.request_id,
                success=False,
                error="classify_timeout",
            ).to_dict()
        ]


class _DeterministicSummarizeAsyncFn(AsyncFunction):
    async def async_invoke(self, value):
        work = AsyncWorkItem.from_dict(value)
        payload = work.payload or {}
        events = payload.get("events", [])
        snippets = [str(e.get("payload", "")) for e in events[:3]]
        summary = " | ".join(snippets)
        return [
            AsyncResult(
                key=work.key,
                task_type="summarize",
                request_id=work.request_id,
                success=True,
                result={
                    "summary": summary,
                    "version": int(payload.get("current_version", 0)) + 1,
                    "event_count": len(events),
                },
            ).to_dict()
        ]

    def timeout(self, value):
        work = AsyncWorkItem.from_dict(value)
        return [
            AsyncResult(
                key=work.key,
                task_type="summarize",
                request_id=work.request_id,
                success=False,
                error="summarize_timeout",
            ).to_dict()
        ]


class _DeterministicRetrieveAsyncFn(AsyncFunction):
    async def async_invoke(self, value):
        work = AsyncWorkItem.from_dict(value)
        payload = work.payload or {}
        query = str(payload.get("query", ""))
        query_l = query.lower()

        candidates: List[Dict[str, Any]] = []
        if "budget" in query_l or "project" in query_l:
            candidates.extend(
                [
                    {
                        "candidate_id": "mem_project_budget",
                        "content": "Project budget and timeline were reviewed.",
                        "score": 0.91,
                    },
                    {
                        "candidate_id": "mem_project_risk",
                        "content": "Project risk mitigation steps were discussed.",
                        "score": 0.84,
                    },
                ]
            )
        if "travel" in query_l or "flight" in query_l or "hotel" in query_l:
            candidates.extend(
                [
                    {
                        "candidate_id": "mem_travel_flight",
                        "content": "Flight and hotel booking for Tokyo were planned.",
                        "score": 0.93,
                    },
                    {
                        "candidate_id": "mem_travel_agenda",
                        "content": "Tokyo meeting agenda was finalized.",
                        "score": 0.81,
                    },
                ]
            )
        if not candidates:
            candidates.append(
                {
                    "candidate_id": "mem_generic",
                    "content": "General conversation memory entry.",
                    "score": 0.5,
                }
            )

        return [
            AsyncResult(
                key=work.key,
                task_type="retrieve",
                request_id=work.request_id,
                success=True,
                result={
                    "query": query,
                    "event_seq_id": int(payload.get("event_seq_id", 0)),
                    "candidates": candidates,
                },
            ).to_dict()
        ]

    def timeout(self, value):
        work = AsyncWorkItem.from_dict(value)
        return [
            AsyncResult(
                key=work.key,
                task_type="retrieve",
                request_id=work.request_id,
                success=False,
                error="retrieve_timeout",
            ).to_dict()
        ]


class _JsonLLMAsyncFn(AsyncFunction):
    """Helper for DeepSeek-backed in-memory workflow workers."""

    def __init__(self, llm_config: LLMClientConfig) -> None:
        self._llm_config = llm_config
    
    @staticmethod
    def _parse_json_object(text: str) -> Dict[str, Any]:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise
            parsed = json.loads(text[start:end + 1])
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Expected JSON object, got {type(parsed).__name__}")
        return parsed

    async def _call_json(self, prompt: str) -> Dict[str, Any]:
        client = create_llm_client(self._llm_config)
        try:
            text, _ = await client.call(prompt)
            return self._parse_json_object(text)
        finally:
            session = getattr(client, "_session", None)
            if session is not None and not session.closed:
                await session.close()

    def close(self) -> None:
        return


class _RealClassifyAsyncFn(_JsonLLMAsyncFn):
    async def async_invoke(self, value):
        work = AsyncWorkItem.from_dict(value)
        payload = work.payload or {}
        event = payload.get("event", {})
        prompt = (
            "You assign a conversational event to one semantic group.\n"
            "Allowed group_id values: topic_project, topic_travel, topic_general.\n"
            "Return JSON only with keys group_id, confidence, label.\n"
            f"Event payload: {event.get('payload', '')}\n"
            f"Tentative group: {payload.get('tentative_group', '')}\n"
        )
        try:
            parsed = await self._call_json(prompt)
            group_id = str(parsed.get("group_id", "topic_general"))
            if group_id not in {"topic_project", "topic_travel", "topic_general"}:
                group_id = "topic_general"
            return [
                AsyncResult(
                    key=work.key,
                    task_type="classify",
                    request_id=work.request_id,
                    success=True,
                    result={
                        "group_id": group_id,
                        "confidence": float(parsed.get("confidence", 0.5)),
                        "label": str(parsed.get("label", group_id)),
                        "event_seq_id": int(event.get("seq_id", 0)),
                        "payload": str(event.get("payload", "")),
                    },
                ).to_dict()
            ]
        except Exception as exc:
            return [
                AsyncResult(
                    key=work.key,
                    task_type="classify",
                    request_id=work.request_id,
                    success=False,
                    error=f"classify_api_error: {exc}",
                ).to_dict()
            ]

    def timeout(self, value):
        work = AsyncWorkItem.from_dict(value)
        return [
            AsyncResult(
                key=work.key,
                task_type="classify",
                request_id=work.request_id,
                success=False,
                error="classify_timeout",
            ).to_dict()
        ]


class _RealSummarizeAsyncFn(_JsonLLMAsyncFn):
    async def async_invoke(self, value):
        work = AsyncWorkItem.from_dict(value)
        payload = work.payload or {}
        events = payload.get("events", [])
        prompt = (
            "Summarize the following conversation events into one concise memory.\n"
            "Return JSON only with key summary.\n"
            f"Events: {json.dumps(events, ensure_ascii=False)}\n"
        )
        try:
            parsed = await self._call_json(prompt)
            return [
                AsyncResult(
                    key=work.key,
                    task_type="summarize",
                    request_id=work.request_id,
                    success=True,
                    result={
                        "summary": str(parsed.get("summary", "")),
                        "version": int(payload.get("current_version", 0)) + 1,
                        "event_count": len(events),
                    },
                ).to_dict()
            ]
        except Exception as exc:
            return [
                AsyncResult(
                    key=work.key,
                    task_type="summarize",
                    request_id=work.request_id,
                    success=False,
                    error=f"summarize_api_error: {exc}",
                ).to_dict()
            ]

    def timeout(self, value):
        work = AsyncWorkItem.from_dict(value)
        return [
            AsyncResult(
                key=work.key,
                task_type="summarize",
                request_id=work.request_id,
                success=False,
                error="summarize_timeout",
            ).to_dict()
        ]


class _RealRetrieveAsyncFn(_JsonLLMAsyncFn):
    def __init__(
        self,
        llm_config: LLMClientConfig,
        retrieval_corpus: List[Dict[str, Any]],
    ) -> None:
        super().__init__(llm_config)
        self._retrieval_corpus = retrieval_corpus

    async def async_invoke(self, value):
        work = AsyncWorkItem.from_dict(value)
        payload = work.payload or {}
        query = str(payload.get("query", ""))
        max_candidates = int(payload.get("max_candidates", 4))
        prompt = (
            "You are doing retrieval over memory items.\n"
            f"Select up to {max_candidates} most relevant candidate_ids for the query.\n"
            'Return JSON only in the form {"matches":[{"candidate_id":"...", "score":0.0}]}.\n'
            f"Query: {query}\n"
            f"Candidates: {json.dumps(self._retrieval_corpus, ensure_ascii=False)}\n"
        )
        try:
            parsed = await self._call_json(prompt)
            raw_matches = parsed.get("matches", [])
            if not isinstance(raw_matches, list):
                raw_matches = []
            by_id = {
                str(item.get("candidate_id", "")): dict(item)
                for item in self._retrieval_corpus
                if item.get("candidate_id")
            }
            candidates: List[Dict[str, Any]] = []
            for match in raw_matches[:max_candidates]:
                if not isinstance(match, dict):
                    continue
                candidate_id = str(match.get("candidate_id", ""))
                if candidate_id not in by_id:
                    continue
                candidate = dict(by_id[candidate_id])
                candidate["score"] = float(match.get("score", candidate.get("score", 0.0)))
                candidates.append(candidate)
            return [
                AsyncResult(
                    key=work.key,
                    task_type="retrieve",
                    request_id=work.request_id,
                    success=True,
                    result={
                        "query": query,
                        "event_seq_id": int(payload.get("event_seq_id", 0)),
                        "candidates": candidates,
                    },
                ).to_dict()
            ]
        except Exception as exc:
            return [
                AsyncResult(
                    key=work.key,
                    task_type="retrieve",
                    request_id=work.request_id,
                    success=False,
                    error=f"retrieve_api_error: {exc}",
                ).to_dict()
            ]

    def timeout(self, value):
        work = AsyncWorkItem.from_dict(value)
        return [
            AsyncResult(
                key=work.key,
                task_type="retrieve",
                request_id=work.request_id,
                success=False,
                error="retrieve_timeout",
            ).to_dict()
        ]


# ---------------------------------------------------------------------------
# Replay data and config
# ---------------------------------------------------------------------------


def _build_use_case_events() -> List[Dict[str, Any]]:
    base_ts = 1_700_000_000_000
    key = "user_001"
    return [
        {
            "key": key,
            "stream_type": "memory_event",
            "seq_id": 1,
            "event_time_ms": base_ts + 1_000,
            "payload": "Project alpha kickoff and planning notes",
            "boundary_flags": {"topic_shift": False},
            "metadata": {"msg_id": "m1"},
        },
        {
            "key": key,
            "stream_type": "memory_event",
            "seq_id": 2,
            "event_time_ms": base_ts + 2_000,
            "payload": "Project alpha budget and timeline discussion",
            "boundary_flags": {"topic_shift": False},
            "metadata": {"msg_id": "m2"},
        },
        {
            "key": key,
            "stream_type": "memory_event",
            "seq_id": 3,
            "event_time_ms": base_ts + 3_000,
            "payload": "Project alpha risk review and mitigation",
            "boundary_flags": {"topic_shift": True},
            "metadata": {"msg_id": "m3"},
        },
        {
            "key": key,
            "stream_type": "query_request",
            "seq_id": 101,
            "event_time_ms": base_ts + 3_500,
            "payload": "What was discussed about project budget?",
            "metadata": {"query_id": "q1"},
        },
        {
            "key": key,
            "stream_type": "memory_event",
            "seq_id": 4,
            "event_time_ms": base_ts + 4_000,
            "payload": "Travel plan for Tokyo conference",
            "boundary_flags": {"topic_shift": False},
            "metadata": {"msg_id": "m4"},
        },
        {
            "key": key,
            "stream_type": "memory_event",
            "seq_id": 5,
            "event_time_ms": base_ts + 5_000,
            "payload": "Book flight and hotel in Tokyo",
            "boundary_flags": {"topic_shift": False},
            "metadata": {"msg_id": "m5"},
        },
        {
            "key": key,
            "stream_type": "memory_event",
            "seq_id": 6,
            "event_time_ms": base_ts + 6_000,
            "payload": "Finalize Tokyo meeting agenda",
            "boundary_flags": {"topic_shift": True},
            "metadata": {"msg_id": "m6"},
        },
        {
            "key": key,
            "stream_type": "query_request",
            "seq_id": 102,
            "event_time_ms": base_ts + 6_500,
            "payload": "What travel actions were planned?",
            "metadata": {"query_id": "q2"},
        },
    ]


def _build_configs():
    window_cfg = SemWindowConfig(max_window_events=4, window_timeout_ms=60_000)
    groupby_cfg = SemGroupbyConfig(
        max_groups_per_key=16,
        confidence_threshold=0.95,
        new_group_creation_threshold=0.1,
    )
    retrieve_cfg = CtsRetrieveConfig(max_candidates_per_request=4, max_cache_entries_per_key=32)
    topk_cfg = SemTopKConfig(max_candidates=16, recompute_interval_ms=0, emission_policy="snapshot")
    topk_qs = TopKQuerySpec(k=2)
    return window_cfg, groupby_cfg, retrieve_cfg, topk_cfg, topk_qs


def _build_retrieval_corpus(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    corpus: List[Dict[str, Any]] = []
    for event in events:
        if event.get("stream_type") != "memory_event":
            continue
        corpus.append(
            {
                "candidate_id": f"mem_{event.get('seq_id', 0)}",
                "content": str(event.get("payload", "")),
                "score": 0.0,
                "source_msg_id": event.get("metadata", {}).get("msg_id", ""),
            }
        )
    return corpus


# ---------------------------------------------------------------------------
# In-memory replay harness
# ---------------------------------------------------------------------------


def _split_outputs(items: Iterable[Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    main: List[Dict[str, Any]] = []
    side: List[Dict[str, Any]] = []
    for item in items:
        if isinstance(item, tuple) and len(item) == 2:
            side.append(item[1])
        else:
            main.append(item)
    return main, side


def _async_call(fn: AsyncFunction, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return asyncio.run(fn.async_invoke(payload))


def _merge_with_async(
    main_rows: List[Dict[str, Any]],
    side_work_rows: List[Dict[str, Any]],
    merge_fn,
    async_fn: Optional[AsyncFunction],
    fallback_stage: str,
    key: str,
) -> List[Dict[str, Any]]:
    ctx = _FakeContext(key)
    merged: List[Dict[str, Any]] = list(main_rows)
    if async_fn is not None:
        for work in side_work_rows:
            for async_result in _async_call(async_fn, work):
                merged.extend(list(merge_fn.process_element(async_result, ctx)))
        return merged

    fallback = _MissingAsyncFallbackMapper(fallback_stage)
    for work in side_work_rows:
        merged.append(fallback(work))
    return merged


def _run_memory_path(
    events: List[Dict[str, Any]],
    agg_mode: str,
    classify_async_fn: Optional[AsyncFunction],
    summarize_async_fn: Optional[AsyncFunction],
) -> List[Dict[str, Any]]:
    key = "user_001"
    window_cfg, groupby_cfg, _, _, _ = _build_configs()
    agg_cfg = SemAggConfig(mode=agg_mode, max_buffer_events=2, flush_interval_ms=0)

    sem_window = SemWindowFunction(window_cfg)
    sem_window._event_buffer = _FakeListState()
    sem_window._window_meta = _FakeValueState(None)

    sem_groupby = SemGroupbyFunction(groupby_cfg)
    sem_groupby._group_profiles = _FakeMapState()
    sem_groupby._meta = _FakeValueState(None)

    sem_agg = SemAggFunction(agg_cfg)
    sem_agg._buffer = _FakeListState()
    sem_agg._agg_value = _FakeValueState(None)
    sem_agg._meta = _FakeValueState(None)

    classify_merge = _ClassifyAsyncMergeFunction()
    to_agg = _GroupbyToAggEnvelope()
    summarize_merge = _SummarizeAsyncMergeFunction()

    window_ctx = _FakeContext(key)
    groupby_ctx = _FakeContext(key)
    to_agg_ctx = _FakeContext(key)
    agg_ctx = _FakeContext(key)
    summarize_ctx = _FakeContext(key)

    snapshots: List[Dict[str, Any]] = []
    for event in events:
        if event.get("stream_type") != "memory_event":
            continue
        snapshots.extend(list(sem_window.process_element(event, window_ctx)))

    grouped_main: List[Dict[str, Any]] = []
    grouped_side: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        outs = list(sem_groupby.process_element(snapshot, groupby_ctx))
        main, side = _split_outputs(outs)
        grouped_main.extend(main)
        grouped_side.extend(side)

    grouped_merged = _merge_with_async(
        grouped_main,
        grouped_side,
        classify_merge,
        classify_async_fn,
        "classify",
        key,
    )

    agg_inputs: List[Dict[str, Any]] = []
    for row in grouped_merged:
        agg_inputs.extend(list(to_agg.process_element(row, to_agg_ctx)))

    agg_main: List[Dict[str, Any]] = []
    agg_side: List[Dict[str, Any]] = []
    for event in agg_inputs:
        outs = list(sem_agg.process_element(event, agg_ctx))
        main, side = _split_outputs(outs)
        agg_main.extend(main)
        agg_side.extend(side)

    agg_merged = _merge_with_async(
        agg_main,
        agg_side,
        summarize_merge,
        summarize_async_fn,
        "summarize",
        key,
    )
    final_rows: List[Dict[str, Any]] = []
    for row in agg_merged:
        final_rows.extend(list(summarize_merge.process_element(row, summarize_ctx)))
    return final_rows


def _run_retrieval_path(
    events: List[Dict[str, Any]],
    retrieve_async_fn: Optional[AsyncFunction],
) -> List[Dict[str, Any]]:
    key = "user_001"
    _, _, retrieve_cfg, topk_cfg, topk_qs = _build_configs()

    retrieve = CtsRetrieveFunction(retrieve_cfg)
    retrieve._cache = _FakeMapState()
    retrieve._meta = _FakeValueState(None)

    retrieve_merge = _RetrieveAsyncMergeFunction()
    expander = _RetrievalEnvelopeExpander()
    topk = SemTopKFunction(topk_cfg, query_spec=topk_qs)
    topk._candidates = _FakeMapState()
    topk._snapshot = _FakeValueState(None)
    topk._meta = _FakeValueState(None)
    normalize = _RetrievalToAnswerEnvelope()

    retrieve_ctx = _FakeContext(key)
    retrieve_merge_ctx = _FakeContext(key)
    expander_ctx = _FakeContext(key)
    topk_ctx = _FakeContext(key)
    normalize_ctx = _FakeContext(key)

    retrieve_main: List[Dict[str, Any]] = []
    retrieve_side: List[Dict[str, Any]] = []
    for event in events:
        if event.get("stream_type") != "query_request":
            continue
        outs = list(retrieve.process_element(event, retrieve_ctx))
        main, side = _split_outputs(outs)
        retrieve_main.extend(main)
        retrieve_side.extend(side)

    merged_retrieve = _merge_with_async(
        retrieve_main,
        retrieve_side,
        retrieve_merge,
        retrieve_async_fn,
        "retrieve",
        key,
    )

    # Expand retrieval envelopes into flat candidates before feeding to topk
    expanded_candidates: List[Dict[str, Any]] = []
    for row in merged_retrieve:
        expanded_candidates.extend(list(expander.process_element(row, expander_ctx)))

    scored_candidate_rows = [row for row in expanded_candidates if "candidate_id" in row]
    passthrough_rows = [row for row in expanded_candidates if "candidate_id" not in row]

    topk_rows: List[Dict[str, Any]] = []
    for row in scored_candidate_rows:
        topk_rows.extend(list(topk.process_element(row, topk_ctx)))

    normalized: List[Dict[str, Any]] = []
    for row in passthrough_rows + topk_rows:
        normalized.extend(list(normalize.process_element(row, normalize_ctx)))
    return normalized


def _run_answer_path(
    retrieval_rows: List[Dict[str, Any]],
    workflow_version: str = "v0.2.workflow_test",
    config_version: str = "cfg.mock.v1",
) -> List[Dict[str, Any]]:
    key = "user_001"
    synth = _AnswerSynthesiser(
        prompt_template="Context:\n{context}\n\nQuery:\n{query}\n\nAnswer:",
        workflow_version=workflow_version,
        config_version=config_version,
    )
    ctx = _FakeContext(key)
    answers: List[Dict[str, Any]] = []
    for row in retrieval_rows:
        answers.extend(list(synth.process_element(row, ctx)))
    return answers


def _write_trace(
    events: List[Dict[str, Any]],
    memory_rows: List[Dict[str, Any]],
    retrieval_rows: List[Dict[str, Any]],
    answer_rows: List[Dict[str, Any]],
) -> str:
    path = pathlib.Path("/tmp") / f"v02_workflow_trace_{int(time.time())}.json"
    payload = {
        "workflow": "v0.2_lotus_inspired_in_memory",
        "input_event_count": len(events),
        "memory_output_count": len(memory_rows),
        "retrieval_output_count": len(retrieval_rows),
        "answer_output_count": len(answer_rows),
        "memory_sample": memory_rows[:3],
        "retrieval_sample": retrieval_rows[:3],
        "answer_sample": answer_rows[:3],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


async def _run_real_answer_completion(
    answer_requests: List[Dict[str, Any]],
    llm_config: LLMClientConfig,
) -> List[Dict[str, Any]]:
    client = create_llm_client(llm_config)
    try:
        results: List[Dict[str, Any]] = []
        for request in answer_requests:
            prompt = (
                request.get("prompt", "")
                + "\nReturn JSON only with keys answer and confidence."
            )
            try:
                text, _ = await client.call(prompt)
                parsed = _JsonLLMAsyncFn._parse_json_object(text)
                out = dict(request)
                out["answer"] = str(parsed.get("answer", ""))
                out["answer_confidence"] = float(parsed.get("confidence", 0.0))
                out["answer_degraded"] = False
                out["answer_error"] = ""
                results.append(out)
            except Exception as exc:
                out = dict(request)
                out["answer"] = ""
                out["answer_confidence"] = 0.0
                out["answer_degraded"] = True
                out["answer_error"] = str(exc)
                results.append(out)
        return results
    finally:
        session = getattr(client, "_session", None)
        if session is not None and not session.closed:
            await session.close()


def _parse_dotenv_line(raw: str) -> Tuple[Optional[str], Optional[str]]:
    line = raw.strip()
    if not line or line.startswith("#"):
        return None, None
    if line.startswith("export "):
        line = line[len("export "):].strip()
    if "=" not in line:
        return None, None
    key, val = line.split("=", 1)
    key = key.strip()
    val = val.strip()
    if not key:
        return None, None
    if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
        val = val[1:-1]
    return key, val


def _try_load_env_file(env_file: str) -> Optional[str]:
    path_arg = pathlib.Path(env_file).expanduser()
    if path_arg.is_absolute():
        candidates = [path_arg]
    else:
        repo_root = pathlib.Path(__file__).resolve().parents[4]
        candidates = [pathlib.Path.cwd() / path_arg, repo_root / path_arg]

    for candidate in candidates:
        if not candidate.exists():
            continue
        loaded = 0
        with candidate.open("r", encoding="utf-8") as f:
            for raw in f:
                key, val = _parse_dotenv_line(raw)
                if not key or key in os.environ:
                    continue
                os.environ[key] = val or ""
                loaded += 1
        return f"{candidate} (loaded={loaded})"
    return None


def _build_real_llm_config(args: argparse.Namespace) -> LLMClientConfig:
    if not os.environ.get(args.api_key_env):
        raise RuntimeError(
            f"Environment variable {args.api_key_env} is empty. "
            f"Export it first or provide --env-file."
        )
    return LLMClientConfig(
        backend="openai",
        model=args.model,
        api_base=args.api_base,
        api_key_env=args.api_key_env,
        timeout_s=args.llm_timeout_s,
        max_retries=args.llm_max_retries,
        retry_base_delay_s=args.llm_retry_base_delay_s,
    )


def _write_run_artifacts(
    artifact_dir: str,
    mode: str,
    env_info: Optional[str],
    memory_rows: List[Dict[str, Any]],
    retrieval_rows: List[Dict[str, Any]],
    answer_requests: List[Dict[str, Any]],
    answer_rows: List[Dict[str, Any]],
) -> Tuple[str, str]:
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = pathlib.Path(artifact_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"v02_evermemos_{mode}_{ts}.json"
    md_path = out_dir / f"v02_evermemos_{mode}_{ts}.md"

    payload = {
        "mode": mode,
        "env_file": env_info,
        "memory_output_count": len(memory_rows),
        "retrieval_output_count": len(retrieval_rows),
        "answer_request_count": len(answer_requests),
        "answer_output_count": len(answer_rows),
        "answer_degraded_count": sum(1 for row in answer_rows if row.get("answer_degraded")),
        "memory_rows": memory_rows,
        "retrieval_rows": retrieval_rows,
        "answer_requests": answer_requests,
        "answer_rows": answer_rows,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        f"# V0.2 EverMemOS Workflow ({mode})",
        "",
        f"- env_file: `{env_info or ''}`",
        f"- memory_output_count: `{len(memory_rows)}`",
        f"- retrieval_output_count: `{len(retrieval_rows)}`",
        f"- answer_request_count: `{len(answer_requests)}`",
        f"- answer_output_count: `{len(answer_rows)}`",
        f"- answer_degraded_count: `{sum(1 for row in answer_rows if row.get('answer_degraded'))}`",
        "",
        "## Answer Samples",
    ]
    for row in answer_rows[:5]:
        md_lines.append(
            f"- q: {row.get('query', '')} | answer: {row.get('answer', '')}"
        )
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    return str(json_path), str(md_path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_v02_workflow_lotus_inspired_contract_and_counts():
    events = _build_use_case_events()
    query_count = sum(1 for e in events if e.get("stream_type") == "query_request")

    classify_fn = _DeterministicClassifyAsyncFn()
    summarize_fn = _DeterministicSummarizeAsyncFn()
    retrieve_fn = _DeterministicRetrieveAsyncFn()

    memory_rows = _run_memory_path(events, agg_mode="algebraic", classify_async_fn=classify_fn, summarize_async_fn=summarize_fn)
    retrieval_rows = _run_retrieval_path(events, retrieve_async_fn=retrieve_fn)
    answer_rows = _run_answer_path(retrieval_rows)
    answer_rows_again = _run_answer_path(retrieval_rows)

    trace_path = _write_trace(events, memory_rows, retrieval_rows, answer_rows)
    assert pathlib.Path(trace_path).exists()

    # Hard gate: no schema crash
    assert all(isinstance(r, dict) for r in memory_rows)
    assert all(isinstance(r, dict) for r in retrieval_rows)
    assert all(isinstance(r, dict) for r in answer_rows)

    # Hard gate: deterministic counts on same replay input
    assert len(answer_rows) == len(answer_rows_again)

    # Hard gate: no silent drop on query chain.
    # With envelope expansion, topk emits per-candidate-update (not per-envelope),
    # so we get >= query_count rows.  The key invariant is no silent drop.
    assert len(retrieval_rows) >= query_count
    assert len(answer_rows) == len(retrieval_rows)

    # Retrieval envelope normalization
    for row in retrieval_rows:
        assert "key" in row
        assert "query" in row
        assert "retrieved_context" in row
        assert isinstance(row["retrieved_context"], list)
        assert "total_candidates" in row
        assert row["total_candidates"] <= 4

    # Answer audit envelope
    required = [
        "workflow_version",
        "config_version",
        "key",
        "query",
        "prompt",
        "retrieved_ids",
        "total_candidates",
    ]
    for row in answer_rows:
        for k in required:
            assert k in row
        assert row["workflow_version"] == "v0.2.workflow_test"
        assert row["config_version"] == "cfg.mock.v1"
        assert isinstance(row["retrieved_ids"], list)

    # Classify and retrieve async merge-back evidence
    group_sources = set()
    for row in memory_rows:
        aggregate = row.get("aggregate", {})
        if isinstance(aggregate, dict):
            metadata = aggregate.get("metadata", {})
            if isinstance(metadata, dict):
                src = metadata.get("group_source")
                if src:
                    group_sources.add(src)
    assert "async_classify" in group_sources
    assert any(str(r.get("source", "")).startswith("async_retrieve") for r in retrieval_rows)


def test_v02_workflow_retrieve_path_via_mock_search_backend():
    events = _build_use_case_events()
    backend = MockSearchBackend(
        query_rules={
            "budget": [
                {
                    "candidate_id": "mem_project_budget",
                    "text": "Project budget and timeline were reviewed.",
                    "score": 0.91,
                },
                {
                    "candidate_id": "mem_project_risk",
                    "text": "Project risk mitigation steps were discussed.",
                    "score": 0.84,
                },
            ],
            "travel": [
                {
                    "candidate_id": "mem_travel_flight",
                    "text": "Flight and hotel booking for Tokyo were planned.",
                    "score": 0.93,
                }
            ],
        },
        default_results=[
            {
                "candidate_id": "mem_generic",
                "text": "General conversation memory entry.",
                "score": 0.5,
            }
        ],
    )
    retrieve_fn = SearchBackendAsyncFn(backend)
    retrieval_rows = _run_retrieval_path(events, retrieve_async_fn=retrieve_fn)

    assert retrieval_rows
    assert any(str(r.get("source", "")).startswith("async_retrieve") for r in retrieval_rows)
    budget_rows = [r for r in retrieval_rows if "budget" in str(r.get("query", "")).lower()]
    assert budget_rows
    first_budget_ids = [
        item.get("candidate_id")
        for item in budget_rows[0].get("retrieved_context", [])
        if isinstance(item, dict)
    ]
    assert "mem_project_budget" in first_budget_ids


def test_v02_workflow_summarize_and_missing_async_fallback():
    events = _build_use_case_events()

    # Summarize async merge-back path
    memory_rows = _run_memory_path(
        events,
        agg_mode="summarize",
        classify_async_fn=_DeterministicClassifyAsyncFn(),
        summarize_async_fn=_DeterministicSummarizeAsyncFn(),
    )
    assert any(r.get("mode") == "summarize_async" for r in memory_rows)

    # Missing async worker -> explicit degraded fallback records
    retrieval_rows_missing = _run_retrieval_path(events, retrieve_async_fn=None)
    memory_rows_missing = _run_memory_path(
        events,
        agg_mode="summarize",
        classify_async_fn=_DeterministicClassifyAsyncFn(),
        summarize_async_fn=None,
    )

    assert any(r.get("degraded") is True for r in retrieval_rows_missing)
    assert any(r.get("error") == "async_retrieve_missing_worker" for r in retrieval_rows_missing)
    assert any(r.get("mode") == "summarize_missing_worker" for r in memory_rows_missing)


def _close_workers(*workers: Optional[AsyncFunction]) -> None:
    for worker in workers:
        if worker is None:
            continue
        close_fn = getattr(worker, "close", None)
        if callable(close_fn):
            close_fn()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V0.2 EverMemOS workflow runner")
    p.add_argument("--mode", choices=["mock", "real"], default="mock")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--artifact-dir", default="/tmp/cp_v02_evermemos_artifacts")
    p.add_argument("--api-key-env", default="DEEPSEEK_API_KEY")
    p.add_argument("--api-base", default="https://api.deepseek.com/v1")
    p.add_argument("--model", default="deepseek-chat")
    p.add_argument("--llm-timeout-s", type=float, default=45.0)
    p.add_argument("--llm-max-retries", type=int, default=2)
    p.add_argument("--llm-retry-base-delay-s", type=float, default=0.8)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    events = _build_use_case_events()

    if args.mode == "mock":
        classify_fn = _DeterministicClassifyAsyncFn()
        summarize_fn = _DeterministicSummarizeAsyncFn()
        retrieve_fn = _DeterministicRetrieveAsyncFn()
        try:
            memory_rows = _run_memory_path(
                events,
                agg_mode="summarize",
                classify_async_fn=classify_fn,
                summarize_async_fn=summarize_fn,
            )
            retrieval_rows = _run_retrieval_path(events, retrieve_async_fn=retrieve_fn)
            answer_requests = _run_answer_path(retrieval_rows)
            answer_rows = [dict(row) for row in answer_requests]
            for row in answer_rows:
                row["answer"] = row.get("context_str", "")
                row["answer_confidence"] = 1.0
                row["answer_degraded"] = False
                row["answer_error"] = ""
            json_path, md_path = _write_run_artifacts(
                args.artifact_dir,
                "mock",
                None,
                memory_rows,
                retrieval_rows,
                answer_requests,
                answer_rows,
            )
            print("=== V0.2 EverMemOS Workflow ===")
            print("mode: mock")
            print(f"artifact_json: {json_path}")
            print(f"artifact_md:   {md_path}")
        finally:
            _close_workers(classify_fn, summarize_fn, retrieve_fn)
        return

    env_info = _try_load_env_file(args.env_file)
    llm_cfg = _build_real_llm_config(args)
    retrieval_corpus = _build_retrieval_corpus(events)
    classify_fn = _RealClassifyAsyncFn(llm_cfg)
    summarize_fn = _RealSummarizeAsyncFn(llm_cfg)
    retrieve_fn = _RealRetrieveAsyncFn(llm_cfg, retrieval_corpus)

    try:
        memory_rows = _run_memory_path(
            events,
            agg_mode="summarize",
            classify_async_fn=classify_fn,
            summarize_async_fn=summarize_fn,
        )
        retrieval_rows = _run_retrieval_path(events, retrieve_async_fn=retrieve_fn)
        answer_requests = _run_answer_path(
            retrieval_rows,
            workflow_version="v0.2.evermemos.real",
            config_version="cfg.deepseek.real",
        )
        answer_rows = asyncio.run(_run_real_answer_completion(answer_requests, llm_cfg))
        json_path, md_path = _write_run_artifacts(
            args.artifact_dir,
            "real",
            env_info,
            memory_rows,
            retrieval_rows,
            answer_requests,
            answer_rows,
        )
        print("=== V0.2 EverMemOS Workflow ===")
        print("mode: real")
        print(f"env_file: {env_info}")
        print(f"artifact_json: {json_path}")
        print(f"artifact_md:   {md_path}")
        print(f"memory_output_count: {len(memory_rows)}")
        print(f"retrieval_output_count: {len(retrieval_rows)}")
        print(f"answer_output_count: {len(answer_rows)}")
        print(
            "answer_degraded_count: "
            f"{sum(1 for row in answer_rows if row.get('answer_degraded'))}"
        )
    finally:
        _close_workers(classify_fn, summarize_fn, retrieve_fn)


if __name__ == "__main__":
    main()
