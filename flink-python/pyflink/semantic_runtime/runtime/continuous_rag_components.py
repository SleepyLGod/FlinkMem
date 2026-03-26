# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Internal helper components for the continuous RAG workflow."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from pyflink.common import Types
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import KeyedProcessFunction

from pyflink.semantic_runtime.runtime.async_bridge import build_async_bridge
from pyflink.semantic_runtime.runtime.event_model import (
    group_assignment_to_sem_event,
    retrieve_to_answer_context,
    retrieve_to_topk_items,
    topk_to_answer_context,
)
from pyflink.semantic_runtime.runtime.external_search_backend import SearchBackendAsyncFn

if TYPE_CHECKING:
    from pyflink.semantic_runtime.runtime.continuous_rag_workflow import ContinuousRAGConfig


logger = logging.getLogger(__name__)


MEMORY_EVENT_TAG = OutputTag("memory_events", Types.PICKLED_BYTE_ARRAY())
QUERY_REQUEST_TAG = OutputTag("query_requests", Types.PICKLED_BYTE_ARRAY())


class _StreamRouter(KeyedProcessFunction):
    """Split mixed input events into memory and query streams."""

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            logger.warning("StreamRouter: non-dict input dropped: %s", type(value))
            return

        stream_type = value.get("stream_type", "")
        if stream_type == "memory_event":
            yield MEMORY_EVENT_TAG, value
            return
        if stream_type == "query_request":
            yield QUERY_REQUEST_TAG, value
            return

        logger.debug(
            "StreamRouter: unknown stream_type '%s', routing as memory_event",
            stream_type,
        )
        yield MEMORY_EVENT_TAG, value


class _AnswerSynthesiser(KeyedProcessFunction):
    """Combine retrieval context with the original query for answer synthesis."""

    def __init__(
        self,
        prompt_template: str,
        workflow_version: str = "v0.2.0",
        config_version: str = "default",
    ) -> None:
        self._prompt_template = prompt_template
        self._workflow_version = workflow_version
        self._config_version = config_version

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return

        now_ms = int(time.time() * 1000)
        query = value.get("query", value.get("payload", ""))
        context_items = value.get(
            "retrieved_context",
            value.get("topk", value.get("candidates", [])),
        )

        if isinstance(context_items, list):
            context_str = "\n".join(
                str(item.get("payload", item.get("content", str(item))))
                for item in context_items
            )
        else:
            context_str = str(context_items)

        prompt = self._prompt_template.format(context=context_str, query=query)

        retrieved_ids: list[str] = []
        if isinstance(context_items, list):
            for item in context_items:
                candidate_id = item.get("candidate_id", item.get("id", ""))
                if candidate_id:
                    retrieved_ids.append(candidate_id)

        yield {
            "key": value.get("key", str(ctx.get_current_key())),
            "stream_type": "answer_request",
            "prompt": prompt,
            "query": query,
            "context_str": context_str,
            "retrieved_ids": retrieved_ids,
            "memory_version": value.get("memory_version", value.get("version", 0)),
            "workflow_version": self._workflow_version,
            "config_version": self._config_version,
            "timestamp_ms": now_ms,
            "total_candidates": value.get(
                "total_candidates",
                value.get("candidate_count", 0),
            ),
            "retrieval_changed": value.get(
                "retrieval_changed",
                value.get("changed", False),
            ),
            "source": value.get("source", ""),
            "error": value.get("error", ""),
        }


class _RetrieveAsyncMergeFunction(KeyedProcessFunction):
    """Merge retrieve async results into normalized retrieval envelopes."""

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return
        if value.get("task_type") != "retrieve":
            yield value
            return
        if not value.get("success", False):
            error = value.get("error", "retrieve_async_failed")
            raise RuntimeError(f"retrieve async bridge failed: {error}")

        now_ms = int(time.time() * 1000)
        payload = value.get("payload", {})
        result = value.get("result", {})
        candidates = result.get("candidates", [])
        yield {
            "key": value.get("key", str(ctx.get_current_key())),
            "query": result.get("query", payload.get("query", "")),
            "query_seq_id": int(result.get("event_seq_id", payload.get("event_seq_id", 0))),
            "candidates": candidates,
            "candidate_count": len(candidates),
            "truncated": bool(result.get("truncated", False)),
            "source": "async_retrieve",
            "timestamp_ms": now_ms,
        }


class _GroupbyToAggEnvelope(KeyedProcessFunction):
    """Normalize sem_groupby outputs into SemEvent-like envelopes for sem_agg."""

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return
        if {"key", "payload", "seq_id"}.issubset(value.keys()):
            yield value
            return
        yield group_assignment_to_sem_event(value)


class _RetrievalToAnswerEnvelope(KeyedProcessFunction):
    """Normalize retrieval/top-k outputs into answer-input envelopes."""

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return
        if "retrieved_context" in value:
            yield value
            return
        if "topk" in value or "top_items" in value:
            out = topk_to_answer_context(value, query_payload=value.get("query", ""))
            out["memory_version"] = value.get("version", 0)
            out["error"] = value.get("error", "")
            yield out
            return

        out = retrieve_to_answer_context(value)
        out["memory_version"] = value.get("version", 0)
        out["error"] = value.get("error", "")
        yield out


class _RetrievalEnvelopeExpander(KeyedProcessFunction):
    """Expand retrieval envelopes into flat candidate dicts for pointwise top-k."""

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        if not isinstance(value, dict):
            return
        if "candidates" in value and isinstance(value["candidates"], list):
            items = retrieve_to_topk_items(value)
            if items:
                for item in items:
                    yield item
                return
            yield value
            return
        yield value


def _wire_async_bridge_if_configured(
    operator_ds: DataStream,
    async_fn: Any,
    merge_fn: KeyedProcessFunction,
    stage_name: str,
    config: ContinuousRAGConfig,
) -> DataStream:
    """Wire an async side-output bridge and fail fast when it is missing."""

    if async_fn is None:
        raise ValueError(f"Async bridge not configured for stage {stage_name!r}")

    return build_async_bridge(
        main_ds=operator_ds,
        async_fn=async_fn,
        merge_fn=merge_fn,
        key_selector=config.key_selector,
        timeout_ms=config.async_timeout_ms,
        capacity=config.async_capacity,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

def _resolve_retrieve_async_fn(config: ContinuousRAGConfig) -> Any:
    """Resolve the retrieve async worker from workflow config."""

    if config.retrieve_async_fn is not None:
        return config.retrieve_async_fn

    backend = getattr(config.retrieve_config, "search_backend", None)
    if backend is not None:
        return SearchBackendAsyncFn(backend)
    return None
