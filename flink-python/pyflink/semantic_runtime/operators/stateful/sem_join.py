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

"""Low-level true two-input `sem_join` runtime.

This module implements the native dual-side state runtime for V0.3 `sem_join`.
It is an internal/expert-layer kernel, not a public API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime.pushdown.common import parse_window_snapshot
from pyflink.semantic_runtime.runtime.state_descriptors import (
    sem_join_left_buffer_descriptor,
    sem_join_left_windows_descriptor,
    sem_join_right_buffer_descriptor,
    sem_join_right_windows_descriptor,
)
from pyflink.semantic_runtime.runtime.event_model import window_snapshot_to_sem_events
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder
from pyflink.semantic_runtime.runtime.steps import evaluate_sem_match_block_sync
from pyflink.semantic_runtime.sem_spec import JoinQuerySpec


@dataclass
class SemJoinConfig:
    """Internal kernel config for true two-input `sem_join`.

    Attributes:
        ttl_seconds: Retention bound for buffered left/right records.
        max_left_buffer: Maximum retained left-side records per key.
        max_right_buffer: Maximum retained right-side records per key.
        pair_block_size: Number of candidate pairs evaluated per semantic request.
    """

    ttl_seconds: int = 3600
    max_left_buffer: int = 128
    max_right_buffer: int = 128
    pair_block_size: int = 8
    prefilter_strategy: str = "none"
    prefilter_min_score: float = 0.0

    def __post_init__(self) -> None:
        if self.ttl_seconds <= 0:
            raise ValueError("sem_join requires ttl_seconds > 0")
        if self.max_left_buffer <= 0:
            raise ValueError("sem_join requires max_left_buffer > 0")
        if self.max_right_buffer <= 0:
            raise ValueError("sem_join requires max_right_buffer > 0")
        if self.pair_block_size <= 0:
            raise ValueError("sem_join requires pair_block_size > 0")
        if self.prefilter_strategy not in {"none", "hashing_similarity"}:
            raise ValueError(
                "sem_join requires prefilter_strategy to be one of "
                "{'none', 'hashing_similarity'}"
            )
        if not 0.0 <= self.prefilter_min_score <= 1.0:
            raise ValueError("sem_join requires 0.0 <= prefilter_min_score <= 1.0")


def _validate_sem_join_backend(query_spec: JoinQuerySpec) -> None:
    backend = query_spec.semantic.backend
    if backend not in {"llm", "hybrid"}:
        raise NotImplementedError(
            f"sem_join currently supports only llm/hybrid semantic backend, got {backend!r}"
        )


def _build_pair_rows(
    left_rows: List[Any],
    right_rows: List[Any],
    *,
    kernel_config: SemJoinConfig,
) -> List[Dict[str, Any]]:
    if not left_rows or not right_rows:
        return []

    if kernel_config.prefilter_strategy == "none":
        return [{"left": left, "right": right} for left in left_rows for right in right_rows]

    encoder = HashingTextEncoder(dim=128)
    pair_rows: List[Dict[str, Any]] = []
    for left in left_rows:
        left_text = str(left)
        for right in right_rows:
            right_text = str(right)
            score = encoder.similarity(left_text, right_text)
            if score < kernel_config.prefilter_min_score:
                continue
            pair_rows.append({"left": left, "right": right})
    return pair_rows


def _pair_blocks(
    pairs: List[Dict[str, Any]],
    *,
    pair_block_size: int,
) -> Iterable[List[Dict[str, Any]]]:
    for start in range(0, len(pairs), pair_block_size):
        yield pairs[start : start + pair_block_size]


def _evaluate_pair_rows(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    query_spec: JoinQuerySpec,
    kernel_config: SemJoinConfig,
    left_rows: List[Any],
    right_rows: List[Any],
) -> List[Dict[str, Any]]:
    pair_rows = _build_pair_rows(
        left_rows,
        right_rows,
        kernel_config=kernel_config,
    )
    if not pair_rows:
        return []

    outputs: List[Dict[str, Any]] = []
    for block in _pair_blocks(pair_rows, pair_block_size=kernel_config.pair_block_size):
        matches = evaluate_sem_match_block_sync(
            client=client,
            llm_config=llm_config,
            intent=query_spec.semantic.instruction,
            pair_block=block,
        )
        for match in matches:
            if not bool(match["matched"]):
                continue
            pair = block[int(match["pair_idx"])]
            outputs.append(
                {
                    "left": pair["left"],
                    "right": pair["right"],
                    "matched": True,
                    "match_score": float(match["match_score"]),
                    "reason": str(match["reason"]),
                }
            )
    return outputs


class SemJoinFunction(KeyedCoProcessFunction):
    """Native keyed two-input semantic join runtime."""

    def __init__(
        self,
        query_spec: JoinQuerySpec,
        llm_config: LLMClientConfig,
        kernel_config: SemJoinConfig,
    ) -> None:
        self._query_spec = query_spec
        self._llm_config = llm_config
        self._kernel_config = kernel_config
        self._client: Optional[LLMClient] = None
        self._left_buffer = None
        self._right_buffer = None

    def open(self, runtime_context: RuntimeContext) -> None:
        _validate_sem_join_backend(self._query_spec)
        self._client = create_llm_client(self._llm_config)
        ttl_seconds = self._kernel_config.ttl_seconds
        self._left_buffer = runtime_context.get_list_state(
            sem_join_left_buffer_descriptor(ttl_seconds)
        )
        self._right_buffer = runtime_context.get_list_state(
            sem_join_right_buffer_descriptor(ttl_seconds)
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def process_element1(
        self,
        value: Any,
        ctx: "KeyedCoProcessFunction.Context",
    ) -> Iterable[Dict[str, Any]]:
        current_time_ms = self._current_time_ms(ctx)
        left_record = self._stamp_record(value, current_time_ms)
        left_buffer = self._load_buffer(self._left_buffer)
        right_buffer = self._prune_expired(self._load_buffer(self._right_buffer), current_time_ms)
        left_buffer = self._append_bounded(
            left_buffer,
            left_record,
            max_size=self._kernel_config.max_left_buffer,
        )
        self._store_buffer(self._left_buffer, left_buffer)
        self._store_buffer(self._right_buffer, right_buffer)
        return self._evaluate_pairs(left_rows=[left_record], right_rows=right_buffer)

    def process_element2(
        self,
        value: Any,
        ctx: "KeyedCoProcessFunction.Context",
    ) -> Iterable[Dict[str, Any]]:
        current_time_ms = self._current_time_ms(ctx)
        right_record = self._stamp_record(value, current_time_ms)
        left_buffer = self._prune_expired(self._load_buffer(self._left_buffer), current_time_ms)
        right_buffer = self._load_buffer(self._right_buffer)
        right_buffer = self._append_bounded(
            right_buffer,
            right_record,
            max_size=self._kernel_config.max_right_buffer,
        )
        self._store_buffer(self._left_buffer, left_buffer)
        self._store_buffer(self._right_buffer, right_buffer)
        return self._evaluate_pairs(left_rows=left_buffer, right_rows=[right_record])

    def on_timer(
        self,
        timestamp: int,
        ctx: "KeyedCoProcessFunction.OnTimerContext",
    ) -> Iterable[Dict[str, Any]]:
        current_time_ms = int(timestamp)
        left_buffer = self._prune_expired(self._load_buffer(self._left_buffer), current_time_ms)
        right_buffer = self._prune_expired(self._load_buffer(self._right_buffer), current_time_ms)
        self._store_buffer(self._left_buffer, left_buffer)
        self._store_buffer(self._right_buffer, right_buffer)
        return []

    def _current_time_ms(self, ctx: "KeyedCoProcessFunction.Context") -> int:
        timer_service = ctx.timer_service()
        current_time_ms = int(timer_service.current_processing_time())
        timer_service.register_processing_time_timer(
            current_time_ms + self._kernel_config.ttl_seconds * 1000
        )
        return current_time_ms

    def _load_buffer(self, state_handle) -> List[Dict[str, Any]]:
        assert state_handle is not None
        return [dict(item) for item in state_handle.get()]

    def _store_buffer(self, state_handle, items: List[Dict[str, Any]]) -> None:
        assert state_handle is not None
        state_handle.clear()
        for item in items:
            state_handle.add(item)

    def _stamp_record(self, value: Any, observed_at_ms: int) -> Dict[str, Any]:
        return {
            "payload": value,
            "_observed_at_ms": observed_at_ms,
        }

    def _append_bounded(
        self,
        items: List[Dict[str, Any]],
        item: Dict[str, Any],
        *,
        max_size: int,
    ) -> List[Dict[str, Any]]:
        out = list(items)
        out.append(item)
        if len(out) > max_size:
            out = out[-max_size:]
        return out

    def _prune_expired(
        self,
        items: List[Dict[str, Any]],
        current_time_ms: int,
    ) -> List[Dict[str, Any]]:
        ttl_ms = self._kernel_config.ttl_seconds * 1000
        cutoff = current_time_ms - ttl_ms
        return [item for item in items if int(item.get("_observed_at_ms", 0)) >= cutoff]

    def _evaluate_pairs(
        self,
        *,
        left_rows: List[Dict[str, Any]],
        right_rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not left_rows or not right_rows:
            return []
        assert self._client is not None, "open() was not called"
        return _evaluate_pair_rows(
            client=self._client,
            llm_config=self._llm_config,
            query_spec=self._query_spec,
            kernel_config=self._kernel_config,
            left_rows=[row["payload"] for row in left_rows],
            right_rows=[row["payload"] for row in right_rows],
        )


class WindowOwnedSemJoinFunction(KeyedCoProcessFunction):
    """Bounded window-owned semantic join over aligned WindowSnapshot inputs."""

    def __init__(
        self,
        query_spec: JoinQuerySpec,
        llm_config: LLMClientConfig,
        kernel_config: SemJoinConfig,
    ) -> None:
        self._query_spec = query_spec
        self._llm_config = llm_config
        self._kernel_config = kernel_config
        self._client: Optional[LLMClient] = None
        self._left_windows = None
        self._right_windows = None

    def open(self, runtime_context: RuntimeContext) -> None:
        _validate_sem_join_backend(self._query_spec)
        self._client = create_llm_client(self._llm_config)
        ttl_seconds = self._kernel_config.ttl_seconds
        self._left_windows = runtime_context.get_map_state(
            sem_join_left_windows_descriptor(ttl_seconds)
        )
        self._right_windows = runtime_context.get_map_state(
            sem_join_right_windows_descriptor(ttl_seconds)
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def process_element1(
        self,
        value: Any,
        ctx: "KeyedCoProcessFunction.Context",
    ) -> Iterable[Dict[str, Any]]:
        snapshot = parse_window_snapshot(value, operator_name="window-owned sem_join")
        window_id = self._window_id(snapshot)
        self._left_windows.put(window_id, snapshot)
        self._register_cleanup_timer(ctx)
        if not self._right_windows.contains(window_id):
            return []
        right_snapshot = self._right_windows.get(window_id)
        outputs = self._evaluate_window_pair(snapshot, right_snapshot)
        self._left_windows.remove(window_id)
        self._right_windows.remove(window_id)
        return outputs

    def process_element2(
        self,
        value: Any,
        ctx: "KeyedCoProcessFunction.Context",
    ) -> Iterable[Dict[str, Any]]:
        snapshot = parse_window_snapshot(value, operator_name="window-owned sem_join")
        window_id = self._window_id(snapshot)
        self._right_windows.put(window_id, snapshot)
        self._register_cleanup_timer(ctx)
        if not self._left_windows.contains(window_id):
            return []
        left_snapshot = self._left_windows.get(window_id)
        outputs = self._evaluate_window_pair(left_snapshot, snapshot)
        self._left_windows.remove(window_id)
        self._right_windows.remove(window_id)
        return outputs

    def on_timer(
        self,
        timestamp: int,
        ctx: "KeyedCoProcessFunction.OnTimerContext",
    ) -> Iterable[Dict[str, Any]]:
        self._prune_windows(self._left_windows, int(timestamp))
        self._prune_windows(self._right_windows, int(timestamp))
        return []

    def _window_id(self, snapshot: Dict[str, Any]) -> str:
        window_id = snapshot.get("window_id")
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("window-owned sem_join requires non-empty window_id")
        return window_id

    def _register_cleanup_timer(self, ctx: "KeyedCoProcessFunction.Context") -> None:
        current_time_ms = int(ctx.timer_service().current_processing_time())
        ctx.timer_service().register_processing_time_timer(
            current_time_ms + self._kernel_config.ttl_seconds * 1000
        )

    def _prune_windows(self, state_handle, current_time_ms: int) -> None:
        ttl_ms = self._kernel_config.ttl_seconds * 1000
        cutoff = current_time_ms - ttl_ms
        to_remove: List[str] = []
        for window_id, snapshot in state_handle.items():
            close_time_ms = int(snapshot.get("close_time_ms", 0) or 0)
            if close_time_ms < cutoff:
                to_remove.append(str(window_id))
        for window_id in to_remove:
            state_handle.remove(window_id)

    def _evaluate_window_pair(
        self,
        left_snapshot: Dict[str, Any],
        right_snapshot: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        assert self._client is not None, "open() was not called"
        left_rows = [event.get("payload") for event in window_snapshot_to_sem_events(left_snapshot)]
        right_rows = [event.get("payload") for event in window_snapshot_to_sem_events(right_snapshot)]
        outputs = _evaluate_pair_rows(
            client=self._client,
            llm_config=self._llm_config,
            query_spec=self._query_spec,
            kernel_config=self._kernel_config,
            left_rows=left_rows,
            right_rows=right_rows,
        )
        for row in outputs:
            row["window_id"] = left_snapshot["window_id"]
        return outputs


def build_sem_join_operator(
    query_spec: JoinQuerySpec,
    llm_config: LLMClientConfig,
    kernel_config: SemJoinConfig,
) -> SemJoinFunction:
    """Build the low-level true two-input sem_join runtime."""
    return SemJoinFunction(
        query_spec=query_spec,
        llm_config=llm_config,
        kernel_config=kernel_config,
    )


def build_window_owned_sem_join_operator(
    query_spec: JoinQuerySpec,
    llm_config: LLMClientConfig,
    kernel_config: SemJoinConfig,
) -> WindowOwnedSemJoinFunction:
    """Build the low-level bounded window-owned sem_join runtime."""
    return WindowOwnedSemJoinFunction(
        query_spec=query_spec,
        llm_config=llm_config,
        kernel_config=kernel_config,
    )
