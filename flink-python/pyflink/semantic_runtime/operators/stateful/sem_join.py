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

Canonical model:
1. two unbounded keyed streams,
2. continuous dual-side state,
3. scope only controls ingress/pruning boundaries,
4. semantic match executes over candidate pairs,
5. no window-owned pairing path.
"""

from __future__ import annotations

import concurrent.futures
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime.event_model import (
    is_window_snapshot,
    window_snapshot_to_sem_events,
)
from pyflink.semantic_runtime.runtime.pushdown.common import (
    parse_json_or_passthrough,
    parse_window_snapshot,
)
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder
from pyflink.semantic_runtime.runtime.state_descriptors import (
    sem_join_left_buffer_descriptor,
    sem_join_left_seen_seq_descriptor,
    sem_join_right_buffer_descriptor,
    sem_join_right_seen_seq_descriptor,
)
from pyflink.semantic_runtime.runtime.stateful_async_executor import (
    ensure_thread_pool_executor,
)
from pyflink.semantic_runtime.runtime.steps import evaluate_sem_match_block_sync
from pyflink.semantic_runtime.sem_spec import JoinQuerySpec


DEFAULT_SEM_JOIN_ASYNC_MAX_WORKERS = 20
DEFAULT_SEM_JOIN_ASYNC_POLL_INTERVAL_MS = 200
EMBEDDING_SIMILARITY_DIM = 128
UNKNOWN_WATERMARK = -1

LEFT_SIDE = "left"
RIGHT_SIDE = "right"
LEFT_JOIN_FAMILY = {"left", "full"}
RIGHT_JOIN_FAMILY = {"right", "full"}
PAIRING_METHOD_BRUTE_FORCE = "brute_force"
PAIRING_METHOD_CANDIDATE_PRUNED = "candidate_pruned"
PAIRING_METHOD_EMBEDDING_PREFILTER = "embedding_prefilter"
PAIRING_METHOD_BLOCKING = "blocking"


@dataclass
class SemJoinConfig:
    """Internal kernel config for true two-input `sem_join`."""

    ttl_seconds: int = 3600
    max_left_buffer: int = 128
    max_right_buffer: int = 128
    pair_block_size: int = 8
    prefilter_strategy: str = "none"
    prefilter_min_score: float = 0.0
    async_max_workers: int = DEFAULT_SEM_JOIN_ASYNC_MAX_WORKERS
    async_poll_interval_ms: int = DEFAULT_SEM_JOIN_ASYNC_POLL_INTERVAL_MS

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
        if self.async_max_workers <= 0:
            raise ValueError("sem_join requires async_max_workers > 0")
        if self.async_poll_interval_ms <= 0:
            raise ValueError("sem_join requires async_poll_interval_ms > 0")


def _validate_sem_join_backend(query_spec: JoinQuerySpec) -> None:
    backend = query_spec.semantic.backend
    if backend not in {"llm", "hybrid", "embedding"}:
        raise NotImplementedError(
            "sem_join supports semantic backends {'llm', 'hybrid', 'embedding'}; "
            f"got {backend!r}"
        )


def _validate_sem_join_trigger(query_spec: JoinQuerySpec) -> None:
    trigger_mode = query_spec.trigger_policy.mode
    if trigger_mode not in {"on_event", "on_scope_close"}:
        raise NotImplementedError(
            "sem_join supports trigger_policy.mode in {'on_event', 'on_scope_close'}; "
            f"got {trigger_mode!r}"
        )


def _record_id(side: str) -> str:
    return f"{side}-{uuid.uuid4().hex}"


def _is_sem_event_envelope(value: Any) -> bool:
    return isinstance(value, dict) and "payload" in value and "seq_id" in value


def _payload_for_join(value: Any) -> Any:
    if _is_sem_event_envelope(value):
        return value["payload"]
    return value


def _embedding_threshold(query_spec: JoinQuerySpec) -> float:
    threshold = query_spec.semantic.threshold
    if threshold is None:
        raise ValueError(
            "sem_join embedding backend requires semantic.threshold in JoinQuerySpec"
        )
    numeric = float(threshold)
    if not 0.0 <= numeric <= 1.0:
        raise ValueError("sem_join embedding threshold must be within [0.0, 1.0]")
    return numeric


def _pairing_embedding_threshold(query_spec: JoinQuerySpec) -> float:
    """Return the threshold used by embedding-prefilter pairing."""
    threshold = query_spec.semantic.threshold
    if threshold is None:
        raise ValueError(
            "sem_join embedding_prefilter pairing requires semantic.threshold in JoinQuerySpec"
        )
    numeric = float(threshold)
    if not 0.0 <= numeric <= 1.0:
        raise ValueError(
            "sem_join embedding_prefilter threshold must be within [0.0, 1.0]"
        )
    return numeric


def _payload_text(payload: Any) -> str:
    if isinstance(payload, dict):
        for field_name in ("payload", "text", "content", "message"):
            if field_name in payload:
                return str(payload[field_name])
        return " ".join(str(value) for value in payload.values())
    return str(payload)


def _blocking_key(payload: Any) -> str:
    normalized = _payload_text(payload).strip().lower()
    if not normalized:
        return ""
    return normalized.split()[0]


def _build_candidate_pairs(
    *,
    left_records: Sequence[Dict[str, Any]],
    right_records: Sequence[Dict[str, Any]],
    kernel_config: SemJoinConfig,
    pairing_method: str,
    query_spec: JoinQuerySpec,
) -> List[Dict[str, Any]]:
    if not left_records or not right_records:
        return []

    if pairing_method == PAIRING_METHOD_BRUTE_FORCE:
        return [
            {
                "left_record_id": str(left["record_id"]),
                "right_record_id": str(right["record_id"]),
                "left": left["payload"],
                "right": right["payload"],
            }
            for left in left_records
            for right in right_records
        ]

    if pairing_method == PAIRING_METHOD_BLOCKING:
        right_index: Dict[str, List[Dict[str, Any]]] = {}
        for right in right_records:
            right_index.setdefault(_blocking_key(right["payload"]), []).append(right)
        pairs: List[Dict[str, Any]] = []
        for left in left_records:
            candidates = right_index.get(_blocking_key(left["payload"]), [])
            for right in candidates:
                pairs.append(
                    {
                        "left_record_id": str(left["record_id"]),
                        "right_record_id": str(right["record_id"]),
                        "left": left["payload"],
                        "right": right["payload"],
                    }
                )
        return pairs

    if (
        pairing_method == PAIRING_METHOD_CANDIDATE_PRUNED
        and kernel_config.prefilter_strategy == "none"
    ):
        return [
            {
                "left_record_id": str(left["record_id"]),
                "right_record_id": str(right["record_id"]),
                "left": left["payload"],
                "right": right["payload"],
            }
            for left in left_records
            for right in right_records
        ]

    encoder = HashingTextEncoder(dim=EMBEDDING_SIMILARITY_DIM)
    threshold = (
        _pairing_embedding_threshold(query_spec)
        if pairing_method == PAIRING_METHOD_EMBEDDING_PREFILTER
        else float(kernel_config.prefilter_min_score)
    )
    pairs: List[Dict[str, Any]] = []
    for left in left_records:
        left_payload = left["payload"]
        left_text = _payload_text(left_payload)
        for right in right_records:
            right_payload = right["payload"]
            right_text = _payload_text(right_payload)
            similarity = encoder.similarity(left_text, right_text)
            if similarity < threshold:
                continue
            pairs.append(
                {
                    "left_record_id": str(left["record_id"]),
                    "right_record_id": str(right["record_id"]),
                    "left": left_payload,
                    "right": right_payload,
                }
            )
    return pairs


def _pair_blocks(
    pairs: Sequence[Dict[str, Any]],
    *,
    pair_block_size: int,
) -> Iterable[Sequence[Dict[str, Any]]]:
    for start in range(0, len(pairs), pair_block_size):
        yield pairs[start : start + pair_block_size]


def _evaluate_pairs_with_llm(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    query_spec: JoinQuerySpec,
    kernel_config: SemJoinConfig,
    pair_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    outputs: List[Dict[str, Any]] = []
    for pair_block in _pair_blocks(pair_rows, pair_block_size=kernel_config.pair_block_size):
        matches = evaluate_sem_match_block_sync(
            client=client,
            llm_config=llm_config,
            intent=query_spec.semantic.instruction,
            pair_block=list(pair_block),
        )
        for match in matches:
            outputs.append(
                {
                    "pair_idx": int(match["pair_idx"]),
                    "matched": bool(match["matched"]),
                    "match_score": float(match["match_score"]),
                    "reason": str(match["reason"]),
                    "block_size": len(pair_block),
                }
            )
    return outputs


def _evaluate_pairs_with_embedding(
    *,
    pair_rows: Sequence[Dict[str, Any]],
    threshold: float,
) -> List[Dict[str, Any]]:
    encoder = HashingTextEncoder(dim=EMBEDDING_SIMILARITY_DIM)
    outputs: List[Dict[str, Any]] = []
    for pair_idx, pair in enumerate(pair_rows):
        score = float(encoder.similarity(str(pair["left"]), str(pair["right"])))
        outputs.append(
            {
                "pair_idx": pair_idx,
                "matched": bool(score >= threshold),
                "match_score": score,
                "reason": "embedding_similarity",
                "block_size": len(pair_rows),
            }
        )
    return outputs


class SemJoinFunction(KeyedCoProcessFunction):
    """Native keyed two-input continuous semantic join runtime."""

    def __init__(
        self,
        query_spec: JoinQuerySpec,
        llm_config: LLMClientConfig,
        kernel_config: SemJoinConfig,
    ) -> None:
        self._query_spec = query_spec
        self._llm_config = llm_config
        self._kernel_config = kernel_config
        self._use_event_time = query_spec.scope_policy.time_basis == "event"

        self._client: Optional[LLMClient] = None
        self._left_buffer = None
        self._right_buffer = None
        self._left_seen_seq = None
        self._right_seen_seq = None

        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._pending_futures: Dict[str, concurrent.futures.Future] = {}
        self._pending_payloads: Dict[str, Dict[str, Any]] = {}
        self._pending_by_key: Dict[str, List[str]] = {}

    def open(self, runtime_context: RuntimeContext) -> None:
        _validate_sem_join_backend(self._query_spec)
        _validate_sem_join_trigger(self._query_spec)
        if self._query_spec.semantic.backend in {"llm", "hybrid"}:
            self._client = create_llm_client(self._llm_config)
        self._left_buffer = runtime_context.get_list_state(
            sem_join_left_buffer_descriptor(self._kernel_config.ttl_seconds)
        )
        self._right_buffer = runtime_context.get_list_state(
            sem_join_right_buffer_descriptor(self._kernel_config.ttl_seconds)
        )
        self._left_seen_seq = runtime_context.get_map_state(
            sem_join_left_seen_seq_descriptor(self._kernel_config.ttl_seconds)
        )
        self._right_seen_seq = runtime_context.get_map_state(
            sem_join_right_seen_seq_descriptor(self._kernel_config.ttl_seconds)
        )
        if self._query_spec.pairing_method not in {
            PAIRING_METHOD_CANDIDATE_PRUNED,
            PAIRING_METHOD_EMBEDDING_PREFILTER,
            PAIRING_METHOD_BLOCKING,
            PAIRING_METHOD_BRUTE_FORCE,
        }:
            raise ValueError(
                f"sem_join unsupported pairing_method={self._query_spec.pairing_method!r}"
            )

    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None
        if self._client is not None:
            self._client.close()
            self._client = None

    def process_element1(
        self,
        value: Any,
        ctx: "KeyedCoProcessFunction.Context",
    ) -> Iterable[Dict[str, Any]]:
        return self._process_arrival(value=value, side=LEFT_SIDE, ctx=ctx)

    def process_element2(
        self,
        value: Any,
        ctx: "KeyedCoProcessFunction.Context",
    ) -> Iterable[Dict[str, Any]]:
        return self._process_arrival(value=value, side=RIGHT_SIDE, ctx=ctx)

    def on_timer(
        self,
        timestamp: int,
        ctx: "KeyedCoProcessFunction.OnTimerContext",
    ) -> Iterable[Dict[str, Any]]:
        key = self._resolve_current_key(ctx=ctx, value=None)
        now_ms = int(timestamp)
        timer_service = ctx.timer_service()
        cutoff_ms = self._retention_cutoff_ms(timer_service=timer_service, now_ms=now_ms)
        left_buffer = self._load_buffer(self._left_buffer)
        right_buffer = self._load_buffer(self._right_buffer)
        outputs = self._poll_pending_results(key=key, now_ms=now_ms, left_buffer=left_buffer, right_buffer=right_buffer)
        left_buffer, right_buffer, prune_outputs = self._prune_and_finalize(
            key=key,
            now_ms=now_ms,
            cutoff_ms=cutoff_ms,
            left_buffer=left_buffer,
            right_buffer=right_buffer,
        )
        outputs.extend(prune_outputs)
        self._store_buffer(self._left_buffer, left_buffer)
        self._store_buffer(self._right_buffer, right_buffer)
        if self._pending_count(key) > 0:
            timer_service.register_processing_time_timer(
                now_ms + self._kernel_config.async_poll_interval_ms
            )
        return outputs

    def _process_arrival(
        self,
        *,
        value: Any,
        side: str,
        ctx: "KeyedCoProcessFunction.Context",
    ) -> List[Dict[str, Any]]:
        key = self._resolve_current_key(ctx=ctx, value=value)
        now_ms = self._current_time_ms(ctx)
        timer_service = ctx.timer_service()
        cutoff_ms = self._retention_cutoff_ms(timer_service=timer_service, now_ms=now_ms)

        left_buffer = self._load_buffer(self._left_buffer)
        right_buffer = self._load_buffer(self._right_buffer)

        outputs = self._poll_pending_results(
            key=key,
            now_ms=now_ms,
            left_buffer=left_buffer,
            right_buffer=right_buffer,
        )

        ingress_payloads = self._normalize_ingress_payloads(
            value=value,
            side=side,
            now_ms=now_ms,
        )
        for payload in ingress_payloads:
            if side == LEFT_SIDE:
                if self._pending_count(key) > 0 and len(left_buffer) >= self._kernel_config.max_left_buffer:
                    raise RuntimeError(
                        "sem_join left buffer reached max_left_buffer while async requests are in-flight"
                    )
                left_record = self._new_record(payload=payload, observed_at_ms=now_ms, side=LEFT_SIDE)
                self._register_event_time_finalize_timer(timer_service=timer_service, record=left_record)
                right_records = self._prune_expired(right_buffer, cutoff_ms)
                left_buffer = self._append_bounded(
                    left_buffer,
                    left_record,
                    max_size=self._kernel_config.max_left_buffer,
                )
                pair_rows = _build_candidate_pairs(
                    left_records=[left_record],
                    right_records=right_records,
                    kernel_config=self._kernel_config,
                    pairing_method=self._query_spec.pairing_method,
                    query_spec=self._query_spec,
                )
                right_buffer = right_records
            else:
                if self._pending_count(key) > 0 and len(right_buffer) >= self._kernel_config.max_right_buffer:
                    raise RuntimeError(
                        "sem_join right buffer reached max_right_buffer while async requests are in-flight"
                    )
                right_record = self._new_record(payload=payload, observed_at_ms=now_ms, side=RIGHT_SIDE)
                self._register_event_time_finalize_timer(timer_service=timer_service, record=right_record)
                left_records = self._prune_expired(left_buffer, cutoff_ms)
                right_buffer = self._append_bounded(
                    right_buffer,
                    right_record,
                    max_size=self._kernel_config.max_right_buffer,
                )
                pair_rows = _build_candidate_pairs(
                    left_records=left_records,
                    right_records=[right_record],
                    kernel_config=self._kernel_config,
                    pairing_method=self._query_spec.pairing_method,
                    query_spec=self._query_spec,
                )
                left_buffer = left_records

            if not pair_rows or self._query_spec.trigger_policy.mode != "on_event":
                continue
            outputs.extend(
                self._dispatch_or_evaluate(
                    key=key,
                    now_ms=now_ms,
                    pair_rows=pair_rows,
                    left_buffer=left_buffer,
                    right_buffer=right_buffer,
                )
            )

        left_buffer, right_buffer, prune_outputs = self._prune_and_finalize(
            key=key,
            now_ms=now_ms,
            cutoff_ms=cutoff_ms,
            left_buffer=left_buffer,
            right_buffer=right_buffer,
        )
        outputs.extend(prune_outputs)

        self._store_buffer(self._left_buffer, left_buffer)
        self._store_buffer(self._right_buffer, right_buffer)
        if self._pending_count(key) > 0:
            timer_service.register_processing_time_timer(
                now_ms + self._kernel_config.async_poll_interval_ms
            )
        return outputs

    def _new_record(
        self,
        *,
        payload: Any,
        observed_at_ms: int,
        side: str,
    ) -> Dict[str, Any]:
        event_time_ms = self._extract_event_time_ms(payload)
        if self._use_event_time and event_time_ms is None:
            raise ValueError(
                "sem_join time_basis='event' requires event_time_ms/timestamp_ms/proc_time_ms in payload"
            )
        return {
            "record_id": _record_id(side),
            "payload": payload,
            "_observed_at_ms": int(observed_at_ms),
            "_event_time_ms": int(event_time_ms) if event_time_ms is not None else None,
            "matched_count": 0,
            "semi_emitted": False,
            "close_eval_dispatched": False,
        }

    def _normalize_ingress_payloads(
        self,
        *,
        value: Any,
        side: str,
        now_ms: int,
    ) -> List[Any]:
        candidate_value: Any = value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{"):
                candidate_value = parse_json_or_passthrough(value, operator_name="sem_join")

        if not isinstance(candidate_value, dict) or not is_window_snapshot(candidate_value):
            seq_id = self._extract_seq_id(candidate_value)
            if not self._mark_seq_seen(side=side, seq_id=seq_id, now_ms=now_ms):
                return []
            return [_payload_for_join(candidate_value)]

        snapshot = parse_window_snapshot(candidate_value, operator_name="sem_join")
        unseen_payloads: List[Any] = []
        for event_dict in window_snapshot_to_sem_events(snapshot):
            seq_id = int(event_dict.get("seq_id", 0))
            if not self._mark_seq_seen(side=side, seq_id=seq_id, now_ms=now_ms):
                continue
            unseen_payloads.append(_payload_for_join(event_dict))
        return unseen_payloads

    def _dispatch_or_evaluate(
        self,
        *,
        key: str,
        now_ms: int,
        pair_rows: Sequence[Dict[str, Any]],
        left_buffer: List[Dict[str, Any]],
        right_buffer: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        backend = self._query_spec.semantic.backend
        if backend == "embedding":
            decisions = _evaluate_pairs_with_embedding(
                pair_rows=pair_rows,
                threshold=_embedding_threshold(self._query_spec),
            )
            return self._apply_decisions(
                pair_rows=pair_rows,
                decisions=decisions,
                left_buffer=left_buffer,
                right_buffer=right_buffer,
            )

        if self._executor is None:
            self._executor = ensure_thread_pool_executor(
                self._executor,
                max_workers=self._kernel_config.async_max_workers,
                thread_name_prefix="sem-join",
            )
        if self._client is None:
            raise RuntimeError("sem_join LLM backend requires initialized client")

        request_id = f"{key}:{uuid.uuid4().hex}"
        future = self._executor.submit(
            _evaluate_pairs_with_llm,
            client=self._client,
            llm_config=self._llm_config,
            query_spec=self._query_spec,
            kernel_config=self._kernel_config,
            pair_rows=list(pair_rows),
        )
        self._pending_futures[request_id] = future
        self._pending_payloads[request_id] = {
            "key": key,
            "pair_rows": list(pair_rows),
            "created_at_ms": int(now_ms),
        }
        self._pending_by_key.setdefault(key, []).append(request_id)
        return []

    def _poll_pending_results(
        self,
        *,
        key: str,
        now_ms: int,
        left_buffer: List[Dict[str, Any]],
        right_buffer: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        _ = now_ms
        outputs: List[Dict[str, Any]] = []
        pending_ids = list(self._pending_by_key.get(key, []))
        if not pending_ids:
            return outputs
        remaining_ids: List[str] = []
        for request_id in pending_ids:
            future = self._pending_futures.get(request_id)
            payload = self._pending_payloads.get(request_id)
            if future is None or payload is None:
                raise RuntimeError(f"sem_join lost pending request state for {request_id!r}")
            if not future.done():
                remaining_ids.append(request_id)
                continue
            decisions = future.result()
            pair_rows = payload.get("pair_rows")
            if not isinstance(pair_rows, list):
                raise RuntimeError("sem_join pending payload missing pair_rows")
            outputs.extend(
                self._apply_decisions(
                    pair_rows=pair_rows,
                    decisions=decisions,
                    left_buffer=left_buffer,
                    right_buffer=right_buffer,
                )
            )
            del self._pending_futures[request_id]
            del self._pending_payloads[request_id]
        if remaining_ids:
            self._pending_by_key[key] = remaining_ids
        else:
            self._pending_by_key.pop(key, None)
        return outputs

    def _apply_decisions(
        self,
        *,
        pair_rows: Sequence[Dict[str, Any]],
        decisions: Sequence[Dict[str, Any]],
        left_buffer: List[Dict[str, Any]],
        right_buffer: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        left_index = {str(item["record_id"]): item for item in left_buffer}
        right_index = {str(item["record_id"]): item for item in right_buffer}
        outputs: List[Dict[str, Any]] = []
        join_type = self._query_spec.join_type
        for decision in decisions:
            pair_idx = int(decision["pair_idx"])
            if not bool(decision["matched"]):
                continue
            if pair_idx < 0 or pair_idx >= len(pair_rows):
                raise ValueError(f"sem_join matcher returned invalid pair_idx={pair_idx}")
            pair = pair_rows[pair_idx]
            left_record = left_index.get(str(pair["left_record_id"]))
            right_record = right_index.get(str(pair["right_record_id"]))
            if left_record is None or right_record is None:
                continue
            left_record["matched_count"] = int(left_record.get("matched_count", 0)) + 1
            right_record["matched_count"] = int(right_record.get("matched_count", 0)) + 1
            score = float(decision["match_score"])
            reason = str(decision["reason"])

            if join_type in {"inner", "left", "right", "full"}:
                outputs.append(
                    self._matched_row(
                        left=pair["left"],
                        right=pair["right"],
                        score=score,
                        reason=reason,
                    )
                )
            elif join_type == "semi":
                if not bool(left_record.get("semi_emitted", False)):
                    left_record["semi_emitted"] = True
                    outputs.append(
                        self._semi_row(
                            left=pair["left"],
                            score=score,
                            reason=reason,
                        )
                    )
            elif join_type == "anti":
                continue
            else:
                raise ValueError(f"sem_join received unsupported join_type {join_type!r}")
        return outputs

    def _matched_row(
        self,
        *,
        left: Any,
        right: Any,
        score: float,
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "join_type": self._query_spec.join_type,
            "matched": True,
            "left": left,
            "right": right,
            "match_score": float(score),
            "reason": reason,
        }

    def _semi_row(
        self,
        *,
        left: Any,
        score: float,
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "join_type": self._query_spec.join_type,
            "matched": True,
            "left": left,
            "right": None,
            "match_score": float(score),
            "reason": reason,
        }

    def _unmatched_row(
        self,
        *,
        side: str,
        payload: Any,
    ) -> Dict[str, Any]:
        return {
            "join_type": self._query_spec.join_type,
            "matched": False,
            "left": payload if side == LEFT_SIDE else None,
            "right": payload if side == RIGHT_SIDE else None,
            "match_score": 0.0,
            "reason": f"unmatched_{side}_finalized",
        }

    def _prune_and_finalize(
        self,
        *,
        key: str,
        now_ms: int,
        cutoff_ms: Optional[int],
        left_buffer: List[Dict[str, Any]],
        right_buffer: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        if self._pending_count(key) > 0:
            return left_buffer, right_buffer, []
        if cutoff_ms is None:
            return left_buffer, right_buffer, []
        if self._query_spec.trigger_policy.mode == "on_scope_close":
            return self._prune_and_finalize_on_scope_close(
                key=key,
                now_ms=now_ms,
                cutoff_ms=cutoff_ms,
                left_buffer=left_buffer,
                right_buffer=right_buffer,
            )
        return self._prune_and_finalize_on_event(
            key=key,
            cutoff_ms=cutoff_ms,
            left_buffer=left_buffer,
            right_buffer=right_buffer,
        )

    def _prune_and_finalize_on_event(
        self,
        *,
        key: str,
        cutoff_ms: int,
        left_buffer: List[Dict[str, Any]],
        right_buffer: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        _ = key

        left_outputs: List[Dict[str, Any]] = []
        right_outputs: List[Dict[str, Any]] = []
        kept_left: List[Dict[str, Any]] = []
        kept_right: List[Dict[str, Any]] = []

        join_type = self._query_spec.join_type

        for record in left_buffer:
            record_time_ms = self._record_time_ms(record)
            if record_time_ms >= cutoff_ms:
                kept_left.append(record)
                continue
            if int(record.get("matched_count", 0)) > 0:
                continue
            if join_type in LEFT_JOIN_FAMILY or join_type == "anti":
                left_outputs.append(self._unmatched_row(side=LEFT_SIDE, payload=record["payload"]))

        for record in right_buffer:
            record_time_ms = self._record_time_ms(record)
            if record_time_ms >= cutoff_ms:
                kept_right.append(record)
                continue
            if int(record.get("matched_count", 0)) > 0:
                continue
            if join_type in RIGHT_JOIN_FAMILY:
                right_outputs.append(self._unmatched_row(side=RIGHT_SIDE, payload=record["payload"]))

        return kept_left, kept_right, left_outputs + right_outputs

    def _prune_and_finalize_on_scope_close(
        self,
        *,
        key: str,
        now_ms: int,
        cutoff_ms: int,
        left_buffer: List[Dict[str, Any]],
        right_buffer: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        expired_left = [
            record for record in left_buffer if self._record_time_ms(record) < cutoff_ms
        ]
        expired_right = [
            record for record in right_buffer if self._record_time_ms(record) < cutoff_ms
        ]
        kept_left = [
            record for record in left_buffer if self._record_time_ms(record) >= cutoff_ms
        ]
        kept_right = [
            record for record in right_buffer if self._record_time_ms(record) >= cutoff_ms
        ]
        if not expired_left and not expired_right:
            return left_buffer, right_buffer, []

        dispatch_left = [
            record for record in expired_left if not bool(record.get("close_eval_dispatched", False))
        ]
        dispatch_right = [
            record for record in expired_right if not bool(record.get("close_eval_dispatched", False))
        ]
        pair_rows: List[Dict[str, Any]] = []
        if dispatch_left:
            pair_rows.extend(
                _build_candidate_pairs(
                    left_records=dispatch_left,
                    right_records=right_buffer,
                    kernel_config=self._kernel_config,
                    pairing_method=self._query_spec.pairing_method,
                    query_spec=self._query_spec,
                )
            )
        if dispatch_right:
            pair_rows.extend(
                _build_candidate_pairs(
                    left_records=kept_left,
                    right_records=dispatch_right,
                    kernel_config=self._kernel_config,
                    pairing_method=self._query_spec.pairing_method,
                    query_spec=self._query_spec,
                )
            )
        for record in dispatch_left:
            record["close_eval_dispatched"] = True
        for record in dispatch_right:
            record["close_eval_dispatched"] = True

        outputs: List[Dict[str, Any]] = []
        if pair_rows:
            outputs.extend(
                self._dispatch_or_evaluate(
                    key=key,
                    now_ms=now_ms,
                    pair_rows=pair_rows,
                    left_buffer=left_buffer,
                    right_buffer=right_buffer,
                )
            )
            if self._pending_count(key) > 0:
                return left_buffer, right_buffer, outputs

        join_type = self._query_spec.join_type
        for record in expired_left:
            if int(record.get("matched_count", 0)) > 0:
                continue
            if join_type in LEFT_JOIN_FAMILY or join_type == "anti":
                outputs.append(self._unmatched_row(side=LEFT_SIDE, payload=record["payload"]))
        for record in expired_right:
            if int(record.get("matched_count", 0)) > 0:
                continue
            if join_type in RIGHT_JOIN_FAMILY:
                outputs.append(self._unmatched_row(side=RIGHT_SIDE, payload=record["payload"]))
        return kept_left, kept_right, outputs

    def _pending_count(self, key: str) -> int:
        return len(self._pending_by_key.get(key, []))

    def _resolve_current_key(self, *, ctx: Any, value: Optional[Any]) -> str:
        if hasattr(ctx, "get_current_key"):
            return str(ctx.get_current_key())
        if isinstance(value, dict) and "key" in value:
            return str(value["key"])
        if len(self._pending_by_key) == 1:
            only_key = next(iter(self._pending_by_key.keys()))
            return str(only_key)
        return ""

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

    def _store_buffer(self, state_handle, items: Sequence[Dict[str, Any]]) -> None:
        assert state_handle is not None
        state_handle.clear()
        for item in items:
            state_handle.add(dict(item))

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
        items: Sequence[Dict[str, Any]],
        cutoff_ms: Optional[int],
    ) -> List[Dict[str, Any]]:
        if cutoff_ms is None:
            return [dict(item) for item in items]
        return [
            item
            for item in items
            if self._record_time_ms(item) >= cutoff_ms
        ]

    def _extract_event_time_ms(self, payload: Any) -> Optional[int]:
        if not isinstance(payload, dict):
            return None
        for field_name in ("event_time_ms", "timestamp_ms", "proc_time_ms"):
            raw_value = payload.get(field_name)
            if raw_value is None:
                continue
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                continue
        return None

    def _extract_seq_id(self, payload: Any) -> Optional[int]:
        if not isinstance(payload, dict):
            return None
        raw_seq_id = payload.get("seq_id")
        if raw_seq_id is None:
            return None
        try:
            seq_id = int(raw_seq_id)
        except (TypeError, ValueError):
            return None
        if seq_id <= 0:
            return None
        return seq_id

    def _mark_seq_seen(self, *, side: str, seq_id: Optional[int], now_ms: int) -> bool:
        if seq_id is None:
            return True
        seen_state = self._left_seen_seq if side == LEFT_SIDE else self._right_seen_seq
        if seen_state is None:
            raise RuntimeError("sem_join seen-seq state is not initialized")
        if seen_state.contains(seq_id):
            return False
        seen_state.put(seq_id, int(now_ms))
        return True

    def _record_time_ms(self, record: Dict[str, Any]) -> int:
        if self._use_event_time:
            raw_event_time_ms = record.get("_event_time_ms")
            if raw_event_time_ms is None:
                raise RuntimeError(
                    "sem_join event-time record is missing _event_time_ms during finalize"
                )
            return int(raw_event_time_ms)
        return int(record.get("_observed_at_ms", 0))

    def _retention_cutoff_ms(
        self,
        *,
        timer_service: Any,
        now_ms: int,
    ) -> Optional[int]:
        ttl_ms = self._kernel_config.ttl_seconds * 1000
        if not self._use_event_time:
            return int(now_ms - ttl_ms)
        if not hasattr(timer_service, "current_watermark"):
            raise RuntimeError("sem_join event-time mode requires timer_service.current_watermark()")
        watermark_ms = int(timer_service.current_watermark())
        if watermark_ms <= UNKNOWN_WATERMARK:
            return None
        return int(watermark_ms - ttl_ms)

    def _register_event_time_finalize_timer(
        self,
        *,
        timer_service: Any,
        record: Dict[str, Any],
    ) -> None:
        if not self._use_event_time:
            return
        event_time_ms = record.get("_event_time_ms")
        if event_time_ms is None:
            raise RuntimeError("sem_join event-time record missing _event_time_ms")
        if not hasattr(timer_service, "register_event_time_timer"):
            raise RuntimeError("sem_join event-time mode requires register_event_time_timer")
        fire_at_ms = int(event_time_ms) + self._kernel_config.ttl_seconds * 1000
        timer_service.register_event_time_timer(fire_at_ms)


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
