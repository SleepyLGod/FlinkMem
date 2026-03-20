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

"""
Canonical event model and key schema for V0.2 stateful semantic operators.

All V0.2 stateful operators expect events in the :class:`SemanticEvent` shape.
Key extraction helpers are provided to produce stable Flink key-by selectors.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Canonical event payload
# ---------------------------------------------------------------------------

@dataclass
class SemanticEvent:
    """Canonical event payload for V0.2 operators.

    Required fields
    ---------------
    key : str
        Primary keying field (e.g. ``user_id``, ``session_id``, or composite).
    payload : str
        Message body / text content.
    seq_id : int
        Monotonically increasing sequence number within the key partition.
        Used for ordering guarantees and deduplication.

    Time fields
    -----------
    event_time_ms : int | None
        Event-time timestamp in milliseconds since epoch. ``None`` means
        processing-time-only mode.
    proc_time_ms : int
        Processing-time timestamp (auto-filled on creation).

    Optional fields
    ---------------
    metadata : dict
        Arbitrary operator/trace metadata (e.g. ``operator_trace_id``,
        ``prompt_version``).
    candidates : list[dict]
        Pre-retrieved candidate records (for retrieval-backed operators).
    boundary_flags : dict
        Upstream semantic boundary signals (e.g. ``{"topic_shift": True}``).
    """

    key: str
    payload: str
    seq_id: int
    event_time_ms: Optional[int] = None
    proc_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    metadata: Dict[str, Any] = field(default_factory=dict)
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    boundary_flags: Dict[str, bool] = field(default_factory=dict)

    # ---- helpers -----------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain dict (JSON-ready)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SemanticEvent":
        """Deserialise from a plain dict."""
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def has_boundary(self, flag_name: str = "topic_shift") -> bool:
        """Check whether a specific boundary flag is set."""
        return self.boundary_flags.get(flag_name, False)

    @property
    def effective_time_ms(self) -> int:
        """Return event_time_ms if set, otherwise proc_time_ms."""
        return self.event_time_ms if self.event_time_ms is not None else self.proc_time_ms


# ---------------------------------------------------------------------------
# Window snapshot — emitted by sem_window when a boundary is reached
# ---------------------------------------------------------------------------

@dataclass
class WindowSnapshot:
    """Emitted by ``sem_window`` when a window boundary is triggered.

    Contains the accumulated events and metadata about the window.
    """

    key: str
    window_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    events: List[Dict[str, Any]] = field(default_factory=list)
    event_count: int = 0
    open_time_ms: int = 0
    close_time_ms: int = 0
    trigger_reason: str = ""           # "count" | "time" | "semantic_boundary"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WindowSnapshot":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Contract adapters — bridge between operator output shapes
# ---------------------------------------------------------------------------

def is_window_snapshot(d: Dict[str, Any]) -> bool:
    """Return True if *d* looks like a serialised WindowSnapshot."""
    return (
        isinstance(d, dict)
        and "window_id" in d
        and "events" in d
        and "trigger_reason" in d
    )


def window_snapshot_to_semantic_events(
    snap: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Convert a WindowSnapshot dict into a list of SemanticEvent dicts.

    Each original event stored in the snapshot is unwrapped.  If the
    stored events are already SemanticEvent-shaped they are returned
    as-is with ``window_id`` injected into ``metadata``.  Otherwise a
    minimal SemanticEvent is synthesised with the event text as
    ``payload``.

    This is the **Subflow A adapter** that sits between ``sem_window``
    output and ``sem_groupby`` / ``sem_agg`` input.
    """
    events = snap.get("events", [])
    window_id = snap.get("window_id", "")
    key = snap.get("key", "")
    close_time_ms = snap.get("close_time_ms", 0)

    result: List[Dict[str, Any]] = []
    for idx, evt in enumerate(events):
        if isinstance(evt, dict) and "payload" in evt and "seq_id" in evt:
            # Already SemanticEvent-shaped — inject window provenance
            out = dict(evt)
            out.setdefault("metadata", {})
            out["metadata"]["window_id"] = window_id
            out["metadata"]["window_trigger"] = snap.get("trigger_reason", "")
        else:
            # Raw event — wrap into SemanticEvent shape
            out = {
                "key": key,
                "payload": str(evt) if not isinstance(evt, dict)
                           else evt.get("payload", str(evt)),
                "seq_id": idx,
                "event_time_ms": close_time_ms or None,
                "metadata": {
                    "window_id": window_id,
                    "window_trigger": snap.get("trigger_reason", ""),
                },
                "candidates": [],
                "boundary_flags": {},
            }
        result.append(out)
    return result


def window_snapshot_to_summary_event(
    snap: Dict[str, Any],
) -> Dict[str, Any]:
    """Convert a WindowSnapshot dict into a single summary SemanticEvent dict.

    The ``payload`` is a concatenation of all event payloads in the window,
    which is suitable for downstream aggregation operators that expect one
    event per window rather than one event per original message.
    """
    events = snap.get("events", [])
    key = snap.get("key", "")
    window_id = snap.get("window_id", "")

    payloads = []
    for evt in events:
        if isinstance(evt, dict):
            payloads.append(evt.get("payload", str(evt)))
        else:
            payloads.append(str(evt))

    return {
        "key": key,
        "payload": "\n".join(payloads),
        "seq_id": snap.get("event_count", 0),
        "event_time_ms": snap.get("close_time_ms") or None,
        "metadata": {
            "window_id": window_id,
            "window_trigger": snap.get("trigger_reason", ""),
            "source_event_count": len(events),
        },
        "candidates": [],
        "boundary_flags": {},
    }


def group_assignment_to_semantic_event(
    assignment: Dict[str, Any],
) -> Dict[str, Any]:
    """Normalize a sem_groupby assignment into a SemanticEvent dict.

    This adapter is used between ``sem_groupby`` and ``sem_agg`` so the
    aggregation stage always receives a stable event envelope with required
    SemanticEvent fields.
    """
    key = assignment.get("key", "")
    seq_id = int(assignment.get("event_seq_id", assignment.get("seq_id", 0)))
    group_id = assignment.get("group_id", "")
    source = assignment.get("source", "")
    confidence = assignment.get("confidence", 0.0)
    payload = assignment.get(
        "payload",
        f"group_assignment group={group_id} source={source} confidence={confidence}",
    )

    metadata = dict(assignment.get("metadata", {}))
    metadata.update(
        {
            "group_id": group_id,
            "group_source": source,
            "group_confidence": confidence,
            "group_request_id": assignment.get("request_id", ""),
        }
    )

    return {
        "key": key,
        "payload": str(payload),
        "seq_id": seq_id,
        "event_time_ms": assignment.get("event_time_ms"),
        "metadata": metadata,
        "candidates": assignment.get("candidates", []),
        "boundary_flags": assignment.get("boundary_flags", {}),
    }


def retrieve_to_topk_items(
    retrieve_output: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Expand a ``cts_retrieve`` output into individual candidate dicts
    suitable for ``sem_topk``'s ``process_element``.

    Each candidate dict gets a ``candidate_id`` field (required by
    ``SemTopKFunction``) and inherits ``key`` and ``query_seq_id``
    from the retrieval envelope.
    """
    candidates = retrieve_output.get("candidates", [])
    key = retrieve_output.get("key", "")
    query_seq_id = retrieve_output.get("query_seq_id", 0)
    # Envelope-level metadata to propagate to each expanded candidate
    source = retrieve_output.get("source", "")
    query = retrieve_output.get("query", "")
    error = retrieve_output.get("error", "")

    result: List[Dict[str, Any]] = []
    for cand in candidates:
        out = dict(cand)
        # Ensure candidate_id exists
        if "candidate_id" not in out:
            out["candidate_id"] = out.get("id", f"{key}_{len(result)}")
        out["key"] = key
        out["query_seq_id"] = query_seq_id
        out.setdefault("source", source)
        out.setdefault("query", query)
        out.setdefault("error", error)
        result.append(out)
    return result


def retrieve_to_answer_context(
    retrieve_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Normalize cts_retrieve output for answer synthesis path."""
    items = retrieve_output.get("candidates", [])
    return {
        "key": retrieve_output.get("key", ""),
        "query": retrieve_output.get("query", ""),
        "query_seq_id": retrieve_output.get("query_seq_id", 0),
        "retrieved_context": items,
        "total_candidates": retrieve_output.get("candidate_count", len(items)),
        "truncated": bool(retrieve_output.get("truncated", False)),
        "retrieval_changed": True,
        "source": retrieve_output.get("source", ""),
    }


def topk_to_answer_context(
    topk_output: Dict[str, Any],
    query_payload: str = "",
) -> Dict[str, Any]:
    """Normalise ``sem_topk`` output into the shape ``AnswerSynthesiser``
    expects: ``retrieved_context`` list + ``query`` string.
    """
    items = topk_output.get("top_items", topk_output.get("topk", []))
    return {
        "key": topk_output.get("key", ""),
        "query": query_payload or topk_output.get("query", ""),
        "query_seq_id": topk_output.get("query_seq_id", 0),
        "retrieved_context": items,
        "total_candidates": topk_output.get("total_candidates", len(items)),
        "truncated": bool(topk_output.get("truncated", False)),
        "retrieval_changed": topk_output.get("changed", True),
        "source": topk_output.get("source", ""),
    }


# ---------------------------------------------------------------------------
# Key selector helpers
# ---------------------------------------------------------------------------

def simple_key_selector(event_dict: Dict[str, Any]) -> str:
    """Extract the ``key`` field from a dict-shaped event."""
    return event_dict.get("key", "")


def composite_key_selector(*fields: str):
    """Return a key selector that concatenates the given fields with ``|``."""
    def _select(event_dict: Dict[str, Any]) -> str:
        return "|".join(str(event_dict.get(f, "")) for f in fields)
    return _select
