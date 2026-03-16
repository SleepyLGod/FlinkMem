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

