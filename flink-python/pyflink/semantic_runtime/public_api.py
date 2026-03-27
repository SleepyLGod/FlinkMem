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

"""Public semantic facade.

This module defines the intended user-facing API for semantic operators.
It captures only semantic intent and business context.

Lowering into runtime-specific specs, triggers, paths, and backend configs is
an internal concern handled elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


VALID_CONTEXT_KINDS = {
    "record",
    "stream",
    "window",
    "session",
    "semantic_segment",
}
VALID_AGG_MODES = {"algebraic", "summarize", "compressive"}
VALID_JOIN_TYPES = {"inner", "left", "right", "full", "semi", "anti"}


@dataclass(frozen=True)
class SemContext:
    """Public business context for semantic operations.

    Attributes:
        kind: Business boundary kind.
        metadata: Optional user-facing context metadata.
    """

    kind: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind not in VALID_CONTEXT_KINDS:
            raise ValueError(
                f"Invalid context kind {self.kind!r}. "
                f"Expected one of {sorted(VALID_CONTEXT_KINDS)!r}."
            )


@dataclass(frozen=True)
class SemMapRequest:
    """Public semantic map request."""

    intent: str
    output_schema: Optional[Dict[str, Any]] = None
    output_mode: str = "json"


@dataclass(frozen=True)
class SemFilterRequest:
    """Public semantic filter request."""

    intent: str


@dataclass(frozen=True)
class SemLocalTopKRequest:
    """Public row-level local top-k request."""

    intent: str
    k: int

    def __post_init__(self) -> None:
        if self.k <= 0:
            raise ValueError("sem_local_topk requires k > 0")


@dataclass(frozen=True)
class SemLookupJoinRequest:
    """Public row-level lookup join request."""

    intent: str
    candidate_source: Any


@dataclass(frozen=True)
class SemWindowRequest:
    """Public semantic window request."""

    context: SemContext


@dataclass(frozen=True)
class SemTopKRequest:
    """Public stateful semantic top-k request."""

    intent: str
    k: int
    context: SemContext

    def __post_init__(self) -> None:
        if self.k <= 0:
            raise ValueError("sem_topk requires k > 0")


@dataclass(frozen=True)
class SemGroupbyRequest:
    """Public stateful semantic groupby request."""

    intent: str
    context: SemContext


@dataclass(frozen=True)
class SemAggRequest:
    """Public stateful semantic aggregation request."""

    intent: str
    mode: str
    context: SemContext

    def __post_init__(self) -> None:
        if self.mode not in VALID_AGG_MODES:
            raise ValueError(
                f"Invalid sem_agg mode {self.mode!r}. "
                f"Expected one of {sorted(VALID_AGG_MODES)!r}."
            )


@dataclass(frozen=True)
class SemJoinRequest:
    """Public stateful semantic join request."""

    intent: str
    context: SemContext
    right_input: Any
    join_type: str = "inner"

    def __post_init__(self) -> None:
        if self.join_type not in VALID_JOIN_TYPES:
            raise ValueError(
                f"Invalid sem_join join_type {self.join_type!r}. "
                f"Expected one of {sorted(VALID_JOIN_TYPES)!r}."
            )


def context(kind: str, **metadata: Any) -> SemContext:
    """Create a public semantic context."""
    return SemContext(kind=kind, metadata=dict(metadata))


def sem_map(
    *,
    intent: str,
    output_schema: Optional[Dict[str, Any]] = None,
    output_mode: str = "json",
) -> SemMapRequest:
    """Create a public semantic map request."""
    return SemMapRequest(intent=intent, output_schema=output_schema, output_mode=output_mode)


def sem_filter(*, intent: str) -> SemFilterRequest:
    """Create a public semantic filter request."""
    return SemFilterRequest(intent=intent)


def sem_local_topk(*, intent: str, k: int) -> SemLocalTopKRequest:
    """Create a public local semantic top-k request."""
    return SemLocalTopKRequest(intent=intent, k=k)


def sem_lookup_join(*, intent: str, candidate_source: Any) -> SemLookupJoinRequest:
    """Create a public semantic lookup join request."""
    return SemLookupJoinRequest(intent=intent, candidate_source=candidate_source)


def sem_window(*, context: SemContext) -> SemWindowRequest:
    """Create a public semantic window request."""
    return SemWindowRequest(context=context)


def sem_topk(*, intent: str, k: int, context: SemContext) -> SemTopKRequest:
    """Create a public stateful semantic top-k request."""
    return SemTopKRequest(intent=intent, k=k, context=context)


def sem_groupby(*, intent: str, context: SemContext) -> SemGroupbyRequest:
    """Create a public stateful semantic groupby request."""
    return SemGroupbyRequest(intent=intent, context=context)


def sem_agg(*, intent: str, mode: str, context: SemContext) -> SemAggRequest:
    """Create a public stateful semantic aggregation request."""
    return SemAggRequest(intent=intent, mode=mode, context=context)


def sem_join(
    *,
    intent: str,
    context: SemContext,
    right_input: Any,
    join_type: str = "inner",
) -> SemJoinRequest:
    """Create a public stateful semantic join request."""
    return SemJoinRequest(
        intent=intent,
        context=context,
        right_input=right_input,
        join_type=join_type,
    )
