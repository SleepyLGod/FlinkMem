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
Reusable timer policy for V0.2 stateful semantic operators.

This module provides a uniform timer registration and dispatch pattern
used by ``sem_window``, ``sem_groupby``, ``sem_agg``, ``sem_search``,
and ``sem_topk``.

Timer callback safety rules
----------------------------
- **Allowed** in timer callbacks: local state reads/writes, sorting,
  truncation, eviction, metric updates, emitting to main/side output.
- **Not allowed** in timer callbacks: blocking or async LLM calls.
- Any LLM-required work discovered during a timer callback must be
  emitted as a side-output work item for async processing.

Usage
-----
Subclass or compose with ``TimerCallbackMixin`` to get standard
``register_*`` helpers and a ``dispatch_timer`` method that routes
timer firings to the correct handler based on a namespace prefix
encoded in the timer's metadata state.
"""

from __future__ import annotations

import enum
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Timer categories
# ---------------------------------------------------------------------------

class TimerCategory(enum.Enum):
    """Logical timer categories used across V0.2 operators."""
    FLUSH = "flush"             # emit buffered state (e.g. window snapshot)
    RECOMPUTE = "recompute"     # re-evaluate ranking / aggregation
    EVICT = "evict"             # remove stale state entries


# ---------------------------------------------------------------------------
# Timer policy configuration
# ---------------------------------------------------------------------------

@dataclass
class TimerPolicy:
    """Per-operator timer policy configuration.

    Each field is the interval in milliseconds.  A value of ``0`` means
    the corresponding timer category is disabled for this operator.
    """
    flush_interval_ms: int = 30_000        # default 30s flush
    recompute_interval_ms: int = 0         # disabled by default
    evict_interval_ms: int = 60_000        # default 60s eviction sweep
    use_event_time: bool = False           # False = processing-time


# ---------------------------------------------------------------------------
# Timer metadata helpers
# ---------------------------------------------------------------------------

# Timer namespace encoding: we use a compact dict stored in ValueState
# alongside each operator's metadata.  The dict maps
#   timer_category_name -> registered_timestamp
# This allows dispatch_timer to figure out which handler to call.

def encode_timer_key(category: TimerCategory) -> str:
    """Return a state-key for the timer category."""
    return f"_timer_{category.value}"


def register_timer(
    timer_service,
    meta: Dict[str, Any],
    category: TimerCategory,
    fire_at_ms: int,
    use_event_time: bool = False,
) -> None:
    """Register a timer and record it in the metadata dict.

    Parameters
    ----------
    timer_service : TimerService
        From ``ctx.timer_service()``.
    meta : dict
        Mutable operator metadata dict (will be updated in-place).
    category : TimerCategory
        Logical category of this timer.
    fire_at_ms : int
        Absolute timestamp (ms) at which the timer should fire.
    use_event_time : bool
        If True, register as event-time timer; otherwise processing-time.
    """
    tkey = encode_timer_key(category)
    if use_event_time:
        timer_service.register_event_time_timer(fire_at_ms)
    else:
        timer_service.register_processing_time_timer(fire_at_ms)
    meta[tkey] = fire_at_ms


def resolve_timer_category(
    meta: Dict[str, Any], fired_timestamp: int, tolerance_ms: int = 200
) -> Optional[TimerCategory]:
    """Determine which category a fired timer belongs to.

    Matches ``fired_timestamp`` against registered timestamps in *meta*
    within ±tolerance_ms.  Returns ``None`` if no match (stale timer).
    """
    for cat in TimerCategory:
        tkey = encode_timer_key(cat)
        registered = meta.get(tkey, 0)
        if registered and abs(registered - fired_timestamp) <= tolerance_ms:
            return cat
    return None


def clear_timer_registration(meta: Dict[str, Any], category: TimerCategory) -> None:
    """Remove the timer registration from metadata after it fires."""
    tkey = encode_timer_key(category)
    meta.pop(tkey, None)


# ---------------------------------------------------------------------------
# Convenience: schedule all enabled timers from a policy
# ---------------------------------------------------------------------------

def schedule_policy_timers(
    timer_service,
    meta: Dict[str, Any],
    policy: TimerPolicy,
    base_time_ms: Optional[int] = None,
) -> None:
    """Register all enabled timers from a ``TimerPolicy`` relative to *base_time_ms*.

    Skips any category whose interval is 0.
    """
    base = base_time_ms or int(time.time() * 1000)
    if policy.flush_interval_ms > 0:
        register_timer(
            timer_service, meta, TimerCategory.FLUSH,
            base + policy.flush_interval_ms, policy.use_event_time,
        )
    if policy.recompute_interval_ms > 0:
        register_timer(
            timer_service, meta, TimerCategory.RECOMPUTE,
            base + policy.recompute_interval_ms, policy.use_event_time,
        )
    if policy.evict_interval_ms > 0:
        register_timer(
            timer_service, meta, TimerCategory.EVICT,
            base + policy.evict_interval_ms, policy.use_event_time,
        )

