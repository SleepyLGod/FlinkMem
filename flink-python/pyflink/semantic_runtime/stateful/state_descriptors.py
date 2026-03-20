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
Centralized state descriptor declarations and TTL/overflow policy for V0.2.

All state descriptors used by stateful semantic operators are declared here
to ensure:
  - consistent naming across operators,
  - uniform TTL configuration,
  - single point of change for checkpoint compatibility.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass
from typing import Optional

from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import (
    ListStateDescriptor,
    MapStateDescriptor,
    StateTtlConfig,
    ValueStateDescriptor,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Overflow / degrade policy
# ---------------------------------------------------------------------------

class OverflowPolicy(enum.Enum):
    """Action when a state container reaches its hard limit."""
    DROP_OLDEST = "drop_oldest"      # evict oldest entries
    DROP_NEWEST = "drop_newest"      # reject incoming entry


@dataclass
class StateSafetyConfig:
    """Per-operator state safety configuration.

    Every stateful semantic operator should accept one of these at construction
    time and enforce the limits in ``process_element`` / ``on_timer``.
    """
    ttl_seconds: int = 3600                    # default 1 hour
    max_window_events: int = 500               # max events in a single window buffer
    max_groups_per_key: int = 50               # max semantic groups per key
    max_candidates_per_key: int = 200          # max retrieval candidates per key
    max_topk_candidates: int = 100             # max topk candidate buffer size
    max_pending_async_items: int = 50          # max pending side-output work items
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST


# ---------------------------------------------------------------------------
# TTL builder helper
# ---------------------------------------------------------------------------

def build_ttl_config(ttl_seconds: int = 3600) -> StateTtlConfig:
    """Build a standard TTL config for V0.2 stateful operators.

    - Update on read and write (keeps active state alive).
    - Never return expired values.
    - Background cleanup enabled.
    """
    return (
        StateTtlConfig
        .new_builder(Time.seconds(ttl_seconds))
        .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
        .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build()
    )


# ---------------------------------------------------------------------------
# sem_window descriptors
# ---------------------------------------------------------------------------

def sem_window_event_buffer_descriptor(
    ttl_seconds: int = 3600,
) -> ListStateDescriptor:
    """ListState descriptor for the semantic window event buffer.

    Each element is a pickled ``SemanticEvent.to_dict()`` dict.
    """
    desc = ListStateDescriptor("sem_window_event_buffer", Types.PICKLED_BYTE_ARRAY())
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


def sem_window_meta_descriptor(
    ttl_seconds: int = 3600,
) -> ValueStateDescriptor:
    """ValueState descriptor for semantic window metadata.

    Stores a pickled dict with keys like ``open_time_ms``, ``event_count``,
    ``window_id``.
    """
    desc = ValueStateDescriptor("sem_window_meta", Types.PICKLED_BYTE_ARRAY())
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


# ---------------------------------------------------------------------------
# sem_groupby descriptors
# ---------------------------------------------------------------------------

def sem_groupby_profiles_descriptor(
    ttl_seconds: int = 3600,
) -> MapStateDescriptor:
    """MapState descriptor for semantic group profiles.

    Key: group_id (str), Value: pickled group profile dict.
    """
    desc = MapStateDescriptor(
        "sem_groupby_profiles", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
    )
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


# ---------------------------------------------------------------------------
# cts_retrieve descriptors
# ---------------------------------------------------------------------------

def cts_retrieve_cache_descriptor(
    ttl_seconds: int = 1800,
) -> MapStateDescriptor:
    """MapState descriptor for retrieval cache/index hints.

    Key: candidate_id (str), Value: pickled candidate dict.
    """
    desc = MapStateDescriptor(
        "cts_retrieve_cache", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
    )
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


# ---------------------------------------------------------------------------
# sem_agg descriptors
# ---------------------------------------------------------------------------

def sem_agg_buffer_descriptor(
    ttl_seconds: int = 3600,
) -> ListStateDescriptor:
    """ListState descriptor for the sem_agg event buffer (summarization path).

    Each element is a pickled event/snapshot dict awaiting summarization.
    """
    desc = ListStateDescriptor("sem_agg_buffer", Types.PICKLED_BYTE_ARRAY())
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


def sem_agg_value_descriptor(
    ttl_seconds: int = 3600,
) -> ValueStateDescriptor:
    """ValueState descriptor for the sem_agg accumulated aggregate.

    Stores a pickled dict with running algebraic aggregate or latest summary.
    """
    desc = ValueStateDescriptor("sem_agg_value", Types.PICKLED_BYTE_ARRAY())
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


def sem_agg_meta_descriptor(
    ttl_seconds: int = 3600,
) -> ValueStateDescriptor:
    """ValueState descriptor for sem_agg metadata (counters, version)."""
    desc = ValueStateDescriptor("sem_agg_meta", Types.PICKLED_BYTE_ARRAY())
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


# ---------------------------------------------------------------------------
# sem_topk descriptors
# ---------------------------------------------------------------------------

def sem_topk_candidates_descriptor(
    ttl_seconds: int = 3600,
) -> MapStateDescriptor:
    """MapState descriptor for the sem_topk candidate buffer.

    Key: candidate_id (str), Value: pickled candidate record with score.
    """
    desc = MapStateDescriptor(
        "sem_topk_candidates", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
    )
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc


def sem_topk_snapshot_descriptor(
    ttl_seconds: int = 3600,
) -> ValueStateDescriptor:
    """ValueState descriptor for the current top-k snapshot / frontier."""
    desc = ValueStateDescriptor("sem_topk_snapshot", Types.PICKLED_BYTE_ARRAY())
    desc.enable_time_to_live(build_ttl_config(ttl_seconds))
    return desc
