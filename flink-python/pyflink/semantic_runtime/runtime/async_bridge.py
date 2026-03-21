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
Reusable async bridge: side-output → AsyncDataStream → keyed merge.

This module provides the topology wiring pattern used by ``sem_groupby``,
``sem_agg``, and ``sem_search`` whenever a ``KeyedProcessFunction`` needs
to offload work to an async LLM call and merge the result back into keyed
state.

Topology
--------
::

    keyed_stream.process(StatefulOp)
        │                        │
        ├─ main output           └─ side output (OutputTag: async_work_items)
        │                                  │
        │                           AsyncDataStream.unordered_wait(AsyncWorker)
        │                                  │
        └───── union ─────────────────────-┘
                │
          key_by(key_selector)
                │
          process(MergeFunction)  ← merges async results back into keyed state

Usage
-----
::

    from pyflink.semantic_runtime.runtime.async_bridge import (
        ASYNC_WORK_TAG,
        AsyncWorkItem,
        build_async_bridge,
    )

    # In your stateful operator's process_element:
    #   yield ASYNC_WORK_TAG, work_item.to_dict()
    #
    # Then wire the topology:
    #   main_ds = keyed.process(MyStatefulOp(), ...)
    #   merged = build_async_bridge(main_ds, my_async_fn, key_selector, my_merge_fn, ...)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, Optional

from pyflink.common.typeinfo import Types
from pyflink.datastream import AsyncDataStream, DataStream, OutputTag
from pyflink.datastream.functions import KeyedProcessFunction

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Side output tag (shared convention)
# ---------------------------------------------------------------------------

ASYNC_WORK_TAG = OutputTag("async_work_items")


# ---------------------------------------------------------------------------
# Work item schema
# ---------------------------------------------------------------------------

@dataclass
class AsyncWorkItem:
    """Canonical work item emitted via side output for async processing.

    Attributes
    ----------
    key : str
        Must match the upstream keying so the merge operator can route it back.
    task_type : str
        Identifies the kind of async work (e.g. ``"classify"``, ``"summarize"``,
        ``"rerank"``).
    payload : dict
        Operator-specific data needed by the async worker.
    request_id : str
        Unique ID for deduplication / correlation.
    """

    key: str
    task_type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AsyncWorkItem":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Async result schema
# ---------------------------------------------------------------------------

@dataclass
class AsyncResult:
    """Result returned by the async worker, to be merged back into keyed state.

    Attributes
    ----------
    key : str
        Same key as the originating ``AsyncWorkItem``.
    task_type : str
        Echoed from the work item.
    result : dict
        The async computation result.
    request_id : str
        Correlation ID from the work item.
    success : bool
        Whether the async call succeeded.
    error : str
        Error message if ``success`` is False.
    """

    key: str
    task_type: str
    result: Dict[str, Any] = field(default_factory=dict)
    request_id: str = ""
    success: bool = True
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AsyncResult":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Topology builder
# ---------------------------------------------------------------------------

def build_async_bridge(
    main_ds: DataStream,
    async_fn,
    merge_fn: KeyedProcessFunction,
    key_selector: Callable,
    timeout_ms: int = 30_000,
    capacity: int = 20,
    output_type=None,
) -> DataStream:
    """Wire the side-output → async → keyed-merge topology.

    Parameters
    ----------
    main_ds : DataStream
        The ``SingleOutputStreamOperator`` returned by
        ``keyed_stream.process(StatefulOp, ...)``.
    async_fn : AsyncFunction
        The async worker that processes ``AsyncWorkItem`` dicts and returns
        ``AsyncResult`` dicts.
    merge_fn : KeyedProcessFunction
        A keyed process function that receives unioned main + async results
        and merges them back into keyed state.
    key_selector : callable
        Key selector applied to both main output and async results.
    timeout_ms : int
        Timeout for the async operation.
    capacity : int
        Max concurrent async requests.
    output_type
        Optional Flink TypeInformation for the final merged output.

    Returns
    -------
    DataStream
        The merged output stream.
    """
    # 1. Extract side output stream
    side_ds = main_ds.get_side_output(ASYNC_WORK_TAG)

    # 2. Apply async processing to side output
    async_result_ds = AsyncDataStream.unordered_wait(
        side_ds, async_fn, timeout_ms, capacity,
    )

    # 3. Union main output with async results
    unified_ds = main_ds.union(async_result_ds)

    # 4. Key-by and process with merge function
    merged_ds = unified_ds.key_by(key_selector).process(merge_fn)

    return merged_ds

