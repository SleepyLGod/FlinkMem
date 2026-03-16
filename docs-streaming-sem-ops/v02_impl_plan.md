<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# V0.2 Implementation Plan — Stateful Semantic Window + Continuous Operators

## Goal

Deliver the first **stateful/continuous** semantic layer on top of PyFlink keyed processing:

- `sem_window`: semantic boundary detection and window materialization over keyed streams.
- `sem_agg`: semantic aggregation/summarization over bounded keyed state.
- continuous `sem_topk`: continuously maintained top-k per key with timer-driven refresh/eviction.
- optional stateful retrieve-assisted `sem_join` variant (still one-input + retrieval; not two-input join).

**Completion criteria:** deterministic keyed behavior, bounded state growth, and correct recovery
after restart/checkpoint restore.

**V0.2 measurable SLOs (required):**
- No unbounded state growth: each key respects configured hard limits and TTL policy.
- Timer-driven updates are deterministic (same input order + watermark policy -> same output).
- Out-of-order input handling is correct under configured event-time/processing-time policy.
- Stateful outputs are restored correctly after simulated restart/checkpoint recovery.

## Architecture

```text
Input Stream (message/event)
  │
  ├─ key_by(user/session/topic key)
  ▼
┌────────────────────────────────────────────────────────────────┐
│ KeyedProcessFunction Semantic State Machine                   │
│  - process_element: update context/state                      │
│  - semantic boundary logic (sem_window)                       │
│  - register timers (flush/recompute/evict)                    │
│  - maintain sem_topk frontier / sem_agg buffers               │
└──────────────┬─────────────────────────────────────────────────┘
               │
               ├─ main output: stateful semantic updates
               │
               └─ optional side output: async summarization work items
                              │
                              ▼
                   AsyncDataStream semantic summarizer
                              │
                              ▼
                    keyed merge/update of aggregated state
```

## Step-by-Step

### Step 1: Key Design + Canonical Stateful Event Model

- Define stable keying strategy (`user_id`, `session_id`, or composite key).
- Define canonical event payload for V0.2 operators:
  - required: key fields, event time/proc time, message payload, sequence id.
  - optional: retrieved candidates, semantic metadata, operator trace id.
- Add key-level invariants (monotonic sequence assumptions if used).

**Deliverable:** stable key schema and keyed stream entrypoint.

### Step 2: `sem_window` State Machine (`KeyedProcessFunction`)

- Implement keyed semantic window state with `ListState`/`ValueState`:
  - append events,
  - evaluate semantic boundary condition,
  - emit window snapshot when boundary reached.
- Support two boundary trigger families:
  - structural/time-based (count/time threshold),
  - semantic-boundary-based (topic shift / intent shift flag).
- Keep window payload bounded from day 1.

**Deliverable:** deterministic semantic window emission per key.

### Step 3: Timer Policy (Flush / Recompute / Eviction)

- Add timer registration in `process_element`.
- Use timer callbacks to:
  - flush delayed windows,
  - trigger deferred recomputation,
  - evict stale state.
- Explicitly choose time semantics per operator path:
  - processing-time first (default),
  - event-time path if watermark requirements exist.

**Deliverable:** timer-driven state transitions with explicit policy.

### Step 4: `sem_agg` (Two Modes)

- Algebraic path:
  - use `ReducingState` / `AggregatingState` when aggregation can be incremental.
- Summarization path:
  - use bounded `ListState`,
  - emit summarize tasks to downstream async operator,
  - merge summarize output back into `ValueState`.
- Do not place blocking LLM I/O directly in timer callback.

**Deliverable:** stateful semantic aggregation with bounded memory.

### Step 5: Continuous `sem_topk`

- Maintain per-key candidate buffer + current top-k snapshot:
  - candidate state (`MapState`/`ListState`),
  - frontier/scores (`ValueState`).
- Recompute top-k on new evidence or timer trigger.
- Emit only meaningful changes (delta or snapshot policy).

**Deliverable:** continuously updated keyed semantic top-k.

### Step 6: Optional Stateful Retrieve-Assisted `sem_join` (One-Input)

- Keep one-input retrieval shape (not true two-input join yet):
  - per-event retrieval + cache/index hints in keyed state,
  - cache TTL and max-size policy.
- Reuse V0.1 async wrappers for semantic matching call.

**Deliverable:** stateful retrieval-assisted join path without two-input coordination.

### Step 7: State Safety Controls

- Enforce hard bounds per key:
  - max buffered events,
  - max candidate cache entries,
  - max pending async work items.
- Enforce TTL and deterministic eviction policy.
- Add overflow behavior (truncate/drop/degrade tag) with explicit metrics.

### Step 8: Metrics + Auditability

- Track keyed/stateful metrics:
  - state size by operator/key distribution,
  - timer fire counts,
  - eviction counts,
  - async summarize queue depth,
  - stale-window count.
- Attach operator/version tags for replay/debug.

### Step 9: Integration Tests

- Structural tests:
  - keyed count conservation,
  - state bound and TTL enforcement,
  - timer determinism.
- Correctness tests:
  - semantic window split behavior on crafted conversation streams,
  - sem_agg output consistency under retries/restarts,
  - continuous top-k update consistency.
- Recovery tests:
  - checkpoint restore retains expected keyed semantic state.

## File Structure

```text
flink-python/pyflink/semantic_runtime/
├── stateful/
│   ├── __init__.py
│   ├── semantic_window.py          # KeyedProcessFunction for sem_window
│   ├── sem_agg_stateful.py         # stateful sem_agg orchestration
│   ├── sem_topk_continuous.py      # continuous keyed sem_topk
│   └── retrieval_cache.py          # optional keyed cache for retrieve-assisted join
├── operators/
│   └── ...                         # reuse V0.1 async semantic wrappers
└── tests/
    ├── test_v02_sem_window.py
    ├── test_v02_sem_agg.py
    ├── test_v02_continuous_topk.py
    └── test_v02_recovery.py
```

## Execution Order

Recommended: **1 → 2 → 3 → 7 → 4 → 5 → 8 → 9**

Build state safety controls early (Step 7), before expanding semantic logic complexity.

## Excluded from V0.2

- True two-input `sem_join` with `ConnectedStreams` + `KeyedCoProcessFunction` (→ V0.3a).
- CP batching/fusion optimization (→ V0.3b).
- Dynamic broadcast control / MOBO optimizer loop (→ V0.4).
- VectraFlow-style vector index acceleration internals (→ V1.0 optional).
