> Historical note: this document is retained as implementation history. It is not the current cleanup source of truth. The canonical cleanup status and active constraints live in `v02_semantic_runtime_cleanup_plan.md`.

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
- `sem_groupby`: dynamic semantic grouping over keyed state.
- `cts_retrieve`: continuous retrieval over evolving keyed memory/state.
- continuous RAG workflow integration as a composed stateful pipeline (not a new runtime primitive in V0.2).

**Operator boundary note (`sem_join_retrieve` vs `cts_retrieve`):**
- `V0.1 sem_join_retrieve` remains the row-local per-event retrieval + semantic match path.
- `V0.2 cts_retrieve` is the stateful/continuous retrieval path with keyed cache/index metadata.
- They coexist by design in V0.2:
  - use `sem_join_retrieve` for simple stateless retrieval-then-match;
  - use `cts_retrieve` when retrieval quality/latency depends on evolving keyed memory/state.

**CP semantic-window alignment note (important):**
- In the CP paper, semantic windows are defined by semantic continuity/boundary logic, not by one
  mandatory runtime implementation shape.
- Therefore, two implementation routes are both valid:
  - **Option A (paper-near window-operator route):** model semantic windows as native window-lifecycle
    components (assign/trigger/cleanup style).
  - **Option B (state-machine route, V0.2 default):** implement semantic boundaries in
    `KeyedProcessFunction` state machines and emit bounded window snapshots.
- V0.2 chooses Option B as default for engineering feasibility in PyFlink, while keeping Option A as
  an explicit alternative path.

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
  ├─ (optional pre-classification async step)
  │      └─ emits topic_shift / intent_shift labels
  │
  ├─ key_by(user/session/topic key)
  ▼
┌────────────────────────────────────────────────────────────────┐
│ KeyedProcessFunction Semantic State Machine                   │
│  - process_element: update context/state                      │
│  - semantic boundary logic (sem_window, Option B default)     │
│  - register timers (flush/recompute/evict)                    │
│  - maintain sem_topk frontier / sem_agg buffers               │
└──────────────┬─────────────────────────────────────────────────┘
               │
               ├─ main output: stateful semantic updates
               │
               └─ side output: async semantic work items
                              │
                              ▼
                   AsyncDataStream semantic workers
                              │
                              ▼
              keyed merge operator (connect/union + keyed process)

Alternative research branch (Option A):
Input -> key_by -> custom semantic window-lifecycle path (assign/trigger/cleanup) -> semantic operators
```

## Step-by-Step

### Step 1: Key Design + Canonical Stateful Event Model

- Define stable keying strategy (`user_id`, `session_id`, or composite key).
- Define canonical event payload for V0.2 operators:
  - required: key fields, event time/proc time, message payload, sequence id.
  - optional: retrieved candidates, semantic metadata, operator trace id.
- Add key-level invariants (monotonic sequence assumptions if used).
- Lock default time policy and watermark policy:
  - default: processing-time deterministic path,
  - event-time path: bounded out-of-orderness watermark + explicit allowed lateness.
- Establish state-safety foundation from day 1:
  - define shared descriptor declarations (`stateful/state_descriptors.py`),
  - set default TTL policy per state family,
  - define overflow/degrade policy interface used by all stateful operators.

**Deliverable:** stable key schema and keyed stream entrypoint.

### Step 2: `sem_window` State Machine (`KeyedProcessFunction`)

- Implement keyed semantic window state with `ListState`/`ValueState` + TTL from day 1:
  - append events,
  - evaluate semantic boundary condition,
  - emit window snapshot when boundary reached.
- Support two boundary trigger families:
  - structural/time-based (count/time threshold),
  - semantic-boundary-based (topic shift / intent shift flag).
- **Option B (V0.2 default):**
  - semantic boundary signal is produced upstream (async pre-classification),
  - `KeyedProcessFunction` consumes boundary flags and controls window state transitions.
- **Option A (explicit alternative, paper-near route):**
  - implement semantic windows through native window-lifecycle abstractions
    (custom assign/trigger/cleanup style path),
  - treat this as a separate engineering branch due to PyFlink runtime constraints.
- Keep window payload bounded from day 1 (size cap + overflow policy).
- Define baseline timer pattern in this step and make it reusable:
  - timer registration points,
  - flush/recompute/evict callback templates,
  - timer-key naming/versioning conventions.

**Deliverable:** deterministic semantic window emission per key.

### Step 3: `sem_groupby` State Machine (`KeyedProcessFunction`)

- Input shape:
  - keyed events, or
  - keyed semantic-window snapshots emitted by `sem_window`.
- State model:
  - `MapState[group_id -> group_profile]` for semantic bucket summaries,
  - `MapState[group_id -> counters/last_update]` for bounded maintenance.
- Assignment flow:
  - local candidate-group proposal from current state,
  - optional side-output async semantic classifier for ambiguous assignment,
  - keyed merge update to group profile/counters.
- Required async merge-back topology for ambiguous assignments:
  - keyed `sem_groupby` emitter (main path + side output of ambiguity tasks),
  - side output -> `AsyncDataStream` semantic classifier,
  - `connect`/`union` + keyed merge operator to update `MapState[group_id -> profile]`.
- Reuse a shared async bridge utility for this pattern:
  - `stateful/async_bridge.py` for side-output routing, async execution, and keyed merge contract.
- Guardrails:
  - `max_groups_per_key`,
  - per-group TTL and deterministic overflow/eviction policy,
  - explicit new-group creation threshold.
- V0.2 boundary:
  - no retroactive full reassignment over historical records.

**Deliverable:** bounded, deterministic semantic grouping with explicit per-key limits.

### Step 4: `cts_retrieve` Stateful Retrieval Path

- Build continuous retrieval as a keyed/stateful operator path (one-input shape in V0.2):
  - query/request record in,
  - retrieve candidate memory items from keyed cache/index + external store fallback,
  - emit bounded retrieved candidate set with stable ordering metadata.
- Maintain keyed retrieval state:
  - recent retrieval cache/index hints,
  - retrieval counters/version markers.
- Reuse existing async wrappers for expensive semantic rerank/verification where needed.
- Explicit relationship to `V0.1 sem_join_retrieve`:
  - `cts_retrieve` is the stateful evolution of retrieval behavior,
  - `sem_join_retrieve` is still valid and should remain available for stateless/simple workloads.
- Guardrails:
  - `max_candidates_per_request`,
  - strict retrieval timeout budget,
  - deterministic overflow/degrade tagging.

**Deliverable:** deterministic, bounded continuous retrieval output stream for downstream ranking/answering.

### Step 5: Timer Policy (Flush / Recompute / Eviction)

- Execute this step immediately after Step 2, then reuse in Steps 3/4/6/8.

- Add timer registration in `process_element`.
- Use timer callbacks to:
  - flush delayed windows,
  - trigger deferred recomputation,
  - evict stale state.
- Explicitly choose time semantics per operator path:
  - processing-time first (default),
  - event-time path if watermark requirements exist.
- Timer callback safety rule:
  - allowed in timer: local state math / sorting / truncation / eviction,
  - not allowed in timer: blocking or async LLM calls,
  - any LLM-required timer task must be emitted to side output and handled by async workers.

**Deliverable:** timer-driven state transitions with explicit policy.

### Step 6: `sem_agg` (Two Modes)

- Algebraic path:
  - use `ReducingState` / `AggregatingState` when aggregation can be incremental.
- Summarization path:
  - use bounded `ListState`,
  - emit summarize tasks to downstream async operator,
  - merge summarize output back through a keyed merge operator.
- Do not place blocking LLM I/O directly in timer callback.
- Required topology for summarization path:
  - keyed stateful emitter (`sem_agg` task producer)
  - side output summarize tasks
  - `AsyncDataStream` summarizer
  - `connect`/`union` + keyed merge operator to update `ValueState`.
- Reuse the same bridge utility introduced in Step 3 (`stateful/async_bridge.py`).

**Deliverable:** stateful semantic aggregation with bounded memory.

### Step 7: Optional Stateful Retrieve-Assisted `sem_join` (One-Input)

- Keep one-input retrieval shape (not true two-input join yet):
  - per-event retrieval + cache/index hints in keyed state,
  - cache TTL and max-size policy.
- Reuse V0.1 async wrappers for semantic matching call.

**Deliverable:** stateful retrieval-assisted join path without two-input coordination.

### Step 8: Continuous `sem_topk`

- Maintain per-key candidate buffer + current top-k snapshot:
  - candidate state (`MapState`/`ListState`),
  - frontier/scores (`ValueState`).
- Recompute top-k on new evidence or timer trigger.
- Emit only meaningful changes (delta or snapshot policy).

**Deliverable:** continuously updated keyed semantic top-k.

### Step 9: Continuous RAG Workflow Integration (Required Scenario)

- Model continuous RAG as a required composed workflow, not a new runtime function type.
- Subflow A (memory build):
  - `sem_window -> sem_groupby -> sem_agg -> memory entries`.
- Subflow B (query retrieval):
  - request -> `cts_retrieve` -> optional `sem_topk` rerank.
- Subflow C (response + audit):
  - `sem_map` answer synthesis over request + retrieved context,
  - emit audit fields: `memory_version`, `retrieved_ids`, `prompt_version/config_version`.
- V0.2 boundary:
  - retrieval/state path only, no true two-input semantic join in this phase.

**Deliverable:** end-to-end continuous RAG replay/query workflow over evolving stream state.

### Step 10: State Safety Audit + Hardening

- Validate and harden the safety foundation from Steps 1/2 across all implemented operators:
  - descriptor consistency and compatibility guarantees,
  - TTL policy coverage check,
  - overflow/degrade policy coverage check.
- Enforce hard bounds per key:
  - max buffered events,
  - max candidate cache entries,
  - max pending async work items.
- Enforce TTL and deterministic eviction policy.
- Add overflow behavior (truncate/drop/degrade tag) with explicit metrics.

### Step 11: Metrics + Auditability

- Track keyed/stateful metrics:
  - state size by operator/key distribution,
  - timer fire counts,
  - eviction counts,
  - async summarize queue depth,
  - stale-window count.
- Attach operator/version tags for replay/debug.

### Step 12: Integration Tests

- Unit/structural tests:
  - keyed count conservation,
  - state bound and TTL enforcement,
  - timer determinism,
  - descriptor compatibility and state transition determinism.
- Integration correctness tests:
  - semantic window split behavior on crafted conversation streams,
  - semantic group assignment stability and bounded-group behavior,
  - `cts_retrieve` consistency under repeated queries and evolving memory versions,
  - continuous RAG memory-update/retrieval/answer consistency on replay streams,
  - sem_agg output consistency under retries/restarts,
  - continuous top-k update consistency.
- Recovery tests (script-driven where needed):
  - checkpoint/savepoint restore retains expected keyed semantic state.

## File Structure

```text
flink-python/pyflink/semantic_runtime/
├── stateful/
│   ├── __init__.py
│   ├── state_descriptors.py       # centralized descriptor declarations
│   ├── async_bridge.py            # reusable side-output -> async -> keyed-merge bridge
│   ├── semantic_window.py          # KeyedProcessFunction for sem_window
│   ├── sem_groupby_stateful.py     # keyed semantic grouping
│   ├── cts_retrieve.py             # continuous retrieval over evolving keyed state
│   ├── sem_agg_stateful.py         # stateful sem_agg orchestration
│   ├── continuous_rag_workflow.py  # composed workflow (not standalone runtime primitive)
│   ├── sem_topk_continuous.py      # continuous keyed sem_topk
│   └── retrieval_cache.py          # optional keyed cache for retrieve-assisted join
├── operators/
│   └── ...                         # reuse V0.1 async semantic wrappers
└── tests/
    ├── test_v02_sem_window.py
    ├── test_v02_sem_groupby.py
    ├── test_v02_cts_retrieve.py
    ├── test_v02_sem_agg.py
    ├── test_v02_continuous_rag.py
    ├── test_v02_continuous_topk.py
    └── test_v02_recovery.py
```

## Execution Order

Recommended: **1 → 2 → 5 → 3 → 4 → 10 → 6 → 7(optional) → 8 → 9 → 11 → 12**

State safety is established in Steps 1/2 and then audited/hardened in Step 10.

If pursuing Option A in parallel:
- Branch after Step 1 into an experimental track for native semantic window-lifecycle integration.
- Keep Option B as the production path until Option A proves equivalent correctness and stable runtime behavior.

## Excluded from V0.2

- No new runtime lowering/function type changes in this phase.
- No global cross-key semantic clustering in `sem_groupby`.
- No true two-input semantic join for continuous RAG memory in this phase.
- True two-input `sem_join` with `ConnectedStreams` + `KeyedCoProcessFunction` (→ V0.3a).
- CP batching/fusion optimization (→ V0.3b).
- Dynamic broadcast control / MOBO optimizer loop (→ V0.4).
- VectraFlow-style vector index acceleration internals (→ V1.0 optional).
