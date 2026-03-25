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

# V0.2+ Implementation Plan

## 1. Goal

Close the gap between the current V0.2 keyed-state foundation and the intended
CP-style semantic runtime before true two-input `sem_join` work begins.

This plan is execution-oriented. It assumes the current V0.2 foundation exists
and focuses on the remaining V0.2+ work items that should be finished before
Phase V0.3.

## 2. Scope

V0.2+ includes:

1. public semantic API cleanup and stabilization,
2. scorer-complete `sem_topk`,
3. query-spec-driven stateful operator design,
4. unified semantic/config abstractions,
5. pluggable retrieval backend boundaries,
6. naming/package cleanup preparation.

V0.2+ does **not** include:

1. true two-input `sem_join` kernel,
2. production external backend selection and deployment,
3. engine-level vector acceleration,
4. MOBO/CBO completion.

## 3. Current Status

The following items are already in place:

1. `sem_map` dual-mode API (`structured` + `free-form`).
2. public rename direction:
   - `sem_lookup_join`
   - `sem_search`
   - `sem_local_topk`
   - canonical stateful `sem_topk`
3. canonical `sem_topk` kernel + pipeline split:
   - `TopKQuerySpec`
   - pure scored-item kernel
   - pointwise scorer backends
4. shared external retrieval contract:
   - `ExternalSearchBackend`
   - `MockSearchBackend`
   - `FaissSearchBackend` demo
5. shared local lightweight encoder path for:
   - `sem_search`
   - `sem_topk` embedding pointwise scorer
   - FAISS demo backend

## 4. Remaining Work Packages

### WP1. Query-Spec Layer Completion

Add clean query-spec types for the remaining stateful operators:

1. `GroupbyQuerySpec`
2. `AggQuerySpec`
3. `JoinQuerySpec`

Principle:

- `SemanticSpec` describes semantic criterion.
- `XxxQuerySpec` describes operator-level continuous query semantics.
- `XxxConfig` remains kernel/runtime configuration.

### WP2. `sem_topk` Rerank Method Completion

Complete `sem_topk` execution strategies beyond pointwise scoring.

Required:

1. `ranking_method="pointwise"` — complete, baseline.
2. `ranking_method="pairwise"` — bounded-pool rerank strategy.
3. `ranking_method="listwise"` — bounded-pool rerank strategy.

Rules:

1. pairwise/listwise apply only to bounded candidate pools,
2. retrieval is optional optimization, not part of canonical top-k semantics,
3. top-k kernel remains pure and stateful,
4. current first implementation should target `on_scope_close`-like pool boundaries
   first (for example retrieval envelopes or closed windows),
5. `TriggerPolicy` remains user-visible now; optimizer-driven trigger selection
   is a later enhancement.

### WP3. `sem_groupby` Refactor

Refactor `sem_groupby` into:

1. pure grouping kernel,
2. assignment/refinement strategy layer.

Planned strategy modes:

1. `rule`
2. `embedding`
3. `llm`
4. `llm_refine`

### WP4. `sem_agg` Refactor

Refactor `sem_agg` into:

1. pure aggregation kernel,
2. aggregation strategy layer.

Planned strategy modes:

1. `algebraic`
2. `summarize`
3. `compressive`

### WP5. RuntimeConfig Completion

Complete the unified runtime entry point:

1. loadable top-level config,
2. typed hydration into operator configs,
3. clean user-facing setup flow.

### WP6. Naming / Package Cleanup

Prepare and then execute a targeted cleanup pass:

1. public semantics remain primary,
2. package structure reflects implementation role,
3. avoid mixing public semantics and historical suffixes.

This remains secondary to semantics/query-spec completion.

## 5. Execution Order

The execution order is intentionally narrow:

1. WP1 — Query-spec completion
2. WP2 — `sem_topk` rerank method completion
3. WP3 — `sem_groupby` refactor
4. WP4 — `sem_agg` refactor
5. WP5 — RuntimeConfig completion
6. WP6 — naming/package cleanup
7. V0.3 — true two-input `sem_join`

## 6. Immediate Next Step

Implement WP1 now:

1. add `GroupbyQuerySpec`,
2. add `AggQuerySpec`,
3. add `JoinQuerySpec`,
4. export them publicly,
5. add focused tests.

This gives the remaining stateful operators the same semantic/config boundary
that `sem_topk` already has.

## 7. Acceptance Criteria

V0.2+ is complete when:

1. every stateful semantic operator has a clean query-spec boundary,
2. `sem_topk` supports all planned rerank methods for bounded pools,
3. `sem_groupby` and `sem_agg` are split into kernel vs strategy layers,
4. unified config is usable from one top-level entry point,
5. external retrieval remains pluggable without locking to one production backend,
6. true two-input `sem_join` is still deferred to V0.3.
