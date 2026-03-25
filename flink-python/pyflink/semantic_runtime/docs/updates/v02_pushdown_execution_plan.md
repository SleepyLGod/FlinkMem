# V0.2 Pushdown Execution Plan

This document defines the next execution-phase cleanup for `semantic_runtime`.

It focuses on one question:

> When a semantic operator can be executed as `semantic step + Flink original operator`,
> how should the system do that cleanly without exposing runtime details to users?

This plan is intentionally strict.

- No compatibility shims.
- No fallback or degrade logic.
- No user-visible backend/runtime knobs.
- Fail fast when a request cannot be lowered or executed.

## 1. Goal

The public API is already moving toward a simple facade:

```python
sem_map(intent=..., output_schema=...)
sem_filter(intent=...)
sem_local_topk(intent=..., k=...)
sem_lookup_join(intent=..., candidate_source=...)

sem_window(context=...)
sem_topk(intent=..., k=..., context=...)
sem_groupby(intent=..., context=...)
sem_agg(intent=..., mode=..., context=...)
```

The next step is to make execution as clean as the API:

1. Keep the public facade simple.
2. Lower requests into internal semantic IR.
3. Execute with one of two physical strategies:
   - semantic step + Flink original operator chain
   - native semantic runtime kernel

The system must choose this internally. The user must not see:

- backend choice
- thresholds
- chunk size
- trigger details
- path selection
- pair-block geometry
- internal configs

## 2. Three-Layer Architecture

The execution architecture should keep only three meaningful layers.

### 2.1 Public Request Layer

This layer contains only user intent and business context.

Examples:

- `SemFilterRequest`
- `SemTopKRequest`
- `SemGroupbyRequest`
- `SemContext`

This layer must remain declarative.

### 2.2 Internal Operator Plan Layer

This layer is the merged internal plan consumed by execution assembly.

It should absorb the operational parts of the older internal objects:

- `RuntimeConfig`
- `QuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- `Sem*Config`

These objects may continue to exist during migration, but they are not the target
shape. The target shape is one internal plan per operator.

Examples:

- `SemFilterPlan`
- `SemTopKPlan`
- `SemGroupbyPlan`
- `SemLookupJoinPlan`

### 2.3 Runtime Kernel Layer

This layer contains executable Flink runtime code.

Examples:

- low-level `Sem*Function` classes
- native stateful kernels
- internal pushdown application builders

This layer must stay explicit. Runtime kernels should not infer hidden planner
state or silently recover from invalid configurations.

## 3. Internal Semantic IR

Physical pushdown should not lower directly from a public request into a final
Flink chain. There should be a small internal semantic IR that captures the
meaningful semantic step.

This IR is internal only. It must not be exported through the public API.

### 3.1 Minimal IR Set

The minimal internal semantic IR for this phase is:

- `sem_transform`
  - row-wise semantic structured transformation
  - used by `sem_map`
- `sem_predicate`
  - row-wise semantic boolean decision
  - used by `sem_filter`
- `sem_score`
  - semantic scoring over records or candidates
  - used by pointwise `sem_topk`
- `sem_label`
  - semantic assignment to group identity
  - used by `window_owned sem_groupby`
- `sem_match`
  - semantic compatibility/match judgment between left/right candidates
  - used by `sem_lookup_join` and future `sem_join`
- `sem_search`
  - bounded candidate retrieval primitive
  - used internally by `sem_lookup_join`

These are planner/runtime concepts, not user-facing operators.

## 4. Physical Execution Modes

Each public semantic operator must lower to one of two physical modes.

### 4.1 Pushdown Mode

The operator becomes:

1. one semantic IR step
2. one Flink original operator chain

Examples:

- `sem_filter`
  - `sem_predicate` + native `filter`
- `sem_map`
  - `sem_transform` + native map/projection semantics
- `sem_local_topk`
  - `sem_score` + bounded local top-k selection
- `window_owned sem_groupby`
  - `sem_label` + group-by over assigned labels
- pointwise bounded `sem_topk`
  - `sem_score` + top-n
- derived-attribute `sem_agg`
  - semantic derived attribute + classical aggregation

### 4.2 Native Runtime Mode

The operator remains a custom runtime kernel because no clean classical
operator chain exists.

Examples:

- `operator_owned sem_groupby`
- contextual `sem_topk`
- summarize/compressive `sem_agg`
- `sem_window`
- `sem_search`

## 5. Strict Per-Operator Pushdown Matrix

### 5.1 `sem_map`

Public meaning:

- semantic transformation from one record to one transformed record

Internal IR:

- `sem_transform`

Physical target:

- async semantic transform step
- native map/projection semantics over transformed output

Target status:

- should be physically pushdown-capable in this phase

### 5.2 `sem_filter`

Public meaning:

- semantic predicate over one record

Internal IR:

- `sem_predicate`

Physical target:

- async semantic predicate generation
- native Flink `filter`

Target status:

- first concrete physical pushdown sample in this phase

### 5.3 `sem_local_topk`

Public meaning:

- local semantic top-k over a bounded candidate set

Internal IR:

- `sem_score`

Physical target:

- semantic scoring or bounded rerank inside one bounded input
- local top-k materialization

Notes:

- internal chunking is allowed
- row-level operator still does not own timers or scope

Target status:

- should be pushdown-capable after `sem_filter` and `sem_map`

### 5.4 `sem_lookup_join`

Public meaning:

- semantic lookup join against a bounded right-side candidate source

Internal IR:

- `sem_search`
- `sem_match`
- pair-block evaluation plan

Physical target:

- bounded candidate retrieval
- semantic pair-block evaluation
- native projection/filter over accepted joins

Important:

- `sem_lookup_join` is a retrieval-aware public exception
- user may bind the candidate source
- user must not control backend, pair-block geometry, thresholds, or timeout

Target status:

- boundary already cleaned
- pair-block execution logic still needs implementation

### 5.5 `sem_topk`

Public meaning:

- continuous semantic top-k over a business context

Internal IR:

- pointwise path: `sem_score`
- contextual path: pool-level semantic rerank plan

Physical target:

- bounded pointwise path: `sem_score` + top-n style chain
- contextual path: native runtime kernel

Target status:

- pointwise/window-owned path should move toward real pushdown
- contextual path stays native runtime

### 5.6 `sem_groupby`

Public meaning:

- assign data to an existing group or create a new group

Internal IR:

- `sem_label`

Physical target:

- `window_owned`: `sem_label` + group-by over labels
- `operator_owned`: native runtime kernel

Important:

- internal chunking is allowed via one internal `scope_chunk_size`
- this remains internal planner/runtime state, not public API

Target status:

- contract is already corrected
- real physical pushdown for `window_owned` is still pending

### 5.7 `sem_agg`

Public meaning:

- semantic aggregation over a business context

Internal IR:

- derived-attribute path: semantic value/label/score IR
- summary/compressive path: semantic reduction plan

Physical target:

- derived-attribute path: semantic IR + classical aggregation
- summarize/compressive path: native runtime kernel

Target status:

- algebraic/derived-attribute path is future pushdown work
- summarize/compressive remains native runtime

### 5.8 `sem_window`

Public meaning:

- materialize a business context boundary into a bounded scope

Internal IR:

- none in this pushdown sense

Physical target:

- native scope/window runtime

Target status:

- not a semantic attribute operator
- not part of pushdown-to-classical-op work

## 6. `sem_lookup_join` Pair-Block Plan

`sem_lookup_join` must not be modeled as three separate public modes.

The public contract stays:

```python
sem_lookup_join(intent=..., candidate_source=...)
```

Internally, the join evaluation space is:

```text
P = {(l, r) | l in left_rows, r in candidate_right_rows(l)}
```

The correct internal abstraction is:

- pair-block evaluation

That means one semantic evaluation call processes a block of pair candidates,
not necessarily one row and not necessarily the whole join space.

### 6.1 Internal Pair-Block Geometry

The internal plan should carry:

- `left_block_size`
- `right_block_size`

These define one evaluation block.

Special cases are only geometry choices:

- one left + all right
- one left + right chunk
- left chunk + all right
- left chunk + right chunk

These must not become separate public APIs.

### 6.2 Retrieval and Match Pipeline

The internal execution pipeline for `sem_lookup_join` should become:

1. retrieve bounded candidates with `sem_search`
2. optionally prune or shortlist candidates internally
3. build pair blocks
4. evaluate each pair block with `sem_match`
5. project accepted joins into final join results

### 6.3 Cost Reduction

Embedding or other cheaper strategies may reduce the number of expensive match
calls, but only internally.

Allowed internal optimizations:

- embedding prefilter
- shortlist generation
- block geometry tuning
- planner-selected backend choice

Disallowed public exposure:

- embedding threshold
- pair block size
- timeout budget
- shortlist size

### 6.4 Current Status

Current code does not yet implement true pair-block geometry.

Current execution is effectively:

- one left row
- bounded candidate retrieval
- one LLM call over the full candidate list

This phase must make the pair-block plan explicit in code.

## 7. Migration Rules

### 7.1 Public API

The public facade remains the only intended user API.

Public users should not rely on:

- `RuntimeConfig`
- `QuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- `Sem*Config`
- low-level `Sem*Function`
- low-level pipeline builders

### 7.2 Internal API

Internal planning and runtime assembly may continue to use transitional
objects while the merged per-operator plans are being completed.

This is allowed only as an implementation detail.

It is not a reason to expose those objects publicly.

### 7.3 Failure Policy

All new pushdown execution must stay strict:

- invalid lowering -> raise
- invalid runtime config -> raise
- invalid backend output -> raise
- timeout -> raise
- unsupported candidate source -> raise

No fallback, no soft-degrade, no compatibility mode.

## 8. Implementation Order

### Phase A: Planning and first pushdown sample

1. Write this plan
2. Implement `sem_filter` physical pushdown
   - internal `sem_predicate` step
   - native Flink `filter`
3. Add focused tests for the new chain

### Phase B: Row-level pushdown

4. Implement `sem_map` physical pushdown structure
5. Implement `sem_local_topk` bounded pushdown structure
6. Implement `sem_lookup_join` pair-block internal plan
7. Implement `sem_lookup_join` pair-block execution

### Phase C: Stateful pushdown

8. Implement `window_owned sem_groupby` physical pushdown
9. Implement bounded pointwise `sem_topk` physical pushdown
10. Implement derived-attribute `sem_agg` pushdown when the semantic IR is stable

### Phase D: Cleanup

11. Remove active documentation that still implies logical lowering is already
    equivalent to physical pushdown
12. Re-run end-to-end tests and unrestricted PyFlink positive path

## 9. Definition of Done

This phase is done only when all of the following are true:

1. Public API remains facade-only.
2. Internal IR is explicit and internal-only.
3. Operators that are pushdown-capable use real semantic step + native Flink
   operator execution, not only logical labeling.
4. Operators that are not pushdown-capable remain clean native runtime kernels.
5. `sem_lookup_join` uses explicit pair-block planning internally.
6. Full Python test suite passes.
7. Positive unrestricted PyFlink end-to-end execution passes.
8. No degrade or fallback logic is introduced.
