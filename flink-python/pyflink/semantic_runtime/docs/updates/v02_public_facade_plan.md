# V0.2 Public Facade Plan

This document defines the target user-facing API for `semantic_runtime`.

It supersedes any implicit assumption that end users should directly work with:

- `RuntimeConfig`
- `QuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- `Sem*Config`
- low-level `Sem*Function` classes

Those remain valid internal runtime/planner concepts. They are not the
intended public surface.

## Goal

The public API should let a user express only:

- semantic intent
- required output shape
- required business context

The public API must not expose:

- backend selection
- trigger details
- window-owned vs operator-owned path
- chunk size
- thresholds
- retries
- timeouts
- low-level state descriptors

All of those are internal runtime / planner / CBO concerns.

## Three-Layer Target

The current codebase still contains too many distinct internal objects:

- `RuntimeConfig`
- `QuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- `Sem*Config`
- low-level `Sem*Function`

These must not remain as six equally visible layers.

The target architecture keeps only three meaningful layers:

1. **Public Request**
   - user-facing semantic intent and business context
2. **Internal Operator Plan**
   - per-operator merged plan used by lowering/planner/runtime assembly
3. **Runtime Kernel**
   - executable Flink operator/function

### Merge rule

The system should merge:

- `QuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- operator-specific parts of `RuntimeConfig`
- operator-specific parts of `Sem*Config`

into one per-operator internal plan.

The runtime kernel remains separate and should not be merged away.

### Example

Instead of keeping:

- `TopKQuerySpec`
- `TopKScopePolicy`
- `TriggerPolicy`
- `SemTopKConfig`
- `RuntimeConfig`

as separate primary concepts, the internal assembly path should produce one
merged `TopKPlan` that is then consumed by the top-k runtime kernel.

## Public API Shape

### Row-level

```python
sem_map(intent=..., output_schema=...)
sem_filter(intent=...)
sem_local_topk(intent=..., k=...)
sem_lookup_join(intent=..., candidate_source=...)
```

### Stateful

```python
sem_window(context=...)
sem_topk(intent=..., k=..., context=...)
sem_groupby(intent=..., context=...)
sem_agg(intent=..., mode=..., context=...)
```

## Public Semantics

### `intent`

`intent` is the user-visible semantic instruction.

Examples:

- `"Extract sentiment: {input}"`
- `"Keep only weather-related events"`
- `"Rank candidates by relevance to the current user intent"`
- `"Assign each event to an existing topic group or create a new one"`

### `context`

`context` expresses business boundary semantics only.

It must not expose trigger/runtime details.

Examples of valid public context concepts:

- `record`
- `window`
- `session`
- `semantic_segment`

Examples of invalid public context details:

- `trigger_policy`
- `idle_flush`
- `count_threshold`
- `periodic`
- `execution_path`
- `window_owned`
- `operator_owned`

Those are internal lowering/runtime decisions.

## Internal Lowering Rule

The system lowers public requests into internal runtime objects:

- `SemSpec`
- `TopKQuerySpec`
- `GroupbyQuerySpec`
- `AggQuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- `Sem*Config`

The mapping is internal and deterministic.

### Internal decisions include

- backend selection (`llm`, `embedding`, `rule`, `external_score`)
- A-path vs B-path
- trigger plan
- chunk size
- threshold selection
- shortlist / pruning policy

## Row-level Principles

Row-level operators are stateless from the public API perspective.

They:

- do not own scope
- do not own trigger
- do not own timer
- may consume bounded payloads
- may use internal chunking

### `sem_lookup_join`

`sem_lookup_join` is the one row-level exception with retrieval-aware public
surface.

Public API may expose:

- semantic join intent
- candidate source binding

Public API must not expose:

- LLM backend
- embedding backend
- timeout
- candidate cap
- chunk size
- pair-block planning

Internal execution should be modeled as **pair-block evaluation** over the
join pair space. Variants such as:

- one left row + all right rows
- chunked left-right pairs
- multiple left rows + all right rows

are all internal block-geometry choices, not separate public modes.

## Stateful Principles

Stateful operators own continuous runtime state internally.

Public API should not expose the runtime mechanics directly.

Users provide:

- semantic intent
- required aggregation/ranking/grouping mode
- business context

The runtime decides:

- window-owned or operator-owned
- early fire / periodic / idle flush / scope close
- state retention strategy

### `sem_window`

`sem_window` is public, but it belongs to the window/scope primitive layer,
not the same semantic-operator layer as:

- `sem_topk`
- `sem_groupby`
- `sem_agg`

## Migration Plan

### Phase 1: Add public facade layer

Introduce a dedicated public module that exports:

- public context dataclasses
- row-level facade constructors
- stateful facade constructors

These constructors return declarative request objects, not low-level runtime
functions.

### Phase 2: Add internal lowering from facade to runtime

Add internal lowering helpers:

- facade request -> `SemSpec`
- facade request -> `QuerySpec`
- facade request -> per-operator internal plan
- per-operator internal plan -> runtime kernel

This phase does not change runtime semantics.

### Phase 3: Switch docs and tests to facade

Active docs should present only the facade API as the default public usage.

Tests should be split into:

- public facade tests
- internal lowering tests
- runtime tests

### Phase 4: Demote low-level public exports

Move low-level runtime entry points out of the primary public surface:

- `RuntimeConfig`
- `Sem*Config`
- `Sem*Function`
- low-level builder functions

They remain importable from internal modules for runtime/planner/testing use,
but are no longer shown as the normal user API.

## Non-goals

This plan does not introduce:

- compatibility shims
- degraded outputs
- fallback runtime contracts
- new semantic behavior beyond the required boundary cleanup

## Completion Criteria

This refactor is complete only when all of the following are true:

1. Public docs show the facade API, not low-level runtime config.
2. Generic public operators do not expose backend selection.
3. Public stateful APIs do not expose trigger/runtime mechanics.
4. Low-level runtime entry points are no longer the primary public exports.
5. Full Python test suite passes.
6. Isolated runtime e2e passes.
