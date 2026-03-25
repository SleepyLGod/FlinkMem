> Historical note: this document is retained as implementation history. It is not the current cleanup source of truth. The canonical cleanup status and active constraints live in `v02_semantic_runtime_cleanup_plan.md`.

# V0.2 Cleanup Audit

Historical note:

- this audit records issues found before the strict fail-fast cleanup landed
- several items below have already been resolved
- treat this file as audit history, not as the current source of truth
- current cleanup state lives in
  `docs/updates/v02_semantic_runtime_cleanup_plan.md`

## Goal

This audit does **not** propose new features.

It answers a narrower question:

- given the current V0.2 implementation,
- what is structurally redundant,
- what is overdesigned,
- what fallback/degraded paths change semantics,
- and what is still missing from end-to-end validation.

The standard is first-principles:

1. one semantic intent should have one clear source of truth
2. unsupported combinations should fail early, not silently degrade
3. public concepts should either be real execution concepts or not exist yet
4. planner/lowering metadata must match physical execution


## Executive Judgment

The current V0.2 codebase is **functionally broad enough** to be called complete for:

- `sem_topk`
- `sem_groupby`
- `sem_agg`
- typed `RuntimeConfig`
- internal lowering plans
- composed workflow assembly

But it is **not yet clean enough** to be the long-term base for CBO/planner work without one cleanup round.

The main problem is no longer missing functionality. The main problem is that the code still carries:

- transition-era compatibility
- fail-soft passthrough behavior
- duplicated planning decisions
- public abstractions that are not yet part of the real execution path


## Audit Scope

Reviewed areas:

- `stateful/sem_topk_pipeline.py`
- `stateful/sem_topk_continuous.py`
- `stateful/sem_groupby_stateful.py`
- `stateful/sem_groupby_window.py`
- `stateful/sem_agg_stateful.py`
- `stateful/sem_agg_pipeline.py`
- `stateful/continuous_rag_workflow.py`
- `stateful/semantic_lowering.py`
- `runtime_config.py`
- related tests and public operator exports


## Findings

## 1. Compatibility / Patch-Layer Logic That Should Be Reduced

### 1.1 `sem_topk` still uses degraded passthrough where it should often fail fast

**Symptom**

`sem_topk` currently converts multiple incompatible conditions into degraded passthrough records instead of failing builder-time or operator-time:

- incompatible input shape for chosen path
- empty bounded pool
- missing external score
- unsupported backend behavior

Examples:

- `topk_scope_close_requires_bounded_pool`
- `topk_operator_owned_requires_flat_candidates`
- `topk_operator_owned_contextual_requires_flat_candidates`
- `topk_missing_external_score:*`

Relevant code:

- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:300)
- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:1172)
- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:1224)
- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:1267)
- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:1298)

**Why this is a problem**

This is not just resilience. It weakens semantic guarantees:

- a caller may believe top-k ran
- downstream may receive a retrieval-like degraded envelope instead
- the type contract becomes "top-k or maybe not"

That is acceptable in a demo path, but not as the default semantics of a canonical operator.

**Judgment**

This is transition-era patch logic. Some of it is useful for workflow robustness, but too much of it is currently inside the top-k execution path.

**Cleanup direction**

- keep degraded passthrough only for true runtime failures:
  - scorer timeout
  - external backend error
- remove degraded passthrough for configuration incompatibility
- convert path/input incompatibility into builder-time exceptions


### 1.2 `sem_agg` legacy trigger branch

**Symptom**

`SemAggFunction` previously supported a compatibility branch when
`query_spec is None`, using `legacy_buffered` trigger behavior inside the
operator core.

Relevant code:

- [sem_agg_stateful.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_agg_stateful.py:201)
- [sem_agg_stateful.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_agg_stateful.py:206)
- [sem_agg_stateful.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_agg_stateful.py:261)

**Why this was a problem**

That meant there were still two semantic entry modes:

- query-spec-driven trigger semantics
- legacy config-driven trigger semantics

That is an extra branch in the core operator, not just at the config boundary.

**Judgment**

Architectural debt. The core operator should execute one trigger model only.

**Cleanup direction**

- normalize config-only construction into one canonical `AggQuerySpec`
- move config translation to construction time
- keep `SemAggFunction` internally query-spec-native


### 1.3 `RuntimeConfig` still treats flat legacy layout as first-class

**Symptom**

`RuntimeConfig` keeps both:

- flat legacy operator sections
- nested `query_spec` / `kernel`

Relevant code:

- [runtime_config.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/runtime_config.py:29)
- [runtime_config.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/runtime_config.py:168)

**Why this is a problem**

As long as legacy flat layout remains fully first-class, typed configuration is not the single source of truth.

**Judgment**

Still acceptable in V0.2, but should be frozen, not extended.

**Cleanup direction**

- keep compatibility in parsing
- stop adding new semantics to flat layout
- document nested layout as the only evolving contract


## 2. Overdesign / Duplicate Decision Layers

### 2.1 Execution-path decisions are expressed in three places

**Symptom**

The following all influence execution choice:

- `QuerySpec.execution_path`
- `SemanticLoweringPlan`
- builder heuristics in pipeline code

Relevant code:

- [semantic_lowering.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/semantic_lowering.py:81)
- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:1119)
- [sem_groupby_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_groupby_pipeline.py:50)
- [sem_agg_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_agg_pipeline.py:42)

**Why this is a problem**

There is no longer one obvious source of truth for physical planning:

- user intent
- logical lowering
- physical path heuristic

These can drift.

**Judgment**

This is the most important structural cleanup item for planner readiness.

**Cleanup direction**

- `QuerySpec` should express semantic intent
- `SemanticLoweringPlan` should express logical lowering only
- builder should consume lowering plan and input shape, not invent extra planning rules

In other words:

- user intent -> logical plan -> physical plan
- not user intent + logical plan + builder heuristics all competing


### 2.2 Public lowering-facing operators exist, but are not part of real A-path execution

**Symptom**

Public operators now exist:

- `SemScoreFunction`
- `SemLabelFunction`
- `SemMatchFunction`

But they are only used in:

- exports
- docs
- dedicated tests

They are not used by current A-path builders.

Evidence:

- only references are in exports/docs/tests, not runtime builders
- [_semantic_attrs.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/operators/_semantic_attrs.py:37)

**Why this is a problem**

This creates a split:

- internal lowering says "semantic attribute + classical operator"
- public API says the same
- actual execution still uses custom pipeline workers, not those attribute operators

So the abstraction is conceptually correct, but operationally disconnected.

**Judgment**

This is not useless, but it is currently ahead of the execution model.

**Cleanup direction**

Decide one of two directions:

1. make these public attribute operators explicitly user-facing only, not execution primitives
2. or make A-path builder actually lower through a shared attribute-generation layer

Current state is between the two.


### 2.3 Workflow config still stores split sub-configs instead of runtime bundles

**Symptom**

Even after typed `RuntimeConfig` support, `ContinuousRAGConfig` still stores:

- `groupby_config` + `groupby_query_spec`
- `agg_config` + `agg_query_spec`
- `topk_config` + `topk_query_spec`

Relevant code:

- [continuous_rag_workflow.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/continuous_rag_workflow.py:131)

**Why this is a problem**

The runtime bundle already exists and already includes:

- query spec
- kernel config
- lowering plan

But workflow immediately decomposes it back into pieces.

**Judgment**

This is duplication, not a hard bug.

**Cleanup direction**

- make workflow config hold runtime bundles directly
- keep convenience accessors if needed


## 3. Fallback / Degraded Paths That Change Business Semantics

### 3.1 `sem_topk auto + on_scope_close` currently mixes A and B semantics

**Symptom**

In `auto + on_scope_close`, `sem_topk` first schedules scope-close pool results from bounded pools, then may switch `execution_path` to `operator_owned` for pointwise lowering:

- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:1188)
- [sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_topk_pipeline.py:1198)

So the same build can combine:

- A-path pool-final results
- B-path flat-candidate operator-owned processing

**Why this is a problem**

Even if intentional, this is not clean semantics.

`auto` should choose one physical interpretation for one logical intent, not partially do both.

**Judgment**

This is a real semantic smell, not just an aesthetic issue.

**Cleanup direction**

- `auto` must resolve to one physical path per logical sub-case
- if mixed-input mode is desired, it should be a separate explicit adapter stage, not hidden inside `auto`


### 3.2 `llm_refine` in `sem_groupby` still means "local refine plus async classify"

**Symptom**

Current `llm_refine` behavior still relies on local refinement helpers:

- local merge
- local relabel
- async classify for some assignment flow

Relevant code:

- [sem_groupby_stateful.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_groupby_stateful.py:203)
- [sem_groupby_stateful.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_groupby_stateful.py:865)
- [sem_groupby_window.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/stateful/sem_groupby_window.py:97)

**Why this is a problem**

The name suggests:

- true LLM-driven refinement

The implementation actually provides:

- local heuristic maintenance
- plus async classify in some places

That is a mismatch between name and business meaning.

**Judgment**

This is one of the clearest semantics-vs-name mismatches in the codebase.

**Cleanup direction**

Either:

- rename current mode to something like `local_refine_async_verify`

or:

- keep `llm_refine` only when a true async refine worker exists


### 3.3 Row-wise operators are fail-soft by default

**Symptom**

V0.1 row-wise operators produce degraded output envelopes instead of failing:

- `sem_map`
- `sem_filter`
- `sem_lookup_join`
- `sem_topk` V0.1 row-wise variant

Examples:

- [sem_map.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/operators/sem_map.py:121)
- [sem_filter.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/operators/sem_filter.py:110)
- [sem_join_retrieve.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/operators/sem_join_retrieve.py:205)

**Why this is a problem**

This is a legitimate design choice, but it means the system is not fail-strict.

If downstream business logic ignores `_degraded`, semantic failures become silent business drift.

**Judgment**

This is acceptable only if degraded handling is treated as part of the contract everywhere.

**Cleanup direction**

- document degraded output as a hard contract
- add contract-level downstream tests that assert degraded is surfaced, not silently ignored


## 4. Validation Gaps

### 4.1 There is strong unit/in-memory integration coverage, but not full runtime-chain validation

**Current coverage**

- operator/spec/runtime unit coverage:
  - [test_v02_stateful_foundation.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/tests/test_v02_stateful_foundation.py)
- top-k pipeline coverage:
  - [test_v02_sem_topk_pipeline.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/tests/test_v02_sem_topk_pipeline.py)
- workflow integration coverage:
  - [test_v02_evermemos_workflow.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/tests/test_v02_evermemos_workflow.py)
- public semantic attribute operators:
  - [test_semantic_attrs.py](/Users/von/Projects/FlinkMem/flink-python/pyflink/semantic_runtime/tests/test_semantic_attrs.py)

**Missing**

There is still no real runtime-level validation for:

- `RuntimeConfig -> build_continuous_rag_workflow_from_runtime_config -> actual DataStream execution`
- checkpoint / restore / recovery semantics
- planner/lowering decision consistency against physical path

**Judgment**

Current tests prove logic correctness well.
They do **not** yet prove runtime contract correctness end-to-end.


## 5. What Is Not a Problem

These are not cleanup targets right now:

1. keeping A-path and B-path in separate files for `groupby` and `agg`
   - this is structurally correct

2. keeping `semantic_lowering.py`
   - this is the right insertion point for future planner/CBO work

3. keeping typed `RuntimeConfig`
   - this is necessary and now correctly wired into workflow assembly

4. keeping public derived-attribute operators
   - not wrong by themselves; the issue is only that their role is not yet fully integrated with A-path execution


## Cleanup Priorities

## Priority 1 — remove semantic ambiguity

### P1.1 Make `sem_topk auto` choose one path, not partially both

Do not allow one build to silently mix:

- bounded-pool final path
- operator-owned flat path

### P1.2 Convert builder incompatibility from degraded passthrough to fail-fast

Especially for:

- incompatible input shape
- impossible path/method/trigger combinations

Keep degraded passthrough only for real runtime failures.

### P1.3 Fix `llm_refine` naming or implementation

Current naming overstates the implementation.


## Priority 2 — remove transition debt

### P2.1 Freeze legacy flat config

- keep parsing support
- stop evolving it

### P2.2 Remove `sem_agg` legacy trigger mode from operator core

- translate legacy input in config layer
- keep operator internally query-spec-native


## Priority 3 — reduce duplicated planning state

### P3.1 Make runtime bundle the workflow assembly unit

Workflow config should carry bundles, not split fields.

### P3.2 Make A-path lowering either real or explicitly symbolic

Choose one:

- shared attribute-generation execution layer
- or documentation-only/public-user lowering API

Do not stay in the middle.


## Priority 4 — add missing validation

### P4.1 Add one runtime-level typed-config smoke path

Required:

- typed `RuntimeConfig`
- typed workflow builder
- actual DataStream run

### P4.2 Add restore/checkpoint-oriented stateful tests

At least for:

- `sem_topk`
- `sem_groupby`
- `sem_agg`

### P4.3 Add degraded-contract tests

Explicitly assert downstream handling of degraded records.


## Final Recommendation

The codebase is **not a rewrite candidate**.

But it is ready for one deliberate cleanup pass before any further feature work.

The right order is:

1. remove semantic ambiguity in `sem_topk auto` and degraded incompatibility handling
2. fix the `llm_refine` semantics/name mismatch
3. freeze legacy config evolution
4. promote runtime bundle to workflow assembly unit
5. add one real typed-config runtime smoke test

If these are done, the code becomes a much cleaner base for:

- planner/CBO integration
- naming/package cleanup
- V0.3 `sem_join`
