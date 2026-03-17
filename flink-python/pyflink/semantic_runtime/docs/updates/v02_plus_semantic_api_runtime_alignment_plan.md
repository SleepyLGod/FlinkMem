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

# V0.2+ Semantic Runtime API And Operator Alignment Plan

## 1. Purpose

This document records the decisions made after the first V0.2 implementation review.

The goal is not to replace the existing V0.2 stateful foundation. The goal is to align:

1. public semantic-operator names,
2. operator semantics,
3. scorer backends,
4. configuration structure,
5. package/file naming,
6. external retrieval expectations,

with the actual target system shape.

The most important conclusion is this:

- the current V0.2 codebase already has a workable keyed-state runtime foundation,
- but the public semantic API is still partially inconsistent with the intended CP-style operator model.

This document defines the next cleanup layer as **V0.2+**:

- still before true two-input semantic join,
- still before engine-level vector acceleration,
- but after the first stateful foundation exists.

## 2. Why A V0.2+ Layer Is Needed

Several mismatches now exist between the intended semantic API and the current code shape.

### 2.1 `sem_map` Is Too Narrow

Current `sem_map` in `operators/sem_map.py` is schema-bound:

- it always expects structured JSON,
- it always validates against `output_schema`,
- it behaves more like `sem_extract` than a general semantic map.

This is too narrow for the long-term API because `sem_map` must support:

- structured extraction,
- free-form transformation,
- rewriting / normalization,
- answer synthesis,
- memory distillation prompts.

Decision:

- keep one public operator name: `sem_map`,
- support two modes under the same API:
  - structured mode,
  - free-form mode.

### 2.2 `sem_join_retrieve` Is Operationally Correct But Publicly Misnamed

The current V0.1 join path is not a true two-input join.

Its actual shape is:

1. one input record arrives,
2. an external candidate set is retrieved,
3. the LLM judges semantic match against that candidate set.

This is closer to a lookup-style join than to a general `sem_join`.

Decision:

- rename the public operator to `sem_lookup_join`,
- reserve `sem_join` for the later true two-input semantic join.

### 2.3 `cts_retrieve` Is Runtime-Centric, Not User-Centric

`cts_retrieve` is a reasonable internal name for "continuous retrieval",
but it is not the cleanest public API name.

From the user perspective, the semantic intent is:

- search relevant memory/documents/items for the current semantic query.

Decision:

- expose this family as `sem_search`,
- allow the current `cts_retrieve` implementation to remain as the internal keyed-state runtime path during transition.

### 2.4 `sem_topk` Naming Is Backwards

Current V0.1 `sem_topk` is a bounded, per-record rerank over an already finite candidate list.

Current V0.2 `sem_topk_continuous` is the actual stream/stateful top-k maintenance path.

That means the more limited operator currently owns the more important name.

Decision:

- rename the V0.1 bounded operator to `sem_local_topk`,
- let the stream/stateful operator own the canonical public name `sem_topk`.

### 2.5 `sem_topk` Is Still Missing Real Semantic Scoring

The current stateful top-k implementation maintains a frontier over an existing score field.

This is a good state-management primitive, but it is not yet a complete semantic top-k operator.

The target `sem_topk` must support three scorer backends:

1. `llm`
2. `embedding`
3. `external_score`

Reason:

- retrieved candidates may not come with a meaningful ranking score,
- some pipelines want direct LLM reranking,
- some pipelines want embedding-based reranking,
- some pipelines want retrieval to supply scores externally.

### 2.6 Configuration Is Still Too Fragmented

The current codebase uses multiple typed dataclasses, which is good:

- `SemWindowConfig`
- `SemGroupbyConfig`
- `SemAggConfig`
- `CtsRetrieveConfig`
- `SemTopKConfig`

But there is no unified, user-friendly entry point yet.

Decision:

- keep typed operator configs,
- add a unified top-level configuration object later,
- support loading from YAML/JSON/env through that unified entry point.

### 2.7 Naming And Package Boundaries Have Drifted

Examples:

- `sem_agg_stateful.py`
- `sem_groupby_stateful.py`
- `sem_topk_continuous.py`
- `semantic_window.py`
- `cts_retrieve.py`

These names mix:

- public semantics,
- implementation style,
- runtime behavior,
- historical evolution.

Decision:

- converge toward a cleaner package and file naming strategy,
- separate row-local operators, stateful operators, workflow composition, and runtime utilities more clearly.

## 3. Locked Decisions

The following decisions are considered locked for planning purposes.

### 3.1 Public Operator Names

The public semantic operator surface should move toward:

- `sem_map`
- `sem_filter`
- `sem_search`
- `sem_topk`
- `sem_lookup_join`
- `sem_agg`
- `sem_groupby`
- `sem_window`

Supporting variant names:

- `sem_local_topk` for the bounded local rerank variant

Deferred names:

- `sem_join` remains reserved for true two-input semantic join

### 3.2 `sem_map` Keeps One Name, Two Modes

`sem_map` should not be split into separate public operator names only to represent
"structured" vs "free-form" output.

Instead:

- one operator name,
- one implementation family,
- two API modes.

### 3.3 `sem_topk` Must Own Stream Semantics

`sem_topk` is treated as naturally stream-oriented in the target system:

- it can still operate over a bounded candidate set,
- but its primary meaning is a continuously maintained semantic top-k operator.

Therefore:

- the current stateful top-k path becomes the canonical `sem_topk`,
- the current local/bounded path becomes `sem_local_topk`.

### 3.4 No Native Flink WindowFunction Rewrite Yet

The semantic operators should not be rewritten into native
`WindowFunction` / `ProcessWindowFunction` forms yet.

Reason:

- the higher-leverage problems are still API consistency, scorer backends, retrieval integration, and config structure,
- the current `KeyedProcessFunction` foundation is sufficient for V0.2 and V0.2+.

Decision:

- keep the current keyed state-machine route,
- defer native window wrappers / window-scoped variants until the operator API stabilizes.

### 3.5 External Retrieval Backend Must Become A First-Class TODO

The real test currently proves async semantic retrieval flow shape,
but it does not yet prove integration with a real external vector/search backend.

Decision:

- keep external retrieval backend selection open,
- make it explicit in plan/docs that the implementation needs a pluggable external search backend,
- do not prematurely lock to one external component before the surrounding application architecture is clearer.

## 4. Public API Direction

### 4.1 `sem_map`

Target direction:

```python
sem_map(
    prompt=...,
    output_schema=...,
    ...
)
```

or

```python
sem_map(
    prompt=...,
    output_schema=None,
    return_mode="text",
    ...
)
```

Meaning:

- if `output_schema` is present, structured mode is used,
- if `output_schema` is absent, free-form mode is used,
- both should still return a stable envelope rather than ad hoc raw values.

### 4.2 `sem_lookup_join`

Target direction:

- one input event,
- external candidate lookup,
- semantic predicate / scorer over `(left_record, right_candidate)` pairs,
- bounded join result output.

This keeps the current V0.1 implementation shape but gives it the correct public semantics.

### 4.3 `sem_search`

Target direction:

- semantic query or current message arrives,
- stateful/local cache retrieval is attempted first,
- external backend retrieval is used when needed,
- candidate set is emitted for downstream ranking / joining / answer synthesis.

Internal transition note:

- the current `cts_retrieve` runtime path can remain internally while the public operator name moves toward `sem_search`.

### 4.4 `sem_local_topk`

Target direction:

- bounded, row-local reranking over a finite candidate list already carried by the record,
- no keyed continuous state required,
- still useful as a lightweight V0.1 helper.

### 4.5 `sem_topk`

Target direction:

- keyed/stateful continuous top-k maintenance,
- accepts candidate streams or retrieval envelopes,
- owns semantic scoring/reranking responsibility when configured to do so.

Required scorer backends:

1. `llm`
2. `embedding`
3. `external_score`

## 5. Semantic Criterion Unification

The operator family needs a common semantic specification object.

This is the conceptual layer that unifies:

- prompt,
- predicate,
- scoring criterion,
- output mode,
- thresholds,
- backend choice.

Recommended direction:

```python
SemanticSpec(
    instruction=...,
    backend="llm" | "embedding" | "hybrid" | "rule",
    output_mode="bool" | "label" | "score" | "json" | "text" | "summary",
    schema=...,
    threshold=...,
)
```

Why this matters:

- `predicate` and `semantic criterion` are the same planning problem in different operator contexts,
- once this is unified, later optimizer work becomes cleaner,
- scorer backend switching becomes a runtime/config problem rather than a per-operator ad hoc patch.

## 6. Configuration Direction

The configuration system should move toward:

1. typed per-operator dataclasses,
2. one top-level runtime config object,
3. one loader layer for YAML/JSON/env.

Recommended structure:

- backend config:
  - LLM backend config
  - embedding backend config
  - retrieval backend config
- common runtime config:
  - async
  - ttl
  - timers
  - overflow defaults
  - metrics/audit
- operator config bundle:
  - `sem_map`
  - `sem_filter`
  - `sem_search`
  - `sem_lookup_join`
  - `sem_window`
  - `sem_groupby`
  - `sem_agg`
  - `sem_topk`
- workflow config:
  - routing
  - key selection
  - versioning
  - audit tags

This keeps the internal code strongly typed while giving the user one clean configuration entry point.

## 7. Runtime And Scorer Changes Required

### 7.1 `sem_topk`

The current stateful top-k path already provides:

- candidate state,
- frontier maintenance,
- timer-driven recomputation,
- delta/snapshot emission policy.

It still needs:

1. scorer backend abstraction,
2. LLM rerank path,
3. embedding rerank path,
4. explicit handling of candidates that arrive with no meaningful retrieval score.

### 7.2 `sem_search`

The current stateful retrieval path already provides:

- per-key cache,
- local retrieval path,
- async fallback path,
- timer-driven eviction.

It still needs:

1. explicit external backend abstraction,
2. real vector/search backend integration,
3. configurable search strategy selection,
4. cleaner public naming.

### 7.3 `sem_lookup_join`

The current V0.1 retrieval-backed join path already provides:

- async retrieval hook,
- async LLM join judgment,
- bounded candidate set,
- degraded/fallback behavior.

It still needs:

1. real external retriever implementations,
2. a cleaner public name,
3. eventual compatibility with `sem_search` outputs when composed in workflows.

## 8. Default Processing Style Clarification

The current V0.2 stateful operators are already mostly one-by-one by default:

- `sem_groupby`: one event in -> one assignment/update decision
- `sem_topk`: one update in -> recompute frontier immediately
- `sem_agg` algebraic mode: one event in -> one incremental reduction

The main exceptions are:

- `sem_window`, which is intentionally accumulate-then-seal
- `sem_agg` summarize mode, which is intentionally buffer-then-summarize

This means V0.2+ is not about changing the system from batch-style to one-by-one.
It is about making the one-by-one path semantically complete.

## 9. Package And Naming Direction

The package layout should move toward a clearer separation of concerns.

Recommended target layout:

- `row_ops/`
  - row-local async operators
- `stateful_ops/`
  - keyed/stateful semantic operators
- `workflow/`
  - composed multi-stage pipelines such as continuous RAG
- `runtime/`
  - async bridge, event model, timer helpers, metrics, config loaders

Recommended operator/file naming direction:

- `sem_map.py`
- `sem_filter.py`
- `sem_lookup_join.py`
- `sem_local_topk.py`
- `sem_window.py`
- `sem_groupby.py`
- `sem_agg.py`
- `sem_search.py`
- `sem_topk.py`

This naming strategy keeps public semantics primary and pushes implementation detail into package boundaries rather than file suffixes.

## 10. Explicit TODO: External Search Backend

This remains a required follow-up item.

The system must support a real external search backend for the case:

- continuous input arrives,
- each input must retrieve relevant documents/items from an attached external database,
- retrieved items are brought back into the stream,
- semantic join / ranking / answer synthesis is applied downstream.

The current real test validates:

- async retrieval control flow,
- merge-back behavior,
- answer path integration.

It does not yet validate:

- a real vector database,
- a real search index,
- application-specific external document storage.

Therefore the implementation plan must keep this as an explicit TODO rather than pretending the backend decision is already solved.

## 11. What V0.2+ Includes

V0.2+ should include:

1. public naming cleanup:
   - `sem_join_retrieve` -> `sem_lookup_join`
   - `cts_retrieve` -> public `sem_search`
   - `sem_topk` local -> `sem_local_topk`
   - `sem_topk` stateful -> canonical `sem_topk`
2. `sem_map` dual-mode API
3. scorer backend abstraction for `sem_topk`
4. `SemanticSpec`-style semantic criterion layer
5. unified top-level runtime configuration entry point
6. explicit external search backend TODO in plan/docs
7. package/file/class naming cleanup plan

## 12. What V0.2+ Does Not Include

V0.2+ does not imply:

1. true two-input semantic join
2. native Flink `WindowFunction`/`ProcessWindowFunction` rewrite
3. hard commitment to one external vector/search backend
4. engine-level vector acceleration
5. optimizer-level MOBO/CBO completion

Those remain later-phase work.

## 13. Relationship To The Existing Blueprint

These changes do not invalidate the blueprint's overall architecture.

They do, however, require blueprint updates in four places:

1. public operator naming,
2. `sem_topk` semantic ownership,
3. `cts_retrieve` vs `sem_search` distinction,
4. a new V0.2+ phase between the stateful foundation and true two-input join.

In other words:

- the old blueprint is directionally correct,
- but it now underspecifies the API cleanup layer needed after first V0.2 delivery.
