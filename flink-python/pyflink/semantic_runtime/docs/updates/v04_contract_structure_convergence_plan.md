# V0.4 Contract + Structure Convergence Plan

## Goal

Close the remaining contract/structure gaps before optimizer-forward work:

1. keep semantic behavior unchanged,
2. keep default path LLM-first,
3. keep public API backend-agnostic,
4. remove ambiguous contracts and duplicated runtime mechanics.

This plan supersedes conflicting pending TODOs in older V0.4 drafts for this
implementation wave.

## Hard Rules

1. No fallback/degrade compatibility shims.
2. Let it crash on invalid contract.
3. No hidden semantic behavior change.
4. No magic numbers in logic paths.
5. Every plan-layer claim must match runtime behavior.

## First-Principles Clarifications

### A) Stateful sem-op model

Stateful semantic operators are continuous keyed operators. Scope is only a
bounded input/update mechanism; it is not operator identity.

### B) Pushdown model still exists

Pushdown still exists, but only where semantic output is a stable derived
attribute under the chosen contract. Continuous stateful semantics remain
native runtime paths.

### C) `sem_xxx` vs `sem_xxx_pipeline`

1. `sem_xxx`: keyed state machine kernel (canonical operator semantics).
2. `sem_xxx_pipeline`: orchestration/lowering wrapper (input shape routing,
   scorer/reranker workers, async topology composition).

## Phase Plan

## Phase 0: Contract Closure (Must do first)

1. `persistence_policy="hybrid"` handling:
   - reject it fail-fast everywhere.
   - only `persistent_across_scopes` and `reset_per_scope` are valid.
2. Resolve `sem_join` wording conflicts:
   - remove `window-owned join` language from query-scope docs if runtime is
     canonical two-input continuous join.
3. Align `resolve_join_lowering_plan` with current runtime truth:
   - no "derived attribute then classical join" claim when runtime is native
     two-input stateful.
4. Ensure backend contract layering is explicit:
   - public query contract defaults to LLM,
   - internal kernel keeps embedding/external paths for future CBO,
   - `sem_agg` keeps LLM summarize/compressive only in this wave (embedding
     backend explicitly deferred).

Acceptance:

1. No contradictory contract text across runtime/spec/docs/plans.
2. Join lowering metadata no longer misrepresents runtime semantics.

## Phase 1: Plan + Doc Convergence

1. Declare one canonical V0.4 execution plan for active implementation.
2. Move conflicting completed/superseded V0.4 drafts to `docs/history`.
3. Keep only active TODOs that still map to code work.

Acceptance:

1. No cross-doc contradiction on `window_join pairing` status.
2. One authoritative plan entrypoint for contributors.

## Phase 2: Shared Runtime Mechanics Extraction

1. Extract shared scope-boundary decision primitive used by `sem_groupby` and
   `sem_agg` (`session` / `tumbling` / `semantic`).
2. Extract shared trigger-resolution helper duplicated across
   `sem_topk` and `sem_topk_scope_runtime`.
3. Keep semantic apply logic operator-local.

Acceptance:

1. Reduced duplicate branch logic.
2. No behavior delta in existing operator tests.

## Phase 3: `sem_join` Timer Registration Efficiency (Deferred)

Status: recorded in V0.4 plan, not implemented in current wave.

1. Replace per-event processing-time timer registration with per-key
   `next_finalize_due_ms` discipline.
2. Preserve exact finalize semantics (no accuracy/semantic drift).
3. Keep async pending poll timers unchanged except where unified by same
   deadline discipline.

Acceptance:

1. Identical output semantics in join regression/e2e tests.
2. Reduced timer registration frequency under high-throughput keys.

## Phase 4 (Optional): File Naming and Module Layout Cleanups

1. Rename misleading modules where runtime intent is clearer
   (e.g. scoped persistent vs purely window-owned naming).
2. Canonical naming for stateful operators:
   - `sem_xxx_kernel.py`
   - `sem_xxx_pipeline.py`
   - `sem_xxx_worker.py`
   - `sem_xxx_bounded.py`
3. Keep `sem_xxx` kernel + `sem_xxx_pipeline` orchestration split.
4. Split oversized files only by responsibility, not by arbitrary size.

Acceptance:

1. Improved discoverability with no semantic change.

## Verification Matrix

1. Unit tests for contract resolution/lowering metadata.
2. Stateful operator regressions (`groupby/topk/agg/join/window`).
3. E2E integration full suite.
4. Focused perf smoke for join timer scheduling (registration-count oriented).

## Out of Scope (This Wave)

1. Workflow optimizer strategy implementation.
2. New semantic algorithms beyond current contract.
3. Public API expansion for backend knobs.
