# V0.4 Stateful Async Convergence Plan

## Goal

Converge `sem_groupby`, `sem_agg`, and `sem_topk` to one clean stateful model:

1. operator is continuous and state-owning,
2. scope is a bounded working-set source,
3. trigger decides when to compute/emit,
4. async execution is explicit and non-blocking for the state owner.

This plan keeps `window-owned` specializations, but treats them as bounded
specializations rather than default execution path.

## Hard Constraints

1. No public semantic contract change.
2. No compatibility shim.
3. No hidden fallback/degrade behavior.
4. Let it crash on invalid or unsupported contract.
5. Accuracy must not be reduced by runtime refactor.
6. LLM-call cardinality must remain unchanged for equivalent input/trigger
   semantics.

## First-Principles Model

### 1) Scope vs Trigger vs State

1. Scope answers: which tuples belong to the current bounded working set.
2. Trigger answers: when to compute, maintain, or emit on the active set.
3. State answers: what must persist across time and scope transitions.

Mixing these three concerns causes correctness and latency issues.

### 2) Why per-key serial visibility is mandatory

For stateful semantic operators, the keyed state is not immutable lookup data.
It is mutable semantic memory.

Example (`sem_groupby`, same key):

1. `t1` arrives, starts assignment request `R1` against current groups `G0`.
2. `t2` arrives immediately, starts assignment request `R2`.
3. If `R2` writes before `R1`, then `t2` may create/update groups as if `t1`
   never happened.
4. When `R1` later writes, it mutates catalog based on stale view `G0`.

Result:

1. final group catalog depends on completion race, not event order,
2. maintenance (merge/split/rename) observes non-deterministic state,
3. output is unstable and hard to debug.

Therefore each key needs:

1. single-flight apply (or equivalent serializable write discipline),
2. version/epoch guard for async results,
3. deterministic apply order.

Operationally, this means:

1. same key may still receive many input events quickly,
2. but only one remote semantic request for that key may be authoritative at a
   time,
3. later events for that key must queue behind the in-flight request or remain
   buffered until the previous result is applied,
4. cross-key concurrency is still allowed and desired.

## Current Runtime Facts

### sem_groupby

1. `LLMClient` is opened once per operator instance.
2. sync wrappers create a new event loop per call.
3. owner thread still blocks until assignments/refinement return.

Implication:

1. chunk calls may run concurrently inside one loop,
2. but owner-side blocking remains.
3. same-key assignment/refinement still executes on the keyed owner hot path.

### sem_agg

1. summarize/compressive path uses internal `ThreadPoolExecutor`.
2. completion is polled by timer.
3. helper currently creates/tears down client per summary task.

Implication:

1. extra latency floor from polling interval,
2. extra overhead from per-request client lifecycle.
3. the semantic single-flight idea is correct, but the transport/execution
   placement is not yet ideal.

### sem_topk

1. major scoring/rerank paths already use `AsyncDataStream.unordered_wait`.
2. state owner is comparatively clean.

Implication:

1. async shape is closer to target,
2. still needs family-level primitive convergence.

## Current Decision (A) — 2026-03-27

The runtime now explicitly follows option A:

1. `persistent_across_scopes` stateful paths keep owner-internal async control
   (including polling where currently used).
2. standard Flink async pushdown is used on no-feedback bounded paths
   (`window` + `reset_per_scope`).
3. no attempt is made in this phase to force a closed-loop
   `dispatch -> async worker -> keyed apply -> redispatch` inside one PyFlink
   DataStream job.

### Progress Snapshot

1. `sem_agg` bounded no-feedback path (`window + reset_per_scope`) now supports
   async summarize/compressive pushdown.
2. `sem_groupby` persistent default path now supports owner-nonblocking async
   assignment/refine dispatch with per-key serial apply guards.
3. `sem_topk` remains on existing async scoring/rerank pipeline.

Reason:

1. current PyFlink DataStream API has no iterate/feedback primitive for this
   loopback shape.
2. forcing it now would require bigger architecture changes (cross-job/external
   feedback channel), outside this convergence phase.

## Why "external async wait + keyed apply"

This means:

1. keep the keyed operator as the only state writer,
2. move expensive remote semantic calls to async worker operators,
3. return result envelope back to keyed apply stage,
4. keyed apply validates version/epoch and mutates state.

Concretely:

1. keyed owner dispatches request `R(version=v, epoch=e)`,
2. async worker performs the remote semantic call,
3. completion event returns with the same `v/e`,
4. keyed apply stage writes the result only if the current keyed state still
   matches `v/e`,
5. otherwise the result is stale and must be dropped.

Compared with "internal threadpool + polling":

1. less owner-thread waiting overhead,
2. no poll-interval completion tax,
3. clearer separation of responsibilities,
4. easier to reuse across operators.

## Friend's Concern: Client/Event Loop Overhead

The concern is valid.

1. New event loop per sync wrapper call is overhead.
2. Per-request client creation/close is larger overhead.
3. sync wrapper over async can become a bottleneck under load.

Preferred direction:

1. long-lived async client/session per operator task (or per worker instance),
2. event loop owned by async runtime operator, not per request,
3. state owner does not run blocking sync wrappers on hot path.

### Current Status

1. The above direction is accepted and recorded in V0.4.
2. It is **not fully implemented yet**.
3. Current baseline remains functional and correctness-first.
4. Optimization work is deferred to the V0.4 async convergence phases below.

`concurrent.futures` is still useful for CPU-bound local work, but remote I/O
LLM paths should be async-first with reusable transport session.

### SDK Integration Cases

There are only two clean cases:

1. async SDK available
   - preferred case,
   - worker owns long-lived async client/session,
   - no per-request event loop creation,
   - no sync wrapper on owner hot path.
2. only sync SDK available
   - worker may use a bounded threadpool,
   - still keep owner thread non-blocking,
   - still keep client/session reuse if SDK supports it,
   - this is second-best because thread count and blocking I/O become capacity
     constraints.

The critical rule is not "async everywhere".
The critical rule is:

1. remote semantic I/O must not block the keyed state owner,
2. keyed state writes must remain serial and deterministic per key.

### Optimizer-Plan Tie-In (Deferred)

This async convergence plan is a prerequisite for the latest V0.4 optimizer
work, but not part of optimizer/CBO rollout itself.

1. First complete stateful async convergence for `sem_groupby`, `sem_agg`,
   `sem_topk`.
2. Then hook optimizer/runtime plan switching on top of the converged async
   skeleton.
3. Keep optimizer expansion out of this implementation track until the stateful
   runtime ownership model is stable.

## Four-Phase Execution Plan

## Phase 1: Shared Stateful Runtime Primitives

Goal: remove repeated control logic before moving async boundaries.

### Scope

1. Extract shared `ScopeBoundaryRuntime`:
   - session/tumbling/sliding/semantic decisions,
   - pre-reset/post-reset planning.
2. Extract shared `TriggerTimerDriver`:
   - on_event/periodic/idle/count/on_scope_close timer mechanics.
3. Extract shared `AsyncResultEnvelope` schema:
   - `request_id`, `key`, `scope_epoch`, `state_version`, `trigger_reason`,
     `payload`.
4. Extract shared `AsyncApplyGuard`:
   - stale/duplicate/out-of-order envelope rejection.
5. Extract shared per-key mailbox / single-flight contract:
   - one authoritative in-flight request per key,
   - buffered follow-up work,
   - explicit apply-after-completion discipline.

### Acceptance

1. No operator behavior change.
2. Existing stateful tests remain green.

## Phase 2: sem_groupby Canonical Asyncization

Goal: keep semantic behavior identical, remove owner-side blocking.

### Scope

1. Split into three stages:
   - owner-dispatch,
   - async-assign/refine worker,
   - keyed-apply.
2. Keep assignment chunk policy identical (`assignment_batch_size` semantics).
3. Keep refinement trigger semantics identical.
4. Enforce one-key serial apply discipline.
5. Reuse long-lived worker client/session.
6. Treat assignment and refinement as the same keyed mailbox family:
   - no refine apply while assignment is in flight for that key,
   - no assignment apply while refine is mutating that key.
7. Preserve request basis exactly:
   - assignment request sees the same group-state snapshot it would have seen in
     the synchronous baseline,
   - apply is rejected if that basis has become stale.

### Non-Goal

1. No change to prompt contract.
2. No change to assignment/refine call counts for same chunking + triggers.

### Acceptance

1. Deterministic per-key output under artificial async jitter.
2. Lower owner blocking time.
3. No accuracy drift relative to baseline path.

## Phase 3: sem_agg Async Path Convergence

Goal: preserve fold semantics, remove polling tax and per-request client churn.

### Scope

1. Replace internal executor+poll with external async wait + keyed apply.
2. Keep one pending summarize per key (single-flight gate).
3. Keep summarize/compressive semantic update contract:
   - `current_summary + added_events -> updated_summary`.
4. Reuse long-lived async client/session on worker side.
5. Keep delta semantics exact:
   - request is formed from the same `current_summary` and `added_events` as the
     synchronous baseline,
   - only one summary update may be authoritative per key at a time.
6. Remove per-request client construction from the hot path.

### Non-Goal

1. No change to agg semantic contract.
2. No change to trigger semantics.
3. No change to summary call cardinality per trigger.

### Acceptance

1. same semantic output on deterministic fixtures,
2. reduced completion latency variance versus poll path,
3. no stale result apply across scope epochs.

## Phase 4: Family Convergence + sem_topk Alignment

Goal: unify the three operators on same control/runtime skeleton.

### Scope

1. Align `sem_topk` envelopes and apply guards with phase-1 primitives.
2. Deduplicate external-scope progress tracking utilities.
3. Keep `window-owned` operators, but mark planner default to persistent paths.
4. Consolidate metrics:
   - dispatch latency,
   - async service time,
   - apply latency,
   - stale-drop count,
   - per-key pending depth.
5. Normalize runtime ownership model across all three operators:
   - owner writes keyed state,
   - worker performs remote semantic call,
   - apply stage validates and commits.

### Acceptance

1. one consistent internal runtime pattern across all three operators,
2. no public API changes,
3. no hidden fallbacks,
4. deterministic keyed semantics under async load.

## Testing And Verification Plan

### Correctness

1. per-key determinism with shuffled completion order,
2. stale envelope rejection by epoch/version,
3. trigger equivalence regression (`on_event`, `periodic`, `idle_flush`,
   `count_threshold`, `on_scope_close`),
4. external window + persistent cross-scope consistency.

### Accuracy

1. golden fixtures for same prompt inputs and chunk policy,
2. compare output equality / rank equality / group assignment equality,
3. verify no extra or missing LLM calls per operator per trigger scenario.

### Performance

1. owner thread busy-time reduction,
2. end-to-end p50/p95/p99 latency comparison,
3. throughput under sustained load with bounded pending depth,
4. connection/client reuse effectiveness,
5. overhead comparison:
   - per-request event loop,
   - per-request client creation,
   - long-lived worker client/session.

## Redundancy Cleanup Checklist

1. Remove duplicated scope/timer code from each operator.
2. Remove per-step sync wrappers on owner hot paths.
3. Remove per-request client construction in remote semantic calls.
4. Keep bounded specializations, but avoid duplicate business logic where
   shared kernels can be reused.

## Explicit Non-Goals

1. No `sem_join` semantic-window pairing contract work in this plan.
2. No optimizer/CBO strategy expansion in this plan.
3. No public API expansion for internal async knobs.

## Exit Criteria

Plan is complete when:

1. three operators share one stateful async skeleton,
2. semantics and accuracy are unchanged,
3. owner blocking on remote calls is eliminated,
4. tests and benchmarks show stable deterministic behavior and improved latency.
