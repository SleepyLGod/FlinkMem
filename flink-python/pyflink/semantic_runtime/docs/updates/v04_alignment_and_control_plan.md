# V0.4 Alignment And Control Plan

## Goal

V0.4 starts after V0.3a has already delivered:

1. true two-input `sem_join`,
2. reusable internal semantic execution steps,
3. shared window materialization,
4. standard-window `sem_join(window)` for:
   - `tumbling`,
   - `sliding`,
   - `session`.

V0.4 should not begin with global optimizer machinery. It should first create
the clean optimization space that CP-style planning actually needs.

That means:

1. operator variants first,
2. planner foundation second,
3. workflow-level optimization third.

The two still-open semantic questions remain blocked TODOs:

1. semantic-window cross-stream pairing for `window_join`,
2. `sem_topk` scoring and rerank consistency across deltas.

Those problems stay explicit and fail-fast until their contracts are chosen.

## Rule

V0.4 must continue following the same project rules:

- public API stays facade-only,
- internal execution knobs stay internal,
- no compatibility shims,
- no degrade/fallback paths,
- let it crash when the contract is not yet defined.

## Scope Split

## Foundational Model

The active V0.4 work should use one clarified runtime model:

1. semantic stateful operators are **continuous queries**,
2. `window` is a **scope provisioning strategy**, not operator identity,
3. state persistence is a **separate internal policy**.

This is consistent with Flink's own execution model:

1. `ProcessFunction` / `KeyedProcessFunction` are invoked for each event and
   own keyed state and timers,
2. `KeyedCoProcessFunction` is invoked for each event on either input and
   owns keyed state and timers across both inputs,
3. windows split infinite streams into finite buckets and then apply a window
   function to those buckets.

Therefore V0.4 should not treat:

- `window_owned`
- `operator_owned`

as two different semantic operators.

Instead, it should treat them as two different ways of sourcing the current
bounded scope for one continuous operator.

### Scope source

There are two clean ways to source the current bounded scope:

1. **external / system-provided scope**
   - native Flink window assigners,
   - `sem_window`,
   - any other upstream bounded-set materializer,
2. **internal / operator-maintained scope**
   - keyed state,
   - TTL,
   - timers,
   - operator-local boundary logic.

### State persistence

State persistence must be modeled separately from scope source:

1. **per-scope reset**
   - state is scoped to one bounded set and then cleared,
2. **cross-scope persistent**
   - state survives across externally materialized scopes,
3. **hybrid (deferred)**
   - reserved design direction; not accepted as a runtime `persistence_policy`
     in the current wave.

This matters because an externally provided window does **not** imply that the
operator must forget its state at every window boundary.

The same continuous operator may:

1. consume externally materialized windows,
2. update persistent keyed state across them,
3. emit per-trigger bounded results.

### Pushdown consequence

Pushdown / lowering should follow the semantic step, not the scope source.

That means:

1. `sem_match` is still the matching step,
2. `sem_score` is still the scoring step,
3. `sem_rerank` is still the reranking step,
4. continuity / labeling / assignment steps remain the same semantic steps.

What changes with scope source is:

1. which bounded items are visible at a given trigger,
2. when those steps run,
3. how state is reset or preserved.

What should **not** change is the operator's semantic identity.

### In Scope For Active V0.4 Work

1. CP-style operator variants for `sem_window`,
2. CP-style operator variants for `sem_groupby`,
3. planner/runtime foundation for variant selection,
4. execution-layer support for batching/fusion where the contract is already clean,
5. profiling and planning primitives needed before workflow optimization.

### Explicitly Deferred TODOs

1. semantic-window `window_join` pairing contract,
2. implementation of `sem_join(window_kind="semantic")`,
3. `sem_topk` scoring consistency contract,
4. `sem_topk` rerank consistency contract,
5. `sem_join` timer registration efficiency optimization (recorded in V0.4 plan, deferred after functional convergence).

These remain V0.4 topics, but they are not the first active implementation
step unless their contracts are explicitly chosen.

---

## Phase 1: Operator Variants

The first V0.4 phase should add the operator-implementation alternatives that
CP-style optimization needs.

Without these variants, later planner/optimizer work has little real choice to
make.

### 1.1 `sem_window` Variants

Add an internal variant family for semantic continuity / segmentation:

1. `pairwise`
   - compare the incoming tuple with the previous tuple,
2. `summary`
   - compare the incoming tuple against the current window summary,
3. `embedding`
   - compare the incoming tuple against one window representative or cluster state,
4. `all_history`
   - compare the incoming tuple against the active window as a whole and decide
     whether it still belongs to that window.

#### Current gap

The current `sem_window` runtime is not yet this CP-style variant family.

Today it is primarily:

1. one keyed window state machine,
2. one event buffer,
3. one timer/flush path,
4. one boundary-flag-driven semantic close path.

That is a clean window materializer, but it is not yet a clean family of
continuity implementations.

#### Contract rule

Public API does not change:

```python
sem_window(context=...)
```

Variant choice stays internal.

#### Runtime rule

Keep one window state machine, but separate:

1. window state ownership,
2. continuity judgement.

That means:

- `SemWindowFunction` should remain the owner of:
  - buffer state,
  - timer/flush logic,
  - snapshot emission,
- while continuity scoring becomes an internal execution variant.

#### Why this is the right scope

CP's semantic-window optimization is not mainly about changing the public
window contract. It is about changing how continuity is judged inside one
stateful window runtime.

#### Variant-specific analysis

##### `pairwise`

Contract:

```text
(prev_event, current_event) -> continue current window ? or open new window
```

Strengths:

1. smallest continuity contract,
2. easiest LLM-based variant to reason about,
3. no extra summary state required.

Problems:

1. LLM cost can be high if every incoming event may trigger a judgement,
2. local noise can cause over-segmentation,
3. in this system, an honest LLM implementation creates a pending-decision
   problem:
   - the event arrives,
   - continuity judgement is still in flight,
   - the runtime must not guess whether to append or split.

Implementation implication:

- pairwise LLM continuity should ultimately use an async bridge / merge-back path,
- timer callbacks must remain local-only,
- the runtime must not invent provisional continuity outcomes,
- before full optimizer work exists, the minimal execution improvement should be:
  - simple concurrent / batched LLM dispatch where the contract is already clear,
  - while keeping more advanced batching/fusion policy as later work.

##### `summary`

Contract:

```text
(window_summary, current_event) -> continue current window ? or open new window
```

Strengths:

1. more topic-centric than pairwise,
2. less sensitive to purely local noise,
3. better matches the intuition of "does this event still belong to the
   current semantic segment?".

Problems:

1. this is the hardest variant to implement cleanly,
2. it requires explicit summary state,
3. it also requires an explicit summary update policy,
4. if summary maintenance itself is semantic, the operator now contains two
   semantic sub-problems:
   - continuity judgement,
   - summary maintenance.

Implementation implication:

- this must not be treated as "just another prompt",
- it needs explicit internal state for `window_summary`,
- it should be implemented only after the simpler variants are already clean,
- like pairwise, it should ultimately move to an async execution path,
- simple batching / concurrent dispatch can exist before the later optimizer
  phases, but more complex policy belongs later.

##### `embedding`

Contract:

```text
(window_representative_or_centroid, current_event) -> similarity score
```

Strengths:

1. local and cheap,
2. deterministic compared with LLM variants,
3. does not require async merge-back,
4. the cleanest fast variant for planner/runtime use later.

Problems:

1. threshold calibration matters,
2. representative or centroid drift matters,
3. it may over-split or over-stick,
4. its decisions are weaker semantically than LLM reasoning.

Implementation implication:

- this should be implemented as a local continuity scorer inside the same
  state machine,
- it should not create a second operator shape.

##### `all_history`

Contract:

```text
(active_window_events, current_event) -> continue current window ? or open new window
```

Current preferred rule:

1. first check the local hard window-size threshold,
2. only if the active window is still under that threshold,
3. send the full active window plus the current event to the semantic
   continuity judgement.

That keeps the hard size bound local and deterministic.

Strengths:

1. this directly matches the first-principles window-membership question,
2. it does not rely on only one previous tuple,
3. it can be more robust than pure pairwise continuity.

Problems:

1. it is the most expensive continuity shape if implemented naively,
2. it overlaps semantically with `summary`,
3. it requires an explicit contract for how much window history is exposed:
   - full history,
   - truncated history,
   - representative subset,
4. if the active window grows large, batching and prompt-size constraints
   become a real execution issue.

Implementation implication:

- this is a legitimate `sem_window` variant,
- the default history contract should be **full active window history**,
- future truncation / representative-subset support may exist, but only as an
  explicit internal execution option above this variant, not as a silent
  default,
- the local hard window-size threshold should be checked before any LLM call,
- the current implementation now supports this variant with full-history
  membership judgement and pre-LLM hard-size rollover.

#### Clean implementation rule

Do not create three separate `sem_window` operators.

The clean structure is:

1. one `SemWindowFunction` that owns:
   - event buffer,
   - open/close state,
   - timers,
   - snapshot emission,
2. one internal continuity execution layer,
3. multiple continuity variants behind that layer.

The continuity layer should answer only:

```text
given current window state and one event, continue or open a new window?
```

It should not own:

1. timer registration,
2. snapshot emission,
3. buffer lifecycle.

#### Recommended implementation order

Do not implement the variants in paper order. Implement them in system order:

1. `pairwise`
2. `embedding`
3. `summary`
4. `all_history`

Reason:

1. `pairwise` is the smallest LLM-based continuity contract and validates the
   async bridge cleanly,
2. `embedding` provides the cheap deterministic local variant,
3. `summary` is the most stateful CP variant and the easiest to make messy, so
   it should come after the simpler variants,
4. `all_history` is a real first-principles method, now implemented with a
   full-history default contract, and it must remain separate from `summary`.

#### Relation to semantic-window join

Improving `sem_window` does **not** by itself solve cross-stream
semantic-window join.

Why:

1. `sem_window` solves segmentation:
   - how one stream is cut into bounded semantic windows,
2. `window_join` still needs alignment:
   - which left semantic window should pair with which right semantic window.

So better `sem_window` variants improve the quality of each stream's own
segmentation, but they do not define cross-stream pairing.

That pairing problem remains a separate V0.4 TODO and must not be smuggled
into `sem_window` implementation.

#### Ownership model

CP-style `sem_window` variants are inherently operator-owned / self-state.

`sem_window` is the layer that *produces* windows. It is not itself a
window-owned consumer.

#### Async execution note

The current `sem_window` baseline should remain **synchronous** until the
operator family and workflow baselines are fully implemented and benchmarked.

Reason:

1. `sem_window` owns the canonical segmentation state,
2. LLM-based continuity results (`pairwise`, `summary`, `all_history`) must
   merge back into that same state,
3. the existing generic async bridge pattern is a one-way
   `side-output -> async -> merge` topology and does not, by itself, preserve
   one single canonical segmentation owner for `sem_window`.

Therefore V0.4 should defer a **dedicated `sem_window` async topology** until
the end of the phase.

That later async topology should:

1. keep exactly one canonical semantic-window state owner,
2. reuse the common async work/result envelope types where possible,
3. avoid split-brain state between an upstream `SemWindowFunction` and a
   downstream merge operator,
4. support simple batching / concurrent dispatch before any later optimizer
   policy.

This async topology is a real V0.4 task, but it should come **after**:

1. faithful `sem_window` variants,
2. the remaining semantic operators,
3. baseline workflow / benchmark runs on the default full-LLM path.

### 1.2 `sem_groupby` Variants

Add an internal variant family for dynamic semantic grouping:

1. `llm_basic`
   - incremental assignment:
     - existing group
     - or new group
2. `llm_refine`
   - incremental assignment plus periodic or close-time refinement:
     - merge
     - split
     - rename
3. `embedding`
   - embedding-driven incremental clustering,
   - optional naming/explanation pass.

#### Contract rule

Public API does not change:

```python
sem_groupby(intent=..., context=...)
```

The contract remains:

```text
event + existing_groups -> existing_group | new_group
```

Variant choice stays internal.

#### Runtime rule

Do not collapse `sem_groupby` into a fake `sem_label` operator.

Keep the current contract, and express variants as different internal
assignment / maintenance implementations.

#### Important note

The current runtime already has:

- `rule`,
- `embedding`,
- `llm`,
- local maintenance hooks.

V0.4 should not replace that with marketing names only. It should make the CP
variant family explicit and complete the missing refinement semantics where
needed.

#### Ownership model

CP-style `sem_groupby` optimization is centered on operator-owned state:

1. evolving groups,
2. incremental assignment,
3. optional refinement over active state.

`window-owned sem_groupby` remains valid as a bounded specialization, but it is
not the main semantic shape of the CP-style optimization space.

### 1.3 What Phase 1 deliberately does not include

1. semantic-window `window_join` pairing,
2. top-k consistency policy,
3. MOBO,
4. global workflow optimization.

These need either:

- a later contract choice,
- or a planner foundation that does not yet exist.

### Phase 1 Completion Criteria

Phase 1 is done only when:

1. `sem_window` has real internal variants,
2. `sem_groupby` has real internal variants,
3. those variants are represented explicitly in internal plans,
4. none of this leaks into the public API.

---

## Phase 2: Planner Foundation

Only after operator variants exist should V0.4 add planner/runtime foundation.

This phase is not yet the full workflow optimizer. It is the layer that makes
workflow optimization possible later.

### 2.1 Variant Registry

Add an internal capability registry describing:

1. which operator has which variants,
2. which variants support batching,
3. which variants support fusion,
4. which variants are lowerable or partially lowerable,
5. which variants require native runtime.

### 2.2 Profiling Hooks

Add profiling interfaces for:

1. throughput,
2. latency,
3. accuracy proxy,
4. token/cost accounting where relevant.

This should happen at operator / variant granularity, not only at pipeline
granularity.

### 2.3 Plan-Space Representation

Add internal plan representation for:

1. operator variant choice,
2. batch size,
3. fusion grouping,
4. path capability flags.

This is where future workflow optimization will get its legal search space.

### 2.4 Constraint / Pruning Foundation

Add the non-negotiable pruning rules that do not depend on optimizer heuristics,
for example:

1. incompatible fusion pairs,
2. unsupported batching for a given variant,
3. invalid window/trigger combinations,
4. native-only path constraints.

### 2.5 What Phase 2 deliberately does not include

1. adaptive plan switching,
2. MOBO,
3. semantic-window `window_join` pairing policy,
4. top-k consistency policy.

These are higher-level choices. This phase only builds the substrate.

### Phase 2 Completion Criteria

Phase 2 is done only when:

1. operator variants are first-class internal planning objects,
2. batching/fusion capabilities are explicit,
3. plan legality can be checked without guessing,
4. later optimizer work has a clean substrate.

---

## Phase 3: Workflow Optimization

Only after Phases 1 and 2 should V0.4 add CP-style workflow optimization.

### 3.1 Plan Generation

Enumerate candidate workflow plans over:

1. operator variants,
2. batching choices,
3. fusion choices,
4. hybrid combinations.

### 3.2 Plan Pruning

Prune:

1. illegal plans,
2. semantically inconsistent plans,
3. obviously dominated or unsupported plans.

### 3.3 Cost / Accuracy Estimation

Introduce model-driven estimation for:

1. throughput,
2. latency,
3. accuracy proxy,
4. cost.

### 3.4 Pareto Selection

Choose non-dominated plans and keep the optimizer objective explicit:

1. accuracy,
2. throughput,
3. latency,
4. cost.

### 3.5 MOBO / Probe Budget Search

Only after the above exists should the system add:

1. sparse probing,
2. acquisition strategy,
3. cost-aware exploration,
4. online plan re-selection.

### 3.6 Fusion / Adaptive Execution

This phase is also where the remaining execution-strategy family belongs:

1. tuple batching beyond fixed execution blocks,
2. operator fusion,
3. adaptive batching,
4. path switching,
5. quality/latency/cost-driven plan choice,
6. broadcast-driven dynamic control.

### Phase 3 Completion Criteria

Phase 3 is done only when:

1. the optimizer is choosing among real operator variants,
2. plan generation and pruning are explicit,
3. cost/accuracy estimation is grounded in variant-level profiling,
4. adaptive control is built on an explicit contract, not on guessed semantics.

---

## Blocked TODOs

These remain outside the active execution order until their contracts are
chosen.

### TODO 1: `window_join` Semantic-Window Pairing

The runtime must continue failing explicitly for:

```text
sem_join(window_kind="semantic")
```

until one pairing contract is chosen.

The system must not guess:

1. one-to-one pairing,
2. overlap pairing,
3. anchor-stream pairing,
4. latest-window pairing,
5. trigger-side pairing.

### TODO 2: `sem_topk` Consistency Contract

The runtime must not pretend that:

1. pointwise LLM scores are globally calibrated across time,
2. pairwise/listwise rerank results are interchangeable,
3. embedding ranking and LLM ranking have the same stability properties.

This remains a later V0.4 contract choice.

---

## Final Recommendation

V0.4 should be treated as:

1. **first, the operator-variant phase**,
2. **second, the planner-foundation phase**,
3. **third, the workflow-optimizer phase**,

while keeping:

1. semantic-window `window_join`,
2. top-k consistency semantics,

as explicit blocked TODOs until their contracts are chosen.
