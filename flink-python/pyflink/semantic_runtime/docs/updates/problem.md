# Open Problems After V0.3

## Goal

This document records the major open semantic-runtime problems that remain
after the current V0.3 implementation work.

---

## 1. `sem_topk` Scoring And Reranking Consistency

### 1.1 What the current implementation already does

The current system already distinguishes two layers:

1. row-level `sem_local_topk`
   - bounded items
   - one-shot ranking
2. stateful `sem_topk`
   - continuous query
   - maintain a top-k frontier over a changing candidate pool

The current runtime also separates internal execution steps:

- `sem_score`
- `sem_rerank`

That separation is correct.

What remains unresolved is not the existence of these steps.

What remains unresolved is:

- score stability,
- ranking consistency across deltas,
- consistency between pointwise, pairwise, and listwise strategies.

### 1.2 Why pointwise LLM scoring is difficult

Pointwise semantic ranking looks simple:

```text
item -> score -> top-k
```

But if the scorer is an LLM, the score itself may not be stable:

- score(A) when A is asked first may differ from score(A) when asked later,
- score(A) and score(B) may not be calibrated well enough for direct global sorting,
- delta arrivals can cause ranking drift that is not explained by item quality alone.

So a stateful `sem_topk` that relies only on independent LLM scores may have a
consistency problem:

- new arrivals get judged under slightly different latent standards than old arrivals.

That means pointwise LLM scoring may need:

- extra prompt discipline,
- calibration rules,
- periodic rerank,
- or explicit consistency contracts.

### 1.3 Why pairwise and listwise do not eliminate the problem

Pairwise and listwise are not magical fixes.

They move the problem, they do not remove it.

#### Pairwise

Typical contract:

```text
which item is better, A or B?
```

Advantages:

- relative preference is often more stable than absolute scoring,
- useful for hard boundaries near top-k cutoffs.

Problems:

- too many comparisons,
- aggregation is nontrivial,
- cycles can appear,
- graph/tournament inference becomes part of execution.

#### Listwise

Typical contract:

```text
rank this bounded list
```

Advantages:

- can model interactions inside the candidate set,
- often produces better local ordering.

Problems:

- limited by context window size,
- often needs chunked listwise execution,
- chunk-level rankings then need another merge layer,
- different chunkings can produce different global rankings.

So even after moving from pointwise to pairwise/listwise, consistency remains a
real systems issue.

### 1.4 Why embeddings are different

Embedding-based ranking is different in one crucial way:

- the score is normally deterministic given the same encoder and vectors,
- it is independent of the arrival order of the request batch.

That does not mean embedding ranking is always semantically better.

It does mean:

- ordering stability is easier,
- delta behavior is easier to reason about,
- score calibration is less fragile than LLM free-form scoring.

So if the question is specifically:

- "does the order of arrivals change the result?"

then embedding-based ranking is fundamentally easier than LLM scoring.

### 1.5 Why this belongs more to IR/reranking than semantic-operator papers

This issue is discussed much more seriously in:

- IR reranking,
- LLM reranking,
- pairwise preference aggregation,
- listwise chunking and merge,

than in most semantic-operator systems papers.

Semantic-operator systems usually define:

- `sem_topk`,
- `sem_search`,
- ranking pipelines,

but they often stop before rigorously solving:

- cross-delta score calibration,
- pointwise vs pairwise/listwise consistency,
- ranking stability under continuous updates.

### 1.6 Current system decision

The current runtime keeps the execution layers explicit:

- `sem_score` for one-score-per-item paths,
- `sem_rerank` for bounded reranking paths.

But it does **not** claim to have solved the full ranking-consistency problem.

That problem remains open and should be treated as:

- a semantic ranking contract problem,
- an IR/reranking systems problem,
- and likely a later optimizer/planner problem.

---

## 2. Planning Consequence

### 2.1 What stays in V0.3

V0.3 should stay focused on:

1. true two-input `sem_join`,
2. reusable semantic execution steps (`sem_match`, `sem_score`, `sem_rerank`),
3. shared window materialization,
4. standard-window `sem_join(window)` pairing,
5. strict fail-fast boundaries where the contract is not yet defined.

### 2.2 What should move toward V0.4

The following now look like V0.4-level work:

1. semantic-window cross-stream alignment,
2. adaptive window-pair selection and alignment scoring,
3. rerank consistency policy across deltas,
4. adaptive switching between pointwise / pairwise / listwise paths,
5. optimizer-driven fusion and batching strategy.

### 2.3 Why this split is correct

This split follows the current project rules:

- keep the contract clean,
- do not guess missing semantics,
- only push down what stays logically clean,
- fail fast when the system does not yet know the correct behavior.
