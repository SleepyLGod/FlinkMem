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

# Reproducing VectraFlow + Continuous Prompts on PyFlink

- **VectraFlow**: vector-native stream processing layer (vector filter/join/top-k and retrieval acceleration).
- **Continuous Prompts (CP)** : LLM-augmented continuous operator layer and runtime optimization loop.

## 1. Semantic Operators

### 1.1 Base Semantic Operators

use the following base semantic operators as the common algebra:

- `sem_map`: semantic extraction/classification/transformation.
- `sem_filter`: semantic predicate over text/event/document.
- `sem_topk`: relevance/similarity ranking.
- `sem_join`: semantic match/join across streams or stream-state.
- `sem_agg`: semantic aggregation/summarization-style state reduction.

### 1.2 Continuous Semantic Operators (CP Emphasis)

extend to continuous operators in CP style:

- `sem_window`: semantic computation over windowed state.
- `sem_groupby`: semantic grouping over dynamic categories/intents/topics.
- `cts_filter`: continuously updated filtering decisions.
- `cts_topk`: continuously maintained semantic ranking.
- continuous retrieval/RAG operators backed by evolving stream state.

can treat three properties as non-optional:

- outputs can be revised as state evolves,
- semantic results are maintained incrementally,
- stream-time correctness (watermarks/timers/order constraints) is preserved when required.

## 2. Optimization Summary From Both Papers

### 2.1 VectraFlow Optimization Layer (Vector-Centric)

1. Indexed vector filtering (`iV-Filter`) with OPList-style candidate pruning and centroid-aware organization.
2. Ordered scanning and early-stop strategies for candidate traversal.
3. Batch distance computation with matrix/vectorized execution style.
4. Over-retrieval followed by reranking (`over-retrieval + rerank`).
5. Multi-stage filter cascades with cheap first-pass and expensive second-pass checks.
6. Vector-aware join/top-k optimizations via partitioning/clustering-based candidate reduction.

use these optimizations primarily to reduce expensive exact semantic checks and control candidate explosion.

### 2.2 CP Optimization Layer (LLM Pipeline-Centric)

1. Tuple batching for operator invocations.
2. Operator fusion, including filter-aware fusion variants.
3. Cost/quality model-driven plan search (dynamic-programming-style plan selection over alternatives).
4. Online tuning with MOBO (warm-up exploration, surrogate modeling, constrained optimization).
5. Runtime variant selection (prompt template, batch size, candidate width, timeout profile, etc.).

## 3. Mapping To PyFlink

| Paper Concept                                               | PyFlink Primitive                                                     | Feasibility |
| ----------------------------------------------------------- | --------------------------------------------------------------------- | ----------- |
| Async semantic call (`sem_map`, `sem_filter`)           | `AsyncDataStream.unordered_wait/ordered_wait` + `AsyncFunction`   | High        |
| Stateful continuous semantics (`sem_window`, `cts_*`)   | `KeyedProcessFunction` + `ValueState/ListState/MapState` + timers | High        |
| Dynamic control/config updates                              | `BroadcastStream` + `KeyedBroadcastProcessFunction`               | High        |
| Two-stream semantic join                                    | `KeyedCoProcessFunction` + dual-side state                          | High        |
| Operator-level batching                                     | operator-local buffer + timer/size flush                              | High        |
| Operator fusion                                             | fat operator / chained logic / prompt fusion                          | Medium-High |
| Plan-level rewrite + DP search                              | external planner + runtime config push                                | Medium      |
| MOBO online control loop                                    | external Python optimizer + metrics sink + broadcast commands         | High        |
| In-operator vector index internals fully in Python          | custom index/state in Python operator path                            | Medium-Low  |
| Very low-latency SIMD-heavy vector kernels in operator path | pure Python path is not ideal                                         | Low         |

### 3.1 PyFlink Operator Layers

for implementation planning, it is useful to separate PyFlink operators into three layers.

1. API layer: user-facing stream abstractions such as `DataStream`, `KeyedStream`,
   `WindowedStream`, `ConnectedStreams`, and `BroadcastConnectedStream`. These are the entry
   points where users call `map`, `filter`, `process`, `window`, `connect`, and related methods.
2. Python semantic adapter layer: Python-side function families such as `ProcessFunction`,
   `KeyedProcessFunction`, `CoProcessFunction`, `KeyedCoProcessFunction`, `WindowFunction`, and
   `ProcessWindowFunction`. This layer defines the semantic contract that user logic must satisfy.
3. runtime lowering layer: the internal translation path that turns Python operators into runtime
   operators, mainly `_get_one_input_stream_operator`, `_get_two_input_stream_operator`, the
   `FunctionType.WINDOW` enum value in `flink-python/pyflink/proto/flink-fn-execution.proto`, and
   the Java-side window/runtime operators such as
   `flink-python/src/main/java/org/apache/flink/streaming/api/operators/python/embedded/EmbeddedPythonWindowOperator.java`.

when this blueprint says "change" in the first iteration, it means adding semantic wrappers or
operator-specific logic at the API layer or Python semantic adapter layer. when it says "do not
change", it means keeping the runtime lowering layer intact:

- add semantic wrappers, helper classes, and operator-local logic on top of existing PyFlink APIs.
- do not introduce new runtime function types.
- do not alter `_get_one_input_stream_operator`, `_get_two_input_stream_operator`, or Java-side
  Python operator paths in the first iteration.

two clarifications are important here:

- `AsyncFunction` does receive a `RuntimeContext` through `open(...)`, but the `AsyncDataStream`
  path should still be treated as a row-local async execution path rather than the primary keyed
  state execution model.
- observability is a cross-cutting concern across the stateful and async layers. metrics for async
  capacity, state growth, config version, and quality drift are part of the control loop, not an
  afterthought.

### 3.2 Existing Operator Families

the built-in DataStream-side operators can be grouped into five families:

1. single-input operators on `DataStream`: `map`, `flat_map`, `filter`, `process`, `union`,
   watermark assignment, and related one-input transformations.
2. keyed/stateful operators on `KeyedStream`: `map`, `flat_map`, `reduce`, `filter`, `process`,
   `window`, and `count_window`.
3. window operators on `WindowedStream` and `AllWindowedStream`: `reduce`, `aggregate`, `apply`,
   and `process`.
4. two-input operators on `ConnectedStreams`: `map`, `flat_map`, and `process`.
5. control-plane operators on `BroadcastConnectedStream`: `process` with
   `BroadcastProcessFunction` or `KeyedBroadcastProcessFunction`.

the name overlap between `DataStream.map/filter` and `KeyedStream.map/filter` is intentional, but
the execution semantics differ:

- `DataStream.*` is non-keyed per-record processing. it does not provide per-key state or per-key
  timers.
- `KeyedStream.*` runs after `key_by(...)`, so the stream is partitioned by key and the operator
  can rely on per-key state and per-key timers through keyed process semantics.

this means two operators with the same surface name may belong to different semantic families even
when they look identical in user code.

### 3.3 Keyed, Window, and Core Operator Concepts

the most important concepts for CP-style integration are the following:

- `key_by`: partition the stream by a key so that all records with the same key are routed to the
  same keyed operator instance.
- `keyed`: shorthand for "this operator is running with per-key context", which means state and
  timers are scoped by key rather than by the entire stream.
- `reduce`: an incremental aggregation operator that keeps combining values as new records arrive.
  it is best suited for algebraic or compact aggregations.
- `aggregate`: a more general incremental aggregation operator that uses an accumulator. it is
  suitable when the intermediate aggregation state differs from the final output.
- `apply`: a full-window evaluation operator. it assumes the window contents are materialized and
  made available for evaluation as a collection.
- `process`: the most general operator family. it is best understood as a user-defined state
  machine that can access time, watermarks, timers, side outputs, and keyed state.

in practice:

- `reduce` and `aggregate` are the preferred incremental aggregation paths.
- `apply` expects full-window materialization and is therefore heavier.
- `process` is the most flexible choice and is the natural starting point for CP-style semantic
  windows, continuous revisions, and custom stateful control logic.

### 3.4 Window Functions vs Window Operators

PyFlink distinguishes between window functions, window API constructs, and runtime window
operators.

- `WindowFunction` and `ProcessWindowFunction` in
  `flink-python/pyflink/datastream/functions.py` are user-facing Python interfaces.
- `WindowedStream` and `AllWindowedStream` in
  `flink-python/pyflink/datastream/data_stream.py` are API constructs. they describe the intended
  windowed computation, but they are not the final runtime operator themselves.
- `WindowOperationDescriptor` in `flink-python/pyflink/datastream/window.py` bundles the actual
  window metadata: assigner, trigger, allowed lateness, late-data side output, state descriptor,
  serializer, and internal window function.
- the runtime execution path then uses the `WINDOW` function type in
  `flink-python/pyflink/proto/flink-fn-execution.proto` and lowers the operation through
  `_get_one_input_stream_operator` in `flink-python/pyflink/datastream/data_stream.py`.
- on the Java side, the embedded runtime path is represented by
  `flink-python/src/main/java/org/apache/flink/streaming/api/operators/python/embedded/EmbeddedPythonWindowOperator.java`.

the practical consequence is that a "window function" is not the same thing as a "window
operator": the function is user logic, while the operator is the runtime machinery that manages
window state, timers, triggers, and serialization.

## 4. Boundaries

make these boundaries explicit before implementation:

1. do not assume every paper internal can be reproduced inside Python operators.
2. expect Python/JVM crossing overhead and Python operator overhead to limit extreme low-latency scenarios.
3. treat dynamic broadcast reconfiguration as a potential instability source unless I enforce config versioning, rollout policy, and rollback.
4. expect semantic state to grow quickly; I enforce TTL, bounded buffers, hard state limits, and state compaction from the first stateful phase onward.
5. do not claim online optimizer correctness without quality signals/proxy labels.
6. do not assume fusion always helps; token growth and error coupling can cancel throughput gains.
7. `AsyncFunction` is not supported in PyFlink `thread` mode, so the async baseline assumes the process-mode path.
8. the async path can use `RuntimeContext` during `open(...)`, but I do not treat `AsyncDataStream` as the default keyed-state path for retrieval caches or keyed semantic memory.
9. `REVISE_OUTPUT` exists in the runtime protocol, but I do not assume it already provides CP-style continuous revision semantics.
10. timer-triggered semantic summarization may require a two-stage topology; I do not assume I can place arbitrary async LLM I/O directly inside `on_timer(...)`.

## 5. Step-By-Step Implementation Plan

### 5.0 LOTUS-Informed Operator Placement

align operator phasing with the semantic operator model rather than only the operator names.

- in LOTUS, `sem_map` and `sem_filter` are row-wise semantic transformations, so they fit the first async/operator-wrapper phase.
- in LOTUS, `sem_agg` is a relation-level aggregation and `sem_topk` is an ordering operator; in a stream runtime these split into local and continuous variants.
- in LOTUS, `sem_join` is a natural-language join predicate, but in PyFlink it can appear in two distinct execution shapes:
  - `retrieve-backed sem_join`: each input event retrieves a finite set of candidate rows from a database or vector store and then performs semantic matching over that local candidate set.
  - `true two-input sem_join`: both sides are represented as stream/state inputs and require explicit two-input coordination.

this leads to the following clean phase split:

| operator | LOTUS semantic shape | first implementation phase | PyFlink primitive | source-code anchor |
| --- | --- | --- | --- | --- |
| `sem_map` | row-wise semantic projection/transformation | `V0.1` | `AsyncDataStream.unordered_wait/ordered_wait` + `AsyncFunction` | `flink-python/pyflink/datastream/async_data_stream.py`, `flink-python/pyflink/datastream/data_stream.py` |
| `sem_filter` | row-wise natural-language predicate | `V0.1` | `AsyncDataStream.unordered_wait/ordered_wait` + downstream `filter` or `process` | `flink-python/pyflink/datastream/async_data_stream.py`, `flink-python/pyflink/datastream/data_stream.py` |
| `sem_topk` (local rerank) | ranking within a finite per-record candidate set | `V0.1` | same async row-local path as `sem_map` | `flink-python/pyflink/datastream/async_data_stream.py` |
| `sem_join` (retrieve-backed) | per-record semantic join against a finite retrieved candidate set | `V0.1` by default, `V0.2` if cached/stateful | async row-local path or keyed stateful path without a second stream | `flink-python/pyflink/datastream/async_data_stream.py`, `flink-python/pyflink/datastream/data_stream.py`, `flink-python/pyflink/datastream/functions.py` |
| `sem_agg` | relation-level semantic aggregation | `V0.2` | `KeyedProcessFunction` + `ReducingState/AggregatingState` or `ListState` | `flink-python/pyflink/datastream/functions.py`, `flink-python/pyflink/datastream/state.py` |
| `sem_topk` (continuous) | continuously maintained ranking over a keyed stream | `V0.2` | `key_by(...).process(...)` + keyed state + timers | `flink-python/pyflink/datastream/data_stream.py`, `flink-python/pyflink/datastream/functions.py` |
| `sem_join` (true two-input) | two-input natural-language join across stream/state inputs | `V0.3` | `connect(...).process(...)` + `KeyedCoProcessFunction` + dual-side state | `flink-python/pyflink/datastream/data_stream.py`, `flink-python/pyflink/datastream/functions.py` |

do not introduce new Python function types in the first iteration. keep the existing lowering paths intact:

- single-input path: `DataStream.process/KeyedStream.process` -> `_get_one_input_stream_operator`.
- async row-local path: `AsyncDataStream.*` -> `_get_one_input_stream_operator`.
- two-input path: `ConnectedStreams.process` -> `_get_two_input_stream_operator`.

for operator design, this means:

- use `AsyncDataStream` for row-local semantic calls and retrieval-backed operators when the
  candidate set is finite per input record.
- switch to `KeyedProcessFunction` once retrieval metadata, caches, or semantic memory must be
  maintained per key.
- reserve `ConnectedStreams.process(...)` for true two-input coordination, not for ordinary
  per-record retrieval.

### Phase V0.1 - Baseline Async Semantic Map/Filter

1. `sem_map` via `AsyncDataStream` + structured output schema.
2. `sem_filter` via `AsyncDataStream` with confidence/reason fields.
3. optional `local sem_topk` only for the case where each incoming record already carries a finite candidate set and the operator only reranks that local set.
4. `retrieve-backed sem_join` where each incoming record retrieves a finite candidate set from an external database or vector store and performs semantic matching against that local set.
5. implement the first version as thin semantic wrappers over the existing async/process lowering path, without changing `_get_one_input_stream_operator` or introducing new function types.
6. explicitly keep retrieval-backed `sem_join` on the one-input/async path in this phase; do not route it through `ConnectedStreams.process(...)`.
7. explicitly exclude `thread` mode from this async baseline.
8. timeout/retry/fallback policy.
9. core metrics:
   - call latency,
   - timeout/error rate,
   - async capacity utilization,
   - token usage,
   - invalid/null output rate.

capacity must be tuned empirically rather than fixed once. the correct value depends on model
latency, retry policy, and target throughput.

explicitly do not place the following into `V0.1`:

- `sem_agg`, because it requires keyed state and an explicit aggregation scope.
- continuous `sem_topk`, because it requires incremental state maintenance and timer-driven updates.
- true two-input `sem_join`, because it requires two-input coordination and dual-side state.

define completion as: stable execution under sustained load without backpressure collapse and deterministic retry behavior.

### Phase V0.2 - Stateful Semantic Window

1. `sem_window` with `KeyedProcessFunction`.
2. `sem_agg` on top of keyed state:
   - use `ReducingState` / `AggregatingState` when the aggregation can be made algebraic,
   - for summarization-style aggregation, start with `ListState` + explicit aggregation logic and
     a timer-triggered work emission path to a downstream async summarizer.
3. continuous `sem_topk` on top of keyed state:
   - use `ListState/MapState/ValueState` for candidate buffering, current score frontier, and emitted top-k snapshot,
   - use timers for deferred recomputation, eviction, and bounded updates.
4. optional cached or stateful retrieval-assisted variants of `sem_join` when retrieval metadata or reusable candidate caches must be maintained per key but no second stream is introduced.
5. `ListState` for context/events and `ValueState` for current semantic result.
6. timer-driven emission/update policy.
7. watermark-aware handling (if event-time semantics are required).
8. TTL/cleanup policy on state.
9. hard state bounds and eviction policy from the beginning of the phase.

the intended code path in this phase is:

- `key_by(...)` -> `KeyedStream.process(...)` -> `KeyedProcessFunction.process_element/on_timer`.
- state access via `RuntimeContext.get_state/get_list_state/get_map_state/get_reducing_state/get_aggregating_state`.
- treat this phase as the first explicit L4 + L5 interaction point whenever semantic aggregation
  needs downstream async summarization.

define completion as: consistent results under out-of-order input and correct recovery after restart/checkpoint restore.

### Phase V0.3a - True Two-Input Semantic Join

1. true two-input `sem_join` via `connect(...).process(...)` + `KeyedCoProcessFunction`.
2. dual-side buffered state with explicit TTL and candidate pruning policy.
3. optional first-stage cheap candidate selection before expensive semantic verification.

the intended code path in this phase is:

- `stream1.connect(stream2).process(...)` -> `ConnectedStreams.process(...)` -> `_get_two_input_stream_operator`.
- keyed two-input logic in `KeyedCoProcessFunction.process_element1/process_element2`.

define completion as: correct two-input semantic matching under bounded state growth and stable
recovery semantics.

### Phase V0.3b - CP-Style Batching And Fusion

1. batching buffer with two flush triggers:
   - max batch size,
   - max wait time.
2. shared-prefix prompt batching.
3. fusion path and fallback non-fusion path.
4. per-path quality proxy and latency/cost logs.
5. explicit quality guardrails for automatic rollback from fusion to non-fusion paths.

define completion as: measurable throughput gain with bounded quality drift relative to V0.3a.

### Phase V0.4 - Dynamic Brain (External Optimizer + Broadcast Control)

1. external optimizer process:
   - heuristic policy first,
   - MOBO after metrics stabilize.
2. metrics ingestion from runtime sinks.
3. control command output:
   - batch size,
   - fusion switch,
   - candidate width,
   - timeout/retry,
   - prompt/model variant.
4. broadcast command stream consumed by semantic operators.
5. safe rollout controls:
   - config versioning,
   - canary rollout fraction,
   - automatic rollback trigger on SLO violations.
6. attach config-version tags to metrics or emitted audit records so that mixed-version intervals
   can be analyzed explicitly.
7. if MOBO becomes unstable or the workload drifts beyond the regime used to fit the surrogate
   model, fall back automatically to the heuristic policy.

define completion as: online policy outperforms static baseline without prolonged instability.

### Optional Phase V1.0 - VectraFlow-Style Vector Acceleration

1. two-stage retrieval (fast candidate generation + precise rerank).
2. early termination heuristics.
3. OPList-like candidate organization approximation.
4. optional quantized first-stage vector representation.

define completion as: lower retrieval latency while keeping recall/quality within target bounds.
