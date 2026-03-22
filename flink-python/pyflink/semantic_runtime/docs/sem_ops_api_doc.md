# Semantic Operators — Complete Technical Reference

> **Applicable Versions**: V0.1 (Non-Stateful) + V0.2 (Stateful) + V0.2+ (Alignment)
> **Generated On**: 2026-03-17
> **Code Path**: `flink-python/pyflink/semantic_runtime/`

---

## Table Of Contents

1. [Overview](#1-overview)
2. [V0.1 Non-Stateful Semantic Operators](#2-v01-non-stateful-semantic-operators)
   - 2.1 [Shared Infrastructure (`_common.py`)](#21-shared-infrastructure)
   - 2.2 [`sem_filter` — Semantic Filtering](#22-sem_filter--semantic-filtering)
   - 2.3 [`sem_map` — Semantic Mapping / Extraction](#23-sem_map--semantic-mapping--extraction)
   - 2.4 [`sem_lookup_join` — Retrieval-Augmented Semantic Lookup Join](#24-sem_lookup_join--retrieval-augmented-semantic-lookup-join)
   - 2.5 [`sem_local_topk` — Local Semantic Re-ranking](#25-sem_local_topk--local-semantic-re-ranking)
3. [V0.2 Stateful Foundation Modules](#3-v02-stateful-foundation-modules)
   - 3.1 [`event_model.py` — Event Model And Contract Adapters](#31-event_modelpy--event-model-and-contract-adapters)
   - 3.2 [`state_descriptors.py` — Centralized State Descriptors](#32-state_descriptorspy--centralized-state-descriptors)
   - 3.3 [`timer_policy.py` — Timer Policy](#33-timer_policypy--timer-policy)
   - 3.4 [`async_bridge.py` — Async Bridge Pattern](#34-async_bridgepy--async-bridge-pattern)
4. [V0.2 Stateful Semantic Operators](#4-v02-stateful-semantic-operators)
   - 4.1 [`sem_window` — Semantic Window](#41-sem_window--semantic-window)
   - 4.2 [`sem_groupby` — Dynamic Semantic Grouping](#42-sem_groupby--dynamic-semantic-grouping)
   - 4.3 [`sem_agg` — Semantic Aggregation](#43-sem_agg--semantic-aggregation)
   - 4.4 [`sem_search` — Internal Continuous Retrieval Helper](#44-sem_search--internal-continuous-retrieval-helper)
   - 4.5 [`sem_topk` — Continuous Top-K](#45-sem_topk--continuous-top-k)
5. [Continuous RAG Workflow](#5-continuous-rag-workflow)
6. [Glossary](#6-glossary)
7. [Metrics — Metric System](#7-metrics--metric-system)
8. [V0.2+ Addendum — API Alignment Changes](#8-v02-addendum--api-alignment-changes)

---

## 1. Overview

This project builds a set of **Semantic Operators** on top of Apache Flink (PyFlink), embedding LLM calls into stream processing pipelines. The implementation is split into two phases:

| Phase | Operator Type | Flink Base Class | State Management | LLM Interaction |
|------|---------------|------------------|------------------|-----------------|
| **V0.1** | Non-Stateful | `AsyncFunction` | No keyed state | Each record directly calls the LLM |
| **V0.2** | Stateful | `KeyedProcessFunction` | Flink keyed state + TTL | Uses Async Bridge side outputs for asynchronous LLM calls |

**V0.1** provides four public row-style operators: `sem_filter`, `sem_map`, `sem_lookup_join`, and `sem_local_topk`.
**V0.2** provides four public stateful operators: `sem_window`, `sem_groupby`, `sem_agg`, and `sem_topk`. It also contains the internal workflow helper `sem_search`; workflow composition and the metric system are documented separately in Sections 5 and 7.

**V0.2+ internal lowering view**:
- pointwise `sem_topk` is treated as an internal logical lowering:
  - semantic score generation
  - followed by classical Top-N semantics
- bounded/window-owned `sem_groupby` is treated as:
  - semantic label generation
  - followed by classical group-by semantics
- future `sem_join` follows the same logical pattern:
  - semantic match predicate / score
  - followed by classical join/filter semantics
- `sem_agg` remains the main exception:
  - `summarize` and `compressive` are modeled as native semantic reduce
- internal lowering may use semantic score/label/match steps, but these are
  implementation details rather than public first-class operators

---

## 1.1 Current Package Layout

| Directory | Role |
|----------|------|
| `public_api.py` | Public facade: user-facing requests and business context |
| `operators/row/` | Low-level row operator kernels and expert/internal builders |
| `operators/stateful/` | Low-level stateful kernels and expert/internal builders |
| `runtime/` | Internal runtime infrastructure, workflow, and search helpers |

## 1.2 Public vs Internal API

The top-level package `pyflink.semantic_runtime` now exposes only the public
facade:

- `context(...)`
- `sem_map(...)`
- `sem_filter(...)`
- `sem_local_topk(...)`
- `sem_lookup_join(...)`
- `sem_window(...)`
- `sem_topk(...)`
- `sem_groupby(...)`
- `sem_agg(...)`

The following remain available in submodules, but they are internal or
expert-layer concepts rather than the intended user-facing API:

- `RuntimeConfig`
- `QuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- `Sem*Config`
- low-level `Sem*Function`

## 2. V0.1 Non-Stateful Semantic Operators

All V0.1 operators inherit from `AsyncFunction` and use Flink's `AsyncDataStream` asynchronous I/O pattern.

**Shared design principles**:
- **`__init__` stores only serializable configuration**: no live LLM connections or runtime objects
- **`open()` creates the LLM client**: deferred initialization through `create_llm_client(config)`
- **strict fail-fast**: invalid model output, retrieval failure, and Flink timeout propagate as exceptions
- **1:1 success contract**: on success, every input record produces exactly one output

### 2.1 Shared Infrastructure

**File**: `operators/row/_common.py`

| Function | Purpose |
|---------|---------|
| `validate_schema(obj, schema)` | Shallow type check: verifies whether a dict has the required keys with matching types |
| `attach_metrics(parsed, metrics)` | Attaches LLM call metrics (latency, tokens, attempts) to the output dict |

### 2.2 `sem_filter` — Semantic Filtering

**File**: `operators/row/sem_filter.py`
**Low-level builder**: `build_sem_filter_operator(semantic, runtime_config)`

**Purpose**: Build a row-style semantic filter from semantic intent. Backend selection stays internal.

| Item | Description |
|------|-------------|
| **Input** | Arbitrary string records (Flink `Types.STRING()` or `PICKLED_BYTE_ARRAY`) |
| **Output** | JSON string: `{"decision": bool, "confidence": float, "reason": str, "_input": ..., "_metrics": {...}}` |
| **Semantic intent** | `SemSpec.for_sem_filter(...)` or `SemSpec(..., output_mode="bool")` |
| **Failure** | Raises on LLM errors, invalid JSON/schema, and Flink timeout |

**Implementation idea**:
1. The low-level builder validates semantic intent and binds internal runtime config
2. `async_invoke(value)` → format the input with the prompt template → call the LLM
2. Parse the LLM JSON response → verify the presence of the three keys `{decision, confidence, reason}`
3. Normalize types: `decision→bool`, `confidence→float`, `reason→str`
4. Attach `_metrics` and `_input` → return a JSON string
5. The actual filtering is **not performed inside the operator** — downstream uses `ds.filter(lambda x: json.loads(x)["decision"])`

> ⚠️ Design highlight: the operator itself is a **1:1 mapping** rather than a filter, preserving auditability for rejected records.

### 2.3 `sem_map` — Semantic Mapping / Extraction

**File**: `operators/row/sem_map.py`
**Low-level builder**: `build_sem_map_operator(semantic, runtime_config)`

**Purpose**: Build a row-style semantic map from semantic intent. Backend selection stays internal.

| Item | Description |
|------|-------------|
| **Input** | Arbitrary string records |
| **Output** | JSON string whose structure is defined by `output_schema`, with `_metrics` attached |
| **Semantic intent** | `SemSpec.for_sem_map(...)` |
| **Schema** | `SemSpec.schema` — e.g. `{"sentiment": str, "confidence": float}` |
| **Failure** | Raises on LLM errors, invalid JSON/schema, and Flink timeout |

**Implementation idea**:
1. The low-level builder validates semantic intent and binds internal runtime config
2. `async_invoke(value)` → format the prompt → call the LLM
2. Parse the JSON → verify keys and types with `validate_schema(parsed, output_schema)`
3. Attach `_metrics` → return a JSON string
4. If parsing fails or the schema does not match → raise an exception

### 2.4 `sem_lookup_join` — Retrieval-Augmented Semantic Lookup Join

**File**: `operators/row/sem_lookup_join.py`
**Class**: `SemLookupJoinFunction(AsyncFunction)`

**Purpose**: For each record, first retrieve an external candidate set, then let the LLM perform semantic matching / joining.

| Item | Description |
|------|-------------|
| **Input** | Arbitrary string record (query) |
| **Output** | JSON: `{"_input": ..., "join_result": <LLM parsed result>, "candidate_count": int, "truncated": bool, "_metrics": {...}}` |
| **Prompt** | `prompt_template.format(input=value, candidates=json.dumps(candidates))` |
| **Retriever** | `CandidateRetriever` abstract interface; now supports `MockCandidateRetriever` and `CandidateRetrieverFromSearchBackend` |
| **Failure** | Retrieval timeout/error, LLM failure, invalid JSON, or Flink timeout raise directly |

**Implementation idea**:
1. `async_invoke(value)` → call `CandidateRetriever.retrieve(query, max_candidates)` to fetch candidates
2. Strict timeout control: `asyncio.wait_for(retrieve_call, timeout=retrieve_timeout_ms/1000)`
3. Hard upper bound truncation: if candidate count exceeds `max_candidates_per_record`, truncate and mark `truncated=True`
4. Send the input and candidate set to the LLM for semantic matching
5. Parse the LLM output → wrap it as `join_result` → return

**V0.2+ compatibility update**:
- `sem_lookup_join` can now reuse the V0.2 external retrieval contract by wrapping an `ExternalSearchBackend` with `CandidateRetrieverFromSearchBackend`
- this keeps V0.1 lookup join compatible while avoiding a second long-lived external retrieval abstraction

**Key configuration** (`SemLookupJoinConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_candidates_per_record` | 20 | Maximum number of candidates per record |
| `retrieve_timeout_ms` | 5000 | Retrieval timeout in milliseconds |
| `search_backend` | None | Optional `ExternalSearchBackend`; when set, lookup join retrieves through the shared V0.2 backend contract |
| `mock_candidates` | None | Fixed candidate set for testing |

### 2.5 `sem_local_topk` — Local Semantic Re-ranking

**File**: `operators/row/sem_local_topk.py`
**Low-level builder**: `build_sem_local_topk_operator(semantic, k, runtime_config)`

**Purpose**: Build a row-style bounded semantic top-k operator. Backend selection stays internal.

| Item | Description |
|------|-------------|
| **Input** | JSON string that must contain the candidate list designated by `candidates_field` |
| **Output** | JSON: `{"_input": ..., "top_k": [reranked list], "k": int, "original_count": int, "_metrics": {...}}` |
| **Semantic intent** | `SemSpec.for_sem_topk(...)` |
| **Failure** | Input parse failure or non-list LLM output fails the operator run under strict mode |

**Implementation idea**:
1. The low-level builder validates semantic intent and binds internal runtime config
2. `async_invoke(value)` → parse the input JSON → extract the `candidates` field
2. Send the full record and the candidate list to the LLM using the prompt
3. The LLM returns a reranked JSON list → truncate to top-k
4. Wrap the result → return

**V0.1 vs V0.2 TopK comparison**:

| Dimension | V0.1 `sem_local_topk` | V0.2 `sem_topk` |
|----------|------------------------|-----------------|
| Base class | `AsyncFunction` | `KeyedProcessFunction` |
| State | None (reruns each time) | Yes (keyed `MapState` maintains a candidate pool) |
| Trigger | Every input | Incremental update + timer-driven rerank |
| LLM | Called every time | The current implementation does not embed an LLM call; it relies on upstream scores and timer-driven local recomputation |

### 2.6 Internal Lowering Helpers

Internal lowering may materialize semantic score / label / match attributes as
private implementation steps. These helpers live behind lowering/planning and
are not part of the public operator surface.

## 3. V0.2 Stateful Foundation Modules

V0.2 introduces four core foundation modules, and all stateful operators depend on them. Workflow composition and metrics are documented separately below.

### 3.1 `event_model.py` — Event Model And Contract Adapters

**File**: `runtime/event_model.py`

Provides two core data classes and seven contract adapter functions.

**`SemEvent`** — the standard input format for all V0.2 operators:

| Field | Type | Required | Meaning |
|------|------|----------|---------|
| `key` | `str` | ✅ | Primary partitioning key (`user_id` / `session_id`) |
| `payload` | `str` | ✅ | Text content |
| `seq_id` | `int` | ✅ | Monotonically increasing sequence number (ordering + deduplication) |
| `event_time_ms` | `int \| None` | ❌ | Event timestamp (`None` = processing-time-only) |
| `proc_time_ms` | `int` | auto | Processing timestamp (auto-filled on creation) |
| `metadata` | `dict` | ❌ | Arbitrary tracing metadata |
| `candidates` | `list[dict]` | ❌ | Pre-retrieved candidate records |
| `boundary_flags` | `dict` | ❌ | Semantic boundary signal such as `{"topic_shift": True}` |

**`WindowSnapshot`** — the output of `sem_window`, representing a complete semantic window:

| Field | Meaning |
|------|---------|
| `key` | Partition key |
| `window_id` | UUID-based window identifier |
| `events` | List of all event dicts inside the window |
| `event_count` | Number of events |
| `open_time_ms` / `close_time_ms` | Window open / close timestamps |
| `trigger_reason` | `"count"` / `"time"` / `"semantic_boundary"` |

**Contract adapter functions**:

| Function | Conversion Direction | Purpose |
|---------|----------------------|---------|
| `is_window_snapshot(d)` | Type detection | Determines whether a dict is a `WindowSnapshot` |
| `window_snapshot_to_sem_events(snap)` | `WindowSnapshot → List[SemEvent dict]` | **Subflow A adapter**: expands a window into per-event records and injects `window_id` into metadata |
| `window_snapshot_to_summary_event(snap)` | `WindowSnapshot → single SemEvent dict` | Merges the whole window into a single summary event (`payload` = concatenated event payloads) |
| `group_assignment_to_sem_event(assignment)` | `sem_groupby output → SemEvent dict` | Normalizes grouping results into a stable event envelope that `sem_agg` can consume directly |
| `retrieve_to_topk_items(output)` | `SemSearch output → List[candidate dict]` | **Subflow B adapter**: expands retrieval results and guarantees that every item has a `candidate_id` |
| `retrieve_to_answer_context(output)` | `SemSearch output → AnswerSynthesiser input` | Normalizes retrieval output directly into `{query, retrieved_context}` |
| `topk_to_answer_context(output)` | `TopK output → AnswerSynthesiser input` | **Subflow C adapter**: normalizes into `{query, retrieved_context}` |

**Key selectors**:
- `simple_key_selector(event_dict)`: returns `event_dict["key"]`
- `composite_key_selector(*fields)`: concatenates multiple fields with `|`

### 3.2 `state_descriptors.py` — Centralized State Descriptors

**File**: `runtime/state_descriptors.py`

**Design goal**: all Flink State Descriptors used by operators are declared in a **single file**, ensuring:
- consistent naming across operators (avoids accidental naming collisions that can break checkpoints)
- unified TTL configuration
- checkpoint compatibility changes only need to be made in one place

#### OverflowPolicy

Handling strategy when a state container reaches its hard limit:

| Policy | Behavior |
|-------|----------|
| `DROP_OLDEST` | Evict the oldest entry |
| `DROP_NEWEST` | Reject the new entry |

#### StateSafetyConfig

State safety configuration for each operator:

| Parameter | Default | Meaning |
|----------|---------|---------|
| `ttl_seconds` | 3600 | State TTL in seconds |
| `max_window_events` | 500 | Maximum events per window |
| `max_groups_per_key` | 50 | Maximum groups for one key |
| `max_candidates_per_key` | 200 | Maximum retrieved candidates for one key |
| `max_topk_candidates` | 100 | Size of the TopK candidate pool |
| `max_pending_async_items` | 50 | Maximum pending async items |

#### TTL Configuration

`build_ttl_config(ttl_seconds)` builds the standard TTL configuration:
- **UpdateType**: `OnReadAndWrite` (refresh TTL on both reads and writes)
- **StateVisibility**: `NeverReturnExpired` (never return expired values)

#### Descriptor List

| Descriptor Function | State Type | Used For |
|---------------------|-----------|----------|
| `sem_window_event_buffer_descriptor` | `ListState` | Window event buffer |
| `sem_window_meta_descriptor` | `ValueState` | Window metadata |
| `sem_groupby_profiles_descriptor` | `MapState` | Group profiles |
| `sem_groupby_pending_events_descriptor` | `ListState` | Pending async groupby event chunk |
| `sem_search_cache_descriptor` | `MapState` | Retrieval cache |
| `sem_agg_buffer_descriptor` | `ListState` | Aggregation event buffer |
| `sem_agg_value_descriptor` | `ValueState` | Aggregation accumulated value |
| `sem_agg_meta_descriptor` | `ValueState` | Aggregation metadata |
| `sem_topk_candidates_descriptor` | `MapState` | TopK candidate pool |
| `sem_topk_snapshot_descriptor` | `ValueState` | Current TopK snapshot |

### 3.3 `timer_policy.py` — Timer Policy

**File**: `runtime/timer_policy.py`

**Design goal**: provide a unified timer registration, dispatch, and cleanup mechanism for all V0.2 stateful operators.

#### TimerCategory

Three standardized timer categories:

| Category | Purpose | Typical Users |
|---------|---------|---------------|
| `FLUSH` | Timeout-based flushing of buffered state | `sem_window` (window timeout), `sem_agg` (aggregation flush) |
| `RECOMPUTE` | Periodic recomputation | `sem_topk` (periodic rerank) |
| `EVICT` | Clearing expired state | `sem_groupby`, `sem_search`, and any keyed operator with eviction scans enabled |

#### TimerPolicy

Each operator is configured with one `TimerPolicy` instance:

| Parameter | Default | Meaning |
|----------|---------|---------|
| `flush_interval_ms` | 30,000 | `FLUSH` timer interval (`0` = disabled) |
| `recompute_interval_ms` | 0 | `RECOMPUTE` timer interval (`0` = disabled) |
| `evict_interval_ms` | 60,000 | `EVICT` timer interval (`0` = disabled) |
| `use_event_time` | False | `True` = event-time timer, `False` = processing-time timer |

#### Core Functions

| Function | Purpose |
|---------|---------|
| `encode_timer_key(category)` | Encodes a `TimerCategory` into a state key such as `"_timer_flush"` |
| `register_timer(timer_service, meta, category, fire_at_ms)` | Registers a timer and records it in the `meta` dict |
| `resolve_timer_category(meta, fired_timestamp, tolerance_ms=200)` | Infers which timer category fired based on the trigger timestamp |
| `clear_timer_registration(meta, category)` | Clears the registration after the timer fires |
| `schedule_policy_timers(timer_service, meta, policy, base_time_ms)` | Batch-registers all enabled timers from a policy |

**Timer safety rules**:
- ✅ Allowed in timer callbacks: state reads/writes, sorting, truncation, eviction, metric updates, emitting output
- ❌ Forbidden in timer callbacks: blocking or asynchronous LLM calls
- Work that requires the LLM must go through **Side Output → Async Bridge**

### 3.4 `async_bridge.py` — Async Bridge Pattern

**File**: `runtime/async_bridge.py`

**Design goal**: solve the fact that `KeyedProcessFunction` cannot directly perform asynchronous LLM calls.

#### Topology Pattern

```text
keyed_stream.process(StatefulOp)
    │                        │
    ├─ main output           └─ side output (OutputTag: "async_work_items")
    │                                  │
    │                           AsyncDataStream.unordered_wait(AsyncWorker)
    │                                  │
    └───── union ─────────────────────-┘
            │
      key_by(key_selector)
            │
      process(MergeFunction)  ← merges async results back
```

#### Core Data Classes

**`AsyncWorkItem`** — a work item sent through side output:

| Field | Meaning |
|------|---------|
| `key` | Must match the upstream keying |
| `task_type` | Work type: the current V0.2 workflow uses `"classify"` / `"summarize"` / `"retrieve"` |
| `payload` | Operator-defined payload |
| `request_id` | UUID used for deduplication / correlation |

**`AsyncResult`** — the result returned after asynchronous work completes:

| Field | Meaning |
|------|---------|
| `key` | Same as above |
| `task_type` | Same as above |
| `result` | Async computation result |
| `success` | Whether it succeeded |
| `error` | Error message |

#### `build_async_bridge()` Function

One function call wires the whole topology:

```python
merged = build_async_bridge(
    main_ds=operator_output,
    async_fn=my_llm_worker,
    merge_fn=MyMergeFunction(),
    key_selector=simple_key_selector,
    timeout_ms=30_000,
    capacity=20,
)
```

Internal steps:
1. `main_ds.get_side_output(ASYNC_WORK_TAG)` → get the side output stream
2. `AsyncDataStream.unordered_wait(side_ds, async_fn, ...)` → async processing
3. `main_ds.union(async_result_ds)` → merge streams
4. `unified_ds.key_by(...).process(merge_fn)` → keyed merge

---

## 4. V0.2 Stateful Semantic Operators

All V0.2 operators inherit from `KeyedProcessFunction` and share the same lifecycle:

1. **`__init__`**: stores only a `Config` dataclass (serializable)
2. **`open(runtime_context)`**: gets state handles from `state_descriptors`, initializes `StatefulOperatorMetrics`
3. **`process_element(value, ctx)`**: main processing entry, automatically detects input type (single event / `WindowSnapshot` / async merge-back)
4. **`on_timer(timestamp, ctx)`**: timer callback, only local state work + emit
5. **Side Output**: work that needs the LLM is sent through `ctx.output(ASYNC_WORK_TAG, work_item)`

### 4.1 `sem_window` — Semantic Window

**File**: `operators/stateful/sem_window.py`
**Class**: `SemWindowFunction(KeyedProcessFunction)`

**Purpose**: split an event stream into windows based on semantic boundaries rather than fixed time/count only.

| Item | Description |
|------|-------------|
| **Input** | `SemEvent` dict (keyed stream) |
| **Output** | `WindowSnapshot` dict (contains all events in the window) |
| **State** | `ListState[event_buffer]` + `ValueState[window_meta]` |
| **Timer** | `FLUSH`: registered when the window opens, forces a flush on timeout |

**Configuration** (`SemWindowConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_window_events` | 50 | Count trigger threshold |
| `window_timeout_ms` | 30,000 | Time trigger in milliseconds |
| `boundary_flag` | `"topic_shift"` | Semantic boundary flag name |
| `overflow_policy` | `DROP_OLDEST` | Overflow eviction strategy |

**Three trigger types**:

| Trigger Type | Condition | Source |
|-------------|-----------|--------|
| **Count** | Event count ≥ `max_window_events` | Local counter |
| **Time** | Time since opening ≥ `window_timeout_ms` | Processing-time timer |
| **Semantic** | Event `boundary_flags` contains `boundary_flag` | Upstream pre-classifier |

**Processing flow**:
1. Receive an event → append it to the `ListState` event buffer
2. If it is the first event → initialize `window_meta` (`window_id`, `open_time_ms`), register a `FLUSH` timer
3. Check trigger conditions → `_check_triggers()` returns a trigger reason or `None`
4. If triggered → build and yield a `WindowSnapshot` → clear the buffer and metadata
5. On overflow (full buffer) → apply `overflow_policy` to evict old events

### 4.2 `sem_groupby` — Dynamic Semantic Grouping

**Files**:
- `operators/stateful/sem_groupby.py`
- `operators/stateful/sem_groupby_window.py`
- `operators/stateful/sem_groupby_pipeline.py`

**Classes**:
- `SemGroupbyFunction(KeyedProcessFunction)` — operator-owned continuous path
- `WindowOwnedSemGroupbyFunction(KeyedProcessFunction)` — bounded/window-owned path

**Purpose**: dynamically assign each event to one existing semantic group or create one new group.

| Item | Description |
|------|-------------|
| **Input** | `SemEvent` dict or `WindowSnapshot` dict (automatically expanded) |
| **Output** | Main-path assignment envelope: `{key, group_id, confidence, source, event_seq_id, payload, event_time_ms, metadata, boundary_flags}` |
| **Side Output** | `AsyncWorkItem(task_type="classify")` — emitted only when the internal assignment backend is async |
| **State** | `MapState[group_id → group_profile]` + `ValueState[meta]` |

**Query-spec integration** (`GroupbyQuerySpec`):
- public query spec expresses grouping intent, scope, and trigger only
- `maintenance_trigger_policy` is available as a separate maintenance/refinement trigger
- `scope_policy` now also supports close-capable operator-owned scopes:
  - `window_kind = tumbling | semantic | session | sliding | None`
  - `window_size_ms`
  - `session_gap_ms`
  - `boundary_flag`
- `trigger_policy` is part of the spec, but the current runtime support is intentionally narrow:
  - `operator_owned`: only `on_event`
  - `window_owned`: bounded/window snapshot grouping
- physical path selection is internal:
  - default: `window_owned` when the input is already a bounded `WindowSnapshot`
  - otherwise: `operator_owned`

**Current maintenance support**:
- `operator_owned`: supports two local maintenance/refinement modes:
  - `maintenance_trigger_policy.mode="periodic"`:
    - periodic `RECOMPUTE` timer
    - greedy merge of highly-similar groups using the current local scoring method
    - optional local label refresh when enabled in internal kernel config
    - metadata heartbeat (`last_refine_ms`, `refine_count`, `last_merge_count`)
  - `maintenance_trigger_policy.mode="on_scope_close"` on close-capable scopes:
    - supported for `session`, `tumbling`, and `semantic`
    - runs one local refine pass at close
    - then resets operator-owned group state for the next scope epoch
- `window_owned`: supports `maintenance_trigger_policy.mode="on_scope_close"`:
  - run one bounded local refine pass at snapshot close
  - remap assignment rows after local greedy merge
  - optional local label refresh before final emission when enabled in internal kernel config
  - no cross-scope group persistence
- `sliding` / pure TTL do not have a natural operator-owned close event and therefore remain unsupported for `maintenance_trigger_policy.mode="on_scope_close"`

**Current internal path support**:

| Path | Input kind | Status | Notes |
|------|------------|--------|-------|
| `operator_owned` | flat event stream | Implemented | Continuous keyed-state grouping over active groups; assignment trigger is `on_event` |
| `window_owned` | `WindowSnapshot` | Implemented | Bounded grouping within one snapshot; no cross-scope group state |

**Configuration** (`SemGroupbyConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_groups_per_key` | 50 | Maximum number of groups for one key |
| `assignment_method` | `rule` | Internal assignment backend used by planner/runtime |
| `scope_chunk_size` | `1` | Internal assignment granularity for semantic grouping (`1` = per-event, `N` = chunked, `len(scope)` = full-scope) |
| `confidence_threshold` | 0.7 | Internal reuse threshold for local assignment methods |
| `new_group_creation_threshold` | 0.3 | Internal maintenance merge threshold seed |
| `refresh_labels_during_maintenance` | `False` | Whether maintenance locally refreshes survivor labels |
| `overflow_policy` | `DROP_OLDEST` | Eviction policy when the number of groups overflows |

**Assignment flow**:
1. Path resolution:
   - `window_owned` runs bounded grouping on one snapshot
   - `operator_owned` maintains keyed group state across events
2. The operator evaluates the current `existing_groups`
3. Final outcome is always one of:
   - assign to one existing group
   - create one new group
4. Local methods (`rule`, `embedding`) decide synchronously using internal thresholds
5. Async semantic methods (`llm`) emit `AsyncWorkItem("classify")` and only output the final assignment after async merge-back
6. Internal chunking is planner-controlled through `scope_chunk_size`
   - `1` = event-by-event assignment
   - `N` = chunked assignment
   - `len(scope)` = one full-scope assignment
7. `window_owned + llm` currently emits one scope-level async request and lets the async worker apply the internal chunk size within that scope
8. `operator_owned + llm` buffers pending events until one chunk is ready, then emits one async request for that chunk; scope-close flushes any remainder

**Group Profile structure**:
```json
{"group_id": "abc123", "label": "technical discussion", "event_count": 42, "created_ms": ..., "last_update_ms": ..., "summary": "..."}
```

### 4.3 `sem_agg` — Semantic Aggregation

**Files**:
- `operators/stateful/sem_agg.py`
- `operators/stateful/sem_agg_window.py`
- `operators/stateful/sem_agg_pipeline.py`

**Classes**:
- `SemAggFunction(KeyedProcessFunction)` — operator-owned continuous path
- `WindowOwnedSemAggFunction(KeyedProcessFunction)` — bounded/window-owned path

**Purpose**: perform semantic aggregation over keyed streams or bounded snapshots.

| Item | Description |
|------|-------------|
| **Input** | `SemEvent` dict; the bounded/window-owned path also accepts `WindowSnapshot` dict |
| **Output** | Aggregation result dict: `{key, aggregate, event_count, version, mode, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="summarize")` — emitted in summarize / compressive modes |
| **State** | `ListState[buffer]` + `ValueState[aggregate]` + `ValueState[meta]` |

**Query-spec integration** (`AggQuerySpec`):
- `agg_method = algebraic | summarize | compressive`
- `trigger_policy` is part of the spec
- `scope_policy` now also supports close-capable operator-owned scopes:
  - `window_kind = tumbling | semantic | session | sliding | None`
  - `window_size_ms`
  - `session_gap_ms`
  - `boundary_flag`
- physical path selection is internal:
  - default: `window_owned` when the input is already a bounded `WindowSnapshot`
  - otherwise: `operator_owned`

**Configuration** (`SemAggConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `mode` | `"algebraic"` | Base runtime mode; `AggQuerySpec` can resolve to `algebraic`, `summarize`, or `compressive` |
| `max_buffer_events` | 100 | Maximum buffered events in summarize mode |
| `flush_interval_ms` | 30,000 | Timer-driven summarize flush |
| `reduce_fn` | None | Binary reduction function for algebraic mode |
| `overflow_policy` | `DROP_OLDEST` | Buffer overflow policy |

When `SemAggFunction` is constructed without an explicit `AggQuerySpec`, the
config is normalized into one canonical internal query spec before execution.
The operator core does not keep a separate legacy trigger branch.

**Current internal path support**:

| Path | Input kind | Status | Notes |
|------|------------|--------|-------|
| `operator_owned` | flat event stream | Implemented | Continuous keyed-state aggregation |
| `window_owned` | `WindowSnapshot` | Implemented | Bounded aggregation within one snapshot; no cross-scope aggregate state |

**Current trigger support**:

| Path | Trigger | Status | Notes |
|------|---------|--------|-------|
| `window_owned` | upstream-owned (`on_scope_close` / `on_event`) | Implemented | The runtime consumes one bounded `WindowSnapshot`; trigger semantics are owned by the upstream window/snapshot layer |
| `operator_owned` | `on_event` | Implemented | `algebraic` emits running aggregates; `summarize` / `compressive` emit summarize work per accepted event |
| `operator_owned` | `periodic` | Implemented | Timer-driven aggregate emit / summarize flush |
| `operator_owned` | `idle_flush` | Implemented | Idle timer drives aggregate emit / summarize flush |
| `operator_owned` | `count_threshold` | Implemented | Emit / summarize after every N accepted events |
| `operator_owned` | `on_scope_close` | Implemented on close-capable scopes | Supports `session`, `tumbling`, and `semantic`; rejects `sliding` / pure TTL |

**Mode 1 — Algebraic**:
- The user provides `reduce_fn(accumulator, new_event) → updated_accumulator`
- Each incoming event triggers an immediate reduction and emits the latest accumulated value
- **No LLM needed**, pure local computation
- Example: summation `lambda acc, evt: {"total": acc["total"] + evt["value"]}`

**Mode 2 — Summarize**:
- Events are buffered in `ListState`
- `trigger_policy` now decides *when* summarize work is emitted:
  - `on_event`
  - `periodic`
  - `idle_flush`
  - `count_threshold`
  - `on_scope_close` on close-capable scopes
- `max_buffer_events` remains a **hard buffer cap**; when the live buffer hits
  that bound and no summarize request is already in flight, the runtime emits
  summarize work immediately
- The LLM asynchronously produces a summary → merge-back updates `ValueState[aggregate]`
- Events that arrive while one summarize request is in flight are preserved in the buffer; successful merge-back only removes the emitted prefix
- Suitable for semantically meaningful scenarios such as conversation summary or document synthesis

**Mode 3 — Compressive**:
- Shares the summarize runtime path
- Before emitting the summarize request, the buffered event set is locally compressed to a smaller suffix budget
- This is currently a local bounded compaction heuristic, not a separate async compressive worker

**`window_owned` handling**:
- `algebraic` → reduce the bounded snapshot directly and emit one final aggregate row
- `summarize` / `compressive` → emit one bounded summarize work item for the snapshot

**Current `operator_owned` runtime note**:
- `AggQuerySpec` now overrides runtime mode / TTL / buffer / flush settings
- `trigger_policy` now drives operator-owned runtime for:
  - `algebraic`: `on_event`, `periodic`, `idle_flush`, `count_threshold`
  - `summarize` / `compressive`: `on_event`, `periodic`, `idle_flush`, `count_threshold`
- `operator_owned + on_scope_close` is now supported for close-capable scopes:
  - `session`: idle gap closes the current scope
  - `tumbling`: bucket rollover closes the current scope
  - `semantic`: boundary flag closes the current scope after the boundary event is ingested
- `sliding` / pure TTL still do not have a natural close event and remain unsupported for `on_scope_close`

### 4.4 `sem_search` — Internal Continuous Retrieval Helper

**File**: `runtime/steps/sem_search.py`
**Class**: `SemSearchFunction(KeyedProcessFunction)`

**Purpose**: maintain a per-key retrieval cache. If the local cache hits, return directly; otherwise use Async Bridge to call an external retrieval service.

| Item | Description |
|------|-------------|
| **Input** | `SemEvent` dict (query request) |
| **Output** | Retrieval result envelope: `{key, query, query_seq_id, candidates, candidate_count, truncated, source, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="retrieve")` — emitted on cache miss |
| **State** | `MapState[candidate_id → candidate_record]` + `ValueState[meta]` |

**Configuration** (`SemSearchConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_candidates_per_request` | 20 | Maximum number returned by one retrieval |
| `max_cache_entries_per_key` | 200 | Per-key cache limit |
| `ttl_seconds` | 1800 | Cache TTL (30 minutes) |
| `evict_interval_ms` | 120,000 | Eviction scan interval |
| `cache_match_fn_name` | `"keyword"` | Local matching strategy: `keyword` or lightweight local `embedding` |
| `cache_embedding_dim` | 128 | Hashing encoder dimension for local embedding mode |
| `min_relevance_score` | 0.0 | Minimum relevance score |
| `search_backend` | None | Optional `ExternalSearchBackend`; when set, workflow code can auto-wrap it into `SearchBackendAsyncFn` |

**Retrieval flow**:
1. Receive a query event → scan the local cache (`keyword` / embedding match)
2. As long as the local cache returns any hits → emit a cache result immediately, truncated to `max_candidates_per_request`
3. Only if the local cache fully misses → emit side output `AsyncWorkItem("retrieve")` for external retrieval
4. Async merge-back: receive external retrieval results → update the `MapState` cache → emit an `source="async_store"` result
5. The current implementation does not emit a partial local result plus async supplement in the same request path; external retrieval is triggered only on a full miss

**External backend status**:
- `MockSearchBackend` is implemented and used by tests/local workflow replay
- `FaissSearchBackend` is implemented as a **very simple optional demo backend**
- `cache_match_fn_name="embedding"` now uses a local `HashingTextEncoder`; it is a lightweight hashing vectoriser, not a production semantic encoder
- the FAISS backend requires local `faiss` installation and reuses the same lightweight hashing encoder, not a production embedding model
- real production backends (Milvus/Qdrant/Elasticsearch/Pinecone/etc.) remain future work

**Relationship to V0.1 `sem_lookup_join`**:
- V0.1 is stateless: each request retrieves independently, with no cache
- V0.2 `sem_search` maintains a per-key cache, so repeated queries under the same key can achieve better cache hit rates

### 4.5 `sem_topk` — Continuous Top-K

**File**: `operators/stateful/sem_topk.py`
**Class**: `SemTopKFunction(KeyedProcessFunction)`

**Purpose**: maintain a per-key candidate pool, continuously update top-k ranking, and emit updates only when the ranking changes.

**V0.2+ kernel/builder split**:
- `SemTopKFunction` is now treated as a **pure scored-item kernel**
- scoring orchestration moved into `operators/stateful/sem_topk_pipeline.py`
- currently supported scorer/rerank paths are:
  - `external_score`
  - `embedding` (currently `mock` / lightweight local `local_hashing`)
  - `llm`
- `pairwise` / `listwise` now run over **bounded pools or operator-owned scope snapshots**
  and emit top-k snapshots directly; they do not reuse the pointwise pure kernel
- current trigger support is explicit:
  - `on_event`
  - `on_scope_close`
  - `periodic`
  - `idle_flush`
  - `count_threshold`
- physical path selection is internal:
  - bounded pools / window snapshots use the bounded path
  - flat candidate streams use the operator-owned path when trigger semantics require keyed state

| Item | Description |
|------|-------------|
| **Input** | Already-scored flat candidate record dict |
| **Output** | Top-K snapshot dict: `{key, topk, top_ids, query, query_seq_id, source, total_candidates, version, changed, emission_policy, error, timestamp_ms}` |
| **Side Output** | None in the pure kernel; async scoring is handled by the top-k pipeline builder |
| **State** | `MapState[candidate_id → candidate]` + `ValueState[snapshot]` |

**Configuration** (`SemTopKConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_candidates` | 100 | Candidate pool limit |
| `recompute_interval_ms` | 10,000 | `RECOMPUTE` timer interval |
| `emission_policy` | `"delta"` | `"delta"` or `"snapshot"` |
| `score_field` | `"score"` | Score field used for sorting |

> Note:
> - `k` now lives in `TopKQuerySpec`, not `SemTopKConfig`
> - `retrieve_to_topk_items()` only expands `candidates` and guarantees `candidate_id`; it does not rename the score field. If upstream retrieval uses `_score` or another field name, `SemTopKConfig.score_field` must be set accordingly.

**Two emission policies**:

| Policy | Behavior |
|-------|----------|
| **delta** | Emit only when the top-k list differs from the previous snapshot (reduces downstream load) |
| **snapshot** | Emit on every rerank (suitable for downstreams that need full state) |

**Processing flow**:
1. Receive a new **scored** candidate → write it into the `MapState` candidate pool
2. If the candidate pool exceeds `max_candidates` → apply `overflow_policy` to evict lower-scoring items
3. Rerank top-k: sort by `score_field` descending → take the first `k`
4. Compare with the previous snapshot → if changed (or under snapshot policy) → emit the new snapshot
5. `RECOMPUTE` timer: periodically force local recomputation from the current candidate pool and existing scores

For retrieval-assisted workflows, the candidate flow is now:

- pointwise: `retrieval envelope -> expander -> optional pointwise scorer -> pure top-k kernel`
- pairwise/listwise: `bounded retrieval pool or scope snapshot -> contextual reranker -> top-k snapshot`

**Current internal dispatch / trigger boundary**:

| Input shape | Trigger | Current Status | Notes |
| ----------- | ------- | -------------- | ----- |
| bounded pool / closed snapshot | `on_scope_close` | Implemented | Runs bounded final top-k directly on the pool |
| bounded pool / early snapshot | `on_event` | Implemented | Assumes the upstream window/pool layer emits incremental snapshots |
| flat candidate stream | `on_event` | Implemented | Canonical continuous pointwise top-k path |
| flat candidate stream | `periodic` | Implemented | Pointwise emits from the pure kernel; contextual rerank emits from operator-owned scope snapshots |
| flat candidate stream | `idle_flush` | Implemented | Pointwise emits from the pure kernel; contextual rerank emits from operator-owned scope snapshots |
| flat candidate stream | `count_threshold` | Implemented | Pointwise emits from the pure kernel; contextual rerank emits from operator-owned scope snapshots |
| flat candidate stream | `on_scope_close` | Implemented on close-capable scopes | Supports `session`, `tumbling`, and `semantic`; pointwise rejects `sliding` / pure TTL |

This means `sem_topk` now has a clean first-stage trigger contract:
- bounded inputs stay on the bounded path
- flat candidate streams stay on the operator-owned path
- unsupported combinations fail explicitly rather than silently changing semantics

---

## 5. Continuous RAG Workflow

**File**: `runtime/continuous_rag_workflow.py`

Composes all V0.2 operators into a full Continuous RAG pipeline, split into three subflows.

### 5.1 Routing Mechanism

Input events are routed by the `stream_type` field:

| `stream_type` Value | Target Subflow |
|--------------------|----------------|
| `"memory_event"` | Subflow A (memory building) |
| `"query_request"` | Subflow B (retrieval) → Subflow C (synthesis) |

Routing is implemented by a lightweight `_StreamRouter(KeyedProcessFunction)` using `OutputTag` side outputs.

### 5.2 Subflow A — Memory Building

```text
input events → key_by → sem_window → sem_groupby → sem_agg → memory sink
```

| Stage | Operator | Input | Output |
|------|----------|-------|--------|
| Windowing | `SemWindowFunction` | `SemEvent` | `WindowSnapshot` |
| Grouping | `SemGroupbyFunction` | `WindowSnapshot` (auto-expanded) | Grouping result |
| Aggregation | `SemAggFunction` | Grouping result | Aggregated memory entry |

**Async Bridge integration**: the classify side output of `sem_groupby` is connected through `build_async_bridge` to an LLM classifier.

### 5.3 Subflow B — Query Retrieval

```text
query requests → key_by → sem_search → sem_topk → retrieved context
```

| Stage | Operator | Input | Output |
|------|----------|-------|--------|
| Retrieval | `SemSearchFunction` | `SemEvent` (query) | Retrieval envelope |
| Re-ranking | `SemTopKFunction` | Candidate records (adapted by `retrieve_to_topk_items`) | Top-K snapshot |

**Contract adapter**: `retrieve_to_topk_items()` expands the `candidates` list from retrieval output into per-candidate records and guarantees that each one has a `candidate_id`. The score field name is preserved and must match `SemTopKConfig.score_field`.

### 5.4 Subflow C — Answer Synthesis

```text
(query ⊕ top-k context) → sem_map (V0.1 async) → answer with audit
```

`build_answer_subflow()` currently uses `_AnswerSynthesiser` to construct a normalized answer-request envelope with `prompt`, `query`, `retrieved_ids`, `workflow_version`, `config_version`, and other audit fields. Whether a real LLM is called for final answer generation is decided by the downstream test harness or external consumer; in the real-mode workflow test, this step is executed through DeepSeek.

### 5.5 Stage-Aware Async Merge Functions

`continuous_rag_workflow.py` now defines stage-specific merge-back functions rather than a single generic `_AsyncMergeFunction`:

| Class | Role |
|------|------|
| `_ClassifyAsyncMergeFunction` | Normalizes classify results into grouping assignment envelopes |
| `_SummarizeAsyncMergeFunction` | Normalizes summarize results into `sem_agg`-style outputs |
| `_RetrieveAsyncMergeFunction` | Normalizes retrieve results into retrieval envelopes |

---

## 6. Glossary

| Term | Meaning |
|------|---------|
| **SemEvent** | Standard V0.2 input event, containing fields such as `key`, `payload`, and `seq_id` |
| **WindowSnapshot** | Full window snapshot produced by `sem_window`, containing all events in the window |
| **Keyed State** | Flink state partitioned by key; each key has its own independent state instance |
| **ListState** | Ordered list state, used for event buffering (`sem_window`, `sem_agg`) |
| **MapState** | Key-value mapping state, used for group profiles, retrieval cache, and candidate pools |
| **ValueState** | Single-value state, used for metadata, accumulators, and snapshots |
| **TTL (Time-To-Live)** | Automatic state expiration mechanism that prevents unbounded growth |
| **OverflowPolicy** | Strategy used when a state container is full: `DROP_OLDEST` / `DROP_NEWEST` |
| **Side Output** | Flink `OutputTag` mechanism that routes data to side streams outside the main output |
| **Async Bridge** | Async bridge topology: side output → `AsyncDataStream` → union → merge |
| **AsyncWorkItem** | Async work request emitted by an operator (`task_type` + `payload`) |
| **AsyncResult** | Result returned after async work finishes |
| **Merge-back** | The process where async results return to the main stream and are handled by a `MergeFunction` |
| **TimerCategory** | Timer type: `FLUSH` (flush), `RECOMPUTE` (recompute), `EVICT` (evict) |
| **TimerPolicy** | Per-operator timer interval configuration |
| **Boundary Flag** | Semantic boundary marker in an event (e.g. `topic_shift`), used to trigger window closure |
| **Delta Emission** | Emit updates only when the top-k list changes (vs snapshot mode, which emits every time) |
| **Contract Adapter** | Format conversion function between operators (e.g. `window_snapshot_to_sem_events`) |
| **Retrieval Envelope** | Unified dict format for retrieval results: `{key, query, query_seq_id, candidates, candidate_count, truncated, source, timestamp_ms}` |
| **Continuous RAG** | Streaming RAG mode with continuously built memory plus continuously executed retrieval |
| **Subflow** | A sub-pipeline inside the topology (`A=memory build`, `B=retrieval`, `C=answer synthesis`) |

---

## 7. Metrics — Metric System

**File**: `runtime/stateful_metrics.py`

### 7.1 Architecture

Each V0.2 operator creates a metric instance in `open()` by calling `StatefulOperatorMetrics.from_runtime_context(ctx, operator_name)`.

**Dual-track mode**:
- **Flink MetricGroup mode**: in real Flink runtime, registers under the metric group `cp_stateful.<operator_name>`
- **Local-only mode**: when Flink `MetricGroup` is unavailable (e.g. unit tests), it automatically degrades to local accumulators

### 7.2 OperatorTag

Each operator instance carries immutable metadata tags:

| Field | Meaning |
|------|---------|
| `operator_name` | Operator name (e.g. `"sem_window"`) |
| `operator_version` | Operator version (`"v0.2.0"`) |
| `workflow_version` | Workflow version |
| `config_hash` | Configuration hash (used for drift detection) |

These tags are embedded into output records and metric labels for auditing and replay.

### 7.3 Counters

| Metric Name | Method | Meaning |
|------------|--------|---------|
| `events_processed` | `record_event_processed()` | Total number of processed events |
| `timer_fires` | `record_timer_fire()` | Number of timer callback invocations |
| `evictions` | `record_eviction(count)` | Number of entries evicted by the overflow policy |
| `overflows` | `record_overflow()` | Number of overflow incidents (buffer/state hit the limit) |
| `stale_windows` | `record_stale_window()` | Number of windows closed due to timeout |
| `boundary_triggers` | `record_boundary_trigger()` | Number of semantic-boundary-triggered closures |
| `recomputes` | `record_recompute()` | Number of top-k / aggregation recomputations |
| `async_emits` | `record_async_emit()` | Number of emitted async work items |

### 7.4 Gauges

| Metric Name | Method | Meaning |
|------------|--------|---------|
| `state_size` | `update_state_size(size)` | Current number of keyed state entries |
| `async_queue_depth` | `update_async_queue_depth(depth)` | Number of pending async work items |

### 7.5 Metric Snapshot

`metrics.snapshot()` returns a dict containing all counters and tags, which can be embedded into audit records:

```python
{
    "operator_name": "sem_window",
    "operator_version": "v0.2.0",
    "events_processed": 1042,
    "timer_fires": 15,
    "evictions": 3,
    "overflows": 1,
    "stale_windows": 2,
    "boundary_triggers": 8,
    "recomputes": 0,
    "async_emits": 0,
    "state_size": 47,
    "async_queue_depth": 0,
}
```

### 7.6 Metric Usage By Operator

| Operator | Main Metrics Used |
|---------|-------------------|
| `sem_window` | `events_processed`, `timer_fires`, `stale_windows`, `boundary_triggers`, `evictions` |
| `sem_groupby` | `events_processed`, `async_emits` (`classify`), `overflows`, `evictions` |
| `sem_agg` | `events_processed`, `timer_fires` (`flush`), `async_emits` (`summarize`), `overflows` |
| `sem_search` | `events_processed`, `async_emits` (`retrieve`), `evictions`, `state_size` |
| `sem_topk` | `events_processed`, `recomputes`, `evictions`, `state_size` |

---

## 8. V0.2+ Addendum — API Alignment Changes

This section documents the naming, configuration, and capability changes introduced in the V0.2+ alignment pass. Deprecated public aliases are being removed rather than preserved indefinitely.

### 8.1 Naming Cleanup

This section documents the naming cleanup introduced in the V0.2+ alignment pass.
Deprecated public aliases are being removed rather than preserved indefinitely.

| Older Public Name | Canonical Name | Module | Reason |
| ----------------- | -------------- | ------ | ------ |
| `sem_join_retrieve` | `sem_lookup_join` | `operators/row/sem_lookup_join.py` | Aligns with Flink SQL `LOOKUP JOIN` semantics |
| `SemTopKFunction` (row-style local) | `SemLocalTopKFunction` | `operators/row/sem_local_topk.py` | Disambiguates from stateful `SemTopKFunction` |

**Import examples:**

```python
from pyflink.semantic_runtime import (
    context,
    sem_agg,
    sem_filter,
    sem_groupby,
    sem_local_topk,
    sem_lookup_join,
    sem_map,
    sem_topk,
    sem_window,
)
```

### 8.2 `sem_map` Dual-Mode

`sem_map` now supports two modes controlled by `SemSpec.schema` and `SemSpec.output_mode`:

| Mode | `output_schema` | `return_mode` | Output | Use Case |
| ---- | --------------- | ------------- | ------ | -------- |
| **Structured** | `dict` (required) | `"json"` (default) | JSON with validated schema | Extraction, classification |
| **Free-form** | `None` | `"text"` (implicit) | `{"input": ..., "text": ..., "_mode": "text"}` | Rewriting, summarisation, answer synthesis |

**Key rules:**

- When `output_schema` is `None`, `return_mode` is forced to `"text"` regardless of the passed value.
- In free-form mode, no JSON parsing or schema validation is performed on the LLM response.
- The output envelope always includes `_mode` and `_latency_ms` for observability.

```python
# Structured mode request
req = sem_map(
    intent="Extract sentiment: {input}",
    output_schema={"sentiment": str, "confidence": float},
)

# Free-form text mode request
req = sem_map(intent="Rewrite this in formal English: {input}")
```

### 8.3 `sem_topk` Ranking Semantics

The public `sem_topk` contract is now expressed as:

- `sem_topk(intent=..., k=..., context=...)`

Internal planning then decides:

- ranking method
- scorer backend
- path selection
- chunking
- trigger plan

| Ranking Method | Current Status | Boundary Requirement |
| -------------- | -------------- | -------------------- |
| `"pointwise"` | Implemented | None beyond normal active-scope maintenance |
| `"pairwise"` | Implemented | Requires a bounded candidate pool or an operator-owned scope snapshot |
| `"listwise"` | Implemented | Requires a bounded candidate pool or an operator-owned scope snapshot |

**Important current boundary**:

- trigger and path decisions are internal.
- The current implementation realizes:
  - bounded-pool final rerank (`window_owned + on_scope_close`)
  - bounded-pool early-snapshot rerank (`window_owned + on_event`)
  - operator-owned continuous pointwise top-k (`operator_owned + on_event`)
  - operator-owned timer-driven pointwise top-k (`operator_owned + periodic`)
  - operator-owned contextual rerank over scope snapshots
    - `periodic`
    - `idle_flush`
    - `count_threshold`
    - `on_scope_close` on natural-close scopes
    - internal close surrogates for non-close scopes such as `sliding` / pure `TTL`
- optimizer-selected trigger policies remain internal planner/CBO work; they
  are not part of the public user-facing API.

**Internal contextual execution plan**:

- `TopKContextualPlan` is internal-only; it is not part of `TopKQuerySpec`
- it currently carries:
  - `context_chunk_size`
  - `merge_strategy`
  - `close_surrogate`
- current defaults:
  - `pairwise` -> `context_chunk_size=2`, `merge_strategy="tournament"`
  - `listwise` -> full-pool context, `merge_strategy="global_rank"`
- current surrogate choices for operator-owned contextual rerank:
  - `sliding + on_scope_close` -> internal `epoch_close`
  - pure `TTL + on_scope_close` -> internal `periodic_snapshot`

### 8.4 `SemSpec` — Unified Semantic Criterion

**File**: `sem_spec.py`
**Class**: `SemSpec` (dataclass)

A minimal specification that captures the semantic "what to do" for an operator, decoupled from state management and topology.

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `instruction` | `str` | `""` | Prompt template, predicate, or scoring criterion |
| `backend` | `str` | `"llm"` | One of `"llm"`, `"embedding"`, `"hybrid"`, `"rule"`, `"external_score"` |
| `output_mode` | `str` | `"json"` | One of `"bool"`, `"label"`, `"score"`, `"json"`, `"text"`, `"summary"` |
| `schema` | `dict \| None` | `None` | Expected key→type mapping when `output_mode="json"` |
| `threshold` | `float \| None` | `None` | Confidence / score decision boundary |
| `examples` | `list` | `[]` | Few-shot examples for the LLM backend |
| `metadata` | `dict` | `{}` | Arbitrary operator-specific metadata |

**Convenience constructors:**

```python
# For sem_map
spec = SemSpec.for_sem_map("Extract: {input}",
                                 output_schema={"key": str},
                                 return_mode="json")

# For sem_topk
spec = SemSpec.for_sem_topk(
    "Rank by relevance",
    threshold=0.5,
)
```

Supports `to_dict()` / `from_dict()` for JSON/YAML serialisation.

### 8.5 `RuntimeConfig` — Internal Typed Configuration Entry

**File**: `runtime_config.py`
**Class**: `RuntimeConfig` (dataclass)

`RuntimeConfig` is an internal assembly/configuration object. It is used by
planner/runtime code, not by the intended public user-facing API.

It now does three things:

- stores backend/default config
- stores operator-specific internal runtime config
- hydrates typed query specs, typed kernel configs, and typed runtime bundles

| Section | Sub-Config | Key Fields |
| ------- | ---------- | ---------- |
| `defaults` | `DefaultsConfig` | `ttl_seconds`, `overflow_policy`, `async_timeout_ms`, `async_capacity`, `metrics_enabled` |
| `llm` | `LLMBackendConfig` | `backend`, `model`, `api_key`, `endpoint`, `temperature`, `max_tokens` |
| `embedding` | `EmbeddingBackendConfig` | `backend`, `model`, `endpoint`, `dimensions` |
| `operators` | `Dict[str, Dict]` | Nested operator config sections. Semantic operators use `query_spec` + `kernel`; runtime helpers such as `sem_window` / `sem_search` use `kernel` only |
| `workflow` | `Dict` | Workflow-level settings |

Public users should prefer the facade:

```python
req = sem_topk(
    intent="rank weather days",
    k=5,
    context=context("window"),
)
```

Internal runtime assembly then lowers that request using `RuntimeConfig`.

**Typed helpers**:

- `get_topk_query_spec()`
- `get_groupby_query_spec()`
- `get_agg_query_spec()`
- `get_join_query_spec()`
- `get_window_config()`
- `get_topk_kernel_config()`
- `get_groupby_kernel_config()`
- `get_agg_kernel_config()`
- `resolve_topk_runtime_bundle()`
- `resolve_groupby_runtime_bundle()`
- `resolve_agg_runtime_bundle()`
- `resolve_join_runtime_bundle()`

**Runtime bundle contents**:

- typed `QuerySpec`
- typed kernel config
- internal `SemLoweringPlan`

This is the bridge between:

- public semantic config
- internal lowering view (`semantic attribute + classical operator`)
- native runtime path selection

**Layout rules**:

- semantic operators (`sem_topk`, `sem_groupby`, `sem_agg`, `sem_join`) must use nested `query_spec` + `kernel`
- runtime helpers (`sem_window`, `sem_search`) must use nested `kernel`
- defaults such as `ttl_seconds` are injected into operator scope policies during typed hydration

**Workflow bridge**:

The composed stateful workflow can now be built from the same typed config
entry:

```python
workflow_cfg = ContinuousRAGConfig.from_runtime_config(cfg)
streams = build_continuous_rag_workflow_from_runtime_config(input_ds, cfg)
```

This keeps workflow assembly on the same `QuerySpec + kernel + lowering`
contract already used by the per-operator builders.

### 8.6 External Search Backend Interface

**File**: `runtime/external_search_backend.py`
**Status**: Abstract contract with demo implementations.

Defines the pluggable interface for external vector/search backends used by `sem_search` (`sem_search`) and `sem_lookup_join`.

| Class | Description |
| ----- | ----------- |
| `SearchResult` | Dataclass: `candidate_id`, `text`, `score`, `metadata` |
| `ExternalSearchBackend` | Abstract base class with `open()`, `close()`, `async search(query, top_k)` |

**Current implementations**:

- `MockSearchBackend`
- `FaissSearchBackend` (demo only; not production-grade semantic retrieval)

**Planned production-grade implementations** (future phase):

- `MilvusSearchBackend`
- `QdrantSearchBackend`
- `ElasticsearchSearchBackend`
- `PineconeSearchBackend`

```python
class MilvusSearchBackend(ExternalSearchBackend):
    async def search(self, query: str, top_k: int = 10, **kwargs) -> list[SearchResult]:
        # call milvus client
        return [SearchResult(candidate_id="...", text="...", score=0.95)]
```
