# Semantic Operators — Complete Technical Reference

> **Applicable Versions**: V0.1 (Non-Stateful) + V0.2 (Stateful)
> **Generated On**: 2026-03-17
> **Code Path**: `flink-python/pyflink/semantic_runtime/`

---

## Table Of Contents

1. [Overview](#1-overview)
2. [V0.1 Non-Stateful Semantic Operators](#2-v01-non-stateful-semantic-operators)
   - 2.1 [Shared Infrastructure (`_common.py`)](#21-shared-infrastructure)
   - 2.2 [`sem_filter` — Semantic Filtering](#22-sem_filter--semantic-filtering)
   - 2.3 [`sem_map` — Semantic Mapping / Extraction](#23-sem_map--semantic-mapping--extraction)
   - 2.4 [`sem_join_retrieve` — Retrieval-Augmented Semantic Join](#24-sem_join_retrieve--retrieval-augmented-semantic-join)
   - 2.5 [`sem_topk` — Local Semantic Re-ranking](#25-sem_topk--local-semantic-re-ranking)
3. [V0.2 Stateful Foundation Modules](#3-v02-stateful-foundation-modules)
   - 3.1 [`event_model.py` — Event Model And Contract Adapters](#31-event_modelpy--event-model-and-contract-adapters)
   - 3.2 [`state_descriptors.py` — Centralized State Descriptors](#32-state_descriptorspy--centralized-state-descriptors)
   - 3.3 [`timer_policy.py` — Timer Policy](#33-timer_policypy--timer-policy)
   - 3.4 [`async_bridge.py` — Async Bridge Pattern](#34-async_bridgepy--async-bridge-pattern)
4. [V0.2 Stateful Semantic Operators](#4-v02-stateful-semantic-operators)
   - 4.1 [`sem_window` — Semantic Window](#41-sem_window--semantic-window)
   - 4.2 [`sem_groupby` — Dynamic Semantic Grouping](#42-sem_groupby--dynamic-semantic-grouping)
   - 4.3 [`sem_agg` — Semantic Aggregation](#43-sem_agg--semantic-aggregation)
   - 4.4 [`cts_retrieve` — Continuous Retrieval](#44-cts_retrieve--continuous-retrieval)
   - 4.5 [`sem_topk_continuous` — Continuous Top-K](#45-sem_topk_continuous--continuous-top-k)
5. [Continuous RAG Workflow](#5-continuous-rag-workflow)
6. [Glossary](#6-glossary)
7. [Metrics — Metric System](#7-metrics--metric-system)

---

## 1. Overview

This project builds a set of **Semantic Operators** on top of Apache Flink (PyFlink), embedding LLM calls into stream processing pipelines. The implementation is split into two phases:

| Phase | Operator Type | Flink Base Class | State Management | LLM Interaction |
|------|---------------|------------------|------------------|-----------------|
| **V0.1** | Non-Stateful | `AsyncFunction` | No keyed state | Each record directly calls the LLM |
| **V0.2** | Stateful | `KeyedProcessFunction` | Flink keyed state + TTL | Uses Async Bridge side outputs for asynchronous LLM calls |

**V0.1** provides four operators: `sem_filter`, `sem_map`, `sem_join_retrieve`, and `sem_topk`.
**V0.2** provides five stateful operators: `sem_window`, `sem_groupby`, `sem_agg`, `cts_retrieve`, and `sem_topk_continuous`, along with four foundation modules; workflow composition and the metric system are documented separately in Sections 5 and 7.

---

## 2. V0.1 Non-Stateful Semantic Operators

All V0.1 operators inherit from `AsyncFunction` and use Flink's `AsyncDataStream` asynchronous I/O pattern.

**Shared design principles**:
- **`__init__` stores only serializable configuration**: no live LLM connections or runtime objects
- **`open()` creates the LLM client**: deferred initialization through `create_llm_client(config)`
- **`timeout()` never throws**: it returns a degraded record
- **1:1 output guarantee**: every input record produces exactly one output (normal or degraded)

### 2.1 Shared Infrastructure

**File**: `operators/_common.py`

| Function | Purpose |
|---------|---------|
| `validate_schema(obj, schema)` | Shallow type check: verifies whether a dict has the required keys with matching types |
| `make_degraded(value, error)` | Creates a degraded output envelope: `{"_input": value, "_error": error, "_degraded": True}` |
| `make_degraded_json(value, error)` | JSON string version of `make_degraded` |
| `attach_metrics(parsed, metrics)` | Attaches LLM call metrics (latency, tokens, attempts) to the output dict |

### 2.2 `sem_filter` — Semantic Filtering

**File**: `operators/sem_filter.py`
**Class**: `SemFilterFunction(AsyncFunction)`

**Purpose**: Calls the LLM for each record to decide whether it should be kept, producing `{decision, confidence, reason}`.

| Item | Description |
|------|-------------|
| **Input** | Arbitrary string records (Flink `Types.STRING()` or `PICKLED_BYTE_ARRAY`) |
| **Output** | JSON string: `{"decision": bool, "confidence": float, "reason": str, "_input": ..., "_metrics": {...}}` |
| **Prompt** | `prompt_template.format(input=value)`, requiring the LLM to return `{decision, confidence, reason}` as JSON |
| **Degraded** | `{"_degraded": True, "decision": default_decision, "confidence": 0.0, "reason": error_msg}` |

**Implementation idea**:
1. `async_invoke(value)` → format the input with the prompt template → call the LLM
2. Parse the LLM JSON response → verify the presence of the three keys `{decision, confidence, reason}`
3. Normalize types: `decision→bool`, `confidence→float`, `reason→str`
4. Attach `_metrics` and `_input` → return a JSON string
5. The actual filtering is **not performed inside the operator** — downstream uses `ds.filter(lambda x: json.loads(x)["decision"])`

> ⚠️ Design highlight: the operator itself is a **1:1 mapping** rather than a filter, preserving auditability for rejected records.

### 2.3 `sem_map` — Semantic Mapping / Extraction

**File**: `operators/sem_map.py`
**Class**: `SemMapFunction(AsyncFunction)`

**Purpose**: Calls the LLM for each record and extracts/transforms it into structured JSON output.

| Item | Description |
|------|-------------|
| **Input** | Arbitrary string records |
| **Output** | JSON string whose structure is defined by `output_schema`, with `_metrics` attached |
| **Prompt** | `prompt_template.format(input=value)` |
| **Schema** | `output_schema: Dict[str, type]` — e.g. `{"sentiment": str, "confidence": float}` |
| **Degraded** | `{"_input": value, "_error": ..., "_degraded": True}` |

**Implementation idea**:
1. `async_invoke(value)` → format the prompt → call the LLM
2. Parse the JSON → verify keys and types with `validate_schema(parsed, output_schema)`
3. Attach `_metrics` → return a JSON string
4. If parsing fails or the schema does not match → return a degraded record

### 2.4 `sem_join_retrieve` — Retrieval-Augmented Semantic Join

**File**: `operators/sem_join_retrieve.py`
**Class**: `SemJoinRetrieveFunction(AsyncFunction)`

**Purpose**: For each record, first retrieve an external candidate set, then let the LLM perform semantic matching / joining.

| Item | Description |
|------|-------------|
| **Input** | Arbitrary string record (query) |
| **Output** | JSON: `{"_input": ..., "join_result": <LLM parsed result>, "candidate_count": int, "truncated": bool, "_metrics": {...}}` |
| **Prompt** | `prompt_template.format(input=value, candidates=json.dumps(candidates))` |
| **Retriever** | `CandidateRetriever` abstract interface (V0.1 only includes `MockCandidateRetriever`) |
| **Degraded** | `{"_input": ..., "_error": "retrieve_timeout"/"llm_call_error"/...}` |

**Implementation idea**:
1. `async_invoke(value)` → call `CandidateRetriever.retrieve(query, max_candidates)` to fetch candidates
2. Strict timeout control: `asyncio.wait_for(retrieve_call, timeout=retrieve_timeout_ms/1000)`
3. Hard upper bound truncation: if candidate count exceeds `max_candidates_per_record`, truncate and mark `truncated=True`
4. Send the input and candidate set to the LLM for semantic matching
5. Parse the LLM output → wrap it as `join_result` → return

**Key configuration** (`SemJoinRetrieveConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_candidates_per_record` | 20 | Maximum number of candidates per record |
| `retrieve_timeout_ms` | 5000 | Retrieval timeout in milliseconds |
| `mock_candidates` | None | Fixed candidate set for testing |

### 2.5 `sem_topk` — Local Semantic Re-ranking

**File**: `operators/sem_topk.py`
**Class**: `SemTopKFunction(AsyncFunction)` *(note the distinction from the V0.2 class with the same name)*

**Purpose**: For records that contain a candidate list, call the LLM to rerank and return top-k.

| Item | Description |
|------|-------------|
| **Input** | JSON string that must contain the candidate list designated by `candidates_field` |
| **Output** | JSON: `{"_input": ..., "top_k": [reranked list], "k": int, "original_count": int, "_metrics": {...}}` |
| **Prompt** | `prompt_template.format(input=json.dumps(record), candidates=json.dumps(candidates))` |
| **Degraded** | Degraded record on input parse failure or if the LLM does not return a list |

**Implementation idea**:
1. `async_invoke(value)` → parse the input JSON → extract the `candidates` field
2. Send the full record and the candidate list to the LLM using the prompt
3. The LLM returns a reranked JSON list → truncate to top-k
4. Wrap the result → return

**V0.1 vs V0.2 TopK comparison**:

| Dimension | V0.1 `sem_topk` | V0.2 `sem_topk_continuous` |
|----------|------------------|----------------------------|
| Base class | `AsyncFunction` | `KeyedProcessFunction` |
| State | None (reruns each time) | Yes (keyed `MapState` maintains a candidate pool) |
| Trigger | Every input | Incremental update + timer-driven rerank |
| LLM | Called every time | The current implementation does not embed an LLM call; it relies on upstream scores and timer-driven local recomputation |

---

## 3. V0.2 Stateful Foundation Modules

V0.2 introduces four core foundation modules, and all stateful operators depend on them. Workflow composition and metrics are documented separately below.

### 3.1 `event_model.py` — Event Model And Contract Adapters

**File**: `stateful/event_model.py`

Provides two core data classes and seven contract adapter functions.

**`SemanticEvent`** — the standard input format for all V0.2 operators:

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
| `window_snapshot_to_semantic_events(snap)` | `WindowSnapshot → List[SemanticEvent dict]` | **Subflow A adapter**: expands a window into per-event records and injects `window_id` into metadata |
| `window_snapshot_to_summary_event(snap)` | `WindowSnapshot → single SemanticEvent dict` | Merges the whole window into a single summary event (`payload` = concatenated event payloads) |
| `group_assignment_to_semantic_event(assignment)` | `sem_groupby output → SemanticEvent dict` | Normalizes grouping results into a stable event envelope that `sem_agg` can consume directly |
| `retrieve_to_topk_items(output)` | `CtsRetrieve output → List[candidate dict]` | **Subflow B adapter**: expands retrieval results and guarantees that every item has a `candidate_id` |
| `retrieve_to_answer_context(output)` | `CtsRetrieve output → AnswerSynthesiser input` | Normalizes retrieval output directly into `{query, retrieved_context}` |
| `topk_to_answer_context(output)` | `TopK output → AnswerSynthesiser input` | **Subflow C adapter**: normalizes into `{query, retrieved_context}` |

**Key selectors**:
- `simple_key_selector(event_dict)`: returns `event_dict["key"]`
- `composite_key_selector(*fields)`: concatenates multiple fields with `|`

### 3.2 `state_descriptors.py` — Centralized State Descriptors

**File**: `stateful/state_descriptors.py`

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
| `DEGRADE_TAG` | Accept it but mark it as degraded |

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
| `cts_retrieve_cache_descriptor` | `MapState` | Retrieval cache |
| `sem_agg_buffer_descriptor` | `ListState` | Aggregation event buffer |
| `sem_agg_value_descriptor` | `ValueState` | Aggregation accumulated value |
| `sem_agg_meta_descriptor` | `ValueState` | Aggregation metadata |
| `sem_topk_candidates_descriptor` | `MapState` | TopK candidate pool |
| `sem_topk_snapshot_descriptor` | `ValueState` | Current TopK snapshot |

### 3.3 `timer_policy.py` — Timer Policy

**File**: `stateful/timer_policy.py`

**Design goal**: provide a unified timer registration, dispatch, and cleanup mechanism for all V0.2 stateful operators.

#### TimerCategory

Three standardized timer categories:

| Category | Purpose | Typical Users |
|---------|---------|---------------|
| `FLUSH` | Timeout-based flushing of buffered state | `sem_window` (window timeout), `sem_agg` (aggregation flush) |
| `RECOMPUTE` | Periodic recomputation | `sem_topk` (periodic rerank) |
| `EVICT` | Clearing expired state | `sem_groupby`, `cts_retrieve`, and any keyed operator with eviction scans enabled |

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

**File**: `stateful/async_bridge.py`

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

**File**: `stateful/semantic_window.py`
**Class**: `SemWindowFunction(KeyedProcessFunction)`

**Purpose**: split an event stream into windows based on semantic boundaries rather than fixed time/count only.

| Item | Description |
|------|-------------|
| **Input** | `SemanticEvent` dict (keyed stream) |
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

**File**: `stateful/sem_groupby_stateful.py`
**Class**: `SemGroupbyFunction(KeyedProcessFunction)`

**Purpose**: dynamically assign events to semantic categories (`group`s), supporting new group creation and asynchronous LLM classification.

| Item | Description |
|------|-------------|
| **Input** | `SemanticEvent` dict or `WindowSnapshot` dict (automatically expanded) |
| **Output** | Main-path assignment envelope: `{key, group_id, confidence, source, event_seq_id, payload, event_time_ms, metadata, boundary_flags}` |
| **Side Output** | `AsyncWorkItem(task_type="classify")` — emitted on low-confidence assignment |
| **State** | `MapState[group_id → group_profile]` + `ValueState[meta]` |

**Configuration** (`SemGroupbyConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_groups_per_key` | 50 | Maximum number of groups for one key |
| `confidence_threshold` | 0.7 | Below this value → send async classification |
| `new_group_creation_threshold` | 0.3 | If similarity to existing groups is below this value → create a new group |
| `overflow_policy` | `DROP_OLDEST` | Eviction policy when the number of groups overflows |

**Assignment flow**:
1. Input detection: `WindowSnapshot` → expand via `window_snapshot_to_semantic_events()` → process one by one
2. Local candidate matching: iterate over existing `group_profile`s and compute keyword-based similarity
3. High-confidence match (≥ threshold) → assign directly and update the profile counter
4. Low-confidence but with a candidate → temporary assignment + side output `AsyncWorkItem("classify")` for async confirmation
5. No matching candidate → create a new group → assign
6. Async merge-back (standalone operator mode): when `{task_type: "classify"}` is received → update the group profile label / timestamp and emit a compact confirmation result `{key, group_id, source, request_id}`

**Group Profile structure**:
```json
{"group_id": "abc123", "label": "technical discussion", "event_count": 42, "created_ms": ..., "last_update_ms": ..., "summary": "..."}
```

### 4.3 `sem_agg` — Semantic Aggregation

**File**: `stateful/sem_agg_stateful.py`
**Class**: `SemAggFunction(KeyedProcessFunction)`

**Purpose**: perform incremental aggregation over keyed event streams, with support for two modes.

| Item | Description |
|------|-------------|
| **Input** | `SemanticEvent` dict or `WindowSnapshot` dict (automatically expanded) |
| **Output** | Aggregation result dict: `{key, aggregate, event_count, version, mode, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="summarize")` — emitted in summarize mode |
| **State** | `ListState[buffer]` + `ValueState[aggregate]` + `ValueState[meta]` |

**Configuration** (`SemAggConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `mode` | `"algebraic"` | `"algebraic"` or `"summarize"` |
| `max_buffer_events` | 100 | Maximum buffered events in summarize mode |
| `flush_interval_ms` | 30,000 | Timer-driven summarize flush |
| `reduce_fn` | None | Binary reduction function for algebraic mode |
| `overflow_policy` | `DROP_OLDEST` | Buffer overflow policy |

**Mode 1 — Algebraic**:
- The user provides `reduce_fn(accumulator, new_event) → updated_accumulator`
- Each incoming event triggers an immediate reduction and emits the latest accumulated value
- **No LLM needed**, pure local computation
- Example: summation `lambda acc, evt: {"total": acc["total"] + evt["value"]}`

**Mode 2 — Summarize**:
- Events are buffered in `ListState`
- When the buffer reaches `max_buffer_events` or the `FLUSH` timer fires → emit `AsyncWorkItem(task_type="summarize")`
- The LLM asynchronously produces a summary → merge-back updates `ValueState[aggregate]`
- Suitable for semantically meaningful scenarios such as conversation summary or document synthesis

**`WindowSnapshot` handling**: same as `sem_groupby` — expand with `window_snapshot_to_semantic_events()` and process per event.

### 4.4 `cts_retrieve` — Continuous Retrieval

**File**: `stateful/cts_retrieve.py`
**Class**: `CtsRetrieveFunction(KeyedProcessFunction)`

**Purpose**: maintain a per-key retrieval cache. If the local cache hits, return directly; otherwise use Async Bridge to call an external retrieval service.

| Item | Description |
|------|-------------|
| **Input** | `SemanticEvent` dict (query request) |
| **Output** | Retrieval result envelope: `{key, query, query_seq_id, candidates, candidate_count, source, degraded, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="retrieve")` — emitted on cache miss |
| **State** | `MapState[candidate_id → candidate_record]` + `ValueState[meta]` |

**Configuration** (`CtsRetrieveConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `max_candidates_per_request` | 20 | Maximum number returned by one retrieval |
| `max_cache_entries_per_key` | 200 | Per-key cache limit |
| `ttl_seconds` | 1800 | Cache TTL (30 minutes) |
| `evict_interval_ms` | 120,000 | Eviction scan interval |
| `cache_match_fn_name` | `"keyword"` | Local matching strategy |
| `min_relevance_score` | 0.0 | Minimum relevance score |

**Retrieval flow**:
1. Receive a query event → scan the local cache (`keyword` / embedding match)
2. As long as the local cache returns any hits → emit a cache result immediately, truncated to `max_candidates_per_request`
3. Only if the local cache fully misses → emit side output `AsyncWorkItem("retrieve")` for external retrieval
4. Async merge-back: receive external retrieval results → update the `MapState` cache → emit an `source="async_store"` result
5. The current implementation does not emit a partial local result plus async supplement in the same request path; external retrieval is triggered only on a full miss

**Relationship to V0.1 `sem_join_retrieve`**:
- V0.1 is stateless: each request retrieves independently, with no cache
- V0.2 `cts_retrieve` maintains a per-key cache, so repeated queries under the same key can achieve better cache hit rates

### 4.5 `sem_topk_continuous` — Continuous Top-K

**File**: `stateful/sem_topk_continuous.py`
**Class**: `SemTopKFunction(KeyedProcessFunction)`

**Purpose**: maintain a per-key candidate pool, continuously update top-k ranking, and emit updates only when the ranking changes.

| Item | Description |
|------|-------------|
| **Input** | Candidate record dict, or a retrieval envelope from `cts_retrieve` (the operator will expand `candidates` internally) |
| **Output** | Top-K snapshot dict: `{key, topk, top_ids, query, query_seq_id, source, total_candidates, version, changed, emission_policy, degraded, error, timestamp_ms}` |
| **Side Output** | The current implementation has no dedicated async rerank side output |
| **State** | `MapState[candidate_id → candidate]` + `ValueState[snapshot]` |

**Configuration** (`SemTopKConfig`):

| Parameter | Default | Meaning |
|----------|---------|---------|
| `k` | 10 | Number of top items to keep |
| `max_candidates` | 100 | Candidate pool limit |
| `recompute_interval_ms` | 10,000 | `RECOMPUTE` timer interval |
| `emission_policy` | `"delta"` | `"delta"` or `"snapshot"` |
| `score_field` | `"score"` | Score field used for sorting |

> Note: `retrieve_to_topk_items()` only expands `candidates` and guarantees `candidate_id`; it does not rename the score field. If upstream retrieval uses `_score` or another field name, `SemTopKConfig.score_field` must be set accordingly.

**Two emission policies**:

| Policy | Behavior |
|-------|----------|
| **delta** | Emit only when the top-k list differs from the previous snapshot (reduces downstream load) |
| **snapshot** | Emit on every rerank (suitable for downstreams that need full state) |

**Processing flow**:
1. Receive a new candidate → write it into the `MapState` candidate pool
2. If the candidate pool exceeds `max_candidates` → apply `overflow_policy` to evict lower-scoring items
3. Rerank top-k: sort by `score_field` descending → take the first `k`
4. Compare with the previous snapshot → if changed (or under snapshot policy) → emit the new snapshot
5. `RECOMPUTE` timer: periodically force local recomputation from the current candidate pool and existing scores

---

## 5. Continuous RAG Workflow

**File**: `stateful/continuous_rag_workflow.py`

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
| Windowing | `SemWindowFunction` | `SemanticEvent` | `WindowSnapshot` |
| Grouping | `SemGroupbyFunction` | `WindowSnapshot` (auto-expanded) | Grouping result |
| Aggregation | `SemAggFunction` | Grouping result | Aggregated memory entry |

**Async Bridge integration**: the classify side output of `sem_groupby` is connected through `build_async_bridge` to an LLM classifier.

### 5.3 Subflow B — Query Retrieval

```text
query requests → key_by → cts_retrieve → sem_topk_continuous → retrieved context
```

| Stage | Operator | Input | Output |
|------|----------|-------|--------|
| Retrieval | `CtsRetrieveFunction` | `SemanticEvent` (query) | Retrieval envelope |
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
| **SemanticEvent** | Standard V0.2 input event, containing fields such as `key`, `payload`, and `seq_id` |
| **WindowSnapshot** | Full window snapshot produced by `sem_window`, containing all events in the window |
| **Keyed State** | Flink state partitioned by key; each key has its own independent state instance |
| **ListState** | Ordered list state, used for event buffering (`sem_window`, `sem_agg`) |
| **MapState** | Key-value mapping state, used for group profiles, retrieval cache, and candidate pools |
| **ValueState** | Single-value state, used for metadata, accumulators, and snapshots |
| **TTL (Time-To-Live)** | Automatic state expiration mechanism that prevents unbounded growth |
| **OverflowPolicy** | Strategy used when a state container is full: `DROP_OLDEST` / `DROP_NEWEST` / `DEGRADE_TAG` |
| **Side Output** | Flink `OutputTag` mechanism that routes data to side streams outside the main output |
| **Async Bridge** | Async bridge topology: side output → `AsyncDataStream` → union → merge |
| **AsyncWorkItem** | Async work request emitted by an operator (`task_type` + `payload`) |
| **AsyncResult** | Result returned after async work finishes |
| **Merge-back** | The process where async results return to the main stream and are handled by a `MergeFunction` |
| **TimerCategory** | Timer type: `FLUSH` (flush), `RECOMPUTE` (recompute), `EVICT` (evict) |
| **TimerPolicy** | Per-operator timer interval configuration |
| **Boundary Flag** | Semantic boundary marker in an event (e.g. `topic_shift`), used to trigger window closure |
| **Delta Emission** | Emit updates only when the top-k list changes (vs snapshot mode, which emits every time) |
| **Degraded Record** | Fallback output produced when an LLM call fails, marked with `_degraded: True` |
| **Contract Adapter** | Format conversion function between operators (e.g. `window_snapshot_to_semantic_events`) |
| **Retrieval Envelope** | Unified dict format for retrieval results: `{key, query, query_seq_id, candidates, candidate_count, source, degraded, timestamp_ms}` |
| **Continuous RAG** | Streaming RAG mode with continuously built memory plus continuously executed retrieval |
| **Subflow** | A sub-pipeline inside the topology (`A=memory build`, `B=retrieval`, `C=answer synthesis`) |

---

## 7. Metrics — Metric System

**File**: `stateful/stateful_metrics.py`

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
| `cts_retrieve` | `events_processed`, `async_emits` (`retrieve`), `evictions`, `state_size` |
| `sem_topk` | `events_processed`, `recomputes`, `evictions`, `state_size` |
