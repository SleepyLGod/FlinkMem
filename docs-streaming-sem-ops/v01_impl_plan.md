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

# V0.1 Implementation Plan — Baseline Async Semantic Map/Filter

## Goal

Deliver 4 row-local semantic operator async wrappers (`sem_map`, `sem_filter`, local `sem_topk`,
retrieve-backed `sem_join`) on `AsyncDataStream`, with timeout/retry/fallback and core metrics,
running in **process mode** (thread mode explicitly excluded).

**Completion criteria:** stable execution under sustained load without backpressure collapse and
deterministic retry behavior.

**V0.1 measurable SLOs (required):**
- Backpressure test (QPS 100, mock latency 2s, `capacity=10`) runs 10+ minutes without task failure.
- Retry attempts per record are deterministic and bounded by policy (`max_attempts` at one owner
  layer).
- Timeout + invalid-output paths produce tagged degraded records (no silent drops).
- p95 end-to-end latency and timeout/error rates are recorded and reported per run.

## Architecture

```text
User Code
  │
  ▼
┌──────────────────────────────────────────┐
│  Semantic Operator Wrappers              │
│  sem_map / sem_filter / sem_topk /       │
│  sem_join_retrieve                       │
│  - each is an AsyncFunction subclass     │
│  - prompt construction + LLM call        │
│  - output parsing + validation           │
│  - metrics collection                    │
└────────────┬─────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────┐
│  LLM Client Abstraction                  │
│  - unified async def call(prompt) -> str │
│  - OpenAI / local mock backends          │
│  - retry / circuit breaker               │
└────────────┬─────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────┐
│  PyFlink AsyncDataStream                 │
│  unordered_wait / ordered_wait           │
│  + AsyncRetryStrategy                    │
└──────────────────────────────────────────┘
```

## Step-by-Step

### Step 1: Project Skeleton + Environment Validation

- Create working directory `flink-python/pyflink/semantic_runtime/`.
- Write a minimal PyFlink DataStream job: source → `AsyncDataStream.unordered_wait(mock_async_func)` → print sink.
- Mock function: `async_invoke` does `await asyncio.sleep(0.1)` + returns original data.
- Add an explicit startup guard that validates Python execution mode is not `thread`.
- Validate end-to-end in process mode (Java env, Python Worker, Py4J channel all healthy).

**Deliverable:** runnable minimal async job confirming the dev environment.

### Step 2: LLM Client Abstraction

- Define `LLMClient` protocol/base: `async def call(self, prompt: str) -> str`.
- Implement two backends:
  - `MockLLMClient`: fixed delay + fixed/templated response (dev/test).
  - `OpenAILLMClient`: real OpenAI API (or compatible endpoint).
- Client-level timeout and basic retry (exponential backoff for 429/503/timeout), but only for
  network/API-layer transient failures.
- Record per-call latency, token usage, success/failure.

**Deliverable:** independently testable LLM client, no Flink dependency.

### Step 3: `sem_map` — First Semantic Operator

- Implement `SemMapFunction(AsyncFunction)`:
  - `open(runtime_context)`: initialize `LLMClient` (NOT in `__init__` — pickle constraint).
  - `async_invoke(value)`: construct prompt → call LLM → parse structured output → yield `[result]`.
  - `timeout(value)`: return fallback result with error marker (do not throw).
- Accept `prompt_template: str` and `output_schema: dict` in `__init__`.
- Output parsing: LLM returns JSON → validate schema → mark invalid on parse failure.
- Usage: `AsyncDataStream.unordered_wait(ds, SemMapFunction(template, schema, llm_config), timeout, capacity)`.

**Key design:** `__init__` params must be fully picklable. `LLMClient` instance created in `open()`.

### Step 4: `sem_filter` — Semantic Filter with Confidence

- Implement `SemFilterFunction(AsyncFunction)`:
  - Prompt requires LLM to return `{decision: bool, confidence: float, reason: str}`.
  - `async_invoke` returns `[original_record_with_decision]` (1:1 mapping, no in-operator filtering).
  - Actual filtering via downstream `DataStream.filter(lambda x: x.decision)`.
- Rationale: keeps async operator output count consistent; filtered records available for quality audit.

### Step 5: Local `sem_topk` + Retrieve-Backed `sem_join`

- `SemTopKFunction(AsyncFunction)`: input record carries candidate list, LLM reranks, returns top-k.
- `SemJoinRetrieveFunction(AsyncFunction)`: `async_invoke` retrieves candidates from external vector store/DB, then LLM performs semantic matching.
- Add hard bounds for retrieve-backed join in V0.1:
  - `max_candidates_per_record` (hard cap).
  - `retrieve_timeout_ms` (strict timeout budget).
  - deterministic overflow policy (`truncate` with explicit metric tag).
- Both share the same async execution path as `sem_map`; differ in prompt construction and output handling.

### Step 6: Metrics Collection

- In each `AsyncFunction.open()`, register metrics via `runtime_context.get_metrics_group()` if available; fall back to side output / logging.
- Core metrics:
  - `call_latency_ms` (histogram or avg)
  - `timeout_count` / `error_count`
  - `token_usage` (input + output tokens)
  - `invalid_output_count` (parse failures)
  - `capacity_utilization` (estimated at wrapper layer or via external monitoring)

### Step 7: Retry Strategy

- Split retry ownership to avoid multiplicative retries:
  1. **LLM Client layer (owner):** retries only transient API/network failures (429/503/connection timeout).
  2. **Flink AsyncRetryStrategy (owner):** retries only operator-level transient failures outside client-call retry scope.
- Explicitly disallow same error class being retried by both layers.
- `timeout()` returns degraded result instead of throwing, preventing single-record timeout from killing the task.
- Add run-level metric checks: effective attempts per record, retry success rate, and retry exhaustion count.

### Step 8: End-to-End Integration Tests

- Pipeline: `source → sem_map → sem_filter → filter → sink` using `MockLLMClient`.
- Test scenarios:
  1. **Normal:** verify output schema correctness.
  2. **Timeout:** mock delay > timeout, verify fallback behavior.
  3. **Error:** mock returns invalid JSON, verify parse failure handling.
  4. **Backpressure:** mock delay 2s + capacity 10 + input QPS 100; verify stable run for 10+ minutes,
     no task restarts, and no silent record drops.
  5. **Retry:** mock first N calls fail then succeed; verify bounded attempts and no duplicate retry
     ownership between client and Flink layers.

**Pass/Fail thresholds (must be logged per test run):**
- `p95_end_to_end_latency_ms`
- `timeout_rate`
- `invalid_output_rate`
- `avg_attempts_per_record` and `max_attempts_per_record`
- `degraded_output_rate`

## File Structure

```text
flink-python/pyflink/semantic_runtime/
├── __init__.py
├── llm_client.py              # LLMClient base + MockLLMClient + OpenAILLMClient
├── operators/
│   ├── __init__.py
│   ├── sem_map.py             # SemMapFunction(AsyncFunction)
│   ├── sem_filter.py          # SemFilterFunction(AsyncFunction)
│   ├── sem_topk.py            # SemTopKFunction(AsyncFunction)
│   └── sem_join_retrieve.py   # SemJoinRetrieveFunction(AsyncFunction)
├── metrics.py                 # metrics collection utilities
└── examples/
    └── v01_baseline.py        # end-to-end example job
```

## Execution Order

Recommended: **1 → 2 → 3 → 8 (single-operator e2e) → 4 → 5 → 6 → 7 → 8 (full test suite)**

Get Step 1 working first — if the local PyFlink environment has issues, everything else is blocked.

## Excluded from V0.1

- `sem_agg` (requires keyed state + explicit aggregation scope → V0.2).
- Continuous `sem_topk` (requires incremental state + timer-driven updates → V0.2).
- True two-input `sem_join` (requires two-input coordination + dual-side state → V0.3a).
- Batching / fusion (→ V0.3b).
- Dynamic broadcast control (→ V0.4).
