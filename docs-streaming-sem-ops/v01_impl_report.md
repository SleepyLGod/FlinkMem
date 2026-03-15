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

# V0.1 Implementation Report — Baseline Async Semantic Operators

**Status**: ✅ Complete
**Date**: 2026-03-14
**PyFlink Version**: 2.2.0
**Execution Mode**: `process` (thread mode explicitly excluded)

---

## 1. Overview

V0.1 delivers four row-local semantic operator `AsyncFunction` wrappers on PyFlink's
`AsyncDataStream`, with timeout/retry/fallback, structured metrics, and a dual-layer
retry strategy.  All operators run in **process mode** through PyFlink's standard
Java↔Python Worker channel (Py4J + loopback).

### Operators Implemented

| Operator | Class | Description |
|----------|-------|-------------|
| `sem_map` | `SemMapFunction` | 1:1 semantic transformation — LLM returns structured JSON |
| `sem_filter` | `SemFilterFunction` | 1:1 semantic filtering — LLM returns `{decision, confidence, reason}` |
| `sem_topk` | `SemTopKFunction` | Local top-k reranking — LLM reranks candidate list |
| `sem_join_retrieve` | `SemJoinRetrieveFunction` | Retrieve-backed semantic join — async retrieval + LLM matching |

---

## 2. Architecture

```text
User Code
  │
  ▼
┌──────────────────────────────────────────┐
│  Semantic Operator Wrappers              │
│  (SemMapFunction / SemFilterFunction /   │
│   SemTopKFunction / SemJoinRetrieveFunc) │
│  - prompt construction + LLM call        │
│  - output parsing + schema validation    │
│  - metrics recording (OperatorMetrics)   │
│  - degraded fallback on timeout/error    │
└────────────┬─────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────┐
│  LLM Client Abstraction                  │
│  - MockLLMClient / OpenAILLMClient       │
│  - Layer 1 retry: 429/503/connection     │
│  - Per-call metrics (LLMCallMetrics)     │
└────────────┬─────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────┐
│  PyFlink AsyncDataStream                 │
│  unordered_wait / unordered_wait_with_   │
│  retry + AsyncRetryStrategy (Layer 2)    │
└──────────────────────────────────────────┘
```

### Pickle Constraint

All `AsyncFunction.__init__` parameters must be fully picklable (Flink serialises
operators for distribution).  Heavy objects like `LLMClient` instances are created in
`open(runtime_context)`, not in `__init__`.

---

## 3. Key Design Decisions

### 3.1 Dual-Layer Retry Strategy

Two retry layers exist with **disjoint** error class ownership to avoid multiplicative retries:

| Layer | Owner | Error Classes Retried | Config |
|-------|-------|-----------------------|--------|
| Layer 1 | LLM Client | `429`, `503`, connection timeout, network errors | `LLMClientConfig.max_retries`, `retry_base_delay_s` |
| Layer 2 | Flink `AsyncRetryStrategy` | `flink_timeout`, `json_parse_error`, `schema_validation_error`, `retrieve_timeout` | `create_flink_retry_strategy(max_attempts, backoff_time_millis)` |

**Invariant**: `LLM_CLIENT_OWNED_ERRORS ∩ FLINK_RETRYABLE_ERRORS = ∅` (enforced by unit test).

`llm_call_error` (client already exhausted retries) is **not** retried at Layer 2.

### 3.2 Degraded Records (No Silent Drops)

When an operator encounters a timeout, parse error, or LLM failure, it returns a
**degraded record** instead of throwing or dropping:

```json
{"_input": "original_value", "_error": "json_parse_error: ...", "_degraded": true}
```

This ensures:

- No silent record drops under any failure mode.
- Downstream consumers can filter/audit degraded records.
- Flink-level retry can inspect the `_error` tag to decide whether to re-invoke.

### 3.3 sem_filter: 1:1 Mapping (Not In-Operator Filtering)

`SemFilterFunction` always emits exactly one record per input, containing
`{decision, confidence, reason, _input}`.  Actual filtering happens via a
downstream `DataStream.filter()`.  This keeps the async operator's output count
deterministic and makes filtered records available for quality auditing.

### 3.4 sem_join_retrieve: Hard Bounds

`SemJoinRetrieveFunction` enforces hard safety limits:

- `max_candidates_per_record` — truncates candidates if exceeded.
- `retrieve_timeout_ms` — strict timeout budget for retrieval step.
- `truncated` flag in output when candidates were cut.

### 3.5 Metrics via Flink MetricGroup

`OperatorMetrics.from_runtime_context()` registers counters and distributions with
Flink's native `MetricGroup`.  Falls back to local in-memory accumulators when the
metric group is unavailable (e.g. unit tests).  Tracked metrics:

- `call_count`, `call_latency_ms`, `call_rate`
- `error_count`, `timeout_count`, `invalid_output_count`, `retry_exhaustion_count`
- `input_tokens_total`, `output_tokens_total`

---

## 4. File Structure

```text
flink-python/pyflink/semantic_runtime/
├── __init__.py
├── llm_client.py                  # LLMClient base + Mock + OpenAI backends
├── metrics.py                     # OperatorMetrics (Flink MetricGroup wrapper)
├── retry.py                       # Dual-layer retry strategy + predicates
├── operators/
│   ├── __init__.py
│   ├── _common.py                 # validate_schema, make_degraded_json, attach_metrics
│   ├── sem_map.py                 # SemMapFunction(AsyncFunction)
│   ├── sem_filter.py              # SemFilterFunction(AsyncFunction)
│   ├── sem_topk.py                # SemTopKFunction(AsyncFunction)
│   └── sem_join_retrieve.py       # SemJoinRetrieveFunction(AsyncFunction)
└── tests/
    ├── conftest.py                # Symlink bootstrap for isolation env
    ├── test_async_baseline.py     # Minimal AsyncDataStream validation
    ├── test_sem_map.py            # sem_map normal + timeout paths
    ├── test_operators.py          # All 4 operators smoke tests
    ├── test_retry.py              # Retry predicate + strategy unit tests
    └── test_e2e_integration.py    # Full pipeline E2E (5 scenarios + SLO)
```

---

## 5. Test Results & SLO Verification

### 5.1 Operator Smoke Tests

| Operator | Input Records | Output | Status |
|----------|---------------|--------|--------|
| `sem_map` | 2 | 2/2 ✅ | Structured JSON output |
| `sem_filter` | 3 | 3/3 ✅ | decision/confidence/reason |
| `sem_topk` | 2 | 2/2 ✅ | top_k correctly truncated to k=2 |
| `sem_join_retrieve` | 2 | 2/2 ✅ | candidate_count=3, truncated=false |

### 5.2 Retry Unit Tests

9/9 predicate and strategy tests passed, including:
- Normal results → no retry
- `flink_timeout` / `json_parse_error` / `schema_validation_error` → retry (Layer 2)
- `llm_call_error` → no retry (Layer 1 already exhausted)
- Ownership disjointness assertion

### 5.3 E2E Integration Tests

| Test | Records | Degraded | p95 Latency | Status |
|------|---------|----------|-------------|--------|
| Normal pipeline | 3 | 0 (0%) | ~50ms | ✅ PASSED |
| Timeout pipeline | 2 | 2 (100%) | N/A | ✅ PASSED |
| Error pipeline | 3 | 3 (100%) | N/A | ✅ PASSED |
| Retry pipeline | 2 | 0 (0%) after retry | ~50ms | ✅ PASSED |
| Backpressure (500 records) | 500 | 0 (0%) | ~2009ms | ✅ PASSED |

### 5.4 Backpressure SLO (500 records, 2s mock latency, capacity=10)

```
total_records:          500
degraded_count:         0
degraded_rate:          0.00%
timeout_rate:           0.00%
invalid_output_rate:    0.00%
p95_e2e_latency_ms:     2008.5
avg_attempts:           1.00
max_attempts:           1
wall_time_s:            104.1
effective_throughput:    4.8 rec/s
```

**Conclusions**: Zero record loss, zero unexpected degraded records, stable ~4.8 rec/s
throughput (theoretical max = capacity/latency = 10/2 = 5 rec/s).

---

## 6. Git Evidence

| Commit | Message | Key Changes |
|--------|---------|-------------|
| `9f3a325fe4b` | `[build] Update .gitignore + isolation env script` | `.gitignore`, `mac_enter_isolated_env.sh` |
| `a91d84e1a5f` | `[doc] Add implementation plan` | `V0_1_IMPLEMENTATION_PLAN.md`, blueprint, sem_queries docs |
| `2753a9d3257` | `[feat] Add initial LLM client + operator wrappers` | `llm_client.py`, `cp/__init__.py`, `operators/__init__.py` |
| `ad6177a8c5d` | `[feat] Add semantic operators` | `sem_filter.py`, `sem_topk.py`, `sem_join_retrieve.py`, `_common.py` |
| `44a2618e56a` | `[feat] Integrate metrics + retry strategy` | `metrics.py`, `retry.py`, operator metrics integration |

---

## 7. What's Excluded (→ V0.2+)

| Feature | Reason | Target |
|---------|--------|--------|
| `sem_agg` | Requires keyed state + aggregation scope | V0.2 |
| Continuous `sem_topk` | Requires incremental state + timer-driven updates | V0.2 |
| Two-input `sem_join` | Requires two-input coordination + dual-side state | V0.3a |
| Batching / fusion | Requires batching layer + prompt fusion | V0.3b |
| Dynamic broadcast control | Requires broadcast state + control stream | V0.4 |
