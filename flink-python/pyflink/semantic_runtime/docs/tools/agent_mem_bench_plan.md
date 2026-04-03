# A Benchmarking Plan for a Stream-Based Agent Memory System

## Purpose

This document turns the earlier discussion into a concrete benchmark design for evaluating a **stream-based agent memory system**—especially one with a Flink-like processing model—using **LongMemEval** as the semantic workload source and an additional **systems harness** for replay, tracing, token accounting, and local/API model execution.

The core idea is simple:

- **Do not treat LongMemEval as a complete systems benchmark out of the box.**
- **Treat it as the semantic workload layer** (long-horizon chat histories, timestamps, evidence labels, questions, and answers).
- **Add a replay/scheduling/observability layer** to measure the properties that matter for a production memory system: concurrency, throughput, end-to-end latency, token cost, freshness, and prompt redundancy.

---

## 1. What We Actually Want to Measure

The benchmark should target an **agent memory system**, not only a question-answering pipeline. In this project, “agent memory” is best modeled as:

1. An **append-only, time-series stream of raw interaction data**.
2. A **memory workflow** that transforms raw interactions into more concise and accurate memory artifacts.
3. A **retrieval + reading path** that uses those artifacts to answer future questions.

Under this definition, the benchmark must evaluate five classes of properties:

### 1.1 Performance under multi-user, high-concurrency conditions

We want to measure:

- multi-user throughput
- write-path throughput and latency
- query throughput and latency
- scaling behavior as users, models, GPUs, and workers increase
- queueing and backpressure behavior

### 1.2 Token usage at every stage

This is essential because later optimizations may reduce token usage—especially during **memory insertion**. The benchmark should therefore record token consumption at each stage rather than only the final generation stage.

### 1.3 Profiling and observability

The system should be easy to profile. That means:

- stage-level latency breakdowns
- distributed traces
- counters/histograms for system health
- per-request metadata for debugging bottlenecks

### 1.4 Time semantics

LongMemEval already contains timestamps, and those timestamps should be used meaningfully rather than simply replaying sessions in order. For a stream-like memory system, **event time** and **processing time** should be separated.

### 1.5 Flexible execution backends

The benchmark should run both:

- against hosted APIs
- against local open-weight models
- across multiple models / multiple GPUs / possibly multiple nodes

### 1.6 Full final-prompt logging

Every final prompt sent to a model should be logged for later analysis of:

- repeated scaffolding
- redundant retrieved memory
- wasteful instructions
- recurring prompt patterns across workloads

---

## 2. Why LongMemEval Is a Good Base—But Not Enough by Itself

LongMemEval is a strong foundation because it was designed specifically for **long-term memory in chat assistants**. The paper frames the problem in terms of three execution stages—**indexing, retrieval, and reading**—which maps well to a stream-based memory system.[^1]

The public dataset and repository also expose a clean instance structure. Each evaluation sample includes fields such as:

- `question_id`
- `question_type`
- `question`
- `answer`
- `question_date`
- `haystack_session_ids`
- `haystack_dates`
- `haystack_sessions`
- `answer_session_ids`

The repository documents that `answer_session_ids` identify the evidence sessions used for session-level recall evaluation, and that the released benchmark files include `longmemeval_oracle.json`, `longmemeval_s_cleaned.json`, and `longmemeval_m_cleaned.json`.[^2]

This makes LongMemEval useful for:

- correctness evaluation
- retrieval evaluation
- timestamp-aware workloads
- testing knowledge updates and temporal reasoning

However, LongMemEval is **not** a full systems benchmark. It gives you the **semantic content of the workload**, but not the missing systems layer:

- no multi-user global scheduler
- no concurrency model
- no replay clock model
- no queue/backpressure instrumentation
- no token ledger by stage
- no standardized profiling pipeline

So the right design is:

> **Keep LongMemEval intact as the semantic benchmark layer, and wrap it in a systems benchmark harness.**

---

## 3. The Key Design Decision: Keep Two Layers Separate

The benchmark should have **two cleanly separated layers**.

## 3.1 Layer A: Semantic workload layer

This layer is essentially LongMemEval itself.

Its job is to define:

- the chat history
- the timestamps
- the query
- the gold answer
- the evidence labels
- the question type

This layer should remain as close as possible to the original benchmark so that results remain comparable to prior LongMemEval results.[^1][^2]

## 3.2 Layer B: Systems replay and observability layer

This is the layer you add.

Its job is to define:

- how many users exist simultaneously
- how user streams interleave
- when events arrive in replay time
- when questions are issued
- which model backend handles each stage
- what tokens are spent at each step
- how metrics and traces are collected

This separation is crucial. If you overload the original LongMemEval schema with runtime-specific fields, you lose comparability and make experimentation harder.

---

## 4. Proposed Time Model: Use Dual Time (or Triple Time)

Because the memory system is stream-based, timestamps should not be treated as decoration.

The benchmark should explicitly support **at least two clocks**, and ideally three:

### 4.1 Event time

This comes from LongMemEval itself:

- `haystack_dates`
- `question_date`

It represents the **semantic time** of the interaction.

This matters for:

- temporal reasoning
- knowledge update precedence
- time-aware retrieval
- retention logic
- window semantics
- staleness analysis

LongMemEval explicitly includes temporal reasoning and time-aware mechanisms in its design and codebase, which is a strong signal that timestamps should remain first-class in your system benchmark as well.[^1][^2]

### 4.2 Replay time

This is the time generated by the benchmark harness.

It controls when events are injected into the system during the experiment. It can be:

- order-only replay
- scaled replay
- burst replay
- stress replay

Examples:

- 1 month of semantic time compressed into 5 seconds of replay time
- 100 users all issuing queries within the same replay window
- append-heavy bursts followed by read-heavy bursts

### 4.3 Processing time

This is the wall-clock time at which the system actually processes the event.

It is what you use to measure:

- queueing delays
- backpressure
- end-to-end latency
- write visibility lag
- update propagation lag

## 4.4 Recommendation

For a Flink-like memory system, I recommend the following:

- **Event time**: used for semantic correctness and memory freshness semantics
- **Replay time**: used for benchmark control and concurrency generation
- **Processing time**: used for performance measurement

That gives you the ability to ask meaningful systems questions such as:

- How long after an append does the memory become visible to queries?
- Does event-time ordering remain correct under backlog or burst load?
- How often are queries answered with stale memory?

---

## 5. Replay Modes the Benchmark Should Support

A useful systems benchmark should not have only one workload mode. I recommend four:

### 5.1 Terminal query mode

This is the closest to the original LongMemEval setting.

- replay all history sessions for a user
- then issue the final question

Use this to preserve comparability with the original benchmark.

### 5.2 Mixed online mode

- append sessions and queries are interleaved
- some questions arrive before the entire history has been replayed

Use this to simulate real assistants that are queried continuously while memory is still being updated.

### 5.3 Update-sensitive mode

This mode should emphasize LongMemEval’s **knowledge-update** questions.[^1]

- when a memory update occurs
- immediately trigger a follow-up query

Use this to measure freshness and update propagation lag.

### 5.4 Burst/stress mode

- many users arrive concurrently
- append or query events are packed into a short replay window

Use this for scaling and throughput testing.

---

## 6. Metrics: What the Benchmark Must Measure

The benchmark should report at least five families of metrics.

## 6.1 System performance metrics

### Write path

- `append_qps`
- `append_latency_ms`
- `fact_extraction_latency_ms`
- `session_summary_latency_ms`
- `embedding_latency_ms`
- `index_write_latency_ms`
- `memory_visibility_lag_ms`

### Read path

- `retrieval_latency_ms`
- `rerank_latency_ms`
- `prompt_build_latency_ms`
- `generation_latency_ms`
- `e2e_query_latency_ms`

### Global

- `throughput_sessions_per_sec`
- `throughput_queries_per_sec`
- `p50/p95/p99_latency`
- `queue_depth`
- `backpressure_time_ms`
- `max_sustainable_qps_under_sla`
- `scaling_efficiency`

## 6.2 Token accounting metrics

This is non-negotiable if you want to study token-saving optimizations.

Recommended counters per request/event:

### Write path tokens

- `tokens_extract`
- `tokens_summarize`
- `tokens_rewrite`
- `tokens_embed`
- `tokens_index_augmentation`

### Read path tokens

- `tokens_query_expansion`
- `tokens_retrieval_context`
- `tokens_rerank`
- `tokens_reader_input`
- `tokens_reader_output`

### Evaluation tokens

- `tokens_judge`

This should usually be tracked separately from online serving cost, because it belongs to the evaluation harness rather than the online memory system.

### Derived metrics

- `tokens_per_inserted_session`
- `tokens_per_answered_query`
- `tokens_per_correct_answer`
- `token_reduction_ratio_vs_baseline`
- `write_tokens_vs_read_tokens_ratio`

## 6.3 Correctness guardrails

Even though the focus is performance, the benchmark still needs correctness guardrails so that speedups and token savings remain interpretable.

Recommended metrics:

- end-to-end answer correctness
- retrieval `Recall@k`
- retrieval `NDCG@k`
- per-question-type accuracy
- accuracy under load

LongMemEval’s paper and code explicitly support retrieval-oriented evaluation in addition to final answer evaluation.[^1][^2]

## 6.4 Freshness and stream-specific metrics

These are especially important for a streaming memory system.

- `memory_visible_after_ms`
- `update_visible_after_ms`
- `staleness_lag_ms`
- `queries_answered_with_stale_memory`
- `late_event_count`
- `watermark_delay_ms` (if your system uses watermark semantics)
- `compaction_lag_ms`

## 6.5 Prompt redundancy and waste metrics

Because you want to log every final generated prompt, you should also quantify prompt waste.

- `final_prompt_tokens`
- `retrieved_memory_tokens`
- `unique_memory_tokens`
- `template_overhead_tokens`
- `repeat_chunk_count`
- `redundancy_ratio`
- `normalized_prompt_hash_frequency`
- `prompt_prefix_similarity`

---

## 7. Recommended Observability Stack

A stream-based memory system should be observable by design.

I recommend the following stack:

- **OpenTelemetry** for instrumentation and spans
- **Prometheus** for metrics collection and time-series analysis
- **Jaeger** for distributed trace visualization

### 7.1 Why OpenTelemetry

The OpenTelemetry Python documentation explicitly defines instrumentation as the act of adding observability code yourself, using the SDK and API to emit telemetry from your application.[^3]

That makes it a strong fit for a custom stream-processing memory system where you want explicit spans around stages like:

- append ingestion
- fact extraction
- summarization
- embedding
- index write
- retrieval
- reranking
- prompt assembly
- generation

### 7.2 Why Prometheus

Prometheus uses a multi-dimensional data model where every time series is identified by a metric name and key-value labels.[^4][^5]

That is ideal for slicing benchmark metrics by:

- user
- tenant
- model
- GPU
- provider
- stage
- workload mode
- question type

### 7.3 Why Jaeger

Jaeger is an open-source distributed tracing system used to monitor workflows, identify bottlenecks, track root causes, and analyze service dependencies.[^6]

That makes it especially useful when requests flow through multiple components, such as:

- replay driver
- memory writer
- summarization model
- embedding service
- vector index
- reader model
- evaluation service

---

## 8. A Practical Span Model for Profiling

I recommend two root trace types.

## 8.1 Append trace

Each memory insertion event should generate a root span such as:

- `append_root`
  - `parse_session`
  - `extract_facts`
  - `summarize_session`
  - `embed_memory`
  - `write_index`
  - `optional_compaction`

## 8.2 Query trace

Each user question should generate a root span such as:

- `query_root`
  - `retrieve_memory`
  - `rerank_memory`
  - `assemble_prompt`
  - `llm_generate`
  - `postprocess`
  - `evaluate_answer`

## 8.3 Span attributes that should always exist

At minimum:

- `trace_id`
- `user_id`
- `tenant_id`
- `session_id`
- `question_id`
- `question_type`
- `model_name`
- `provider`
- `gpu_id`
- `token_in`
- `token_out`
- `event_time`
- `processing_time`
- `workload_mode`

This gives you the ability to diagnose not only whether the system is slow, but also:

- which question types are slow
- whether a specific model backend is the bottleneck
- whether GPU placement matters
- whether write-path or read-path dominates latency

---

## 9. Local Execution, Multiple Models, Multiple GPUs

The benchmark should not be tied to remote APIs.

## 9.1 Why vLLM is a strong default for local serving

vLLM provides an **OpenAI-compatible server**, which is ideal for unifying remote and local execution behind the same client interface.[^7]

It also exposes metrics via a `/metrics` endpoint. The vLLM metrics documentation describes both:

- **server-level metrics**
- **request-level metrics**

and earlier vLLM documentation explicitly notes that the OpenAI-compatible server exposes metrics through `/metrics`.[^8][^9]

This makes vLLM a strong fit for:

- local open-weight readers
- local summarization/extraction models
- token accounting and request telemetry
- Prometheus-based monitoring

## 9.2 Why Ray Serve is a strong outer orchestration layer

Ray Serve supports multi-model serving, autoscaling, and model multiplexing.[^10][^11]

This is helpful when:

- different stages use different models
- multiple GPU pools exist
- sparse traffic should share replicas efficiently
- the benchmark needs to scale from single-node to multi-node serving

## 9.3 Recommendation

Do **not** write a scheduler from scratch at the beginning.

Instead:

- define a unified provider interface for generation and embedding
- use hosted APIs when needed
- use **vLLM** for local single-node serving
- use **Ray Serve + vLLM** for multi-model, multi-GPU, or multi-node experiments

That gives you a much cleaner experimental stack:

- same workload format
- same token ledger
- same tracing pipeline
- interchangeable execution backends

---

## 10. Prompt Logging Should Be a First-Class Output

You explicitly want to “log every final generated prompt” to observe patterns and redundancy. This is the right instinct.

The benchmark should log the **exact final prompt** sent to the model, not just the prompt template name.

Each prompt log entry should contain:

- timestamp
- trace id
- user id
- session id / question id
- model and provider
- retrieval candidates and scores
- final rendered prompt text
- token counts
- prompt hash
- normalized prompt hash

### Suggested prompt log schema

```json
{
  "ts": "2026-03-25T10:00:00Z",
  "trace_id": "...",
  "user_id": "u_0042",
  "question_id": "q_00123",
  "model": "reader-model-x",
  "provider": "vllm",
  "template_id": "reader_v3",
  "retrieved_ids": ["s17", "s03", "fact_22"],
  "retrieval_scores": [0.91, 0.88, 0.74],
  "final_prompt_text": "...",
  "final_prompt_tokens": 4123,
  "memory_tokens": 3010,
  "instruction_tokens": 480,
  "question_tokens": 92,
  "prompt_hash": "...",
  "normalized_prompt_hash": "..."
}
```

### Why this matters

It enables several useful analyses:

- whether the same memory chunks recur across many requests
- whether templates are consistently overlong
- whether retrieval repeatedly brings back overlapping evidence
- whether token-saving optimizations reduce real prompt redundancy or merely shift it elsewhere

---

## 11. The Benchmark Harness Should Add a Workload Trace Layer

A clean design is to keep the original LongMemEval sample untouched and add a wrapper object.

### Suggested structure

```json
{
  "instance_id": "longmemeval_m_00123",
  "user_id": "u_0042",
  "benchmark_source": "longmemeval_m_cleaned",
  "semantic_instance": {
    "question_id": "...",
    "question_type": "...",
    "question": "...",
    "answer": "...",
    "question_date": "...",
    "haystack_session_ids": ["s1", "s2"],
    "haystack_dates": ["...", "..."],
    "haystack_sessions": [[...], [...]],
    "answer_session_ids": ["s2"]
  },
  "event_trace": [
    {
      "event_id": "e1",
      "event_type": "append_session",
      "session_id": "s1",
      "event_time": "2025-01-01T10:00:00Z",
      "replay_time_ms": 0
    },
    {
      "event_id": "e2",
      "event_type": "append_session",
      "session_id": "s2",
      "event_time": "2025-01-05T12:00:00Z",
      "replay_time_ms": 20
    },
    {
      "event_id": "eq",
      "event_type": "query",
      "question_id": "...",
      "event_time": "2025-02-01T09:00:00Z",
      "replay_time_ms": 1000
    }
  ],
  "workload_meta": {
    "mode": "terminal",
    "tenant_id": "tenant_a",
    "arrival_policy": "poisson",
    "priority": "normal"
  }
}
```

This structure preserves compatibility while giving you the systems metadata you actually need.

---

## 12. Suggested Output Artifacts from Each Run

Each benchmark run should emit at least four artifacts.

## 12.1 Benchmark definition

- workload config
- replay config
- model config
- system config

## 12.2 Per-event runtime log

This should be an append-only JSONL file containing:

- event ids
- timestamps
- stage latencies
- token counts
- resource identifiers
- trace ids
- correctness outputs

## 12.3 Metrics backend outputs

- Prometheus metrics scrape data
- Grafana dashboards if desired

## 12.4 Prompt logs

- every final prompt
- prompt hashes
- normalized hashes

Optional fifth artifact:

## 12.5 Trace export

- Jaeger traces or OTLP-exported spans for deep profiling

---

## 13. A Minimal MVP Plan

A sensible implementation strategy is to build this in three phases.

## Phase 1: Single-node benchmark MVP

Goal: validate the end-to-end benchmark architecture.

Recommended setup:

- `LongMemEval-S`
- replay wrapper with multiple users
- one local vLLM server or one hosted API backend
- token accounting
- OpenTelemetry spans
- Prometheus metrics
- full prompt logging

Questions to answer in this phase:

- Is the benchmark harness stable?
- Are stage-level metrics recorded correctly?
- Are prompt logs informative?
- Is the token ledger complete enough for later optimization studies?

## Phase 2: Streaming semantics and freshness

Goal: make the benchmark truly suitable for a Flink-like memory system.

Add:

- dual/triple time semantics
- update-sensitive query mode
- watermark/late-event handling if relevant
- freshness and staleness metrics
- optional background compaction events

Questions to answer in this phase:

- How long does it take for memory updates to become visible?
- Does the system answer with stale memory under burst load?
- How does event-time logic behave under processing backlog?

## Phase 3: Multi-model, multi-GPU, scaling experiments

Goal: stress the serving and scheduling layer.

Add:

- Ray Serve orchestration
- multiple model types for different stages
- multiple GPUs / nodes
- autoscaling and model multiplexing where appropriate

Questions to answer in this phase:

- How does throughput scale with more replicas or GPUs?
- Is the bottleneck in memory insertion, retrieval, or final generation?
- Which models dominate latency and token cost?

---

## 14. Bottom-Line Recommendations

If the goal is to benchmark a stream-based agent memory system, my recommendations are:

1. **Use LongMemEval as the semantic workload source, not as the entire systems benchmark.**[^1][^2]
2. **Add a systems replay layer with multiple users, event traces, and configurable arrival models.**
3. **Treat time seriously** by separating event time, replay time, and processing time.
4. **Record token usage at every stage** so that future token-saving optimizations remain interpretable.
5. **Make observability first-class** with OpenTelemetry, Prometheus, and Jaeger.[^3][^4][^6]
6. **Log every final generated prompt**, including hashes and retrieval payload metadata.
7. **Prefer vLLM for local serving** and **Ray Serve for higher-level orchestration**, rather than writing a scheduler from scratch.[^7][^8][^10][^11]

In one sentence:

> Build a **systems benchmark harness on top of LongMemEval** that combines semantic correctness, streaming replay, stage-level token accounting, distributed tracing, and pluggable local/API model execution.

---

## References

[^1]: Di Wu et al., *LongMemEval: Benchmarking Chat Assistants on Long-Term Interactive Memory*, arXiv:2410.10813. https://arxiv.org/abs/2410.10813
[^2]: LongMemEval GitHub repository and dataset schema documentation. https://github.com/xiaowu0162/LongMemEval
[^3]: OpenTelemetry Python instrumentation documentation. https://opentelemetry.io/docs/languages/python/instrumentation/
[^4]: Prometheus data model documentation. https://prometheus.io/docs/concepts/data_model/
[^5]: Prometheus overview. https://prometheus.io/docs/introduction/overview/
[^6]: Jaeger documentation. https://www.jaegertracing.io/docs/latest/
[^7]: vLLM OpenAI-compatible server documentation. https://docs.vllm.ai/en/v0.8.3/serving/openai_compatible_server.html
[^8]: vLLM metrics documentation. https://docs.vllm.ai/en/stable/design/metrics/
[^9]: vLLM production metrics documentation. https://docs.vllm.ai/en/v0.6.3/serving/metrics.html
[^10]: Ray Serve overview and documentation. https://docs.ray.io/en/latest/serve/index.html
[^11]: Ray Serve model multiplexing documentation. https://docs.ray.io/en/latest/serve/model-multiplexing.html
