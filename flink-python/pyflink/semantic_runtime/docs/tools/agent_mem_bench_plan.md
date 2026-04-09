# Agent Memory Benchmark Plan (Unified)

## Purpose

This document is the single source of truth for benchmarking agent-memory systems in this repository.

Core principle:

- Primary objective: **performance and cost**
- Secondary objective: **quality as a guardrail**

The benchmark is defined by a **data protocol** and a **metric protocol**. It does not copy any single benchmark's pipeline logic as-is.

---

## Locked Decisions

1. `v1` is mandatory and must complete before cross-domain expansion.
2. `v1` headline conclusions come only from conversational apples-to-apples comparison.
3. `Track C` (cross-domain streaming workloads) is protocol-frozen now and implemented after `v1`.
4. `MemoryAgentBench` is out-of-scope for now.
5. `LoCoMo main-track` is optional (`v1.5`) and not a blocker for `v2`.

---

## Benchmark Governance Contract (v1 Mandatory)

All benchmark runs must satisfy this governance contract. A run that violates any mandatory item is invalid and must not be used for conclusion claims.

### 1) Immutable Run Manifest

Each run must emit a manifest with:

- `run_id`
- `schema_version`
- `code_commit`
- `dataset_id`
- `dataset_hash`
- `config_hash`
- `seed`
- `started_at` / `ended_at`

The manifest is immutable after run start except for status and timestamps.

### 2) Schema Versioning

All artifacts must carry explicit schema versions:

- runtime event log schema
- metrics export schema
- prompt log schema
- report summary schema

Any schema change must bump the corresponding version field.

### 3) Baseline Matrix Lock

Performance/cost comparison is valid only when compared cells are matched on:

- track
- dataset and split
- replay policy
- workflow mode
- model/provider and major generation config
- evaluation setup

If any key differs, report as a separate experiment, not as direct improvement.

### 4) Statistical Protocol

Default protocol for each comparison cell:

- `repeats_per_cell >= 5`
- fixed seed list declared in manifest/config
- report `mean`, `median`, `p95`, and `95% CI`
- paired significance test for baseline vs candidate with declared `alpha`

Do not report single-run deltas as final conclusions.

### 5) Cost Normalization Protocol

Costs must be reported using fixed normalized denominators:

- `cost_per_1k_ingest_events`
- `cost_per_1k_queries`
- `cost_per_correct_answer`

Online serving cost and offline evaluation/judging cost must be separated.

### 6) Failure Accounting Semantics

Timeouts, retries, and failures are first-class outcomes:

- timeout/error events count toward denominator for success-rate and SLA metrics
- retry calls count toward total latency and total token/cost accounting
- missing critical accounting fields invalidates the run

No silent fallback or hidden downgrade policy is allowed in benchmark accounting.

### 7) Data Governance and Label Provenance

Each labeled artifact must include provenance:

- `label_source` (`human`, `weak`, `synthetic`, `derived`)
- `label_pipeline_version` (if generated)
- `annotator_or_judge_id` (when applicable)
- dataset version/hash in run manifest

All reports must disclose `task drift`, `label drift`, and `workload drift`.

### 8) Within-Track Reporting Only

Primary headline results must be reported within each track. Cross-track mixed single-score ranking is disallowed.

---

## Benchmark Contract

The benchmark contract below is valid only when paired with the mandatory governance contract above.

### Data Contract

Each run must define all three inputs:

1. `ingest_stream`: append/update events fed into memory workflows.
2. `eval_query_set`: query set with gold labels/evidence when available.
3. `replay_policy`: user/tenant concurrency, arrival model, and time scaling.

### Metric Contract

Two metric profiles are mandatory:

1. `perf_cost_core`
   - write/read throughput
   - p50/p95/p99 latency
   - token usage by stage
   - monetary cost
   - scaling behavior under concurrency
2. `quality_guardrail`
   - answer accuracy (or task success)
   - retrieval quality when applicable (`Recall@k`, `NDCG@k`)
   - degradation under load

Hard rule: no speedup/cost claim is valid if quality drops below the configured guardrail threshold.

### Time Contract (Triple Clock)

1. `event_time`: semantic time from dataset records.
2. `replay_time`: benchmark harness injection time.
3. `processing_time`: wall-clock execution time in system.

All freshness and staleness claims must explicitly specify which clock pair is used.

### Replay Modes

1. `terminal_query`: replay history, then issue final query.
2. `mixed_online`: append and query interleaved.
3. `update_sensitive`: trigger follow-up query after update events.
4. `burst_stress`: concentrated arrivals for overload/scaling behavior.

---

## Track Structure

### Track A (Main, Apples-to-Apples)

Purpose: primary comparison track for this project.

- Domain: conversational long-term memory
- Data requirement: both `ingest_stream` and `eval_query_set`
- Output: perf/cost + quality guardrail

`v1` is Track A only.

### Track B (Perf Stress)

Purpose: stress write/update/index paths where labels are weak or absent.

- Data requirement: strong `ingest_stream`; labels optional
- Output: perf/cost/freshness stress behavior, no primary accuracy claim

### Track C (Cross-Domain Streaming Agent Memory)

Purpose: evaluate agent-memory behavior beyond chatbot-only workloads.

- Domains: monitoring, financial analysis, event streams
- Data requirement: stream-native workloads; labels may be task-specific or augmented
- Output: perf/cost primary, task quality as domain-specific guardrail

Status: protocol frozen now; implementation starts after `v1`.

---

## Dataset Capability Matrix

| Dataset / Source | Has Ingest Stream | Has Gold QA / Evidence | Has Event Time | Recommended Track | Notes |
|---|---:|---:|---:|---|---|
| LongMemEval (cleaned) | Yes | Yes | Yes (`haystack_dates`, `question_date`) | Track A | Main v1 dataset. |
| Local `locomo.json` snapshot in this repo | Yes | No (dialog-only snapshot) | Weak/derived | Track B | Good for perf-only ingest stress unless full QA annotations are added. |
| LoCoMo (official full release) | Yes | Yes (`qa`, `evidence`) | Yes (session timestamps) | Track A or B | Optional `v1.5`; not required before v2. |
| CP-inspired FNSPID pipeline data | Yes | Task labels available (pipeline-defined) | Yes | Track C | Financial/news monitoring style workload. |
| CP-inspired MiDe22 pipeline data | Yes | Task labels available (event monitoring metrics) | Yes | Track C | Misinformation/event monitoring style workload. |
| StreamBench | Mixed | Yes (task dependent) | Task dependent | Track C (later) | Good for continual-improvement tasks; schema is heterogeneous. |
| MemoryArena | Session-like tasks | Yes | Task dependent | Track C (later) | Agentic multi-session tasks, not direct chat-memory insertion format. |
| LogHub | Yes (strong) | Usually no memory-QA labels | Yes | Track B/C (later) | Strong systems stress source; label augmentation required for quality claims. |
| GDELT | Yes (strong) | No direct memory-QA labels | Yes (time-native) | Track B/C (later) | High-volume event stream; requires task/label design for quality claims. |

---

## v1 Scope (Must Complete First)

`v1` delivers Track A only with LongMemEval.

Required outputs:

1. apples-to-apples perf/cost comparison under fixed configs
2. quality guardrail report in the same run
3. stage-level token/cost accounting
4. reproducible replay profile and run artifacts
5. immutable run manifest + schema versions for all artifacts
6. statistical report (repeats, CI, significance) for each comparison cell
7. normalized cost report (`per_1k_ingest`, `per_1k_queries`, `per_correct`)

Out-of-scope for `v1`:

1. cross-domain streaming conclusions
2. mixed single-score ranking across tracks
3. Track C implementation

---

## Track C Protocol (Frozen for Post-v1 Implementation)

Track C must reuse the same benchmark contract as Track A/B:

1. `ingest_stream`
2. `eval_query_set`
3. `replay_policy`
4. `perf_cost_core`
5. `quality_guardrail`

Track C runs must also satisfy the same governance contract (`manifest`, schema versioning, statistical protocol, normalization, and failure accounting) without track-specific relaxations.

Initial post-v1 target workloads:

1. FNSPID-aligned financial/news monitoring
2. MiDe22-aligned event monitoring

Optional later expansions:

1. StreamBench subsets
2. MemoryArena subsets
3. LogHub and GDELT stress tracks (with explicit label augmentation policy)

---

## Metric Taxonomy (Detailed)

### 1) System Performance

Write path:

- `append_qps`
- `append_latency_ms`
- `fact_extraction_latency_ms`
- `session_summary_latency_ms`
- `embedding_latency_ms`
- `index_write_latency_ms`
- `memory_visibility_lag_ms`

Read path:

- `retrieval_latency_ms`
- `rerank_latency_ms`
- `prompt_build_latency_ms`
- `generation_latency_ms`
- `e2e_query_latency_ms`

Global:

- `throughput_sessions_per_sec`
- `throughput_queries_per_sec`
- `p50/p95/p99_latency`
- `queue_depth`
- `backpressure_time_ms`
- `max_sustainable_qps_under_sla`
- `scaling_efficiency`

### 2) Token and Cost Accounting

Write path tokens:

- `tokens_extract`
- `tokens_summarize`
- `tokens_rewrite`
- `tokens_embed`
- `tokens_index_augmentation`

Read path tokens:

- `tokens_query_expansion`
- `tokens_retrieval_context`
- `tokens_rerank`
- `tokens_reader_input`
- `tokens_reader_output`

Evaluation tokens:

- `tokens_judge`

Derived:

- `tokens_per_inserted_session`
- `tokens_per_answered_query`
- `tokens_per_correct_answer`
- `token_reduction_ratio_vs_baseline`
- `write_tokens_vs_read_tokens_ratio`

### 3) Freshness and Stream Metrics

- `memory_visible_after_ms`
- `update_visible_after_ms`
- `staleness_lag_ms`
- `queries_answered_with_stale_memory`
- `late_event_count`
- `watermark_delay_ms` (if applicable)
- `compaction_lag_ms`

### 4) Prompt Redundancy Metrics

- `final_prompt_tokens`
- `retrieved_memory_tokens`
- `unique_memory_tokens`
- `template_overhead_tokens`
- `repeat_chunk_count`
- `redundancy_ratio`
- `normalized_prompt_hash_frequency`
- `prompt_prefix_similarity`

---

## Logging and Artifact Schemas

### Prompt Log (Mandatory)

Each final prompt record should include at least:

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

### Run Artifacts (Mandatory)

1. immutable run manifest (`run_id`, hashes, commit, seed, schema versions)
2. benchmark definition (`workload/replay/model/system config`)
3. per-event runtime log (`jsonl`)
4. metrics export (`timeseries`)
5. prompt log (`full prompt + hashes`)
6. report summary (aggregates + CI + significance results)
7. optional trace export (`otlp/jaeger`)

### Minimal Benchmark Config Schema

```yaml
benchmark:
  schema_version: 1.0.0
  run_manifest:
    run_id: run_20260409_0001
    schema_version: 1.0.0
    code_commit: abcdef123456
    dataset_id: longmemeval_s_cleaned
    dataset_hash: sha256:...
    config_hash: sha256:...
    seed: 42
    started_at: "2026-04-09T10:00:00Z"
    ended_at: "2026-04-09T10:31:00Z"
  track_id: track_a
  dataset_id: longmemeval_s_cleaned
  replay_policy:
    users: 64
    tenants: 8
    arrival: poisson
    time_scale: 20x
  metric_profile:
    perf_cost_core: true
    quality_guardrail: true
  stats:
    repeats_per_cell: 5
    ci_method: bootstrap
    ci_level: 0.95
    significance_test: paired_permutation
    alpha: 0.05
  cost_policy:
    currency: USD
    price_book_version: 2026-04
    normalized_units:
      ingest_events: 1000
      queries: 1000
  failure_policy:
    count_timeout_as_failure: true
    include_retry_cost: true
    missing_critical_fields: invalidate_run
  label_policy:
    require_provenance: true
    allowed_sources: [human, weak, synthetic, derived]
  report_scope: within_track
```

---

## Roadmap

### v1 (Now)

Deliver Track A only:

- LongMemEval apples-to-apples runs
- perf/cost core + quality guardrail
- stable artifact pipeline with governance-complete manifests and statistical reports

### v2 (After v1)

Implement Track C using protocol frozen in this doc:

1. FNSPID-aligned financial/news monitoring workload
2. MiDe22-aligned event monitoring workload

Optional later expansion:

1. StreamBench subsets
2. MemoryArena subsets
3. LogHub / GDELT stress tracks (with label augmentation where needed)

---

## Reporting, Statistical, and Validity Rules

### Reporting Scope (Hard Rules)

1. Report results **within each track**. Do not combine Track A/B/C into one headline score.
2. Do not report direct baseline-vs-candidate improvement unless baseline matrix keys are fully matched.
3. Separate online serving cost from offline judging/evaluation cost in every report.

### Statistical Rules

1. Use `repeats_per_cell >= 5` for final comparison claims.
2. Report per-cell `mean`, `median`, `p95`, and `95% CI`.
3. Use paired significance testing with declared `alpha` for baseline-vs-candidate cells.
4. Single-run outputs may be logged, but cannot be used as final conclusion evidence.

### Cost Normalization Rules

1. Always report:
   - `cost_per_1k_ingest_events`
   - `cost_per_1k_queries`
   - `cost_per_correct_answer`
2. Include retry token/cost in total cost accounting.
3. Include price-book version and currency in run artifacts.

### Failure Accounting Rules

1. Timeouts and hard errors count as failed outcomes in denominator-based metrics.
2. Retry attempts count toward latency and cost totals.
3. Missing critical accounting fields (manifest IDs, schema version, cost fields, or failure counters) invalidates the run.

### Required Metric Dimensions and Drift Disclosure

1. Record metrics by:
   - `track_id`
   - `dataset_id`
   - `workflow`
   - `stage`
   - `model/provider`
   - `run_id`
2. Explicitly disclose:
   - `task drift`
   - `label drift`
   - `workload drift`

---

## Appendix A: Suggested Observability Stack (Non-Binding)

This section is guidance, not a mandatory requirement.

1. OpenTelemetry for stage spans and trace propagation.
2. Prometheus for metric collection.
3. Jaeger or OTLP backend for distributed trace analysis.

Suggested root spans:

1. `append_root`
   - `parse_session`
   - `extract_facts`
   - `summarize_session`
   - `embed_memory`
   - `write_index`
2. `query_root`
   - `retrieve_memory`
   - `rerank_memory`
   - `assemble_prompt`
   - `llm_generate`
   - `evaluate_answer`

Suggested standard span attributes:

- `trace_id`
- `user_id`
- `tenant_id`
- `session_id`
- `question_id`
- `question_type`
- `model_name`
- `provider`
- `token_in`
- `token_out`
- `event_time`
- `processing_time`
- `workload_mode`

---

## Appendix B: Execution Backend Notes (Non-Binding)

The benchmark protocol is backend-agnostic. Typical practical setup options:

1. hosted APIs for quick baseline runs
2. local vLLM for open-weight local serving
3. ray-serve-like orchestration for multi-model/multi-gpu experiments

These are implementation options, not benchmark-defining requirements.

---

## References

1. LongMemEval: https://github.com/xiaowu0162/LongMemEval
2. LoCoMo: https://github.com/snap-research/locomo
3. Continuous Prompts: https://arxiv.org/abs/2512.03389
4. StreamBench: https://arxiv.org/abs/2406.08747
5. MemoryArena: https://arxiv.org/abs/2602.16313
6. LogHub: https://github.com/logpai/loghub
7. GDELT: https://www.gdeltproject.org/data.html
