# Windowing vs. Streaming for Agent Memory

## Purpose

This note separates three ideas that are often conflated:

- `windowing` is a **logical processing abstraction**
- `streaming` is a **systems/workload abstraction**
- `agent memory` is an **application/architectural concept**

These are related, but not equivalent. A workflow can use windows without being a high-velocity streaming workload, and a system can be implemented on top of a stream processor without implying that the target deployment is natively "high-speed append".

## Core Claim

For the agent memory systems in this repository, the safest claim is:

> Agent memory pipelines often exhibit **windowed, stateful, and incremental** behavior over append-only conversational histories.

This is **not** the same as claiming:

> Agent memory is inherently a **high-velocity streaming** problem.

That stronger claim is usually not supported by current conversational-memory benchmarks such as LoCoMo or LongMemEval.

## Why `window != streaming`

The term `window` is overloaded:

- In relational systems, window functions (`OVER`, `PARTITION BY`, frame clauses) operate over bounded row groups inside a finite relation. Mainstream SQL engines support this directly; for example, Microsoft documents SQL Server window functions through the `OVER` clause and related training material.  
  Sources: [Microsoft Learn: `OVER` clause (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql?view=sql-server-2016), [Microsoft Learn: write queries that use window functions](https://learn.microsoft.com/en-us/training/modules/write-queries-that-use-window-functions/)
- In streaming systems, windows are a way to carve an unbounded input stream into bounded working sets, usually by time, count, or session semantics.  
  Source: [Microsoft Learn: Azure Stream Analytics `OVER`](https://learn.microsoft.com/en-us/stream-analytics-query/over-azure-stream-analytics)

These are not identical execution models, but they share the same idea: define a bounded subset of growing data for local computation. Therefore, saying that an agent memory workflow contains window-like logic does **not** imply that the workload is a high-throughput stream or that a stream processor is the only appropriate implementation substrate.

## These Abstractions Are Not Unique to Streaming

Stateful operators, incremental maintenance, and bounded-window processing are not exclusive to stream processors. They belong to a broader family of **incremental data management** techniques.

- In classic database work, incremental view maintenance (IVM) already studies how to update derived state from base-table deltas instead of recomputing from scratch.  
  Source: [Gupta and Mumick, 1995](https://sigmod.org/publications/dblp/db/journals/debu/GuptaM95.html)
- In streaming systems, the same core idea is specialized for **continuous queries over unbounded inputs**, where the system must react to ongoing arrivals, maintain state online, and often deal with window expiry, event-time semantics, late data, and low-latency triggers.  
  Source: [ViewDF: declarative IVM for streaming data](https://www.sciencedirect.com/science/article/abs/pii/S0306437917303897)

So the most accurate statement is:

> agent memory often looks like a **stateful incremental maintenance** problem; streaming is one possible execution substrate, not the definition of the problem itself.

This also means that "streaming is more natural" is not a universal truth. It may be more natural for continuously running, event-driven agents, but a database-centric design can be equally or more appropriate for low-rate, request-driven memory workloads.

## What the Three Memory Systems Actually Look Like

### EverMemOS

EverMemOS has the clearest explicit windowing behavior in this repository. Its insertion workflow accumulates incoming messages in a tumbling buffer and seals a segment when a semantic or heuristic boundary is reached. That is a genuine windowed/stateful pattern.

Relevant local references:

- [data_flow_evermemos.md](./data_flow_evermemos.md)
- [sem_queries.md](../sem_queries.md)

This tumbling/semantic window does **not** imply high append rate. It only shows that the system needs bounded segmentation over incrementally arriving conversation history.

### Zep / Graphiti

Zep is less window-native than EverMemOS, but it still uses bounded recent context and incremental updates. Insertion is processed per request against a recent slice and the historical graph state. This is window-like in the sense of bounded working context, but not as explicit as a stream-processing window operator.

Relevant local reference:

- [data_flow_zep.md](./data_flow_zep.md)

### Mem0

Mem0 is the least window-centered of the three. Its insertion workflow is dominated by per-message extraction, retrieval of relevant prior memories, and resolution/update against existing state. The core abstraction is incremental retrieval and update, not explicit window segmentation.

Relevant local reference:

- [data_flow_mem0.md](./data_flow_mem0.md)

## What LoCoMo and LongMemEval Actually Measure

### LoCoMo

LoCoMo is a benchmark for **very long-term conversational memory**. The ACL paper describes the dataset as conversations averaging roughly 600 turns and 16K tokens, spanning up to 32 sessions, and presents question answering, event summarization, and multi-modal dialogue generation tasks over those long conversations.  
Source: [ACL Anthology: LoCoMo](https://aclanthology.org/2024.acl-long.747/)

LoCoMo is clearly relevant to agent memory. But it is a **memory-quality benchmark**, not a throughput benchmark and not a benchmark designed to justify a high-velocity streaming workload model.

In other words:

- Yes, it tests long-horizon memory needs.
- No, it does not show that the underlying workload is "high-speed append".

It is also important to separate **benchmark relevance** from **benchmark realism**. LoCoMo is highly relevant for evaluating whether a memory system can survive long, multi-session, temporally entangled conversations. But it is still a constructed benchmark environment, not a direct measurement of production chatbot traffic or ingest rates. It supports claims about **memory difficulty**, not **deployment velocity**.

### LongMemEval

LongMemEval is also explicitly a long-term memory benchmark for chat assistants. The official repository describes it as testing information extraction, multi-session reasoning, knowledge updates, temporal reasoning, and abstention, and states that systems must parse dynamic interactions online for memorization and answer after all sessions. It also includes variants with very long histories, such as roughly 115K tokens or around 500 sessions.  
Source: [LongMemEval GitHub](https://github.com/xiaowu0162/LongMemEval)

Again, LongMemEval is relevant to memory systems, but not to the claim that the workload is high-throughput streaming. It stresses **history length, multi-session accumulation, and online memory maintenance**, not ingest rate.

The same caution applies here: LongMemEval is a benchmark for the *capabilities* required by long-term chat memory, not a benchmark for the *traffic profile* of deployed assistants.

## Existing Benchmark Gap

Current memory benchmarks mostly test:

- long-horizon conversational accumulation
- multi-session recall and reasoning
- dynamic updates to facts or user profiles

They do **not** usually test:

- high arrival-rate ingestion
- event-time or out-of-order processing
- standing continuous queries over unbounded streams
- stream throughput/latency under sustained load

This is why benchmarks such as LoCoMo, LongMemEval, and PersonaMem are best viewed as **memory benchmarks**, not **streaming benchmarks**.

At the same time, some broader agent benchmarks are moving closer to stream-like or continual settings. For example, **StreamBench** explicitly evaluates LLM agents under streaming scenarios and continuous improvement.  
Source: [StreamBench](https://stream-bench.github.io/)

But these broader benchmarks usually target **online adaptation** or **continual improvement**, not long-term memory alone. So the field still lacks a widely adopted benchmark that directly combines:

- agent memory maintenance
- persistent state updates
- genuinely stream-based workload assumptions

## What Counts as Agent Memory

There is no single official standard definition of agent memory. The literature instead converges on a family resemblance:

- memory supports **long-term agent-environment interaction**
- memory stores and updates information beyond the immediate context window
- memory is later retrieved to improve planning, action, personalization, or reasoning

This broader view is consistent across several influential papers:

- the survey *A Survey on the Memory Mechanism of Large Language Model based Agents* describes memory as a key component supporting long-term and complex agent-environment interaction  
  Source: [arXiv 2404.13501](https://arxiv.org/abs/2404.13501)
- CoALA frames language agents as having modular memory components and structured actions over internal memory and external environments  
  Source: [CoALA](https://arxiv.org/abs/2309.02427)
- Generative Agents builds agents that store experiences, synthesize reflections, and retrieve memories to guide future behavior  
  Source: [Generative Agents](https://arxiv.org/abs/2304.03442)
- MemGPT/Letta treats persistent memory as a general mechanism for extended conversations **and** document analysis, not just chat history recall  
  Source: [MemGPT](https://research.memgpt.ai/)

Under this broader definition, chatbot memory is only one subcase of agent memory. Other examples include simulation agents, tool-using assistants, monitoring agents, robotics agents, security agents, and financial-analysis agents.

## What Continuous Prompts Actually Targets

The Continuous Prompts paper is framed very differently. Its abstract explicitly targets **monitoring unstructured streams**, persistent semantic computation over evolving streams, and implementation inside a stream processing system (VectraFlow).  
Source: [arXiv: Continuous Prompts](https://arxiv.org/abs/2512.03389)

The Continuous Prompts paper is best understood as a paper about:

- streaming-native semantic operators
- long-running stream analytics
- adaptive planning under evolving stream workloads

That makes it conceptually useful for agent memory, especially for semantic windows, stateful grouping, and incremental operators. But its motivating workload class is broader stream analytics, not specifically conversational agent memory. A secondary review of the paper describes evaluated pipelines such as stock/news monitoring and error-event monitoring, which is consistent with that framing.  
Secondary source: [Moonlight review](https://www.themoonlight.io/en/review/continuous-prompts-llm-augmented-pipeline-processing-over-unstructured-streams)

The right way to state the relationship is:

- Continuous Prompts is **not identical** to agent memory as a research area.
- But some Continuous Prompt workloads can be interpreted as **agent memory maintenance** if they are embedded inside a persistent autonomous loop: perceive -> update internal state -> act.

So the friend's intuition is partly right: CP can overlap with agent memory in autonomous monitoring-style agents. The part that needs caution is turning that overlap into an identity claim.

## What Can Be Claimed Safely

The following claims are defensible:

- Agent memory workflows often operate over **append-only conversational histories**.
- They often require **bounded working sets**, **stateful updates**, **incremental maintenance**, and sometimes **windowed segmentation**.
- Stream processing concepts such as keyed state, triggers, windows, and continuous operators can provide a useful **prototype abstraction** or **execution model** for these workflows.
- Benchmarks such as LoCoMo and LongMemEval are good evidence for **memory-oriented difficulty** and **long-horizon interaction structure**, but not for **high-throughput traffic assumptions**.
- Agent memory is broader than chatbot memory; conversational benchmarks illuminate one important subcase, not the full design space.

## What Should Not Be Claimed Without Extra Evidence

The following stronger claims need separate empirical support and generally should not be inferred from LoCoMo/LongMemEval alone:

- Agent memory benchmarks represent **high-velocity** streaming workloads.
- Conversational memory applications are best characterized as **streaming-first systems** rather than database-centric incremental systems.
- A stream processor is the best implementation substrate for typical user-chatbot memory workloads.
- Every continuous semantic analytics pipeline should automatically be labeled "agent memory".

## Recommended Framing for a Paper or Prototype

If the implementation is built on Flink or another stream processor, the strongest careful framing is:

> We use stream-processing abstractions as a prototype substrate for stateful, incremental memory maintenance over append-only interaction logs.

If the scope is broader than chatbots, an even safer version is:

> We study agent memory as a stateful incremental maintenance problem. Stream processing provides one attractive substrate, especially for continuously running or high-arrival-rate agents, while database-centric designs remain competitive for low-rate conversational workloads.

This framing preserves the important insight:

- windows, state, and continuous updates are useful abstractions for memory pipelines

without overclaiming:

- that current conversational-memory benchmarks are high-throughput streams
- or that agent memory is inherently a high-speed streaming application

## Bottom Line

LoCoMo and LongMemEval are relevant because they test **memory ability under long-horizon conversational accumulation**. They are **not** evidence that agent memory is a high-velocity streaming workload.

So the right position is not "agent memory is streaming" in the strong workload sense. The more defensible position is:

> agent memory is a broader family of mechanisms for persistent state maintenance in agents. Many memory workflows are naturally expressible using windowed, stateful, and incremental operators; stream processors are one powerful realization of that idea, but not the only one.

> Windowing should be treated as a logical abstraction, not as proof of a streaming workload. Agent memory often requires bounded context, persistent state, and incremental maintenance, but these properties can be supported by both stream processors and database-centric systems. Current conversational memory benchmarks such as LoCoMo and LongMemEval justify claims about long-horizon memory difficulty, not high-velocity ingestion. More broadly, agent memory is not limited to chatbots; monitoring, security, robotics, and financial agents may also maintain persistent memory over ongoing observations. In those broader settings, stream-processing abstractions may become especially attractive, but it is still more accurate to frame agent memory as a stateful incremental maintenance problem rather than to claim that it is inherently a high-speed streaming problem.
