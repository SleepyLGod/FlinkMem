# Agent Memory Workflows (Flink Semantic Runtime)

This directory contains end-to-end workflow reconstructions for agent-memory systems on top of `semantic_runtime` operators.

## Current Status (Important)

1. EverMemOS:

- Workflow + external config + external runtime adapters are implemented.
- Real backend wiring path exists (`MongoDB`, optional `Elasticsearch`, optional `Milvus`).

2. Zep / Graphiti:

- Workflow + external config + external runtime adapters are implemented.
- Real backend wiring path exists (`Neo4j`).

3. Mem0 / Mem0-Graph:

- Workflow + source-aligned external config + external runtime adapters are implemented.
- Basic path uses local FAISS embedding backend (in-process, lightweight).
- Graph path uses Neo4j store + embedding recall over graph rows.

## Implemented Workflows

1. `evermemos/`

- EverMemOS insertion/retrieval reconstruction.
- Supports boundary strategy switch (`sem_filter` or `all_history`) through operator runtime config.
- Keeps external runtime/config contracts for DB/LLM/embedding backends.

2. `mem0/`

- Mem0 Basic workflow (`add`, `search`).
- Mem0 Graph workflow (`add`, `search`) including entity and relation updates.
- Recall backend supports `embedding` and `llm` (default `embedding`) for both Basic and Graph paths.

3. `common/`

- Shared contracts and protocol interfaces used by memory workflows.

4. `zep/`

- Zep/Graphiti `add_episode` workflow skeleton.
- Includes source-aligned backend config object (`LLM + Embedder + Neo4j`).
- Includes Neo4j external runtime adapter and bundle/client factory.

## Semantic Query Coverage (Current)

Reference query spec:
`flink-python/pyflink/semantic_runtime/docs/tools/sem_queries_agentmem.md`

### Zep / Graphiti

1. Q1 `sem_map` entity extraction: implemented (`extract_entities`)
2. Q2 `sem_join` entity resolution: implemented as `search_entity_candidates + resolve_entity`
3. Q3 `sem_map` entity summary update: implemented (`summarize_entity`)
4. Q4 `sem_map` edge extraction: implemented (`extract_edges`)
5. Q5 duplicate fact detection: implemented in `resolve_edge(action=DUPLICATE)`
6. Q6 contradiction fact detection: implemented in `resolve_edge(action=CONTRADICTS)`
7. Q7 community summary fusion: TODO
8. Q8 community naming: TODO

### Mem0 / Mem0-Graph

1. Basic Q1-Q4: implemented (`sem_map` + `sem_lookup_join` style recall + resolution actions)
2. Graph Q5-Q9: implemented (entity extraction/recall/identity + relation extraction/resolution)

### EverMemOS

1. Core insertion/retrieval path implemented.
2. Boundary strategy supports `sem_filter` and `all_history`.

## Local Dependency Matrix

| Workflow       | Required DB/Index  | Optional DB/Index                          | Semantic Backend |
| -------------- | ------------------ | ------------------------------------------ | ---------------- |
| EverMemOS      | MongoDB            | Elasticsearch, Milvus                      | LLM + Embedding  |
| Mem0 Basic     | FAISS (in-process) | Qdrant/other vector DB via future adapters | LLM + Embedding  |
| Mem0 Graph     | Neo4j + Embedding  | -                                          | LLM + Embedding  |
| Zep / Graphiti | Neo4j              | -                                          | LLM + Embedding  |

## Isolated Python Environment Setup

```bash
cd /Users/von/Projects/FlinkMem
PY=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python

$PY -m pip install -U pip
$PY -m pip install \
  pymongo \
  elasticsearch \
  pymilvus \
  neo4j
```

Optional (if you want additional Mem0 vector DB SDKs locally):

```bash
$PY -m pip install \
  qdrant-client \
  chromadb
```

## CPU-Only Local Bring-up (Mac Apple Silicon)

1. Start Ollama and pull a lightweight embedding model:

```bash
ollama pull all-minilm
```

2. Start databases (Docker examples):

```bash
# MongoDB
docker run -d --name am-mongo -p 27017:27017 mongo:7

# Neo4j (for Zep / Mem0-Graph)
docker run -d --name am-neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/secret123 \
  -e NEO4J_PLUGINS='["apoc"]' \
  neo4j:5.26-community

# Qdrant (optional vector backend for Mem0 Basic)
docker run -d --name am-qdrant -p 6333:6333 qdrant/qdrant:latest

# Elasticsearch (optional for EverMemOS keyword retrieval)
docker run -d --name am-es \
  -p 9200:9200 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  docker.elastic.co/elasticsearch/elasticsearch:8.13.4
```

3. Milvus (optional for EverMemOS vector retrieval):

```bash
wget https://github.com/milvus-io/milvus/releases/download/v2.5.14/milvus-standalone-docker-compose.yml -O docker-compose.milvus.yml
docker compose -f docker-compose.milvus.yml up -d
```

4. Minimal env examples:

```bash
# EverMemOS
export EVERMEMOS_MONGO_URI="mongodb://localhost:27017"
export EVERMEMOS_ES_ENABLED="false"
export EVERMEMOS_MILVUS_ENABLED="false"

# Mem0 Basic (local FAISS + Ollama embedding)
export MEM0_EMBEDDER_PROVIDER="ollama"
export MEM0_EMBEDDER_MODEL="all-minilm"
export MEM0_EMBEDDER_OLLAMA_BASE_URL="http://localhost:11434"
export MEM0_EMBEDDER_MAX_INPUT_CHARS="512"
export OLLAMA_EMBED_MAX_INPUT_CHARS="512"
export MEM0_VECTOR_PROVIDER="faiss"
export MEM0_VECTOR_PATH="/tmp/mem0-faiss"

# Mem0 Graph
export MEM0_GRAPH_ENABLED="true"
export MEM0_GRAPH_PROVIDER="neo4j"
export MEM0_GRAPH_URL="bolt://localhost:7687"
export MEM0_GRAPH_USERNAME="neo4j"
export MEM0_GRAPH_PASSWORD="secret123"

# Zep
export ZEP_GRAPH_URI="bolt://localhost:7687"
export ZEP_GRAPH_USERNAME="neo4j"
export ZEP_GRAPH_PASSWORD="secret123"
```

5. Workflow-specific minimums:

- EverMemOS minimal path: `MongoDB` + `EVERMEMOS_ES_ENABLED=false` + `EVERMEMOS_MILVUS_ENABLED=false`.
- Zep minimal path: `Neo4j`.
- Mem0 Basic minimal path: local FAISS + embedding function.
- Mem0-Graph minimal path: `Neo4j` + embedding function.

6. Prepare `.env` (DeepSeek + dataset path):

```bash
# required for LLM calls
export DEEPSEEK_API_KEY="your_deepseek_key"

# optional overrides (defaults already match DeepSeek OpenAI-compatible API)
export SEM_RUNTIME_API_KEY_ENV="DEEPSEEK_API_KEY"
export SEM_RUNTIME_API_BASE="https://api.deepseek.com/v1"
export SEM_RUNTIME_MODEL="deepseek-chat"

# optional per-workflow overrides
export MEM0_LLM_MODEL="deepseek-chat"
export ZEP_LLM_MODEL="deepseek-chat"
# optional step-level override (defaults to ZEP_LLM_MODEL)
export ZEP_SUMMARY_LLM_MODEL="deepseek-reasoner"

# required dataset path (choose one)
export LONGMEMEVAL_DATASET_PATH="/absolute/path/longmemeval_s_cleaned.json"
# or
export LOCOMO_DATASET_PATH="/absolute/path/locomo.json"
```

Quick start from template:

```bash
cd /Users/von/Projects/FlinkMem
cp tools/agent_memory/.env.example .env
# then edit .env with real key + dataset path
```

`.env` behavior:

- `tools/agent_memory/run_local_agent_memory_stack.sh` will auto-load `repo/.env` and `repo/tools/agent_memory/.env`.
- `tools/agent_memory/agent_memory_real_smoke.py` also auto-loads the same two paths when run directly.
- If both shell env and `.env` define the same key, shell env takes precedence.
- Runtime implementation lives under
  `flink-python/pyflink/semantic_runtime/runtime/workflows/agent_memory/runtime/`.
  `tools/agent_memory/agent_memory_real_smoke.py` is a thin wrapper entrypoint.

7. Quick run commands (copy/paste):

```bash
cd /Users/von/Projects/FlinkMem
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

Default behavior:

- Starts `MongoDB + Neo4j`.
- Runs real workflow calls for EverMemOS/Mem0/Zep.
- Always removes containers after run.

7.1 Isolated-env command variants:

```bash
cd /Users/von/Projects/FlinkMem
PYTHON_BIN=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
NEO4J_PASSWORD=secret123 \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

Run one workflow only:

```bash
cd /Users/von/Projects/FlinkMem
PYTHON_BIN=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
NEO4J_PASSWORD=secret123 \
WORKFLOWS=evermemos \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

```bash
cd /Users/von/Projects/FlinkMem
PYTHON_BIN=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
NEO4J_PASSWORD=secret123 \
WORKFLOWS=mem0 \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

```bash
cd /Users/von/Projects/FlinkMem
PYTHON_BIN=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
NEO4J_PASSWORD=secret123 \
WORKFLOWS=zep \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

```bash
# Zep fail-fast index endpoint mode
cd /Users/von/Projects/FlinkMem
PYTHON_BIN=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
NEO4J_PASSWORD=secret123 \
WORKFLOWS=zep \
ZEP_EDGE_REFERENCE_MODE=index \
ZEP_DRIFT_POLICY=fail_fast \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

8. Choose dataset source:

```bash
cd /Users/von/Projects/FlinkMem
DATASET_SOURCE=longmemeval \
DATASET_PATH=/absolute/path/longmemeval_s_cleaned.json \
bash tools/agent_memory/run_local_agent_memory_stack.sh

cd /Users/von/Projects/FlinkMem
DATASET_SOURCE=locomo \
DATASET_PATH=/absolute/path/locomo.json \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

9. Keep run artifacts (logs + JSON result) after cleanup:

```bash
cd /Users/von/Projects/FlinkMem
KEEP_ARTIFACTS=true bash tools/agent_memory/run_local_agent_memory_stack.sh
```

Containers are still cleaned; only files under `/tmp/agent_memory_stack_artifacts/run_<timestamp>/` are kept.

10. Select dataset sample and message budget:

```bash
cd /Users/von/Projects/FlinkMem
DATASET_SAMPLE_INDEX=0 \
DATASET_MAX_MESSAGES=24 \
KEEP_ARTIFACTS=true \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

10.1 Enable workflow profiling (stage + llm aggregate):

```bash
cd /Users/von/Projects/FlinkMem
AGENT_MEMORY_PROFILE_ENABLED=1 \
AGENT_MEMORY_PROFILE_OUTPUT=artifact \
KEEP_ARTIFACTS=true \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

Profiling knobs:

- `AGENT_MEMORY_PROFILE_ENABLED=0|1` (default `0`)
- `AGENT_MEMORY_PROFILE_OUTPUT=artifact|stdout|both` (default `artifact`)

Profiling outputs when enabled:

- `workflow_result.json` includes a top-level `profiling` object.
- `profiling.json` is written under artifact dir when output mode includes `artifact`.
- `smoke_runner` prints profiling mode banner to stderr when output mode includes `stdout`.

11. Data source notes:

- Input messages are read from LongMemEval/LoCoMo dataset files (not synthetic inline messages).
- Loader entrypoint: `flink-python/pyflink/semantic_runtime/runtime/workflows/agent_memory/runtime/dataset_loader.py`.
- The runner selects one conversation sample by `DATASET_SAMPLE_INDEX`, then trims to `DATASET_MAX_MESSAGES`.
- Replay semantics are message-by-message in event-time order; workflows are triggered per message.
- Optional replay input scope policy for Mem0/Zep:
  - `MEMORY_INPUT_SCOPE_POLICY=none|sliding|session` (default `none`)
  - `MEMORY_INPUT_SCOPE_SLIDING_SIZE` (used when `sliding`)
  - `MEMORY_INPUT_SCOPE_SESSION_GAP_MS` (used when `session`)
- Zep intra-message concurrency knobs:
  - `ZEP_ENTITY_RESOLVE_CONCURRENCY`
  - `ZEP_ENTITY_SUMMARY_CONCURRENCY` (LLM summary generation stage)
  - `ZEP_ENTITY_UPSERT_GROUP_CONCURRENCY` (DB write stage)
  - `ZEP_EDGE_RESOLVE_CONCURRENCY`
  - `ZEP_EDGE_WRITE_GROUP_CONCURRENCY`
  - `ZEP_EDGE_REFERENCE_MODE=name|index` (default `name`)
  - `ZEP_DRIFT_POLICY=fail_fast|upstream_compatible` (default `upstream_compatible`)
- Mem0 relation endpoint mode:
  - `MEM0_RELATION_REFERENCE_MODE=name|index` (default `name`)
  - `MEM0_DRIFT_POLICY=fail_fast|upstream_compatible` (default `upstream_compatible`)

12. DeepSeek + Ollama status:

- LLM calls now use `LLMClientConfig(backend=openai)` with DeepSeek-compatible endpoint.
- `MEM0_LLM_*` and `ZEP_LLM_*` now override `SEM_RUNTIME_*` for their respective workflows in smoke runs.
- `ZEP_SUMMARY_LLM_*` can override the `summarize_entity` stage only (defaults to `ZEP_LLM_*`).
- Embedding now uses Ollama (`/api/embeddings`) via `MEM0_EMBEDDER_OLLAMA_BASE_URL` + `MEM0_EMBEDDER_MODEL`.
- For long-memory updates, set `MEM0_EMBEDDER_MAX_INPUT_CHARS` (default in smoke stack: `512`) to avoid Ollama context overflow on very long fact content.
- `OLLAMA_EMBED_MAX_INPUT_CHARS` is applied at smoke-runner embedding-call boundary (default follows `MEM0_EMBEDDER_MAX_INPUT_CHARS` in stack script).
- For Zep throughput tuning, start with `ZEP_ENTITY_SUMMARY_CONCURRENCY=8` and keep
  `ZEP_ENTITY_UPSERT_GROUP_CONCURRENCY` lower when Neo4j write pressure becomes the bottleneck.
- `ZEP_EDGE_REFERENCE_MODE` controls edge endpoint representation:
  - `name`: model returns `source_entity_name`/`destination_entity_name`
  - `index`: model returns `source_index`/`destination_index` into `allowed_entity_names`
- `MEM0_RELATION_REFERENCE_MODE` controls relation endpoint representation:
  - `name`: model returns `source`/`destination`
  - `index`: model returns `source_index`/`destination_index`
- `ZEP_DRIFT_POLICY` and `MEM0_DRIFT_POLICY` control drift handling:
  - `upstream_compatible` (default): continue on endpoint drift
    - zep: unresolved edge endpoints are skipped
    - mem0 graph: unresolved relation endpoints are materialized as placeholder entities before relation resolution
  - `fail_fast`: unresolved/malformed endpoint rows fail immediately
- Mem0 smoke retrieval query uses benchmark question (`metadata.question`) to align with retrieval semantics and avoid passing long raw messages directly into embedding recall.
- Zep Neo4j fulltext indexes are bootstrapped by runtime on startup (`node_name_and_summary`, `edge_name_and_fact`), so explicit manual index creation is not required.
- Zep LLM prompt budget is bounded by environment knobs to avoid oversized `extract_edges` calls:
  - `ZEP_PROMPT_MESSAGE_MAX_CHARS`
  - `ZEP_PROMPT_MAX_RECENT_EPISODES`
  - `ZEP_PROMPT_RECENT_EPISODE_MAX_CHARS`
  - `ZEP_PROMPT_MAX_EDGE_ENTITIES`
  - smoke defaults: `1200 / 3 / 400 / 16`

13. Real-run failure patterns (from recent logs):

- Mem0 graph in `MEM0_DRIFT_POLICY=fail_fast` can fail hard when extracted relation endpoints are not exact members of `allowed_entity_names`.
  - Example failure shape: relation destination like `"stress relief"` while extracted entity list does not contain that exact surface form.
  - This is not a DB outage; it is an entity-reference contract mismatch at relation extraction.
- Zep with `ZEP_DRIFT_POLICY=fail_fast` can fail similarly when edge endpoints drift from extracted entity names.
- Full combined run (`evermemos,mem0,zep`) can fail even when isolated runs pass, due to aggregate LLM + embedding load causing more retries/timeouts and more lexical drift under fail-fast policy.
- Neo4j startup warnings such as "label/property does not exist" are expected on empty graph startup and are not fatal by themselves.

14. Upstream alignment notes (mem0 / zep):

- Mem0 upstream graph extraction is name-oriented (`source`, `relationship`, `destination`) and relies on prompt/schema constraints plus normalization.
  - Source-aligned baseline: `MEM0_RELATION_REFERENCE_MODE=name` + `MEM0_DRIFT_POLICY=upstream_compatible`.
- Zep/Graphiti upstream extraction is also name-oriented, but invalid edge endpoints are typically skipped with warnings in parts of upstream flow.
  - Source-aligned baseline: `ZEP_EDGE_REFERENCE_MODE=name` + `ZEP_DRIFT_POLICY=upstream_compatible`.
- Explicit failure-driven evaluation:
  - keep `*_REFERENCE_MODE=name|index` as needed
  - set `*_DRIFT_POLICY=fail_fast`
- Practical recommendation:
  - Use `REFERENCE_MODE=name` + `DRIFT_POLICY=upstream_compatible` for apples-to-apples upstream reconstruction.
  - Use `DRIFT_POLICY=fail_fast` for controlled failure-surface experiments.
- Upstream references checked for this behavior:
  - mem0 relation extraction schema and graph flow:
    - `https://github.com/mem0ai/mem0/blob/main/mem0/graphs/tools.py`
    - `https://github.com/mem0ai/mem0/blob/main/mem0/memory/graph_memory.py`
  - zep/graphiti edge extraction prompt + runtime validation path:
    - `https://github.com/getzep/graphiti/blob/main/graphiti_core/prompts/extract_edges.py`
    - `https://github.com/getzep/graphiti/blob/main/graphiti_core/utils/maintenance/edge_operations.py`

Troubleshooting (`docker: command not found`):

```bash
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

Troubleshooting (`ollama embedding request failed: HTTP Error 502`):

```bash
# 1) Check Ollama is up
curl -sSf http://localhost:11434/api/tags

# 2) Ensure model exists
ollama pull all-minilm

# 3) Re-run stack
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
OLLAMA_MAX_RETRIES=6 \
OLLAMA_RETRY_BASE_DELAY_S=0.5 \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

Notes:

- `OLLAMA_MAX_RETRIES` and `OLLAMA_RETRY_BASE_DELAY_S` control embedding retry for transient Ollama 429/5xx.
- Keep these low-to-moderate (for example `4~8`) to avoid overloading local CPU inference.
- If you hit `input length exceeds context length`, lower `MEM0_EMBEDDER_MAX_INPUT_CHARS` and `OLLAMA_EMBED_MAX_INPUT_CHARS` (for example `256~1024`).

Troubleshooting (`sem_map LLM call failed ... timeout`):

```bash
cd /Users/von/Projects/FlinkMem
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
NEO4J_PASSWORD=secret123 \
WORKFLOWS=evermemos \
DATASET_MAX_MESSAGES=8 \
SEM_RUNTIME_TIMEOUT_S=120 \
SEM_RUNTIME_MAX_RETRIES=4 \
./tools/agent_memory/run_local_agent_memory_stack.sh
```

Notes:

- This first isolates EverMemOS and reduces prompt size to validate end-to-end connectivity.
- Then raise `DATASET_MAX_MESSAGES` and add back `mem0,zep`.

Troubleshooting (`endpoint drift mode passes sometimes, full run fails with KeyError/ValueError on entity endpoints`):

```bash
# fail-fast with name endpoints
MEM0_RELATION_REFERENCE_MODE=name \
ZEP_EDGE_REFERENCE_MODE=name \
MEM0_DRIFT_POLICY=fail_fast \
ZEP_DRIFT_POLICY=fail_fast \
bash tools/agent_memory/run_local_agent_memory_stack.sh

# fail-fast with index endpoints (for lexical instability experiments)
MEM0_RELATION_REFERENCE_MODE=index \
ZEP_EDGE_REFERENCE_MODE=index \
MEM0_DRIFT_POLICY=fail_fast \
ZEP_DRIFT_POLICY=fail_fast \
bash tools/agent_memory/run_local_agent_memory_stack.sh

# upstream-compatible mode (default)
MEM0_RELATION_REFERENCE_MODE=name \
ZEP_EDGE_REFERENCE_MODE=name \
MEM0_DRIFT_POLICY=upstream_compatible \
ZEP_DRIFT_POLICY=upstream_compatible \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

Notes:

- If isolated runs pass but full run fails, first reduce `DATASET_MAX_MESSAGES` and split workflows (`WORKFLOWS=...`) to identify load-sensitive step(s).
- Then decide mode by experiment goal:
  - strict parity/failure-surface: use `*_DRIFT_POLICY=fail_fast`
  - upstream reproduction baseline: use `*_DRIFT_POLICY=upstream_compatible`

## Design Rules

1. Workflow orchestration lives here; generic semantic logic stays in semantic operators.
2. Storage-specific logic stays in adapters/protocol implementations, not in operator kernels.
3. Default path remains explicit and deterministic; no silent fallback.

## Tests

Run from repository root:

```bash
cd /Users/von/Projects/FlinkMem
```

Use isolated Python (recommended):

```bash
/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python
```

Run memory workflow tests:

```bash
/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python -m pytest -q \
  flink-python/pyflink/semantic_runtime/tests/test_agent_memory_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_agent_memory_external_config.py \
  flink-python/pyflink/semantic_runtime/tests/test_agent_memory_external_runtime.py \
  flink-python/pyflink/semantic_runtime/tests/test_mem0_basic_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_mem0_graph_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_mem0_external_config.py \
  flink-python/pyflink/semantic_runtime/tests/test_zep_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_zep_external_config.py \
  flink-python/pyflink/semantic_runtime/tests/test_zep_external_runtime.py
```

Run workflow-specific tests:

```bash
/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python -m pytest -q \
  flink-python/pyflink/semantic_runtime/tests/test_zep_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_zep_external_config.py \
  flink-python/pyflink/semantic_runtime/tests/test_zep_external_runtime.py

/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python -m pytest -q \
  flink-python/pyflink/semantic_runtime/tests/test_mem0_basic_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_mem0_graph_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_mem0_external_config.py \
  flink-python/pyflink/semantic_runtime/tests/test_mem0_external_runtime.py

/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python -m pytest -q \
  flink-python/pyflink/semantic_runtime/tests/test_agent_memory_workflow.py \
  flink-python/pyflink/semantic_runtime/tests/test_agent_memory_external_config.py \
  flink-python/pyflink/semantic_runtime/tests/test_agent_memory_external_runtime.py
```

## Notes for Next Work

1. Zep Q7/Q8 (community-level fusion + naming) is still TODO.
2. Prompt/predicate parity should be tracked by tests to avoid drift from upstream workflow specs.

## Recommended Run Commands

Use these as the default command schemes from repository root.

1. Baseline single-workflow smoke (fast fail surface, easiest to debug):

```bash
WORKFLOWS=evermemos \
SEM_RUNTIME_MODEL=deepseek-chat \
SEM_RUNTIME_TIMEOUT_S=60 \
SEM_RUNTIME_MAX_RETRIES=3 \
DATASET_MAX_MESSAGES=12 \
KEEP_ARTIFACTS=true \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

2. Mem0 isolated run with strict embedding bounds:

```bash
WORKFLOWS=mem0 \
SEM_RUNTIME_MODEL=deepseek-chat \
SEM_RUNTIME_TIMEOUT_S=60 \
SEM_RUNTIME_MAX_RETRIES=3 \
MEM0_RELATION_REFERENCE_MODE=name \
MEM0_DRIFT_POLICY=upstream_compatible \
MEM0_EMBEDDER_MODEL=nomic-embed-text \
MEM0_EMBEDDER_MAX_INPUT_CHARS=512 \
OLLAMA_EMBED_MAX_INPUT_CHARS=512 \
DATASET_MAX_MESSAGES=24 \
KEEP_ARTIFACTS=true \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

3. Zep isolated run with separated summary model:

```bash
WORKFLOWS=zep \
ZEP_LLM_MODEL=deepseek-chat \
ZEP_SUMMARY_LLM_MODEL=deepseek-reasoner \
SEM_RUNTIME_TIMEOUT_S=120 \
SEM_RUNTIME_MAX_RETRIES=4 \
DATASET_MAX_MESSAGES=24 \
KEEP_ARTIFACTS=true \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

4. Full workflows run (comparison baseline):

```bash
WORKFLOWS=evermemos,mem0,zep \
SEM_RUNTIME_MODEL=deepseek-chat \
SEM_RUNTIME_TIMEOUT_S=90 \
SEM_RUNTIME_MAX_RETRIES=4 \
MEM0_RELATION_REFERENCE_MODE=name \
ZEP_EDGE_REFERENCE_MODE=name \
MEM0_DRIFT_POLICY=upstream_compatible \
ZEP_DRIFT_POLICY=upstream_compatible \
MEM0_EMBEDDER_MODEL=nomic-embed-text \
MEM0_EMBEDDER_MAX_INPUT_CHARS=512 \
OLLAMA_EMBED_MAX_INPUT_CHARS=512 \
DATASET_SAMPLE_INDEX=0 \
DATASET_MAX_MESSAGES=24 \
KEEP_ARTIFACTS=true \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

4.1 Full workflows run (fail-fast index endpoint mode):

```bash
WORKFLOWS=evermemos,mem0,zep \
SEM_RUNTIME_MODEL=deepseek-chat \
SEM_RUNTIME_TIMEOUT_S=90 \
SEM_RUNTIME_MAX_RETRIES=4 \
MEM0_RELATION_REFERENCE_MODE=index \
ZEP_EDGE_REFERENCE_MODE=index \
MEM0_DRIFT_POLICY=fail_fast \
ZEP_DRIFT_POLICY=fail_fast \
MEM0_EMBEDDER_MODEL=nomic-embed-text \
MEM0_EMBEDDER_MAX_INPUT_CHARS=512 \
OLLAMA_EMBED_MAX_INPUT_CHARS=512 \
DATASET_SAMPLE_INDEX=0 \
DATASET_MAX_MESSAGES=24 \
KEEP_ARTIFACTS=true \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

5. Replay policy experiment (same data, different input scope):

```bash
WORKFLOWS=mem0,zep \
MEMORY_INPUT_SCOPE_POLICY=sliding \
MEMORY_INPUT_SCOPE_SLIDING_SIZE=8 \
SEM_RUNTIME_MODEL=deepseek-chat \
DATASET_MAX_MESSAGES=24 \
KEEP_ARTIFACTS=true \
DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker \
bash tools/agent_memory/run_local_agent_memory_stack.sh
```

6. Quick artifact inspection after any run:

```bash
LATEST_RUN_DIR="$(ls -td /tmp/agent_memory_stack_artifacts/run_* | head -n 1)"
echo "${LATEST_RUN_DIR}"
cat "${LATEST_RUN_DIR}/workflow_result.json"
tail -n 120 "${LATEST_RUN_DIR}/smoke_stdout.log"
```
