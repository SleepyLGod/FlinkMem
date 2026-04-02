#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.isolation/venv/py312/bin/python}"
DOCKER_BIN="${DOCKER_BIN:-docker}"

RUN_ID="$(date +%s)"
MONGO_CONTAINER="am_mongo_${RUN_ID}"
NEO4J_CONTAINER="am_neo4j_${RUN_ID}"
ES_CONTAINER="am_es_${RUN_ID}"
MILVUS_COMPOSE_FILE=""

WITH_ES="${WITH_ES:-false}"
WITH_MILVUS="${WITH_MILVUS:-false}"
KEEP_ARTIFACTS="${KEEP_ARTIFACTS:-false}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-/tmp/agent_memory_stack_artifacts}"
ARTIFACT_DIR="${ARTIFACT_ROOT}/run_${RUN_ID}"
DATASET_SOURCE="${DATASET_SOURCE:-longmemeval}"
DATASET_PATH="${DATASET_PATH:-}"
DATASET_SAMPLE_INDEX="${DATASET_SAMPLE_INDEX:-0}"
DATASET_MAX_MESSAGES="${DATASET_MAX_MESSAGES:-24}"
WORKFLOWS="${WORKFLOWS:-evermemos,mem0,zep}"

MONGO_PORT="${MONGO_PORT:-27017}"
NEO4J_HTTP_PORT="${NEO4J_HTTP_PORT:-7474}"
NEO4J_BOLT_PORT="${NEO4J_BOLT_PORT:-7687}"
ES_PORT="${ES_PORT:-9200}"

NEO4J_USERNAME="${NEO4J_USERNAME:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-secret123}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"

WAIT_MAX_ATTEMPTS=60
WAIT_INTERVAL_SECONDS=2

ENV_CANDIDATES=(
  "${ROOT_DIR}/.env"
  "${ROOT_DIR}/tools/agent_memory/.env"
)

# Preserve command-line exported env vars over values loaded from .env files.
ENV_BEFORE_DOTENV_FILE="$(mktemp -t am_env_before_dotenv_XXXXXX)"
env > "${ENV_BEFORE_DOTENV_FILE}"

for env_file in "${ENV_CANDIDATES[@]}"; do
  if [[ -f "${env_file}" ]]; then
    set -a
    source "${env_file}"
    set +a
  fi
done

while IFS='=' read -r key value; do
  if [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    export "${key}=${value}"
  fi
done < "${ENV_BEFORE_DOTENV_FILE}"
rm -f "${ENV_BEFORE_DOTENV_FILE}"

if [[ -z "${DATASET_PATH}" ]]; then
  if [[ "${DATASET_SOURCE}" == "longmemeval" ]]; then
    DATASET_PATH="${LONGMEMEVAL_DATASET_PATH:-}"
  elif [[ "${DATASET_SOURCE}" == "locomo" ]]; then
    DATASET_PATH="${LOCOMO_DATASET_PATH:-}"
  else
    echo "Unsupported DATASET_SOURCE=${DATASET_SOURCE}"
    exit 1
  fi
fi
if [[ -z "${DATASET_PATH}" ]]; then
  echo "DATASET_PATH is required. Set DATASET_PATH or LONGMEMEVAL_DATASET_PATH/LOCOMO_DATASET_PATH."
  exit 1
fi
if [[ ! -f "${DATASET_PATH}" ]]; then
  echo "DATASET_PATH does not exist: ${DATASET_PATH}"
  exit 1
fi
if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "PYTHON_BIN is not executable: ${PYTHON_BIN}"
  exit 1
fi
if ! command -v "${DOCKER_BIN}" >/dev/null 2>&1; then
  echo "Container runtime command not found: ${DOCKER_BIN}"
  echo "Set DOCKER_BIN to your docker binary path, e.g.:"
  echo "  DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker ./tools/agent_memory/run_local_agent_memory_stack.sh"
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required but not found in PATH"
  exit 1
fi
if [[ ${#NEO4J_PASSWORD} -lt 8 ]]; then
  echo "NEO4J_PASSWORD must be at least 8 characters for Neo4j 5+"
  exit 1
fi

function require_python_module() {
  local module_name="$1"
  if ! "${PYTHON_BIN}" -c "import ${module_name}" >/dev/null 2>&1; then
    echo "Missing Python dependency: ${module_name}"
    echo "Install with: ${PYTHON_BIN} -m pip install ${module_name}"
    exit 1
  fi
}

require_python_module "aiohttp"
if [[ "${WORKFLOWS}" == *"evermemos"* ]]; then
  require_python_module "pymongo"
fi
if [[ "${WORKFLOWS}" == *"mem0"* || "${WORKFLOWS}" == *"zep"* ]]; then
  require_python_module "neo4j"
fi

export SEM_RUNTIME_API_KEY_ENV="${SEM_RUNTIME_API_KEY_ENV:-DEEPSEEK_API_KEY}"
export SEM_RUNTIME_API_BASE="${SEM_RUNTIME_API_BASE:-https://api.deepseek.com/v1}"
export SEM_RUNTIME_MODEL="${SEM_RUNTIME_MODEL:-deepseek-chat}"
export SEM_RUNTIME_TIMEOUT_S="${SEM_RUNTIME_TIMEOUT_S:-120}"
export SEM_RUNTIME_MAX_RETRIES="${SEM_RUNTIME_MAX_RETRIES:-4}"
export SEM_RUNTIME_RETRY_BASE_DELAY_S="${SEM_RUNTIME_RETRY_BASE_DELAY_S:-0.5}"
export AGENT_MEMORY_PROFILE_ENABLED="${AGENT_MEMORY_PROFILE_ENABLED:-0}"
export AGENT_MEMORY_PROFILE_OUTPUT="${AGENT_MEMORY_PROFILE_OUTPUT:-artifact}"
if [[ -z "${!SEM_RUNTIME_API_KEY_ENV:-}" ]]; then
  echo "Missing LLM API key env: ${SEM_RUNTIME_API_KEY_ENV}"
  exit 1
fi
LLM_API_KEY_VALUE="${!SEM_RUNTIME_API_KEY_ENV}"

mkdir -p "${ARTIFACT_DIR}"

function collect_artifacts() {
  set +e
  "${DOCKER_BIN}" logs "${MONGO_CONTAINER}" >"${ARTIFACT_DIR}/mongo.log" 2>&1
  "${DOCKER_BIN}" logs "${NEO4J_CONTAINER}" >"${ARTIFACT_DIR}/neo4j.log" 2>&1
  if [[ "${WITH_ES}" == "true" ]]; then
    "${DOCKER_BIN}" logs "${ES_CONTAINER}" >"${ARTIFACT_DIR}/elasticsearch.log" 2>&1
  fi
}

function cleanup() {
  set +e
  collect_artifacts
  if [[ -n "${MILVUS_COMPOSE_FILE}" && -f "${MILVUS_COMPOSE_FILE}" ]]; then
    "${DOCKER_BIN}" compose -f "${MILVUS_COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1
    rm -f "${MILVUS_COMPOSE_FILE}"
  fi
  "${DOCKER_BIN}" rm -f "${ES_CONTAINER}" >/dev/null 2>&1
  "${DOCKER_BIN}" rm -f "${NEO4J_CONTAINER}" >/dev/null 2>&1
  "${DOCKER_BIN}" rm -f "${MONGO_CONTAINER}" >/dev/null 2>&1
  if [[ "${KEEP_ARTIFACTS}" != "true" ]]; then
    rm -rf "${ARTIFACT_DIR}"
  fi
}

trap cleanup EXIT INT TERM

function wait_mongo() {
  local attempt=1
  until "${DOCKER_BIN}" exec "${MONGO_CONTAINER}" mongosh --quiet --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; do
    if (( attempt >= WAIT_MAX_ATTEMPTS )); then
      echo "MongoDB did not become ready in time"
      exit 1
    fi
    sleep "${WAIT_INTERVAL_SECONDS}"
    attempt=$((attempt + 1))
  done
}

function wait_neo4j() {
  local attempt=1
  until "${DOCKER_BIN}" exec "${NEO4J_CONTAINER}" cypher-shell -u "${NEO4J_USERNAME}" -p "${NEO4J_PASSWORD}" "RETURN 1;" >/dev/null 2>&1; do
    if (( attempt >= WAIT_MAX_ATTEMPTS )); then
      echo "Neo4j did not become ready in time"
      exit 1
    fi
    sleep "${WAIT_INTERVAL_SECONDS}"
    attempt=$((attempt + 1))
  done
}

function wait_es() {
  local attempt=1
  until curl -sf "http://localhost:${ES_PORT}" >/dev/null 2>&1; do
    if (( attempt >= WAIT_MAX_ATTEMPTS )); then
      echo "Elasticsearch did not become ready in time"
      exit 1
    fi
    sleep "${WAIT_INTERVAL_SECONDS}"
    attempt=$((attempt + 1))
  done
}

function wait_ollama() {
  local ollama_base_url="$1"
  local attempt=1
  until curl -sf "${ollama_base_url%/}/api/tags" >/dev/null 2>&1; do
    if (( attempt >= WAIT_MAX_ATTEMPTS )); then
      echo "Ollama did not become ready in time: ${ollama_base_url}"
      exit 1
    fi
    sleep "${WAIT_INTERVAL_SECONDS}"
    attempt=$((attempt + 1))
  done
}

function ensure_ollama_model_exists() {
  local ollama_base_url="$1"
  local model_name="$2"
  if ! curl -sf "${ollama_base_url%/}/api/tags" | "${PYTHON_BIN}" -c '
import json
import os
import sys

model_name = os.environ["MEM0_EMBEDDER_MODEL"].strip()
payload = json.load(sys.stdin)
models = payload.get("models", [])
names = []
if isinstance(models, list):
    for row in models:
        if isinstance(row, dict):
            name = row.get("name")
            if isinstance(name, str):
                names.append(name)
target_names = {model_name, f"{model_name}:latest"}
sys.exit(0 if any(name in target_names for name in names) else 1)
'; then
    echo "Ollama model not found: ${model_name}"
    echo "Run: ollama pull ${model_name}"
    exit 1
  fi
}

function probe_ollama_embedding() {
  local ollama_base_url="$1"
  local model_name="$2"
  if ! curl -sf "${ollama_base_url%/}/api/embeddings" \
    -H "Content-Type: application/json" \
    -d "{\"model\":\"${model_name}\",\"prompt\":\"embedding health check\"}" \
    | "${PYTHON_BIN}" -c '
import json
import sys
payload = json.load(sys.stdin)
embedding = payload.get("embedding")
if not isinstance(embedding, list) or len(embedding) == 0:
    sys.exit(1)
sys.exit(0)
'; then
    echo "Ollama embedding probe failed at ${ollama_base_url} for model ${model_name}"
    exit 1
  fi
}

echo "[stack] starting MongoDB container: ${MONGO_CONTAINER}"
"${DOCKER_BIN}" run -d --name "${MONGO_CONTAINER}" -p "${MONGO_PORT}:27017" mongo:7 >/dev/null
wait_mongo

echo "[stack] starting Neo4j container: ${NEO4J_CONTAINER}"
"${DOCKER_BIN}" run -d \
  --name "${NEO4J_CONTAINER}" \
  -p "${NEO4J_HTTP_PORT}:7474" \
  -p "${NEO4J_BOLT_PORT}:7687" \
  -e "NEO4J_AUTH=${NEO4J_USERNAME}/${NEO4J_PASSWORD}" \
  neo4j:5.26-community >/dev/null
wait_neo4j

if [[ "${WITH_ES}" == "true" ]]; then
  echo "[stack] starting Elasticsearch container: ${ES_CONTAINER}"
  "${DOCKER_BIN}" run -d \
    --name "${ES_CONTAINER}" \
    -p "${ES_PORT}:9200" \
    -e "discovery.type=single-node" \
    -e "xpack.security.enabled=false" \
    docker.elastic.co/elasticsearch/elasticsearch:8.13.4 >/dev/null
  wait_es
fi

if [[ "${WITH_MILVUS}" == "true" ]]; then
  MILVUS_COMPOSE_FILE="$(mktemp -t milvus-compose-XXXXXX.yml)"
  echo "[stack] downloading Milvus compose file: ${MILVUS_COMPOSE_FILE}"
  curl -L "https://github.com/milvus-io/milvus/releases/download/v2.5.14/milvus-standalone-docker-compose.yml" -o "${MILVUS_COMPOSE_FILE}" >/dev/null
  echo "[stack] starting Milvus compose stack"
  "${DOCKER_BIN}" compose -f "${MILVUS_COMPOSE_FILE}" up -d >/dev/null
fi

export EVERMEMOS_MONGO_URI="mongodb://localhost:${MONGO_PORT}"
export EVERMEMOS_ES_ENABLED="${WITH_ES}"
export EVERMEMOS_MILVUS_ENABLED="${WITH_MILVUS}"
if [[ "${WITH_ES}" == "true" ]]; then
  export EVERMEMOS_ES_HOSTS="http://localhost:${ES_PORT}"
fi
if [[ "${WITH_MILVUS}" == "true" ]]; then
  export EVERMEMOS_MILVUS_URI="http://localhost:19530"
fi

export MEM0_EMBEDDER_PROVIDER="${MEM0_EMBEDDER_PROVIDER:-ollama}"
export MEM0_EMBEDDER_MODEL="${MEM0_EMBEDDER_MODEL:-all-minilm}"
export MEM0_EMBEDDER_OLLAMA_BASE_URL="${MEM0_EMBEDDER_OLLAMA_BASE_URL:-http://localhost:11434}"
export MEM0_EMBEDDER_EMBEDDING_DIM="${MEM0_EMBEDDER_EMBEDDING_DIM:-384}"
export MEM0_EMBEDDER_MAX_INPUT_CHARS="${MEM0_EMBEDDER_MAX_INPUT_CHARS:-512}"
export OLLAMA_EMBED_MAX_INPUT_CHARS="${OLLAMA_EMBED_MAX_INPUT_CHARS:-${MEM0_EMBEDDER_MAX_INPUT_CHARS}}"
export MEM0_LLM_PROVIDER="${MEM0_LLM_PROVIDER:-openai}"
export MEM0_LLM_MODEL="${MEM0_LLM_MODEL:-${SEM_RUNTIME_MODEL}}"
export MEM0_LLM_BASE_URL="${MEM0_LLM_BASE_URL:-${SEM_RUNTIME_API_BASE}}"
export MEM0_LLM_API_KEY="${MEM0_LLM_API_KEY:-${LLM_API_KEY_VALUE}}"
export MEM0_VECTOR_PROVIDER="${MEM0_VECTOR_PROVIDER:-faiss}"
export MEM0_VECTOR_PATH="${MEM0_VECTOR_PATH:-/tmp/mem0-faiss-runtime}"
export MEM0_GRAPH_ENABLED="true"
export MEM0_GRAPH_PROVIDER="neo4j"
export MEM0_GRAPH_URL="bolt://localhost:${NEO4J_BOLT_PORT}"
export MEM0_GRAPH_USERNAME="${NEO4J_USERNAME}"
export MEM0_GRAPH_PASSWORD="${NEO4J_PASSWORD}"
export MEM0_GRAPH_DATABASE="${NEO4J_DATABASE}"
export MEM0_RELATION_REFERENCE_MODE="${MEM0_RELATION_REFERENCE_MODE:-name}"
export MEM0_DRIFT_POLICY="${MEM0_DRIFT_POLICY:-upstream_compatible}"

export ZEP_GRAPH_URI="bolt://localhost:${NEO4J_BOLT_PORT}"
export ZEP_GRAPH_USERNAME="${NEO4J_USERNAME}"
export ZEP_GRAPH_PASSWORD="${NEO4J_PASSWORD}"
export ZEP_GRAPH_DATABASE="${NEO4J_DATABASE}"
export ZEP_LLM_PROVIDER="${ZEP_LLM_PROVIDER:-openai}"
export ZEP_LLM_MODEL="${ZEP_LLM_MODEL:-${SEM_RUNTIME_MODEL}}"
export ZEP_LLM_BASE_URL="${ZEP_LLM_BASE_URL:-${SEM_RUNTIME_API_BASE}}"
export ZEP_LLM_API_KEY="${ZEP_LLM_API_KEY:-${LLM_API_KEY_VALUE}}"
export ZEP_SUMMARY_LLM_MODEL="${ZEP_SUMMARY_LLM_MODEL:-${ZEP_LLM_MODEL}}"
export ZEP_EMBEDDER_PROVIDER="${ZEP_EMBEDDER_PROVIDER:-ollama}"
export ZEP_EMBEDDER_MODEL="${ZEP_EMBEDDER_MODEL:-${MEM0_EMBEDDER_MODEL}}"
export ZEP_EMBEDDER_OLLAMA_BASE_URL="${ZEP_EMBEDDER_OLLAMA_BASE_URL:-${MEM0_EMBEDDER_OLLAMA_BASE_URL}}"
export ZEP_EMBEDDER_EMBEDDING_DIM="${ZEP_EMBEDDER_EMBEDDING_DIM:-${MEM0_EMBEDDER_EMBEDDING_DIM}}"
export ZEP_EDGE_REFERENCE_MODE="${ZEP_EDGE_REFERENCE_MODE:-name}"
export ZEP_DRIFT_POLICY="${ZEP_DRIFT_POLICY:-upstream_compatible}"

if [[ "${MEM0_EMBEDDER_PROVIDER}" == "ollama" ]]; then
  wait_ollama "${MEM0_EMBEDDER_OLLAMA_BASE_URL}"
  ensure_ollama_model_exists "${MEM0_EMBEDDER_OLLAMA_BASE_URL}" "${MEM0_EMBEDDER_MODEL}"
  probe_ollama_embedding "${MEM0_EMBEDDER_OLLAMA_BASE_URL}" "${MEM0_EMBEDDER_MODEL}"
fi

echo "[stack] running real-backend smoke script"
SMOKE_CMD=(
  "${PYTHON_BIN}"
  "${ROOT_DIR}/tools/agent_memory/agent_memory_real_smoke.py"
  --workflows
  "${WORKFLOWS}"
  --dataset-source
  "${DATASET_SOURCE}"
  --dataset-path
  "${DATASET_PATH}"
  --sample-index
  "${DATASET_SAMPLE_INDEX}"
  --max-messages
  "${DATASET_MAX_MESSAGES}"
  --artifact-dir
  "${ARTIFACT_DIR}"
)
echo "[stack] workflows=${WORKFLOWS} dataset_source=${DATASET_SOURCE} sample_index=${DATASET_SAMPLE_INDEX} max_messages=${DATASET_MAX_MESSAGES}"
echo "[stack] llm_model=${SEM_RUNTIME_MODEL} timeout_s=${SEM_RUNTIME_TIMEOUT_S} max_retries=${SEM_RUNTIME_MAX_RETRIES}"
echo "[stack] mem0_llm_model=${MEM0_LLM_MODEL} zep_llm_model=${ZEP_LLM_MODEL} zep_summary_llm_model=${ZEP_SUMMARY_LLM_MODEL}"
echo "[stack] mem0_relation_reference_mode=${MEM0_RELATION_REFERENCE_MODE} mem0_drift_policy=${MEM0_DRIFT_POLICY}"
echo "[stack] zep_edge_reference_mode=${ZEP_EDGE_REFERENCE_MODE} zep_drift_policy=${ZEP_DRIFT_POLICY}"
echo "[stack] embed_model=${MEM0_EMBEDDER_MODEL} embed_max_input_chars=${OLLAMA_EMBED_MAX_INPUT_CHARS}"
echo "[stack] profiling_enabled=${AGENT_MEMORY_PROFILE_ENABLED} profiling_output=${AGENT_MEMORY_PROFILE_OUTPUT}"
"${SMOKE_CMD[@]}" 2>&1 | tee "${ARTIFACT_DIR}/smoke_stdout.log"

echo "[stack] smoke run completed; cleanup will run automatically"
if [[ "${KEEP_ARTIFACTS}" == "true" ]]; then
  echo "[stack] artifacts: ${ARTIFACT_DIR}"
fi
