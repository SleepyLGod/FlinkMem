#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""
V0.1 Conversational Correctness Suite (dual trigger modes + real API evaluation).

What this suite covers:
1) Per-message trigger pipeline (row-local semantic operator chain).
2) Window-first trigger pipeline (Flink built-in count window -> semantic chain).
3) LoCoMo single-user QA correctness evaluation (1 user x N QA, default N=20).

Two-level metrics:
- Structural/operator hard-gates (count conservation, parseability, degraded tagging,
  non-degraded schema validity).
- Semantic quality metrics (exact match + relaxed match).

Notes:
- This suite intentionally stays in V0.1 boundaries:
  no keyed-state semantic operators are introduced.
- In `real/all` mode, this script will also try to load a local `.env` file
  (without overriding already-exported environment variables).

Run examples (isolated env):
  1) Mock-only dual-path structural check
     export JAVA_HOME=/Users/von/Projects/FlinkMem/.isolation/jdk/jdk-17.0.18+8/Contents/Home
     export PYFLINK_CLIENT_EXECUTABLE=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python
     export PATH=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin:$PATH
     python flink-python/pyflink/semantic_runtime/tests/test_v01_conversational_correctness_suite.py --mode mock

  2) Real API check (LoCoMo 1 user x 20 QA)
     export JAVA_HOME=/Users/von/Projects/FlinkMem/.isolation/jdk/jdk-17.0.18+8/Contents/Home
     export PYFLINK_CLIENT_EXECUTABLE=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin/python
     export PATH=/Users/von/Projects/FlinkMem/.isolation/venv/py312/bin:$PATH
     python flink-python/pyflink/semantic_runtime/tests/test_v01_conversational_correctness_suite.py \
       --mode real --locomo-data /tmp/locomo10.json --sample-index 0 --qa-limit 20
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# Bootstrap: ensure pyflink.semantic_runtime is importable via symlink.
import pyflink as _pf

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)

from pyflink.common import Time, Types
from pyflink.datastream import AsyncDataStream, StreamExecutionEnvironment
from pyflink.datastream.functions import WindowFunction

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.sem_filter import SemFilterFunction
from pyflink.semantic_runtime.operators.sem_join_retrieve import SemJoinRetrieveConfig, SemJoinRetrieveFunction
from pyflink.semantic_runtime.operators.sem_map import SemMapFunction


EVENT_REQUIRED_FIELDS = (
    "user_id",
    "session_id",
    "msg_id",
    "ts",
    "current_message",
    "prev_message",
)


def canonical_event(
    user_id: str,
    session_id: str,
    msg_id: str,
    ts: int,
    current_message: str,
    prev_message: str,
    window_messages: Optional[List[str]] = None,
) -> Dict[str, Any]:
    event = {
        "user_id": user_id,
        "session_id": session_id,
        "msg_id": msg_id,
        "ts": int(ts),
        "current_message": str(current_message),
        "prev_message": str(prev_message),
    }
    if window_messages is not None:
        event["window_messages"] = [str(x) for x in window_messages]
    return event


def ensure_event_schema(event: Dict[str, Any]) -> None:
    for key in EVENT_REQUIRED_FIELDS:
        if key not in event:
            raise AssertionError(f"Event missing required field: {key}")
    if not isinstance(event["ts"], int):
        raise AssertionError("Event field 'ts' must be int")


def dumps_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def loads_json_or_raise(s: str) -> Dict[str, Any]:
    obj = json.loads(s)
    if not isinstance(obj, dict):
        raise AssertionError(f"Expected JSON object, got {type(obj).__name__}")
    return obj


def validate_shallow_schema(obj: Dict[str, Any], schema: Dict[str, type]) -> bool:
    for key, typ in schema.items():
        if key not in obj:
            return False
        if not isinstance(obj[key], typ):
            return False
    return True


def normalize_text(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def relaxed_match(pred: str, gold: str) -> bool:
    p = normalize_text(pred)
    g = normalize_text(gold)
    if not p or not g:
        return False
    return p == g or p in g or g in p


def _parse_dotenv_line(raw: str) -> Tuple[Optional[str], Optional[str]]:
    line = raw.strip()
    if not line or line.startswith("#"):
        return None, None
    if line.startswith("export "):
        line = line[len("export "):].strip()
    if "=" not in line:
        return None, None
    key, val = line.split("=", 1)
    key = key.strip()
    val = val.strip()
    if not key:
        return None, None
    if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
        val = val[1:-1]
    return key, val


def try_load_env_file(env_file: str) -> Optional[str]:
    path_arg = pathlib.Path(env_file).expanduser()
    if path_arg.is_absolute():
        candidates = [path_arg]
    else:
        repo_root = pathlib.Path(__file__).resolve().parents[4]
        candidates = [pathlib.Path.cwd() / path_arg, repo_root / path_arg]

    for candidate in candidates:
        if not candidate.exists():
            continue
        loaded = 0
        with candidate.open("r", encoding="utf-8") as f:
            for raw in f:
                key, val = _parse_dotenv_line(raw)
                if not key:
                    continue
                if key in os.environ:
                    continue
                os.environ[key] = val or ""
                loaded += 1
        return f"{candidate} (loaded={loaded})"
    return None


def collect_stream(stream, job_name: str) -> List[str]:
    results: List[str] = []
    with stream.execute_and_collect(job_name) as it:
        for row in it:
            results.append(str(row))
    return results


def run_async_stage(
    stage_name: str,
    inputs: List[str],
    async_fn,
    timeout_s: int,
    capacity: int,
    ordered: bool = False,
) -> List[str]:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(inputs, type_info=Types.STRING())
    if ordered:
        out = AsyncDataStream.ordered_wait(
            ds, async_fn, Time.seconds(timeout_s), capacity, Types.STRING()
        )
    else:
        out = AsyncDataStream.unordered_wait(
            ds, async_fn, Time.seconds(timeout_s), capacity, Types.STRING()
        )
    return collect_stream(out, f"cp_{stage_name}")


class BundleWindowFunction(WindowFunction):
    """Bundles count-window records into a single canonical event with window_messages."""

    def apply(self, key, window, inputs: Iterable[str]) -> Iterable[str]:
        rows: List[Dict[str, Any]] = []
        for raw in inputs:
            row = json.loads(raw) if isinstance(raw, str) else json.loads(str(raw))
            rows.append(row)
        if not rows:
            return []
        rows.sort(key=lambda x: int(x.get("ts", 0)))
        first = rows[0]
        last = rows[-1]
        messages = [str(r.get("current_message", "")) for r in rows]
        bundled = canonical_event(
            user_id=str(key),
            session_id=f"{first.get('session_id', 'session')}__window",
            msg_id=f"{first.get('msg_id', 'start')}..{last.get('msg_id', 'end')}",
            ts=int(last.get("ts", 0)),
            current_message=str(last.get("current_message", "")),
            prev_message=str(first.get("prev_message", "")),
            window_messages=messages,
        )
        bundled["source_msg_ids"] = [str(r.get("msg_id", "")) for r in rows]
        return [dumps_json(bundled)]


def build_window_events(
    events_json: List[str],
    window_size: int,
    window_slide: int,
) -> List[str]:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(events_json, type_info=Types.STRING())
    keyed = ds.key_by(lambda s: json.loads(s)["user_id"], key_type=Types.STRING())
    if window_slide <= 0:
        windowed = keyed.count_window(window_size)
    else:
        windowed = keyed.count_window(window_size, window_slide)
    bundled = windowed.apply(BundleWindowFunction(), output_type=Types.STRING())
    return collect_stream(bundled, "cp_window_bundle")


def stage_structural_stats(
    stage_name: str,
    inputs: List[str],
    outputs: List[str],
    non_degraded_schema: Optional[Dict[str, type]] = None,
    non_degraded_extra_check: Optional[Callable[[Dict[str, Any]], bool]] = None,
    enforce_one_to_one: bool = True,
    max_allowed_attempts: Optional[int] = None,
) -> Dict[str, Any]:
    if enforce_one_to_one and len(outputs) != len(inputs):
        raise AssertionError(
            f"{stage_name}: silent drop/dup detected. input={len(inputs)}, output={len(outputs)}"
        )

    parsed_ok = 0
    degraded_count = 0
    degraded_tag_errors = 0
    non_degraded_count = 0
    non_degraded_schema_invalid = 0
    latencies: List[float] = []
    attempts_list: List[int] = []

    for out in outputs:
        try:
            obj = loads_json_or_raise(out)
            parsed_ok += 1
        except Exception:
            continue

        is_degraded = bool(obj.get("_degraded", False))
        if is_degraded:
            degraded_count += 1
            if not str(obj.get("_error", "")).strip():
                degraded_tag_errors += 1
            continue

        non_degraded_count += 1
        if non_degraded_schema is not None and not validate_shallow_schema(obj, non_degraded_schema):
            non_degraded_schema_invalid += 1
        if non_degraded_extra_check is not None and not non_degraded_extra_check(obj):
            non_degraded_schema_invalid += 1

        metrics = obj.get("_metrics", {})
        if isinstance(metrics, dict) and isinstance(metrics.get("latency_ms"), (int, float)):
            latencies.append(float(metrics["latency_ms"]))
        if isinstance(metrics, dict) and isinstance(metrics.get("attempts"), int):
            attempts_list.append(int(metrics["attempts"]))

    parse_fail_count = len(outputs) - parsed_ok
    if parse_fail_count != 0:
        raise AssertionError(f"{stage_name}: {parse_fail_count} outputs are not valid JSON objects")
    if degraded_tag_errors != 0:
        raise AssertionError(f"{stage_name}: {degraded_tag_errors} degraded outputs missing _error")
    if non_degraded_schema_invalid != 0:
        raise AssertionError(
            f"{stage_name}: {non_degraded_schema_invalid} non-degraded outputs violate schema"
        )
    if max_allowed_attempts is not None:
        if attempts_list and max(attempts_list) > max_allowed_attempts:
            raise AssertionError(
                f"{stage_name}: max attempts {max(attempts_list)} exceeds bound {max_allowed_attempts}"
            )

    p95 = 0.0
    if latencies:
        sorted_lat = sorted(latencies)
        idx = int(len(sorted_lat) * 0.95)
        p95 = sorted_lat[min(idx, len(sorted_lat) - 1)]
    schema_valid_output_count = non_degraded_count - non_degraded_schema_invalid
    schema_valid_output_rate = schema_valid_output_count / max(len(outputs), 1)

    return {
        "stage": stage_name,
        "input_count": len(inputs),
        "output_count": len(outputs),
        "degraded_count": degraded_count,
        "degraded_rate": degraded_count / max(len(outputs), 1),
        "timeout_rate": sum(1 for o in outputs if "flink_timeout" in o) / max(len(outputs), 1),
        "non_degraded_count": non_degraded_count,
        "schema_valid_output_rate": schema_valid_output_rate,
        "p95_latency_ms": round(p95, 2),
        "avg_attempts": round(sum(attempts_list) / max(len(attempts_list), 1), 2),
        "max_attempts": max(attempts_list) if attempts_list else 0,
        "parse_fail_count": parse_fail_count,
        "schema_invalid_count": non_degraded_schema_invalid,
    }


def get_openai_llm_config(args) -> LLMClientConfig:
    api_key_env = args.api_key_env
    if not os.environ.get(api_key_env):
        raise RuntimeError(
            f"Environment variable {api_key_env} is empty. "
            f"Export it first (for example: source .env)."
        )
    return LLMClientConfig(
        backend="openai",
        model=args.model,
        api_base=args.api_base,
        api_key_env=api_key_env,
        timeout_s=args.llm_timeout_s,
        max_retries=args.llm_max_retries,
        retry_base_delay_s=args.llm_retry_base_delay_s,
    )


def get_mock_config(response_json: str, delay_s: float = 0.02) -> LLMClientConfig:
    return LLMClientConfig(
        backend="mock",
        mock_delay_s=delay_s,
        mock_response=response_json,
    )


def make_entity_map_prompt() -> str:
    return (
        "Input is a JSON object representing one conversational event.\\n"
        "Return JSON only with exactly these keys:\\n"
        "entities(list of strings), entity_count(int), topic_hint(string).\\n"
        "Rules:\\n"
        "1) Extract up to 5 key entities from current_message into entities.\\n"
        "2) entity_count = len(entities).\\n"
        "3) topic_hint is a short phrase (<= 8 words) summarizing message topic.\\n"
        "4) Output raw JSON only, no markdown, no code fences, no extra keys.\\n"
        "Input:\\n{input}"
    )


def make_topic_filter_prompt() -> str:
    return (
        "Input is a JSON event enriched with entities.\\n"
        "Decide if current_message continues the topic of prev_message.\\n"
        "Return JSON only: {{\"decision\": bool, \"confidence\": float, \"reason\": string}}.\\n"
        "Output raw JSON only, no markdown, no code fences.\\n"
        "Input:\\n{input}"
    )


def make_join_prompt() -> str:
    return (
        "Input is a conversational event decision payload. Candidates are retrieved memory rows.\\n"
        "Select relevant candidates for memory update.\\n"
        "Return JSON only: {{\"selected_memories\": [string], \"rationale\": string}}.\\n"
        "Output raw JSON only, no markdown, no code fences.\\n"
        "Input:\\n{input}\\nCandidates:\\n{candidates}"
    )


def make_memory_action_prompt() -> str:
    return (
        "Input is a semantic-join result over one conversational event.\\n"
        "Return JSON only with keys: action(string), memory_update(string).\\n"
        "action must be one of ADD, UPDATE, NOOP.\\n"
        "Output raw JSON only, no markdown, no code fences.\\n"
        "Input:\\n{input}"
    )


def make_qa_prompt() -> str:
    return (
        "You are a strict QA model.\\n"
        "Given question and conversation excerpt, answer using ONLY the excerpt.\\n"
        "Return JSON only: {{\"answer\": string}}.\\n"
        "Output raw JSON only, no markdown, no code fences.\\n"
        "Input JSON:\\n{input}"
    )


def extra_check_sem_join_non_degraded(obj: Dict[str, Any]) -> bool:
    if "join_result" not in obj:
        return False
    if not isinstance(obj.get("candidate_count"), int):
        return False
    if not isinstance(obj.get("truncated"), bool):
        return False
    return True


@dataclass
class PipelineResult:
    final_outputs: List[str]
    stage_stats: Dict[str, Dict[str, Any]]


def run_semantic_chain(
    inputs: List[str],
    llm_cfg_entity: LLMClientConfig,
    llm_cfg_filter: LLMClientConfig,
    llm_cfg_join_match: LLMClientConfig,
    llm_cfg_action: LLMClientConfig,
    timeout_s: int,
    capacity: int,
    retriever_candidates: List[str],
    stage_prefix: str,
    max_allowed_attempts: int,
) -> PipelineResult:
    stage_stats: Dict[str, Dict[str, Any]] = {}

    # Stage 1: sem_map(entity extraction + passthrough)
    entity_schema = {
        "entities": list,
        "entity_count": int,
        "topic_hint": str,
    }
    s1_fn = SemMapFunction(make_entity_map_prompt(), entity_schema, llm_cfg_entity)
    s1_out = run_async_stage(
        f"{stage_prefix}_sem_map_entity", inputs, s1_fn, timeout_s, capacity, ordered=False
    )
    stage_stats["sem_map_entity"] = stage_structural_stats(
        "sem_map_entity",
        inputs,
        s1_out,
        non_degraded_schema=entity_schema,
        max_allowed_attempts=max_allowed_attempts,
    )

    # Stage 2: sem_filter(topic continuity)
    s2_fn = SemFilterFunction(make_topic_filter_prompt(), llm_cfg_filter, default_decision=False)
    s2_out = run_async_stage(
        f"{stage_prefix}_sem_filter_topic", s1_out, s2_fn, timeout_s, capacity, ordered=False
    )
    stage_stats["sem_filter_topic"] = stage_structural_stats(
        "sem_filter_topic",
        s1_out,
        s2_out,
        non_degraded_schema={"decision": bool, "confidence": float, "reason": str, "_input": str},
        max_allowed_attempts=max_allowed_attempts,
    )

    # Stage 3: sem_join_retrieve
    join_cfg = SemJoinRetrieveConfig(
        max_candidates_per_record=min(max(len(retriever_candidates), 1), 32),
        retrieve_timeout_ms=5000.0,
        mock_candidates=retriever_candidates[:32],
        mock_retrieve_delay_s=0.01,
    )
    s3_fn = SemJoinRetrieveFunction(make_join_prompt(), llm_cfg_join_match, join_cfg)
    s3_out = run_async_stage(
        f"{stage_prefix}_sem_join_retrieve", s2_out, s3_fn, timeout_s, capacity, ordered=False
    )
    stage_stats["sem_join_retrieve"] = stage_structural_stats(
        "sem_join_retrieve",
        s2_out,
        s3_out,
        non_degraded_schema={"candidate_count": int, "truncated": bool},
        non_degraded_extra_check=extra_check_sem_join_non_degraded,
        max_allowed_attempts=max_allowed_attempts,
    )

    # Stage 4: sem_map(memory action)
    action_schema = {"action": str, "memory_update": str}
    s4_fn = SemMapFunction(make_memory_action_prompt(), action_schema, llm_cfg_action)
    s4_out = run_async_stage(
        f"{stage_prefix}_sem_map_action", s3_out, s4_fn, timeout_s, capacity, ordered=False
    )
    stage_stats["sem_map_action"] = stage_structural_stats(
        "sem_map_action",
        s3_out,
        s4_out,
        non_degraded_schema=action_schema,
        max_allowed_attempts=max_allowed_attempts,
    )

    return PipelineResult(final_outputs=s4_out, stage_stats=stage_stats)


def iter_locomo_turns(record: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    conv = record["conversation"]
    session_keys: List[Tuple[int, str]] = []
    for k, v in conv.items():
        m = re.match(r"session_(\d+)$", k)
        if m and isinstance(v, list):
            session_keys.append((int(m.group(1)), k))
    session_keys.sort(key=lambda x: x[0])
    for _, sk in session_keys:
        for item in conv[sk]:
            yield sk, item


def build_locomo_events(record: Dict[str, Any], max_events: Optional[int]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    prev_message = ""
    ts = 0
    user_id = str(record.get("sample_id", "locomo_user"))

    for session_id, turn in iter_locomo_turns(record):
        text = str(turn.get("text", "")).strip()
        msg_id = str(turn.get("dia_id", f"{session_id}:{ts}"))
        if not text:
            continue
        event = canonical_event(
            user_id=user_id,
            session_id=session_id,
            msg_id=msg_id,
            ts=ts,
            current_message=text,
            prev_message=prev_message,
        )
        ensure_event_schema(event)
        events.append(event)
        prev_message = text
        ts += 1
        if max_events is not None and len(events) >= max_events:
            break
    return events


def find_dialogue(conversation: Dict[str, Any], dia_id: str) -> Tuple[str, int]:
    for key, val in conversation.items():
        if key.startswith("session_") and isinstance(val, list):
            for idx, msg in enumerate(val):
                if str(msg.get("dia_id", "")) == dia_id:
                    return key, idx
    raise ValueError(f"Could not locate dia_id={dia_id}")


def build_locomo_qa_requests(
    record: Dict[str, Any],
    qa_limit: int,
    context_window: int,
) -> List[Dict[str, Any]]:
    qa_rows = record.get("qa", [])[:qa_limit]
    conv = record["conversation"]
    requests: List[Dict[str, Any]] = []
    prev_q = ""
    user_id = str(record.get("sample_id", "locomo_user"))

    for i, qa in enumerate(qa_rows):
        q = str(qa.get("question", "")).strip()
        a = str(qa.get("answer", "")).strip()
        evidence = [str(x) for x in qa.get("evidence", [])]

        snippets: List[str] = []
        for eid in evidence:
            try:
                sk, center = find_dialogue(conv, eid)
            except ValueError:
                continue
            turns = conv[sk]
            left = max(0, center - context_window)
            right = min(len(turns), center + context_window + 1)
            for t in turns[left:right]:
                snippets.append(
                    f"{t.get('dia_id','')}\t{t.get('speaker','Unknown')}: {t.get('text','')}"
                )
        if not snippets:
            for t in conv.get("session_1", [])[:8]:
                snippets.append(
                    f"{t.get('dia_id','')}\t{t.get('speaker','Unknown')}: {t.get('text','')}"
                )

        payload = {
            "qa_index": i,
            "sample_id": user_id,
            "question": q,
            "conversation_excerpt": snippets,
        }
        req_event = canonical_event(
            user_id=user_id,
            session_id="qa",
            msg_id=f"qa_{i}",
            ts=i,
            current_message=dumps_json(payload),
            prev_message=prev_q,
        )
        req_event["gold_answer"] = a
        req_event["qa_question"] = q
        req_event["evidence"] = evidence
        requests.append(req_event)
        prev_q = q
    return requests


def run_qa_evaluation(
    qa_requests: List[Dict[str, Any]],
    llm_cfg: LLMClientConfig,
    timeout_s: int,
    capacity: int,
    max_allowed_attempts: int,
) -> Dict[str, Any]:
    inputs = [dumps_json(x) for x in qa_requests]
    qa_fn = SemMapFunction(make_qa_prompt(), {"answer": str}, llm_cfg)
    outputs = run_async_stage(
        "real_locomo_sem_map_qa", inputs, qa_fn, timeout_s, capacity, ordered=True
    )
    qa_stage_stats = stage_structural_stats(
        "qa_sem_map",
        inputs,
        outputs,
        non_degraded_schema={"answer": str},
        max_allowed_attempts=max_allowed_attempts,
    )

    per_sample = []
    exact = 0
    relaxed = 0
    degraded = 0
    for req, out in zip(qa_requests, outputs):
        obj = loads_json_or_raise(out)
        if obj.get("_degraded", False):
            degraded += 1
            pred = ""
        else:
            pred = str(obj.get("answer", "")).strip()
        gold = str(req["gold_answer"]).strip()
        em = normalize_text(pred) == normalize_text(gold) and bool(pred)
        rm = relaxed_match(pred, gold)
        exact += 1 if em else 0
        relaxed += 1 if rm else 0
        per_sample.append(
            {
                "qa_index": req["msg_id"],
                "question": req["qa_question"],
                "gold_answer": gold,
                "pred_answer": pred,
                "exact_match": em,
                "relaxed_match": rm,
                "degraded": bool(obj.get("_degraded", False)),
                "error": obj.get("_error", ""),
            }
        )

    total = len(qa_requests)
    failed_examples = [x for x in per_sample if not x["relaxed_match"]][:5]
    return {
        "stage_stats": qa_stage_stats,
        "total": total,
        "exact_match": exact,
        "relaxed_match": relaxed,
        "exact_match_rate": exact / max(total, 1),
        "relaxed_match_rate": relaxed / max(total, 1),
        "degraded_count": degraded,
        "failed_examples": failed_examples,
        "samples": per_sample,
    }


def create_mock_conversation_events() -> List[Dict[str, Any]]:
    msgs = [
        "Hey, I started a pottery class last week.",
        "Nice! Are you still planning the mountain trip?",
        "Yes, in June. Also I adopted a dog named Pixel.",
        "Great. Pixel sounds adorable.",
        "I might switch jobs to counseling next year.",
        "Interesting, does that relate to your support-group work?",
        "Yes, very much. I want to help trans youth.",
        "That is meaningful. Let's discuss a training plan next time.",
    ]
    events: List[Dict[str, Any]] = []
    prev = ""
    for i, m in enumerate(msgs):
        events.append(
            canonical_event(
                user_id="mock_user",
                session_id="session_1",
                msg_id=f"M{i+1}",
                ts=i,
                current_message=m,
                prev_message=prev,
            )
        )
        prev = m
    return events


def write_artifacts(summary: Dict[str, Any], artifact_dir: str, run_id: str) -> Tuple[str, str]:
    os.makedirs(artifact_dir, exist_ok=True)
    json_path = os.path.join(artifact_dir, f"{run_id}.json")
    md_path = os.path.join(artifact_dir, f"{run_id}.md")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    md_lines: List[str] = []
    md_lines.append(f"# V0.1 Conversational Correctness Suite Report ({run_id})")
    md_lines.append("")
    md_lines.append("## Overview")
    md_lines.append(f"- mode: `{summary['mode']}`")
    md_lines.append(f"- locomo_sample_id: `{summary.get('locomo_sample_id', '')}`")
    md_lines.append("")
    md_lines.append("## Operator Structural Stats")
    for pipeline_name in ("mock_per_message", "mock_window_first", "real_per_message", "real_window_first"):
        if pipeline_name not in summary.get("pipelines", {}):
            continue
        md_lines.append("")
        md_lines.append(f"### {pipeline_name}")
        md_lines.append("| stage | in | out | schema_valid_output_rate | degraded_rate | timeout_rate | p95_latency_ms | avg_attempts | max_attempts |")
        md_lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for st_name, st in summary["pipelines"][pipeline_name]["stage_stats"].items():
            md_lines.append(
                f"| {st_name} | {st['input_count']} | {st['output_count']} | "
                f"{st['schema_valid_output_rate']:.2%} | {st['degraded_rate']:.2%} | "
                f"{st['timeout_rate']:.2%} | {st['p95_latency_ms']:.2f} | "
                f"{st['avg_attempts']:.2f} | {st['max_attempts']} |"
            )

    if "qa_eval" in summary:
        qa = summary["qa_eval"]
        md_lines.append("")
        md_lines.append("## QA Semantic Metrics")
        md_lines.append(f"- total: {qa['total']}")
        md_lines.append(f"- exact_match_rate: {qa['exact_match_rate']:.2%}")
        md_lines.append(f"- relaxed_match_rate: {qa['relaxed_match_rate']:.2%}")
        md_lines.append(f"- degraded_count: {qa['degraded_count']}")
        md_lines.append("")
        md_lines.append("### Failed Examples (top 5)")
        if qa["failed_examples"]:
            for item in qa["failed_examples"]:
                md_lines.append(
                    f"- q: {item['question']} | gold: {item['gold_answer']} | pred: {item['pred_answer']}"
                )
        else:
            md_lines.append("- none")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    return json_path, md_path


def run_mock_suite(args, events: List[Dict[str, Any]]) -> Dict[str, Any]:
    events_json = [dumps_json(e) for e in events]
    candidates = [e["current_message"] for e in events][:20]

    mock_entity_resp = dumps_json(
        {
            "entities": ["Pixel", "pottery"],
            "entity_count": 2,
            "topic_hint": "personal update",
        }
    )
    mock_filter_resp = dumps_json(
        {"decision": True, "confidence": 0.91, "reason": "topic continuity likely"}
    )
    mock_join_resp = dumps_json(
        {"selected_memories": ["memory_a", "memory_b"], "rationale": "relevant memory"}
    )
    mock_action_resp = dumps_json({"action": "UPDATE", "memory_update": "mock memory update"})

    per_msg = run_semantic_chain(
        inputs=events_json,
        llm_cfg_entity=get_mock_config(mock_entity_resp),
        llm_cfg_filter=get_mock_config(mock_filter_resp),
        llm_cfg_join_match=get_mock_config(mock_join_resp),
        llm_cfg_action=get_mock_config(mock_action_resp),
        timeout_s=args.timeout_s,
        capacity=args.capacity,
        retriever_candidates=candidates,
        stage_prefix="mock_per_message",
        max_allowed_attempts=args.max_allowed_attempts,
    )

    window_records = build_window_events(events_json, args.window_size, args.window_slide)
    if not window_records:
        raise AssertionError("Mock window-first path produced zero window records")
    win_msg = run_semantic_chain(
        inputs=window_records,
        llm_cfg_entity=get_mock_config(mock_entity_resp),
        llm_cfg_filter=get_mock_config(mock_filter_resp),
        llm_cfg_join_match=get_mock_config(mock_join_resp),
        llm_cfg_action=get_mock_config(mock_action_resp),
        timeout_s=args.timeout_s,
        capacity=args.capacity,
        retriever_candidates=candidates,
        stage_prefix="mock_window_first",
        max_allowed_attempts=args.max_allowed_attempts,
    )

    return {
        "mock_per_message": {"stage_stats": per_msg.stage_stats, "final_count": len(per_msg.final_outputs)},
        "mock_window_first": {"stage_stats": win_msg.stage_stats, "final_count": len(win_msg.final_outputs)},
    }


def run_real_suite(args, record: Dict[str, Any]) -> Dict[str, Any]:
    events = build_locomo_events(record, max_events=args.real_pipeline_event_limit)
    events_json = [dumps_json(e) for e in events]
    candidates = [e["current_message"] for e in events][:32]
    if not events_json:
        raise AssertionError("No conversational events generated for real suite")

    openai_cfg = get_openai_llm_config(args)
    per_msg = run_semantic_chain(
        inputs=events_json,
        llm_cfg_entity=openai_cfg,
        llm_cfg_filter=openai_cfg,
        llm_cfg_join_match=openai_cfg,
        llm_cfg_action=openai_cfg,
        timeout_s=args.timeout_s,
        capacity=args.capacity,
        retriever_candidates=candidates,
        stage_prefix="real_per_message",
        max_allowed_attempts=args.max_allowed_attempts,
    )

    window_records = build_window_events(events_json, args.window_size, args.window_slide)
    if not window_records:
        raise AssertionError("Real window-first path produced zero window records")
    win_msg = run_semantic_chain(
        inputs=window_records,
        llm_cfg_entity=openai_cfg,
        llm_cfg_filter=openai_cfg,
        llm_cfg_join_match=openai_cfg,
        llm_cfg_action=openai_cfg,
        timeout_s=args.timeout_s,
        capacity=args.capacity,
        retriever_candidates=candidates,
        stage_prefix="real_window_first",
        max_allowed_attempts=args.max_allowed_attempts,
    )

    qa_requests = build_locomo_qa_requests(record, args.qa_limit, args.qa_context_window)
    qa_eval = run_qa_evaluation(
        qa_requests,
        openai_cfg,
        args.timeout_s,
        args.capacity,
        args.max_allowed_attempts,
    )

    return {
        "real_per_message": {"stage_stats": per_msg.stage_stats, "final_count": len(per_msg.final_outputs)},
        "real_window_first": {"stage_stats": win_msg.stage_stats, "final_count": len(win_msg.final_outputs)},
    }, qa_eval


def load_locomo_record(path: str, sample_index: int) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"LoCoMo data not found at {path}. "
            "Download example: curl -L https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json -o /tmp/locomo10.json"
        )
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or sample_index >= len(data):
        raise AssertionError("Invalid LoCoMo data or sample index out of range")
    return data[sample_index]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V0.1 conversational correctness suite")
    p.add_argument("--mode", choices=["mock", "real", "all"], default="all")
    p.add_argument("--locomo-data", default="/tmp/locomo10.json")
    p.add_argument("--sample-index", type=int, default=0)
    p.add_argument("--qa-limit", type=int, default=20)
    p.add_argument("--qa-context-window", type=int, default=4)
    p.add_argument("--real-pipeline-event-limit", type=int, default=8)
    p.add_argument("--window-size", type=int, default=4)
    p.add_argument("--window-slide", type=int, default=4)
    p.add_argument("--timeout-s", type=int, default=60)
    p.add_argument("--capacity", type=int, default=4)
    p.add_argument("--max-allowed-attempts", type=int, default=6)
    p.add_argument("--artifact-dir", default="/tmp/cp_v01_correctness_artifacts")
    p.add_argument("--env-file", default=".env")

    p.add_argument("--api-key-env", default="DEEPSEEK_API_KEY")
    p.add_argument("--api-base", default="https://api.deepseek.com/v1")
    p.add_argument("--model", default="deepseek-chat")
    p.add_argument("--llm-timeout-s", type=float, default=45.0)
    p.add_argument("--llm-max-retries", type=int, default=2)
    p.add_argument("--llm-retry-base-delay-s", type=float, default=0.8)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_id = datetime.now().strftime("v01_correctness_%Y%m%d_%H%M%S")
    summary: Dict[str, Any] = {"mode": args.mode, "pipelines": {}}

    record: Optional[Dict[str, Any]] = None
    if args.mode in ("real", "all"):
        env_load_info = try_load_env_file(args.env_file)
        if env_load_info is not None:
            summary["env_file"] = env_load_info
        record = load_locomo_record(args.locomo_data, args.sample_index)
        summary["locomo_sample_id"] = record.get("sample_id", "")

    if args.mode in ("mock", "all"):
        mock_events = create_mock_conversation_events()
        mock_result = run_mock_suite(args, mock_events)
        summary["pipelines"].update(mock_result)

    if args.mode in ("real", "all"):
        assert record is not None
        real_pipelines, qa_eval = run_real_suite(args, record)
        summary["pipelines"].update(real_pipelines)
        summary["qa_eval"] = qa_eval

    json_path, md_path = write_artifacts(summary, args.artifact_dir, run_id)
    print("=== V0.1 Conversational Correctness Suite ===")
    print(f"mode: {args.mode}")
    print(f"artifact_json: {json_path}")
    print(f"artifact_md:   {md_path}")
    if "qa_eval" in summary:
        qa = summary["qa_eval"]
        print(
            f"qa_total={qa['total']} em={qa['exact_match_rate']:.2%} "
            f"relaxed={qa['relaxed_match_rate']:.2%} degraded={qa['degraded_count']}"
        )


if __name__ == "__main__":
    main()
