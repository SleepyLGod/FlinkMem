#!/usr/bin/env python3
"""Dataset loaders for LongMemEval and LoCoMo conversational samples."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


SUPPORTED_AGENT_MEMORY_DATASETS = frozenset({"longmemeval", "locomo"})
DEFAULT_MAX_MESSAGES = 48
DEFAULT_BASE_TIMESTAMP_MS = 1_700_000_000_000
DEFAULT_MESSAGE_STEP_MS = 60_000


@dataclass(frozen=True)
class DatasetConversation:
    """Normalized conversation sample extracted from benchmark datasets."""

    dataset_name: str
    sample_id: str
    messages: Sequence[Mapping[str, Any]]
    metadata: Mapping[str, Any]


def load_dataset_conversation(
    *,
    dataset_name: str,
    dataset_path: str,
    sample_index: int,
    max_messages: int = DEFAULT_MAX_MESSAGES,
) -> DatasetConversation:
    """Load one conversation sample from LongMemEval or LoCoMo dataset."""
    normalized_name = str(dataset_name).strip().lower()
    if normalized_name not in SUPPORTED_AGENT_MEMORY_DATASETS:
        raise ValueError(
            f"dataset_name must be one of {sorted(SUPPORTED_AGENT_MEMORY_DATASETS)!r}"
        )
    if int(sample_index) < 0:
        raise ValueError("sample_index must be >= 0")
    if int(max_messages) <= 0:
        raise ValueError("max_messages must be > 0")

    payload = _read_json_payload(dataset_path=dataset_path)
    rows = _extract_samples(payload=payload)
    if int(sample_index) >= len(rows):
        raise IndexError(
            f"sample_index={sample_index} out of range for dataset size={len(rows)}"
        )
    row = rows[int(sample_index)]
    if normalized_name == "longmemeval":
        return _load_longmemeval_sample(row=row, max_messages=int(max_messages))
    return _load_locomo_sample(row=row, max_messages=int(max_messages))


def _read_json_payload(*, dataset_path: str) -> Any:
    path = Path(dataset_path)
    if not path.exists():
        raise FileNotFoundError(f"dataset file not found: {dataset_path}")
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def _extract_samples(*, payload: Any) -> Sequence[Mapping[str, Any]]:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            rows = payload["data"]
        elif "samples" in payload and isinstance(payload["samples"], list):
            rows = payload["samples"]
        else:
            rows = [payload]
    else:
        raise TypeError(f"unsupported dataset payload type: {type(payload)!r}")
    normalized_rows: List[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError(f"dataset sample must be object, got {type(row)!r}")
        normalized_rows.append(dict(row))
    if not normalized_rows:
        raise ValueError("dataset contains no samples")
    return normalized_rows


def _load_longmemeval_sample(
    *,
    row: Mapping[str, Any],
    max_messages: int,
) -> DatasetConversation:
    sample_id = _first_non_empty_string(
        row.get("question_id"),
        row.get("instance_id"),
        row.get("id"),
    )
    haystack_sessions = row.get("haystack_sessions")
    if not isinstance(haystack_sessions, list):
        raise ValueError("longmemeval sample requires list field haystack_sessions")
    normalized_messages: List[Dict[str, Any]] = []
    timestamp_ms = DEFAULT_BASE_TIMESTAMP_MS
    message_idx = 0
    for session_idx, session in enumerate(haystack_sessions):
        if not isinstance(session, list):
            raise ValueError(
                f"longmemeval haystack_sessions[{session_idx}] must be list"
            )
        for turn in session:
            if message_idx >= max_messages:
                break
            content = _extract_turn_text(turn=turn)
            role = _extract_turn_role(turn=turn, default_role="user")
            speaker = _extract_turn_speaker(turn=turn, fallback=f"session_{session_idx}_{role}")
            normalized_messages.append(
                {
                    "message_id": f"{sample_id}_m_{message_idx}",
                    "sender_id": speaker,
                    "role": role,
                    "content": content,
                    "timestamp_ms": timestamp_ms,
                }
            )
            message_idx += 1
            timestamp_ms += DEFAULT_MESSAGE_STEP_MS
        if message_idx >= max_messages:
            break
    if not normalized_messages:
        raise ValueError("longmemeval sample produced no messages")
    metadata = {
        "question_id": row.get("question_id"),
        "question": row.get("question"),
        "answer": row.get("answer"),
        "question_date": row.get("question_date"),
        "answer_session_ids": row.get("answer_session_ids"),
        "source_file_schema": "longmemeval",
    }
    return DatasetConversation(
        dataset_name="longmemeval",
        sample_id=sample_id,
        messages=normalized_messages,
        metadata=metadata,
    )


def _load_locomo_sample(
    *,
    row: Mapping[str, Any],
    max_messages: int,
) -> DatasetConversation:
    sample_id = _first_non_empty_string(
        row.get("dialog_id"),
        row.get("conversation_id"),
        row.get("id"),
    )
    dialogs = row.get("dialog")
    if dialogs is None:
        dialogs = row.get("dialogue")
    if dialogs is None:
        dialogs = row.get("conversation")
    if not isinstance(dialogs, list):
        raise ValueError(
            "locomo sample requires list field dialog/dialogue/conversation"
        )
    normalized_messages: List[Dict[str, Any]] = []
    timestamp_ms = DEFAULT_BASE_TIMESTAMP_MS
    for message_idx, turn in enumerate(dialogs[:max_messages]):
        content = _extract_turn_text(turn=turn)
        role = _extract_turn_role(turn=turn, default_role="user")
        speaker = _extract_turn_speaker(turn=turn, fallback=f"speaker_{message_idx % 2}")
        normalized_messages.append(
            {
                "message_id": f"{sample_id}_m_{message_idx}",
                "sender_id": speaker,
                "role": role,
                "content": content,
                "timestamp_ms": timestamp_ms,
            }
        )
        timestamp_ms += DEFAULT_MESSAGE_STEP_MS
    if not normalized_messages:
        raise ValueError("locomo sample produced no messages")
    metadata = {
        "question_id": row.get("question_id"),
        "question": row.get("question"),
        "answer": row.get("answer"),
        "source_file_schema": "locomo",
    }
    return DatasetConversation(
        dataset_name="locomo",
        sample_id=sample_id,
        messages=normalized_messages,
        metadata=metadata,
    )


def _extract_turn_text(*, turn: Any) -> str:
    if isinstance(turn, str):
        text = turn.strip()
        if not text:
            raise ValueError("turn text must be non-empty")
        return text
    if not isinstance(turn, Mapping):
        raise TypeError(f"turn must be string/object, got {type(turn)!r}")
    candidates = [
        turn.get("content"),
        turn.get("text"),
        turn.get("utterance"),
        turn.get("message"),
        turn.get("value"),
    ]
    text = _first_non_empty_string(*candidates)
    return text


def _extract_turn_role(*, turn: Any, default_role: str) -> str:
    if not isinstance(turn, Mapping):
        return default_role
    role = turn.get("role")
    if role is None:
        role = turn.get("speaker_role")
    if role is None:
        return default_role
    normalized = str(role).strip().lower()
    if not normalized:
        return default_role
    return normalized


def _extract_turn_speaker(*, turn: Any, fallback: str) -> str:
    if not isinstance(turn, Mapping):
        return fallback
    speaker = turn.get("speaker")
    if speaker is None:
        speaker = turn.get("sender")
    if speaker is None:
        speaker = turn.get("name")
    if speaker is None:
        return fallback
    normalized = str(speaker).strip()
    if not normalized:
        return fallback
    return normalized


def _first_non_empty_string(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    raise ValueError("unable to resolve non-empty string from candidates")
