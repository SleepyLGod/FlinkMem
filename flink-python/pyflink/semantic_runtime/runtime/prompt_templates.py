"""Internal prompt synthesis for semantic facade execution.

These helpers translate user-facing semantic intent into concrete prompt
templates consumed by row-level async kernels. They are internal runtime
infrastructure and not public API.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _schema_description(output_schema: Optional[Dict[str, Any]]) -> str:
    if not output_schema:
        return "no structured schema"
    parts = []
    for key, value_type in output_schema.items():
        type_name = getattr(value_type, "__name__", str(value_type))
        parts.append(f'"{key}": {type_name}')
    return ", ".join(parts)


def build_sem_map_prompt(
    intent: str,
    *,
    output_schema: Optional[Dict[str, Any]],
    output_mode: str,
) -> str:
    """Build the internal sem_map prompt template."""
    if output_mode == "text" or output_schema is None:
        return (
            f"{intent}\n\n"
            "Input:\n{input}\n\n"
            "Return only the final text answer. Do not wrap it in JSON."
        )
    return (
        f"{intent}\n\n"
        "Input:\n{input}\n\n"
        "Return one JSON object matching this shallow schema exactly:\n"
        f"{_schema_description(output_schema)}"
    )


def build_sem_filter_prompt(intent: str) -> str:
    """Build the internal sem_filter prompt template."""
    return (
        f"{intent}\n\n"
        "Input:\n{input}\n\n"
        'Return one JSON object with exactly these fields: '
        '{{"decision": bool, "confidence": float, "reason": str}}.'
    )


def build_sem_local_topk_scoring_prompt(intent: str) -> str:
    """Build the internal semantic scoring prompt for local top-k."""
    return (
        f"{intent}\n\n"
        "Input record:\n{input}\n\n"
        "Candidate list:\n{candidates}\n\n"
        'Return one JSON object with a "scored_candidates" field. '
        'Each item must be an object with fields: '
        '{{"candidate": any, "score": float, "reason": str}}.'
    )


def build_sem_lookup_join_prompt(intent: str) -> str:
    """Build the internal semantic match prompt for lookup join blocks."""
    return (
        f"{intent}\n\n"
        "Left input:\n{input}\n\n"
        "Candidate block:\n{candidates}\n\n"
        "Choose the best candidate from this block for the left input.\n"
        'Return one JSON object with exactly these fields: '
        '{{"matched": bool, "match_score": float, "selected_candidate": any, "reason": str}}.'
    )


def build_sem_groupby_scope_prompt(intent: str) -> str:
    """Build the internal semantic assignment prompt for one bounded grouping scope."""
    return (
        f"{intent}\n\n"
        "Existing groups:\n{existing_groups}\n\n"
        "Events to assign:\n{events}\n\n"
        "For each event, assign it to one existing group_id or create one new group_id.\n"
        'Return one JSON object with exactly this shape: '
        '{{"assignments": [{{"event_seq_id": int, "group_id": str, "confidence": float, "label": str}}]}}.'
    )
