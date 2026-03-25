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


def build_sem_score_prompt(intent: str) -> str:
    """Build the internal semantic scoring prompt for one item."""
    return (
        f"{intent}\n\n"
        "Item to score:\n{input}\n\n"
        'Return one JSON object with exactly these fields: '
        '{{"score": float, "confidence": float, "reason": str}}.'
    )


def build_sem_score_block_prompt(intent: str) -> str:
    """Build the internal semantic scoring prompt for one bounded item block."""
    return (
        f"{intent}\n\n"
        "Items to score:\n{input}\n\n"
        'Return one JSON object with exactly this shape: '
        '{{"scores": [{{"item_idx": int, "score": float, "confidence": float, "reason": str}}]}}.'
    )


def build_sem_rerank_block_prompt(intent: str, *, method: str) -> str:
    """Build the internal semantic rerank prompt for one bounded item block."""
    return (
        f"{intent}\n\n"
        f"Rerank method: {method}\n\n"
        "Items to rerank:\n{input}\n\n"
        'Return one JSON object with exactly this shape: '
        '{{"ranked_item_ids": [str, ...]}}.'
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


def build_sem_match_block_prompt(intent: str) -> str:
    """Build the internal semantic pair-block prompt for sem_match."""
    return (
        f"{intent}\n\n"
        "Candidate pairs:\n{input}\n\n"
        "For each pair, decide whether the pair semantically joins.\n"
        'Return one JSON object with exactly this shape: '
        '{{"matches": [{{"pair_idx": int, "matched": bool, "match_score": float, "reason": str}}]}}.'
    )


def build_sem_window_pairwise_prompt() -> str:
    """Build the internal semantic continuity prompt for pairwise sem_window."""
    return (
        "Decide whether the current event continues the same semantic window as the "
        "previous event.\n\n"
        "Input payload:\n{input}\n\n"
        'Return one JSON object with exactly these fields: '
        '{{"continue_window": bool, "confidence": float, "reason": str}}.'
    )


def build_sem_window_summary_continuity_prompt() -> str:
    """Build the internal semantic continuity prompt for summary sem_window."""
    return (
        "Decide whether the current event continues the same semantic window as the "
        "current window summary.\n\n"
        "Input payload:\n{input}\n\n"
        'Return one JSON object with exactly these fields: '
        '{{"continue_window": bool, "confidence": float, "reason": str}}.'
    )


def build_sem_window_summary_update_prompt() -> str:
    """Build the internal semantic summary-update prompt for summary sem_window."""
    return (
        "Update the current semantic window summary after appending the new event.\n\n"
        "Input payload:\n{input}\n\n"
        'Return one JSON object with exactly these fields: '
        '{{"summary": str}}.'
    )


def build_sem_window_all_history_prompt() -> str:
    """Build the internal semantic membership prompt for all-history sem_window."""
    return (
        "Decide whether the current event still belongs to the active semantic "
        "window formed by the full active window history.\n\n"
        "Input payload:\n{input}\n\n"
        'Return one JSON object with exactly these fields: '
        '{{"continue_window": bool, "confidence": float, "reason": str}}.'
    )
