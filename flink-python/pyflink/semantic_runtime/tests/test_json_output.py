"""Tests for LLM JSON object parsing helpers."""

from __future__ import annotations

import pytest

from pyflink.semantic_runtime.runtime.json_output import parse_llm_json_object


def test_parse_llm_json_object_plain_json() -> None:
    payload = parse_llm_json_object('{"subject":"career"}', operator_name="sem_map")
    assert payload == {"subject": "career"}


def test_parse_llm_json_object_json_code_fence() -> None:
    payload = parse_llm_json_object(
        '```json\n{"subject":"career"}\n```',
        operator_name="sem_map",
    )
    assert payload == {"subject": "career"}


def test_parse_llm_json_object_with_preface_text() -> None:
    payload = parse_llm_json_object(
        'Here is the result:\n{"subject":"career"}\nThanks.',
        operator_name="sem_map",
    )
    assert payload == {"subject": "career"}


def test_parse_llm_json_object_empty_text_raises() -> None:
    with pytest.raises(ValueError, match="response text is empty"):
        parse_llm_json_object("", operator_name="sem_map")

