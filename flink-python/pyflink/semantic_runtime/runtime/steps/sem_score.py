"""Internal `sem_score` step."""

from __future__ import annotations

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction


class SemScoreFunction(SemMapFunction):
    """Internal semantic score attribute helper."""

    def __init__(self, prompt_template: str, llm_config: LLMClientConfig) -> None:
        super().__init__(
            prompt_template=prompt_template,
            output_schema={"score": float, "confidence": float, "reason": str},
            llm_config=llm_config,
        )
