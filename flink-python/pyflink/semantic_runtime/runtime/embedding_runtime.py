"""Internal embedding runtime helpers.

This module provides one small internal abstraction for embedding-backed
similarity execution. It keeps backend selection and backend-specific wiring
out of individual operators.

The initial goal is modest:

1. support the current local hashing path cleanly,
2. provide explicit skeletons for future API / local-model backends,
3. optionally support a FAISS-backed local similarity path when dependencies
   are installed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder
from pyflink.semantic_runtime.runtime_config import EmbeddingBackendConfig


class EmbeddingRuntime(ABC):
    """Abstract similarity runtime for embedding-backed semantic operators."""

    @abstractmethod
    def similarity(self, left_text: str, right_text: str) -> float:
        """Return one similarity score in `[0, 1]`-like space."""

    def close(self) -> None:
        """Release backend resources when needed."""


@dataclass
class LocalHashingEmbeddingRuntime(EmbeddingRuntime):
    """Deterministic local hashing encoder runtime."""

    encoder: HashingTextEncoder

    def similarity(self, left_text: str, right_text: str) -> float:
        """Return deterministic local similarity."""
        return float(self.encoder.similarity(left_text, right_text))


class FaissEmbeddingRuntime(EmbeddingRuntime):
    """Optional FAISS-backed local runtime over hashed dense vectors.

    This is still not a production semantic embedding backend. It only provides
    a slightly more structured local runtime skeleton for later operators.
    """

    def __init__(self, *, dim: int) -> None:
        try:
            import faiss  # type: ignore
            import numpy as np
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "FaissEmbeddingRuntime requires local 'faiss' and 'numpy' packages"
            ) from exc

        self._encoder = HashingTextEncoder(dim=max(dim, 1))
        self._faiss = faiss
        self._np = np

    def similarity(self, left_text: str, right_text: str) -> float:
        """Return hashed dense-vector inner-product similarity."""
        left = self._np.asarray(
            [self._encoder.encode_dense(left_text)],
            dtype="float32",
        )
        right = self._np.asarray(
            [self._encoder.encode_dense(right_text)],
            dtype="float32",
        )
        self._faiss.normalize_L2(left)
        self._faiss.normalize_L2(right)
        score_matrix = left @ right.T
        return float(score_matrix[0, 0])


class LocalModelEmbeddingRuntime(EmbeddingRuntime):
    """Skeleton for a future local-model embedding backend."""

    def __init__(self, config: EmbeddingBackendConfig) -> None:
        self._config = config
        raise NotImplementedError("Local model embedding runtime is not implemented yet")

    def similarity(self, left_text: str, right_text: str) -> float:
        raise NotImplementedError("Local model embedding runtime is not implemented yet")


class ApiEmbeddingRuntime(EmbeddingRuntime):
    """Skeleton for a future remote embedding API backend."""

    def __init__(self, config: EmbeddingBackendConfig) -> None:
        self._config = config
        raise NotImplementedError("Remote embedding API runtime is not implemented yet")

    def similarity(self, left_text: str, right_text: str) -> float:
        raise NotImplementedError("Remote embedding API runtime is not implemented yet")


def create_embedding_runtime(config: EmbeddingBackendConfig) -> EmbeddingRuntime:
    """Build one embedding runtime from the internal backend config."""
    backend = str(config.backend or "mock")
    dim = int(config.dimensions or 128)

    if backend in {"mock", "local_hashing", "local_lexical"}:
        return LocalHashingEmbeddingRuntime(encoder=HashingTextEncoder(dim=max(dim, 1)))
    if backend == "faiss":
        return FaissEmbeddingRuntime(dim=dim)
    if backend == "local_model":
        return LocalModelEmbeddingRuntime(config)
    if backend in {"api", "remote_api"}:
        return ApiEmbeddingRuntime(config)
    raise ValueError(f"Unsupported embedding backend: {backend!r}")
