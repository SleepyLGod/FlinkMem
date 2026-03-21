# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
External search backend — abstract interface stub.

This module defines the pluggable interface for external vector/search
backends used by ``sem_search`` (``sem_search``) and ``sem_lookup_join``
when local cache misses occur.

**Status: interface stub — no concrete implementations yet.**

The system must support real external search backends for the case where:

- continuous input arrives,
- each input must retrieve relevant documents/items from an attached
  external database (vector DB, full-text search, hybrid, etc.),
- retrieved items are brought back into the stream,
- semantic join / ranking / answer synthesis is applied downstream.

The current real tests validate async retrieval control flow, merge-back
behaviour, and answer path integration — but they do NOT yet validate
a real vector database, a real embedding pipeline, or real search latency.

Concrete implementations (e.g. Milvus, Qdrant, Elasticsearch, Pinecone)
will be added in a later phase once the surrounding application
architecture is clearer.

Usage (future)::

    class MilvusSearchBackend(ExternalSearchBackend):
        async def search(self, query, top_k, **kwargs):
            # call milvus client
            return [{"candidate_id": ..., "text": ..., "score": ...}, ...]

    # Wire into sem_search / sem_search:
    retrieve_cfg = SemSearchConfig(search_backend=MilvusSearchBackend(...))
"""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.semantic_runtime.runtime.async_bridge import AsyncResult, AsyncWorkItem
from pyflink.semantic_runtime.runtime.simple_text_encoder import (
    HashingTextEncoder,
    tokenize_text,
)


@dataclass
class SearchResult:
    """A single search result returned by an external backend.

    Attributes
    ----------
    candidate_id : str
        Unique identifier for the retrieved document/item.
    text : str
        The content/payload of the result.
    score : float
        Relevance score (higher = more relevant).
    metadata : dict
        Backend-specific metadata (e.g. distance, collection name).
    """
    candidate_id: str = ""
    text: str = ""
    score: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "text": self.text,
            "score": self.score,
            "metadata": self.metadata,
        }

    @classmethod
    def from_any(cls, value: Any) -> "SearchResult":
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            metadata = dict(value.get("metadata", {}))
            return cls(
                candidate_id=str(value.get("candidate_id", value.get("id", ""))),
                text=str(value.get("text", value.get("content", ""))),
                score=float(value.get("score", 0.0)),
                metadata=metadata,
            )
        raise TypeError(f"Unsupported SearchResult source: {type(value).__name__}")


class ExternalSearchBackend(abc.ABC):
    """Abstract interface for pluggable external search backends.

    Concrete implementations must provide:

    - ``async search(query, top_k, **kwargs) -> List[SearchResult]``
    - ``open()`` / ``close()`` for lifecycle management (optional)

    The ``search`` method is called from the async bridge worker when
    ``sem_search`` / ``sem_search`` encounters a cache miss.
    """

    def open(self) -> None:
        """Initialize connections, clients, etc.  Called once on startup."""

    def close(self) -> None:
        """Release resources.  Called on shutdown."""

    @abc.abstractmethod
    async def search(
        self,
        query: str,
        top_k: int = 10,
        **kwargs: Any,
    ) -> List[SearchResult]:
        """Execute a search query against the external backend.

        Parameters
        ----------
        query : str
            The search query (natural language or embedding vector).
        top_k : int
            Maximum number of results to return.
        **kwargs
            Backend-specific parameters (filters, namespaces, etc.).

        Returns
        -------
        list[SearchResult]
            Ranked list of search results, highest relevance first.
        """
        ...


def lexical_search_score(query: str, text: str) -> float:
    """Deterministic lexical relevance surrogate for tests/mock backends."""
    q = set(tokenize_text(query))
    t = set(tokenize_text(text))
    if not q or not t:
        return 0.0
    overlap = len(q & t)
    return overlap / max(len(q), 1)


class MockSearchBackend(ExternalSearchBackend):
    """Deterministic external search backend for tests and local workflow runs.

    Supports:
    - fixed corpus search via lexical overlap
    - query-conditioned rule hits
    - optional artificial delay
    """

    def __init__(
        self,
        *,
        corpus: Optional[List[Any]] = None,
        query_rules: Optional[Dict[str, List[Any]]] = None,
        default_results: Optional[List[Any]] = None,
        delay_s: float = 0.0,
    ) -> None:
        self._corpus = [SearchResult.from_any(v) for v in (corpus or [])]
        self._query_rules = {
            str(k).lower(): [SearchResult.from_any(v) for v in values]
            for k, values in (query_rules or {}).items()
        }
        self._default_results = [SearchResult.from_any(v) for v in (default_results or [])]
        self._delay_s = delay_s
        self._opened = False

    @property
    def opened(self) -> bool:
        return self._opened

    def open(self) -> None:
        self._opened = True

    def close(self) -> None:
        self._opened = False

    async def search(
        self,
        query: str,
        top_k: int = 10,
        **kwargs: Any,
    ) -> List[SearchResult]:
        if self._delay_s > 0:
            await asyncio.sleep(self._delay_s)

        query_l = str(query).lower()
        matched: Dict[str, SearchResult] = {}

        for token, results in self._query_rules.items():
            if token and token in query_l:
                for result in results:
                    matched[result.candidate_id] = SearchResult(
                        candidate_id=result.candidate_id,
                        text=result.text,
                        score=result.score,
                        metadata=dict(result.metadata),
                    )

        if matched:
            ranked = list(matched.values())
            ranked.sort(key=lambda r: r.score, reverse=True)
            return ranked[:top_k]

        lexical_hits: List[SearchResult] = []
        for result in self._corpus:
            score = lexical_search_score(query_l, result.text)
            if score > 0.0:
                lexical_hits.append(
                    SearchResult(
                        candidate_id=result.candidate_id,
                        text=result.text,
                        score=max(result.score, score),
                        metadata=dict(result.metadata),
                    )
                )
        if lexical_hits:
            lexical_hits.sort(key=lambda r: r.score, reverse=True)
            return lexical_hits[:top_k]

        return self._default_results[:top_k]


class FaissSearchBackend(ExternalSearchBackend):
    """Very simple FAISS-backed demo search backend.

    This backend is intentionally minimal:
    - it builds a FAISS ``IndexFlatIP`` over deterministic hashed text vectors
    - it does not depend on an external embedding model
    - it is suitable only as a lightweight demo / local experiment backend

    Important:
    - requires local ``faiss`` and ``numpy`` installation
    - this is not a production semantic retrieval backend
    - for real use, replace the hashed vectoriser with a proper embedding model
    """

    def __init__(
        self,
        corpus: List[Any],
        *,
        dim: int = 128,
        delay_s: float = 0.0,
    ) -> None:
        self._corpus = [SearchResult.from_any(v) for v in corpus]
        self._dim = dim
        self._delay_s = delay_s
        self._faiss = None
        self._np = None
        self._index = None
        self._id_to_result: List[SearchResult] = []
        self._encoder = HashingTextEncoder(dim=dim)

    def open(self) -> None:
        try:
            import faiss  # type: ignore
            import numpy as np  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "FaissSearchBackend requires local 'faiss' and 'numpy' packages"
            ) from exc

        self._faiss = faiss
        self._np = np
        self._index = faiss.IndexFlatIP(self._dim)
        self._id_to_result = list(self._corpus)
        if not self._corpus:
            return
        vectors = np.stack(
            [self._encoder.encode_dense(result.text) for result in self._corpus],
            axis=0,
        ).astype("float32")
        self._index.add(vectors)

    def close(self) -> None:
        self._index = None
        self._id_to_result = []

    async def search(
        self,
        query: str,
        top_k: int = 10,
        **kwargs: Any,
    ) -> List[SearchResult]:
        if self._delay_s > 0:
            await asyncio.sleep(self._delay_s)
        if self._index is None or self._faiss is None or self._np is None:
            self.open()
        assert self._np is not None
        if self._index is None or not self._id_to_result:
            return []

        query_vec = self._np.asarray(
            self._encoder.encode_dense(query), dtype="float32"
        ).reshape(1, -1)
        distances, indices = self._index.search(query_vec, min(top_k, len(self._id_to_result)))

        results: List[SearchResult] = []
        for score, idx in zip(distances[0], indices[0]):
            if idx < 0 or idx >= len(self._id_to_result):
                continue
            base = self._id_to_result[int(idx)]
            results.append(
                SearchResult(
                    candidate_id=base.candidate_id,
                    text=base.text,
                    score=float(score),
                    metadata=dict(base.metadata),
                )
            )
        return results


class SearchBackendAsyncFn(AsyncFunction):
    """Async bridge worker that delegates cache misses to an ExternalSearchBackend."""

    def __init__(self, backend: ExternalSearchBackend) -> None:
        self._backend = backend

    def open(self, runtime_context: RuntimeContext) -> None:
        self._backend.open()

    def close(self) -> None:
        self._backend.close()

    async def async_invoke(self, value):
        work = AsyncWorkItem.from_dict(value)
        payload = work.payload or {}
        query = str(payload.get("query", ""))
        max_candidates = int(payload.get("max_candidates", 10))
        try:
            results = await self._backend.search(
                query,
                top_k=max_candidates,
                event_seq_id=int(payload.get("event_seq_id", 0)),
            )
            candidates = []
            for result in results:
                sr = SearchResult.from_any(result)
                candidates.append(
                    {
                        "candidate_id": sr.candidate_id,
                        "content": sr.text,
                        "text": sr.text,
                        "score": float(sr.score),
                        "metadata": dict(sr.metadata),
                    }
                )
            return [
                AsyncResult(
                    key=work.key,
                    task_type="retrieve",
                    request_id=work.request_id,
                    success=True,
                    result={
                        "query": query,
                        "event_seq_id": int(payload.get("event_seq_id", 0)),
                        "candidates": candidates,
                    },
                ).to_dict()
            ]
        except Exception as exc:
            return [
                AsyncResult(
                    key=work.key,
                    task_type="retrieve",
                    request_id=work.request_id,
                    success=False,
                    error=f"retrieve_backend_error: {exc}",
                ).to_dict()
            ]

    def timeout(self, value):
        work = AsyncWorkItem.from_dict(value)
        return [
            AsyncResult(
                key=work.key,
                task_type="retrieve",
                request_id=work.request_id,
                success=False,
                error="retrieve_timeout",
            ).to_dict()
        ]


# ---------------------------------------------------------------------------
# TODO: Concrete implementations (later phase)
# ---------------------------------------------------------------------------
# - MilvusSearchBackend
# - QdrantSearchBackend
# - ElasticsearchSearchBackend
# - PineconeSearchBackend
# - Real vector / hybrid search backends
# ---------------------------------------------------------------------------
