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
backends used by ``sem_search`` (``cts_retrieve``) and ``sem_lookup_join``
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

    # Wire into cts_retrieve / sem_search:
    retrieve_cfg = CtsRetrieveConfig(search_backend=MilvusSearchBackend(...))
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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


class ExternalSearchBackend(abc.ABC):
    """Abstract interface for pluggable external search backends.

    Concrete implementations must provide:

    - ``async search(query, top_k, **kwargs) -> List[SearchResult]``
    - ``open()`` / ``close()`` for lifecycle management (optional)

    The ``search`` method is called from the async bridge worker when
    ``cts_retrieve`` / ``sem_search`` encounters a cache miss.
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


# ---------------------------------------------------------------------------
# TODO: Concrete implementations (later phase)
# ---------------------------------------------------------------------------
# - MilvusSearchBackend
# - QdrantSearchBackend
# - ElasticsearchSearchBackend
# - PineconeSearchBackend
# - MockSearchBackend (for testing)
# ---------------------------------------------------------------------------

