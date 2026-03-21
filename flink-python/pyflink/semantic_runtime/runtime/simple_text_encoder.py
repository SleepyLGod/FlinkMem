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
Lightweight local text encoder for demo retrieval and scoring paths.

This module intentionally provides a tiny, dependency-light encoder:

- tokenise text
- map tokens into a fixed hashing space
- L2-normalise the resulting vector

It is useful for:

- local cache matching in ``sem_search`` / ``sem_search``
- FAISS demo indexing without requiring a real embedding model

It is not a production semantic encoder.
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from typing import Dict, List


def tokenize_text(text: str) -> List[str]:
    return [tok for tok in re.findall(r"[A-Za-z0-9_]+", str(text).lower()) if tok]


def _stable_bucket(token: str, dim: int) -> int:
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "little") % dim


@dataclass
class HashingTextEncoder:
    """Small deterministic hashing encoder.

    The encoder keeps the implementation local and deterministic so tests and
    demos do not depend on a remote embedding API.
    """

    dim: int = 128

    def encode_sparse(self, text: str) -> Dict[int, float]:
        buckets: Dict[int, float] = {}
        for token in tokenize_text(text):
            idx = _stable_bucket(token, self.dim)
            buckets[idx] = buckets.get(idx, 0.0) + 1.0

        if not buckets:
            return {}

        norm = math.sqrt(sum(v * v for v in buckets.values()))
        if norm <= 0:
            return {}
        return {idx: value / norm for idx, value in buckets.items()}

    def encode_dense(self, text: str) -> List[float]:
        sparse = self.encode_sparse(text)
        dense = [0.0] * self.dim
        for idx, value in sparse.items():
            dense[idx] = value
        return dense

    def similarity(self, left: str, right: str) -> float:
        left_sparse = self.encode_sparse(left)
        right_sparse = self.encode_sparse(right)
        if not left_sparse or not right_sparse:
            return 0.0
        if len(left_sparse) > len(right_sparse):
            left_sparse, right_sparse = right_sparse, left_sparse
        return sum(value * right_sparse.get(idx, 0.0) for idx, value in left_sparse.items())
