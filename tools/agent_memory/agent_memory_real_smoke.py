#!/usr/bin/env python3
"""Thin CLI wrapper for agent-memory real-backend smoke runner."""

from __future__ import annotations

import os
import pathlib
import sys

import pyflink as _pf


def _bootstrap_semantic_runtime_path() -> pathlib.Path:
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    sem_runtime_src = repo_root / "flink-python" / "pyflink" / "semantic_runtime"
    sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
    if not sem_runtime_dst.exists():
        os.symlink(sem_runtime_src, sem_runtime_dst)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


_bootstrap_semantic_runtime_path()

from pyflink.semantic_runtime.runtime.workflows.agent_memory.runtime.smoke_runner import (  # noqa: E402
    main,
)


if __name__ == "__main__":
    main()
