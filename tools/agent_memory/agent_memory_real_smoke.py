#!/usr/bin/env python3
"""Thin CLI wrapper for agent-memory real-backend smoke runner."""

from __future__ import annotations

import pathlib
import sys

def _bootstrap_python_path() -> pathlib.Path:
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    pyflink_src_root = repo_root / "flink-python"
    if str(pyflink_src_root) not in sys.path:
        sys.path.insert(0, str(pyflink_src_root))
    return repo_root


_bootstrap_python_path()

from pyflink.semantic_runtime.runtime.workflows.agent_memory.runtime.smoke_runner import (  # noqa: E402
    main,
)


if __name__ == "__main__":
    main()
