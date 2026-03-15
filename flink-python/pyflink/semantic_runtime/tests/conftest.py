"""
Shared setup for CP semantic operator tests.

Ensures ``pyflink.semantic_runtime`` is importable by symlinking our source tree into the
installed pyflink package (avoids shadowing PyFlink 2.2.0 with the local
2.3-SNAPSHOT source tree).

Usage — at the top of each test file, BEFORE any ``pyflink.semantic_runtime`` import::

    import os, pathlib, pyflink as _pf
    _sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
    _sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
    if not _sem_runtime_dst.exists():
        os.symlink(_sem_runtime_src, _sem_runtime_dst)

Or simply run this module once to create the symlink::

    python flink-python/pyflink/semantic_runtime/tests/conftest.py
"""

import os
import pathlib

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]   # flink-python/pyflink/semantic_runtime

import pyflink as _pf

_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)


if __name__ == "__main__":
    print(f"Symlink: {_sem_runtime_dst} → {_sem_runtime_src}")
    print("pyflink.semantic_runtime is importable." if _sem_runtime_dst.exists() else "ERROR: symlink failed")

