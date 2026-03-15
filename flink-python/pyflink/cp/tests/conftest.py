"""
Shared setup for CP semantic operator tests.

Ensures ``pyflink.cp`` is importable by symlinking our source tree into the
installed pyflink package (avoids shadowing PyFlink 2.2.0 with the local
2.3-SNAPSHOT source tree).

Usage — at the top of each test file, BEFORE any ``pyflink.cp`` import::

    import os, pathlib, pyflink as _pf
    _cp_src = pathlib.Path(__file__).resolve().parents[1]
    _cp_dst = pathlib.Path(_pf.__file__).parent / "cp"
    if not _cp_dst.exists():
        os.symlink(_cp_src, _cp_dst)

Or simply run this module once to create the symlink::

    python flink-python/pyflink/cp/tests/conftest.py
"""

import os
import pathlib

_cp_src = pathlib.Path(__file__).resolve().parents[1]   # flink-python/pyflink/cp

import pyflink as _pf

_cp_dst = pathlib.Path(_pf.__file__).parent / "cp"
if not _cp_dst.exists():
    os.symlink(_cp_src, _cp_dst)


if __name__ == "__main__":
    print(f"Symlink: {_cp_dst} → {_cp_src}")
    print("pyflink.cp is importable." if _cp_dst.exists() else "ERROR: symlink failed")

