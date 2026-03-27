"""Shared executor helper for stateful semantic operators.

This module centralizes a tiny mechanical pattern used by multiple operators:
create one thread-pool executor lazily and reuse it for subsequent async work.
"""

from __future__ import annotations

import concurrent.futures


def ensure_thread_pool_executor(
    executor: concurrent.futures.ThreadPoolExecutor | None,
    *,
    max_workers: int,
    thread_name_prefix: str,
) -> concurrent.futures.ThreadPoolExecutor:
    """Return a reusable thread-pool executor, creating it lazily.

    Args:
        executor: Existing executor instance or ``None``.
        max_workers: Maximum worker threads for a newly created pool.
        thread_name_prefix: Thread name prefix for a newly created pool.

    Returns:
        A usable ``ThreadPoolExecutor`` instance.
    """
    if executor is not None:
        return executor
    return concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix,
    )

