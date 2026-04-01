"""Concurrency helpers for agent-memory workflows."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, List, Sequence, TypeVar


_ItemT = TypeVar("_ItemT")
_ResultT = TypeVar("_ResultT")


async def _cancel_pending_tasks(tasks: Sequence[asyncio.Task[object]]) -> None:
    pending = [task for task in tasks if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


def _validate_concurrency(*, concurrency: int) -> None:
    if int(concurrency) <= 0:
        raise ValueError("concurrency must be > 0")


async def amap_ordered_bounded(
    *,
    items: Sequence[_ItemT],
    concurrency: int,
    worker: Callable[[int, _ItemT], Awaitable[_ResultT]],
) -> List[_ResultT]:
    """Run ``worker`` over ``items`` with bounded concurrency and stable output order."""
    _validate_concurrency(concurrency=concurrency)
    if not items:
        return []

    effective_concurrency = min(int(concurrency), len(items))
    semaphore = asyncio.Semaphore(effective_concurrency)
    results: list[_ResultT | None] = [None] * len(items)

    async def _run_one(index: int, item: _ItemT) -> None:
        async with semaphore:
            results[index] = await worker(index, item)

    tasks = [
        asyncio.create_task(_run_one(index, item))
        for index, item in enumerate(items)
    ]
    try:
        await asyncio.gather(*tasks)
    except Exception:
        await _cancel_pending_tasks(tasks)
        raise

    output: list[_ResultT] = []
    for index, value in enumerate(results):
        if value is None:
            raise RuntimeError(
                "amap_ordered_bounded produced incomplete results at "
                f"index={index}"
            )
        output.append(value)
    return output


async def amap_grouped_serial_bounded(
    *,
    items: Sequence[_ItemT],
    group_key: Callable[[int, _ItemT], str],
    concurrency: int,
    worker: Callable[[int, _ItemT], Awaitable[_ResultT]],
) -> List[_ResultT]:
    """Run grouped tasks in parallel while preserving serial order inside each group."""
    _validate_concurrency(concurrency=concurrency)
    if not items:
        return []

    grouped_items: dict[str, list[tuple[int, _ItemT]]] = {}
    for index, item in enumerate(items):
        key = str(group_key(index, item))
        grouped_items.setdefault(key, []).append((index, item))

    effective_concurrency = min(int(concurrency), len(grouped_items))
    semaphore = asyncio.Semaphore(effective_concurrency)
    results: list[_ResultT | None] = [None] * len(items)

    async def _run_group(rows: Sequence[tuple[int, _ItemT]]) -> None:
        async with semaphore:
            for index, item in rows:
                results[index] = await worker(index, item)

    tasks = [
        asyncio.create_task(_run_group(rows))
        for rows in grouped_items.values()
    ]
    try:
        await asyncio.gather(*tasks)
    except Exception:
        await _cancel_pending_tasks(tasks)
        raise

    output: list[_ResultT] = []
    for index, value in enumerate(results):
        if value is None:
            raise RuntimeError(
                "amap_grouped_serial_bounded produced incomplete results at "
                f"index={index}"
            )
        output.append(value)
    return output
