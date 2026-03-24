"""Shared window-owned materialization.

This module converts raw event streams into ``WindowSnapshot`` streams for
window-owned operators. Standard windows reuse native Flink window APIs when
possible. Semantic windows reuse ``SemWindowFunction``.

This module is internal runtime infrastructure.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from pyflink.common import Time, Types
from pyflink.datastream import DataStream
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import (
    ContinuousEventTimeTrigger,
    ContinuousProcessingTimeTrigger,
    EventTimeSessionWindows,
    SlidingEventTimeWindows,
    SlidingProcessingTimeWindows,
    TimeWindow,
    TumblingEventTimeWindows,
    TumblingProcessingTimeWindows,
    ProcessingTimeSessionWindows,
)

from pyflink.semantic_runtime.runtime.event_model import WindowSnapshot, simple_key_selector
from pyflink.semantic_runtime.runtime.pushdown.common import parse_json_or_passthrough
from pyflink.semantic_runtime.sem_spec import TriggerPolicy


class _EventRecordNormalizer(MapFunction):
    """Parse JSON strings and require dict-shaped event records."""

    def __init__(self, *, operator_name: str) -> None:
        self._operator_name = operator_name

    def map(self, value: Any) -> Any:
        """Normalize one event record into a dict."""
        record = parse_json_or_passthrough(value, operator_name=self._operator_name)
        if not isinstance(record, dict):
            raise ValueError(f"{self._operator_name} requires dict-shaped event input")
        if "key" not in record:
            raise ValueError(f"{self._operator_name} requires event input with non-empty key")
        return record


class _WindowSnapshotBuilder(ProcessWindowFunction):
    """Convert one native keyed window into one WindowSnapshot dict."""

    def __init__(self, *, window_kind: str, time_basis: str) -> None:
        self._window_kind = window_kind
        self._time_basis = time_basis

    def process(self, key, context, elements):
        """Emit one deterministic WindowSnapshot for the current native window."""
        event_rows = list(elements)
        if not event_rows:
            return
        window = context.window()
        snapshot = WindowSnapshot(
            key=str(key),
            window_id=_window_id(window),
            events=event_rows,
            event_count=len(event_rows),
            open_time_ms=_window_start(window),
            close_time_ms=_window_end(window),
            trigger_reason=_native_trigger_reason(context),
            metadata={
                "window_kind": self._window_kind,
                "time_basis": self._time_basis,
            },
        )
        yield snapshot.to_dict()


def _window_start(window: Any) -> int:
    """Return the deterministic start timestamp for one native window."""
    start = getattr(window, "start", None)
    if start is None:
        raise TypeError(f"Unsupported native window type {type(window)!r}")
    return int(start)


def _window_end(window: Any) -> int:
    """Return the deterministic end timestamp for one native window."""
    end = getattr(window, "end", None)
    if end is None:
        raise TypeError(f"Unsupported native window type {type(window)!r}")
    return int(end)


def _window_id(window: Any) -> str:
    """Build one deterministic window id shared across aligned streams."""
    if isinstance(window, TimeWindow):
        return f"{int(window.start)}:{int(window.end)}"
    start = getattr(window, "start", None)
    end = getattr(window, "end", None)
    if start is not None and end is not None:
        return f"{int(start)}:{int(end)}"
    raise TypeError(f"Unsupported native window type {type(window)!r}")


def _native_trigger_reason(context: ProcessWindowFunction.Context) -> str:
    """Return one coarse trigger reason for native-window emissions."""
    return "native_window"


def _build_native_window_assigner(scope_policy: Any) -> Any:
    """Build one native Flink window assigner from an internal scope policy."""
    window_kind = scope_policy.window_kind
    time_basis = scope_policy.time_basis
    window_size_ms = int(scope_policy.window_size_ms or 0)

    if window_kind == "tumbling":
        if time_basis == "processing":
            return TumblingProcessingTimeWindows.of(Time.milliseconds(window_size_ms))
        return TumblingEventTimeWindows.of(Time.milliseconds(window_size_ms))

    if window_kind == "sliding":
        slide_ms = int(scope_policy.slide_ms or 0)
        if time_basis == "processing":
            return SlidingProcessingTimeWindows.of(
                Time.milliseconds(window_size_ms),
                Time.milliseconds(slide_ms),
            )
        return SlidingEventTimeWindows.of(
            Time.milliseconds(window_size_ms),
            Time.milliseconds(slide_ms),
        )

    if window_kind == "session":
        gap_ms = int(scope_policy.session_gap_ms or 0)
        if time_basis == "processing":
            return ProcessingTimeSessionWindows.with_gap(Time.milliseconds(gap_ms))
        return EventTimeSessionWindows.with_gap(Time.milliseconds(gap_ms))

    raise ValueError(f"Unsupported native window kind {window_kind!r}")


def _apply_native_window_trigger(windowed_stream: Any, trigger_policy: TriggerPolicy, *, time_basis: str):
    """Apply a native Flink trigger when the policy is supported."""
    if trigger_policy.mode == "on_scope_close":
        return windowed_stream
    if trigger_policy.mode == "periodic":
        if time_basis == "processing":
            return windowed_stream.trigger(
                ContinuousProcessingTimeTrigger.of(Time.milliseconds(int(trigger_policy.interval_ms or 0)))
            )
        return windowed_stream.trigger(
            ContinuousEventTimeTrigger.of(Time.milliseconds(int(trigger_policy.interval_ms or 0)))
        )
    raise NotImplementedError(
        f"Shared native window materialization does not support trigger mode {trigger_policy.mode!r}"
    )


def materialize_window_stream(
    input_stream: DataStream,
    *,
    scope_policy: Any,
    trigger_policy: TriggerPolicy,
    runtime_config: Any,
    operator_name: str,
) -> DataStream:
    """Materialize one raw event stream into a WindowSnapshot stream.

    When ``scope_policy.window_kind`` is ``None``, the input stream is assumed
    to already contain ``WindowSnapshot`` records and is returned unchanged.
    """
    if scope_policy.window_kind is None:
        return input_stream

    normalized_stream = input_stream.map(
        _EventRecordNormalizer(operator_name=operator_name),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    if scope_policy.window_kind == "semantic":
        from pyflink.semantic_runtime.operators.stateful.sem_window import SemWindowFunction

        sem_window_config = replace(
            runtime_config.get_window_config(),
            boundary_flag=scope_policy.boundary_flag,
        )
        return normalized_stream.key_by(simple_key_selector).process(
            SemWindowFunction(sem_window_config),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )

    assigner = _build_native_window_assigner(scope_policy)
    windowed = normalized_stream.key_by(simple_key_selector).window(assigner)
    windowed = _apply_native_window_trigger(
        windowed,
        trigger_policy,
        time_basis=scope_policy.time_basis,
    )
    return windowed.process(
        _WindowSnapshotBuilder(
            window_kind=str(scope_policy.window_kind),
            time_basis=str(scope_policy.time_basis),
        ),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

