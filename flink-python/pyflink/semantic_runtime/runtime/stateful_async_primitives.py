"""Shared async primitives for stateful semantic operators.

This module provides small, explicit utilities reused by stateful operators:

1. request basis envelope for async dispatch/apply,
2. stale-result guard checks,
3. per-key single-flight metadata management on keyed meta dict.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class AsyncRequestBasis:
    """Immutable request basis carried across async dispatch/apply."""

    request_id: str
    key: str
    scope_epoch: int
    state_version: int
    trigger_reason: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize basis as a plain dictionary."""
        return {
            "request_id": self.request_id,
            "key": self.key,
            "scope_epoch": int(self.scope_epoch),
            "state_version": int(self.state_version),
            "trigger_reason": self.trigger_reason,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "AsyncRequestBasis":
        """Build one request basis from a dictionary payload."""
        return cls(
            request_id=str(payload["request_id"]),
            key=str(payload["key"]),
            scope_epoch=int(payload["scope_epoch"]),
            state_version=int(payload["state_version"]),
            trigger_reason=str(payload.get("trigger_reason", "")),
        )


@dataclass(frozen=True)
class AsyncResultEnvelope:
    """Async result envelope carrying one immutable request basis + payload."""

    basis: AsyncRequestBasis
    payload: Dict[str, Any]

    @classmethod
    def from_basis(
        cls,
        basis: AsyncRequestBasis,
        payload: Optional[Dict[str, Any]] = None,
    ) -> "AsyncResultEnvelope":
        """Build one envelope from request basis and payload."""
        return cls(
            basis=basis,
            payload=dict(payload) if payload is not None else {},
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize one envelope as a dictionary."""
        return {
            "basis": self.basis.to_dict(),
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "AsyncResultEnvelope":
        """Deserialize one envelope from a dictionary."""
        raw_basis = value.get("basis")
        if not isinstance(raw_basis, dict):
            raise RuntimeError("async result envelope requires 'basis' dict")
        raw_payload = value.get("payload", {})
        if not isinstance(raw_payload, dict):
            raise RuntimeError("async result envelope requires 'payload' dict")
        return cls(
            basis=AsyncRequestBasis.from_dict(raw_basis),
            payload=dict(raw_payload),
        )


class AsyncApplyGuard:
    """Stale-result guard for keyed apply stages."""

    @staticmethod
    def is_stale(
        *,
        basis: AsyncRequestBasis,
        current_scope_epoch: int,
        current_state_version: int,
        enforce_state_version: bool,
    ) -> bool:
        """Return whether one async result is stale for current keyed state."""
        if int(current_scope_epoch) != int(basis.scope_epoch):
            return True
        if enforce_state_version and int(current_state_version) != int(basis.state_version):
            return True
        return False


def _lane_flag_key(lane: str) -> str:
    return f"_single_flight_{lane}_in_flight"


def _lane_basis_key(lane: str) -> str:
    return f"_single_flight_{lane}_basis"


def single_flight_is_in_flight(meta: Dict[str, Any], lane: str) -> bool:
    """Return whether one lane currently has an in-flight request."""
    return bool(meta.get(_lane_flag_key(lane), False))


def single_flight_get_basis(
    meta: Dict[str, Any],
    lane: str,
) -> Optional[AsyncRequestBasis]:
    """Return the current lane basis if present."""
    payload = meta.get(_lane_basis_key(lane))
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise RuntimeError(f"single-flight basis for lane={lane!r} must be a dict")
    return AsyncRequestBasis.from_dict(payload)


def single_flight_begin(
    meta: Dict[str, Any],
    lane: str,
    basis: AsyncRequestBasis,
) -> None:
    """Begin one single-flight request for the lane."""
    if single_flight_is_in_flight(meta, lane):
        raise RuntimeError(f"single-flight lane={lane!r} already has an in-flight request")
    meta[_lane_flag_key(lane)] = True
    meta[_lane_basis_key(lane)] = basis.to_dict()


def single_flight_complete(
    meta: Dict[str, Any],
    lane: str,
    request_id: str,
) -> AsyncRequestBasis:
    """Complete one single-flight request and return its basis."""
    if not single_flight_is_in_flight(meta, lane):
        raise RuntimeError(f"single-flight lane={lane!r} has no in-flight request to complete")
    basis = single_flight_get_basis(meta, lane)
    if basis is None:
        raise RuntimeError(f"single-flight lane={lane!r} missing basis metadata")
    if basis.request_id != str(request_id):
        raise RuntimeError(
            f"single-flight lane={lane!r} completion request_id mismatch: "
            f"expected={basis.request_id!r}, actual={request_id!r}"
        )
    meta.pop(_lane_flag_key(lane), None)
    meta.pop(_lane_basis_key(lane), None)
    return basis
