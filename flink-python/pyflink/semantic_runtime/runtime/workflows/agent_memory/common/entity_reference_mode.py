"""Entity-reference and drift-policy constants for agent-memory workflows."""

from __future__ import annotations

ENTITY_REFERENCE_MODE_NAME = "name"
ENTITY_REFERENCE_MODE_INDEX = "index"
VALID_ENTITY_REFERENCE_MODES = frozenset(
    {
        ENTITY_REFERENCE_MODE_NAME,
        ENTITY_REFERENCE_MODE_INDEX,
    }
)

DRIFT_POLICY_FAIL_FAST = "fail_fast"
DRIFT_POLICY_UPSTREAM_COMPATIBLE = "upstream_compatible"
VALID_DRIFT_POLICIES = frozenset(
    {
        DRIFT_POLICY_FAIL_FAST,
        DRIFT_POLICY_UPSTREAM_COMPATIBLE,
    }
)


def normalize_entity_reference_mode(value: str, *, field_name: str) -> str:
    """Validate one entity-reference mode string."""
    normalized = str(value).strip()
    if normalized not in VALID_ENTITY_REFERENCE_MODES:
        raise ValueError(
            f"{field_name} must be one of {sorted(VALID_ENTITY_REFERENCE_MODES)!r}"
        )
    return normalized


def normalize_drift_policy(value: str, *, field_name: str) -> str:
    """Validate one drift-policy string."""
    normalized = str(value).strip()
    if normalized not in VALID_DRIFT_POLICIES:
        raise ValueError(
            f"{field_name} must be one of {sorted(VALID_DRIFT_POLICIES)!r}"
        )
    return normalized


def uses_index_entity_reference_mode(mode: str) -> bool:
    """Return whether one mode uses index-based endpoint references."""
    return (
        normalize_entity_reference_mode(
            mode,
            field_name="mode",
        )
        == ENTITY_REFERENCE_MODE_INDEX
    )


def is_upstream_compatible_drift_policy(policy: str) -> bool:
    """Return whether one drift policy follows upstream-compatible behavior."""
    return (
        normalize_drift_policy(
            policy,
            field_name="policy",
        )
        == DRIFT_POLICY_UPSTREAM_COMPATIBLE
    )
