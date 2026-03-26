#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0.

"""Focused tests for typed RuntimeConfig and runtime bundle hydration."""

from __future__ import annotations

import os
import pathlib

import pyflink as _pf
import pytest

_SEM_RUNTIME_SRC = pathlib.Path(__file__).resolve().parents[1]
_SEM_RUNTIME_DST = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _SEM_RUNTIME_DST.exists():
    os.symlink(_SEM_RUNTIME_SRC, _SEM_RUNTIME_DST)

from pyflink.semantic_runtime.runtime_config import RuntimeConfig


class TestRuntimeConfig:
    def test_defaults(self):
        cfg = RuntimeConfig()
        assert cfg.defaults.ttl_seconds == 3600
        assert cfg.llm.backend == "mock"
        assert cfg.operators == {}

    def test_from_dict(self):
        cfg = RuntimeConfig.from_dict(
            {
                "defaults": {"ttl_seconds": 7200},
                "llm": {"backend": "openai", "model": "gpt-4"},
                "operators": {
                    "sem_topk": {
                        "query_spec": {"k": 5},
                        "kernel": {},
                    },
                },
            }
        )
        assert cfg.defaults.ttl_seconds == 7200
        assert cfg.llm.backend == "openai"
        assert cfg.llm.model == "gpt-4"
        assert cfg.operators["sem_topk"]["query_spec"]["k"] == 5

    def test_typed_topk_query_spec_and_kernel_from_nested_config(self):
        cfg = RuntimeConfig.from_dict(
            {
                "defaults": {"ttl_seconds": 900, "overflow_policy": "drop_newest"},
                "operators": {
                    "sem_topk": {
                        "query_spec": {
                            "semantic": {
                                "instruction": "rank weather days",
                                "output_mode": "score",
                            },
                            "k": 5,
                            "ranking_method": "pointwise",
                            "scope_policy": {"max_candidates": 50},
                        },
                        "kernel": {
                            "score_field": "similarity",
                            "recompute_interval_ms": 2000,
                            "scorer_backend": "llm",
                            "overflow_policy": "drop_newest",
                        },
                    }
                },
            }
        )
        spec = cfg.get_topk_query_spec()
        kernel = cfg.get_topk_kernel_config()
        assert spec.k == 5
        assert spec.scope_policy.ttl_seconds == 900
        assert kernel.score_field == "similarity"
        assert kernel.recompute_interval_ms == 2000
        assert kernel.overflow_policy.value == "drop_newest"

    def test_groupby_runtime_bundle_resolves_lowering(self):
        cfg = RuntimeConfig.from_dict(
            {
                "operators": {
                    "sem_groupby": {
                        "query_spec": {
                            "semantic": {
                                "instruction": "label records",
                                "output_mode": "label",
                            },
                        }
                    }
                }
            }
        )
        bundle = cfg.resolve_groupby_runtime_bundle(input_kind="window_snapshot")
        assert bundle.lowering_plan.lowering_kind == "native_runtime"
        assert bundle.query_spec.semantic.backend == "hybrid"

    def test_groupby_runtime_bundle_reset_per_scope_lowers_to_classical_groupby(self):
        cfg = RuntimeConfig.from_dict(
            {
                "operators": {
                    "sem_groupby": {
                        "query_spec": {
                            "semantic": {
                                "instruction": "label records",
                                "output_mode": "label",
                            },
                        },
                        "kernel": {
                            "persistence_policy": "reset_per_scope",
                        },
                    }
                }
            }
        )
        bundle = cfg.resolve_groupby_runtime_bundle(input_kind="window_snapshot")
        assert bundle.lowering_plan.lowering_kind == "derived_attribute_then_classical"
        assert bundle.lowering_plan.classical_operator == "groupby"

    def test_agg_runtime_bundle_preserves_native_reduce_view(self):
        cfg = RuntimeConfig.from_dict(
            {
                "operators": {
                    "sem_agg": {
                        "query_spec": {
                            "agg_method": "summarize",
                        },
                        "kernel": {
                            "max_buffer_events": 12,
                        },
                    }
                }
            }
        )
        bundle = cfg.resolve_agg_runtime_bundle(input_kind="window_snapshot")
        assert bundle.query_spec.agg_method == "summarize"
        assert bundle.kernel_config.max_buffer_events == 12
        assert bundle.lowering_plan.lowering_kind == "native_runtime"

    def test_public_execution_path_is_rejected(self):
        cfg = RuntimeConfig.from_dict(
            {
                "operators": {
                    "sem_topk": {
                        "query_spec": {
                            "execution_path": "window_owned",
                        }
                    }
                }
            }
        )
        with pytest.raises(ValueError, match="no longer accepts public execution_path"):
            cfg.get_topk_query_spec()

    def test_groupby_query_backend_is_rejected(self):
        cfg = RuntimeConfig.from_dict(
            {
                "operators": {
                    "sem_groupby": {
                        "query_spec": {
                            "semantic": {
                                "instruction": "label records",
                                "backend": "embedding",
                                "output_mode": "label",
                            }
                        }
                    }
                }
            }
        )
        with pytest.raises(ValueError, match="sem_groupby query_spec.semantic no longer accepts backend"):
            cfg.get_groupby_query_spec()

    def test_join_runtime_bundle_uses_match_lowering(self):
        cfg = RuntimeConfig.from_dict(
            {
                "defaults": {"ttl_seconds": 1200},
                "operators": {
                    "sem_join": {
                        "query_spec": {
                            "backend": "embedding",
                            "pairing_method": "blocking",
                            "window_kind": "sliding",
                            "window_size_ms": 3000,
                            "slide_ms": 1000,
                        },
                        "kernel": {},
                    }
                },
            }
        )
        bundle = cfg.resolve_join_runtime_bundle()
        assert bundle.query_spec.semantic.backend == "embedding"
        assert bundle.query_spec.scope_policy.ttl_seconds == 1200
        assert bundle.lowering_plan.classical_operator == "join/filter"

    def test_get_window_config_hydrates_typed_window_config(self):
        cfg = RuntimeConfig.from_dict(
            {
                "defaults": {"ttl_seconds": 1800, "overflow_policy": "drop_newest"},
                "operators": {
                    "sem_window": {
                        "kernel": {
                            "max_window_events": 7,
                            "window_timeout_ms": 12000,
                            "boundary_flag": "segment_done",
                        }
                    }
                },
            }
        )
        window_cfg = cfg.get_window_config()
        assert window_cfg.max_window_events == 7
        assert window_cfg.window_timeout_ms == 12000
        assert window_cfg.boundary_flag == "segment_done"
        assert window_cfg.ttl_seconds == 1800
        assert window_cfg.overflow_policy.value == "drop_newest"

    def test_flat_operator_layout_is_rejected(self):
        cfg = RuntimeConfig.from_dict(
            {
                "operators": {
                    "sem_topk": {
                        "k": 3,
                        "ranking_method": "pointwise",
                    }
                },
            }
        )
        with pytest.raises(ValueError, match="nested query_spec/kernel layout only"):
            cfg.get_topk_query_spec()

    def test_window_query_spec_is_rejected(self):
        cfg = RuntimeConfig.from_dict(
            {
                "operators": {
                    "sem_window": {
                        "query_spec": {"foo": "bar"},
                        "kernel": {},
                    }
                },
            }
        )
        with pytest.raises(ValueError, match="does not accept query_spec"):
            cfg.get_window_config()
