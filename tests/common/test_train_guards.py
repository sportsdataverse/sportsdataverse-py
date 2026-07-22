"""Tests for train-time guards + the learned span blend (fold-ins)."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.feature_set import fit_span_blend
from sportsdataverse._common.train_guards import (
    audit_training_frame,
    correlation_prune,
    drop_constant_columns,
)


def _train(n: int = 200, seed: int = 3) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    a = rng.normal(size=n)
    return pl.DataFrame(
        {
            "a": a,
            "a_clone": a * 1.001 + rng.normal(scale=1e-4, size=n),  # collinear with a
            "b": rng.normal(size=n),
            "noise": rng.normal(size=n),
            "const": [1.0] * n,
            "y": a * 2.0 + rng.normal(scale=0.1, size=n),
        }
    )


# ------------------------------------------------------------------- audits


def test_audit_clean_frame_passes() -> None:
    assert audit_training_frame(_train(), target="y", features=["a", "b"]) == []


def test_audit_missing_and_null_columns() -> None:
    df = _train()
    assert audit_training_frame(df, target="y", features=["a", "zzz"]) == ["missing columns ['zzz']"]
    holey = df.with_columns(pl.when(pl.int_range(pl.len()) == 0).then(None).otherwise(pl.col("y")).alias("y"))
    errors = audit_training_frame(holey, target="y", features=["a"])
    assert any("null rows" in e for e in errors)
    assert audit_training_frame(holey, target="y", features=["a"], allow_null_target=True) == []
    all_null = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("b"))
    errors2 = audit_training_frame(all_null, target="y", features=["a", "b"])
    assert any("entirely null" in e for e in errors2)


def test_audit_baseline_and_empty() -> None:
    df = _train()
    holey = df.with_columns(pl.when(pl.int_range(pl.len()) == 0).then(None).otherwise(pl.col("b")).alias("b"))
    errors = audit_training_frame(holey, target="y", features=["a"], baseline="b")
    assert any("baseline" in e for e in errors)
    assert audit_training_frame(df.head(0), target="y", features=["a"]) == ["training frame is empty"]


def test_drop_constant_columns() -> None:
    kept, dropped = drop_constant_columns(_train(), ["a", "const", "b"])
    assert kept == ["a", "b"]
    assert dropped == ["const"]


def test_correlation_prune_drops_clone_and_noise() -> None:
    kept, dropped = correlation_prune(
        _train(),
        ["a", "a_clone", "b", "noise"],
        "y",
        min_target_corr=0.05,
        max_cross_corr=0.95,
    )
    # a and a_clone are near-identical: exactly ONE survives, the other is
    # dropped as collinear with the kept winner (tie order is float noise)
    assert len({"a", "a_clone"} & set(kept)) == 1
    assert len({"a", "a_clone"} & set(dropped)) == 1
    assert "noise" in dropped  # under the target-corr floor
    assert set(kept) | set(dropped) == {"a", "a_clone", "b", "noise"}


# --------------------------------------------------------------- span blend


def test_fit_span_blend_recovers_known_mixture() -> None:
    rng = np.random.default_rng(11)
    short = rng.normal(loc=10, size=500)
    long = rng.normal(loc=10, size=500)
    y = 0.7 * short + 0.3 * long + rng.normal(scale=0.01, size=500)
    df = pl.DataFrame({"pts_mean___5": short, "pts_mean___0": long, "y": y})
    weights = fit_span_blend(df, ["pts_mean___5", "pts_mean___0"], "y")
    assert weights["pts_mean___5"] == pytest.approx(0.7, abs=0.05)
    assert weights["pts_mean___0"] == pytest.approx(0.3, abs=0.05)
    assert sum(weights.values()) == pytest.approx(1.0)


def test_fit_span_blend_validation_and_degenerate() -> None:
    df = pl.DataFrame({"a": [1.0, 2.0], "b": [1.0, 2.0], "y": [1.0, 2.0]})
    with pytest.raises(ValueError, match="at least 2"):
        fit_span_blend(df, ["a"], "y")
    with pytest.raises(ValueError, match="missing columns"):
        fit_span_blend(df, ["a", "zzz"], "y")
    # too few clean rows -> uniform fallback
    thin = df.head(1)
    weights = fit_span_blend(thin, ["a", "b"], "y")
    assert weights == {"a": 0.5, "b": 0.5}
