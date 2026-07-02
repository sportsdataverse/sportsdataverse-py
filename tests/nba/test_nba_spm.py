"""Tests for nba_spm: SpmCoefficients, train_spm, nba_spm (pure math, no live fetch)."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_spm import SPM_FEATURES, SpmCoefficients, nba_spm, train_spm


def test_train_spm_recovers_known_linear_map():
    rng = np.random.default_rng(0)
    n = 400
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame({"player_id": np.arange(n), **feats, "min": np.full(n, 500.0), "gp": np.full(n, 20)})
    # planted linear map: o_rapm = 2*pts - 1.5*tov ; d_rapm = 1.2*stl + 0.8*blk
    o = 2.0 * feats["pts"] - 1.5 * feats["tov"]
    d = 1.2 * feats["stl"] + 0.8 * feats["blk"]
    target = pl.DataFrame({"player_id": np.arange(n), "o_rapm": o, "d_rapm": d})
    coef = train_spm(bf, target, alpha=0.01)
    out = nba_spm(bf, coef)
    # recovered predictions correlate near-perfectly with the planted targets
    assert np.corrcoef(out["ospm"].to_numpy(), o)[0, 1] > 0.99
    assert np.corrcoef(out["dspm"].to_numpy(), d)[0, 1] > 0.99
    assert set(out.columns) == {"player_id", "ospm", "dspm", "spm", "min", "gp"}


def test_spm_features_match_box_logs_stats():
    from sportsdataverse.nba.nba_box_logs import _STATS

    assert SPM_FEATURES == list(_STATS)


def test_spm_coefficients_frozen():
    coef = SpmCoefficients(
        o_coef=np.zeros(len(SPM_FEATURES)),
        d_coef=np.zeros(len(SPM_FEATURES)),
        o_intercept=0.0,
        d_intercept=0.0,
        feature_names=SPM_FEATURES,
    )
    with pytest.raises((AttributeError, TypeError)):
        coef.o_intercept = 1.0  # type: ignore[misc]


def test_nba_spm_output_schema():
    """nba_spm returns the documented column set with correct dtypes."""
    rng = np.random.default_rng(42)
    n = 10
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 200.0),
            "gp": np.full(n, 10, dtype=np.int64),
        }
    )
    coef = SpmCoefficients(
        o_coef=np.ones(len(SPM_FEATURES)),
        d_coef=np.ones(len(SPM_FEATURES)) * 0.5,
        o_intercept=1.0,
        d_intercept=0.5,
        feature_names=SPM_FEATURES,
    )
    out = nba_spm(bf, coef)
    assert out.schema["player_id"] == pl.Int64
    assert out.schema["ospm"] == pl.Float64
    assert out.schema["dspm"] == pl.Float64
    assert out.schema["spm"] == pl.Float64
    assert out.schema["min"] == pl.Float64
    assert out.schema["gp"] == pl.Int64
    assert len(out) == n


def test_nba_spm_return_as_pandas():
    import pandas as pd

    rng = np.random.default_rng(7)
    n = 5
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 100.0),
            "gp": np.full(n, 5, dtype=np.int64),
        }
    )
    coef = SpmCoefficients(
        o_coef=np.zeros(len(SPM_FEATURES)),
        d_coef=np.zeros(len(SPM_FEATURES)),
        o_intercept=0.0,
        d_intercept=0.0,
        feature_names=SPM_FEATURES,
    )
    out_pd = nba_spm(bf, coef, return_as_pandas=True)
    assert isinstance(out_pd, pd.DataFrame)
    assert set(out_pd.columns) == {"player_id", "ospm", "dspm", "spm", "min", "gp"}


def test_train_spm_inner_join_on_player_id():
    """train_spm inner-joins: players in box_features but not in target are excluded."""
    rng = np.random.default_rng(1)
    n = 50
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 300.0),
            "gp": np.full(n, 15, dtype=np.int64),
        }
    )
    # target only covers half the players
    target = pl.DataFrame(
        {
            "player_id": np.arange(25, dtype=np.int64),
            "o_rapm": rng.normal(0, 1, 25),
            "d_rapm": rng.normal(0, 1, 25),
        }
    )
    coef = train_spm(bf, target, alpha=10.0)
    assert isinstance(coef, SpmCoefficients)
    assert len(coef.feature_names) == len(SPM_FEATURES)


def test_train_spm_custom_feature_names():
    """train_spm respects a custom feature_names subset."""
    rng = np.random.default_rng(2)
    n = 100
    feats = {f: rng.normal(0, 1, n) for f in SPM_FEATURES}
    bf = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            **feats,
            "min": np.full(n, 400.0),
            "gp": np.full(n, 20, dtype=np.int64),
        }
    )
    target = pl.DataFrame(
        {
            "player_id": np.arange(n, dtype=np.int64),
            "o_rapm": rng.normal(0, 1, n),
            "d_rapm": rng.normal(0, 1, n),
        }
    )
    subset = ["pts", "ast", "tov"]
    coef = train_spm(bf, target, feature_names=subset, alpha=10.0)
    assert coef.feature_names == subset
    assert len(coef.o_coef) == 3
    assert len(coef.d_coef) == 3
