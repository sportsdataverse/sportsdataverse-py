"""Offline unit tests for the player-impact metric/solver helpers + LEAGUE_CONSTANTS scaffold."""

from __future__ import annotations

import numpy as np
import pytest
import scipy.sparse as sp

from sportsdataverse.nhl.nhl_player_impact_constants import (
    ImpactConfig,
    LEAGUE_CONSTANTS,
    booster_cache_dir,
    calibration_table,
    get_constants,
    spearman_corr,
    team_fullname_to_abbr,
    weighted_ridge,
)


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([9.0, 20.0, 31.0, 42.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_calibration_table_shape():
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, 200)
    p = rng.random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_weighted_ridge_recovers_ols_when_lambda_tiny():
    X = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [2.0, 1.0]])
    beta_true = np.array([2.0, -1.0])
    y = X @ beta_true
    w = np.ones(4)
    beta = weighted_ridge(X, y, w, lam=1e-8)
    assert np.allclose(beta, beta_true, atol=1e-4)


def test_weighted_ridge_accepts_sparse():
    X = sp.csr_matrix(np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]))
    y = np.array([2.0, -1.0, 1.0])
    w = np.ones(3)
    beta = weighted_ridge(X, y, w, lam=1e-6)
    assert beta.shape == (2,)


def test_get_constants_resolves_both_leagues():
    nhl = get_constants("nhl")
    pwhl = get_constants("pwhl")
    assert isinstance(nhl, ImpactConfig)
    assert isinstance(pwhl, ImpactConfig)
    assert set(LEAGUE_CONSTANTS) == {"nhl", "pwhl"}


def test_get_constants_unknown_league_raises():
    with pytest.raises(ValueError):
        get_constants("nba")


def test_pwhl_borrows_nhl_xg_boosters_is_asserted_not_silent():
    assert get_constants("pwhl").xg_booster_league == "nhl"


def test_booster_cache_dir_precedence(monkeypatch):
    monkeypatch.delenv("NHL_XG_MODEL_DIR", raising=False)
    default = booster_cache_dir()
    assert default.name == "nhl_xg_models"

    monkeypatch.setenv("NHL_XG_MODEL_DIR", "/tmp/env_dir")
    assert str(booster_cache_dir()) in ("/tmp/env_dir", "\\tmp\\env_dir")

    # Explicit override always wins over the env var.
    assert str(booster_cache_dir("/tmp/explicit_dir")) in ("/tmp/explicit_dir", "\\tmp\\explicit_dir")


def test_team_fullname_to_abbr_known_and_unknown():
    assert team_fullname_to_abbr("Buffalo Sabres") == "BUF"
    assert team_fullname_to_abbr("New Jersey Devils") == "NJD"
    assert team_fullname_to_abbr("Not A Real Team") is None
