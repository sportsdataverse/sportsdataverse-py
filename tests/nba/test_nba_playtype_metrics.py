"""Unit tests for the play-type/impact spine's shared constants + validation metrics."""

import numpy as np

from sportsdataverse.nba.nba_playtype_constants import (
    LEAGUE_ID_MAP,
    SYNERGY_PLAY_TYPES,
    PlaytypeConfig,
    calibration_slope,
    mae,
    spearman_corr,
    sum_consistency_residual,
)


def test_eleven_play_types():
    assert len(SYNERGY_PLAY_TYPES) == 11
    assert "Transition" in SYNERGY_PLAY_TYPES and "Isolation" in SYNERGY_PLAY_TYPES


def test_league_id_map():
    assert LEAGUE_ID_MAP["nba"] == "00" and LEAGUE_ID_MAP["wnba"] == "10"
    assert LEAGUE_ID_MAP["gleague"] == "20"


def test_config_defaults():
    cfg = PlaytypeConfig()
    assert cfg.min_matchup_poss == 25.0 and cfg.ridge_alphas.ndim == 1


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_sum_consistency_zero_when_exact():
    parts = np.array([[1.0, 2.0], [3.0, 4.0]])  # rows=entities, cols=components
    whole = parts.sum(axis=1)
    assert sum_consistency_residual(parts, whole) < 1e-12


def test_calibration_slope_identity():
    x = np.array([1.0, 2.0, 3.0, 4.0])
    assert abs(calibration_slope(x, x) - 1.0) < 1e-9
