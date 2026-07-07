import numpy as np
import pytest

from sportsdataverse.mbb.mbb_prediction_constants import (
    LEAGUE_CONSTANTS,
    LeagueConstants,
    brier_score,
    calibration_table,
    get_constants,
    log_loss_score,
    mae,
    spearman_corr,
)


def test_brier_perfect_is_zero():
    y = np.array([1, 0, 1, 0])
    p = np.array([1.0, 0.0, 1.0, 0.0])
    assert brier_score(y, p) == 0.0


def test_brier_matches_manual():
    y = np.array([1, 0])
    p = np.array([0.75, 0.25])
    assert abs(brier_score(y, p) - 0.0625) < 1e-9  # mean of (0.25^2, 0.25^2)


def test_spearman_monotonic_is_one():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([10.0, 20.0, 30.0, 40.0])
    assert abs(spearman_corr(a, b) - 1.0) < 1e-9


def test_calibration_table_shape():
    y = np.random.default_rng(0).integers(0, 2, 200)
    p = np.random.default_rng(1).random(200)
    tbl = calibration_table(y, p, n_bins=10)
    assert tbl.columns == ["bin_mid", "mean_pred", "mean_actual", "n"]
    assert tbl.height <= 10


def test_mae_manual():
    assert abs(mae(np.array([1.0, 2.0]), np.array([1.5, 2.5])) - 0.5) < 1e-9


def test_log_loss_perfect_is_near_zero():
    y = np.array([1, 0, 1, 0])
    p = np.array([1.0, 0.0, 1.0, 0.0])
    assert log_loss_score(y, p) < 1e-9


# --- Task 0.3: league-constants scaffold ---


def test_league_constants_table_has_both_leagues():
    assert set(LEAGUE_CONSTANTS) == {"mens", "womens"}


def test_get_constants_resolves_both_leagues():
    for league in ("mens", "womens"):
        c = get_constants(league)
        assert isinstance(c, LeagueConstants)
        assert c.margin_sd > 0
        assert c.avg_tempo > 0
        assert c.avg_efficiency > 0
        assert set(c.quad_thresholds) == {"home", "neutral", "away"}


def test_get_constants_unknown_league_raises():
    with pytest.raises(ValueError, match="nba"):
        get_constants("nba")
