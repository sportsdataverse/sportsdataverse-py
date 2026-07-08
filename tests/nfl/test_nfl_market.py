"""Unit tests for the NFL closed-form pregame market module (Phase 2)."""

from scipy.stats import norm

from sportsdataverse.nfl.nfl_market import predict_margin, predict_total, win_prob_from_margin
from sportsdataverse.nfl.nfl_prediction_constants import get_constants


def test_neutral_margin_carries_no_hfa():
    cfg = get_constants("modern")
    neutral = predict_margin(0.10, 0.05, True)
    at_home = predict_margin(0.10, 0.05, False)
    assert abs(at_home - neutral - cfg.hfa) < 1e-12
    assert abs(neutral - cfg.points_per_net * 0.05) < 1e-12


def test_equal_strength_home_edge_and_neutral_coinflip():
    cfg = get_constants("modern")
    p_home = win_prob_from_margin(predict_margin(0.0, 0.0, False))
    assert abs(p_home - float(norm.cdf(cfg.hfa / cfg.margin_sd))) < 1e-12
    assert p_home > 0.5
    assert win_prob_from_margin(predict_margin(0.0, 0.0, True)) == 0.5


def test_total_rises_with_offense_falls_with_defense():
    # Two strong offenses vs two strong (low allowed-EPA) defenses.
    strong_off = predict_total(0.15, 0.0, 0.15, 0.0)
    strong_def = predict_total(0.0, -0.15, 0.0, -0.15)
    league_avg = predict_total(0.0, 0.0, 0.0, 0.0)
    cfg = get_constants("modern")
    assert abs(league_avg - cfg.avg_total) < 1e-12
    assert strong_off > league_avg > strong_def
