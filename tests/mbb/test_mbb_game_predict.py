"""Tests for the closed-form pregame predictors (``mbb_game_predict``)."""

from scipy.stats import norm

from sportsdataverse.mbb.mbb_game_predict import (
    predict_margin,
    predict_total,
    win_prob_from_margin,
)
from sportsdataverse.mbb.mbb_prediction_constants import get_constants


def test_predict_margin_neutral_has_no_hfa():
    assert predict_margin(10.0, 5.0, neutral=True) == 5.0


def test_predict_margin_home_adds_hfa():
    c = get_constants("mens")
    assert predict_margin(10.0, 5.0, neutral=False) == 5.0 + c.hfa


def test_predict_margin_league_dispatch():
    cw = get_constants("womens")
    assert predict_margin(0.0, 0.0, neutral=False, league="womens") == cw.hfa


def test_win_prob_symmetric_is_half():
    assert abs(win_prob_from_margin(0.0) - 0.5) < 1e-12


def test_win_prob_matches_normal_cdf():
    c = get_constants("mens")
    assert abs(win_prob_from_margin(c.margin_sd) - float(norm.cdf(1.0))) < 1e-12


def test_win_prob_monotone_in_margin():
    assert win_prob_from_margin(10.0) > win_prob_from_margin(5.0) > 0.5


def test_predict_total_average_teams_average_tempo():
    c = get_constants("mens")
    total = predict_total(
        c.avg_efficiency,
        c.avg_efficiency,
        c.avg_efficiency,
        c.avg_efficiency,
        c.avg_tempo,
        c.avg_tempo,
    )
    assert abs(total - 2 * c.avg_efficiency * c.avg_tempo / 100.0) < 1e-9


def test_predict_total_higher_tempo_larger_total():
    c = get_constants("mens")
    args = (c.avg_efficiency, c.avg_efficiency, c.avg_efficiency, c.avg_efficiency)
    assert predict_total(*args, 72.0, 72.0) > predict_total(*args, 62.0, 62.0)
