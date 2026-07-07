"""Tests for the closed-form pregame predictors (``mbb_game_predict``)."""

import math

import pandas as pd
import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse.mbb.mbb_game_predict import (
    in_game_features,
    mbb_in_game_win_prob,
    mbb_predict_games,
    predict_margin,
    predict_total,
    win_prob_from_margin,
)
from sportsdataverse.mbb.mbb_prediction_constants import get_constants


def _ratings() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2024, 2024, 2024],
            "team_id": ["1", "2", "3"],
            "adj_o": [110.0, 100.0, 95.0],
            "adj_d": [90.0, 100.0, 105.0],
            "adj_em": [20.0, 0.0, -10.0],
            "adj_tempo": [70.0, 65.0, 62.0],
        }
    )


def _games() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": ["g1", "g2"],
            "home_team_id": ["1", "3"],
            "away_team_id": ["2", "2"],
            "neutral_site": [False, True],
        }
    )


def test_predict_margin_neutral_has_no_hfa():
    c = get_constants("mens")
    assert predict_margin(10.0, 5.0, neutral=True) == 5.0 * c.em_scale


def test_predict_margin_home_adds_hfa():
    c = get_constants("mens")
    assert predict_margin(10.0, 5.0, neutral=False) == 5.0 * c.em_scale + c.hfa


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


def test_predict_games_matches_scalar_functions():
    out = mbb_predict_games(_games(), _ratings())
    assert out.columns == [
        "game_id",
        "home_team_id",
        "away_team_id",
        "exp_margin",
        "home_win_prob",
        "exp_total",
    ]
    g1 = out.row(0, named=True)
    exp_margin = predict_margin(20.0, 0.0, neutral=False)
    assert abs(g1["exp_margin"] - exp_margin) < 1e-9
    assert abs(g1["home_win_prob"] - win_prob_from_margin(exp_margin)) < 1e-9
    assert abs(g1["exp_total"] - predict_total(110.0, 90.0, 100.0, 100.0, 70.0, 65.0)) < 1e-9


def test_predict_games_neutral_site_drops_hfa():
    out = mbb_predict_games(_games(), _ratings())
    g2 = out.row(1, named=True)
    assert abs(g2["exp_margin"] - predict_margin(-10.0, 0.0, neutral=True)) < 1e-9


def test_predict_games_missing_neutral_site_defaults_home():
    out = mbb_predict_games(_games().drop("neutral_site"), _ratings())
    assert abs(out.row(1, named=True)["exp_margin"] - predict_margin(-10.0, 0.0, neutral=False)) < 1e-9


def test_predict_games_join_key_dtype_guard_raises():
    bad = _ratings().with_columns(pl.col("team_id").cast(pl.Int64))
    with pytest.raises(ValueError, match="dtype"):
        mbb_predict_games(_games(), bad)


def test_predict_games_return_as_pandas():
    out = mbb_predict_games(_games(), _ratings(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert len(out) == 2


def _pbp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [401, 401, 401],
            "start_game_seconds_remaining": [2400, 1200, 30],
            "home_score": [0, 40, 70],
            "away_score": [0, 35, 68],
            "team_id": [10, 20, None],
            "home_team_id": [10, 10, 10],
        }
    )


def test_in_game_features_columns_and_values():
    feats = in_game_features(_pbp(), 0.75)
    assert feats.columns == ["score_diff", "sec_left", "sqrt_sec_left", "pregame_logit", "home_has_ball"]
    assert feats["score_diff"].to_list() == [0.0, 5.0, 2.0]
    assert feats["sec_left"].to_list() == [2400.0, 1200.0, 30.0]
    assert feats["sqrt_sec_left"].to_list() == pytest.approx([math.sqrt(2400), math.sqrt(1200), math.sqrt(30)])
    assert feats["pregame_logit"].to_list() == pytest.approx([math.log(0.75 / 0.25)] * 3)
    # home had it, away had it, dead ball -> 0
    assert feats["home_has_ball"].to_list() == [1, 0, 0]
    assert feats.schema["home_has_ball"] == pl.Int8


def test_in_game_features_boundary_pregame_prob_is_finite():
    # norm.cdf saturates to exact 0/1 for extreme margins; the logit must not crash
    for p0 in (0.0, 1.0):
        feats = in_game_features(_pbp(), p0)
        assert feats["pregame_logit"].is_finite().all()


def test_in_game_features_sec_left_clipped_in_overtime():
    ot = _pbp().with_columns(pl.Series("start_game_seconds_remaining", [2400, -60, -120]))
    feats = in_game_features(ot, 0.5)
    assert feats["sec_left"].to_list() == [2400.0, 0.0, 0.0]
    assert feats["sqrt_sec_left"].to_list() == [math.sqrt(2400), 0.0, 0.0]
    assert feats["pregame_logit"].to_list() == [0.0, 0.0, 0.0]


def _play(sec_left: int, home: int, away: int, home_ball: bool = True) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [1],
            "start_game_seconds_remaining": [sec_left],
            "home_score": [home],
            "away_score": [away],
            "team_id": [10 if home_ball else 20],
            "home_team_id": [10],
        }
    )


def test_in_game_wp_late_big_lead_near_one():
    wp = mbb_in_game_win_prob(_play(10, 80, 62), 0.5)
    assert wp.columns[-1] == "home_win_prob"
    assert wp["home_win_prob"][0] > 0.95


def test_in_game_wp_tied_at_tip_near_pregame():
    for p0 in (0.30, 0.50, 0.70):
        wp = mbb_in_game_win_prob(_play(2400, 0, 0), p0)
        assert abs(wp["home_win_prob"][0] - p0) < 0.10, f"pregame {p0} -> {wp['home_win_prob'][0]}"


def test_in_game_wp_monotone_in_score_diff():
    probs = [mbb_in_game_win_prob(_play(600, 50 + d, 50), 0.5)["home_win_prob"][0] for d in (-15, -5, 0, 5, 15)]
    assert probs == sorted(probs)
    assert probs[0] < 0.5 < probs[-1]
