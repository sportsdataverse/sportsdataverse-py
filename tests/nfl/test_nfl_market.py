"""Unit tests for the NFL closed-form pregame market module (Phase 2)."""

import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse.nfl.nfl_market import (
    nfl_predict_games,
    predict_margin,
    predict_total,
    win_prob_from_margin,
)
from sportsdataverse.nfl.nfl_prediction_constants import get_constants


def _ratings():
    return pl.DataFrame(
        {
            "team_id": ["A", "B", "C"],
            "adj_off_epa": [0.10, 0.00, -0.05],
            "adj_def_epa": [-0.05, 0.00, 0.08],
            "adj_net": [0.15, 0.00, -0.13],
        }
    )


def _games():
    return pl.DataFrame(
        {
            "game_id": ["g1", "g2", "g3"],
            "home_team_id": ["A", "B", "C"],
            "away_team_id": ["B", "C", "A"],
            "neutral_site": [False, True, False],
        }
    )


def test_predict_games_matches_scalar_functions_and_edge():
    odds = pl.DataFrame({"game_id": ["g1", "g3"], "close_spread_home": [7.0, -3.5]})
    out = nfl_predict_games(_games(), _ratings(), odds=odds)
    assert out.columns == [
        "game_id",
        "home_team_id",
        "away_team_id",
        "neutral_site",
        "exp_margin",
        "home_win_prob",
        "exp_total",
        "market_edge",
    ]
    r = {row["game_id"]: row for row in out.to_dicts()}
    # g1: A home vs B, non-neutral.
    assert abs(r["g1"]["exp_margin"] - predict_margin(0.15, 0.00, False)) < 1e-9
    assert abs(r["g1"]["home_win_prob"] - win_prob_from_margin(r["g1"]["exp_margin"])) < 1e-9
    assert abs(r["g1"]["exp_total"] - predict_total(0.10, -0.05, 0.00, 0.00)) < 1e-9
    # g2: neutral -> no HFA.
    assert abs(r["g2"]["exp_margin"] - predict_margin(0.00, -0.13, True)) < 1e-9
    # market_edge = exp_margin - close_spread_home where odds present, null where absent.
    assert abs(r["g1"]["market_edge"] - (r["g1"]["exp_margin"] - 7.0)) < 1e-9
    assert r["g2"]["market_edge"] is None
    assert abs(r["g3"]["market_edge"] - (r["g3"]["exp_margin"] + 3.5)) < 1e-9


def test_predict_games_dtype_guard_raises():
    bad_games = _games().with_columns(pl.col("home_team_id").cast(pl.Categorical))
    with pytest.raises(AssertionError):
        nfl_predict_games(bad_games, _ratings())


def test_predict_games_empty_and_pandas():
    empty = nfl_predict_games(_games().head(0), _ratings())
    assert empty.height == 0 and empty.schema["market_edge"] == pl.Float64
    pdf = nfl_predict_games(_games(), _ratings(), return_as_pandas=True)
    assert not isinstance(pdf, pl.DataFrame) and len(pdf) == 3


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


def test_predict_games_missing_team_yields_null_not_nan():
    # a team absent from ratings must surface as null win prob, not NaN
    # (numpy converts null exp_margin to NaN through norm.cdf)
    games = pl.DataFrame(
        {
            "game_id": ["g9"],
            "home_team_id": ["A"],
            "away_team_id": ["ZZ"],  # not in ratings
            "neutral_site": [False],
        }
    )
    out = nfl_predict_games(games, _ratings())
    row = out.row(0, named=True)
    assert row["exp_margin"] is None
    assert row["home_win_prob"] is None  # null, not NaN
