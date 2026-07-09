"""Tests for the NBA closed-form pregame predictions (Phase 2, models ②③)."""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nba.nba_game_predict import (
    expected_possessions,
    nba_predict_games,
    predict_margin,
    predict_total,
    win_prob_from_margin,
)
from sportsdataverse.nba.nba_prediction_constants import get_constants


def test_expected_possessions_league_average_matchup() -> None:
    c = get_constants("00")
    assert abs(expected_possessions(c.avg_pace, c.avg_pace, league_id="00") - c.avg_pace) < 1e-9


def test_predict_margin_neutral_has_no_hfa() -> None:
    m = predict_margin(10.0, 10.0, home_pace=100.0, away_pace=100.0, neutral=True, league_id="00")
    assert abs(m) < 1e-9


def test_predict_margin_symmetric_teams_nonneutral_equals_hfa() -> None:
    c = get_constants("00")
    m = predict_margin(5.0, 5.0, home_pace=c.avg_pace, away_pace=c.avg_pace, neutral=False, league_id="00")
    assert abs(m - c.hfa) < 1e-9


def test_win_prob_from_margin_symmetric_is_half() -> None:
    assert abs(win_prob_from_margin(0.0, league_id="00") - 0.5) < 1e-9


def test_win_prob_from_margin_positive_favors_home() -> None:
    assert win_prob_from_margin(10.0, league_id="00") > 0.5


def test_predict_total_league_average_matchup() -> None:
    c = get_constants("00")
    total = predict_total(
        c.avg_off_rtg, c.avg_off_rtg, c.avg_off_rtg, c.avg_off_rtg, c.avg_pace, c.avg_pace, league_id="00"
    )
    expected = 2.0 * c.avg_off_rtg * c.avg_pace / 100.0
    assert abs(total - expected) < 1e-6


def test_predict_total_higher_pace_gives_larger_total() -> None:
    c = get_constants("00")
    low = predict_total(
        c.avg_off_rtg, c.avg_off_rtg, c.avg_off_rtg, c.avg_off_rtg, c.avg_pace, c.avg_pace, league_id="00"
    )
    high = predict_total(
        c.avg_off_rtg, c.avg_off_rtg, c.avg_off_rtg, c.avg_off_rtg, c.avg_pace + 10, c.avg_pace + 10, league_id="00"
    )
    assert high > low


def _ratings() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "team_id": ["A", "B"],
            "adj_off_rtg": [118.0, 110.0],
            "adj_def_rtg": [108.0, 112.0],
            "adj_net_rtg": [10.0, -2.0],
            "adj_pace": [100.0, 98.0],
        }
    )


def test_nba_predict_games_matches_scalar_functions() -> None:
    games = pl.DataFrame(
        {
            "game_id": ["G1"],
            "home_team_id": ["A"],
            "away_team_id": ["B"],
            "neutral_site": [False],
        }
    )
    out = nba_predict_games(games, _ratings(), league_id="00")
    expected_margin = predict_margin(10.0, -2.0, home_pace=100.0, away_pace=98.0, neutral=False, league_id="00")
    expected_total = predict_total(118.0, 108.0, 110.0, 112.0, 100.0, 98.0, league_id="00")
    row = out.row(0, named=True)
    assert abs(row["exp_margin"] - expected_margin) < 1e-6
    assert abs(row["exp_total"] - expected_total) < 1e-6
    assert abs(row["home_win_prob"] - win_prob_from_margin(expected_margin, league_id="00")) < 1e-6


def test_nba_predict_games_dtype_mismatch_raises() -> None:
    games = pl.DataFrame(
        {
            "game_id": ["G1"],
            "home_team_id": [1],  # Int64, not Utf8 -- deliberate mismatch
            "away_team_id": ["B"],
        }
    )
    with pytest.raises(ValueError, match="dtype mismatch"):
        nba_predict_games(games, _ratings(), league_id="00")


def test_nba_predict_games_missing_neutral_site_defaults_false() -> None:
    games = pl.DataFrame({"game_id": ["G1"], "home_team_id": ["A"], "away_team_id": ["B"]})
    out = nba_predict_games(games, _ratings(), league_id="00")
    assert out.height == 1
    assert out["exp_margin"][0] is not None
