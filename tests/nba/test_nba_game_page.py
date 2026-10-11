"""nba_game_page: the game-page derivations shared by the GOP processor and the sdv-db stage."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import nba_game_page as gp
from sportsdataverse.nba import nba_game_predict
from sportsdataverse.nba.nba_game_predict import win_prob_from_margin
from sportsdataverse.nba.nba_prediction_constants import get_constants

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "espn"


def test_in_game_win_prob_is_not_memoised_on_the_frame() -> None:
    # lru_cache keyed by DataFrame identity would pin every scored game in memory for the
    # life of the Flask processor; only the artifact loader may cache.
    assert not hasattr(nba_game_predict.nba_in_game_win_prob, "cache_info")
    assert hasattr(nba_game_predict._load_in_game_artifact, "cache_info")


def _pbp(rows: list[dict]) -> pl.DataFrame:
    base = {
        "game_id": 1,
        "home_team_id": 19,
        "team_id": 19,
        "home_score": 0,
        "away_score": 0,
        "period_number": 1,
        "start_game_seconds_remaining": 2880.0,
        "start_quarter_seconds_remaining": 720.0,
        "home_team_spread": 3.0,
        "game_spread_available": True,
    }
    return pl.DataFrame([{**base, **r} for r in rows])


def test_home_prior_uses_the_signed_home_spread() -> None:
    p, src = gp.home_prior(3.0, True)
    assert src == "spread" and p == pytest.approx(win_prob_from_margin(3.0))
    p_dog, _ = gp.home_prior(-3.0, True)
    assert p_dog == pytest.approx(1 - p)  # away favoured by 3 mirrors home favoured by 3


def test_home_prior_without_a_line_is_home_court_only() -> None:
    # hoopR and helper_nba_pickcenter both default to +2.5 when no line exists; the flag gates it
    p, src = gp.home_prior(2.5, False)
    assert src == "hfa" and p == pytest.approx(win_prob_from_margin(get_constants("00").hfa))
    assert gp.home_prior(None, None) == (p, "hfa")


def test_ot_rows_use_the_period_clock_and_are_tagged() -> None:
    df = _pbp(
        [
            {"period_number": 4, "start_game_seconds_remaining": 10.0, "start_quarter_seconds_remaining": 10.0},
            {"period_number": 5, "start_game_seconds_remaining": -300.0, "start_quarter_seconds_remaining": 300.0},
            {"period_number": 5, "start_game_seconds_remaining": -590.0, "start_quarter_seconds_remaining": 10.0},
        ]
    )
    out = gp.ot_adjusted_seconds(df)
    assert out["wp_regime"].to_list() == ["reg", "ot", "ot"]
    assert out["start_game_seconds_remaining"].to_list() == [10.0, 300.0, 10.0]


def test_score_win_prob_adds_the_columns_and_first_delta_is_from_the_prior() -> None:
    df = _pbp(
        [
            {"start_game_seconds_remaining": 2880.0},
            {"start_game_seconds_remaining": 1440.0, "home_score": 50, "away_score": 48},
            {"start_game_seconds_remaining": 10.0, "home_score": 99, "away_score": 100, "team_id": 1},
        ]
    )
    out = gp.score_win_prob(df)
    for c in ["pregame_home_prob", "prior_source", "wp_regime", "home_win_prob", "wp_delta"]:
        assert c in out.columns
    assert out.height == 3 and out["prior_source"][0] == "spread"
    prior = out["pregame_home_prob"][0]
    assert out["wp_delta"][0] == pytest.approx(out["home_win_prob"][0] - prior)
    assert out["wp_delta"][1] == pytest.approx(out["home_win_prob"][1] - out["home_win_prob"][0])
    assert 0.0 < out["home_win_prob"][2] < 0.5  # trailing by one with ten seconds left


def test_score_win_prob_tie_at_ot_start_is_near_half() -> None:
    df = _pbp(
        [
            {
                "period_number": 5,
                "start_game_seconds_remaining": -300.0,
                "start_quarter_seconds_remaining": 300.0,
                "home_score": 101,
                "away_score": 101,
                "home_team_spread": 0.0,
            },
        ]
    )
    out = gp.score_win_prob(df)
    assert out["wp_regime"][0] == "ot"
    assert abs(out["home_win_prob"][0] - 0.5) < 0.15  # the clipped-at-zero model gave 0.39 here


def test_score_win_prob_on_an_empty_frame_keeps_the_schema() -> None:
    out = gp.score_win_prob(_pbp([]).clear())
    assert out.height == 0 and "home_win_prob" in out.columns
