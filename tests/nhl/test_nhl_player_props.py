"""Tests for :mod:`sportsdataverse.nhl.nhl_player_props` (Phase 4)."""

from __future__ import annotations

import importlib

import numpy as np
import polars as pl

from sportsdataverse.nhl.nhl_player_props import _eb_shrink, _p_over, nhl_player_props

# The module sportsdataverse.nhl.nhl_player_props exports a same-named public
# function (nhl_player_props); once sportsdataverse.nhl's __init__ imports
# that function, the dotted attribute sportsdataverse.nhl.nhl_player_props
# resolves to the FUNCTION, not the module -- so monkeypatch.setattr with a
# string path silently patches the wrong object. Resolve the actual module
# via importlib and patch its attributes directly instead.
_props_mod = importlib.import_module("sportsdataverse.nhl.nhl_player_props")


def test_eb_shrink_matches_formula():
    n = np.array([1.0, 10.0, 100.0])
    rate = np.array([5.0, 2.0, 2.5])
    prior = 2.2
    kappa = 6.0
    expected = (n * rate + kappa * prior) / (n + kappa)
    out = _eb_shrink(n, rate, prior, kappa)
    assert np.allclose(out, expected)


def test_eb_shrink_low_n_pulls_toward_prior():
    out = _eb_shrink(np.array([1.0]), np.array([10.0]), prior=2.0, kappa=6.0)
    high_n = _eb_shrink(np.array([100.0]), np.array([10.0]), prior=2.0, kappa=6.0)
    assert abs(out[0] - 2.0) < abs(high_n[0] - 2.0)


def test_p_over_at_line_equal_to_mean_is_half():
    assert abs(_p_over(mean=3.0, line=3.0, sd=1.5) - 0.5) < 1e-9


def test_p_over_monotonic_in_line():
    p_low_line = _p_over(mean=3.0, line=1.0, sd=1.5)
    p_high_line = _p_over(mean=3.0, line=5.0, sd=1.5)
    assert p_low_line > 0.5 > p_high_line


def _mini_boxscores():
    # TOR hosts BOS on 2024-10-10 (game 1), then hosts NYR on 2024-10-12 (game 2).
    # Star Forward (TOR, C) plays both; a low-sample Rookie D also plays both.
    # Opponent goalies/teams "BOS"/"NYR" are present only as placeholders so
    # the opponent self-join has a row to match against.
    return pl.DataFrame(
        {
            "season": [2024] * 6,
            "game_id": [1, 1, 2, 2, 1, 2],
            "game_date": ["2024-10-10", "2024-10-10", "2024-10-12", "2024-10-12", "2024-10-10", "2024-10-12"],
            "player_id": [100, 900, 100, 900, 200, 200],
            "player_name": [
                "Star Forward",
                "Opp Placeholder",
                "Star Forward",
                "Opp Placeholder2",
                "Rookie D",
                "Rookie D",
            ],
            "team_abbrev": ["TOR", "BOS", "TOR", "NYR", "TOR", "TOR"],
            "position": ["C", "C", "C", "C", "D", "D"],
            "shots_on_goal": [4, 2, 6, 3, 1, 2],
            "goals": [1, 0, 2, 1, 0, 0],
            "assists": [1, 0, 0, 0, 1, 0],
            "points": [2, 0, 2, 1, 1, 0],
        }
    )


def _mini_ratings(seasons, **kwargs):
    return pl.DataFrame(
        {
            "season": [2024, 2024, 2024],
            "team": ["TOR", "BOS", "NYR"],
            "adj_xgf": [2.6, 2.5, 2.4],
            "adj_xga": [2.4, 1.8, 2.6],  # BOS is a below-average-xga (tougher) defense
        }
    )


def test_eb_shrinkage_and_matchup(monkeypatch):
    monkeypatch.setattr(_props_mod, "load_nhl_skater_boxscores", lambda seasons: _mini_boxscores())
    monkeypatch.setattr(_props_mod, "nhl_team_ratings", _mini_ratings)
    out = nhl_player_props(2024, stats=("shots",))
    assert out.height > 0
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["team"] == pl.Utf8

    # Rookie D's game-2 projection (as-of only game 1's single shot) should be
    # shrunk much closer to the position prior than the raw 1-game rate (2.0).
    rookie_g2 = out.filter((pl.col("player_id") == "200") & (pl.col("game_id") == "2") & (pl.col("stat") == "shots"))
    assert rookie_g2.height == 1
    assert rookie_g2["proj_mean"][0] != 1.0  # not the naive single-game rate


def test_first_game_has_no_prior_history_but_still_projects(monkeypatch):
    monkeypatch.setattr(_props_mod, "load_nhl_skater_boxscores", lambda seasons: _mini_boxscores())
    monkeypatch.setattr(_props_mod, "nhl_team_ratings", _mini_ratings)
    out = nhl_player_props(2024, stats=("shots",))
    star_g1 = out.filter((pl.col("player_id") == "100") & (pl.col("game_id") == "1") & (pl.col("stat") == "shots"))
    assert star_g1.height == 1
    assert star_g1["proj_mean"][0] > 0


def test_empty_seasons_returns_documented_schema(monkeypatch):
    monkeypatch.setattr(_props_mod, "load_nhl_skater_boxscores", lambda seasons: pl.DataFrame())
    out = nhl_player_props(2099)
    assert out.height == 0
    assert out.columns == [
        "season",
        "game_id",
        "player_id",
        "team",
        "opp_team",
        "stat",
        "proj_mean",
        "proj_sd",
        "p_over",
        "line",
    ]
