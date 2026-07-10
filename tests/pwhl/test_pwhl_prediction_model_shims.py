"""Tests for the PWHL model shims (Task 5.2) -- by-reference re-exports of
each NHL prediction-spine model, defaulting ``league="pwhl"``.

PWHL's ``load_pwhl_pbp`` was confirmed at grounding to carry a categorical
``shot_quality`` column (not a numeric ``xg``) and no ``home_skaters``/
``away_skaters``/``home_goalie_in``/``away_goalie_in`` even-strength columns
-- a fundamentally different shape from the NHL pbp the rating engine needs.
Per the design spec (Sec 9-7), the PWHL oracle gate is explicitly deferred
until xG-bearing PWHL pbp lands in sdv-py; these tests only verify the
wiring (the shim resolves to the right function, defaults the right league,
and returns a correctly-shaped frame on synthetic input) -- not a real PWHL
oracle validation.
"""

from __future__ import annotations

import importlib

import polars as pl

# The nhl package __init__ exports functions (nhl_player_props,
# nhl_team_ratings) whose names shadow their submodules, so
# `import sportsdataverse.nhl.nhl_player_props as m` can bind the FUNCTION, not
# the module -- and monkeypatch.setattr would patch the wrong object.
# importlib.import_module reads sys.modules and returns the same real module
# the shims delegate through.
nhl_market = importlib.import_module("sportsdataverse.nhl.nhl_market")
nhl_props = importlib.import_module("sportsdataverse.nhl.nhl_player_props")
nhl_ratings = importlib.import_module("sportsdataverse.nhl.nhl_team_ratings")
pwhl_market = importlib.import_module("sportsdataverse.pwhl.pwhl_market")
pwhl_props = importlib.import_module("sportsdataverse.pwhl.pwhl_player_props")
pwhl_ratings = importlib.import_module("sportsdataverse.pwhl.pwhl_team_ratings")


def test_pwhl_team_ratings_defaults_league_pwhl(monkeypatch):
    captured = {}

    def fake_nhl_team_ratings(seasons, *, league="nhl", **kwargs):
        captured["league"] = league
        return pl.DataFrame({"season": [2024], "team": ["MTL"], "adj_xg_net": [0.1]})

    monkeypatch.setattr(nhl_ratings, "nhl_team_ratings", fake_nhl_team_ratings)
    out = pwhl_ratings.pwhl_team_ratings(2024)
    assert captured["league"] == "pwhl"
    assert out.height == 1


def test_pwhl_predict_games_defaults_league_pwhl(monkeypatch):
    captured = {}

    def fake_nhl_predict_games(games, ratings, *, league="nhl", **kwargs):
        captured["league"] = league
        return pl.DataFrame({"game_id": ["1"], "home_win_prob": [0.5]})

    monkeypatch.setattr(nhl_market, "nhl_predict_games", fake_nhl_predict_games)
    out = pwhl_market.pwhl_predict_games(pl.DataFrame(), pl.DataFrame())
    assert captured["league"] == "pwhl"
    assert out.height == 1


def test_pwhl_in_game_win_prob_defaults_league_pwhl(monkeypatch):
    captured = {}

    def fake_nhl_in_game_win_prob(pbp, pregame_home_prob, *, league="nhl", **kwargs):
        captured["league"] = league
        return pl.DataFrame({"home_win_prob": [0.6]})

    monkeypatch.setattr(nhl_market, "nhl_in_game_win_prob", fake_nhl_in_game_win_prob)
    out = pwhl_market.pwhl_in_game_win_prob(pl.DataFrame({"x": [1]}), 0.5)
    assert captured["league"] == "pwhl"
    assert out.height == 1


def test_pwhl_player_props_defaults_league_pwhl(monkeypatch):
    captured = {}

    def fake_nhl_player_props(seasons, *, league="nhl", **kwargs):
        captured["league"] = league
        return pl.DataFrame({"player_id": ["1"], "stat": ["shots"]})

    monkeypatch.setattr(nhl_props, "nhl_player_props", fake_nhl_player_props)
    out = pwhl_props.pwhl_player_props(2024)
    assert captured["league"] == "pwhl"
    assert out.height == 1


def test_pwhl_game_total_defaults_league_pwhl(monkeypatch):
    captured = {}

    def fake_nhl_game_total(games, ratings, *, league="nhl", **kwargs):
        captured["league"] = league
        return pl.DataFrame({"game_id": ["1"], "exp_total": [5.5]})

    monkeypatch.setattr(nhl_props, "nhl_game_total", fake_nhl_game_total)
    out = pwhl_props.pwhl_game_total(pl.DataFrame(), pl.DataFrame())
    assert captured["league"] == "pwhl"
    assert out.height == 1
