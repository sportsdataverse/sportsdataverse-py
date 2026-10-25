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
import socket
from pathlib import Path

import polars as pl
import pytest

# The nhl package __init__ exports functions (nhl_player_props,
# nhl_team_ratings) whose names shadow their submodules, so
# `import sportsdataverse.nhl.nhl_player_props as m` can bind the FUNCTION, not
# the module -- and monkeypatch.setattr would patch the wrong object.
# importlib.import_module reads sys.modules and returns the same real module
# the shims delegate through.
nhl_loaders = importlib.import_module("sportsdataverse.nhl.nhl_loaders")
nhl_market = importlib.import_module("sportsdataverse.nhl.nhl_market")
nhl_props = importlib.import_module("sportsdataverse.nhl.nhl_player_props")
nhl_ratings = importlib.import_module("sportsdataverse.nhl.nhl_team_ratings")
pwhl_loaders = importlib.import_module("sportsdataverse.pwhl.pwhl_loaders")
pwhl_market = importlib.import_module("sportsdataverse.pwhl.pwhl_market")
pwhl_props = importlib.import_module("sportsdataverse.pwhl.pwhl_player_props")
pwhl_ratings = importlib.import_module("sportsdataverse.pwhl.pwhl_team_ratings")


FIX = Path(__file__).parent.parent / "fixtures" / "pwhl_player_impact"
_PWHL_SLICES = {
    "load_pwhl_pbp": "play_by_play_2025_slice.parquet",
    "load_pwhl_schedules": "pwhl_schedule_2025_slice.parquet",
    "load_pwhl_game_info": "game_info_2025_slice.parquet",
    "load_pwhl_skater_boxscores": "skater_box_2025_slice.parquet",
}
_NHL_LOADERS = ("load_nhl_pbp_full", "load_nhl_schedules", "load_nhl_skater_boxscores")


@pytest.fixture
def loader_calls(monkeypatch):
    """Sockets closed, every NHL loader refusing, every PWHL loader serving a real 2025 slice."""
    calls: list[str] = []

    def no_network(*args, **kwargs):
        raise OSError("offline test: no sockets")

    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(socket.socket, "connect", no_network)

    def refuse(name):
        def _loader(*args, **kwargs):
            calls.append(name)
            raise AssertionError(f"PWHL call reached the NHL loader {name}{args}")

        return _loader

    for name in _NHL_LOADERS:
        monkeypatch.setattr(nhl_loaders, name, refuse(name))
    monkeypatch.setattr(nhl_props, "load_nhl_skater_boxscores", refuse("load_nhl_skater_boxscores"))

    def serve(name, fname):
        def _loader(seasons, *args, **kwargs):
            calls.append(name)
            return pl.read_parquet(FIX / fname)

        return _loader

    for name, fname in _PWHL_SLICES.items():
        monkeypatch.setattr(pwhl_loaders, name, serve(name, fname))
    return calls


def test_pwhl_team_ratings_routing_reads_pwhl_loaders_only(loader_calls):
    out = pwhl_ratings.pwhl_team_ratings(2025)

    assert set(loader_calls) == {"load_pwhl_pbp", "load_pwhl_schedules", "load_pwhl_game_info"}
    teams = set(pl.read_parquet(FIX / "pwhl_schedule_2025_slice.parquet")["home_team"].to_list())
    assert out.height > 0
    assert set(out["team"].to_list()) <= teams | set(
        pl.read_parquet(FIX / "pwhl_schedule_2025_slice.parquet")["away_team"].to_list()
    )


def test_pwhl_player_props_routing_reads_pwhl_loaders_only(loader_calls):
    out = pwhl_props.pwhl_player_props(2025, stats=("shots",))

    assert set(loader_calls) == set(_PWHL_SLICES)
    box = pl.read_parquet(FIX / "skater_box_2025_slice.parquet")
    skaters = set(box.filter(pl.col("position") != "G")["player_id"].cast(pl.Int64).to_list())
    assert out.height > 0
    assert set(out["player_id"].to_list()) <= skaters
    # every opponent resolved, so the PWHL ratings fed the matchup multiplier
    assert out["opp_team"].null_count() == 0


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


def test_pwhl_team_ratings_league_keyword_is_deprecated_and_ignored(loader_calls):
    with pytest.warns(DeprecationWarning, match=r"pwhl_team_ratings\(league=\.\.\.\) is deprecated"):
        out = pwhl_ratings.pwhl_team_ratings(2025, league="nhl")
    assert out.equals(pwhl_ratings.pwhl_team_ratings(2025))
    assert "load_pwhl_pbp" in loader_calls


def test_pwhl_player_props_skips_a_season_without_game_info(loader_calls, monkeypatch):
    # an unpublished release season: the loader warns and returns a column-less frame
    monkeypatch.setattr(pwhl_loaders, "load_pwhl_game_info", lambda seasons, *a, **k: pl.DataFrame())
    out = pwhl_props.pwhl_player_props(2025, stats=("shots",))
    assert out.height == 0
    assert "player_id" in out.columns
