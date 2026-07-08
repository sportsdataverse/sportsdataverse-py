"""Tests for the public :func:`nhl_team_ratings` entry point (Task 1.3)."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import polars as pl

from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

_EXPECTED_COLS = [
    "season",
    "team",
    "adj_xgf",
    "adj_xga",
    "adj_xg_net",
    "adj_gf",
    "adj_ga",
    "games",
    "off_rank",
    "def_rank",
    "net_rank",
    "net_z",
]


def _mini_pbp():
    rows = []
    game_id = 2022020001
    dates = [dt.date(2023, 1, 1), dt.date(2023, 1, 3)]
    matchups = [("TOR", "BOS"), ("BOS", "TOR")]
    for gid_offset, (home, away) in enumerate(matchups):
        gid = game_id + gid_offset
        for team, xg in ((home, 0.30), (away, 0.20)):
            rows.append(
                {
                    "game_id": gid,
                    "event_team_abbr": team,
                    "home_abbr": home,
                    "away_abbr": away,
                    "home_skaters": 5,
                    "away_skaters": 5,
                    "home_goalie_in": 1,
                    "away_goalie_in": 1,
                    "xg": xg,
                    "event_type": "SHOT",
                }
            )
    return pl.DataFrame(rows)


def _mini_schedule():
    return pl.DataFrame(
        {
            "game_id": [2022020001, 2022020002],
            "season": [2023, 2023],
            "game_type": ["R", "R"],
            "game_date": ["2023-01-01", "2023-01-03"],
            "home_team_abbr": ["TOR", "BOS"],
            "away_team_abbr": ["BOS", "TOR"],
            "home_score": [3, 2],
            "away_score": [2, 3],
        }
    )


def test_nhl_team_ratings_schema_and_dtypes(monkeypatch):
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_pbp_full", lambda seasons: _mini_pbp())
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_schedules", lambda seasons: _mini_schedule())
    out = nhl_team_ratings(2023)
    assert out.columns == _EXPECTED_COLS
    assert out.schema["team"] == pl.Utf8
    assert out.height == 2


def test_nhl_team_ratings_as_of_date_filters(monkeypatch):
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_pbp_full", lambda seasons: _mini_pbp())
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_schedules", lambda seasons: _mini_schedule())
    out = nhl_team_ratings(2023, as_of_date=dt.date(2023, 1, 2))
    # Only the first game (2023-01-01) is strictly before the cutoff.
    assert out.filter(pl.col("team") == "TOR")["games"][0] == 1


def test_nhl_team_ratings_return_as_pandas(monkeypatch):
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_pbp_full", lambda seasons: _mini_pbp())
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_schedules", lambda seasons: _mini_schedule())
    out = nhl_team_ratings(2023, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


def test_nhl_team_ratings_empty_seasons_returns_documented_schema(monkeypatch):
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_pbp_full", lambda seasons: pl.DataFrame())
    monkeypatch.setattr("sportsdataverse.nhl.nhl_loaders.load_nhl_schedules", lambda seasons: pl.DataFrame())
    out = nhl_team_ratings(2099)
    assert out.height == 0
    assert out.columns == _EXPECTED_COLS
