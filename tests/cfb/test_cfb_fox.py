"""Live tests for the Fox Sports (Bifrost) CFB wrappers (``fox_cfb_*``).

Gated behind ``SDV_PY_LIVE_TESTS=1`` (Fox is a third-party live API). Stable
ids: game 41616 (completed 2025 FSU-Kent St.), team 11 (Miami FL).
"""

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_fox_ext import (
    fox_cfb_boxscore,
    fox_cfb_league_leaders,
    fox_cfb_odds,
    fox_cfb_pbp,
    fox_cfb_standings,
    fox_cfb_team_gamelog,
    fox_cfb_team_roster,
    fox_cfb_team_stats,
)
from tests.conftest import skip_if_no_live

pytestmark = skip_if_no_live

GAME = "41616"
TEAM = "11"


def test_fox_cfb_pbp():
    df = fox_cfb_pbp(GAME, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"game_id", "quarter", "drive_result", "play_text"}.issubset(df.columns)


def test_fox_cfb_boxscore():
    df = fox_cfb_boxscore(GAME, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"game_id", "team", "stat_group", "player", "stat", "value"}.issubset(df.columns)


def test_fox_cfb_team_roster():
    df = fox_cfb_team_roster(TEAM, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"team_id", "player", "pos", "athlete_id"}.issubset(df.columns)


def test_fox_cfb_team_stats():
    df = fox_cfb_team_stats(TEAM, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"team_id", "category", "stat", "player", "value"}.issubset(df.columns)


def test_fox_cfb_team_gamelog():
    df = fox_cfb_team_gamelog(TEAM, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"team_id", "season_type", "category", "game_id", "stat", "value"}.issubset(df.columns)


def test_fox_cfb_standings():
    df = fox_cfb_standings(TEAM, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"team_id", "section", "entity_id"}.issubset(df.columns)


def test_fox_cfb_league_leaders():
    df = fox_cfb_league_leaders("passing", return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert "entity_id" in df.columns


def test_fox_cfb_odds():
    # The matchup six-pack market is ephemeral (~60s TTL); tolerate an empty frame.
    df = fox_cfb_odds(GAME, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    if len(df) > 0:
        assert {"game_id", "team", "spread", "to_win", "total"}.issubset(df.columns)


def test_fox_cfb_return_as_pandas():
    df = fox_cfb_team_roster(TEAM, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_fox_cfb_return_parsed_false():
    raw = fox_cfb_pbp(GAME, return_parsed=False)
    assert isinstance(raw, dict)
    assert "pbp" in raw
