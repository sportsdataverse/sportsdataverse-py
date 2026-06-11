"""Live tests for the Fox Sports (Bifrost) NBA/MBB/NHL/MLB wrappers.

Gated behind ``SDV_PY_LIVE_TESTS=1`` (Fox is a third-party live API). Stable
ids captured 2026-06-10. CFB has its own file (tests/cfb/test_cfb_fox.py).
"""

import polars as pl

from sportsdataverse.mbb import (
    fox_mbb_boxscore,
    fox_mbb_pbp,
    fox_mbb_team_roster,
)
from sportsdataverse.mlb import (
    fox_mlb_standings,
    fox_mlb_team_gamelog,
    fox_mlb_team_roster,
)
from sportsdataverse.nba import (
    fox_nba_boxscore,
    fox_nba_league_leaders,
    fox_nba_pbp,
    fox_nba_standings,
    fox_nba_team_roster,
)
from sportsdataverse.nhl import (
    fox_nhl_boxscore,
    fox_nhl_pbp,
    fox_nhl_team_roster,
)
from tests.conftest import skip_if_no_live

pytestmark = skip_if_no_live

NBA_GAME, NBA_TEAM = "106422", "1"
MBB_GAME, MBB_TEAM = "262052", "27"
NHL_GAME, NHL_TEAM = "44398", "1"
MLB_TEAM = "1"

PBP_COLS = {"game_id", "period", "play_id", "play_text", "team"}
BOX_COLS = {"game_id", "team", "stat_group", "player", "stat", "value"}
ROSTER_COLS = {"team_id", "position_group", "player", "athlete_id"}


def _ok(df, cols=None):
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    if cols:
        assert cols.issubset(df.columns)


# ---- NBA ----
def test_fox_nba_pbp():
    _ok(fox_nba_pbp(NBA_GAME, return_as_pandas=False), PBP_COLS)


def test_fox_nba_boxscore():
    _ok(fox_nba_boxscore(NBA_GAME, return_as_pandas=False), BOX_COLS)


def test_fox_nba_team_roster():
    _ok(fox_nba_team_roster(NBA_TEAM, return_as_pandas=False), ROSTER_COLS)


def test_fox_nba_standings():
    _ok(fox_nba_standings(NBA_TEAM, return_as_pandas=False), {"team_id", "section", "entity_id"})


def test_fox_nba_league_leaders():
    _ok(fox_nba_league_leaders("scoring", return_as_pandas=False), {"entity_id"})


# ---- MBB (cbk) ----
def test_fox_mbb_pbp():
    _ok(fox_mbb_pbp(MBB_GAME, return_as_pandas=False), PBP_COLS)


def test_fox_mbb_boxscore():
    _ok(fox_mbb_boxscore(MBB_GAME, return_as_pandas=False), BOX_COLS)


def test_fox_mbb_team_roster():
    _ok(fox_mbb_team_roster(MBB_TEAM, return_as_pandas=False), ROSTER_COLS)


# ---- NHL ----
def test_fox_nhl_pbp():
    _ok(fox_nhl_pbp(NHL_GAME, return_as_pandas=False), PBP_COLS)


def test_fox_nhl_boxscore():
    _ok(fox_nhl_boxscore(NHL_GAME, return_as_pandas=False), BOX_COLS)


def test_fox_nhl_team_roster():
    _ok(fox_nhl_team_roster(NHL_TEAM, return_as_pandas=False), ROSTER_COLS)


# ---- MLB (generic only; no pbp/boxscore via Fox) ----
def test_fox_mlb_team_roster():
    _ok(fox_mlb_team_roster(MLB_TEAM, return_as_pandas=False), ROSTER_COLS)


def test_fox_mlb_standings():
    _ok(fox_mlb_standings(MLB_TEAM, return_as_pandas=False), {"team_id", "section", "entity_id"})


def test_fox_mlb_team_gamelog():
    _ok(fox_mlb_team_gamelog(MLB_TEAM, return_as_pandas=False), {"team_id", "category", "game_id", "stat", "value"})
