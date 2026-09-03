"""Live tests for the Fox Sports (Bifrost) CFB wrappers (``fox_cfb_*``).

Gated behind ``SDV_PY_LIVE_TESTS=1`` (Fox is a third-party live API). Stable
ids: game 41616 (completed 2025 FSU-Kent St.), team 11 (Miami FL).

The team ID is stable but a team's *roster* is not: Fox serves the roster for
the current season, which sits empty between the bowls and fall camp. Prefer the
completed GAME for assertions that just need populated data.
"""

import pandas as pd
import polars as pl
import pytest

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


def _fox_roster_athlete_rows(team_id: str) -> int:
    """How many athlete rows Fox is actually serving for ``team_id``.

    Fox publishes the roster for the CURRENT season only. Between the bowls and
    fall camp the table is unpopulated: the payload is titled e.g. "2026 Miami
    (FL) Hurricanes Roster" but ships a single summary group whose lone row reads
    ``PLAYER COUNT: 0`` and carries no ``entityLink`` (verified July 2026 across
    teams 11 / 2 / 8 / 52 -- it is league-wide, not a bad team id).

    Asserting a row count against that is testing Fox's calendar, not our parser,
    so the roster tests below skip when this is 0. Counting from the RAW payload
    (not the parsed frame) is what keeps the tests honest: if Fox IS serving
    players and our parser drops them, this returns > 0 and the assertions still
    fire.
    """
    raw = fox_cfb_team_roster(team_id, return_parsed=False)
    return sum(
        1
        for g in (raw.get("groups") or [])
        for r in (g.get("rows") or [])
        if "athletes/" in ((r.get("entityLink") or {}).get("contentUri") or "")
    )


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
    if _fox_roster_athlete_rows(TEAM) == 0:
        pytest.skip("Fox has not populated the current-season roster for this team (off-season)")
    assert len(df) > 0
    assert {"team_id", "player", "pos", "athlete_id"}.issubset(df.columns)


def test_fox_cfb_team_stats():
    df = fox_cfb_team_stats(TEAM, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"team_id", "category", "stat", "player", "value"}.issubset(df.columns)


def test_fox_cfb_team_gamelog():
    """The schema is asserted unconditionally; the row count only when Fox has rows.

    A bare ``len(df) > 0`` here fails every offseason and every week 1 -- Fox
    returns ``sectionList: []`` for a team with no games logged yet, which is an
    upstream state, not a defect. Asserting the columns regardless is what keeps
    this test able to catch a real parser break: if the parser stopped emitting
    its keys, the column assertion fails whether or not Fox had data.
    """
    raw = fox_cfb_team_gamelog(TEAM, return_parsed=False)
    df = fox_cfb_team_gamelog(TEAM, return_as_pandas=False)
    assert isinstance(df, pl.DataFrame)
    assert {"team_id", "season_type", "category", "game_id", "stat", "value"}.issubset(df.columns)
    if raw.get("sectionList"):
        assert len(df) > 0


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
    # Uses the completed-2025 game, not the roster: this asserts the
    # return_as_pandas flag, and GAME's data is settled where the roster is
    # season-dependent (see _fox_roster_athlete_rows).
    df = fox_cfb_pbp(GAME, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_fox_cfb_return_parsed_false():
    raw = fox_cfb_pbp(GAME, return_parsed=False)
    assert isinstance(raw, dict)
    assert "pbp" in raw
