"""Offline parser tests for the universal-ESPN parsers in
``sportsdataverse._common_espn_parsers``.

These tests run against captured live payloads in
``tests/fixtures/espn/`` so the parser logic can be exercised without
hitting ESPN. Captures were collected 2026-05-23.

Live integration tests live in ``test_espn_live.py`` (gated by
``SDV_PY_LIVE_TESTS=1``); the offline tests here cover schemas and the
``return_parsed=True`` shim contract end to end.
"""

from __future__ import annotations

import functools
import importlib
import inspect
import json
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest

from tests.conftest import load_fixture


def _load(stem: str) -> dict:
    """Local alias for :func:`tests.conftest.load_fixture` bound to
    this module's fixture category ("espn")."""
    return load_fixture("espn", stem)


# ===========================================================================
# Team-scoped Site v2
# ===========================================================================


def test_parse_team_schedule_returns_one_row_per_event():
    from sportsdataverse._common_espn_parsers import parse_team_schedule

    df = parse_team_schedule(_load("team_schedule_nba"))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 1, "expected at least one scheduled event row"
    # Site v2 schedule events always carry id, date, name, season_year
    for col in ("id", "date", "name", "season_year"):
        assert col in df.columns, f"missing column {col!r}"


def test_parse_team_roster_returns_one_row_per_athlete():
    from sportsdataverse._common_espn_parsers import parse_team_roster

    df = parse_team_roster(_load("team_roster_nba"))
    assert isinstance(df, pl.DataFrame)
    # LAL has ~17 active players in this capture; tolerate roster churn
    assert df.height >= 10, f"expected >=10 athletes, got {df.height}"
    for col in ("id", "first_name", "last_name", "full_name"):
        assert col in df.columns, f"missing column {col!r}"


# ===========================================================================
# News / injuries
# ===========================================================================


def test_parse_news_returns_one_row_per_article():
    from sportsdataverse._common_espn_parsers import parse_news

    df = parse_news(_load("news_nba"))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 1, "expected at least one article row"
    for col in ("id", "headline", "type"):
        assert col in df.columns, f"missing column {col!r}"


def test_parse_injuries_returns_one_row_per_team():
    from sportsdataverse._common_espn_parsers import parse_injuries

    df = parse_injuries(_load("injuries_nba"))
    assert isinstance(df, pl.DataFrame)
    # Live snapshot shows ~26 teams with injuries reported.
    assert df.height >= 1, f"expected >=1 team rows, got {df.height}"
    assert "id" in df.columns
    assert "display_name" in df.columns
    # The inner per-player injuries list is stringified for polars ingestion.
    assert "injuries" in df.columns


# ===========================================================================
# Generic items (Core v2 paginated + entries variants)
# ===========================================================================


@pytest.mark.parametrize(
    "fixture,expected_min_rows",
    [
        ("venues_core_nba", 1),  # 5-row capture, $ref-only items
        ("events_core_nba", 1),  # 1-row capture, $ref-only items
    ],
)
def test_parse_items_handles_items_key(fixture, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load(fixture))
    assert df.height >= expected_min_rows, f"{fixture}: expected >= {expected_min_rows} rows, got {df.height}"
    # Core v2 paginated items often contain just $ref pointers
    assert any(c.endswith("ref") or c == "_ref" or c == "$ref" for c in df.columns), f"no $ref column in {df.columns}"


def test_parse_items_handles_entries_key_for_statisticslog():
    """athlete_statisticslog ships ``{entries: [...]}`` not ``{items: [...]}``;
    parse_items's fallback key list should still find the rows."""
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load("athlete_statslog_lbj"))
    assert df.height >= 1, "parse_items must fall back to the 'entries' key for athlete_statisticslog payloads"


# ===========================================================================
# Empty-payload contract for ALL new parsers
# ===========================================================================


@pytest.mark.parametrize(
    "parser_name",
    [
        "parse_items",
        "parse_team_schedule",
        "parse_team_roster",
        "parse_news",
        "parse_injuries",
    ],
)
def test_new_parser_handles_empty_payload(parser_name):
    from sportsdataverse import _common_espn_parsers as parsers

    parser = getattr(parsers, parser_name)
    df = parser({})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0, f"{parser_name} should return 0 rows on empty"


# ===========================================================================
# pandas opt-in
# ===========================================================================


def test_parse_team_roster_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse._common_espn_parsers import parse_team_roster

    df = parse_team_roster(_load("team_roster_nba"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)


# ===========================================================================
# Registry expansion contract
# ===========================================================================


def test_endpoint_parsers_registry_includes_new_short_names():
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS

    # The five new families we just registered.
    expected_new = {
        "team_schedule",
        "team_roster",
        "news",
        "team_news",
        "athlete_news",
        "injuries",
        "team_injuries",
        "athlete_injuries",
        "venues",
        "franchises",
        "events",
        "athletes_index",
        "athlete_statisticslog",
    }
    missing = expected_new - set(ENDPOINT_PARSERS)
    assert not missing, f"ENDPOINT_PARSERS missing newly-introduced entries: {sorted(missing)}"


def test_return_parsed_kwarg_is_wired_for_new_short_names():
    """A wrapper for a newly-registered parser should now accept
    ``return_parsed=True`` and ``return_as_pandas=True`` kwargs."""
    import inspect

    from sportsdataverse.nba.nba_espn_ext import (
        espn_nba_news,
        espn_nba_team_roster,
        espn_nba_team_schedule,
        espn_nba_venues,
    )

    for fn in (espn_nba_team_schedule, espn_nba_team_roster, espn_nba_news, espn_nba_venues):
        sig = inspect.signature(fn)
        assert "return_parsed" in sig.parameters, f"{fn.__name__} missing return_parsed kwarg"
        assert "return_as_pandas" in sig.parameters, f"{fn.__name__} missing return_as_pandas kwarg"


def test_summary_parsers_handle_real_nba_summary_payload():
    """Live-captured Site v2 summary for the 2024 NBA Finals G5 (event
    401585607). Verifies all 5 sub-parsers + the dispatcher against a
    real ~700KB payload."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary,
        parse_summary_boxscore_player,
        parse_summary_boxscore_team,
        parse_summary_leaders,
        parse_summary_plays,
        parse_summary_winprobability,
    )

    payload = _load("summary_nba")

    # ---- boxscore_player ----
    df = parse_summary_boxscore_player(payload)
    assert df.height >= 20, f"expected >=20 athletes, got {df.height}"
    for col in ("team_id", "team_abbreviation", "athlete_id", "athlete_display_name", "starter", "active"):
        assert col in df.columns, f"missing column {col!r}"

    # ---- boxscore_team ----
    df = parse_summary_boxscore_team(payload)
    # 2 teams x ~23 stats each ~ 46 rows
    assert df.height >= 20, f"expected >=20 team-stat rows, got {df.height}"
    for col in ("team_id", "stat_name", "stat_label", "stat_display_value"):
        assert col in df.columns

    # ---- plays ----
    df = parse_summary_plays(payload)
    assert df.height >= 100, f"expected >=100 plays, got {df.height}"
    for col in ("id", "sequence_number", "text", "home_score", "away_score"):
        assert col in df.columns

    # ---- winprobability ----
    df = parse_summary_winprobability(payload)
    assert df.height >= 100, f"expected >=100 wp ticks, got {df.height}"
    for col in ("home_win_percentage", "tie_percentage", "play_id"):
        assert col in df.columns

    # ---- leaders ----
    df = parse_summary_leaders(payload)
    assert df.height >= 4, f"expected >=4 (team x category) leader rows, got {df.height}"
    for col in ("team_id", "category_name", "athlete_id", "value"):
        assert col in df.columns

    # ---- dispatcher: section=None returns dict of all 18 sections ----
    out = parse_summary(payload)
    # The 5 original sub-parsers should all return non-empty frames for
    # the NBA Finals fixture; the new sections may legitimately be empty
    # (e.g. broadcasts, pickcenter, odds, against_the_spread are sparse
    # for past games — assert presence in the dict, not non-emptiness).
    must_be_present = {
        "boxscore_player",
        "boxscore_team",
        "plays",
        "winprobability",
        "leaders",
        "game_info",
        "officials",
        "header",
        "season_series",
        "against_the_spread",
        "standings",
        "broadcasts",
        "format",
        "pickcenter",
        "odds",
        "article",
        "injuries",
        "news",
        # NFL / CFB only — return empty frames for NBA but still present
        "drives",
        "drive_plays",
        "scoring_plays",
    }
    assert set(out) == must_be_present, (
        f"dispatcher returned unexpected section set: "
        f"missing={must_be_present - set(out)}, extra={set(out) - must_be_present}"
    )
    # The original 5 sub-parsers + the always-present-on-real-games
    # sections must be non-empty for this fixture
    for name in (
        "boxscore_player",
        "boxscore_team",
        "plays",
        "winprobability",
        "leaders",
        "game_info",
        "officials",
        "header",
        "standings",
        "format",
        "article",
        "injuries",
        "news",
    ):
        assert out[name].height > 0, (
            f"dispatcher returned empty frame for {name!r} on the NBA Finals fixture (expected non-empty)"
        )

    # ---- dispatcher: section="plays" returns just that frame ----
    single = parse_summary(payload, section="plays")
    assert single.height >= 100


def test_summary_section_parsers_individually():
    """Spot-check each of the 13 NEW summary section parsers against the
    captured 2024 NBA Finals payload."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary_against_the_spread,
        parse_summary_article,
        parse_summary_broadcasts,
        parse_summary_format,
        parse_summary_game_info,
        parse_summary_header,
        parse_summary_injuries,
        parse_summary_news,
        parse_summary_odds,
        parse_summary_officials,
        parse_summary_pickcenter,
        parse_summary_season_series,
        parse_summary_standings,
    )

    payload = _load("summary_nba")

    # Single-row parsers
    assert parse_summary_game_info(payload).height == 1
    assert "venue_id" in parse_summary_game_info(payload).columns
    assert parse_summary_header(payload).height == 1
    assert "id" in parse_summary_header(payload).columns
    assert parse_summary_format(payload).height == 1
    assert any("regulation" in c for c in parse_summary_format(payload).columns)
    assert parse_summary_article(payload).height == 1
    assert "headline" in parse_summary_article(payload).columns

    # Multi-row parsers
    officials = parse_summary_officials(payload)
    assert officials.height >= 3, "expected >=3 officials per NBA game"
    assert "full_name" in officials.columns

    standings = parse_summary_standings(payload)
    assert standings.height >= 5, "expected >=5 teams in the conference standings"
    assert "team_id" in standings.columns
    assert "group_header" in standings.columns

    injuries = parse_summary_injuries(payload)
    assert injuries.height >= 1
    assert "id" in injuries.columns or "team_id" in injuries.columns

    news = parse_summary_news(payload)
    # news.articles is a list; sparse on some fixtures, present here
    assert news.height >= 1
    assert "headline" in news.columns

    season_series = parse_summary_season_series(payload)
    assert season_series.height >= 1
    assert "title" in season_series.columns

    # Sparse sections — should return zero-row frames without raising
    assert parse_summary_against_the_spread(payload).height == 0
    assert parse_summary_broadcasts(payload).height == 0
    assert parse_summary_pickcenter(payload).height == 0
    assert parse_summary_odds(payload).height == 0


# ===========================================================================
# Cross-league parity for the team-scoped + league-wide universal parsers
# ===========================================================================
#
# Captures from MLB / NFL / NHL / WNBA (the original NBA captures cover
# the NBA case in the dedicated tests above). Proves that the universal
# parsers handle every league's payload shape — same code path, no
# league-specific tweaks.
#
# Discovered shape divergence (handled by parse_team_roster):
#   NBA / WNBA / MBB / WBB / CFB ship ``athletes[]`` as a flat list of
#   player dicts. MLB / NFL / NHL wrap players in position groups —
#   ``athletes[i].position = "Pitchers"`` / ``"offense"`` /
#   ``"Centers"`` etc., with the players in ``athletes[i].items[]``.
#   parse_team_roster auto-detects and unrolls the grouped shape,
#   tagging each player with a ``position_group`` column.


@pytest.mark.parametrize(
    "league,expected_min_rows,grouped",
    [
        # Flat shape (no position_group column)
        ("nba", 12, False),
        ("wnba", 10, False),
        ("mbb", 10, False),  # NCAA M basketball — flat (~14 players)
        ("wbb", 10, False),  # NCAA W basketball — flat (~12 players)
        # Position-grouped shape (rows include position_group column)
        ("mlb", 20, True),  # 4 groups × ~6 players
        ("nfl", 50, True),  # 3 groups × full roster
        ("nhl", 18, True),  # 5 groups × ~5 players
        ("cfb", 60, True),  # CFB: offense + defense + specialTeam (~100 players)
    ],
)
def test_parse_team_roster_handles_both_shapes_across_leagues(
    league,
    expected_min_rows,
    grouped,
):
    from sportsdataverse._common_espn_parsers import parse_team_roster

    df = parse_team_roster(_load(f"team_roster_{league}"))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= expected_min_rows, f"{league}: expected >= {expected_min_rows} players, got {df.height}"
    # Universal columns from the athlete sub-dict
    for col in ("id", "first_name", "last_name", "full_name"):
        assert col in df.columns, f"{league}: missing {col!r}"
    # Position-grouped leagues add a column the flat-shape ones don't
    if grouped:
        assert "position_group" in df.columns, f"{league}: expected position_group column for grouped shape"
        # All values should be non-null strings
        groups = set(df["position_group"].to_list())
        assert all(isinstance(g, str) and g for g in groups), (
            f"{league}: position_group has null/empty values: {groups}"
        )
    else:
        # Flat-shape leagues should NOT have a stray position_group col
        # (would indicate accidental column from one of the player records)
        assert "position_group" not in df.columns


@pytest.mark.parametrize(
    "league,expected_min_rows",
    [
        ("nba", 10),
        ("mlb", 100),  # full MLB season = 162 games
        ("nfl", 15),  # 17 regular + playoffs
        # NHL skipped: off-season capture returned an empty schedule
        ("wnba", 30),  # 40+ regular-season games
        ("mbb", 25),  # NCAA M basketball season (~30-35 games + tournaments)
        ("wbb", 25),  # NCAA W basketball season similar size
        # CFB skipped: off-season capture (college football is fall-only)
    ],
)
def test_parse_team_schedule_works_across_leagues(league, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_team_schedule

    df = parse_team_schedule(_load(f"team_schedule_{league}"))
    assert df.height >= expected_min_rows, f"{league}: expected >= {expected_min_rows} events, got {df.height}"
    for col in ("id", "date", "name", "season_year"):
        assert col in df.columns


@pytest.mark.parametrize("league", ["nhl", "cfb"])
def test_parse_team_schedule_handles_empty_offseason_payload(league):
    """``team_schedule`` captures for sports in their off-season return
    no events — the parser must return a zero-row frame, not raise.
    NHL was off-season for the May 2026 capture; CFB likewise (fall sport
    captured mid-summer)."""
    from sportsdataverse._common_espn_parsers import parse_team_schedule

    df = parse_team_schedule(_load(f"team_schedule_{league}"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


@pytest.mark.parametrize(
    "league,expected_min_rows",
    [
        ("nba", 3),
        ("mlb", 3),
        ("nfl", 3),
        ("nhl", 3),
        ("wnba", 3),
        ("mbb", 3),
        ("wbb", 3),
        ("cfb", 3),
    ],
)
def test_parse_news_works_across_leagues(league, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_news

    df = parse_news(_load(f"news_{league}"))
    assert df.height >= expected_min_rows, f"{league}: expected >= {expected_min_rows} articles, got {df.height}"
    for col in ("id", "headline", "type"):
        assert col in df.columns


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_parse_injuries_empty_for_ncaa_basketball(league):
    """ESPN's league-wide injuries endpoint isn't published for NCAA
    M/W basketball — the payload is `{}` (empty). The parser must
    return a zero-row frame instead of raising."""
    from sportsdataverse._common_espn_parsers import parse_injuries

    df = parse_injuries(_load(f"injuries_{league}"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


@pytest.mark.parametrize(
    "league,expected_min_rows",
    [
        # All 5 pro / WNBA leagues consistently have >= 5 teams reporting
        ("nba", 10),
        ("mlb", 10),
        ("nfl", 10),
        ("nhl", 10),
        ("wnba", 5),  # smaller league, lower floor
        ("cfb", 10),  # CFB has consistent injury reporting
        # MBB / WBB intentionally omitted — ESPN does not surface NCAA
        # basketball injury reports as a league-wide endpoint; both fixtures
        # return empty payloads. Verified separately in
        # test_parse_injuries_empty_for_ncaa_basketball below.
    ],
)
def test_parse_injuries_works_across_leagues(league, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_injuries

    df = parse_injuries(_load(f"injuries_{league}"))
    assert df.height >= expected_min_rows, (
        f"{league}: expected >= {expected_min_rows} teams w/ injuries, got {df.height}"
    )
    for col in ("id", "display_name", "injuries"):
        assert col in df.columns


# ===========================================================================
# Cross-league summary parity
# ===========================================================================
#
# The summary endpoint shape is mostly identical across ESPN sports, but
# there are real per-sport variations (NFL uses drives.previous[] instead
# of top-level plays; NHL doesn't ship per-play win-prob; MLB ships
# different stat keys in boxscore). These tests prove the parsers
# survive every documented variation by running the dispatcher against
# captured fixtures for 5 leagues (NBA / MLB / NFL / NHL / WNBA) and
# asserting the per-sport contract.


SUMMARY_FIXTURES = [
    "summary_nba",
    "summary_mlb",
    "summary_nfl",
    "summary_nhl",
    "summary_wnba",
    "summary_mbb",
    "summary_wbb",
    "summary_cfb",
]


@pytest.mark.parametrize("fixture", SUMMARY_FIXTURES)
def test_summary_dispatcher_returns_full_section_dict_for_every_league(fixture):
    """Each league's summary fixture must produce a complete dict of
    sub-frames from the dispatcher — no missing keys, no exceptions."""
    import polars as pl

    from sportsdataverse._common_espn_parsers import (
        SUMMARY_SECTION_PARSERS,
        parse_summary,
    )

    payload = _load(fixture)
    out = parse_summary(payload)
    assert isinstance(out, dict)
    assert set(out) == set(SUMMARY_SECTION_PARSERS), f"{fixture}: dispatcher returned different sections than registry"
    for name, frame in out.items():
        assert isinstance(frame, pl.DataFrame), f"{fixture}: section {name!r} returned {type(frame)}"


@pytest.mark.parametrize(
    "fixture,min_athletes",
    [
        ("summary_nba", 20),  # 2 NBA rosters, ~24-27 active
        ("summary_mlb", 20),  # 2 MLB rosters, ~26-32 with starters + bullpen
        ("summary_nfl", 50),  # 2 NFL rosters, ~70-90 with full game-day squad
        ("summary_nhl", 20),  # 2 NHL rosters, skaters + goalies
        ("summary_wnba", 15),  # 2 WNBA rosters, ~12-15 each
        ("summary_mbb", 20),  # 2 NCAA M basketball rosters, ~25-35 combined
        ("summary_wbb", 20),  # 2 NCAA W basketball rosters
        ("summary_cfb", 50),  # 2 CFB rosters, ~70+ each (huge squads)
    ],
)
def test_summary_boxscore_player_works_across_leagues(fixture, min_athletes):
    from sportsdataverse._common_espn_parsers import parse_summary_boxscore_player

    df = parse_summary_boxscore_player(_load(fixture))
    assert df.height >= min_athletes, f"{fixture}: expected >={min_athletes} athletes, got {df.height}"
    for col in ("team_id", "team_abbreviation", "athlete_id", "athlete_display_name"):
        assert col in df.columns, f"{fixture}: missing {col!r}"


@pytest.mark.parametrize("fixture", SUMMARY_FIXTURES)
def test_summary_boxscore_team_works_across_leagues(fixture):
    """Every league ships per-team statistics. Row counts vary by sport
    (NBA ~23 stats × 2 teams, MLB ~4 × 2 = 8, NHL ~14 × 2 = 28)."""
    from sportsdataverse._common_espn_parsers import parse_summary_boxscore_team

    df = parse_summary_boxscore_team(_load(fixture))
    assert df.height > 0, f"{fixture}: expected per-team stats, got 0 rows"
    for col in ("team_id", "stat_name", "stat_label"):
        assert col in df.columns


@pytest.mark.parametrize("fixture", SUMMARY_FIXTURES)
def test_summary_game_info_works_across_leagues(fixture):
    from sportsdataverse._common_espn_parsers import parse_summary_game_info

    df = parse_summary_game_info(_load(fixture))
    assert df.height == 1, f"{fixture}: game_info should be 1 row"
    assert any(c.startswith("venue_") for c in df.columns), f"{fixture}: no venue_* columns"


@pytest.mark.parametrize(
    "fixture,min_officials",
    [
        # NBA=3, NHL=6, MLB=6, NFL=7, WNBA=4 — pro leagues consistently >=3
        ("summary_nba", 3),
        ("summary_mlb", 3),
        ("summary_nfl", 3),
        ("summary_nhl", 3),
        ("summary_wnba", 3),
        ("summary_mbb", 3),  # MBB final has 3 officials
        # NCAA-side leagues sometimes ship fewer or zero officials:
        # WBB final = 2; CFB national championship = 0 in capture.
        ("summary_wbb", 0),  # accept any (parser must not raise)
        ("summary_cfb", 0),
    ],
)
def test_summary_officials_works_across_leagues(fixture, min_officials):
    from sportsdataverse._common_espn_parsers import parse_summary_officials

    df = parse_summary_officials(_load(fixture))
    assert df.height >= min_officials, f"{fixture}: expected >={min_officials} officials, got {df.height}"
    if df.height > 0:
        assert "full_name" in df.columns or "display_name" in df.columns


@pytest.mark.parametrize("fixture", SUMMARY_FIXTURES)
def test_summary_header_works_across_leagues(fixture):
    from sportsdataverse._common_espn_parsers import parse_summary_header

    df = parse_summary_header(_load(fixture))
    assert df.height == 1
    assert "id" in df.columns


@pytest.mark.parametrize("fixture", SUMMARY_FIXTURES)
def test_summary_standings_works_across_leagues(fixture):
    """Standings appear in every league summary; row counts reflect
    conference / division structure."""
    from sportsdataverse._common_espn_parsers import parse_summary_standings

    df = parse_summary_standings(_load(fixture))
    assert df.height >= 5, f"{fixture}: expected >=5 teams in standings, got {df.height}"
    assert "team_id" in df.columns


def test_summary_plays_works_for_non_football_leagues():
    """NBA / MLB / WNBA / NHL / MBB / WBB ship plays at the top level.
    NFL + CFB use drives.previous[] instead, which is exercised
    separately."""
    from sportsdataverse._common_espn_parsers import parse_summary_plays

    for fixture in ("summary_nba", "summary_mlb", "summary_nhl", "summary_wnba", "summary_mbb", "summary_wbb"):
        df = parse_summary_plays(_load(fixture))
        assert df.height >= 100, f"{fixture}: expected >=100 plays, got {df.height}"


@pytest.mark.parametrize(
    "fixture,min_drives,min_scoring",
    [
        ("summary_nfl", 10, 1),  # Super Bowl LIX: 26 drives + 11 scoring
        ("summary_cfb", 10, 1),  # OSU vs ND nat'l championship: 17 drives + 9 scoring
    ],
)
def test_summary_drives_works_for_football_leagues(fixture, min_drives, min_scoring):
    """NFL + CFB summaries ship drives.previous[] in lieu of top-level
    plays, plus a dedicated scoringPlays list."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary_drives,
        parse_summary_plays,
        parse_summary_scoring_plays,
    )

    payload = _load(fixture)
    drives = parse_summary_drives(payload)
    assert drives.height >= min_drives, f"{fixture}: expected >={min_drives} drives, got {drives.height}"
    # Football leagues lack the top-level plays[] array
    assert parse_summary_plays(payload).height == 0
    scoring = parse_summary_scoring_plays(payload)
    assert scoring.height >= min_scoring, f"{fixture}: expected >={min_scoring} scoring plays, got {scoring.height}"


def test_summary_drive_plays_unrolls_plays_across_all_drives_for_nfl():
    """parse_summary_drive_plays explodes drives.previous[i].plays[]
    into a long-form frame with drive_id + drive_sequence carried over
    from the parent drive. The output row count equals the sum of plays
    across every drive (~180-200 plays per NFL game), giving NFL true
    PBP parity with NBA / MLB / NHL / WNBA's top-level plays array."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary_drive_plays,
        parse_summary_drives,
    )

    payload = _load("summary_nfl")
    dp = parse_summary_drive_plays(payload)
    drives = parse_summary_drives(payload)

    # Super Bowl LIX has 26 drives + ~186 plays in the capture
    assert drives.height >= 10
    assert dp.height >= 100, f"expected >=100 unrolled plays, got {dp.height}"
    # Join keys for drive ↔ play
    assert "drive_id" in dp.columns
    assert "drive_sequence" in dp.columns
    # Standard play columns (parallel to parse_summary_plays output).
    # Note: ``type``/``clock``/``period`` ship as nested dicts in NFL
    # payloads, so json_normalize expands them to ``type_id`` /
    # ``clock_display_value`` / ``period_number`` etc. — check the
    # bare fields that are always scalar.
    for col in ("id", "sequence_number", "text", "away_score", "home_score", "scoring_play"):
        assert col in dp.columns, f"missing play column {col!r}"
    # Verify the nested fields did flatten (any prefix-match counts)
    assert any(c.startswith("type") for c in dp.columns), (
        f"expected type_* columns from flattened play.type, got {dp.columns[:10]}"
    )
    # drive_sequence is 1-indexed and bounded by the drive count
    seqs = set(dp["drive_sequence"].to_list())
    assert min(seqs) == 1
    assert max(seqs) == drives.height


def test_summary_drives_and_scoring_plays_empty_for_non_football():
    """drives + drive_plays + scoringPlays all return zero-row frames
    for non-football leagues without raising."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary_drive_plays,
        parse_summary_drives,
        parse_summary_scoring_plays,
    )

    for fixture in ("summary_nba", "summary_mlb", "summary_nhl", "summary_wnba", "summary_mbb", "summary_wbb"):
        assert parse_summary_drives(_load(fixture)).height == 0, f"{fixture}: non-football should have 0 drives"
        assert parse_summary_drive_plays(_load(fixture)).height == 0, (
            f"{fixture}: non-football should have 0 drive_plays"
        )
        assert parse_summary_scoring_plays(_load(fixture)).height == 0, (
            f"{fixture}: non-football should have 0 scoringPlays"
        )


def test_summary_winprobability_empty_for_nhl():
    """NHL doesn't ship per-play win probability; the parser must
    return an empty frame, not raise."""
    from sportsdataverse._common_espn_parsers import parse_summary_winprobability

    df = parse_summary_winprobability(_load("summary_nhl"))
    assert df.height == 0


# ===========================================================================
# Sparse-section parsers across leagues
# ===========================================================================
#
# Four summary sections are sparse for past games (broadcasts /
# againstTheSpread / pickcenter / odds — ESPN typically only populates
# these for live or upcoming games). We capture the cross-league
# behaviour explicitly so a future regression that breaks any of these
# parsers shows up immediately.


@pytest.mark.parametrize("league", ["mlb", "nhl"])
def test_summary_broadcasts_present_for_mlb_and_nhl(league):
    """MLB and NHL captures both include a single-row broadcasts entry
    (TV partner info attached even for past games). NBA / NFL / WNBA
    captures consistently ship empty broadcasts arrays for past games."""
    from sportsdataverse._common_espn_parsers import parse_summary_broadcasts

    df = parse_summary_broadcasts(_load(f"summary_{league}"))
    assert df.height >= 1, f"{league}: expected broadcasts data, got {df.height} rows"


@pytest.mark.parametrize("league", ["nba", "nfl", "wnba"])
def test_summary_broadcasts_empty_for_other_leagues_in_capture(league):
    """The captured past-game summaries for NBA / NFL / WNBA ship
    empty broadcasts arrays. Test that the parser handles that without
    raising; this also documents the cross-league shape divergence."""
    from sportsdataverse._common_espn_parsers import parse_summary_broadcasts

    df = parse_summary_broadcasts(_load(f"summary_{league}"))
    assert df.height == 0


@pytest.mark.parametrize(
    "section_parser_name",
    [
        "parse_summary_against_the_spread",
        "parse_summary_pickcenter",
        "parse_summary_odds",
    ],
)
def test_universally_sparse_summary_sections_return_zero_rows(section_parser_name):
    """``againstTheSpread`` / ``pickcenter`` / ``odds`` ship empty
    arrays for ALL 5 past-game captures (NBA finals, MLB WS, Super
    Bowl, SCF, WNBA finals). The parsers must return zero-row frames
    for every league without raising. If a future capture happens to
    have one populated, this test will flag the change for review."""
    from sportsdataverse import _common_espn_parsers as parsers

    parser = getattr(parsers, section_parser_name)
    for league in ("nba", "mlb", "nfl", "nhl", "wnba"):
        df = parser(_load(f"summary_{league}"))
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0, (
            f"{section_parser_name}({league}): expected empty frame, "
            f"got {df.height} rows. ESPN may have started populating "
            f"this section in past games — update the assertion if so."
        )


# ===========================================================================
# Full-coverage regression: every wrapper short name has a parser
# ===========================================================================
#
# These tests lock in catalog <-> parser-module consistency under the
# declarative codegen pipeline: every endpoint's declared ``parser:`` (incl.
# the long tail bound to parse_single_entity / parse_items) must resolve to a
# real callable, and every parser a generated module imports must be defined.
# They catch a new endpoint referencing a missing parser, or catalog/parser
# drift -- the successors to the retired ``_*_WRAPPERS`` registry checks.


def _catalog_parser_names() -> set[str]:
    """Every ``parser:`` declared across the ESPN endpoint catalog."""
    from tools.codegen import generate, spec

    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    apis = [spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    return {ep.parser for api in apis for ep in api.endpoints if ep.parser}


def _ext_modules() -> list:
    """Every generated ``*_espn_ext.py``, at any depth (the soccer leagues sit two levels down)."""
    return sorted((Path(__file__).parents[1] / "sportsdataverse").rglob("*_espn_ext.py"))


def _leagues() -> list:
    from tools.codegen import generate, spec

    return spec.load_leagues(generate.ENDPOINTS / "leagues.yaml").leagues


def _generated_parser_imports() -> set[str]:
    """Every ``parse_*`` name imported from ``_common_espn_parsers`` by any
    generated ``*_espn_ext`` module."""
    import ast

    out: set[str] = set()
    for path in _ext_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "sportsdataverse._common_espn_parsers":
                out.update(alias.name for alias in node.names if alias.name.startswith("parse_"))
    return out


def test_every_catalog_parser_is_defined():
    """Every ``parser:`` referenced in the codegen catalog must resolve to a
    real callable in ``_common_espn_parsers`` -- so a generated wrapper's
    ``return_parsed`` path never dispatches to a missing parser.

    (Replaces the retired ``_*_WRAPPERS``-table check: the declarative catalog
    is now the single source of truth for endpoint -> parser bindings.)"""
    import sportsdataverse._common_espn_parsers as parsers

    missing = {p for p in _catalog_parser_names() if not callable(getattr(parsers, p, None))}
    assert not missing, f"catalog references {len(missing)} undefined parsers: {sorted(missing)}"


def test_generated_modules_import_only_defined_parsers():
    """Every ``parse_*`` imported by a generated ``*_espn_ext`` module must be
    defined in ``_common_espn_parsers`` (catches catalog/parser-module drift)."""
    import sportsdataverse._common_espn_parsers as parsers

    imported = _generated_parser_imports()
    assert imported, "expected generated modules to import at least one parser"
    undefined = {p for p in imported if not callable(getattr(parsers, p, None))}
    assert not undefined, f"generated modules import {len(undefined)} undefined parsers: {sorted(undefined)}"


def test_parser_import_scan_covers_all_modules():
    """A-L15: the scan reads every league's ext module (thirty today), not the 11 a depth-1 glob saw."""
    stems = {p.stem for p in _ext_modules()}
    assert stems == {f"{lg.prefix}_espn_ext" for lg in _leagues()} and len(stems) >= 30


def test_return_parsed_shim_active_on_every_wrapper_all_modules():
    """The return_parsed kwarg must appear in the signature of every
    bound wrapper of every league module in leagues.yaml (A-L15, A-L54)."""
    import importlib
    import inspect

    missing = []
    total = 0
    for lg in _leagues():
        parts = [lg.group, lg.prefix] if lg.group else [lg.prefix]
        mod = importlib.import_module(".".join(["sportsdataverse", *parts, f"{lg.prefix}_espn_ext"]))
        for name in mod.__all__:
            fn = getattr(mod, name, None)
            if fn is None or not callable(fn):
                continue
            total += 1
            if "return_parsed" not in inspect.signature(fn).parameters:
                missing.append(f"{lg.prefix}.{name}")
    assert total >= 3000, f"sanity check: expected >=3000 wrappers across {len(_leagues())} leagues, got {total}"
    assert not missing, f"{len(missing)} wrappers missing the return_parsed shim: {missing[:10]}..."


def test_summary_section_parsers_registry_consistent():
    """SUMMARY_SECTION_PARSERS keys must exactly match the dispatcher's
    output dict keys."""
    from sportsdataverse._common_espn_parsers import (
        SUMMARY_SECTION_PARSERS,
        parse_summary,
    )

    expected_keys = set(SUMMARY_SECTION_PARSERS)
    payload = _load("summary_nba")
    actual_keys = set(parse_summary(payload))
    assert expected_keys == actual_keys, (
        f"registry / dispatcher mismatch: only-in-registry="
        f"{expected_keys - actual_keys}, only-in-output="
        f"{actual_keys - expected_keys}"
    )
    # Every registered parser must be callable
    for name, fn in SUMMARY_SECTION_PARSERS.items():
        assert callable(fn), f"{name} -> {fn!r} not callable"


def test_summary_dispatcher_returns_zero_row_dict_on_empty_payload():
    from sportsdataverse._common_espn_parsers import parse_summary

    out = parse_summary({})
    assert isinstance(out, dict)
    for name, frame in out.items():
        assert frame.height == 0, f"{name}: expected 0 rows on empty payload"


def test_summary_dispatcher_raises_on_unknown_section():
    from sportsdataverse._common_espn_parsers import parse_summary

    with pytest.raises(ValueError, match="Unknown summary section"):
        parse_summary({}, section="not_a_section")


def test_summary_section_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse._common_espn_parsers import parse_summary

    df = parse_summary(_load("summary_nba"), section="plays", return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 100


def test_summary_parser_routes_via_endpoint_parsers_registry():
    """``summary`` is registered in ENDPOINT_PARSERS so the
    return_parsed shim should route through parse_summary."""
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS, parse_summary

    assert "summary" in ENDPOINT_PARSERS
    assert ENDPOINT_PARSERS["summary"] is parse_summary


def test_return_parsed_shim_dispatches_through_new_parser_for_team_roster(monkeypatch):
    """The wrapper closure for espn_nba_team_roster should route through
    parse_team_roster when return_parsed=True. Verify offline by
    monkey-patching the core HTTP fetcher to return a known fixture."""
    import polars as pl

    from tests.conftest import patch_espn_fetch

    fixture = _load("team_roster_nba")
    patch_espn_fetch(monkeypatch, fixture)

    from sportsdataverse.nba.nba_espn_ext import espn_nba_team_roster

    # return_parsed=False recovers the raw Dict payload (0.0.54 default flip).
    raw = espn_nba_team_roster(team_id=13, return_parsed=False)
    assert isinstance(raw, dict)
    assert "athletes" in raw

    # Default (no kwarg) now routes through parse_team_roster → DataFrame.
    df = espn_nba_team_roster(team_id=13)
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 10
    assert "first_name" in df.columns

    # Explicit return_parsed=True must match the default.
    df_explicit = espn_nba_team_roster(team_id=13, return_parsed=True)
    assert isinstance(df_explicit, pl.DataFrame)
    assert df_explicit.height >= 10


# ===========================================================================
# Core v2 recruiting family (recruiting_years / recruiting_athletes /
# recruiting_rankings) — 0.0.73
# ===========================================================================


def test_endpoint_parsers_registry_includes_recruiting_family():
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS, parse_items

    for short in ("recruiting_years", "recruiting_athletes", "recruiting_rankings"):
        assert ENDPOINT_PARSERS.get(short) is parse_items, f"{short} not registered to parse_items"


def test_recruiting_years_fixture_parses_to_ref_frame():
    """Core v2 ``recruiting`` (year index) is a $ref-only paginated list."""
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load("recruiting_years_mbb"))
    assert df.height > 0
    assert "$ref" in df.columns


def test_recruiting_athletes_fixture_flattens_inline_athletes():
    """Core v2 ``recruiting/{year}/athletes`` ships INLINE athlete objects
    (unlike most Core v2 lists, which are $ref-only) — the flattened frame
    carries athlete identity + recruiting grade columns."""
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load("recruiting_athletes_mbb_2026"))
    assert df.height > 0
    for col in ("athlete_id", "athlete_full_name", "grade", "recruiting_class"):
        assert col in df.columns, f"missing flattened column {col}"


def test_recruiting_wrappers_route_through_parse_items():
    """The generated basketball wrappers accept the shim kwargs and dispatch
    the fixture payload through parse_items. Patch the module-level ``_get``
    binding (imported by name from _codegen_runtime) so no network happens."""
    import polars as pl

    import sportsdataverse.mbb.mbb_espn_ext as ext

    fixture = _load("recruiting_athletes_mbb_2026")
    real_get = ext._get
    ext._get = lambda *args, **kwargs: fixture
    try:
        raw = ext.espn_mbb_recruiting_players(year=2026, return_parsed=False)
        assert isinstance(raw, dict)
        assert "items" in raw

        df = ext.espn_mbb_recruiting_players(year=2026)
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert "athlete_full_name" in df.columns
    finally:
        ext._get = real_get


def test_recruiting_rankings_fixture_parses_to_ref_frame():
    """Core v2 ``recruiting/{year}/rankings`` lists ranking sets as $ref items."""
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load("recruiting_rankings_mbb_2026"))
    assert df.height > 0
    assert "$ref" in df.columns


def test_recruiting_athletes_pandas_opt_in():
    import pandas as pd

    from sportsdataverse._common_espn_parsers import parse_items

    pdf = parse_items(_load("recruiting_athletes_mbb_2026"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) > 0


# ===========================================================================
# Site v2 ``rankings`` (cfb / mbb / wbb / mch / wch): the polls sit in a
# top-level ``rankings`` list, not the Core v2 ``{items: [...]}`` shape that
# parse_items reads (it returned zero rows on every league).
# ===========================================================================

# ranks + others per poll on the committed 2026-10-08 captures.
_RANKINGS_ROWS = {"cfb": 208, "mbb": 77, "wbb": 75, "mch": 35, "wch": 20}

# The documented columns (tools/codegen/schemas/rankings.yaml) and their dtypes, for every
# league and for an empty payload: wch ships no team colors, and that must not make
# team_color a Null column there.
_RANKINGS_SCHEMA = {
    "poll_id": pl.Int64,
    "poll_name": pl.Utf8,
    "poll_short_name": pl.Utf8,
    "poll_type": pl.Utf8,
    "season": pl.Int64,
    "season_type": pl.Int64,
    "week": pl.Int64,
    "week_display": pl.Utf8,
    "poll_date": pl.Utf8,
    "ranked": pl.Boolean,
    "team_id": pl.Utf8,
    "rank": pl.Int64,
    "previous_rank": pl.Int64,
    "points": pl.Float64,
    "first_place_votes": pl.Int64,
    "trend": pl.Utf8,
    "record_summary": pl.Utf8,
    "team_uid": pl.Utf8,
    "team_location": pl.Utf8,
    "team_name": pl.Utf8,
    "team_nickname": pl.Utf8,
    "team_abbreviation": pl.Utf8,
    "team_color": pl.Utf8,
    "team_logo": pl.Utf8,
    "last_updated": pl.Utf8,
}


def test_rankings_registered_to_dedicated_parser():
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS, parse_rankings

    assert ENDPOINT_PARSERS["rankings"] is parse_rankings


@pytest.mark.parametrize("league", sorted(_RANKINGS_ROWS))
def test_rankings_one_row_per_ranked_or_vote_receiving_team(league):
    from sportsdataverse._common_espn_parsers import parse_rankings

    payload = _load(f"rankings_{league}")
    df = parse_rankings(payload)
    assert df.height == _RANKINGS_ROWS[league]
    assert dict(df.schema) == _RANKINGS_SCHEMA
    assert df["team_id"].null_count() == 0
    for poll in payload["rankings"]:
        rows = df.filter(pl.col("poll_id") == int(poll["id"]))
        ranked = rows.filter(pl.col("ranked") == True)
        assert ranked["rank"].to_list() == [e["current"] for e in poll["ranks"]]
        assert ranked["team_id"].to_list() == [e["team"]["id"] for e in poll["ranks"]]
        others = rows.filter(pl.col("ranked") == False)
        assert others.height == len(poll["others"])
        # ESPN ships current=0 for a vote-receiving team; 0 is not a rank.
        assert others["rank"].null_count() == others.height


def test_rankings_first_row_is_the_ap_number_one():
    from sportsdataverse._common_espn_parsers import parse_rankings

    row = parse_rankings(_load("rankings_cfb")).row(0, named=True)
    assert row["poll_name"] == "AP Top 25"
    assert (row["season"], row["season_type"], row["week"]) == (2026, 2, 6)
    assert (row["rank"], row["team_id"], row["team_abbreviation"]) == (1, "251", "TEX")
    assert row["first_place_votes"] == 61


def test_rankings_week_is_the_occurrence_value_not_its_running_number():
    """The MBB final poll is ``occurrence`` number 20 but value 3: its Core v2
    ``$ref`` is ``types/3/weeks/3``, so ``week`` must be 3."""
    from sportsdataverse._common_espn_parsers import parse_rankings

    df = parse_rankings(_load("rankings_mbb"))
    assert df["week"].unique().to_list() == [3]
    assert df["season_type"].unique().to_list() == [3]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"rankings": []},
        {"rankings": [None, {"ranks": None}]},
        {"rankings": [{"ranks": 1, "others": "x"}]},
        {"code": 404},
    ],
)
def test_rankings_empty_or_malformed_payload_is_zero_rows(payload):
    """Zero rows that still carry the documented columns, so a caller can select them."""
    from sportsdataverse._common_espn_parsers import parse_rankings

    df = parse_rankings(payload)
    assert df.height == 0
    assert dict(df.schema) == _RANKINGS_SCHEMA
    pdf = parse_rankings(payload, return_as_pandas=True)
    assert len(pdf) == 0
    assert list(pdf.columns) == list(_RANKINGS_SCHEMA)


def test_rankings_team_color_null_sentinel_is_null():
    """ESPN ships the literal string "NULL" as one mch team's color; that is no color."""
    from sportsdataverse._common_espn_parsers import parse_rankings

    payload = _load("rankings_mch")
    sentinel = {
        e["team"]["id"]
        for p in payload["rankings"]
        for k in ("ranks", "others")
        for e in p[k]
        if e["team"].get("color") == "NULL"
    }
    assert sentinel, "the capture no longer carries the sentinel"
    df = parse_rankings(payload)
    assert "NULL" not in df["team_color"].drop_nulls().to_list()
    assert df.filter(pl.col("team_id").is_in(sorted(sentinel)))["team_color"].null_count() > 0


def test_rankings_schema_is_the_documented_one():
    from pathlib import Path

    import yaml

    doc = yaml.safe_load((Path(__file__).parents[1] / "tools/codegen/schemas/rankings.yaml").read_text("utf-8"))
    assert [c["name"] for c in doc["columns"]] == list(_RANKINGS_SCHEMA)


def test_rankings_pandas_opt_in():
    import pandas as pd

    from sportsdataverse._common_espn_parsers import parse_rankings

    pdf = parse_rankings(_load("rankings_cfb"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) == _RANKINGS_ROWS["cfb"]


def test_rankings_wrapper_routes_through_parse_rankings(monkeypatch):
    import sportsdataverse.cfb.cfb_espn_ext as ext

    fixture = _load("rankings_cfb")
    monkeypatch.setattr(ext, "_get", lambda *args, **kwargs: fixture)
    assert ext.espn_cfb_rankings(return_parsed=False) is fixture
    assert ext.espn_cfb_rankings().height == _RANKINGS_ROWS["cfb"]


# ===========================================================================
# Hostile bodies and the dtype snapshot (A-M2, A-L16, A-L17, A-M13, A-M14, A-M15, EX-1.3-a)
# ===========================================================================
#
# Every public ``parse_*`` defined in a ``sportsdataverse/**/*_parsers.py`` module (214 today, the 30
# ESPN registry parsers among them) is DISCOVERED and fed five hostile bodies, so a new parser is
# swept with no edit here. The parser contract: the error envelope may raise the vocabulary's
# failed-fetch errors (NoDataError / AssetFetchError) or return zero rows; the other four shapes must
# return zero rows. The envelope is ESPN's own (tests/fixtures/runtime_errors/espn_site_roster_400.json).
# Every committed top-level ESPN capture is parsed by its endpoint's parser and its (column, dtype)
# set compared with tests/espn_parser_dtype_snapshot.json; regenerate that file with
# ``python -m tests.test_espn_universal_parsers`` only for a deliberate dtype change.

_SNAPSHOT = Path(__file__).parent / "espn_parser_dtype_snapshot.json"
# Four snapshot entries pin zero columns (injuries_mbb, injuries_wbb, team_schedule_cfb, team_schedule_nhl):
# their captures are empty and parse to zero-width frames. Task 5.1 gives empty frames their documented schema.
# scoreboard_cfb_2014_placeholder carries ESPN's real `{}` placeholder event, which has no ids: its row comes back
# all-null, so its id columns keep the String dtype of every other scoreboard (A-M15, task 5.2), but the pandas hop
# turns six Int64 columns Float64 (season_year, season_type, status_period, attendance, home_rank, away_rank;
# A-M14's class, task 5.2). The snapshot pins that measured state.
_ESPN_FIX = Path(__file__).parent / "fixtures" / "espn"
# Captures no registered parser reads (kept for other tests): ``athlete_statistics_core_*`` feeds the
# hand-written core-v2 stat pivot (tests/test_common_espn_player_stats.py) and ``error_404_body`` is
# ESPN's real 404 body, not a payload (tests/scrape/test_scrape_espn_engine.py). ``mbb_season_days_2024``
# bundles seven real scoreboard days keyed by date, not one payload (tests/mbb/test_mbb_schedule.py).
# ``wbb_calendar_2001`` feeds the hand-written espn_wbb_calendar season floor (tests/test_wbb_calendar_floor.py,
# main #860).
_NO_PARSER = {
    "athlete_statistics_core_nba_1966_2024",
    "athlete_statslog_lbj",
    "depthcharts_mlb",
    "depthcharts_nba",
    "depthcharts_nfl",
    "depthcharts_nhl",
    "error_404_body",
    "mbb_season_days_2024",
    "wbb_calendar_2001",
}
_PKG = Path(__file__).parents[1] / "sportsdataverse"


def _registered() -> Dict[str, Any]:
    """``{short: parser}``: the registry, plus the curated returns-schema parsers it lacks (``teams``)."""
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS
    from tools.codegen import generate

    return {**{k: fn for k, (fn, _) in generate._return_schema_parsers().items()}, **ENDPOINT_PARSERS}


def _parsers() -> Dict[str, Any]:
    return {fn.__name__: fn for fn in _registered().values()}


@functools.lru_cache(maxsize=1)
def _public_parsers() -> Dict[str, Any]:
    """``{"<module>.<parse_fn>": fn}`` for every public ``parse_*`` defined in a ``*_parsers.py`` module."""
    out = {}
    for path in sorted(_PKG.rglob("*_parsers.py")):
        mod = ".".join(path.relative_to(_PKG.parent).with_suffix("").parts)
        for name, fn in vars(importlib.import_module(mod)).items():
            if name.startswith("parse_") and callable(fn) and getattr(fn, "__module__", "") == mod:
                out[f"{mod.split('.', 1)[1]}.{name}"] = fn
    return out


def _captures() -> Dict[str, str]:
    """``{fixture stem: short}`` for every top-level ESPN capture a registered parser reads."""
    shorts = sorted(_registered(), key=len, reverse=True)
    out = {}
    for fx in sorted(_ESPN_FIX.glob("*.json")):
        short = next((s for s in shorts if fx.stem.startswith(s + "_")), None)
        if short is not None:
            out[fx.stem] = short
    return out


def _frames(out: Any) -> Dict[str, pl.DataFrame]:
    if isinstance(out, pl.DataFrame):
        return {"": out}
    assert isinstance(out, dict) and all(isinstance(v, pl.DataFrame) for v in out.values()), type(out)
    return out


def _dtype_snapshot() -> Dict[str, Dict[str, Dict[str, str]]]:
    reg = _registered()
    return {
        stem: {sec: {c: str(t) for c, t in df.schema.items()} for sec, df in _frames(reg[short](_load(stem))).items()}
        for stem, short in _captures().items()
    }


_ENVELOPE = json.loads((Path(__file__).parent / "fixtures/runtime_errors/espn_site_roster_400.json").read_text("utf-8"))
_HOSTILE = {"dict": {}, "list": [{}], "none": None, "empty_str": "", "error_envelope": _ENVELOPE}
# Parsers that break the contract today, measured 2026-10-11 (polars 2.0): ``{parser: {shape: what the
# case fails with}}``, AssertionError meaning it returned rows. The fixing task follows from the module.
_HOSTILE_OPEN: Dict[str, Dict[str, type]] = {
    "_common_espn_parsers.parse_athlete_gamelog": {"list": AttributeError},
    "_common_espn_parsers.parse_athlete_overview": {"error_envelope": AssertionError, "list": AttributeError},
    "_common_espn_parsers.parse_athlete_splits": {"error_envelope": AssertionError, "list": AttributeError},
    "_common_espn_parsers.parse_athlete_stats": {"error_envelope": AssertionError, "list": AttributeError},
    "_common_espn_parsers.parse_coaches": {"list": AttributeError},
    "_common_espn_parsers.parse_draft": {"list": AttributeError},
    "_common_espn_parsers.parse_event_competitor_linescores": {"list": AttributeError},
    "_common_espn_parsers.parse_event_competitor_roster": {"list": AttributeError},
    "_common_espn_parsers.parse_event_competitor_statistics": {
        "error_envelope": AssertionError,
        "list": AttributeError,
    },
    "_common_espn_parsers.parse_event_plays": {"list": AttributeError},
    "_common_espn_parsers.parse_fpi": {"list": AttributeError},
    "_common_espn_parsers.parse_leaders": {"error_envelope": AssertionError, "list": AttributeError},
    "_common_espn_parsers.parse_scoreboard": {"list": AttributeError},
    "_common_espn_parsers.parse_single_entity": {"error_envelope": AssertionError},
    "_common_espn_parsers.parse_standings": {"list": AttributeError},
    "_common_espn_parsers.parse_summary": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_against_the_spread": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_broadcasts": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_drive_plays": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_drives": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_game_info": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_injuries": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_news": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_odds": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_officials": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_pickcenter": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_scoring_plays": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_season_series": {"list": AttributeError},
    "_common_espn_parsers.parse_summary_standings": {"list": AttributeError},
    "_common_espn_parsers.parse_weekly_powerindex": {"list": AttributeError},
    "cbs.cbs_napi_parsers.parse_cbs_napi": {"error_envelope": AssertionError, "list": AssertionError},
    "cfb.on3_parsers.parse_on3_rankings": {"list": AttributeError},
    "cfb.on3_parsers.parse_on3_rdb": {"error_envelope": AssertionError, "list": AssertionError},
    "cfb.on3_parsers.parse_on3_team_rankings": {"list": AttributeError},
    "cfb.sports247_parsers.parse_sports247_institution_rankings": {
        "error_envelope": AssertionError,
        "list": AssertionError,
    },
    "cfb.sports247_parsers.parse_sports247_result_set": {"error_envelope": AssertionError, "list": AssertionError},
    "cfb.sports247_parsers.parse_sports247_teams": {"error_envelope": AssertionError, "list": AssertionError},
    "cfb.sports247_site_pages_parsers.parse_sports247_site_page": {"error_envelope": AssertionError},
    "espn_content.espn_content_parsers.parse_espn_content": {"error_envelope": AssertionError, "list": AssertionError},
    "euroleague.euroleague_parsers.parse_euroleague": {"error_envelope": AssertionError},
    "euroleague.euroleague_parsers.parse_euroleague_header": {"error_envelope": AssertionError},
    "euroleague.euroleague_parsers.parse_euroleague_points": {"error_envelope": AssertionError},
    "fox.fox_api_parsers.parse_fox_api": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_events": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_header": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_nav": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_polls": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_roster": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_search": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_standings": {"list": AssertionError},
    "fox.fox_api_parsers.parse_fox_api_trending": {"list": AssertionError},
    "hockeytech._parsers.parse_game_info": {"list": AttributeError},
    "hockeytech._parsers.parse_game_summary": {
        "dict": AssertionError,
        "empty_str": AssertionError,
        "error_envelope": AssertionError,
        "list": AttributeError,
        "none": AssertionError,
    },
    "hockeytech._parsers.parse_leaders": {"list": AttributeError},
    "hockeytech._parsers.parse_pbp": {"list": AssertionError},
    "hockeytech._parsers.parse_player_box": {"list": AttributeError},
    "hockeytech._parsers.parse_player_game_log": {"list": AttributeError},
    "hockeytech._parsers.parse_player_info": {"list": AttributeError},
    "hockeytech._parsers.parse_player_search": {"list": AttributeError},
    "hockeytech._parsers.parse_player_stats": {"list": AttributeError},
    "hockeytech._parsers.parse_playoff_bracket": {"list": AttributeError},
    "hockeytech._parsers.parse_roster": {"list": AttributeError},
    "hockeytech._parsers.parse_schedule": {"list": AttributeError},
    "hockeytech._parsers.parse_scorebar": {"list": AttributeError},
    "hockeytech._parsers.parse_seasons": {"list": AttributeError},
    "hockeytech._parsers.parse_shifts": {"list": AttributeError},
    "hockeytech._parsers.parse_stats": {"list": AttributeError},
    "hockeytech._parsers.parse_streaks": {"list": AttributeError},
    "hockeytech._parsers.parse_teams": {"list": AttributeError},
    "hockeytech._parsers.parse_transactions": {"list": AttributeError},
    "mlb.mlb_api_parsers.parse_mlb_api_draft_latest": {"error_envelope": AssertionError},
    "mlb.mlb_api_parsers.parse_mlb_api_team_roster": {"list": AttributeError},
    "mlb.mlb_api_parsers.parse_mlb_api_teams": {"list": AttributeError},
    "mlb.mlb_api_parsers.parse_mlb_api_timecodes": {"list": AssertionError},
    "mlb.mlb_api_parsers.parse_mlb_api_win_probability": {"list": AssertionError},
    "mlb.mlb_statcast_parsers.parse_mlb_statcast_player": {"error_envelope": TypeError, "list": TypeError},
    "nba.nba_stats_parsers.parse_nba_stats_commonallplayers": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nba.nba_stats_parsers.parse_nba_stats_leaguedashplayerstats": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nba.nba_stats_parsers.parse_nba_stats_playercareerstats": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nba.nba_stats_parsers.parse_nba_stats_result_sets": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_combine_profiles": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_draft_picks": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_game_details_by_slug": {"error_envelope": AssertionError},
    "nfl.nfl_api_parsers.parse_nfl_game_details_v2": {"error_envelope": AssertionError},
    "nfl.nfl_api_parsers.parse_nfl_game_summaries": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_injuries": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_rosters": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_standings": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_team": {
        "dict": AssertionError,
        "error_envelope": AssertionError,
        "list": AssertionError,
    },
    "nfl.nfl_api_parsers.parse_nfl_teams_history": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_weekly_game_details": {
        "empty_str": AttributeError,
        "list": AssertionError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_weeks": {
        "empty_str": AttributeError,
        "list": AttributeError,
        "none": AttributeError,
    },
    "nfl.nfl_api_parsers.parse_nfl_weeks_by_date": {
        "dict": AssertionError,
        "error_envelope": AssertionError,
        "list": AssertionError,
    },
    "nfl.nflpro_parsers.parse_nfl_pro_stats": {"list": AssertionError},
    "nfl.sleeper_parsers.parse_sleeper": {"error_envelope": AssertionError, "list": AssertionError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_draft_picks": {"list": AttributeError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_landing": {"error_envelope": AssertionError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_pbp": {"list": AttributeError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_player_game_log": {"list": AttributeError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_player_landing": {"error_envelope": AssertionError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_player_spotlight": {"list": AssertionError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_score": {"list": AttributeError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_standings": {"list": AttributeError},
    "nhl.nhl_api_web_parsers.parse_nhl_web_standings_season": {"list": AttributeError},
    "nhl.nhl_edge_parsers.parse_edge_detail": {"error_envelope": AssertionError},
    "nhl.nhl_edge_parsers.parse_edge_payload": {"error_envelope": AssertionError},
    "nhl.nhl_edge_parsers.parse_edge_top10": {"list": AssertionError},
    "nhl.nhl_edge_parsers.parse_edge_zone_time": {"error_envelope": AssertionError},
    "odds.kalshi_parsers.parse_kalshi": {"error_envelope": AssertionError, "list": AssertionError},
    "odds.polymarket_parsers.parse_polymarket": {"error_envelope": AssertionError, "list": AssertionError},
    "odds.the_odds_api_parsers.parse_toa_event_odds": {
        "dict": AssertionError,
        "error_envelope": AssertionError,
        "list": AssertionError,
    },
    "odds.the_odds_api_parsers.parse_toa_event_odds_history": {
        "dict": AssertionError,
        "error_envelope": AssertionError,
        "list": AssertionError,
    },
    "odds.the_odds_api_parsers.parse_toa_events": {"list": AssertionError},
    "odds.the_odds_api_parsers.parse_toa_events_history": {
        "dict": AssertionError,
        "error_envelope": AssertionError,
        "list": AssertionError,
    },
    "odds.the_odds_api_parsers.parse_toa_odds": {"list": AssertionError},
    "odds.the_odds_api_parsers.parse_toa_odds_history": {
        "dict": AssertionError,
        "error_envelope": AssertionError,
        "list": AssertionError,
    },
    "odds.the_odds_api_parsers.parse_toa_participants": {"list": AssertionError},
    "odds.the_odds_api_parsers.parse_toa_scores": {"list": AssertionError},
    "odds.the_odds_api_parsers.parse_toa_sports": {"list": AssertionError},
    "soccer.asa_parsers.parse_asa": {"error_envelope": AssertionError},
    "soccer.asa_parsers.parse_asa_goals_added": {"error_envelope": AssertionError},
    "soccer.fifa_parsers.parse_fifa": {"error_envelope": AssertionError},
    "soccer.fotmob_parsers.parse_fotmob": {"error_envelope": AssertionError},
    "soccer.mls.mls_api_parsers.parse_mls_api": {"error_envelope": AssertionError},
    "soccer.mls.mls_api_parsers.parse_mls_entity": {"error_envelope": AssertionError},
    "soccer.openligadb_parsers.parse_openligadb": {"error_envelope": AssertionError, "list": AssertionError},
    "soccer.uefa_parsers.parse_uefa": {"error_envelope": AssertionError},
    "thesportsdb.thesportsdb_parsers.parse_thesportsdb": {"list": AssertionError},
}
# Main's families merged 10-17 (#826 European hockey, #859/#862 international basketball) return rows for a
# hostile body, measured 2026-10-18 on the sync tree (109 cases, every one AssertionError = rows returned).
_HOSTILE_SYNC_1018: Dict[str, Dict[str, type]] = {
    **{
        q: {"error_envelope": AssertionError}
        for q in (
            "basketball.dbl.sportality_parsers.parse_sportality",
            "basketball.dbl.sportality_parsers.parse_sportality_page",
            "basketball.easl.easl_parsers.parse_easl_health",
            "basketball.fiba_hub.fiba_gdap_parsers.parse_fiba_gdap_rounds",
            "basketball.scb.scb_parsers.parse_scb_season_player",
            "basketball.scb.scb_parsers.parse_scb_team",
            "hockey.esports_parsers.parse_esports",
            "hockey.esports_parsers.parse_esports_game_info",
            "hockey.ha.ha_parsers.parse_ha",
            "hockey.ha.ha_parsers.parse_ha_leaders",
            "hockey.ha.ha_parsers.parse_ha_players",
            "hockey.ha.ha_parsers.parse_ha_standings",
            "hockey.ha.ha_parsers.parse_ha_team_stats",
            "hockey.khl.khl_webcaster_parsers.parse_khl_webcaster",
            "hockey.liiga.liiga_parsers.parse_liiga",
            "hockey.liiga.liiga_parsers.parse_liiga_player",
            "hockey.nl.nationalleague_parsers.parse_nationalleague",
            "hockey.nl.nationalleague_parsers.parse_nationalleague_player_stats",
            "hockey.nl.nationalleague_parsers.parse_nationalleague_team_info",
            "hockey.shl.shl_parsers.parse_shl",
            "hockey.shl.shl_parsers.parse_shl_page",
        )
    },
    **{
        q: {"error_envelope": AssertionError, "list": AssertionError}
        for q in (
            "hockey.ha.ha_parsers.parse_ha_games",
            "hockey.ha.ha_parsers.parse_ha_schedule",
        )
    },
    **{
        q: {"list": AssertionError}
        for q in (
            "basketball.cba.cba_parsers.parse_cba_game_team_four",
            "basketball.cba.cba_parsers.parse_cba_game_team_shots",
            "basketball.cba.cba_parsers.parse_cba_match_types",
            "basketball.cba.cba_parsers.parse_cba_schedule_day",
            "basketball.cba.cba_parsers.parse_cba_schedule_month",
            "basketball.cba.cba_parsers.parse_cba_seasons",
            "basketball.cba.cba_parsers.parse_cba_team_basic",
            "basketball.cba.cba_parsers.parse_cba_team_counts",
            "basketball.cba.cba_parsers.parse_cba_team_four",
            "basketball.cba.cba_parsers.parse_cba_team_options",
            "basketball.cba.cba_parsers.parse_cba_teams",
            "basketball.cebl.cebl_parsers.parse_cebl_games",
            "basketball.cebl.cebl_parsers.parse_cebl_news",
            "basketball.cebl.cebl_parsers.parse_cebl_player_gamelog",
            "basketball.cebl.cebl_parsers.parse_cebl_player_seasons",
            "basketball.cebl.cebl_parsers.parse_cebl_player_stats",
            "basketball.cebl.cebl_parsers.parse_cebl_roster",
            "basketball.cebl.cebl_parsers.parse_cebl_seasons",
            "basketball.cebl.cebl_parsers.parse_cebl_standings",
            "basketball.cebl.cebl_parsers.parse_cebl_team_stats",
            "basketball.cebl.cebl_parsers.parse_cebl_teams",
            "basketball.easl.easl_parsers.parse_easl_game",
            "basketball.easl.easl_parsers.parse_easl_players",
            "basketball.easl.easl_parsers.parse_easl_schedule",
            "basketball.easl.easl_parsers.parse_easl_seasons",
            "basketball.easl.easl_parsers.parse_easl_standings",
            "basketball.easl.easl_parsers.parse_easl_team_box",
            "basketball.easl.easl_parsers.parse_easl_team_schedule",
            "basketball.easl.easl_parsers.parse_easl_teams",
            "basketball.genius.genius_parsers.parse_genius_competition_games",
            "basketball.kbl.kbl_parsers.parse_kbl_current_standings",
            "basketball.kbl.kbl_parsers.parse_kbl_pbp",
            "basketball.kbl.kbl_parsers.parse_kbl_player_box",
            "basketball.kbl.kbl_parsers.parse_kbl_player_totals",
            "basketball.kbl.kbl_parsers.parse_kbl_players",
            "basketball.kbl.kbl_parsers.parse_kbl_round_mvps",
            "basketball.kbl.kbl_parsers.parse_kbl_schedule",
            "basketball.kbl.kbl_parsers.parse_kbl_seasons",
            "basketball.kbl.kbl_parsers.parse_kbl_standings",
            "basketball.kbl.kbl_parsers.parse_kbl_team_box",
            "basketball.kbl.kbl_parsers.parse_kbl_teams",
            "basketball.kzs.kzs_parsers.parse_kzs_competitions",
            "basketball.kzs.kzs_parsers.parse_kzs_matches",
            "basketball.kzs.kzs_parsers.parse_kzs_player_leaderboard",
            "basketball.kzs.kzs_parsers.parse_kzs_playing_days",
            "basketball.kzs.kzs_parsers.parse_kzs_roster",
            "basketball.kzs.kzs_parsers.parse_kzs_seasons",
            "basketball.kzs.kzs_parsers.parse_kzs_team_leaderboard",
            "basketball.kzs.kzs_parsers.parse_kzs_teams",
            "basketball.nbl_au.nbl_rosetta_parsers.parse_nbl_rosetta_next_matches",
            "basketball.scb.scb_parsers.parse_scb_games",
            "basketball.scb.scb_parsers.parse_scb_player_statistics",
            "basketball.scb.scb_parsers.parse_scb_referees",
            "basketball.scb.scb_parsers.parse_scb_seasons",
            "basketball.scb.scb_parsers.parse_scb_standings",
            "basketball.scb.scb_parsers.parse_scb_team_statistics",
            "basketball.scb.scb_parsers.parse_scb_transfers",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_divisions",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_games",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_player_games",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_player_shots",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_roster",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_seasons",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_standings",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_team_shots",
            "basketball.tpbl.tpbl_parsers.parse_tpbl_teams",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_best_player_parameters",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_best_team_parameters",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_calendar",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_cross_table",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_playoff",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_seasons",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_standings",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_team_comps",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_team_games",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_team_preview",
            "basketball.vtb.vtb_infobasket_parsers.parse_vtb_infobasket_team_seasons",
            "basketball.vtb.vtb_league_parsers.parse_vtb_league_leagues",
            "basketball.vtb.vtb_league_parsers.parse_vtb_league_matches",
            "basketball.vtb.vtb_league_parsers.parse_vtb_league_seasons",
            "basketball.wjbl.wjbl_parsers.parse_wjbl_leagues",
            "basketball.wjbl.wjbl_parsers.parse_wjbl_teams",
            "hockey.esports_parsers.parse_esports_player_stats",
            "hockey.esports_parsers.parse_esports_shifts",
        )
    },
}
# Plan v2.2: task 4.1 owns the ESPN parsers' zero-row contract (A-L17, A-M13), 4.9 narrows the
# nhl/mlb/nba parsers' error handling (A-L60), and 4.1b owns every other provider parser (A-L17).
_TASK_49 = ("nhl.nhl_edge_parsers.", "nhl.nhl_api_web_parsers.", "mlb.mlb_api_parsers.", "nba.nba_stats_parsers.")


def _open(qual: str, shape: str) -> str:
    """The finding id and fixing task of an open ``(parser, shape)`` case."""
    if qual.startswith("_common_espn_parsers.") and qual.rsplit(".", 1)[1] in _parsers():
        if shape == "error_envelope":
            return "A-M13: ESPN's 200 error envelope parses to rows; task 4.1"
        return "A-L17: a non-dict body raises instead of returning a zero-row frame; task 4.1"
    task = "4.1" if qual.startswith("_common_espn_parsers.") else "4.9" if qual.startswith(_TASK_49) else "4.1b"
    return f"EX-1.3-a: a hostile {shape} body breaks the zero-row contract; task {task}"


def _hostile_params() -> List[Any]:
    params = []
    for qual in sorted(_public_parsers()):
        for shape in _HOSTILE:
            exc = _HOSTILE_OPEN.get(qual, {}).get(shape)
            marks = [pytest.mark.xfail(strict=True, raises=exc, reason=_open(qual, shape))] if exc else []
            if _HOSTILE_SYNC_1018.get(qual, {}).get(shape):
                marks = [
                    pytest.mark.xfail(
                        strict=True,
                        raises=_HOSTILE_SYNC_1018[qual][shape],
                        reason=f"EX-SYNC-g: a hostile {shape} body parses to rows; task providers 4.1b",
                    )
                ]
            params.append(pytest.param(qual, shape, id=f"{qual}-{shape}", marks=marks))
    return params


@pytest.mark.parametrize(("qual", "shape"), _hostile_params())
def test_hostile_body_is_zero_rows_or_a_vocabulary_raise(qual, shape):
    from sportsdataverse.errors import AssetFetchError, NoDataError

    parse = _public_parsers()[qual]
    if shape == "error_envelope":  # a failed fetch: the one body the vocabulary lets a parser raise on
        try:
            out = parse(_HOSTILE[shape])
        except (NoDataError, AssetFetchError):
            return
    else:
        out = parse(_HOSTILE[shape])
    assert {sec: df.height for sec, df in _frames(out).items() if df.height} == {}


def test_hostile_sweep_covers_every_public_parser():
    parsers = _public_parsers()
    assert len(parsers) >= 214 and {f"_common_espn_parsers.{n}" for n in _parsers()} <= set(parsers)
    needs_more = [
        q
        for q, fn in parsers.items()
        if any(
            p.default is p.empty and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
            for p in list(inspect.signature(fn).parameters.values())[1:]
        )
    ]
    assert needs_more == [], "a parser needs more than its payload: give the sweep its arguments"
    assert set(_HOSTILE_OPEN) <= set(parsers), sorted(set(_HOSTILE_OPEN) - set(parsers))
    assert set(_HOSTILE_SYNC_1018) <= set(parsers), sorted(set(_HOSTILE_SYNC_1018) - set(parsers))
    assert not set(_HOSTILE_SYNC_1018) & set(_HOSTILE_OPEN)
    unread = {fx.stem for fx in _ESPN_FIX.glob("*.json")} - set(_captures())
    assert unread == _NO_PARSER, "a new capture needs a parser (or a reason in _NO_PARSER)"


def test_dtype_snapshot_matches_every_committed_capture():
    """A-M2: column names AND dtypes per capture; a parser change that retypes a column fails here."""
    want = json.loads(_SNAPSHOT.read_text(encoding="utf-8"))
    got = _dtype_snapshot()
    assert sorted(got) == sorted(want)
    diffs = [
        (stem, sec, col, want[stem].get(sec, {}).get(col), got[stem].get(sec, {}).get(col))
        for stem in got
        for sec in set(got[stem]) | set(want[stem])
        for col in set(got[stem].get(sec, {})) | set(want[stem].get(sec, {}))
        if want[stem].get(sec, {}).get(col) != got[stem].get(sec, {}).get(col)
    ]
    # Fix the parser, or regenerate only for a deliberate change: (capture, section, column, want, got).
    assert not diffs, f"{len(diffs)} column dtypes changed, e.g. {sorted(diffs, key=str)[:8]}"


def _id_cols(name: str) -> Dict[str, str]:
    """``{capture/section/column: dtype}`` for every id column ``name`` emits on its captures."""
    reg = _registered()
    snap = json.loads(_SNAPSHOT.read_text(encoding="utf-8"))
    return {
        f"{stem}/{sec}/{col}": dtype
        for stem, short in _captures().items()
        if reg[short].__name__ == name
        for sec, cols in snap[stem].items()
        for col, dtype in cols.items()
        if col == "id" or col.endswith("_id")
    }


# Parsers whose id columns are not Int64 on their captures today (String; parse_summary also
# Float64). Task 5.2 makes every ESPN id Int64 at the parser boundary.
_IDS_NOT_INT64 = {
    "parse_athlete_gamelog",
    "parse_athlete_overview",
    "parse_event_plays",
    "parse_fpi",
    "parse_groups",
    "parse_injuries",
    "parse_items",
    "parse_leaders",
    "parse_news",
    "parse_rankings",
    "parse_scoreboard",
    "parse_single_entity",
    "parse_standings",
    "parse_summary",
    "parse_team_roster",
    "parse_team_schedule",
    "parse_teams",
    "parse_transactions",
}


def _id_params(open_ids: set, reason: str) -> List[Any]:
    return [
        pytest.param(
            name,
            marks=[pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason)] if name in open_ids else [],
        )
        for name in sorted(_parsers())
    ]


@pytest.mark.parametrize(
    "name",
    _id_params(_IDS_NOT_INT64, "A-M15: ESPN ids are String here but Int64 in parse_weekly_powerindex; task 5.2"),
)
def test_dtype_snapshot_ids_are_int64(name):
    assert {k: v for k, v in _id_cols(name).items() if v != "Int64"} == {}


@pytest.mark.parametrize(
    "name",
    _id_params({"parse_summary"}, "A-M14: the pandas hop turns point_after_attempt_id into Float64; task 5.2"),
)
def test_dtype_snapshot_ids_are_never_float(name):
    """A float id stringifies as ``"123.0"`` and joins nothing."""
    assert {k: v for k, v in _id_cols(name).items() if v.startswith("Float")} == {}


if __name__ == "__main__":  # regenerate the snapshot after a deliberate dtype change
    _SNAPSHOT.write_text(json.dumps(_dtype_snapshot(), indent=1) + "\n", encoding="utf-8", newline="\n")
