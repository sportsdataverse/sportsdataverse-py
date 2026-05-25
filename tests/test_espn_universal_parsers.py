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

import json
from pathlib import Path

import polars as pl
import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "espn"


def _load(stem: str) -> dict:
    return json.loads((FIXTURE_DIR / f"{stem}.json").read_text(encoding="utf-8"))


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
        ("venues_core_nba",     1),  # 5-row capture, $ref-only items
        ("events_core_nba",     1),  # 1-row capture, $ref-only items
    ],
)
def test_parse_items_handles_items_key(fixture, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load(fixture))
    assert df.height >= expected_min_rows, (
        f"{fixture}: expected >= {expected_min_rows} rows, got {df.height}"
    )
    # Core v2 paginated items often contain just $ref pointers
    assert any(c.endswith("ref") or c == "_ref" or c == "$ref"
               for c in df.columns), f"no $ref column in {df.columns}"


def test_parse_items_handles_entries_key_for_statisticslog():
    """athlete_statisticslog ships ``{entries: [...]}`` not ``{items: [...]}``;
    parse_items's fallback key list should still find the rows."""
    from sportsdataverse._common_espn_parsers import parse_items

    df = parse_items(_load("athlete_statslog_lbj"))
    assert df.height >= 1, (
        "parse_items must fall back to the 'entries' key for "
        "athlete_statisticslog payloads"
    )


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
        "team_schedule", "team_roster",
        "news", "team_news", "athlete_news",
        "injuries", "team_injuries", "athlete_injuries",
        "venues", "franchises", "events", "athletes_index",
        "athlete_statisticslog",
    }
    missing = expected_new - set(ENDPOINT_PARSERS)
    assert not missing, (
        f"ENDPOINT_PARSERS missing newly-introduced entries: {sorted(missing)}"
    )


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

    for fn in (espn_nba_team_schedule, espn_nba_team_roster,
               espn_nba_news, espn_nba_venues):
        sig = inspect.signature(fn)
        assert "return_parsed" in sig.parameters, (
            f"{fn.__name__} missing return_parsed kwarg"
        )
        assert "return_as_pandas" in sig.parameters, (
            f"{fn.__name__} missing return_as_pandas kwarg"
        )


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
    for col in ("team_id", "team_abbreviation", "athlete_id",
                "athlete_display_name", "starter", "active"):
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
        "boxscore_player", "boxscore_team", "plays", "winprobability",
        "leaders", "game_info", "officials", "header", "season_series",
        "against_the_spread", "standings", "broadcasts", "format",
        "pickcenter", "odds", "article", "injuries", "news",
        # NFL / CFB only — return empty frames for NBA but still present
        "drives", "scoring_plays",
    }
    assert set(out) == must_be_present, (
        f"dispatcher returned unexpected section set: "
        f"missing={must_be_present - set(out)}, extra={set(out) - must_be_present}"
    )
    # The original 5 sub-parsers + the always-present-on-real-games
    # sections must be non-empty for this fixture
    for name in ("boxscore_player", "boxscore_team", "plays",
                 "winprobability", "leaders", "game_info", "officials",
                 "header", "standings", "format", "article", "injuries",
                 "news"):
        assert out[name].height > 0, (
            f"dispatcher returned empty frame for {name!r} on the NBA "
            f"Finals fixture (expected non-empty)"
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


@pytest.mark.parametrize("league,expected_min_rows,grouped", [
    # Flat shape (no position_group column)
    ("nba",   12, False),
    ("wnba",  10, False),
    # Position-grouped shape (rows include position_group column)
    ("mlb",   20, True),   # 4 groups × ~6 players
    ("nfl",   50, True),   # 3 groups × full roster
    ("nhl",   18, True),   # 5 groups × ~5 players
])
def test_parse_team_roster_handles_both_shapes_across_leagues(
    league, expected_min_rows, grouped,
):
    from sportsdataverse._common_espn_parsers import parse_team_roster

    df = parse_team_roster(_load(f"team_roster_{league}"))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= expected_min_rows, (
        f"{league}: expected >= {expected_min_rows} players, got {df.height}"
    )
    # Universal columns from the athlete sub-dict
    for col in ("id", "first_name", "last_name", "full_name"):
        assert col in df.columns, f"{league}: missing {col!r}"
    # Position-grouped leagues add a column the flat-shape ones don't
    if grouped:
        assert "position_group" in df.columns, (
            f"{league}: expected position_group column for grouped shape"
        )
        # All values should be non-null strings
        groups = set(df["position_group"].to_list())
        assert all(isinstance(g, str) and g for g in groups), (
            f"{league}: position_group has null/empty values: {groups}"
        )
    else:
        # Flat-shape leagues should NOT have a stray position_group col
        # (would indicate accidental column from one of the player records)
        assert "position_group" not in df.columns


@pytest.mark.parametrize("league,expected_min_rows", [
    ("nba",   10),
    ("mlb",  100),   # full MLB season = 162 games
    ("nfl",   15),   # 17 regular + playoffs
    # NHL skipped: off-season capture returned an empty schedule
    ("wnba",  30),   # 40+ regular-season games
])
def test_parse_team_schedule_works_across_leagues(league, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_team_schedule

    df = parse_team_schedule(_load(f"team_schedule_{league}"))
    assert df.height >= expected_min_rows, (
        f"{league}: expected >= {expected_min_rows} events, got {df.height}"
    )
    for col in ("id", "date", "name", "season_year"):
        assert col in df.columns


def test_parse_team_schedule_handles_empty_offseason_payload():
    """NHL team_schedule capture in the off-season returns no events —
    the parser must return a zero-row frame, not raise."""
    from sportsdataverse._common_espn_parsers import parse_team_schedule

    df = parse_team_schedule(_load("team_schedule_nhl"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


@pytest.mark.parametrize("league,expected_min_rows", [
    ("nba",  3),
    ("mlb",  3),
    ("nfl",  3),
    ("nhl",  3),
    ("wnba", 3),
])
def test_parse_news_works_across_leagues(league, expected_min_rows):
    from sportsdataverse._common_espn_parsers import parse_news

    df = parse_news(_load(f"news_{league}"))
    assert df.height >= expected_min_rows, (
        f"{league}: expected >= {expected_min_rows} articles, got {df.height}"
    )
    for col in ("id", "headline", "type"):
        assert col in df.columns


@pytest.mark.parametrize("league,expected_min_rows", [
    # All 5 leagues consistently have >= 10 teams reporting injuries
    ("nba",  10),
    ("mlb",  10),
    ("nfl",  10),
    ("nhl",  10),
    ("wnba",  5),   # smaller league, lower floor
])
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


SUMMARY_FIXTURES = ["summary_nba", "summary_mlb", "summary_nfl",
                    "summary_nhl", "summary_wnba"]


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
    assert set(out) == set(SUMMARY_SECTION_PARSERS), (
        f"{fixture}: dispatcher returned different sections than registry"
    )
    for name, frame in out.items():
        assert isinstance(frame, pl.DataFrame), (
            f"{fixture}: section {name!r} returned {type(frame)}"
        )


@pytest.mark.parametrize("fixture,min_athletes", [
    ("summary_nba",  20),  # 2 NBA rosters, ~24-27 active
    ("summary_mlb",  20),  # 2 MLB rosters, ~26-32 with starters + bullpen
    ("summary_nfl",  50),  # 2 NFL rosters, ~70-90 with full game-day squad
    ("summary_nhl",  20),  # 2 NHL rosters, skaters + goalies
    ("summary_wnba", 15),  # 2 WNBA rosters, ~12-15 each
])
def test_summary_boxscore_player_works_across_leagues(fixture, min_athletes):
    from sportsdataverse._common_espn_parsers import parse_summary_boxscore_player

    df = parse_summary_boxscore_player(_load(fixture))
    assert df.height >= min_athletes, (
        f"{fixture}: expected >={min_athletes} athletes, got {df.height}"
    )
    for col in ("team_id", "team_abbreviation", "athlete_id",
                "athlete_display_name"):
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
    assert any(c.startswith("venue_") for c in df.columns), (
        f"{fixture}: no venue_* columns"
    )


@pytest.mark.parametrize("fixture", SUMMARY_FIXTURES)
def test_summary_officials_works_across_leagues(fixture):
    from sportsdataverse._common_espn_parsers import parse_summary_officials

    df = parse_summary_officials(_load(fixture))
    # NBA=3, NHL=6, MLB=6, NFL=7, WNBA=4
    assert df.height >= 3, f"{fixture}: expected >=3 officials, got {df.height}"
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
    assert df.height >= 5, (
        f"{fixture}: expected >=5 teams in standings, got {df.height}"
    )
    assert "team_id" in df.columns


def test_summary_plays_works_for_non_football_leagues():
    """NBA / MLB / WNBA / NHL ship plays at the top level. NFL ships
    drives.previous[] instead, which is exercised separately."""
    from sportsdataverse._common_espn_parsers import parse_summary_plays

    for fixture in ("summary_nba", "summary_mlb", "summary_nhl", "summary_wnba"):
        df = parse_summary_plays(_load(fixture))
        assert df.height >= 100, (
            f"{fixture}: expected >=100 plays, got {df.height}"
        )


def test_summary_drives_works_for_nfl():
    """NFL summary ships drives.previous[] in lieu of top-level plays."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary_drives,
        parse_summary_plays,
        parse_summary_scoring_plays,
    )

    payload = _load("summary_nfl")
    drives = parse_summary_drives(payload)
    assert drives.height >= 10, f"expected >=10 drives, got {drives.height}"
    assert parse_summary_plays(payload).height == 0
    scoring = parse_summary_scoring_plays(payload)
    assert scoring.height >= 1, "NFL summary should have scoringPlays"


def test_summary_drives_and_scoring_plays_empty_for_non_football():
    """drives + scoringPlays return zero-row frames for non-football
    leagues without raising."""
    from sportsdataverse._common_espn_parsers import (
        parse_summary_drives,
        parse_summary_scoring_plays,
    )

    for fixture in ("summary_nba", "summary_mlb", "summary_nhl", "summary_wnba"):
        assert parse_summary_drives(_load(fixture)).height == 0, (
            f"{fixture}: non-football should have 0 drives"
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
# Full-coverage regression: every wrapper short name has a parser
# ===========================================================================
#
# These tests lock in the 100%-coverage state achieved by registering
# parse_single_entity / parse_items for the long tail of single-entity
# and list-shape Core v2 endpoints. They catch any future regression
# where a new wrapper is added to _UNIVERSAL_WRAPPERS / _NCAA_WRAPPERS /
# _FOOTBALL_WRAPPERS / _MLB_WRAPPERS without an ENDPOINT_PARSERS entry.


def test_every_wrapper_short_name_has_a_registered_parser():
    """Every short name across all 4 wrapper tables must be in
    ENDPOINT_PARSERS so the return_parsed shim activates on it."""
    from sportsdataverse._common_espn import (
        _FOOTBALL_WRAPPERS,
        _MLB_WRAPPERS,
        _NCAA_WRAPPERS,
        _UNIVERSAL_WRAPPERS,
    )
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS

    all_short = set(
        [s for s, _ in _UNIVERSAL_WRAPPERS]
        + [s for s, _ in _NCAA_WRAPPERS]
        + [s for s, _ in _FOOTBALL_WRAPPERS]
        + [s for s, _ in _MLB_WRAPPERS]
    )
    registered = set(ENDPOINT_PARSERS)
    missing = all_short - registered
    assert not missing, (
        f"ENDPOINT_PARSERS missing {len(missing)} wrapper short names: "
        f"{sorted(missing)}"
    )


def test_no_stale_entries_in_endpoint_parsers_registry():
    """Every key in ENDPOINT_PARSERS must correspond to a real wrapper
    short name in one of the 4 wrapper tables."""
    from sportsdataverse._common_espn import (
        _FOOTBALL_WRAPPERS,
        _MLB_WRAPPERS,
        _NCAA_WRAPPERS,
        _UNIVERSAL_WRAPPERS,
    )
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS

    all_short = set(
        [s for s, _ in _UNIVERSAL_WRAPPERS]
        + [s for s, _ in _NCAA_WRAPPERS]
        + [s for s, _ in _FOOTBALL_WRAPPERS]
        + [s for s, _ in _MLB_WRAPPERS]
    )
    stale = set(ENDPOINT_PARSERS) - all_short
    assert not stale, (
        f"ENDPOINT_PARSERS has {len(stale)} stale entries with no "
        f"corresponding wrapper: {sorted(stale)}"
    )


def test_return_parsed_shim_active_on_every_wrapper_across_all_leagues():
    """The return_parsed kwarg must appear in the signature of every
    bound wrapper across all 7 league extension modules."""
    import inspect

    import sportsdataverse.cfb.cfb_espn_ext as cfb_mod
    import sportsdataverse.mbb.mbb_espn_ext as mbb_mod
    import sportsdataverse.mlb.mlb_espn_ext as mlb_mod
    import sportsdataverse.nba.nba_espn_ext as nba_mod
    import sportsdataverse.nfl.nfl_espn_ext as nfl_mod
    import sportsdataverse.wbb.wbb_espn_ext as wbb_mod
    import sportsdataverse.wnba.wnba_espn_ext as wnba_mod
    from sportsdataverse.cfb.cfb_espn_ext import __all__ as cfb_all
    from sportsdataverse.mbb.mbb_espn_ext import __all__ as mbb_all
    from sportsdataverse.mlb.mlb_espn_ext import __all__ as mlb_all
    from sportsdataverse.nba.nba_espn_ext import __all__ as nba_all
    from sportsdataverse.nfl.nfl_espn_ext import __all__ as nfl_all
    from sportsdataverse.wbb.wbb_espn_ext import __all__ as wbb_all
    from sportsdataverse.wnba.wnba_espn_ext import __all__ as wnba_all

    leagues = [
        ("nba",  nba_all,  nba_mod),
        ("wnba", wnba_all, wnba_mod),
        ("mbb",  mbb_all,  mbb_mod),
        ("wbb",  wbb_all,  wbb_mod),
        ("cfb",  cfb_all,  cfb_mod),
        ("nfl",  nfl_all,  nfl_mod),
        ("mlb",  mlb_all,  mlb_mod),
    ]
    missing = []
    total = 0
    for league, names, mod in leagues:
        for name in names:
            fn = getattr(mod, name, None)
            if fn is None or not callable(fn):
                continue
            total += 1
            sig = inspect.signature(fn)
            if "return_parsed" not in sig.parameters:
                missing.append(f"{league}.{name}")
    assert total >= 800, (
        f"sanity check: expected >=800 wrappers across 7 leagues, got {total}"
    )
    assert not missing, (
        f"{len(missing)} wrappers missing the return_parsed shim: "
        f"{missing[:10]}..."
    )


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

    df = parse_summary(_load("summary_nba"), section="plays",
                       return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 100


def test_summary_parser_routes_via_endpoint_parsers_registry():
    """``summary`` is registered in ENDPOINT_PARSERS so the
    return_parsed shim should route through parse_summary."""
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS, parse_summary

    assert "summary" in ENDPOINT_PARSERS
    assert ENDPOINT_PARSERS["summary"] is parse_summary


def test_return_parsed_shim_dispatches_through_new_parser_for_team_roster():
    """The wrapper closure for espn_nba_team_roster should route through
    parse_team_roster when return_parsed=True. Verify offline by
    monkey-patching the core HTTP fetcher to return a known fixture."""
    import polars as pl

    import sportsdataverse._common_espn as ce

    fixture = _load("team_roster_nba")
    real_get = ce._get
    ce._get = lambda *args, **kwargs: fixture
    try:
        from sportsdataverse.nba.nba_espn_ext import espn_nba_team_roster

        raw = espn_nba_team_roster(team_id=13)
        assert isinstance(raw, dict)
        assert "athletes" in raw

        df = espn_nba_team_roster(team_id=13, return_parsed=True)
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 10
        assert "first_name" in df.columns
    finally:
        ce._get = real_get
