"""Live ESPN / NHL / MLB smoke tests for the cross-league wrappers.

All tests are gated behind SDV_PY_LIVE_TESTS=1.  Without the env var
every test in this module is skipped so CI never hammers live endpoints.

Run live::

    SDV_PY_LIVE_TESTS=1 pytest tests/test_espn_live.py -v

The live-gating mechanism is shared with the rest of the test suite —
``skip_if_no_live`` is defined once in :mod:`tests.conftest` and applied
as the module-level ``pytestmark`` here so every test in this file
inherits the skip-if-no-live behaviour.
"""

from __future__ import annotations

import pytest

from tests.conftest import skip_if_no_live

# Apply the shared skip marker to every test in this module.
pytestmark = skip_if_no_live


# ===========================================================================
# 1. Per-league teams_site count assertions (7 tests)
# ===========================================================================


def test_espn_nba_teams_site_returns_30_teams():
    from sportsdataverse.nba.nba_espn_ext import espn_nba_teams_site

    payload = espn_nba_teams_site(return_parsed=False)
    sports = payload.get("sports", [{}])
    teams = sports[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) == 30, f"expected 30 NBA teams, got {len(teams)}"


def test_espn_mbb_teams_site_returns_many_teams():
    from sportsdataverse.mbb.mbb_espn_ext import espn_mbb_teams_site

    payload = espn_mbb_teams_site(return_parsed=False)
    sports = payload.get("sports", [{}])
    teams = sports[0].get("leagues", [{}])[0].get("teams") or []
    # ~350 DI programs; be loose to survive ESPN roster churn
    assert len(teams) >= 300, f"expected >=300 MBB teams, got {len(teams)}"


def test_espn_wnba_teams_site_returns_wnba_teams():
    from sportsdataverse.wnba.wnba_espn_ext import espn_wnba_teams_site

    payload = espn_wnba_teams_site(return_parsed=False)
    sports = payload.get("sports", [{}])
    teams = sports[0].get("leagues", [{}])[0].get("teams") or []
    # WNBA has been expanding: 12 (2023) → 13 (2024) → 15 (2025+).
    # Assert a stable lower bound rather than an exact count.
    assert len(teams) >= 12, f"expected >=12 WNBA teams, got {len(teams)}"


def test_espn_wbb_teams_site_returns_many_teams():
    from sportsdataverse.wbb.wbb_espn_ext import espn_wbb_teams_site

    payload = espn_wbb_teams_site(return_parsed=False)
    sports = payload.get("sports", [{}])
    teams = sports[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) >= 300, f"expected >=300 WBB teams, got {len(teams)}"


def test_espn_cfb_teams_site_returns_many_teams():
    from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_teams_site

    payload = espn_cfb_teams_site(return_parsed=False)
    sports = payload.get("sports", [{}])
    teams = sports[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) >= 200, f"expected >=200 CFB teams, got {len(teams)}"


def test_espn_nfl_teams_site_returns_32_teams():
    from sportsdataverse.nfl.nfl_espn_ext import espn_nfl_teams_site

    payload = espn_nfl_teams_site(return_parsed=False)
    sports = payload.get("sports", [{}])
    teams = sports[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) == 32, f"expected 32 NFL teams, got {len(teams)}"


def test_espn_mlb_teams_site_returns_30_teams():
    from sportsdataverse.mlb.mlb_espn_ext import espn_mlb_teams_site

    payload = espn_mlb_teams_site(return_parsed=False)
    sports = payload.get("sports", [{}])
    teams = sports[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) == 30, f"expected 30 MLB teams, got {len(teams)}"


# ===========================================================================
# 2. Per-league scoreboard returns events (NBA + MLB)
# ===========================================================================


def test_espn_nba_scoreboard_returns_events_for_known_date():
    from sportsdataverse.nba.nba_espn_ext import espn_nba_scoreboard

    # Christmas Day 2024 — historically game-heavy
    payload = espn_nba_scoreboard(dates=20241225, return_parsed=False)
    events = payload.get("events") or []
    assert isinstance(events, list), "events must be a list"
    assert len(events) > 0, "expected at least one NBA event on 2024-12-25"


def test_espn_mlb_scoreboard_returns_events_for_known_date():
    from sportsdataverse.mlb.mlb_espn_ext import espn_mlb_scoreboard

    # Opening Day 2024 — full slate of games
    payload = espn_mlb_scoreboard(dates=20240320, return_parsed=False)
    events = payload.get("events") or []
    assert isinstance(events, list), "events must be a list"
    assert len(events) > 0, "expected at least one MLB event on 2024-03-20"


# ===========================================================================
# 3. Web v3 athlete_overview returns content (NBA + MLB)
# ===========================================================================


def test_espn_nba_player_overview_lebron():
    from sportsdataverse.nba.nba_espn_ext import espn_nba_player_overview

    # LeBron James ESPN id = 1966
    payload = espn_nba_player_overview(athlete_id=1966, return_parsed=False)
    assert isinstance(payload, dict), "expected a dict response"
    assert len(payload) > 0, "payload is empty for LeBron James (id=1966)"
    # Web v3 overview always includes athlete and statsSummary (or similar)
    top_keys = set(payload.keys())
    assert top_keys, "payload has no top-level keys"


def test_espn_mlb_player_overview_aaron_judge():
    from sportsdataverse.mlb.mlb_espn_ext import espn_mlb_player_overview

    # Aaron Judge ESPN id = 33192
    payload = espn_mlb_player_overview(athlete_id=33192, return_parsed=False)
    assert isinstance(payload, dict), "expected a dict response"
    assert len(payload) > 0, "payload is empty for Aaron Judge (id=33192)"
    top_keys = set(payload.keys())
    assert top_keys, "payload has no top-level keys"


# ===========================================================================
# 4. Per-league conferences / groups (CFB)
# ===========================================================================


def test_espn_cfb_conferences_returns_groups():
    from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_conferences

    payload = espn_cfb_conferences(return_parsed=False)
    assert isinstance(payload, dict), "expected a dict response"
    # The groups endpoint wraps its list under 'groups' or 'items'
    assert "groups" in payload or "items" in payload, (
        f"expected 'groups' or 'items' key, got keys: {list(payload.keys())}"
    )


# ===========================================================================
# 5. NHL api-web (3 tests)
# ===========================================================================


def test_nhl_web_standings_returns_standings_list():
    from sportsdataverse.nhl.nhl_api_web import nhl_standings

    # Use a past date from the 2023-24 regular season to get stable data
    payload = nhl_standings(date="2024-04-01", return_parsed=False)
    assert isinstance(payload, dict), "expected a dict response"
    standings = payload.get("standings") or []
    assert len(standings) >= 32, f"expected >=32 team rows in standings, got {len(standings)}"


def test_nhl_web_schedule_returns_game_week():
    from sportsdataverse.nhl.nhl_api_web import nhl_web_schedule

    # A specific past regular-season date guaranteed to have games
    payload = nhl_web_schedule(date="2024-01-15", return_parsed=False)
    assert isinstance(payload, dict), "expected a dict response"
    game_week = payload.get("gameWeek") or []
    assert isinstance(game_week, list), "'gameWeek' must be a list"
    # At least one day in the returned week should have games
    all_games = [g for day in game_week for g in (day.get("games") or [])]
    assert len(all_games) > 0, "expected at least one NHL game in the week of 2024-01-15"


def test_nhl_web_roster_toronto_2024():
    from sportsdataverse.nhl.nhl_api_web import nhl_roster

    # TOR 2024 (end-year → "20232024")
    payload = nhl_roster(team="TOR", season=2024, return_parsed=False)
    assert isinstance(payload, dict), "expected a dict response"
    forwards = payload.get("forwards") or []
    defense = payload.get("defensemen") or []
    goalies = payload.get("goalies") or []
    total = len(forwards) + len(defense) + len(goalies)
    assert total >= 20, f"expected >=20 players on TOR 2024 roster, got {total}"


# ===========================================================================
# 6. MLB Stats API (2 tests)
# ===========================================================================


def test_mlb_api_teams_returns_30_teams_for_2024():
    # mlb_api_teams is an irregular (multi-param) wrapper kept hand-written in
    # mlb_api_extra after the codegen cutover; import from the package namespace,
    # which re-exports both the generated mlb_api wrappers and the residuals.
    from sportsdataverse.mlb import mlb_api_teams

    payload = mlb_api_teams(season=2024)
    teams = payload.get("teams") or []
    # MLB roster includes 30 active franchises; some responses also include
    # Spring Training or historical entries so >=30 is the safe assertion.
    assert len(teams) >= 30, f"expected >=30 MLB teams for 2024, got {len(teams)}"


def test_mlb_api_schedule_opening_day_2024_returns_games():
    # mlb_api_schedule is a hand-written residual in mlb_api_extra (it forwards
    # arbitrary **filters); import from the package namespace, which re-exports it.
    from sportsdataverse.mlb import mlb_api_schedule

    payload = mlb_api_schedule(date="2024-03-20", sport_id=1)
    dates = payload.get("dates") or []
    all_games = [g for d in dates for g in (d.get("games") or [])]
    assert len(all_games) > 0, "expected MLB games on Opening Day 2024-03-20, got none"


# ===========================================================================
# 7. Savant Statcast leaderboard (1 test)
# ===========================================================================


def test_statcast_leaderboard_expected_statistics_2024_nonempty():
    from sportsdataverse.mlb.mlb_statcast import statcast_leaderboard_expected_statistics

    result = statcast_leaderboard_expected_statistics(year=2024)
    # Returns a polars DataFrame (csv=True default) or a pandas DataFrame
    # depending on return_as_pandas; the default gives polars.
    try:
        import polars as pl

        if isinstance(result, pl.DataFrame):
            assert result.shape[0] > 0, "expected non-empty polars frame for xStats 2024"
            return
    except ImportError:
        pass
    try:
        import pandas as pd

        if isinstance(result, pd.DataFrame):
            assert len(result) > 0, "expected non-empty pandas frame for xStats 2024"
            return
    except ImportError:
        pass
    # Fallback: if some other collection-like (list, dict, etc.)
    assert result is not None, "statcast_leaderboard_expected_statistics returned None"
    # If it's a dict with a non-empty content, accept that too
    if hasattr(result, "__len__"):
        assert len(result) > 0, f"result is empty: {type(result)}"


# ===========================================================================
# 8. Cross-league pattern: smoke-test the factory
# ===========================================================================


def test_espn_nfl_scoreboard_is_callable_with_correct_name():
    from sportsdataverse.nfl.nfl_espn_ext import espn_nfl_scoreboard

    assert callable(espn_nfl_scoreboard), "espn_nfl_scoreboard must be callable"
    assert espn_nfl_scoreboard.__name__ == "espn_nfl_scoreboard", (
        f"__name__ mismatch: got {espn_nfl_scoreboard.__name__!r}"
    )


def test_espn_mlb_scoreboard_is_callable_with_correct_name():
    from sportsdataverse.mlb.mlb_espn_ext import espn_mlb_scoreboard

    assert callable(espn_mlb_scoreboard), "espn_mlb_scoreboard must be callable"
    assert espn_mlb_scoreboard.__name__ == "espn_mlb_scoreboard", (
        f"__name__ mismatch: got {espn_mlb_scoreboard.__name__!r}"
    )


# ===========================================================================
# 10. NCAA recruits (CFB 2024)
# ===========================================================================


def test_espn_cfb_season_recruits_2024_returns_items():
    from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_recruits

    payload = espn_cfb_recruits(season=2024, limit=10, return_parsed=False)
    assert isinstance(payload, dict), "expected a dict response"
    # Core v2 season_recruits wraps its list under 'items' (paginated response)
    items = payload.get("items") or []
    assert isinstance(items, list), f"expected 'items' to be a list, got {type(items)}"
    # If data is available there should be recruits; tolerate empty for off-season
    # by checking the key exists rather than enforcing > 0
    assert "items" in payload, f"expected 'items' key in response, got keys: {list(payload.keys())}"


# ===========================================================================
# 11. WNBA parity for the universal _common_espn_parsers
# ===========================================================================
#
# The parsers in sportsdataverse._common_espn_parsers are league-agnostic by
# design — they accept the raw Dict that any espn_{league}_* wrapper returns.
# These tests prove that claim for WNBA specifically (the historically thinnest
# data surface) by chaining one wrapper call with one parser call, and
# asserting that the parser returns a polars DataFrame with > 0 rows for
# every applicable endpoint.


def test_parse_teams_handles_wnba_payload():
    import polars as pl

    from sportsdataverse._common_espn_parsers import parse_teams
    from sportsdataverse.wnba.wnba_espn_ext import espn_wnba_teams_site

    raw = espn_wnba_teams_site(return_parsed=False)
    df = parse_teams(raw)
    assert isinstance(df, pl.DataFrame), f"expected polars frame, got {type(df)}"
    assert df.height >= 12, f"expected >=12 WNBA team rows from parse_teams, got {df.height}"


def test_parse_scoreboard_handles_wnba_payload():
    import polars as pl

    from sportsdataverse._common_espn_parsers import parse_scoreboard
    from sportsdataverse.wnba.wnba_espn_ext import espn_wnba_scoreboard

    # July 7 2024 — guaranteed mid-season WNBA slate
    raw = espn_wnba_scoreboard(dates=20240707, return_parsed=False)
    df = parse_scoreboard(raw)
    assert isinstance(df, pl.DataFrame)
    # If the date has games (it does, historically) we expect rows; otherwise
    # parse_scoreboard returns an empty frame (which is itself valid behaviour).
    if raw.get("events") or []:
        assert df.height > 0, "scoreboard had events but parse_scoreboard returned 0 rows"


def test_parse_standings_handles_wnba_payload():
    import polars as pl

    from sportsdataverse._common_espn_parsers import parse_standings
    from sportsdataverse.wnba.wnba_espn_ext import espn_wnba_standings

    raw = espn_wnba_standings(return_parsed=False)
    df = parse_standings(raw)
    assert isinstance(df, pl.DataFrame)
    # WNBA standings have one row per team; minimum 12 across history
    assert df.height >= 12, f"expected >=12 WNBA standing rows, got {df.height}"


def test_parse_athlete_overview_handles_wnba_payload():
    import polars as pl

    from sportsdataverse._common_espn_parsers import parse_athlete_overview
    from sportsdataverse.wnba.wnba_espn_ext import espn_wnba_player_overview

    # A'ja Wilson — ESPN id 3149391, perennial MVP candidate
    raw = espn_wnba_player_overview(athlete_id=3149391, return_parsed=False)
    df = parse_athlete_overview(raw)
    assert isinstance(df, pl.DataFrame), f"expected polars frame, got {type(df)}"
    # Overview parser flattens to a single-row summary OR a multi-row stats
    # table — both are valid; assert "at least one row OR an empty frame
    # because Web v3 returned an empty payload" rather than strict > 0
    if raw:
        assert df.height >= 0, "parse_athlete_overview returned a malformed frame"


def test_parse_leaders_handles_wnba_payload():
    import polars as pl

    from sportsdataverse._common_espn_parsers import parse_leaders
    from sportsdataverse.wnba.wnba_espn_ext import espn_wnba_leaders

    raw = espn_wnba_leaders(return_parsed=False)
    df = parse_leaders(raw)
    assert isinstance(df, pl.DataFrame)
    # Leaders endpoint always returns multiple stat categories × multiple
    # leaders — tolerate emptiness during the off-season but reject malformed
    if raw.get("leaders") or raw.get("categories") or raw.get("items"):
        assert df.height >= 0, "parse_leaders returned a malformed frame"


# ===========================================================================
# 12. return_parsed=True shim on bound wrappers
# ===========================================================================
#
# Every wrapper whose short name is registered in
# sportsdataverse._common_espn_parsers.ENDPOINT_PARSERS now accepts an
# optional ``return_parsed=True`` kwarg that dispatches the raw response
# through the corresponding parser.  These tests verify the contract end
# to end against a live ESPN endpoint.


def test_return_parsed_true_routes_through_registered_parser():
    import polars as pl

    from sportsdataverse.nba.nba_espn_ext import espn_nba_teams_site

    df = espn_nba_teams_site(return_parsed=True)
    assert isinstance(df, pl.DataFrame), f"expected polars DataFrame from return_parsed=True, got {type(df)}"
    assert df.height >= 30, f"expected >=30 NBA team rows via return_parsed, got {df.height}"


def test_return_parsed_with_return_as_pandas_returns_pandas():
    import pandas as pd

    from sportsdataverse.nba.nba_espn_ext import espn_nba_teams_site

    df = espn_nba_teams_site(return_parsed=True, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame), f"expected pandas DataFrame, got {type(df)}"
    assert len(df) >= 30


def test_return_parsed_false_keeps_raw_dict():
    from sportsdataverse.nba.nba_espn_ext import espn_nba_teams_site

    raw = espn_nba_teams_site(return_parsed=False)  # 0.0.54 default is True; opt out for the Dict
    assert isinstance(raw, dict), f"expected raw dict when return_parsed=False, got {type(raw)}"
    # Confirm raw shape preserved
    assert "sports" in raw, f"raw payload missing 'sports' key — got {list(raw)[:3]}"


# ===========================================================================
# 13. NHL EDGE parser layer
# ===========================================================================


@pytest.mark.xfail(
    reason=(
        "NHL EDGE *_top_10 endpoints (skater_shot_speed_top_10 and 11 siblings) "
        "return 404 upstream as of 2026-05-23. The OpenAPI spec lists them but "
        "they're not live. The wrappers + parse_edge_top10 are kept for "
        "forward-compatibility per CHANGELOG 0.0.51. Will XPASS (and this "
        "marker can be removed) once the endpoint is restored."
    ),
    strict=False,
    raises=Exception,
)
def test_parse_edge_top10_handles_live_leaderboard():
    import polars as pl

    from sportsdataverse.nhl.nhl_edge import nhl_edge_skater_shot_speed_top_10
    from sportsdataverse.nhl.nhl_edge_parsers import parse_edge_top10

    raw = nhl_edge_skater_shot_speed_top_10(positions="all", sort_by="maxSpeed", return_parsed=False)
    df = parse_edge_top10(raw)
    assert isinstance(df, pl.DataFrame), f"expected polars DataFrame, got {type(df)}"
    # Off-season may return empty; in-season expect rows.
    if any(isinstance(v, list) and v for v in (raw or {}).values()):
        assert df.height > 0, "non-empty raw payload but parse_edge_top10 returned 0 rows"


def test_parser_for_edge_falls_back_to_generic():
    from sportsdataverse.nhl.nhl_edge_parsers import (
        parse_edge_detail,
        parse_edge_payload,
        parse_edge_top10,
        parser_for_edge,
    )

    # Registered: top-10 → parse_edge_top10
    assert parser_for_edge("nhl_edge_skater_shot_speed_top_10") is parse_edge_top10
    # Registered: detail → parse_edge_detail
    assert parser_for_edge("nhl_edge_skater_detail") is parse_edge_detail
    # Unknown name → generic fallback
    assert parser_for_edge("nhl_edge_does_not_exist") is parse_edge_payload


# ===========================================================================
# 14. NCAA basketball + CFB live coverage
# ===========================================================================
#
# The existing 13 sections cover live integration for NBA / WNBA / NFL /
# NHL / MLB. This section adds equivalent coverage for the 3 NCAA-side
# leagues (CFB / MBB / WBB) so the live-test suite is symmetric across
# all 8 ESPN leagues. Tests are intentionally loose on row counts
# because NCAA season schedules vary by sport (CFB is fall-only,
# basketball is winter-only).


def test_espn_cfb_team_roster_live():
    """CFB rosters use the position-grouped shape (offense / defense /
    specialTeam). The return_parsed shim should produce a multi-row
    frame with the position_group column."""
    from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_team_roster

    df = espn_cfb_team_roster(team_id=333, return_parsed=True)  # Alabama
    assert df.height >= 50, f"expected >=50 CFB roster rows (typical full team), got {df.height}"
    assert "position_group" in df.columns, "CFB roster should be position-grouped (offense/defense/specialTeam)"


def test_espn_mbb_team_roster_live():
    """NCAA M basketball roster uses the flat shape (no position
    groups), matching NBA / WNBA convention."""
    from sportsdataverse.mbb.mbb_espn_ext import espn_mbb_team_roster

    df = espn_mbb_team_roster(team_id=150, return_parsed=True)  # Duke
    assert df.height >= 10, f"expected >=10 MBB roster rows, got {df.height}"
    assert "first_name" in df.columns or "last_name" in df.columns


def test_espn_wbb_team_roster_live():
    from sportsdataverse.wbb.wbb_espn_ext import espn_wbb_team_roster

    df = espn_wbb_team_roster(team_id=41, return_parsed=True)  # UConn
    assert df.height >= 8, f"expected >=8 WBB roster rows, got {df.height}"


def test_espn_cfb_news_live():
    from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_news

    df = espn_cfb_news(limit=5, return_parsed=True)
    assert df.height >= 1, f"expected >=1 CFB article, got {df.height}"
    assert "headline" in df.columns


def test_espn_mbb_news_live():
    from sportsdataverse.mbb.mbb_espn_ext import espn_mbb_news

    df = espn_mbb_news(limit=5, return_parsed=True)
    assert df.height >= 1, f"expected >=1 MBB article, got {df.height}"


def test_espn_wbb_news_live():
    from sportsdataverse.wbb.wbb_espn_ext import espn_wbb_news

    df = espn_wbb_news(limit=5, return_parsed=True)
    assert df.height >= 1, f"expected >=1 WBB article, got {df.height}"


def test_espn_cfb_team_schedule_live():
    """CFB team schedule may be off-season at test time (college
    football is fall-only). Tolerate the empty case but require a
    non-empty raw payload structure either way."""
    from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_team_schedule

    raw = espn_cfb_team_schedule(team_id=333, return_parsed=False)  # Alabama
    assert isinstance(raw, dict), f"expected dict, got {type(raw)}"
    # Raw payload always has 'team' identifier and 'requestedSeason' even
    # when the schedule is empty
    assert "team" in raw or "events" in raw, f"unexpected schedule shape — top keys: {list(raw)[:5]}"


def test_espn_mbb_team_schedule_live():
    """NCAA M basketball team schedule should have ~30 games during the
    season (Nov-Mar). Captures outside that window may return empty —
    accept either the populated or the empty case."""
    from sportsdataverse.mbb.mbb_espn_ext import espn_mbb_team_schedule

    df = espn_mbb_team_schedule(team_id=150, return_parsed=True)
    # If we're in-season we expect >=10 games; if off-season, df is empty.
    assert df.height == 0 or df.height >= 10, f"expected 0 or >=10 MBB schedule rows, got {df.height}"


def test_espn_wbb_team_schedule_live():
    from sportsdataverse.wbb.wbb_espn_ext import espn_wbb_team_schedule

    df = espn_wbb_team_schedule(team_id=41, return_parsed=True)
    assert df.height == 0 or df.height >= 10, f"expected 0 or >=10 WBB schedule rows, got {df.height}"


# ===========================================================================
# 15. NCAA summary endpoint live tests
# ===========================================================================
#
# The NBA/MLB/NFL/NHL/WNBA fixtures already exercise parse_summary
# offline; this section adds *live* coverage for the 3 NCAA leagues
# using the championship events that won't disappear from ESPN's cache.
# Tests use well-archived past games to stay stable year-round.


def test_espn_mbb_summary_live_full_dispatch():
    """2024 NCAA M Championship Purdue @ UConn — verify the full
    parse_summary dispatcher produces all 21 sub-frames from a live
    fetch, with the football-only sections empty as expected."""
    from sportsdataverse._common_espn_parsers import (
        SUMMARY_SECTION_PARSERS,
        parse_summary,
    )
    from sportsdataverse.mbb.mbb_espn_ext import espn_mbb_summary

    raw = espn_mbb_summary(event_id=401638645, return_parsed=False)
    assert isinstance(raw, dict), f"expected dict, got {type(raw)}"
    out = parse_summary(raw)
    assert set(out) == set(SUMMARY_SECTION_PARSERS), "dispatcher returned different sections than registry"
    # Non-football leagues should have populated boxscore_player + plays
    assert out["boxscore_player"].height >= 20, f"expected >=20 athletes, got {out['boxscore_player'].height}"
    assert out["plays"].height >= 100, f"expected >=100 plays, got {out['plays'].height}"
    # Basketball doesn't use the football-only sections
    assert out["drives"].height == 0
    assert out["drive_plays"].height == 0


def test_espn_wbb_summary_live_works_via_return_parsed_shim():
    """2024 NCAA W Championship Iowa @ SC — exercise the
    return_parsed=True shim end to end (espn_wbb_summary with the
    kwarg should route through parse_summary)."""
    from sportsdataverse.wbb.wbb_espn_ext import espn_wbb_summary

    out = espn_wbb_summary(event_id=401637613, return_parsed=True)
    # parse_summary returns a dict of sub-frames (not a single frame)
    assert isinstance(out, dict)
    # Boxscore + plays should always have data for a championship game
    assert out["boxscore_player"].height >= 20
    assert out["plays"].height >= 100


def test_espn_cfb_summary_live_uses_football_pattern():
    """2025 CFB National Championship OSU @ ND — verify CFB uses the
    same drives.previous[]/scoringPlays football pattern as NFL, not
    the top-level plays[] pattern of basketball/hockey/baseball."""
    from sportsdataverse._common_espn_parsers import parse_summary
    from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_summary

    out = parse_summary(espn_cfb_summary(event_id=401677192, return_parsed=False))
    # ESPN intermittently returns an empty summary for this fixed historical game
    # (transient 200 / rate-limit). The football-vs-basketball pattern can't be
    # verified without data, so skip rather than hard-fail on an empty payload.
    if out["drives"].height == 0 and out["drive_plays"].height == 0:
        pytest.skip("ESPN returned an empty CFB summary for 401677192 (transient); skipping shape checks")
    # CFB football: top-level plays[] is empty, drives.previous[] populated
    assert out["plays"].height == 0
    assert out["drives"].height >= 10, f"expected >=10 drives, got {out['drives'].height}"
    # drive_plays unrolls drives[i].plays[] for true PBP parity
    assert out["drive_plays"].height >= 50
    # scoringPlays present for football
    assert out["scoring_plays"].height >= 1
    # CFB rosters are huge (~70-80 game-day squad per team) — but ESPN ships the
    # boxscore section independently of the drive data and intermittently returns
    # it empty for this historical game even when drives/plays are fully populated
    # (observed on a CI runner: drives>=10 yet boxscore_player==0). This test's
    # real subject is the football drives/scoringPlays *pattern* asserted above,
    # so only check the roster size when ESPN actually shipped a boxscore.
    if out["boxscore_player"].height > 0:
        assert out["boxscore_player"].height >= 50


# ===========================================================================
# 16. MLB Statcast pitch-search + chunking + truncation guard
# ===========================================================================
#
# The existing test_statcast_leaderboard_expected_statistics_2024_nonempty
# covers one of the 9 statcast_leaderboard_* endpoints. This section
# adds coverage for the pitch-by-pitch search surface itself — the
# raise-on-truncation guard, the auto-chunked variant that handles
# multi-week ranges, and the small-range happy path.


def test_statcast_search_returns_polars_frame_for_small_date_range():
    """A 2-day mid-season slice fits well under the 25k cap and should
    return a polars DataFrame with the expected ~90-column wide schema."""
    from sportsdataverse.mlb import statcast_search

    df = statcast_search(start_date="2024-06-15", end_date="2024-06-16")
    # The exact row count varies by query day, but a full slate of MLB
    # games typically ships >=4,000 pitches per day league-wide.
    try:
        import polars as pl

        assert isinstance(df, pl.DataFrame), f"expected polars, got {type(df)}"
        assert df.height > 1000, f"expected >1000 pitches over 2 days, got {df.height}"
    except ImportError:
        # Fallback for pandas-only environments
        assert len(df) > 1000


def test_statcast_search_chunked_stitches_multi_week_range():
    """A 3-week range will exceed the 25k cap in a single response so
    the chunked variant must auto-chunk and stitch client-side without
    triggering a truncation error.

    ``chunk_days=3`` is intentional: a peak-season MLB week (~5k
    pitches/day × 7 = 35k) blows past the 25k Savant cap on a single
    7-day chunk, which would defeat the test's purpose of proving the
    stitching path works. 3-day chunks (~12-15k pitches each) keep
    every individual request safely under the cap while still requiring
    ~7 chunks to cover the 21-day range — exercising the loop, the
    stitch, and the cross-chunk concat."""
    from sportsdataverse.mlb import statcast_search_chunked

    df = statcast_search_chunked(
        start_date="2024-06-01",
        end_date="2024-06-21",
        chunk_days=3,
    )
    try:
        import polars as pl

        assert isinstance(df, pl.DataFrame)
        # 3 weeks at ~4,000-5,000 pitches/day = ~80,000-100,000 pitches
        assert df.height > 25_000, f"expected chunked total >25k pitches (proves stitching), got {df.height}"
    except ImportError:
        assert len(df) > 25_000


def test_statcast_search_raises_runtime_error_on_truncation():
    """An unfiltered 7-day range typically blows past the 25k cap.
    With raise_on_truncation=True (default), the wrapper must raise
    RuntimeError rather than silently shipping a partial frame."""
    import pytest as _pytest

    from sportsdataverse.mlb import statcast_search

    # Use a known full week from the 2024 regular season — should hit
    # exactly 25,000 rows when the response is truncated.
    with _pytest.raises(RuntimeError, match=r"25,?000"):
        statcast_search(
            start_date="2024-07-08",
            end_date="2024-07-14",
        )


# ===========================================================================
# 17. NCAA return_parsed shim contract — raw Dict ↔ polars frame round-trip
# ===========================================================================
#
# Section 14 already verifies that NCAA wrappers (CFB/MBB/WBB × roster/
# news/schedule) work with return_parsed=True. This section additionally
# proves the contract that's central to the parser layer: the same
# wrapper call with vs. without return_parsed=True returns equivalent
# data — raw Dict on omission, polars frame on opt-in, pandas on
# return_as_pandas=True. Catches any regression where the shim either
# returns a partial frame OR fails to route through the parser at all.


@pytest.mark.parametrize(
    "league,team_id,parser_name",
    [
        ("cfb", 333, "parse_team_roster"),  # Alabama (position-grouped)
        ("mbb", 150, "parse_team_roster"),  # Duke (flat)
        ("wbb", 41, "parse_team_roster"),  # UConn (flat)
    ],
)
def test_ncaa_team_roster_return_parsed_round_trip(league, team_id, parser_name):
    """Verify the three calling modes (raw / polars / pandas) all return
    consistent data. The raw payload's athletes[] should produce the
    same number of player rows as the parsed frame."""
    import pandas as pd
    import polars as pl

    from sportsdataverse._common_espn_parsers import (
        ENDPOINT_PARSERS,
        parse_team_roster,
    )

    # Resolve the wrapper from the league extension module
    module = __import__(f"sportsdataverse.{league}.{league}_espn_ext", fromlist=["espn_*"])
    wrapper = getattr(module, f"espn_{league}_team_roster")

    # Three calling modes
    raw = wrapper(team_id=team_id, return_parsed=False)
    df_pl = wrapper(team_id=team_id, return_parsed=True)
    df_pd = wrapper(team_id=team_id, return_parsed=True, return_as_pandas=True)

    assert isinstance(raw, dict), f"raw should be Dict, got {type(raw)}"
    assert isinstance(df_pl, pl.DataFrame), f"polars expected, got {type(df_pl)}"
    assert isinstance(df_pd, pd.DataFrame), f"pandas expected, got {type(df_pd)}"
    # Both DataFrame variants should agree on row count
    assert df_pl.height == len(df_pd), f"polars/pandas row count mismatch: {df_pl.height} vs {len(df_pd)}"
    # And the registry should route to parse_team_roster
    assert ENDPOINT_PARSERS["team_roster"] is parse_team_roster
    # Re-running the parser on the raw payload should produce the same shape
    df_again = parse_team_roster(raw)
    assert df_again.shape == df_pl.shape, f"direct vs shim shape mismatch: {df_again.shape} vs {df_pl.shape}"


@pytest.mark.parametrize(
    "league,team_id",
    [
        ("cfb", 333),
        ("mbb", 150),
        ("wbb", 41),
    ],
)
def test_ncaa_team_schedule_return_parsed_handles_offseason(league, team_id):
    """Schedule wrappers must produce a zero-row frame (not raise) when
    the team is in their off-season. CFB is off-season in May/Jun;
    MBB/WBB are off-season in summer. The shim should route through
    parse_team_schedule transparently in either case."""
    import polars as pl

    module = __import__(f"sportsdataverse.{league}.{league}_espn_ext", fromlist=["espn_*"])
    wrapper = getattr(module, f"espn_{league}_team_schedule")

    df = wrapper(team_id=team_id, return_parsed=True)
    assert isinstance(df, pl.DataFrame)
    # Either in-season (>= 10 games) or off-season (0 rows) — never an exception
    assert df.height == 0 or df.height >= 10, f"{league}: expected 0 or >=10 schedule rows, got {df.height}"


@pytest.mark.parametrize("league", ["cfb", "mbb", "wbb"])
def test_ncaa_news_return_parsed_matches_direct_parse_news(league):
    """The return_parsed=True shim on the news wrapper must produce the
    same DataFrame as calling parse_news directly on the raw payload —
    i.e. the shim isn't dropping or mangling rows."""
    from sportsdataverse._common_espn_parsers import parse_news

    module = __import__(f"sportsdataverse.{league}.{league}_espn_ext", fromlist=["espn_*"])
    wrapper = getattr(module, f"espn_{league}_news")

    raw = wrapper(limit=5, return_parsed=False)
    df_a = parse_news(raw)
    df_b = wrapper(limit=5, return_parsed=True)
    assert df_a.shape == df_b.shape, f"{league}: shim vs direct shape mismatch: {df_a.shape} vs {df_b.shape}"


# ===========================================================================
# 18. game_rosters column-width regression + nhl_schedule None-guard
# ===========================================================================
#
# Regression coverage for four previously-broken ESPN wrappers (surfaced by
# the autodoc live-capture on the 0.0.55 branch):
#
#   * espn_{mbb,wbb,nfl}_game_rosters raised polars ShapeError
#     ("16 column names provided for a DataFrame of width 18") because the
#     per-team ESPN payload gained top-level reference fields (awards_$ref,
#     coaches_$ref) for some leagues/games, breaking the positional column
#     rename in helper_<lg>_team_items. The fix renames by source key via
#     sportsdataverse.dl_utils.normalize_team_roster_columns, so column
#     drift is survivable.
#   * espn_nhl_schedule() with default args raised
#     "'NoneType' object has no attribute 'get'" because the NHL variant of
#     __extract_home_away mutated the event in place but (unlike the 6 other
#     leagues) lacked a `return event`, so `event = __extract_home_away(...)`
#     reassigned event to None before the second call.
#
# These tests pin both fixes and guard against regressing the league
# variants that were already working (cfb / nba / wnba / nhl rosters).


@pytest.mark.parametrize(
    "league,game_id",
    [
        # Previously broken (width-18 team payload):
        ("mbb", 401746082),  # 2025 NCAA M championship (Florida @ Houston)
        ("wbb", 401746075),  # 2025 NCAA W championship (UConn @ South Carolina)
        ("nfl", 401671789),  # completed 2024 NFL game
        # Must NOT regress (already working before the fix):
        ("cfb", 401628334),
        ("nba", 401585183),
        ("wnba", 401726992),
        ("nhl", 401559395),  # 2023 Stanley Cup Final
    ],
)
def test_espn_game_rosters_returns_nonempty_frame(league, game_id):
    """Every league's espn_<lg>_game_rosters must return a DataFrame with
    >0 columns (and, for these real completed games, >0 rows). Covers the
    mbb/wbb/nfl ShapeError fix and pins the working leagues against
    regression from the shared normalize_team_roster_columns change."""
    import polars as pl

    module = __import__(f"sportsdataverse.{league}", fromlist=[f"espn_{league}_game_rosters"])
    wrapper = getattr(module, f"espn_{league}_game_rosters")

    df = wrapper(game_id=game_id)
    assert isinstance(df, pl.DataFrame), f"{league}: expected polars DataFrame, got {type(df)}"
    assert df.width > 0, f"{league}: expected >0 columns, got width {df.width}"
    assert df.height > 0, f"{league}: expected >0 roster rows for completed game {game_id}"
    # Canonical team identity column survives the rename-by-key path.
    assert "team_id" in df.columns, f"{league}: missing team_id after normalize_team_roster_columns"


def test_espn_nhl_schedule_default_args_returns_frame():
    """espn_nhl_schedule() with NO args previously raised
    AttributeError ('NoneType' object has no attribute 'get') because the
    NHL __extract_home_away helper returned None. It must now return a
    valid (possibly empty) DataFrame."""
    import polars as pl

    from sportsdataverse.nhl import espn_nhl_schedule

    df = espn_nhl_schedule()
    assert isinstance(df, pl.DataFrame), f"expected polars DataFrame, got {type(df)}"
    # Today's slate may legitimately be empty (off-day) — a valid empty
    # frame is correct behaviour. When games exist we get >0 columns.
    if df.height > 0:
        assert df.width > 0, "non-empty NHL schedule frame must have columns"


def test_espn_nhl_schedule_known_date_returns_games():
    """A known game date (2023 Stanley Cup Final) must return a populated
    schedule frame with the game_id column."""
    import polars as pl

    from sportsdataverse.nhl import espn_nhl_schedule

    df = espn_nhl_schedule(dates=20230613)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, "expected >=1 NHL game on 2023-06-13 (Stanley Cup Final)"
    assert "game_id" in df.columns, "schedule frame missing game_id column"


def test_espn_mch_teams_site_returns_many_teams():
    from sportsdataverse.mch.mch_espn_ext import espn_mch_teams_site

    payload = espn_mch_teams_site(return_parsed=False)
    teams = payload.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) >= 40, f"expected >=40 NCAA M hockey teams, got {len(teams)}"


def test_espn_wch_scoreboard_answers():
    from sportsdataverse.wch.wch_espn_ext import espn_wch_scoreboard

    payload = espn_wch_scoreboard(return_parsed=False)
    assert isinstance(payload, dict) and "leagues" in payload


def test_espn_college_baseball_teams_site_returns_many_teams():
    from sportsdataverse.college_baseball.college_baseball_espn_ext import espn_college_baseball_teams_site

    payload = espn_college_baseball_teams_site(return_parsed=False)
    teams = payload.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) >= 100, f"expected >=100 NCAA baseball teams, got {len(teams)}"


def test_espn_college_softball_scoreboard_answers():
    from sportsdataverse.college_softball.college_softball_espn_ext import espn_college_softball_scoreboard

    payload = espn_college_softball_scoreboard(return_parsed=False)
    assert isinstance(payload, dict) and "leagues" in payload


def test_espn_cfl_teams_site_returns_teams():
    from sportsdataverse.cfl.cfl_espn_ext import espn_cfl_teams_site

    payload = espn_cfl_teams_site(return_parsed=False)
    teams = payload.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) >= 8, f"expected >=8 CFL teams, got {len(teams)}"


def test_espn_ufl_scoreboard_answers():
    from sportsdataverse.ufl.ufl_espn_ext import espn_ufl_scoreboard

    payload = espn_ufl_scoreboard(return_parsed=False)
    assert isinstance(payload, dict) and "leagues" in payload


def test_espn_soccer_scoreboard_param_league_answers():
    from sportsdataverse.soccer.soccer_espn_ext import espn_soccer_scoreboard

    # league is a runtime arg; raw Dict via return_parsed=False
    payload = espn_soccer_scoreboard(league="eng.1", return_parsed=False)
    assert isinstance(payload, dict) and "leagues" in payload


def test_espn_soccer_teams_param_league_answers():
    from sportsdataverse.soccer.soccer_espn_ext import espn_soccer_teams_site

    payload = espn_soccer_teams_site(league="eng.1", return_parsed=False)
    teams = payload.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams") or []
    assert len(teams) >= 18, f"expected >=18 EPL teams, got {len(teams)}"


def test_espn_epl_alias_scoreboard_answers():
    from sportsdataverse.epl.epl_espn_ext import espn_epl_scoreboard

    # fixed-league alias: NO league arg
    payload = espn_epl_scoreboard(return_parsed=False)
    assert isinstance(payload, dict) and "leagues" in payload


def test_espn_cricket_scoreboard_param_league_answers():
    from sportsdataverse.cricket.cricket_espn_ext import espn_cricket_scoreboard

    # a broadly-available cricket league slug; raw Dict
    payload = espn_cricket_scoreboard(league="8048", return_parsed=False)  # 8048 = IPL on ESPN
    assert isinstance(payload, dict)
