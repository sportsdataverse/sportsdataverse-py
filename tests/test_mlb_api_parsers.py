"""Offline parser tests for the MLB Stats API parsers in
``sportsdataverse.mlb.mlb_api_parsers``.

Captured fixtures live in ``tests/fixtures/mlb_api/``. See that
directory's README.md for provenance.

Live integration tests for the wrappers themselves live in
``test_espn_live.py`` (e.g. ``test_mlb_api_teams_returns_30_teams_for_2024``);
this module covers the parser logic offline.
"""

from __future__ import annotations

import polars as pl
import pytest

from tests.conftest import load_fixture


def _load(stem: str) -> dict:
    """Local alias for the shared :func:`tests.conftest.load_fixture`
    bound to this module's fixture category ("mlb_api")."""
    return load_fixture("mlb_api", stem)


# ===========================================================================
# Dedicated parsers
# ===========================================================================


def test_parse_mlb_api_schedule_returns_one_row_per_game():
    from sportsdataverse.mlb import parse_mlb_api_schedule

    df = parse_mlb_api_schedule(_load("schedule_2024_09_29"))
    assert isinstance(df, pl.DataFrame)
    # Sept 29, 2024 was a full slate (final regular season day, MLB caps
    # at 15 games per day).
    assert df.height >= 15, f"expected >=15 games, got {df.height}"
    for col in ("schedule_date", "game_pk", "game_type", "game_date"):
        assert col in df.columns, f"missing column {col!r}"


def test_parse_mlb_api_teams_returns_30_active_teams():
    from sportsdataverse.mlb import parse_mlb_api_teams

    df = parse_mlb_api_teams(_load("teams_2024"))
    assert df.height >= 30, f"expected >=30 MLB teams, got {df.height}"
    for col in ("id", "name", "abbreviation", "team_code"):
        assert col in df.columns


def test_parse_mlb_api_team_roster_returns_one_row_per_player():
    from sportsdataverse.mlb import parse_mlb_api_team_roster

    df = parse_mlb_api_team_roster(_load("team_roster_yankees_2024"))
    # 40-man roster + injured / minor reserves can push past 50
    assert df.height >= 40, f"expected >=40 players, got {df.height}"
    for col in ("jersey_number", "person_id", "person_full_name",
                "position_code"):
        assert col in df.columns, f"missing column {col!r}"


def test_parse_mlb_api_standings_unrolls_divisions_to_teams():
    """6 divisions × 5 teams each = 30 rows. The parser must prefix the
    division identifiers (namespaced as ``standings_*``) onto each team
    row so a single output row carries both context."""
    from sportsdataverse.mlb import parse_mlb_api_standings

    df = parse_mlb_api_standings(_load("standings_2024"))
    assert df.height >= 30, f"expected >=30 team-standings rows, got {df.height}"
    # Division context columns (namespaced)
    for col in ("standings_division_id", "standings_division_name",
                "standings_league_id"):
        assert col in df.columns, f"missing division context column {col!r}"
    # Team record columns from the unrolled teamRecords[]
    for col in ("team_id", "games_played", "league_rank"):
        assert col in df.columns, f"missing team-record column {col!r}"


def test_parse_mlb_api_person_stats_unrolls_splits():
    """``stats[].splits[]`` becomes one row per (stat-block × split).
    Aaron Judge's 2024 season has 1 split (the season summary) with a
    wide ``stat`` block (~30 columns of hitting metrics)."""
    from sportsdataverse.mlb import parse_mlb_api_person_stats

    df = parse_mlb_api_person_stats(_load("person_stats_judge_2024"))
    assert df.height >= 1, f"expected >=1 split row, got {df.height}"
    assert "stats_type" in df.columns
    assert "stats_group" in df.columns
    # Stat columns from the inner ``stat`` block (flattened with stat_*)
    assert any(c.startswith("stat_") for c in df.columns), (
        f"no stat_* columns: {df.columns[:10]}"
    )


# ===========================================================================
# Generic parse_mlb_api_list — covers venues / sports / divisions / etc.
# ===========================================================================


@pytest.mark.parametrize("fixture,expected_min", [
    ("venues_active",  20),    # 1,646 venues in the capture
    ("sports",          5),    # 20 sports
    ("divisions",       6),    # 61 divisions across all sport IDs
])
def test_parse_mlb_api_list_works_on_known_list_endpoints(fixture, expected_min):
    from sportsdataverse.mlb import parse_mlb_api_list

    df = parse_mlb_api_list(_load(fixture))
    assert df.height >= expected_min, (
        f"{fixture}: expected >= {expected_min} rows, got {df.height}"
    )


# ===========================================================================
# Empty-payload contract
# ===========================================================================


@pytest.mark.parametrize("parser_name", [
    "parse_mlb_api_schedule",
    "parse_mlb_api_teams",
    "parse_mlb_api_team_roster",
    "parse_mlb_api_standings",
    "parse_mlb_api_person_stats",
    "parse_mlb_api_list",
])
def test_parser_handles_empty_payload(parser_name):
    from sportsdataverse.mlb import mlb_api_parsers

    parser = getattr(mlb_api_parsers, parser_name)
    df = parser({})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


# ===========================================================================
# pandas opt-in
# ===========================================================================


def test_parse_mlb_api_standings_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse.mlb import parse_mlb_api_standings

    df = parse_mlb_api_standings(_load("standings_2024"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 30


# ===========================================================================
# Registry contract
# ===========================================================================


def test_parser_for_mlb_api_always_returns_callable():
    """parser_for_mlb_api should never return None — unknown names
    fall back to parse_mlb_api_list."""
    from sportsdataverse.mlb import (
        parse_mlb_api_list,
        parse_mlb_api_schedule,
        parser_for_mlb_api,
    )

    assert parser_for_mlb_api("mlb_api_schedule") is parse_mlb_api_schedule
    assert parser_for_mlb_api("mlb_api_nope") is parse_mlb_api_list


def test_mlb_api_endpoint_parsers_registry_references_real_wrappers():
    """Every key in MLB_API_ENDPOINT_PARSERS must correspond to an actual function
    in the mlb PACKAGE -- post-cutover the regular wrappers are generated in mlb_api
    and the irregular ones live in mlb_api_extra, both imported into
    sportsdataverse.mlb."""
    import sportsdataverse.mlb as mlb

    from sportsdataverse.mlb import MLB_API_ENDPOINT_PARSERS

    for fn_name in MLB_API_ENDPOINT_PARSERS:
        assert hasattr(mlb, fn_name), (
            f"MLB_API_ENDPOINT_PARSERS references missing wrapper {fn_name!r}"
        )
        assert callable(getattr(mlb, fn_name))
