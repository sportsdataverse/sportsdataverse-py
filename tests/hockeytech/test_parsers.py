# tests/hockeytech/test_parsers.py
from __future__ import annotations

import polars as pl

from tests.conftest import load_fixture


def _load(stem):
    return load_fixture("hockeytech", stem)


def test_parse_seasons_columns_and_year_derivation():
    from sportsdataverse.hockeytech._parsers import parse_seasons

    df = parse_seasons(_load("pwhl_seasons"))
    assert isinstance(df, pl.DataFrame)
    for col in ("season_id", "season_name", "season_short", "season_yr", "game_type_label"):
        assert col in df.columns
    # "2024-25 Regular Season" -> end-year 2025, label "regular"
    row = df.filter(pl.col("season_name").str.contains("2024-25 Regular"))
    if row.height:
        assert row["season_yr"][0] == 2025
        assert row["game_type_label"][0] == "regular"


def test_resolve_season_id_end_year_to_integer(monkeypatch):
    from sportsdataverse.hockeytech import _leagues

    monkeypatch.setattr(_leagues, "_fetch_seasons_raw", lambda league: _load("pwhl_seasons"))
    sid = _leagues.resolve_season_id("pwhl", season=2025, game_type="regular")
    assert isinstance(sid, int) and sid > 0


def test_resolve_season_id_passthrough_explicit_id():
    from sportsdataverse.hockeytech import _leagues

    assert _leagues.resolve_season_id("pwhl", season_id=5) == 5


def test_parse_schedule_one_row_per_game_with_core_cols():
    from sportsdataverse.hockeytech._parsers import parse_schedule

    df = parse_schedule(_load("pwhl_schedule_2025"))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    for col in (
        "game_id",
        "game_date",
        "home_team",
        "home_team_id",
        "away_team",
        "away_team_id",
        "home_score",
        "away_score",
    ):
        assert col in df.columns


def test_parse_standings_has_team_rank_and_points():
    from sportsdataverse.hockeytech._parsers import parse_standings

    df = parse_standings(_load("pwhl_standings_5"))
    for col in (
        "team",
        "team_rank",
        "games_played",
        "points",
        "wins",
        "losses",
        "regulation_wins",
        "non_reg_wins",
        "non_reg_losses",
    ):
        assert col in df.columns

    import polars as pl

    df2 = df.with_columns(
        [
            pl.col("wins").cast(pl.Int64, strict=False),
            pl.col("regulation_wins").cast(pl.Int64, strict=False),
            pl.col("non_reg_wins").cast(pl.Int64, strict=False),
        ]
    )
    # total wins must be >= regulation wins for every team
    assert (df2["wins"] >= df2["regulation_wins"]).all()
    # total wins == regulation + non-regulation
    assert (df2["wins"] == df2["regulation_wins"] + df2["non_reg_wins"]).all()


def test_parse_teams_and_roster():
    from sportsdataverse.hockeytech._parsers import parse_teams, parse_roster

    teams = parse_teams(_load("pwhl_teams_5"))
    assert "team_name" in teams.columns and "team_id" in teams.columns
    roster = parse_roster(_load("pwhl_roster_1_5"))
    assert roster.height > 0
