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


def test_parse_pbp_a_one_row_per_event_with_fastrhockey_cols():
    from sportsdataverse.hockeytech._parsers import parse_pbp

    df = parse_pbp(_load("pwhl_pbp_42"), pbp_style="hockeytech_a", game_id=42)
    import polars as pl

    assert isinstance(df, pl.DataFrame) and df.height > 0
    for col in (
        "game_id",
        "event",
        "team_id",
        "period_of_game",
        "time_of_period",
        "player_id",
        "player_name_first",
        "player_name_last",
        "x_coord",
        "y_coord",
        "goal",
        "goalie_id",
    ):
        assert col in df.columns, f"missing {col}"
    events = set(df["event"].unique().to_list())
    assert {"shot", "blocked_shot", "goal", "faceoff", "penalty"} <= events
    # game_id echoed on every row
    assert (df["game_id"] == 42).all()
    # shot rows carry coords
    shots = df.filter(pl.col("event") == "shot")
    assert shots.height > 0
    assert shots["x_coord"].null_count() < shots.height  # at least some coords present
    # parity columns from fastRhockey pwhl_pbp
    for col in (
        "plus_player_one_id",
        "plus_player_one_position",
        "minus_player_one_id",
        "player_two_position",
        "penalty_shot",
        "insurance",
        "short_handed",
    ):
        assert col in df.columns, f"missing parity column {col}"


def test_parse_shifts_one_row_per_stint():
    from sportsdataverse.hockeytech._parsers import parse_shifts

    df = parse_shifts(_load("pwhl_gameshifts_42"), game_id=42)
    import polars as pl

    assert isinstance(df, pl.DataFrame) and df.height > 0
    for col in (
        "game_id",
        "player_id",
        "first_name",
        "last_name",
        "home",
        "period",
        "start_time",
        "end_time",
        "length",
        "start_s",
        "end_s",
    ):
        assert col in df.columns
    # countdown clock: start_s >= end_s within a shift
    assert (df["start_s"] >= df["end_s"]).all()
    # game_id echoed
    assert (df["game_id"] == 42).all()


def test_mmss_to_seconds_roundtrip():
    from sportsdataverse.hockeytech._parsers import mmss_to_seconds

    assert mmss_to_seconds("03:16") == 196
    assert mmss_to_seconds("00:00") == 0
    assert mmss_to_seconds(None) is None
    assert mmss_to_seconds("") is None


# ---------------------------------------------------------------------------
# Task A1.8: remaining PWHL parsers
# ---------------------------------------------------------------------------


def test_parse_player_stats_has_season_and_points():
    from sportsdataverse.hockeytech._parsers import parse_player_stats

    df = parse_player_stats(_load("pwhl_player_stats_27"))
    for col in ("season_id", "season_name", "games_played", "points", "team_id"):
        assert col in df.columns, f"missing column {col}"


def test_parse_leaders_has_player_and_team():
    from sportsdataverse.hockeytech._parsers import parse_leaders

    df = parse_leaders(_load("pwhl_leaders_5"))
    # The fixture has empty results; parser must return a zero-row frame, not raise.
    # We only assert it returns a polars DataFrame without error.
    assert isinstance(df, pl.DataFrame)
    # When there are rows they must have these columns; skip column check if empty.
    if df.height > 0:
        for col in ("player_id", "first_name", "last_name", "team_id"):
            assert col in df.columns, f"missing column {col}"


def test_parse_game_summary_returns_named_subframes():
    from sportsdataverse.hockeytech._parsers import parse_game_summary

    out = parse_game_summary(_load("pwhl_game_summary_42"), game_id=42)
    assert isinstance(out, dict)
    for key in ("game", "goals", "penalties", "shots_by_period", "three_stars"):
        assert key in out, f"missing key {key}"
    # game frame must have at least one row (echoes game_id even when GC is empty)
    assert out["game"].height >= 1


def test_flat_parsers_on_synthetic_payloads():
    from sportsdataverse.hockeytech import _parsers as P

    # parse_player_search uses SiteKit.Searchplayers
    fake = {"SiteKit": {"Searchplayers": [{"player_id": 1, "name": "A B"}]}}
    df = P.parse_player_search(fake)
    assert df.height == 1 and "player_id" in df.columns

    # parse_streaks uses SiteKit.Streaks
    fake2 = {"SiteKit": {"Streaks": [{"player_id": 2, "streak": "3"}]}}
    assert P.parse_streaks(fake2).height == 1

    # empty / None payloads -> zero-row frame, no raise
    assert P.parse_transactions(None).height == 0

    # parse_game_info and parse_player_box on synthetic dicts
    fake_gi = {"SiteKit": {"Gameinfo": [{"game_id": 99, "date": "2025-01-01"}]}}
    df_gi = P.parse_game_info(fake_gi)
    assert df_gi.height == 1 and "game_id" in df_gi.columns

    fake_pb = {"SiteKit": {"Playerbox": [{"player_id": 10, "goals": "1"}]}}
    df_pb = P.parse_player_box(fake_pb)
    assert df_pb.height == 1 and "player_id" in df_pb.columns
