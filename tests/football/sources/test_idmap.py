"""Id map: NFL ESPN <-> Shield <-> nflverse leg from nfl-raw's crosswalk, Yahoo ids computed, parquet round trip."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.sources.idmap import (
    GAME_SCHEMA,
    TEAM_SCHEMA,
    _build_nfl_idmap,
    _load_idmap,
    _lookup,
    _write_idmap,
    _yahoo_nfl_game_id,
)

FIX = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def idmap():
    return _build_nfl_idmap(FIX / "nfl_espn_crosswalk_games_2026_wk1.json", FIX / "nfl_espn_crosswalk_teams.json")


def test_schema_and_shape(idmap):
    games, teams = idmap
    assert dict(games.schema) == GAME_SCHEMA and dict(teams.schema) == TEAM_SCHEMA
    assert games.height == 17 and teams.height == 37  # 16 week-1 games + Super Bowl LX; 32 clubs + relocations
    assert games["espn_event_id"].n_unique() == games.height
    assert games["cbs_game_id"].null_count() == games.height  # builder not landed yet: stays null, never fabricated
    assert games["shield_game_id"].null_count() == 0 and games["nflverse_game_id"].null_count() == 0


def test_lookup_cle_at_jax(idmap):
    games, teams = idmap
    row = _lookup(games, 401872922, teams)
    assert (
        row["shield_game_id"] == "a8fc1728-4feb-11f1-abca-2c54536568a9"
    )  # == nfl-raw/nfl/raw/2026/2026_01_CLE_JAX.json id
    assert row["nflverse_game_id"] == "2026_01_CLE_JAX"
    assert row["yahoo_game_id"] == "nfl.g.20260913030"
    assert row["home_espn_team_id"] == "30" and row["home_team"]["espn_abbr"] == "JAX"
    assert row["home_team"]["shield_team_id"] == "10402250-89fe-7b86-ef98-9062cd354256"
    assert row["home_team"]["yahoo_team_id"] == "nfl.t.30" and row["away_team"]["yahoo_team_id"] == "nfl.t.5"
    assert _lookup(games, 1) is None


@pytest.mark.parametrize(
    ("kickoff_utc", "home", "expected"),
    [
        ("2026-09-13T17:00Z", "30", "nfl.g.20260913030"),  # Sunday early window
        ("2026-09-10T00:20Z", "26", "nfl.g.20260909026"),  # Wednesday opener: UTC date rolls back in ET
        ("2026-09-15T00:15Z", 12, "nfl.g.20260914012"),  # MNF
        ("2026-02-08T23:30Z", "17", "nfl.g.20260208017"),  # Super Bowl LX, neutral site keeps the ESPN home id
    ],
)
def test_yahoo_nfl_game_id(kickoff_utc, home, expected):
    assert _yahoo_nfl_game_id(kickoff_utc, home) == expected


def test_yahoo_ids_match_the_verified_week(idmap):
    games, _ = idmap
    wk1 = games.filter(pl.col("week") == 1)
    assert wk1.filter(pl.col("espn_event_id") == "401872656")["yahoo_game_id"][0] == "nfl.g.20260909026"
    assert wk1.filter(pl.col("espn_event_id") == "401872931")["yahoo_game_id"][0] == "nfl.g.20260914012"


def test_parquet_round_trip_and_schema_drift(idmap, tmp_path):
    games, teams = idmap
    d = _write_idmap(games, teams, tmp_path / "idmap")
    g2, t2 = _load_idmap(d)
    assert g2.equals(games) and t2.equals(teams)
    games.with_columns(pl.col("week").cast(pl.Int64)).write_parquet(d / "games.parquet")
    with pytest.raises(ValueError, match="schema drift"):
        _load_idmap(d)
