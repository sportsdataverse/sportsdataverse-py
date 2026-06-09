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
