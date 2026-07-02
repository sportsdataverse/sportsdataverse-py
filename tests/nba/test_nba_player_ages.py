"""Tests for nba_player_ages (bulk per-player-season AGE helper)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_player_ages import nba_player_ages


def test_nba_player_ages_parses_biostats():
    def fake(**kw):
        return pl.DataFrame({"player_id": [1, 2], "player_name": ["A", "B"], "age": [25.0, 31.0]})

    df = nba_player_ages("2023-24", fetch=fake)
    assert df.columns == ["player_id", "age"]
    assert df["player_id"].to_list() == [1, 2] and df["age"].to_list() == [25.0, 31.0]
    assert df.schema["player_id"] == pl.Int64 and df.schema["age"] == pl.Float64
