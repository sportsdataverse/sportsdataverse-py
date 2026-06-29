"""Tests for NBA enhanced PBP engine."""

import polars as pl
from sportsdataverse.nba import nba_pbp_constants as C


def test_constants_shape():
    """Verify constants module shape and basic functionality."""
    assert C.ACTION_TYPE_EVENT["Substitution"] == "substitution"
    assert C.ACTION_TYPE_EVENT["Made Shot"] == "made_shot"
    assert "is_substitution" in C.EVENT_FLAG_COLUMNS
    assert C.ENHANCED_PBP_SCHEMA["game_id"] == pl.Utf8
    assert C.ENHANCED_PBP_SCHEMA["person_id"] == pl.Int64
    assert C.LINEUPS_SCHEMA["home_player_1"] == pl.Int64
    df = pl.DataFrame({"clock": ["PT08M24.00S", "PT12M00.00S"]})
    secs = df.select(C.iso_clock_to_seconds(pl.col("clock")).alias("s"))["s"].to_list()
    assert abs(secs[0] - 504.0) < 1e-6 and abs(secs[1] - 720.0) < 1e-6
