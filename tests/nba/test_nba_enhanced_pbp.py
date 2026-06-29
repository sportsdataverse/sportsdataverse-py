"""Tests for NBA enhanced PBP engine."""

import json
import pathlib

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


def _payload() -> dict:
    """Load fixture payload."""
    fx = pathlib.Path("tests/fixtures/nba_engine/0022200001")
    return json.loads((fx / "playbyplayv3.json").read_text())


def test_ingest_normalizes_v3():
    """Test enhanced_pbp_from_payload ingests and normalizes v3 actions."""
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload

    df = enhanced_pbp_from_payload(_payload())
    assert df.schema["game_id"] == pl.Utf8 and df.schema["person_id"] == pl.Int64
    assert df["game_id"][0] == "0022200001"
    row = df.filter(pl.col("clock") == "PT08M24.00S").head(1)
    assert abs(row["seconds_remaining"][0] - 504.0) < 1e-6
    assert df.height == 468
