from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_oracle_data import (
    EPM_ORACLE_SCHEMA,
    RAPM_ORACLE_SCHEMA,
    load_epm,
    load_rapm_ryan_davis,
    normalize_player_name,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "nba_oracle"


def test_normalize_lowercases_and_strips_punctuation():
    assert normalize_player_name("A.J. Green") == "aj green"


def test_normalize_strips_diacritics():
    # real stats.nba.com spells this "Nikola Jokić" (Serbian ć); the DARKO CSV
    # spells it "Nikola Jokic" (plain ASCII) -- both must fold to the same key.
    assert normalize_player_name("Nikola Jokić") == normalize_player_name("Nikola Jokic")
    assert normalize_player_name("Nikola Jokić") == "nikola jokic"


def test_normalize_strips_suffix():
    assert normalize_player_name("Gary Trent Jr.") == normalize_player_name("Gary Trent")
    assert normalize_player_name("Gary Trent Jr.") == "gary trent"


def test_normalize_collapses_whitespace():
    assert normalize_player_name("  Kevin   Durant  ") == "kevin durant"


def test_normalize_empty_string():
    assert normalize_player_name("") == ""


def test_load_rapm_ryan_davis_schema_and_values():
    df = load_rapm_ryan_davis(str(FIXTURES / "rapm_ryan_davis_sample.csv"))
    assert dict(df.schema) == RAPM_ORACLE_SCHEMA
    assert df.height == 3
    row = df.filter(pl.col("player_id") == 201939).to_dicts()[0]
    assert row["player_name"] == "Stephen Curry"
    assert row["season"] == "2009-10"
    assert row["RAPM"] == 5.9
    assert row["RA_TOV"] == 1.0


def test_load_rapm_ryan_davis_empty_header_only(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_text("playerId,playerName,LA_RAPM,RAPM,RA_EFG,RA_FTR,RA_ORBD,RA_TOV,season,primaryKey\n")
    df = load_rapm_ryan_davis(str(empty))
    assert df.height == 0
    assert dict(df.schema) == RAPM_ORACLE_SCHEMA


def test_load_epm_schema_and_values():
    df = load_epm(str(FIXTURES / "epm_sample.csv"))
    assert dict(df.schema) == EPM_ORACLE_SCHEMA
    assert df.height == 3
    row = df.filter(pl.col("player_id") == 203999).to_dicts()[0]
    assert row["player_name"] == "Nikola Jokic"
    assert row["season"] == 2025
    assert abs(row["epm"] - 8.082) < 1e-9


def test_load_epm_empty_header_only(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_text("season,nba_id,name,team,oepm,depm,epm\n")
    df = load_epm(str(empty))
    assert df.height == 0
    assert dict(df.schema) == EPM_ORACLE_SCHEMA
