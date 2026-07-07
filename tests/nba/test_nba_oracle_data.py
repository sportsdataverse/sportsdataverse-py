from __future__ import annotations

import datetime
import glob
import os
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba.nba_oracle_data import (
    DARKO_DPM_ORACLE_SCHEMA,
    DT_STATS_ORACLE_SCHEMA,
    EPM_ORACLE_SCHEMA,
    LEBRON_DAILY_ORACLE_SCHEMA,
    LEBRON_SEASON_ORACLE_SCHEMA,
    RAPM_ORACLE_SCHEMA,
    load_darko_dpm,
    load_dunks_threes_stats,
    load_epm,
    load_lebron_daily,
    load_lebron_season,
    load_rapm_ryan_davis,
    normalize_player_name,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "nba_oracle"

_ORACLE_DIR = os.environ.get("SDV_PY_NBA_ORACLE_DIR")
_has_oracle_dir = bool(_ORACLE_DIR and os.path.isdir(_ORACLE_DIR))
skip_if_no_oracle_dir = pytest.mark.skipif(
    not _has_oracle_dir, reason="set SDV_PY_NBA_ORACLE_DIR to the real oracle-CSV directory to run"
)


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
    assert row["RAPM"] == -0.01
    assert row["RA_TOV"] == 0.31


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


def test_load_lebron_season_schema_and_values():
    df = load_lebron_season(str(FIXTURES / "lebron_season_sample.csv"))
    assert dict(df.schema) == LEBRON_SEASON_ORACLE_SCHEMA
    assert df.height == 3
    row = df.filter(pl.col("player_id") == 2544).to_dicts()[0]
    assert row["player_name"] == "Lebron James"
    assert row["seasons"] == "2026"
    assert row["war"] == 6.154


def test_load_lebron_daily_schema_and_values():
    df = load_lebron_daily(str(FIXTURES / "lebron_daily_sample.csv"))
    assert dict(df.schema) == LEBRON_DAILY_ORACLE_SCHEMA
    assert df.height == 2
    row = df.filter(pl.col("player_id") == 1641705).to_dicts()[0]
    assert row["through_date"] == datetime.date(2026, 4, 12)
    assert row["player_name"] == "Victor Wembanyama"
    assert abs(row["war"] - 11.5398803) < 1e-6


def test_load_lebron_season_empty_header_only(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_text("nba_id,Player,Seasons,Team,LEBRON,O-LEBRON,D-LEBRON,WAR\n")
    df = load_lebron_season(str(empty))
    assert df.height == 0
    assert dict(df.schema) == LEBRON_SEASON_ORACLE_SCHEMA


def test_load_lebron_daily_empty_header_only(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_text("ThroughDate,PLAYER_ID,Name,Season,Mins,LEBRON,OLEBRON,DLEBRON,LEBRON WAR\n")
    df = load_lebron_daily(str(empty))
    assert df.height == 0
    assert dict(df.schema) == LEBRON_DAILY_ORACLE_SCHEMA


def test_load_darko_dpm_schema_and_values():
    df = load_darko_dpm(str(FIXTURES / "darko_dpm_sample.csv"))
    assert dict(df.schema) == DARKO_DPM_ORACLE_SCHEMA
    assert df.height == 4
    row = df.filter(pl.col("player_name") == "Nikola Jokic").to_dicts()[0]
    assert row["team"] == "Denver Nuggets"
    assert row["dpm"] == 7
    assert row["odpm"] == 5
    assert row["ddpm"] == 2


def test_load_darko_dpm_empty_header_only(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_text("﻿#,Player,Team,DPM,ODPM,DDPM\n", encoding="utf-8")
    df = load_darko_dpm(str(empty))
    assert df.height == 0
    assert dict(df.schema) == DARKO_DPM_ORACLE_SCHEMA


def test_load_dunks_threes_stats_schema_and_values():
    df = load_dunks_threes_stats(str(FIXTURES / "dunks_threes_sample.csv"))
    assert dict(df.schema) == DT_STATS_ORACLE_SCHEMA
    assert df.height == 3
    row = df.filter(pl.col("player_id") == 1628983).to_dicts()[0]
    assert row["player_name"] == "Shai Gilgeous-Alexander"
    assert row["team_alias"] == "OKC"
    assert abs(row["ewins"] - 20.9422) < 1e-9


def test_load_dunks_threes_stats_empty_header_only(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_text("season,player_id,player_name,team_alias,ewins\n")
    df = load_dunks_threes_stats(str(empty))
    assert df.height == 0
    assert dict(df.schema) == DT_STATS_ORACLE_SCHEMA


# ---------------------------------------------------------------------------
# WP3 Task 9: gated real-CSV smoke tests (SDV_PY_NBA_ORACLE_DIR)
# ---------------------------------------------------------------------------


@skip_if_no_oracle_dir
def test_real_rapm_ryan_davis_parses():
    df = load_rapm_ryan_davis(os.path.join(_ORACLE_DIR, "rapm_ryan_davis.csv"))
    assert df.height > 1000
    assert df.select(pl.col("RAPM").is_between(-15, 15).all()).item()


@skip_if_no_oracle_dir
def test_real_rapm_multi_ryan_davis_parses():
    df = load_rapm_ryan_davis(os.path.join(_ORACLE_DIR, "rapm_multi_ryan_davis.csv"))
    assert df.height > 1000
    assert df.select(pl.col("RAPM").is_between(-15, 15).all()).item()


@skip_if_no_oracle_dir
def test_real_epm_parses():
    for fname in glob.glob(os.path.join(_ORACLE_DIR, "*_EPM_data.csv")):
        df = load_epm(fname)
        assert df.height > 50
        assert df.select(pl.col("epm").is_between(-15, 15).all()).item()


@skip_if_no_oracle_dir
def test_real_lebron_season_parses():
    files = glob.glob(os.path.join(_ORACLE_DIR, "lebron-data-[0-9][0-9][0-9][0-9].csv"))
    if not files:
        pytest.skip("no per-season lebron-data-YYYY.csv file present")
    df = load_lebron_season(sorted(files)[-1])
    assert df.height > 50


@skip_if_no_oracle_dir
def test_real_lebron_daily_parses():
    files = glob.glob(os.path.join(_ORACLE_DIR, "lebron_daily_*.csv"))
    if not files:
        pytest.skip("no lebron_daily_*.csv snapshot present")
    df = load_lebron_daily(sorted(files)[-1])
    assert df.height > 50
    assert df.select(pl.col("through_date").is_not_null().all()).item()


@skip_if_no_oracle_dir
def test_real_darko_dpm_parses():
    files = glob.glob(os.path.join(_ORACLE_DIR, "*-darko-dpm-leaderboard.csv"))
    if not files:
        pytest.skip("no *-darko-dpm-leaderboard.csv present")
    df = load_darko_dpm(sorted(files)[-1])
    assert df.height > 50
    assert df.select(pl.col("dpm").is_between(-15, 15).all()).item()


@skip_if_no_oracle_dir
def test_real_dunks_threes_stats_parses():
    files = glob.glob(os.path.join(_ORACLE_DIR, "*_Dunks_&_Threes_Stats.csv"))
    if not files:
        pytest.skip("no *_Dunks_&_Threes_Stats.csv present")
    df = load_dunks_threes_stats(sorted(files)[-1])
    assert df.height > 50
    assert df.select(pl.col("ewins").is_between(-5, 40).all()).item()
