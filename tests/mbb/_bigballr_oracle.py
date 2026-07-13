"""Shared helpers for the bigballR/wbigballR parity suites.

Central authority for (1) loading the R-oracle CSVs under
``tests/fixtures/ncaa/bigballr/oracle/{mbb,wbb}/`` and (2) the canonical
column mapping from bigballR's R contract to the sdv-py snake_case contract.
Every ``test_*_ncaa_bigballr*`` parity test loads oracles through this module
so the contract can never diverge between test files.

R ``write.csv`` emits the literal string ``NA`` for missing cells — always
read with ``null_values=["NA"]``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa" / "bigballr"
HTML_DIR = FIXTURES / "html"

#: bigballR play-by-play contract (spec_bigballr.md §1 OUTPUT CONTRACT) →
#: sdv-py snake_case contract. ``Half_Status`` becomes ``period`` (1..n_reg
#: regulation periods, then OTs) — "half" is wrong for WBB quarters.
PBP_RENAME: dict[str, str] = {
    "ID": "game_id",
    "Date": "game_date",
    "Home": "home",
    "Away": "away",
    "Half_Status": "period",
    "Time": "clock",
    "Game_Time": "game_time",
    "Game_Seconds": "game_seconds",
    "Home_Score": "home_score",
    "Away_Score": "away_score",
    "Event_Team": "event_team",
    "Event_Description": "event_description",
    "Player_1": "player_1",
    "Player_2": "player_2",
    "Event_Type": "event_type",
    "Event_Result": "event_result",
    "Shot_Value": "shot_value",
    "Event_Length": "event_length",
    "Poss_Num": "poss_num",
    "Poss_Team": "poss_team",
    "Poss_Length": "poss_length",
    "isTransition": "is_transition",
    "Home.1": "home_1",
    "Home.2": "home_2",
    "Home.3": "home_3",
    "Home.4": "home_4",
    "Home.5": "home_5",
    "Away.1": "away_1",
    "Away.2": "away_2",
    "Away.3": "away_3",
    "Away.4": "away_4",
    "Away.5": "away_5",
    "Status": "status",
    "isGarbageTime": "is_garbage_time",
    "Sub_Deviate": "sub_deviate",
}

#: Target dtypes for the pbp contract. game_id stays Utf8 (opaque contest id;
#: never arithmetic). Team-crosswalk ids elsewhere are Int64 per repo ID rules.
PBP_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "game_date": pl.Utf8,
    "home": pl.Utf8,
    "away": pl.Utf8,
    "period": pl.Int64,
    "clock": pl.Utf8,
    "game_time": pl.Utf8,
    "game_seconds": pl.Int64,
    "home_score": pl.Int64,
    "away_score": pl.Int64,
    "event_team": pl.Utf8,
    "event_description": pl.Utf8,
    "player_1": pl.Utf8,
    "player_2": pl.Utf8,
    "event_type": pl.Utf8,
    "event_result": pl.Utf8,
    "shot_value": pl.Int64,
    "event_length": pl.Int64,
    "poss_num": pl.Int64,
    "poss_team": pl.Utf8,
    "poss_length": pl.Int64,
    "is_transition": pl.Boolean,
    "home_1": pl.Utf8,
    "home_2": pl.Utf8,
    "home_3": pl.Utf8,
    "home_4": pl.Utf8,
    "home_5": pl.Utf8,
    "away_1": pl.Utf8,
    "away_2": pl.Utf8,
    "away_3": pl.Utf8,
    "away_4": pl.Utf8,
    "away_5": pl.Utf8,
    "status": pl.Utf8,
    "is_garbage_time": pl.Boolean,
    "sub_deviate": pl.Int64,
}

#: Fixture game ids per league (tests/fixtures/ncaa/bigballr/README.md).
GAMES = {
    "mbb": ["6470186", "6479639", "6479592", "1613299"],
    "wbb": ["5722355", "5732292", "5728709", "5733807"],
}

#: WBB time-derived columns where the R oracle is WRONG-by-construction
#: (wbigballR applies MBB halves math to quarter-format pages — a regulation
#: WBB game parses as 2 OT). Parity tests skip these for wbb and validate
#: them with invariants instead; see oracle/wbb/README.md.
WBB_CLOCK_TAINTED = [
    "period",
    "game_time",
    "game_seconds",
    "event_length",
    "poss_length",
    "is_transition",
    "is_garbage_time",
    "poss_num",
    "poss_team",
    "status",
    "sub_deviate",
    "home_1",
    "home_2",
    "home_3",
    "home_4",
    "home_5",
    "away_1",
    "away_2",
    "away_3",
    "away_4",
    "away_5",
]


def oracle_dir(league: str) -> Path:
    return FIXTURES / "oracle" / league


def load_oracle(name: str, league: str = "mbb") -> pl.DataFrame:
    """Read one R-oracle CSV verbatim (R column names, NA -> null)."""
    return pl.read_csv(
        oracle_dir(league) / f"{name}.csv",
        null_values=["NA"],
        infer_schema_length=None,
    )


def load_oracle_pbp(league: str = "mbb") -> pl.DataFrame:
    """Oracle play_by_play renamed + cast to the sdv-py pbp contract.

    This is BOTH the expected frame for the game-pbp parity test and the
    canonical INPUT for the pure-transform parity tests (lineups, stats,
    possessions, ...): the R transforms consumed exactly these rows, so
    feeding the renamed frame to the Python transforms isolates transform
    logic from scrape logic.
    """
    df = load_oracle("play_by_play", league)
    df = df.rename({k: v for k, v in PBP_RENAME.items() if k in df.columns})
    casts = [pl.col(c).cast(dtype, strict=False) for c, dtype in PBP_SCHEMA.items() if c in df.columns]
    return df.with_columns(casts)
