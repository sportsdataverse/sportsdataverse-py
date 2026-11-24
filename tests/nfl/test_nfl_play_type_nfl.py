"""``play_type_nfl`` and ``aborted_play`` on both NFL processors, against real nflverse rows.

nflfastR's ``aborted_play`` is ``desc`` containing the fixed string ``"Aborted"``
(``helper_additional_functions.R``, ``clean_pbp``); ``play_type_nfl`` is the NFL feed's raw play type.

* Shield (``nfl_model_pbp``): the feed's own ``playType``, with an intercepted ``PASS`` labelled
  ``INTERCEPTION`` as nflverse's feed does (every 2024 intercepted pass; 99.94% of 2024 plays agree).
* ESPN (``espn_nfl_pbp``): no NFL play type in the feed, so a lookup from ESPN ``type.text`` plus the
  processor's own penalty / sack / pass / rush flags, fit on the even seasons 2006-2024 (98.5-99.5% on every
  odd season 2007-2025, out of sample; ClaudeCowork ``notes/2026-11-24-nfl-espn-pbp-column-gap/fit_multi.py``).

Oracle: ``fixtures/nflverse/play_type_nfl_2024.parquet`` (provenance in ``fixtures/README.md``).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess
from sportsdataverse.nfl.nfl_pbp import ESPN_PLAY_TYPE_NFL
from sportsdataverse.nfl.shield_pbp.build import build_pbp
from sportsdataverse.nfl.shield_pbp.parse import _play_type_nfl

FIX = Path(__file__).parent / "fixtures"
SHIELD = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_shield" / "2024_01_BAL_KC.json.gz"
ORACLE = pl.read_parquet(FIX / "nflverse" / "play_type_nfl_2024.parquet").with_columns(
    pl.col("play_id").cast(pl.Int64), pl.col("aborted_play").cast(pl.Int64)
)
#: the vocabulary nflverse's play_type_nfl uses (every value in its 2024 release)
NFLVERSE_PLAY_TYPES = {
    "PASS",
    "RUSH",
    "KICK_OFF",
    "PENALTY",
    "TIMEOUT",
    "PUNT",
    "SACK",
    "XP_KICK",
    "FIELD_GOAL",
    "END_QUARTER",
    "INTERCEPTION",
    "GAME_START",
    "END_GAME",
    "UNSPECIFIED",
    "PAT2",
    "FUMBLE_RECOVERED_BY_OPPONENT",
    "COMMENT",
}


def _oracle(game_id: str) -> pl.DataFrame:
    return ORACLE.filter(pl.col("game_id") == game_id).select(
        "play_id", pl.col("play_type_nfl").alias("nv_type"), pl.col("aborted_play").alias("nv_aborted")
    )


# --- Shield ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("play_type", "stats", "want"),
    [
        ("PASS", {"interception": 1}, "INTERCEPTION"),
        ("PASS", {"interception": 0}, "PASS"),
        ("PASS", {}, "PASS"),
        ("RUSH", {"interception": 1}, "RUSH"),  # only a pass is relabelled
        ("KICK_OFF", {}, "KICK_OFF"),
        (None, {}, None),
    ],
)
def test_shield_play_type_nfl_is_the_feed_type_with_interceptions_relabelled(play_type, stats, want):
    assert _play_type_nfl(play_type, stats) == want


def test_shield_matches_nflverse_on_a_real_game():
    game = json.load(gzip.open(SHIELD, "rt", encoding="utf-8"))
    plays = build_pbp(game, game_id="2024_01_BAL_KC")
    assert plays.schema["play_type_nfl"] == pl.Utf8 and plays.schema["aborted_play"] == pl.Int64
    j = plays.select(pl.col("play_id").cast(pl.Int64), "play_type_nfl", "aborted_play").join(
        _oracle("2024_01_BAL_KC"), on="play_id", how="inner"
    )
    assert j.height >= 150
    assert (j["play_type_nfl"] == j["nv_type"]).mean() >= 0.99
    assert (j["aborted_play"] == j["nv_aborted"]).all()


# --- ESPN -----------------------------------------------------------------------------------------


def _espn_rows(**cols) -> pl.DataFrame:
    n = len(cols["type.text"])
    base = {
        "text": ["x"] * n,
        "penalty_no_play": [False] * n,
        "sack": [False] * n,
        "pass": [False] * n,
        "rush": [False] * n,
    }
    return pl.DataFrame({**base, **cols})


def _describe(df: pl.DataFrame) -> pl.DataFrame:
    return NFLPlayProcess._NFLPlayProcess__add_description_features(NFLPlayProcess.__new__(NFLPlayProcess), df)


def test_espn_play_type_nfl_rule():
    df = _espn_rows(
        **{
            "type.text": ["Rush", "Pass Incompletion", "Sack", "Fumble Recovery (Own)", "Kickoff", "Brand New Type"],
            "penalty_no_play": [False, True, False, False, False, False],
            "sack": [False, False, True, False, False, False],
            "pass": [False, True, True, True, False, False],
        }
    )
    out = _describe(df)
    # a penalty no-play is PENALTY whatever ESPN typed it; a flag-resolved fumble takes the pass flag;
    # an ESPN type the map has never seen is UNSPECIFIED, never null
    assert out["play_type_nfl"].to_list() == ["RUSH", "PENALTY", "SACK", "PASS", "KICK_OFF", "UNSPECIFIED"]
    assert set(ESPN_PLAY_TYPE_NFL.values()) <= NFLVERSE_PLAY_TYPES


def test_espn_aborted_play_is_the_fixed_string_case_sensitive():
    out = _describe(_espn_rows(**{"type.text": ["Rush"] * 3}, text=["(Aborted) J.Doe FUMBLES", "aborted", None]))
    assert out["aborted_play"].to_list() == [1, 0, 0] and out.schema["aborted_play"] == pl.Int8


def test_espn_matches_nflverse_on_a_real_game():
    summary = json.load(gzip.open(FIX / "summary_401671740_trimmed.json.gz", "rt", encoding="utf-8"))
    proc = NFLPlayProcess(gameId=401671740, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    plays = proc.plays_frame
    assert plays.schema["play_type_nfl"] == pl.Utf8 and plays.schema["aborted_play"] == pl.Int8
    keyed = plays.select(
        pl.col("id")
        .cast(pl.Utf8)
        .str.slice(pl.col("game_id").cast(pl.Utf8).str.len_chars())
        .cast(pl.Int64)
        .alias("play_id"),
        "play_type_nfl",
        pl.col("aborted_play").cast(pl.Int64),
    )
    j = keyed.join(_oracle("2024_04_PHI_TB"), on="play_id", how="inner")
    assert j.height >= 140
    assert (j["play_type_nfl"] == j["nv_type"]).mean() >= 0.97
    assert (j["aborted_play"] == j["nv_aborted"]).all()
    assert j["nv_aborted"].sum() >= 1  # this game has an aborted play, so the flag is exercised


def test_espn_processor_two_point_labels_and_own_fumble_touchdown_resolve():
    """Review #1255: labels the processor itself emits map to nflverse types, never UNSPECIFIED."""
    df = _espn_rows(
        **{
            "type.text": [
                "Two-Point Conversion Good",
                "Two-Point Conversion Missed",
                "Fumble Recovery (Own) Touchdown",
            ],
            "rush": [False, False, True],
        }
    )
    assert _describe(df)["play_type_nfl"].to_list() == ["PAT2", "PAT2", "RUSH"]
