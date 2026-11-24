"""``split_pat_rows``: ESPN's folded PATs as their own nflverse-style rows (opt-in; default rows unchanged).

From about 2014 ESPN folds the extra point / two-point try into the touchdown play's row; nflverse keeps
it as its own ``XP_KICK`` / ``PAT2`` row. ESPN carries no id for that try, so the split row gets
``id = TD id + 1`` (checked free) and ``pat_of_id`` = the TD's id. Older seasons, where ESPN already
emits the PAT as its own row (and the touchdown row carries no PAT flag), are left as they are.

Oracle: ``fixtures/nflverse/play_type_nfl_2024.parquet`` (``2024_04_PHI_TB``, ESPN capture 401671740).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess
from sportsdataverse.nfl.nfl_pbp import split_pat_rows

FIX = Path(__file__).parent / "fixtures"


def _plays() -> pl.DataFrame:
    """Folded XP good, a run, folded 2pt pass success, folded blocked PAT, then an old-era TD + separate PAT."""
    return pl.DataFrame(
        {
            "game_id": [401] * 6,
            "id": [401100, 401130, 401200, 401300, 401400, 401410],
            "game_play_number": [10, 11, 20, 30, 40, 41],
            "type.text": [
                "Passing Touchdown",
                "Rush",
                "Rushing Touchdown",
                "Rushing Touchdown",
                "Rushing Touchdown",
                "Extra Point Good",
            ],
            "text": [
                "J.Hurts pass short left to D.Goedert for 24 yards, TOUCHDOWN. J.Elliott extra point is GOOD, Center-R.Lovato.",
                "S.Barkley right end for 6 yards.",
                "J.Allen right end for 7 yards, TOUCHDOWN.TWO-POINT CONVERSION ATTEMPT. J.Allen pass to K.Shakir is complete. ATTEMPT SUCCEEDS.",
                "S.Barkley up the middle for 1 yard, TOUCHDOWN. J.Elliott extra point is BLOCKED.",
                "B.Westbrook up the middle for 2 yards, TOUCHDOWN.",
                "D.Akers extra point is GOOD.",
            ],
            "pos_team": ["PHI", "PHI", "BUF", "PHI", "PHI", "PHI"],
            "xp_attempt": [True, False, False, True, False, True],
            "two_point_attempt": [False, False, True, False, False, False],
            "extra_point_result": ["good", None, None, None, None, "good"],
            "two_point_conv_result": [None, None, "success", None, None, None],
            "pointAfterAttempt.text": ["Extra Point Good", None, "Two Point Pass", "Blocked PAT", None, None],
        }
    )


def test_split_adds_one_row_per_folded_pat_right_after_its_touchdown():
    out = split_pat_rows(_plays())
    assert out.select("id", "pat_of_id").rows() == [
        (401100, None),
        (401101, 401100),
        (401130, None),
        (401200, None),
        (401201, 401200),
        (401300, None),
        (401301, 401300),
        (401400, None),  # old era: the PAT is already its own row, nothing is split
        (401410, None),
    ]
    assert out.schema["id"] == out.schema["pat_of_id"] == pl.Int64


def test_split_row_values():
    pats = split_pat_rows(_plays()).filter(pl.col("pat_of_id").is_not_null())
    assert pats["play_type_nfl"].to_list() == ["XP_KICK", "PAT2", "XP_KICK"]
    assert pats["extra_point_result"].to_list() == ["good", None, "blocked"]  # ESPN leaves a blocked PAT null
    assert pats["two_point_conv_result"].to_list() == [None, "success", None]
    assert pats["xp_attempt"].to_list() == [True, False, True]
    assert pats["two_point_attempt"].to_list() == [False, True, False]
    assert pats["type.text"].to_list() == ["Extra Point Good", "Two Point Pass", "Blocked PAT"]
    assert pats["text"].to_list() == [
        "J.Elliott extra point is GOOD, Center-R.Lovato.",
        "TWO-POINT CONVERSION ATTEMPT. J.Allen pass to K.Shakir is complete. ATTEMPT SUCCEEDS.",
        "J.Elliott extra point is BLOCKED.",
    ]
    assert pats["pos_team"].to_list() == ["PHI", "BUF", "PHI"]  # context copied from the touchdown row
    assert pats["game_play_number"].to_list() == [10, 20, 30]


def test_touchdown_rows_hand_their_pat_to_the_split_row_and_other_rows_are_unchanged():
    out = split_pat_rows(_plays())
    tds = out.filter(pl.col("id").is_in([401100, 401200, 401300]))
    assert tds["xp_attempt"].to_list() == [False, False, False]
    assert tds["two_point_attempt"].to_list() == [False, False, False]
    assert tds["extra_point_result"].null_count() == 3 and tds["two_point_conv_result"].null_count() == 3
    keep = [c for c in _plays().columns]
    others = [401130, 401400, 401410]
    assert out.filter(pl.col("id").is_in(others)).select(keep).equals(_plays().filter(pl.col("id").is_in(others)))


def test_split_refuses_an_id_collision():
    plays = _plays().with_columns(pl.when(pl.col("id") == 401130).then(401101).otherwise(pl.col("id")).alias("id"))
    with pytest.raises(ValueError, match="401101"):
        split_pat_rows(plays)


def test_no_folded_pat_returns_the_rows_unchanged_plus_the_key_column():
    plays = _plays().filter(pl.col("id") == 401130)
    out = split_pat_rows(plays)
    assert out.drop("pat_of_id").equals(plays) and out["pat_of_id"].null_count() == 1


def test_real_game_pat_rows_match_nflverse():
    summary = json.load(gzip.open(FIX / "summary_401671740_trimmed.json.gz", "rt", encoding="utf-8"))
    proc = NFLPlayProcess(gameId=401671740, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    before = proc.plays_frame.height
    out = split_pat_rows(proc.plays_frame)
    assert proc.plays_frame.height == before  # the processor's own frame is untouched
    ours = out.filter(pl.col("pat_of_id").is_not_null())
    nv = (
        pl.read_parquet(FIX / "nflverse" / "play_type_nfl_2024.parquet")
        .filter((pl.col("game_id") == "2024_04_PHI_TB") & pl.col("play_type_nfl").is_in(["XP_KICK", "PAT2"]))
        .sort("play_id")
    )
    assert ours.height == nv.height == 6
    # in game order, the k-th split PAT is nflverse's k-th PAT
    assert ours["play_type_nfl"].to_list() == nv["play_type_nfl"].to_list()
    assert ours["extra_point_result"].to_list() == nv["extra_point_result"].to_list()  # incl. the blocked one
    assert ours["two_point_conv_result"].to_list() == nv["two_point_conv_result"].to_list()
    assert out["id"].n_unique() == out.height


def test_summary_format_pat_results_read_from_the_text():
    """ESPN's scoring-summary form, where the feed has no pointAfterAttempt / extra_point_result (2016-2019)."""
    plays = pl.DataFrame(
        {
            "game_id": [402] * 3,
            "id": [402100, 402200, 402300],
            "game_play_number": [1, 2, 3],
            "type.text": ["Rushing Touchdown", "Passing Touchdown", "Passing Touchdown"],
            "text": [
                "Christian McCaffrey 2 Yd Run (Joey Slye Kick)",
                "Tyler Higbee 5 Yd pass from Jared Goff (Greg Zuerlein PAT failed)",
                "Terry McLaurin 69 Yd pass from Case Keenum (Dustin Hopkins PAT blocked)",
            ],
            "xp_attempt": [True, True, True],
            "two_point_attempt": [False, False, False],
        }
    )
    pats = split_pat_rows(plays).filter(pl.col("pat_of_id").is_not_null())
    assert pats["extra_point_result"].to_list() == ["good", "failed", "blocked"]
    assert pats["text"].to_list() == ["Joey Slye Kick", "Greg Zuerlein PAT failed", "Dustin Hopkins PAT blocked"]


def test_a_try_a_penalty_wiped_out_is_not_split():
    """2010: a nullified try sits on a Penalty / no-play row with the PAT flags; ESPN lists the re-try itself."""
    plays = pl.DataFrame(
        {
            "game_id": [403] * 2,
            "id": [403100, 403130],
            "game_play_number": [1, 2],
            "type.text": ["Penalty", "Extra Point Good"],
            "text": [
                "D.Buehler extra point is GOOD. PENALTY on DAL-D.Free, False Start, 5 yards, enforced at TEN 2 - No Play.",
                "D.Buehler extra point is GOOD.",
            ],
            "xp_attempt": [True, True],
            "two_point_attempt": [False, False],
            "penalty_no_play": [True, False],
        }
    )
    out = split_pat_rows(plays)
    assert out.height == 2 and out["pat_of_id"].null_count() == 2


def test_a_flag_without_any_trace_of_the_try_is_not_split():
    """2019 has touchdown rows flagged xp_attempt whose text names no try (nflverse has no PAT for them)."""
    plays = pl.DataFrame(
        {
            "game_id": [404],
            "id": [404100],
            "game_play_number": [1],
            "type.text": ["Passing Touchdown"],
            "text": ["Amari Cooper 20 Yd pass from Dak Prescott"],
            "xp_attempt": [True],
            "two_point_attempt": [False],
        }
    )
    assert split_pat_rows(plays)["pat_of_id"].null_count() == 1


def test_a_defensive_touchdowns_try_belongs_to_the_scoring_team():
    """Review #1255: on a pick-six the try is the defense's; ESPN's end-of-play possession says so."""
    plays = pl.DataFrame(
        {
            "game_id": [405] * 2,
            "id": [405100, 405200],
            "game_play_number": [1, 2],
            "type.text": ["Interception Return Touchdown", "Passing Touchdown"],
            "text": [
                "J.Goff pass intended for A.St. Brown INTERCEPTED by D.Bush, TOUCHDOWN. W.Lutz extra point is GOOD.",
                "J.Goff pass to S.LaPorta for 9 yards, TOUCHDOWN. J.Bates extra point is GOOD.",
            ],
            "pos_team": ["Detroit Lions", "Detroit Lions"],
            "def_pos_team": ["Denver Broncos", "Denver Broncos"],
            "pos_team_id": [8, 8],
            "def_pos_team_id": [7, 7],
            "end.pos_team.id": [7, 8],
            "defense_score_play": [True, False],
            "xp_attempt": [True, True],
            "two_point_attempt": [False, False],
        }
    )
    pats = split_pat_rows(plays).filter(pl.col("pat_of_id").is_not_null())
    assert pats["pos_team"].to_list() == ["Denver Broncos", "Detroit Lions"]
    assert pats["pos_team_id"].to_list() == [7, 8] and pats["def_pos_team_id"].to_list() == [8, 7]
    # without end-of-play possession, defense_score_play decides
    no_end = split_pat_rows(plays.drop("end.pos_team.id")).filter(pl.col("pat_of_id").is_not_null())
    assert no_end["pos_team_id"].to_list() == [7, 8]


def test_processor_shape_end_possession_wins_over_defense_score_play():
    """Review #1255: NFLPlayProcess output keeps the numeric team ids in pos_team / def_pos_team (no *_id
    columns); end-of-play possession must still decide, also where defense_score_play disagrees."""
    plays = pl.DataFrame(
        {
            "game_id": [406] * 2,
            "id": [406100, 406200],
            "game_play_number": [1, 2],
            "type.text": ["Fumble Return Touchdown", "Passing Touchdown"],
            "text": [
                "J.Goff FUMBLES, RECOVERED by DEN-D.Bush, TOUCHDOWN. W.Lutz extra point is GOOD.",
                "J.Goff pass to S.LaPorta for 9 yards, TOUCHDOWN. J.Bates extra point is GOOD.",
            ],
            "pos_team": pl.Series([8, 8], dtype=pl.Int64),
            "def_pos_team": pl.Series([7, 7], dtype=pl.Int32),
            "end.pos_team.id": pl.Series([7, 8], dtype=pl.Int32),
            # the two signals disagree on both rows: end-of-play possession wins
            "defense_score_play": [False, True],
            "xp_attempt": [True, True],
            "two_point_attempt": [False, False],
        }
    )
    out = split_pat_rows(plays)
    pats = out.filter(pl.col("pat_of_id").is_not_null())
    assert pats["pos_team"].to_list() == [7, 8] and pats["def_pos_team"].to_list() == [8, 7]
    assert out.schema["pos_team"] == pl.Int64 and out.schema["def_pos_team"] == pl.Int32  # dtypes kept
