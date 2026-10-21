"""Task 2.19: ESPN's out-of-place period labels and the type.text-keyed scoring overlay.

Fixtures: ``fixtures/era_play_order/`` (trimmed nfl-raw summaries, README "Era play order") and the
task 2.20 ``fixtures/td_flag_residuals/`` summaries. Oracle: nflverse ``qtr`` / ``epa`` on the play
(local ``pbp_<season>.parquet``, joined on ``game_id`` + ``play_id``; values below and in the README).

Every EPA ceiling is the error measured on the fixed code, rounded up. Never lower this gate (or raise a
ceiling) to pass: a red here is a regression in the period repair or the scoring overlay, so debug that.
"""

from __future__ import annotations

import functools
import gzip
import json

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_fourth_down as nfl_fourth_down
from sportsdataverse.nfl import NFLPlayProcess
from sportsdataverse.nfl.model_vars import clock_stoppage_vec
from sportsdataverse.nfl.nfl_pbp import _kickoff_next_snap
from tests.nfl._whole_game import FIX


@functools.cache
def _plays(path: str, game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / path, "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(nfl_fourth_down, "_try_load", lambda name: None)
        proc = NFLPlayProcess(gameId=game_id)
        proc.espn_nfl_pbp(summary=summary)
        proc.run_processing_pipeline()
    return proc.plays_frame


def _row(path: str, game_id: int, play_id: int | str) -> dict:
    f = _plays(path, game_id).filter(pl.col("id").cast(pl.Utf8) == f"{game_id}{play_id}")
    assert f.height == 1, f"{game_id}/{play_id} not in the processed plays"
    return f.row(0, named=True)


def _era(game_id: int) -> str:
    return f"era_play_order/summary_{game_id}_trimmed.json.gz"


# (game, play) -> nflverse qtr. 8 cases: ESPN's period is wrong on 3 (261022011/2073, 260910005/2797 and 2843),
# which the repair lowers, and right on the other 5, each of which a looser rule would have "repaired"
# (measured on nfl-raw 2002-2025 against nflverse qtr).
_PERIODS = {
    # R832-10: the Q2 kickoff at 0:08 after the field goal, filed as period 3 between two period-2 rows.
    (261022011, 2073): 2,
    # ESPN alternates Q3 rows between periods 3 and 4 here: 2797 (kickoff) and 2843 (rush) are filed as 4
    # and lowered; 2819 sits as 3 between them and stays 3. ESPN mislabels a play with a LATER period, so
    # a sandwich is only ever lowered (raising one: 50 of 51 wrong on nfl-raw).
    (260910005, 2797): 3,
    (260910005, 2819): 3,
    (260910005, 2843): 3,
    # A Q2 punt at 14:37 whose next row in id order is the late "End Period" row of Q1 at 0:00.
    (270930001, 1000): 2,
    # The Q3 opening kickoff at 15:00 between two Q2 timeouts (1:10, 0:44) in id order: the clock runs back
    # up into it, so this is an out-of-order id, not a mislabel.
    (281005009, 1872): 3,
    # The same out-of-order Q3 kickoff, here after Q2's "End of Half" row (the clock check holds it too).
    (271111013, 1856): 3,
    # 2018 (after ESPN_PERIOD_REPAIR_BEFORE): a Q2 snap whose id sorts among Q1 rows; ESPN is right.
    (401030749, 906): 2,
}


@pytest.mark.parametrize(("game_id", "play_id"), list(_PERIODS))
def test_period_matches_nflverse_qtr(game_id, play_id):
    path = "summary_261022011_trimmed.json.gz" if game_id == 261022011 else _era(game_id)
    row = _row(path, game_id, play_id)
    assert row["period"] == _PERIODS[(game_id, play_id)], (row["type.text"], row["clock.displayValue"])


# (fixture, game, play) -> (nflverse epa, ceiling). Ceilings are the measured error rounded up.
_EPA = {
    # R831-6: GB recovers DAL's kickoff fumble and returns it for a touchdown; ESPN types it "Kickoff Return
    # Touchdown" (offense_score_vec), so the receiver's +6.92 was scored: EPA +6.302 (error 13.97).
    (_era(301107009), 301107009, 1557): (-7.672, 0.14),
    # EX-2.20-a: a short-of-landing-zone kick and an onside kick ESPN types "Kickoff Return Touchdown", not
    # scored (task 2.20 cleared td_play); the overlay still scored the receiver's touchdown: EPA 5.134 / 5.748.
    ("td_flag_residuals/summary_401671715.json.gz", 401671715, 2262): (0.926, 0.26),
    ("td_flag_residuals/summary_401671715.json.gz", 401671715, 4728): (1.419, 0.40),
    # R856-1: a defence-typed score that did not stand. D.Hixon's 73-yard "Punt Return Touchdown" was nullified by
    # holding (td_play false); without td_stood on def_score the punting offence took the receiver's -6.92: EPA
    # -5.833. The punt end state (EX-2.19-a, task 2.24: the receiver's next snap, not ESPN's end yard line) took
    # it from 0.321 to -1.825; the 0.14 ceiling is the remaining error rounded up (it was 2.02 before 2.24).
    (_era(281109021), 281109021, 3307): (-1.690, 0.14),
}


@pytest.mark.parametrize(
    ("path", "game_id", "play_id"), list(_EPA), ids=["kicker-td", "short-kick", "onside", "punt-td-nullified"]
)
def test_scoring_overlay_reads_the_corrected_flags(path, game_id, play_id):
    nflverse_epa, ceiling = _EPA[(path, game_id, play_id)]
    row = _row(path, game_id, play_id)
    assert abs(row["EPA"] - nflverse_epa) <= ceiling, (row["EPA"], row["td_play"], row["type.text"])


# Owner ruling (2026-10-17, R856-2): a touchback runs no clock, so a touchback kickoff's EP_end is scored at
# its START clock. 271223008/1912 (2007, KC kicks at 0:23 of Q2, nflverse touchback, epa 0) took its 1:08
# end clock from a timeout row filed after it in id order, so EP_end (0.590) and EP_start (0.075) were
# scored 45 s apart: EPA +0.516. At the start clock the touchback is worth 0. WPA keeps ESPN's end clock.
_TOUCHBACK_KICKOFFS = {(271223008, 1912): 0.0}
# R856-15: text touchbacks whose ESPN end clock is 0. The period-end clamp has put their end at the 99 (the
# public end.yardsToEndzone, which WP reads, stays there); the EP_end view scores them at the season's touchback
# spot, 1st & 10, at the start clock. Only the text says touchback here (the end yard line is 99, not the
# spot). Before: 400791544/2058 (2015, MIA kicks at 0:27 of Q2) EPA -0.431, 401127996/1913 (2019, DET at 0:14
# of Q2) -0.316, each scored as the receiver's ball on his own 1 at the start clock.
_CLAMPED_TEXT_TOUCHBACKS = {(400791544, 2058): 0.0, (401127996, 1913): 0.0}


@pytest.mark.parametrize(("game_id", "play_id"), list(_TOUCHBACK_KICKOFFS))
def test_touchback_kickoff_ep_end_at_the_start_clock(game_id, play_id):
    row = _row(_era(game_id), game_id, play_id)
    assert row["kickoff_tb"] is True
    assert row["end.TimeSecsRem"] != row["start.TimeSecsRem"]  # ESPN's end clock still moves (WP reads it)
    assert abs(row["EPA"] - _TOUCHBACK_KICKOFFS[(game_id, play_id)]) < 1e-3, (row["EP_start"], row["EP_end"])


# The ruling covers touchbacks only: a kickoff_tb row whose text says touchback or whose receiver next snaps
# at the season's touchback spot. kickoff_tb also flags the 2006-08 bare "KickOff" rows, most of which
# nflverse has as returns (R831-2), and a return runs the clock. 271209010/3234 (2007, the capture, a bare
# "KickOff" nflverse returns, epa 0.242) keeps ESPN's end clock: EPA 0.354; at the start clock 0.400.
_FLAGGED_RETURNS = {(271209010, 3234): (0.242, 0.12)}


@pytest.mark.parametrize(("game_id", "play_id"), list(_FLAGGED_RETURNS))
def test_flagged_bare_kickoff_return_keeps_the_end_clock(game_id, play_id):
    nflverse_epa, ceiling = _FLAGGED_RETURNS[(game_id, play_id)]
    row = _row(f"summary_{game_id}_trimmed.json.gz", game_id, play_id)
    assert row["kickoff_tb"] is True and row["text"] == "KickOff"
    assert abs(row["EPA"] - nflverse_epa) <= ceiling, (row["EPA"], row["end.yardsToEndzone"])


@pytest.mark.parametrize(("game_id", "play_id"), list(_CLAMPED_TEXT_TOUCHBACKS))
def test_text_touchback_with_a_zero_end_clock_ends_at_the_spot(game_id, play_id):
    row = _row(_era(game_id), game_id, play_id)
    assert row["kickoff_tb"] is True and "Touchback" in row["text"]
    assert (row["end.TimeSecsRem"], row["end.yardsToEndzone"]) == (0, 99)  # the public end state is untouched
    assert abs(row["EPA"] - _CLAMPED_TEXT_TOUCHBACKS[(game_id, play_id)]) < 1e-3, (row["EP_start"], row["EP_end"])


# EX-2.19-a / EX-2.19-c (task 2.24): before ESPN_FULL_KICK_TEXT_FROM (2009) the feed writes a bare "Punt", and its
# end yard line is often not the receiver's spot. 280921001/594 (2008, KC punts from its 33; nflverse: "punts 52
# yards to ATL 15", returned to the ATL 8, epa 1.240) has ESPN's end 1 yard from ATL's goal, so EP_end was ATL's
# 1st & goal at the 1: EPA -4.246. Like a kickoff, the punt ends where the receiver's next snap starts (ATL 8,
# 92 yards to go): EPA 1.530. Unless ESPN filed the next snap out of order: 261113029/386 (2006, CAR punts at 7:26
# of Q1 to the TB 5, returned to the 17; nflverse epa -0.119) is followed in id order by TB's 416 at 11:45, a snap
# from before the punt at the TB 45. Its end stays ESPN's 83, where TB's next snap (428, 7:15) starts: EPA 0.005
# (-2.632 at the 45). Each ceiling is the measured error rounded up.
_BARE_PUNTS = {(280921001, 594): (1.240, 92, 0.30), (261113029, 386): (-0.119, 83, 0.13)}


@pytest.mark.parametrize(("game_id", "play_id"), list(_BARE_PUNTS))
def test_bare_punt_ends_at_the_receivers_next_snap(game_id, play_id):
    nflverse_epa, next_snap_y2e, ceiling = _BARE_PUNTS[(game_id, play_id)]
    row = _row(_era(game_id), game_id, f"{play_id:04d}")  # ESPN pads the play part of the id: 2809210010594
    assert row["type.text"] == "Punt" and row["text"] in ("Punt", None)  # the bare pre-2009 feed
    assert row["end.yardsToEndzone"] == next_snap_y2e
    assert abs(row["EPA"] - nflverse_epa) <= ceiling, (row["EP_start"], row["EP_end"])


# R963-1: a punt whose next snap by id is the PUNTING team's keeps ESPN's end. 261113029/126 (2006, TB punts 22 yards
# from the CAR 40 to the CAR 18 at 10:54 of Q1) is followed in id order by TB's own 141 at the CAR 29, filed out of
# order but not earlier on the clock (870 s vs the punt's 1554), so only the team check stops it. ESPN's end, 82, is
# where CAR's next snap (163) starts in nflverse. The EPA cannot gate this row: its error is EP_start and the clock.
_PUNTER_SNAPS_NEXT = {(261113029, 126): 82}


@pytest.mark.parametrize(("game_id", "play_id"), list(_PUNTER_SNAPS_NEXT))
def test_bare_punt_ignores_the_punting_teams_next_snap(game_id, play_id):
    row = _row(_era(game_id), game_id, f"{play_id:04d}")
    assert row["type.text"] == "Punt"
    assert row["end.yardsToEndzone"] == _PUNTER_SNAPS_NEXT[(game_id, play_id)]


# R963-2: the 2009 floor (ESPN_FULL_KICK_TEXT_FROM). 2009+ punts keep ESPN's end even where it is wrong:
# 400554340/1481 (2014) ends at 8, the receiver's next snap is at 92 (EPA -5.36, nflverse 0.08). This asserts the
# RIGHT end, so it xfails today and goes red (strict) if the floor is widened without task nfl 2.25's measurement.
@pytest.mark.xfail(strict=True, reason="2009+ punts keep ESPN's end until task nfl 2.25 (R785-6) deletes this marker")
def test_post_2009_punt_ends_at_the_receivers_next_snap():
    row = _row("summary_400554340_trimmed.json.gz", 400554340, 1481)
    assert row["type.text"] == "Punt"
    assert row["end.yardsToEndzone"] == 92


# R963-4: the next-snap read is windowed by game and half, so a frame of two games reads each game's own next snap.
def test_next_snap_read_stays_inside_its_game():
    stop = pl.col("type.text").is_in(clock_stoppage_vec).fill_null(False)
    kickoff, bare_punt, nxt, next_snap, _ = _kickoff_next_snap(stop)

    def spots(df: pl.DataFrame) -> pl.DataFrame:
        f = df.with_columns(_nxt=nxt)
        return f.select(
            "id", spot=((kickoff | bare_punt) & next_snap).fill_null(False), y2e=pl.col("_nxt").struct.field("y2e")
        )

    games = [_plays(_era(g), g) for g in (280921001, 261113029)]
    apart = pl.concat([spots(g) for g in games])
    together = spots(pl.concat(games, how="diagonal_relaxed"))
    assert apart["spot"].sum() > 10
    assert together.equals(apart)
