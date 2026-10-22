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


# Task nfl 2.25 (R785-6): the receiver's next snap ends a punt in every era, not only the bare pre-2009 feed. ESPN's
# 2009+ end yard line is wrong as often: 400554340/1481 (2014, a fair catch at the MIN 16 and a holding foul on MIN)
# ends 8 yards from MIN's scoring, MIN's next snap is at its own 8; 290927014/3343 (2009, a return to the GB 14 and a
# holding foul) ends at 12, GB snaps at its own 6; 401671835/770 (2024, returned to the CIN 27) ends at 27, CIN snaps
# at its own 27. (game, play) -> (end yard line = the next snap's = nflverse's, nflverse epa, ceiling); EPA on devel-nfl
# -5.360 / -4.130 / -3.468. Each ceiling is the measured error rounded up.
_POST_2009_PUNTS = {
    (400554340, 1481): (92, 0.077, 0.20),
    (290927014, 3343): (94, 1.025, 0.35),
    (401671835, 770): (73, -0.555, 0.02),
}


@pytest.mark.parametrize(("game_id", "play_id"), list(_POST_2009_PUNTS))
def test_post_2009_punt_ends_at_the_receivers_next_snap(game_id, play_id):
    next_snap_y2e, nflverse_epa, ceiling = _POST_2009_PUNTS[(game_id, play_id)]
    row = _row(f"summary_{game_id}_trimmed.json.gz", game_id, play_id)
    assert row["type.text"] == "Punt"
    assert row["end.yardsToEndzone"] == next_snap_y2e
    assert abs(row["EPA"] - nflverse_epa) <= ceiling, (row["EP_start"], row["EP_end"])


# R963-3 / R971-2 / R971-3: the punt takes the snap's clocks too, the half clock and the game clock. Its down and
# distance were already the snap's: carrying them too left all 502 measured games (57 captures, 296 era sample, 149
# held out) byte-identical. (game, play) -> (end.TimeSecsRem, end.adj_TimeSecsRem, end yard line, nflverse epa, ceiling):
# - 270930001/1000 (2007, a punt at 14:37 of Q2): ESPN's end clock was 0, so the EP block's half-end clamp scored it
#   as the half's last play, at the 99 (EPA 0.485). The receiver's snap is at 870 s of the half (2670 of the game), at
#   the 90 (nflverse yardline_100 90, epa 0.061): EPA 0.386.
# - 291203002/3741 (2009, a fair catch at 2:39 of Q4): ESPN files "Timeout #3 by BUF at 03:25" after it, so its end
#   clock was 205 s, later than its own start (159). BUF's sack is at 152 s, nflverse's next play: EPA -0.245 -> -0.272
#   (nflverse -0.458).
_PUNT_CLOCKS = {(270930001, 1000): (870, 2670, 90, 0.061, 0.33), (291203002, 3741): (152, 152, 77, -0.458, 0.19)}


@pytest.mark.parametrize(("game_id", "play_id"), list(_PUNT_CLOCKS))
def test_punt_ends_at_the_next_snaps_clock(game_id, play_id):
    clock, game_clock, y2e, nflverse_epa, ceiling = _PUNT_CLOCKS[(game_id, play_id)]
    row = _row(_era(game_id), game_id, play_id)
    assert (row["end.TimeSecsRem"], row["end.adj_TimeSecsRem"], row["end.yardsToEndzone"]) == (clock, game_clock, y2e)
    assert abs(row["EPA"] - nflverse_epa) <= ceiling, (row["EP_start"], row["EP_end"])


# R963-5: the net-spot read keys on ESPN's scoringPlay, not td_play. A return touchdown ESPN left unscored ends at its
# try, nflfastR's next play, and that is the end state nflverse's WPA reads: 261022011/1629 (a punt return TD) and
# 271209034/1856 (a kickoff return TD) are each nearer nflverse with the try (wpa -0.171 / 0.101) than with ESPN's end
# (-0.136 / 0.001). (fixture, game, play) -> (the try's yard line, nflverse wpa, ceiling).
_UNSCORED_RETURN_TDS = {
    ("summary_261022011_trimmed.json.gz", 261022011, 1629): (2, -0.275, 0.11),
    ("td_flag_residuals/summary_271209034.json.gz", 271209034, 1856): (2, 0.116, 0.02),
}


@pytest.mark.parametrize(("path", "game_id", "play_id"), list(_UNSCORED_RETURN_TDS))
def test_unscored_return_touchdown_ends_at_its_try(path, game_id, play_id):
    try_y2e, nflverse_wpa, ceiling = _UNSCORED_RETURN_TDS[(path, game_id, play_id)]
    row = _row(path, game_id, play_id)
    assert row["scoringPlay"] is False and row["td_play"] is True
    assert row["end.yardsToEndzone"] == try_y2e
    assert abs(row["wpa"] - nflverse_wpa) <= ceiling


# R971-4: a punt's next play may not be a kickoff. 271021019/2661 (2007, NYG's punt blocked out of its end zone for a
# safety at 9:01 of Q3; nflverse safety 1) is not one of ESPN's scoring plays, so the net-spot read reaches it; the next
# play is NYG's free kick, not SF's snap. The punt keeps ESPN's end (99); task 2.28 scores it as a safety.
def test_punt_followed_by_a_free_kick_keeps_espns_end():
    row = _row(_era(271021019), 271021019, 2661)
    assert row["scoringPlay"] is False
    assert row["end.yardsToEndzone"] == 99


# Task 2.28: the same row is a safety ESPN neither scored nor typed (blank text): SF's score rises by 2 on it and NYG
# free-kicks from its own 20 next. On devel-nfl it kept "Blocked Punt" and scored from its end state, EPA 4.292 against
# nflverse 0.624, and the score repair dropped SF's 2 points as a feed error until SF's next score (26-7, nflverse
# 26-9). Read from the score and the free kick, it takes the -2 overlay and keeps ESPN's type: EPA 0.626.
def test_unscored_safety_scores_two_for_the_defence():
    row = _row(_era(271021019), 271021019, 2661)
    assert (row["type.text"], row["safety"], row["fixed_drive_result"]) == ("Blocked Punt", True, "Safety")
    assert abs(row["EPA"] - 0.624) <= 0.01, (row["EP_start"], row["EP_end"])
    kick = _row(_era(271021019), 271021019, 2683)
    assert (kick["homeScore"], kick["awayScore"]) == (26, 9)


# Task 2.28: a safety ESPN scored but typed "Blocked Punt" (271223008/738, text "Safety"), outside defense_score_vec, so
# the -2 overlay skipped it: EPA 2.499, nflverse -0.015. ESPN types the free kick after it "Safety" (764), so that type
# counts as a free kick too: EPA 0.045. The row keeps its type (and so its punt and blocker columns).
def test_scored_safety_typed_a_blocked_punt():
    row = _row(_era(271223008), 271223008, "0738")
    assert (row["type.text"], row["scoringPlay"], row["safety"]) == ("Blocked Punt", True, True)
    assert abs(row["EPA"] - (-0.015)) <= 0.07, (row["EP_start"], row["EP_end"])


# Task 2.28: the untyped-safety read needs both signals and a free kick. Each row has one without the rest and is not a
# safety (nflverse safety 0): a try before a kickoff from the 20 (a 15-yard foul enforced on it) with no 2-point rise;
# ESPN books 2 points for the defence on a snap the offence follows with a snap from its own 20 (no kick); and, a row
# early, on the snap before B.Moorman's intentional safety (996, ESPN "Safety"), which starts at the BUF 24, not the 20.
_NOT_SAFETIES = {
    ("td_flag_residuals/summary_311218002.json.gz", 311218002, 4196): "Extra Point Good",
    (_era(400874522), 400874522, 79): "Rush",
    (_era(271216005), 271216005, "0961"): "Pass",
}


@pytest.mark.parametrize(("path", "game_id", "play_id"), list(_NOT_SAFETIES), ids=["no-rise", "no-kick", "not-from-20"])
def test_one_safety_signal_alone_is_not_a_safety(path, game_id, play_id):
    row = _row(path, game_id, play_id)
    assert (row["type.text"], row["safety"]) == (_NOT_SAFETIES[(path, game_id, play_id)], False)


# Task 2.28: a safety ESPN scored and typed "Safety", but whose text never says "safety" (blank on most pre-2009 rows).
# The -2 overlay keyed on the text, so the row fell through to the defence-touchdown -6.92: 281012016/923 (2008, "Dan
# Orlovsky tackled in end zone by Jared Allen.") had EPA -4.576, nflverse 0.230; read from the safety flag, 0.344. ESPN
# also types a free kick "Safety" without scoring it (271223008/764, nflverse a kickoff, safety 0): not a safety.
def test_scored_safety_whose_text_does_not_say_so():
    row = _row(_era(281012016), 281012016, "0923")
    assert (row["type.text"], row["safety"], row["fixed_drive_result"]) == ("Safety", True, "Safety")
    assert abs(row["EPA"] - 0.230) <= 0.12, (row["EP_start"], row["EP_end"])
    assert _row(_era(271223008), 271223008, "0764")["safety"] is False


# Task 2.28: before 2009 ESPN's kick text is blank, and it types some plain returns "Punt/Kickoff Return Touchdown".
# 251030010/1409 (2005, OAK punts, TEN's return TOUCHDOWN NULLIFIED by penalty; ESPN: no text, no score change, no try,
# not a scoring play) took td_play from the type: EPA -5.552, nflverse 1.334. No try follows, so it is not a
# touchdown: EPA 1.297. A real one ESPN left unscored has its try next (test_unscored_return_touchdown_ends_at_its_try).
# The 2005 feed has no play text at all (so the row's text is null), which the football source contract vetoes, so the
# capture is named outside the summary_* files tests/football/sources/test_contract.py requires to pass it.
def test_pre_2009_return_typed_a_touchdown_without_a_try_is_not_one():
    row = _row("era_play_order/notext_251030010_trimmed.json.gz", 251030010, 1409)
    assert row["type.text"] == "Punt Return Touchdown" and row["td_play"] is False
    assert abs(row["EPA"] - 1.334) <= 0.04, (row["EP_start"], row["EP_end"])


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
