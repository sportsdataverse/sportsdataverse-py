"""Turnover end states and punt-return fumble teams, on real ESPN games (offline).

Found comparing Game on Paper's NFL output with nflfastR on 154 games (2009-2026): turnover plays were the
worst EPA class (13-14% of interception returns and fumble recoveries more than 0.5 from nflfastR).

* ``summary_400791618_trimmed.json.gz`` -- NYG @ NO, 2015 week 8. ESPN ends both fumbles below at a spot
  the next snap contradicts, counted from the wrong goal.
* ``summary_401772776_trimmed.json.gz`` -- TB @ BUF, 2025 week 11. A punt returner's fumble the punting team
  recovers, which ESPN types "Fumble Recovery (Opponent)" (the punt flag drops) and writes in NFL wording
  ("R.Dixon punts 46 yards ... M.Hardman to BUF 41 for no gain (J.Hayes). FUMBLES (J.Hayes), RECOVERED by TB").
* ``summary_331124022_trimmed.json.gz`` -- IND @ ARI, 2013 week 12. A sack fumble the offense keeps, followed by a
  field goal ESPN starts at the kick spot rather than the line of scrimmage.
* ``summary_281123030_trimmed.json.gz`` -- MIN @ JAX, 2008 week 12. An interception whose next play by id is the
  passer's own earlier snap, filed out of order.
* ``summary_400554411_trimmed.json.gz`` -- DET @ GB, 2014 week 17. A free kick from the DET 20 that GB muffs and
  DET recovers, typed "Fumble Recovery (Opponent)", whose pos_team is the kicker.
* ``summary_401128098_trimmed.json.gz`` -- MIN @ GB, 2019 week 2. A 61-yard catch to the GB 3 that drew a flag,
  which ESPN ends 97 yards from the goal.
* ``summary_401772890_trimmed.json.gz`` -- PHI @ DAL, 2025 week 12. A sack whose next snap's start ESPN counts from
  the wrong goal, so its own end spot must stand.
* ``summary_400951762_trimmed.json.gz`` -- CLE @ PIT, 2017 week 17. A fourth-down run stopped at the PIT 2, a
  turnover on downs.
* ``summary_400951721_trimmed.json.gz`` -- ATL @ NYJ, 2017 week 8. A punt return touchdown a flag nullified, which
  ESPN types "Penalty" and ends in the punter's frame.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_pbp as mod

FIX = Path(__file__).parent / "fixtures"


def _plays(game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = mod.NFLPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame


@pytest.fixture(scope="module")
def nyg_no() -> pl.DataFrame:
    return _plays(400791618)


@pytest.fixture(scope="module")
def tb_buf() -> pl.DataFrame:
    return _plays(401772776)


def _row(plays: pl.DataFrame, play_id: int) -> dict:
    return plays.filter(pl.col("id") == play_id).row(0, named=True)


def test_an_opponent_recovery_ends_where_the_recovering_team_snaps(nyg_no):
    # NO recovers at the NYG 37 and returns it to the 36; ESPN ends it 80 yards out, the NYG 20 seen from NO.
    # nflfastR: EPA -3.84. ESPN's spot gave -0.63.
    r = _row(nyg_no, 4007916182944)
    assert r["end.pos_team.id"] == 18
    assert r["end.yardsToEndzone"] == 36
    assert r["EPA"] == pytest.approx(-3.84, abs=0.5)


def test_an_own_recovery_ends_where_the_offense_snaps_next(nyg_no):
    # NYG keeps its fumble on 3rd down and punts from its own 9 next; ESPN ends it 2 yards from the NO goal.
    # nflfastR: EPA -2.02. ESPN's spot gave +3.65.
    r = _row(nyg_no, 4007916181418)
    assert r["end.yardsToEndzone"] == 91
    assert r["EPA"] == pytest.approx(-2.02, abs=0.5)


def test_a_turnover_before_a_field_goal_keeps_its_own_end_spot():
    # ARZ keeps C.Palmer's sack fumble at the IND 30; the field goal that follows starts at the 38, the kick spot.
    # nflfastR: EPA -1.92. Reading the field goal's start gave -3.08.
    r = _row(_plays(331124022), 3311240221541)
    assert r["end.yardsToEndzone"] == 30
    assert r["EPA"] == pytest.approx(-1.92, abs=0.3)


def test_an_out_of_order_next_play_neither_spots_nor_unflips_a_turnover():
    # JAX's next play by id is its own 2:37 snap, earlier than the 2:09 interception: no evidence of where the
    # interception ended or who has the ball, so ESPN's spot stands and the interception still flips EP_end
    # (main: -0.88; reading that play gave +0.80).
    r = _row(_plays(281123030), 2811230304024)
    assert r["end.yardsToEndzone"] == 76
    assert r["EPA"] == pytest.approx(-0.88, abs=0.1)


def test_an_untyped_kickoff_gets_no_kick_teams_from_pos_team():
    # pos_team is the kicker (DET 8) on this "Fumble Recovery (Opponent)" kickoff, unlike a flagged kickoff's;
    # filling kicking_team from def_pos_team would name GB, the receiver.
    r = _row(_plays(400554411), 4005544113835)
    assert r["pos_team"] == 8
    assert r["kicking_team"] != 9 and r["kick_return_team"] != 8


def test_a_plain_play_takes_the_next_snap_when_its_yardage_agrees():
    # 61 yards from the GB 64 is the GB 3, where MIN snaps next; ESPN ended it at the 97 (nflfastR: EPA +4.41,
    # ESPN's spot gave -1.73).
    r = _row(_plays(401128098), 4011280981852)
    assert r["end.yardsToEndzone"] == 3
    assert r["EPA"] == pytest.approx(4.41, abs=0.5)


def test_a_plain_play_keeps_its_end_when_the_next_snap_is_mirrored():
    # Sacked at the DAL 7 from the DAL 13: ESPN's end (93 to go) agrees with the yardage; the next snap's start reads
    # 7, from the wrong goal, so it is not taken (nflfastR: EPA -1.96).
    r = _row(_plays(401772890), 4017728902481)
    assert r["end.yardsToEndzone"] == 93
    assert r["EPA"] == pytest.approx(-1.96, abs=0.3)


def test_a_turnover_on_downs_compares_spots_in_the_new_offenses_frame():
    # 4th & goal at the PIT 1, stopped at the 2 for -1: CLE snaps next at its own 2, 98 to go. The yardage implies 2
    # in PIT's frame, 98 in CLE's; compared in PIT's, ESPN's end won and EPA was +2.1 (nflfastR: -3.70).
    r = _row(_plays(400951762), 400951762791)
    assert r["end.yardsToEndzone"] == 98
    assert r["EPA"] == pytest.approx(-3.70, abs=0.5)


def test_a_punt_a_flag_retyped_penalty_ends_at_the_receivers_next_snap():
    # "L.Edwards punts 51 yards to ATL 20 ... TOUCHDOWN NULLIFIED by Penalty": ESPN ends it 25 from the goal, in the
    # punter's frame (EPA +4.80); ATL snaps next at its own 25.
    r = _row(_plays(400951721), 4009517213623)
    assert r["type.text"] == "Penalty"
    assert r["end.yardsToEndzone"] == 75


def test_a_punt_return_fumble_credits_the_punting_team_with_the_force(tb_buf):
    r = _row(tb_buf, 4017727762481)
    assert (r["pos_team"], r["def_pos_team"]) == (27, 2)  # TB punts
    assert (r["kicking_team"], r["return_team"]) == (27, 2)
    assert (r["punt_team"], r["punt_return_team"]) == (27, 2)
    assert r["fumbling_team"] == 2  # the BUF returner
    assert r["forced_fumble_team"] == 27  # J.Hayes covers the punt for TB
    assert r["fumble_recovery_team"] == 27
    assert r["def_fumble_lost"] is True and r["pos_fumble_lost"] is False


def test_a_turnover_typed_play_the_starting_team_keeps_is_not_flipped(tb_buf):
    # TB punts and recovers BUF's fumble: TB has the ball before and after, so EP_end stays TB's.
    # nflfastR: EPA +5.03. Flipping on the type alone gave -1.07.
    r = _row(tb_buf, 4017727762481)
    assert r["start.pos_team.id"] == r["end.pos_team.id"] == 27
    assert r["EP_end"] > 0
    assert r["EPA"] == pytest.approx(5.03, abs=0.5)
