"""One test per CFB overtime rule set on a committed capture (C-M6).

The rule sets (sdv-internal-refs ``rules/cfb.yaml``): overtime from the 25 with no two-point
shootout before 2019 (``cfb-1996-overtime-introduced``); the shootout from the fifth overtime in
2019-20 (``cfb-2019-overtime-shootout-from-fifth``); from the third from 2021
(``cfb-2021-overtime-shootout-from-third``). A shootout attempt is one two-point play, not a
touchdown's try: it has no kick to weigh it against (``two_pt_recommendation`` is null) and it is
not handed the touchdown's board.

ESPN's overtime period label is not the overtime number (``tests/cfb/fixtures/README.md``,
"Overtime rule sets"): 401112489 files overtimes 1-5 as period 5 and its winning try as period 10,
after the two second-overtime touchdowns; 401628439 files nine of its twelve attempts as period 5
and the other three as periods 9 and 12, after a second-overtime touchdown. The expected
attempts come from the game itself (its overtime count, ESPN's row types and texts), never from
stored sdv-py output.
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from sportsdataverse.cfb.model_vars import OT_SHOOTOUT_FROM_PERIOD

# Processing a default-odds capture warns DefaultOddsWarning incidentally;
# tests/cfb/test_cfb_pbp_offline.py::test_default_odds_warn asserts it.
# 401628439 warns PlayOrderWarning; test_cfb_whole_game_oracle.py::test_period_monotonic_or_the_step_back_warns asserts it.
pytestmark = [
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning"),
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.PlayOrderWarning"),
]

FIX = Path(__file__).parent / "fixtures"
_TWO_POINT = ["Two Point Pass", "Two Point Rush"]


@functools.lru_cache(maxsize=None)
def _plays(game_id: int) -> pl.DataFrame:
    trimmed = FIX / f"summary_{game_id}_trimmed.json.gz"
    if trimmed.exists():
        with gzip.open(trimmed, "rt", encoding="utf-8") as fh:
            summary = json.load(fh)
    else:
        summary = json.loads((FIX / f"summary_{game_id}.json").read_text(encoding="utf-8"))
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame.sort("game_play_number")


def _before(plays: pl.DataFrame, row: dict) -> dict:
    return plays.filter(pl.col("game_play_number") < row["game_play_number"]).row(-1, named=True)


def test_the_rule_table_is_the_catalogued_one() -> None:
    """2019-20 from the fifth overtime (period 9), from 2021 the third (period 7), none before."""
    assert OT_SHOOTOUT_FROM_PERIOD == ((2019, 9), (2021, 7))


def test_before_2019_an_overtime_try_is_its_touchdowns() -> None:
    """272870068, Nevada @ Boise State 2007, four overtimes, every one filed as period 5. Nevada's
    forced third-overtime try "Two-point conversion attempt, PASS FAILED." comes before the
    touchdown it follows (ESPN files Luke Lippincott's 7-yard run after it), so a touchdown-before
    test alone read it as a shootout attempt, which did not exist before 2019: no kick weighed, and
    the attempt's board (0.0) instead of the touchdown's (0.1)."""
    plays = _plays(272870068)
    tries = plays.filter((pl.col("period") >= 5) & pl.col("type.text").str.contains("Conversion|Extra Point"))
    assert tries.height == 6
    forced = tries.filter(pl.col("text") == "Two-point conversion attempt, PASS FAILED.").row(0, named=True)
    assert "touchdown" not in _before(plays, forced)["type.text"].lower()  # the misfiled touchdown
    assert tries["two_pt_recommendation"].null_count() == 0  # every one a touchdown's try


def test_2019_20_the_sixth_overtime_try_is_a_shootout_attempt() -> None:
    """401112489, North Carolina @ Virginia Tech 2019, six overtimes: "Quincy Patterson Run for
    Two-Point Conversion" won it in the sixth, a shootout period under the 2019 rule. ESPN files it
    (period 10) right after the two second-overtime touchdowns, so a touchdown-before test alone
    read it as the second touchdown's try: it weighed a kick (``go_for_2``) and took that
    touchdown's board (0.54)."""
    plays = _plays(401112489)
    (r,) = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT)).iter_rows(named=True)
    assert r["text"] == "Quincy Patterson Run for Two-Point Conversion" and r["period"] == 10
    td = _before(plays, r)
    assert td["type.text"] == "Passing Touchdown" and td["period"] == 6  # the misfiled touchdown
    assert r["two_pt_recommendation"] is None
    assert r["wp_before"] != pytest.approx(td["wp_after"], abs=1e-3)


def test_2021_on_every_overtime_two_point_play_is_a_shootout_attempt() -> None:
    """401628439, Georgia Tech @ Georgia 2024, eight overtimes: the two-point plays of overtimes
    3-8 (twelve, the separate rows; the second-overtime try is folded into its touchdown row) are
    all shootout attempts under the 2021 rule. ESPN labels nine of them period 5 and three
    periods 9 and 12, the first of those after the second-overtime touchdown."""
    plays = _plays(401628439)
    attempts = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT))
    assert attempts.height == 12
    assert sorted(attempts["period"].unique().to_list()) == [5, 9, 12]
    assert attempts["two_pt_recommendation"].null_count() == 12
    first_late = attempts.filter(pl.col("period") == 9).row(0, named=True)
    assert _before(plays, first_late)["type.text"] == "Rushing Touchdown"


def test_2021_on_the_third_overtime_opens_the_shootout() -> None:
    """401282146, Alabama @ Auburn 2021, four overtimes: three two-point plays, overtimes 3 and 4
    (periods 7 and 8), none of them a touchdown's try."""
    plays = _plays(401282146)
    attempts = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT))
    assert attempts["period"].to_list() == [7, 7, 8]
    assert attempts["two_pt_recommendation"].null_count() == 3


def test_2021_on_a_third_overtime_attempt_after_the_second_overtime_touchdown() -> None:
    """401524025, Arizona @ USC 2023, three overtimes: "Caleb Williams Run for Two-Point Conversion"
    (period 7, the third overtime) won it right after Austin Jones's second-overtime touchdown, whose
    forced try ESPN folds into the touchdown row. Under the 2021 rule the third overtime is a
    shootout, so the row is an attempt even with a touchdown before it; with the 2019-20 cut
    (period 9) it read as that touchdown's try (``go_for_2``, the touchdown's board)."""
    plays = _plays(401524025)
    (r,) = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT)).iter_rows(named=True)
    assert r["text"] == "Caleb Williams Run for Two-Point Conversion" and r["period"] == 7
    td = _before(plays, r)
    assert td["text"].startswith("Austin Jones 11 Yd Run") and td["period"] == 6
    assert r["two_pt_recommendation"] is None


def _linescores(game_id: int) -> dict[str, list[int]]:
    """ESPN's header linescores per side: four quarters, then one entry per overtime."""
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        competitors = json.load(fh)["header"]["competitions"][0]["competitors"]
    return {c["homeAway"]: [int(x["displayValue"]) for x in c["linescores"]] for c in competitors}


def _shootout_attempts_expected(game_id: int, from_overtime: int) -> int:
    """Two attempts per shootout overtime, the count read off the linescore."""
    n_ot = len(_linescores(game_id)["home"]) - 4
    return 2 * (n_ot - from_overtime + 1)


def test_2019_20_the_untyped_shootout_attempts_are_attempts() -> None:
    """401112489, six overtimes under the 2019 rule (shootout from the fifth): four attempts by the
    linescore. ESPN types one ("Quincy Patterson Run for Two-Point Conversion") and records the
    other three only as untyped rows, "Quincy Patterson II Two-Point Pass Attempt Failed", "Michael
    Carter Two-Point Rush Attempt Failed", "Sam Howell Two-Point Rush Attempt Failed". As "Unknown"
    they were scored as scrimmage snaps (EPA +0.65, wp_after 0.0); they are missed attempts."""
    plays = _plays(401112489)
    assert _shootout_attempts_expected(401112489, from_overtime=5) == 4
    attempts = plays.filter((pl.col("period") >= 5) & pl.col("text").str.contains("(?i)two-point"))
    assert attempts.height == 4
    missed = attempts.filter(pl.col("text").str.contains("Attempt Failed"))
    assert missed["type.text"].to_list() == ["Two-Point Conversion Missed"] * 3
    assert missed["EPA"].to_list() == pytest.approx([-0.92] * 3)
    assert attempts["two_pt_recommendation"].null_count() == 4
    # ESPN files the three at 37-34, the first-overtime score (the linescore has 41-41): a margin of
    # 3, which a shootout cannot reach, so they have no win probability
    assert sum(_linescores(401112489)["home"][:8]) == sum(_linescores(401112489)["away"][:8]) == 41
    assert missed["pos_score_diff_start"].abs().to_list() == [3, 3, 3]
    for col in ("wp_before", "wp_after", "wp_before_naive", "wp_after_naive"):
        assert missed[col].null_count() == 3, col


def test_a_shootout_attempt_on_impossible_espn_scores_has_no_win_probability() -> None:
    """401628439: the linescore has the shootout open 40-40 (after two overtimes) and end 44-42, but
    ESPN's scores put Georgia eight ahead on every attempt (50-42), which a shootout cannot reach: a
    period opens tied and one attempt moves the margin by 2. Valued from those scores every attempt
    read 0.0 or 1.0. Not re-derived: wp_before and wp_after are null, except the last attempt's
    wp_after, the game's result."""
    ls = _linescores(401628439)
    assert sum(ls["home"][:6]) == sum(ls["away"][:6]) == 40  # tied entering the shootout
    assert (sum(ls["home"]), sum(ls["away"])) == (44, 42)
    plays = _plays(401628439)
    attempts = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT))
    assert attempts.height == _shootout_attempts_expected(401628439, from_overtime=3) == 12
    assert (attempts["pos_score_diff_start"].abs() > 2).all()  # ESPN's defect is in the capture
    for col in ("wp_before", "wp_before_naive"):
        assert attempts[col].null_count() == 12
    for col in ("wp_after", "wp_after_naive"):
        assert attempts[col].null_count() == 11
        assert attempts[col][-1] == 1.0  # Georgia's winning attempt ends the game


def test_a_shootout_attempt_left_unknown_does_not_borrow_a_later_board() -> None:
    """401866418, Jacksonville State @ Ohio 2026, four overtimes: tied 27-27 entering the third, the
    shootout (linescore), four attempts. ESPN's period-6 "End of OT." marker carries 17-34, and the
    third-overtime attempts and Ohio's fourth-overtime one start from it. Those are left unknown in
    full: before the guard their wp_after was borrowed from a later row past the null boards (0.1 and
    0.9). The fourth overtime opens on that score, so Jacksonville State's last attempt has no
    wp_before either; its miss ends the game, so its wp_after is the result (0.0)."""
    ls = _linescores(401866418)
    assert sum(ls["home"][:6]) == sum(ls["away"][:6]) == 27
    plays = _plays(401866418)
    attempts = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT))
    assert attempts.height == _shootout_attempts_expected(401866418, from_overtime=3) == 4
    for col in ("wp_before", "wp_before_naive"):
        assert attempts[col].null_count() == 4, col
    for col in ("wp_after", "wp_after_naive"):
        assert attempts[col].is_null().to_list() == [True, True, True, False], col
        assert attempts[col][-1] == 0.0


def test_a_shootout_period_that_does_not_open_tied_has_no_win_probability() -> None:
    """401677180, Pittsburgh @ Toledo 2024, six overtimes: the linescore has 42-42 after the third
    overtime, 44-44 after the fourth and 46-46 after the fifth, but ESPN's scores run Toledo 2 ahead
    from the third (42-44 after it). Each margin stays within 2, so only the first attempt of a period
    shows it: the fifth overtime opens on -2 ("Julian Dugger Pass to Gavin Bartholomew", valued 0.12).
    A period whose first attempt does not start tied has no win probability on any attempt; Toledo's
    winning attempt keeps the result (1.0)."""
    ls = _linescores(401677180)
    assert sum(ls["home"][:7]) == sum(ls["away"][:7]) == 42
    assert sum(ls["home"][:9]) == sum(ls["away"][:9]) == 46
    plays = _plays(401677180)
    attempts = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT))
    assert attempts.height == 7  # of 8 by the linescore: Pittsburgh's sixth-overtime miss is not in the feed
    ot3_second = attempts.filter(pl.col("text").str.starts_with("Tucker Gleason Pass to Terrell")).row(0, named=True)
    assert ot3_second["end.homeScore"] - ot3_second["end.awayScore"] == 2  # ESPN 42-44 after the third
    ot5_first = attempts.filter(pl.col("text").str.starts_with("Julian Dugger Pass to Gavin")).row(0, named=True)
    assert abs(ot5_first["pos_score_diff_start"]) == 2
    for col in ("wp_before", "wp_before_naive"):
        assert attempts[col].null_count() == 7, col
    for col in ("wp_after", "wp_after_naive"):
        assert attempts[col].null_count() == 6, col
        assert attempts[col][-1] == 1.0


def test_an_unknown_shootout_attempt_stays_unknown_in_a_game_in_progress() -> None:
    """A game in progress: the latest play has no next play, so its wp_after falls back to the
    model's end state, restated when the possession changes. An attempt left unknown keeps no value
    there either: 401866418 read as live, Jacksonville State's last attempt (filed with Ohio as the
    end team) would take 1 - that end state."""
    with gzip.open(FIX / "summary_401866418_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    summary["header"]["competitions"][0]["status"]["type"]["completed"] = False
    proc = CFBPlayProcess(gameId=401866418, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    plays = proc.plays_frame.sort("game_play_number")
    last = plays.row(-1, named=True)
    assert last["type.text"] in _TWO_POINT and last["start.pos_team.id"] != last["end.pos_team.id"]
    attempts = plays.filter((pl.col("period") >= 5) & pl.col("type.text").is_in(_TWO_POINT))
    for col in ("wp_after", "wp_after_naive"):
        assert attempts[col].null_count() == 4, col
