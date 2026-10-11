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
    """243040130, Michigan State @ Michigan 2004, three overtimes: both second-overtime touchdowns
    are followed by a kicked extra point. No shootout exists before 2019, so each try weighs the
    kick and takes its touchdown's board."""
    plays = _plays(243040130)
    tries = plays.filter((pl.col("period") >= 5) & (pl.col("type.text") == "Extra Point Good"))
    assert tries.height == 2
    for r in tries.iter_rows(named=True):
        td = _before(plays, r)
        assert "touchdown" in td["type.text"].lower() and td["pos_team"] == r["pos_team"]
        assert r["two_pt_recommendation"] is not None
        assert r["wp_before"] == pytest.approx(td["wp_after"], abs=1e-6)


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
