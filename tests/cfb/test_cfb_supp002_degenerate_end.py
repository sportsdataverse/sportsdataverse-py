"""supp-002 (quality program task 3.4(a)): a degenerate ESPN end block takes the next snap's start.

ESPN ships some non-scoring fumble plays with ``end {yardLine 0, yardsToEndzone 0}``. The processor's
``<= 0`` clamp read that as the own 1, so a play that kept the ball scored a goal-line disaster
(401856682's Q4 Fumble Recovery (Own): EPA -5.91, the Yahoo and CBS feeds of the same play -0.39). The
rule: when the end block is degenerate, the fumble was not lost (``pos_fumble_lost`` / ``def_fumble_lost``,
not ``change_of_pos_team``) and the same team snaps next, the next snap's start is the play's end.

Candidates: every published ``cfbfastR-cfb-data`` 2019-2026 "Fumble Recovery (Own)" row with
``end.yardsToEndzone >= 95`` and ``EPA < -2.5`` (the auditor's signature; 23 rows on 2026-10-12, 16 when
fp-check counted). Fixtures: ``fixtures/supp002/`` (see its README) plus the committed
``fixtures/summary_401856682.json``; every game runs with the one closing line ``ODDS``.

Oracle for the four ``MOVED`` rows: the ESPN feed's next real snap (id, down, distance, yards to the end
zone, read from the payload, not from the processor), which the play text confirms ("to the UM18",
"to the ULM24", "to the OSU01", "to the NDSU47"). Their EPA is held within 0.1 of the processor's own EP
at that snap minus ``EP_start``, and that snap's ``EP_between`` within 0.1 of 0 (the ball did not change
hands between the two rows). Observed residuals: EPA 0.0 on three rows, 0.084 on 401864518671 (the clock
runs between its end and the punt); never raise the tolerance to pass a new row. Not on 401864518681: a
UNLV Timeout row filed under the timeout team sits between the two NDSU rows with ``change_of_pos_team``
True, so the punt's ``EP_between`` reads a turnover (-1.34; -2.61 before the fix), a separate finding.

``BASE`` is NOT an oracle: it is each candidate's (end.down, end.distance, end.yardsToEndzone, EPA) on
origin/devel-cfb ``87568090f1`` before the fix, a byte-stability pin so that only the four rows move.
Several pinned values are wrong for other reasons (the reason column); a fix for one of those updates
its pin in the same PR.
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"
ODDS = {"gameSpread": 1.5, "overUnder": 50.5, "homeFavorite": True, "gameSpreadAvailable": True}
#: the fixed EPA within 0.1 of the next-snap value (addendum W-M6), and that snap's EP_between within 0.1 of 0
EPA_ABS_TOL = 0.1
#: an unmoved row's EPA: the pins are xgboost float32 outputs; allow a last-bit difference across builds
PIN_ABS_TOL = 1e-5

#: play id -> (game id, end.down, end.distance, end.yardsToEndzone, EPA, why it is a candidate)
BASE = {
    401114225102936506: (401114225, 1, 10, 99, -4.130707263946533, "turnover on downs at the 1"),
    401110791103939502: (401110791, 3, 34, 97, -2.560042142868042, "deep loss, next snap at 97"),
    401119271104978611: (401119271, 1, 10, 99, -3.4576613903045654, "fumble lost, game end"),
    401110854101849903: (401110854, 3, 30, 95, -2.775333881378174, "deep loss, next snap at 95"),
    401132973103899601: (401132973, 4, 27, 99, -2.846648693084717, "deep loss, next snap at 99"),
    401234628102988509: (401234628, 2, 19, 99, -5.905187606811523, "next snap is the other team's kickoff"),
    401309874102874101: (401309874, 3, 32, 95, -2.980499505996704, "deep loss, next snap at 95"),
    401403989103959603: (401403989, 2, 36, 95, -2.768186330795288, "deep loss, next snap at 95"),
    401403973104987001: (401403973, 1, 10, 99, -2.9093503952026367, "fumble lost on 4th & 2 at the 2"),
    401405100103966402: (401405100, 3, 28, 97, -3.1009416580200195, "deep loss, next snap at 97"),
    401405126102999901: (401405126, 3, 13, 99, -2.748067855834961, "half end (Q2 0:00)"),
    401520162103998001: (401520162, 1, 10, 97, -3.913547992706299, "fumble lost on 4th down"),
    401520150103854906: (401520150, 4, 17, 98, -3.425830841064453, "deep loss, next snap at 98"),
    401636874105282435: (401636874, 1, 10, 99, -3.335646152496338, "game end in overtime"),
    401752881299: (401752881, 4, 17, 99, -4.281498908996582, "MOVED"),
    401761641253: (401761641, 2, 1, 99, -3.143930435180664, "MOVED"),
    401754589227: (401754589, 1, 10, 95, -4.553383827209473, "turnover on downs at the 1"),
    4017527863: (401752786, -1, 10, 97, -3.4757142066955566, "opening kickoff, fumble lost"),
    401754614809: (401754614, 4, 14, 99, -4.608710765838623, "game end"),
    401856682824: (401856682, 0, 1, 99, -5.9057087898254395, "MOVED"),
    401856785507: (401856785, 2, 13, 99, -2.5574681758880615, "half end, next snap is a kickoff"),
    401856797637: (401856797, 2, 1, 99, -4.6233367919921875, "mirrored end {yardLine 1, ytez 99}, not degenerate"),
    401864518671: (401864518, 0, 20, 99, -2.7226810455322266, "MOVED"),
}
#: play id -> the ESPN feed's next real snap: (id, start.down, start.distance, start.yardsToEndzone)
MOVED = {
    401752881299: (401752881305, 4, 17, 82),  # Punt; "rush ... for 16 yards loss to the UM18"
    401761641253: (401761641259, 2, 1, 24),  # Pass Incompletion; "rush ... for 9 yards gain to the ULM24"
    401856682824: (401856682831, 3, 1, 1),  # Rushing Touchdown; "rush ... for 0 yards to the OSU01"
    401864518671: (401864518681, 4, 20, 53),  # Punt; "sacked for loss of 6 yards to the NDSU47"
}
GAMES = sorted({v[0] for v in BASE.values()})
assert {pid for pid, v in BASE.items() if v[-1] == "MOVED"} == set(MOVED)

pytestmark = [pytest.mark.whole_game]


def _summary(game_id: int) -> dict:
    trimmed = FIX / "supp002" / f"{game_id}_trimmed.json.gz"
    if trimmed.exists():
        with gzip.open(trimmed, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads((FIX / f"summary_{game_id}.json").read_text(encoding="utf-8"))


@functools.cache
def _frame(game_id: int) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id, join_participants=False, odds_override=ODDS)
    proc.espn_cfb_pbp(summary=_summary(game_id))
    proc.run_processing_pipeline()
    return proc.plays_frame.with_columns(pl.col("id").cast(pl.Int64))


def _row(game_id: int, play_id: int) -> dict:
    hit = _frame(game_id).filter(pl.col("id") == play_id)
    assert hit.height == 1, (play_id, hit.height)
    return hit.row(0, named=True)


def _moved(pid: int, row: dict) -> bool:
    down, dist, ytez, epa = BASE[pid][1:5]
    return (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) != (down, dist, ytez) or (
        abs(row["EPA"] - epa) > PIN_ABS_TOL
    )


@pytest.mark.parametrize("game_id", [pytest.param(g, marks=pytest.mark.xdist_group("cfb-supp002")) for g in GAMES])
def test_moved_rows_take_the_next_snap_and_the_rest_are_byte_stable(game_id):
    for pid in (p for p, v in BASE.items() if v[0] == game_id):
        row = _row(game_id, pid)
        if pid not in MOVED:
            assert not _moved(pid, row), f"{pid} ({BASE[pid][-1]}) moved: {row['end.down']} {row['EPA']}"
            continue
        nid, down, dist, ytez = MOVED[pid]
        assert (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) == (down, dist, ytez), pid
        assert row["change_of_pos_team"] is False, pid
        nxt = _row(game_id, nid)
        oracle = nxt["EP_start"] - row["EP_start"]
        assert abs(row["EPA"] - oracle) <= EPA_ABS_TOL, f"{pid}: EPA {row['EPA']} vs next-snap {oracle}"
        if nid != 401864518681:  # the Timeout-row finding in the module docstring
            assert abs(nxt["EP_between"]) <= EPA_ABS_TOL, f"{nid}: EP_between {nxt['EP_between']}"


@pytest.mark.xdist_group("cfb-supp002")
def test_exactly_the_four_named_rows_move():
    moved = {pid for pid, v in BASE.items() if _moved(pid, _row(v[0], pid))}
    assert moved == set(MOVED), f"moved {sorted(moved)}"
    assert 401856797637 not in moved  # the mirrored-ytez row is a different defect
