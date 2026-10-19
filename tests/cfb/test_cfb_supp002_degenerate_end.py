"""supp-002 (quality program task 3.4(a)): a degenerate ESPN end block takes the next snap's start.

ESPN ships some non-scoring fumble plays with ``end {yardLine 0, yardsToEndzone 0}``. The processor's
``<= 0`` clamp read that as the own 1, so a play that kept the ball scored a goal-line disaster
(401856682's Q4 Fumble Recovery (Own): EPA -5.91, the Yahoo and CBS feeds of the same play -0.39). The
rule: when the end block is degenerate, the play's text mentions a fumble, the ball was kept
(``change_of_pos_team`` False, right at the source since EX-3.4-b; owner ruling R834-7: possession, not
the fumble attribution) and the same team snaps next, the next snap's start is the play's end.

Reach: over every cfbfastR-cfb-raw game (20,719, raw repo ``98deb3f091``) the rule moves the 20 ``MOVED``
rows in 18 games (review PR-834; owner rulings: all ship): 6 Fumble Recovery (Own), 6 Rush and 8
Fumble rows, 2024-2026. Every one has a committed capture here, so the "exactly these rows move" test
covers the whole corpus reach.

Candidates (``BASE``): the 20 rule rows; every published ``cfbfastR-cfb-data`` 2019-2026 "Fumble Recovery
(Own)" row with ``end.yardsToEndzone >= 95`` and ``EPA < -2.5`` (the auditor's signature: 23 rows on
2026-10-12, 4 of them rule rows); and 401778310647, an interception-return fumble with a degenerate end
whose next snap is the other team's (R834-3; since R834-7 both its change_of_pos_team and the same-team
guard keep it from being scored from the wrong side). 401856674165, the retained-fumble companion record of 401856674175 that the recovery
attribution called lost until EX-3.4-d, moves only under the possession predicate (R834-7). Fixtures:
``fixtures/supp002/`` (see its README) plus ``fixtures/summary_401856682.json``; every game runs with
the one closing line ``ODDS``.

Oracle for the ``MOVED`` rows: the ESPN feed's next real snap (id, down, distance, yards to the end zone,
read from the payload, not from the processor). Their EPA is held within 0.1 of the processor's own EP
at that snap minus ``EP_start``, and that snap's ``EP_between`` within 0.1 of 0 (the ball did not change
hands between the two rows; on 401864518 and 401752886 a Timeout row filed under the defense sits in
between, the EX-3.4-b case). Observed residuals: 0.084 at most (401864518671, whose ESPN end clock is
EARLIER than its start clock); never raise the tolerance to pass a new row.

``BASE`` is NOT an oracle: it is each candidate's (end.down, end.distance, end.yardsToEndzone, EPA) on
origin/devel-cfb ``87568090f1`` before the fix, a byte-stability pin so that only the rule rows move.
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
    # mirrored end {yardLine 1, ytez 99}, not degenerate; EX-3.4-a re-pinned it (was 99, EPA -4.6233367919921875)
    401856797637: (401856797, 2, 1, 1, 0.4577484130859375, "mirrored end, fixed by EX-3.4-a"),
    401864518671: (401864518, 0, 20, 99, -2.7226810455322266, "MOVED"),
    401635627103987701: (401635627, 1, 10, 99, -2.3972766399383545, "MOVED"),
    401752791691: (401752791, 0, 10, 99, -3.0535194873809814, "MOVED"),
    401752886602: (401752886, 2, 2, 99, -6.467680931091309, "MOVED"),
    401754576752: (401754576, 2, 16, 99, -3.4405627250671387, "MOVED"),
    40175693428: (401756934, 2, 15, 99, -2.404973030090332, "MOVED"),
    401756934210: (401756934, 4, 23, 99, -3.7762556076049805, "MOVED"),
    401756935417: (401756935, 2, 14, 99, -0.5536208152770996, "MOVED"),
    401762443547: (401762443, 2, 19, 99, -3.4458048343658447, "MOVED"),
    401762443557: (401762443, 4, 18, 99, -3.6605772972106934, "MOVED"),
    401856814559: (401856814, 0, 10, 99, -6.0166730880737305, "MOVED"),
    401858248685: (401858248, 0, 10, 99, -3.6202306747436523, "MOVED"),
    401862703606: (401862703, 0, 10, 99, -1.704806923866272, "MOVED"),
    401862781639: (401862781, 0, 7, 99, -6.141966342926025, "MOVED"),
    401864432249: (401864432, 0, 11, 99, -1.1393542289733887, "MOVED"),
    401864443634: (401864443, 0, 24, 99, -0.25454163551330566, "MOVED"),
    401778310647: (401778310, 0, 9, 99, 5.10091495513916, "degenerate end, next snap is the other team's"),
    401856674165: (401856674, 0, 1, 99, -3.1948139667510986, "MOVED"),
    # kept fumbles whose end ESPN states (the text's spot) while the next snap is a 1st & 10: pinned on
    # origin/devel-cfb dd424de3e2 so the degenerate-end test has a killer once EX-3.4-a fixed 401856797637
    401752886558: (401752886, 3, 10, 53, -1.9471211433410645, "stated end, sacked to the WIS47"),
    40176244395: (401762443, 1, 3, 64, 0.18821382522583008, "stated end, rush to the TULANE36"),
}
#: play id -> the ESPN feed's next real snap: (id, start.down, start.distance, start.yardsToEndzone)
MOVED = {
    401752881299: (401752881305, 4, 17, 82),  # Fumble Recovery (Own) -> Punt; "16 yards loss to the UM18"
    401761641253: (401761641259, 2, 1, 24),  # Fumble Recovery (Own) -> Pass Inc; "to the ULM24"
    401856682824: (401856682831, 3, 1, 1),  # Fumble Recovery (Own) -> Rushing TD; "to the OSU01"
    401864518671: (401864518681, 4, 20, 53),  # Fumble Recovery (Own) -> Punt; "to the NDSU47" (Timeout between)
    401635627103987701: (401635627103989101, 1, 1, 1),  # Fumble Recovery (Own) -> Rush; "to the SMU 0"
    40175693428: (40175693434, 2, 16, 84),  # Fumble Recovery (Own) -> Pass Inc
    401752886602: (401752886609, 2, 2, 2),  # Rush -> Pass Reception (Timeout between; R834-2)
    401754576752: (401754576758, 2, 16, 67),  # Rush -> Pass Reception
    401756934210: (401756934227, 4, 23, 51),  # Rush -> Punt
    401756935417: (401756935424, 2, 14, 79),  # Rush -> Pass Completion
    401762443547: (401762443553, 2, 19, 68),  # Rush -> Rush
    401762443557: (401762443562, 4, 18, 67),  # Rush -> Punt
    # 401752791691 and 401864432249: ESPN files the next snap twice under one sequenceNumber (906, 289),
    # so the oracle is the next sequenceNumber ("2nd & 10 at VAN 30", "3rd & 18 at WKU 12")
    401752791691: (401752791703, 2, 10, 70),  # Fumble -> Pass Reception
    401856814559: (401856814567, 1, 10, 17),  # Fumble -> its own Fumble Recovery (Opponent) record
    401858248685: (401858248692, 1, 10, 62),  # Fumble -> its own Fumble Recovery (Opponent) record
    401862703606: (401862703615, 1, 10, 95),  # Fumble -> Penalty
    401862781639: (401862781647, 1, 7, 7),  # Fumble -> its own Fumble Recovery (Opponent) record
    401864432249: (401864432255, 3, 18, 88),  # Fumble -> Rush
    401864443634: (401864443642, 3, 24, 89),  # Fumble -> its own Fumble Recovery (Opponent) record
    401856674165: (401856674175, 4, 1, 24),  # Fumble -> its own companion record ("4th & 1 at ALA 24")
}
GAMES = sorted({v[0] for v in BASE.values()})
assert {pid for pid, v in BASE.items() if v[-1] == "MOVED"} == set(MOVED)

# 401752886 and 401864443 warn PlayOrderWarning (a period label steps back along the feed's play
# order); tests/cfb/test_cfb_pbp_offline.py::test_period_order_warning_on_a_mislabelled_period asserts it.
pytestmark = [
    pytest.mark.whole_game,
    pytest.mark.filterwarnings(r"ignore:CFB game (401752886|401864443)\b:sportsdataverse.errors.PlayOrderWarning"),
]


def _summary(game_id: int) -> dict:
    for trimmed in (FIX / "supp002" / f"{game_id}_trimmed.json.gz", FIX / f"summary_{game_id}_trimmed.json.gz"):
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
        assert abs(nxt["EP_between"]) <= EPA_ABS_TOL, f"{nid}: EP_between {nxt['EP_between']}"


@pytest.mark.xdist_group("cfb-supp002")
def test_exactly_the_rule_rows_move():
    moved = {pid for pid, v in BASE.items() if _moved(pid, _row(v[0], pid))}
    assert moved == set(MOVED), f"moved {sorted(moved)}"
    assert 401856797637 not in moved  # the mirrored-ytez row is EX-3.4-a's (tests/cfb/test_cfb_end_state_ex34.py)
    assert 401778310647 not in moved  # the next snap is the other team's (R834-3)


@pytest.mark.xdist_group("cfb-supp002")
def test_retained_fumble_companion_is_not_a_lost_fumble():
    """EX-3.4-d: ESPN's degenerate end block on 401856674165 names the defense (Alabama, 333) as its team.

    The recovery-less fumble rule read that team as a possession change and charged Kentucky (96) a lost
    fumble on a play it kept: the companion record 401856674175 ("for a 1ST down") and the next snap
    (401856674182, Kentucky 1st & 10) both say so. The pair is one fumble, retained.
    """
    for pid in (401856674165, 401856674175):
        row = _row(401856674, pid)
        assert row["fumbling_team"] == 96, pid
        got = (row["pos_fumble_lost"], row["def_fumble_lost"], row["is_turnover"], row["turnover_team"])
        assert got == (False, False, False, None), (pid, got)
    assert _row(401856674, 401856674182)["pos_team"] == 96


@pytest.mark.parametrize(
    ("game_id", "play_id"),
    [
        # fumble-return TD behind a degenerate end block: the "did not score" guard keeps it lost
        pytest.param(401858213, 401858213660, marks=pytest.mark.xdist_group("cfb-supp002")),
        # defensive recovery behind a degenerate end block, next snap the defense's: the
        # change_of_pos_team guard keeps it lost
        pytest.param(401752946, 401752946641, marks=pytest.mark.xdist_group("cfb-supp002")),
    ],
)
def test_lost_fumbles_behind_a_degenerate_end_stay_lost(game_id, play_id):
    """EX-3.4-d's clause un-loses only kept, non-scoring rows (review EX34d-1: each guard's killer)."""
    row = _row(game_id, play_id)
    assert row["recovery_team"] is None, play_id  # the recovery-less rule decides it
    assert (row["pos_fumble_lost"], row["is_turnover"], row["turnover_team"]) == (True, True, row["pos_team"]), play_id
