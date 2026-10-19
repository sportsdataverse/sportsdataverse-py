"""EX-3.4-b (quality program task 3.4(a), PR #834): a Timeout row is no change of possession.

ESPN files a Timeout row under the team that CALLED it, with the start state of the play before it. The
processor read a defensive timeout as a turnover: on the Timeout row (``change_of_pos_team`` True), on
the play before it, and through the lag on the play after it, whose ``EP_between`` (and so a penalty
row's EPA) took a possession flip that never happened. Four parts, each with its own registry row:

* the Timeout row's own ``change_of_pos_team`` is False;
* the lagged possession change the next play reads is the last real play's (punt, the receiver's
  timeout, the receiver's snap is still a change of possession);
* the lagged score is the last real play's (score, timeout, kickoff: the kickoff's ``EP_between`` is 0);
* a play's ``wp_after`` reads the next real row's board, not the timeout's stale one.

Oracle: the feed's own possession sequence. A play's ``EP_between`` measures the unexplained jump since
the previous real play ended, so where the ball stayed with one team (or a score explains the jump)
it is ~0; the four penalty rows below are the 4 of the 128 committed captures whose EPA moved (by up
to 6.2 points), pinned at their corrected values. Inputs: committed captures (``fixtures/README.md``),
processed without an odds override (EP and EPA do not read odds).
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
#: |EP_between| where nothing unexplained happened between two real plays
EP_BETWEEN_ABS_TOL = 0.1
#: the same on the four penalty rows, whose EP_between also carries the clock run off between the
#: two snaps: observed 0.0006, 0.258, 0.053, 0.001 (before the fix 2.83, 4.95, 6.20, 4.71)
PENALTY_EP_BETWEEN_ABS_TOL = 0.3
#: a pinned EPA: the EP model is xgboost float32
PIN_ABS_TOL = 1e-4

#: penalty row -> (game, the defensive Timeout row before it, EPA before the fix, EPA now)
PENALTY_ROWS = {
    401752753727: (401752753, 401752753725, 1.8086144924163818, -1.0228596925735474),
    401754571404: (401754571, 401754571400, 3.811105728149414, -1.3962856531143188),
    401756960804: (401756960, 401756960785, 3.877885341644287, -2.3713788986206055),
    40176165681: (401761656, 40176165673, 2.8148951530456543, -1.9001678228378296),
}

# 401752886 (a supp002 capture) warns PlayOrderWarning: a period label steps back along the feed's play order.
# The warning itself is asserted by test_cfb_whole_game_oracle.py::test_period_monotonic_or_the_step_back_warns.
pytestmark = [
    pytest.mark.whole_game,
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning"),
    pytest.mark.filterwarnings(r"ignore:CFB game 401752886\b:sportsdataverse.errors.PlayOrderWarning"),
]


def _summary(game_id: int) -> dict:
    for path in (
        FIX / f"summary_{game_id}.json",
        FIX / f"summary_{game_id}_trimmed.json.gz",
        FIX / "espn_reference" / f"summary_{game_id}.json.gz",
        FIX / "supp002" / f"{game_id}_trimmed.json.gz",
    ):
        if path.exists():
            if path.suffix == ".gz":
                with gzip.open(path, "rt", encoding="utf-8") as fh:
                    return json.load(fh)
            return json.loads(path.read_text(encoding="utf-8"))
    raise FileNotFoundError(game_id)


@functools.cache
def _frame(game_id: int) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=_summary(game_id))
    proc.run_processing_pipeline()
    return proc.plays_frame.with_columns(pl.col("id").cast(pl.Int64))


def _row(game_id: int, play_id: int) -> dict:
    hit = _frame(game_id).filter(pl.col("id") == play_id)
    assert hit.height == 1, (play_id, hit.height)
    return hit.row(0, named=True)


@pytest.mark.parametrize(
    "play_id", [pytest.param(p, marks=pytest.mark.xdist_group(f"cfb-timeout-{v[0]}")) for p, v in PENALTY_ROWS.items()]
)
def test_penalty_after_a_defensive_timeout_reads_no_turnover(play_id):
    game_id, timeout_id, _before, now = PENALTY_ROWS[play_id]
    timeout = _row(game_id, timeout_id)
    assert timeout["type.text"] == "Timeout" and timeout["change_of_pos_team"] is False, timeout_id
    row = _row(game_id, play_id)
    assert abs(row["EP_between"]) <= PENALTY_EP_BETWEEN_ABS_TOL, f"{play_id}: EP_between {row['EP_between']}"
    assert abs(row["EPA"] - now) <= PIN_ABS_TOL, f"{play_id}: EPA {row['EPA']} vs {now}"


@pytest.mark.xdist_group("cfb-timeout-401628559")
def test_punt_then_receiver_timeout_then_snap_is_a_change_of_possession():
    # 401628559: Punt (213) 101939701, Timeout (135) 101944101, Rush (135) 101944103
    punt, timeout, snap = (_row(401628559, i) for i in (401628559101939701, 401628559101944101, 401628559101944103))
    assert punt["start.pos_team.id"] != snap["start.pos_team.id"] == timeout["start.pos_team.id"]
    assert punt["change_of_pos_team"] is True and timeout["change_of_pos_team"] is False
    assert snap["lag_change_of_pos_team"] is True
    assert abs(snap["EP_between"]) <= EP_BETWEEN_ABS_TOL, snap["EP_between"]  # was 2.50


@pytest.mark.xdist_group("cfb-timeout-401643775")
def test_score_then_timeout_then_kickoff_reads_the_score():
    # 401643775: Passing Touchdown 102875601, Timeout 102875604, Kickoff 102875605
    td, kick = _row(401643775, 401643775102875601), _row(401643775, 401643775102875605)
    assert td["scoringPlay"] is True
    assert kick["EP_between"] == 0.0, kick["EP_between"]  # was -5.54


@pytest.mark.xdist_group("cfb-timeout-401856679")
def test_wp_after_reads_the_next_real_board_across_a_timeout():
    # 401856679: OU's pass intercepted (595), OU's timeout (597), Michigan's snap (605)
    pick, snap = _row(401856679, 401856679595), _row(401856679, 401856679605)
    assert pick["start.pos_team.id"] != snap["start.pos_team.id"]
    board = 1 - snap["wp_before"]  # Michigan's board, stated for OU
    assert abs(pick["wp_after"] - board) <= 1e-6, (pick["wp_after"], board)  # was 0.41 / 0.52 vs 0.37
    # lead_wp_before2 is the second real row after it, skipping the timeout (shift(-2) read 605 itself)
    frame = _frame(401856679)
    real = frame.filter(pl.col("type.text") != "Timeout")
    i = real["id"].to_list().index(401856679595)
    second = real.row(i + 2, named=True)
    assert pick["lead_wp_before2"] == second["wp_before"], (pick["lead_wp_before2"], second["id"], second["wp_before"])


#: game -> (defensive Timeout rows: the caller is neither side of the plays around it, every other Timeout row
#: with a known caller and real plays on both sides), counted
DEFENSIVE_TIMEOUTS = {401754571: (1, 3), 401756960: (3, 6), 401864518: (3, 7), 401752886: (5, 1)}


@pytest.mark.parametrize(
    "game_id", [pytest.param(g, marks=pytest.mark.xdist_group(f"cfb-timeout-{g}")) for g in DEFENSIVE_TIMEOUTS]
)
def test_defensive_timeout_row_keeps_the_offense_as_pos_team(game_id):
    """R834-13 (owner ruling 2026-10-18, nflfastR's posteam): a defensive timeout is the offense's row.

    Oracle: the raw feed's team on each row. ESPN files a Timeout under the team that called it; when the
    real plays on either side of it (the processor's order, same half, no score or kickoff between) are the
    other team's, the timeout was called by the defense. On that row ``pos_team`` is the offense, its EP the play before's end
    (EX-3.4-b), its EPA 0, and its score margin the offense's, so ``wp_before`` (margin + EP) reads one
    team's perspective. Before the ruling the row read as the caller's possession with the offense's EP
    (401864518 "Timeout UNLV" in NDSU's drive: wp_before 0.76 for UNLV, from NDSU's EP).
    """
    summary = _summary(game_id)
    raw = {
        int(p["id"]): int(p["start"]["team"]["id"])
        for d in (summary.get("drives") or {}).get("previous", [])
        for p in d.get("plays", [])
        if ((p.get("start") or {}).get("team") or {}).get("id")
    }
    frame = _frame(game_id)
    admin = frame["type.text"].str.contains(r"(?i)^(?:timeout|end period|end of)")
    real = frame.filter(admin == False)  # noqa: E712
    found = kept = 0
    for t in frame.filter(pl.col("type.text") == "Timeout").iter_rows(named=True):
        before = real.filter(pl.col("game_play_number") < t["game_play_number"]).tail(1)
        after = real.filter(pl.col("game_play_number") > t["game_play_number"]).head(1)
        if before.height == 0 or after.height == 0 or {before["half"][0], after["half"][0]} != {t["half"]}:
            continue
        caller, prev_team, next_team = raw.get(t["id"]), raw.get(before["id"][0]), raw.get(after["id"][0])
        # a score or a kickoff beside it: whose possession the timeout is in is not one team's
        if None in (caller, prev_team) or before["scoringPlay"][0] or "kickoff" in after["type.text"][0].lower():
            continue
        if prev_team != next_team or caller == prev_team:
            # the offense's own timeout, or one between two possessions: filed under its caller
            kept += 1
            # EX-3.4-b: a Timeout row never carries a possession change, whoever called it
            assert t["pos_team"] == caller and t["change_of_pos_team"] is False, (t["id"], t["pos_team"], caller)
            continue
        found += 1
        prev = before.row(0, named=True)
        assert t["pos_team"] == prev_team and t["def_pos_team"] == caller, (t["id"], t["pos_team"], caller)
        assert t["EP_start"] == prev["EP_end"] and t["EPA"] == 0, (t["id"], t["EP_start"], prev["EP_end"])
        assert t["pos_score_diff_start"] == prev["pos_score_diff_start"], (t["id"], t["pos_score_diff_start"])
    assert (found, kept) == DEFENSIVE_TIMEOUTS[game_id], (found, kept)
