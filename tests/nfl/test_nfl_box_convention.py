"""NFL box-score totals follow nflverse / ESPN's box: kneels are carries, Yards leaves out INT returns, net pass yards.

``create_box_score``'s rusher lines (``Car`` / ``Yds`` / ``YPC``) and the team row's
``rushing_attempts`` / ``rushing_yards`` count a kneel-down as the kneeler's carry; the EPA columns
and the team ``rushes`` / ``rush_yards`` stay on designed rushes. ``off_yards`` /
``total_off_yards`` / ``total_yards`` zero an interception's statYardage (the return, the
defense's yards), as CFB does. ``pass_yards_net`` is gross passing yards less the sack yards,
ESPN's ``netPassingYards``.

Fixtures (``fixtures/box_convention/``, trimmed as ``fixtures/README.md`` describes), captured
2026-11-19 from ESPN's site summary, 2026 week 10:

* 401873053 NE @ DET -- R.McCreary's 22-yard return of T.DeVito's interception; four kneels.
* 401873059 MIA @ IND -- Daniel Jones' two kneels.
* 401873060 MIN @ GB -- GB 223 gross, 164 net passing.
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures" / "box_convention"
GAMES = (401873053, 401873059, 401873060)
#: Team-games whose ESPN rushing attempts include an aborted snap the parser does not book as a
#: rush (J.Love, D.Jones) or that the parser credits one rush ESPN does not (NE: K.Williams).
ATTEMPT_RESIDUALS = {(401873053, 17), (401873059, 11), (401873060, 9)}


@functools.cache
def _process(gid: int):
    with gzip.open(FIX / f"summary_{gid}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = NFLPlayProcess(gameId=gid, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    box = proc.run_processing_pipeline()["advBoxScore"]
    espn = {}
    for team in summary["boxscore"]["teams"]:
        stats = {s["name"]: s["displayValue"] for s in team["statistics"]}
        espn[int(team["team"]["id"])] = stats
    return proc.plays_frame, box, espn


@pytest.mark.parametrize("gid", GAMES)
def test_team_rushing_totals_count_kneels(gid):
    plays, box, espn = _process(gid)
    for row in box["team"]:
        tid = int(row["pos_team"])
        assert row["rushing_yards"] == int(espn[tid]["rushingYards"]), tid
        kneels = plays.filter((pl.col("kneel_down") == True) & (pl.col("pos_team") == tid)).height  # noqa: E712
        assert row["rushing_attempts"] == row["rushes"] + kneels
        if (gid, tid) not in ATTEMPT_RESIDUALS:
            assert row["rushing_attempts"] == int(espn[tid]["rushingAttempts"]), tid


def test_kneels_are_the_kneelers_carries():
    _plays, box, _espn = _process(401873059)
    jones = next(r for r in box["rush"] if r["rusher_player_name"] == "D.Jones")
    # two designed carries (2 and 10 yards) and two kneels (-2, -1)
    assert (jones["Car"], jones["Yds"]) == (4, 9.0)
    assert jones["YPC"] == pytest.approx(9 / 4)


@pytest.mark.parametrize("gid", GAMES)
def test_epa_columns_stay_on_designed_rushes(gid):
    plays, box, _espn = _process(gid)
    designed = plays.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))  # noqa: E712
    team = {int(r["pos_team"]): r for r in box["team"]}
    for tid, n, epa in designed.group_by("pos_team").agg(pl.len(), pl.col("EPA").sum()).iter_rows():
        assert team[int(tid)]["rushes"] == n
        assert team[int(tid)]["EPA_rushing_overall"] == pytest.approx(epa, abs=0.06)
    lines = {(int(r["pos_team"]), r["rusher_player_name"]): r for r in box["rush"]}
    for tid, name, epa in designed.group_by("pos_team", "rusher_player_name").agg(pl.col("EPA").sum()).iter_rows():
        assert lines[(int(tid), name)]["EPA"] == pytest.approx(epa, abs=0.06)


def test_off_yards_leave_out_the_interception_return():
    plays, box, _espn = _process(401873053)
    team = {int(r["pos_team"]): r for r in box["team"]}
    ints = plays.filter((pl.col("int") == True) & (pl.col("scrimmage_play") == True))  # noqa: E712
    # an intercepted pass's statYardage is its return (R.McCreary's 22, DET 36 to NE 42, on NE's row)
    returned = ints.filter(pl.col("statYardage") != 0)
    assert returned.height >= 1
    for tid in team:
        scrimmage = plays.filter((pl.col("scrimmage_play") == True) & (pl.col("pos_team") == tid))  # noqa: E712
        offense = scrimmage.filter(pl.col("int") != True)["statYardage"].sum()  # noqa: E712
        assert team[tid]["off_yards"] == offense
        assert team[tid]["total_off_yards"] == offense
    # NE: 180 with DeVito's intercepted pass (returned 22 yards), 158 without
    assert team[17]["off_yards"] == 158


@pytest.mark.parametrize("gid", [401873053, 401873060])
def test_pass_yards_net_is_espn_net_passing(gid):
    _plays, box, espn = _process(gid)
    for row in box["team"]:
        tid = int(row["pos_team"])
        assert row["pass_yards_net"] == int(espn[tid]["netPassingYards"]), tid
        assert row["pass_yards_net"] == pytest.approx(row["pass_yards"] + row["sack_yards"])


def test_pass_yards_stay_gross():
    _plays, box, _espn = _process(401873060)
    gb = next(r for r in box["team"] if int(r["pos_team"]) == 9)
    assert (gb["pass_yards"], gb["pass_yards_net"]) == (223, 164)
