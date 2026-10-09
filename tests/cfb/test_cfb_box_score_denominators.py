"""create_box_score rates and counts on their own denominators (offline).

Fixtures: ``summary_401754598.json`` (Florida State @ NC State, 2025-11-21) and
``summary_401856682.json`` + ``participants_401856682.json`` (Ohio State @ Texas).
``download`` is patched to return the summary, so nothing hits the network.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"
FSU, NCSU = 52, 152


def _run(gid: int, participants=None):
    summary = json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid, participants=participants, join_participants=False)
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    return out["advBoxScore"], pl.from_dicts(out["plays"], infer_schema_length=None)


@pytest.fixture(scope="module")
def fsu_ncsu():
    return _run(401754598)


def _by(rows: list[dict], key: str) -> dict:
    return {r[key]: r for r in rows}


def test_late_down_pass_rush_success_rates_use_their_own_plays(fsu_ncsu):
    # E1: the pass / rush rates were means over EVERY late-down play, so they summed
    # to the overall rate (NC State 2/18 passes instead of 2/9).
    box, _ = fsu_ncsu
    for r in box["situational"]:
        assert r["EPA_success_late_down_pass_rate"] == pytest.approx(
            r["EPA_success_late_down_pass"] / r["late_down_pass"]
        )
        assert r["EPA_success_late_down_rush_rate"] == pytest.approx(
            r["EPA_success_late_down_rush"] / r["late_down_rush"]
        )
    assert _by(box["situational"], "pos_team")[NCSU]["EPA_success_late_down_pass_rate"] == pytest.approx(2 / 9)


def test_drive_metrics_are_per_drive(fsu_ncsu):
    # E2: drive columns were averaged over scrimmage PLAYS, weighting each drive by its
    # length. These match ESPN's drives grouping (cfb.drives) for this game.
    box, _ = fsu_ncsu
    drives = _by(box["drives"], "pos_team")
    assert (drives[FSU]["drives"], drives[FSU]["drive_total_gained_yards"]) == (9, 368)
    assert (drives[NCSU]["drives"], drives[NCSU]["drive_total_gained_yards"]) == (11, 252)
    assert drives[FSU]["plays_per_drive"] == pytest.approx(69 / 9)
    assert drives[NCSU]["yards_per_drive"] == pytest.approx(252 / 11)
    for r in drives.values():
        assert 0 <= r["drive_total_gained_yards_rate"] <= 100
    # FSU's defense stopped 7 of NC State's 11 drives (5 punts, 2 turnovers on downs)
    assert _by(box["defensive"], "def_pos_team")[FSU]["drive_stopped_rate"] == pytest.approx(100 * 7 / 11, abs=0.01)


def test_interception_drives_count_as_stopped(fsu_ncsu):
    # ESPN's drive result is "INT" / "INT TD"; the stop pattern looked for "interception".
    _, plays = fsu_ncsu
    ints = plays.filter(pl.col("drive.result").is_in(["INT", "INT TD"]))
    assert ints.height > 0
    assert ints["drive_stopped"].all()


def test_xcomp_sums_completion_probability_over_attempts_only(fsu_ncsu):
    # E5: xComp summed cp over dropbacks INCLUDING sacks but divided by Att, which
    # excludes them; a sacked passer's xCompPct ran high (and past 1.0 in 22 games of 2025).
    box, plays = fsu_ncsu
    bailey = next(r for r in box["pass"] if r["passer_player_name"] == "C.Bailey")
    assert bailey["Sck"] == 4
    attempts = plays.filter(
        (pl.col("passer_player_name") == "C.Bailey")
        & (pl.col("pass_attempt") == True)
        & (pl.col("scrimmage_play") == True)
    )
    assert attempts.height == bailey["Att"]
    assert bailey["xComp"] == pytest.approx(attempts["cp_game_state"].sum(), abs=0.01)
    assert bailey["xCompPct"] == pytest.approx(bailey["xComp"] / bailey["Att"], abs=0.01)


def test_total_fumbles_counts_the_same_plays_as_fumbles_lost(fsu_ncsu):
    # E9: total_fumbles was scrimmage-only and keyed by pos_team, while fumbles_lost
    # counts special teams too. FSU muffed one punt and fumbled a punt return: ESPN's
    # box has FSU 2 fumbles / 2 lost; the box said 0 fumbles, and charged NC State 2.
    box, _ = fsu_ncsu
    to = _by(box["turnover"], "pos_team")
    assert (to[FSU]["total_fumbles"], to[FSU]["fumbles_lost"]) == (2, 2)
    for r in to.values():
        assert r["total_fumbles"] >= r["fumbles_lost_pbp"]
        assert r["total_fumbles"] >= r["fumbles_recovered"]
