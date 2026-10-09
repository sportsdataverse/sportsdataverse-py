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
