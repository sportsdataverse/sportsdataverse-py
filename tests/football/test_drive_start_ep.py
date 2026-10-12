"""The drives box's ``avg_start_ep``: drive starts in EP units (offline, real fixtures).

Fixtures: ``cfb/fixtures/summary_401856682.json`` (Ohio State @ Texas, 2025) and
``nfl/fixtures/summary_401872922.json`` (Jacksonville vs Indianapolis). The expected
values are recomputed here in plain Python from the processed plays: each offense's
drives, valued at the drive's first scrimmage snap of the game on the league's
field-position curve, averaged per team.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_pbp as nfl_mod
from sportsdataverse.cfb.cfb_field_position import load_fp_curve
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from sportsdataverse.nfl.nfl_field_position import load_nfl_fp_curve

TESTS = Path(__file__).parent.parent


def _cfb(gid: int) -> CFBPlayProcess:
    summary = json.loads((TESTS / "cfb" / "fixtures" / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid, join_participants=False)
        proc.espn_cfb_pbp()
        proc.run_processing_pipeline()
    return proc


def _nfl(gid: int):
    summary = json.loads((TESTS / "nfl" / "fixtures" / f"summary_{gid}.json").read_text(encoding="utf-8"))
    proc = nfl_mod.NFLPlayProcess(gameId=gid, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc


@pytest.fixture(scope="module")
def games():
    return {"cfb": (_cfb(401856682), load_fp_curve()), "nfl": (_nfl(401872922), load_nfl_fp_curve())}


def _expected(frame: pl.DataFrame, curve: pl.DataFrame, periods=None) -> dict[int, float]:
    ep = dict(zip(curve["yardline_own"].to_list(), curve["ep"].to_list()))
    first, in_window = {}, set()
    cols = ["scrimmage_play", "pos_team", "drive.id", "start.yardsToEndzone", "period"]
    for r in frame.select(cols).iter_rows(named=True):
        if r["scrimmage_play"] is not True or r["drive.id"] is None or r["pos_team"] is None:
            continue
        key = (int(r["pos_team"]), r["drive.id"])
        first.setdefault(key, r["start.yardsToEndzone"])
        if periods is None or r["period"] in periods:
            in_window.add(key)
    by_team: dict[int, list[float]] = {}
    for team, drive in in_window:
        yte = first[(team, drive)]
        if yte is not None:
            by_team.setdefault(team, []).append(ep[min(max(int(100 - yte), 1), 99)])
    return {t: sum(v) / len(v) for t, v in by_team.items()}


def _box_ep(box: dict) -> dict[int, float]:
    return {int(r["pos_team"]): r["avg_start_ep"] for r in box["drives"]}


@pytest.mark.parametrize("league", ["cfb", "nfl"])
def test_whole_game_drive_start_ep(games, league):
    proc, curve = games[league]
    got = _box_ep(proc.create_box_score(proc.plays_frame))
    want = _expected(proc.plays_frame, curve)
    assert got.keys() == want.keys() and len(got) == 2
    for team, value in want.items():
        assert got[team] == pytest.approx(value, abs=1e-9)


def test_pinned_values_ohio_state_texas(games):
    # guards the definition and the bundled curve: Texas (251) 1.745, Ohio State (194) 2.243
    proc, _ = games["cfb"]
    got = _box_ep(proc.create_box_score(proc.plays_frame))
    assert got[251] == pytest.approx(1.74536, abs=1e-4)
    assert got[194] == pytest.approx(2.24273, abs=1e-4)


@pytest.mark.parametrize(("league", "periods"), [("cfb", {2}), ("cfb", {4}), ("nfl", {4}), ("cfb", {3, 4})])
def test_window_values_each_drive_at_its_real_start(games, league, periods):
    proc, curve = games[league]
    frame = proc.plays_frame
    sliced = frame.filter(pl.col("period").is_in(sorted(periods)))
    got = _box_ep(proc.create_box_score(sliced))
    want = _expected(frame, curve, periods)
    assert got.keys() == want.keys()
    for team, value in want.items():
        assert got[team] == pytest.approx(value, abs=1e-9)


@pytest.mark.parametrize(("league", "periods"), [("cfb", {2}), ("cfb", {4}), ("nfl", {4})])
def test_window_fixture_has_a_carried_over_drive(games, league, periods):
    # the window test above only discriminates if some drive started in the previous
    # period: valued at its first snap IN the window, a team's mean moves
    proc, curve = games[league]
    sliced = proc.plays_frame.filter(pl.col("period").is_in(sorted(periods)))
    naive = _expected(sliced, curve)
    real = _expected(proc.plays_frame, curve, periods)
    assert any(abs(naive[t] - real[t]) > 1e-6 for t in real)
