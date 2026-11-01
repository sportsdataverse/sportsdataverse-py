"""Overtime timeout windows: one timeout per team per overtime period (NCAA Rule 3-1-3-h).

#505: a feed that numbers every overtime 5 (401112081, Baylor @ TCU 2019, 3OT) still gets one timeout per OT.
Baylor's OT2 timeout (play ...234) and OT3 timeout (...255) shared one window, so OT3 read 0 left.

401754372 (California @ Virginia Tech 2025, 2OT): the frame's row order files Cal's OT1
touchdown (...864, drive ...36) between two Virginia Tech OT1 rows (...862 and ...871, drive ...37).
Counting every drive-id change paired three "possessions" into OT1, so Cal's OT2 possession opened a
phantom OT3: Virginia Tech's OT2 timeout (...918, "Timeout Virginia Tech", after its touchdown) was
forgotten and Cal's (...942) was charged to OT3.

401760426 (San Diego State @ New Mexico 2025, 2OT): ESPN files New Mexico's OT2 timeout (...747) under a
second-quarter drive that started at the 25, and that timeout row counted as a fourth possession, so San
Diego State's OT2 snaps read a fresh OT3 window (New Mexico 1 left, truth 0).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def test_each_overtime_allots_its_own_timeout() -> None:
    proc = CFBPlayProcess(gameId=401112081)
    proc.espn_cfb_pbp(summary=json.loads((FIX / "summary_401112081.json").read_text(encoding="utf-8")))
    proc.run_processing_pipeline()
    row = proc.plays_frame.filter(pl.col("id") == 401112081105000255).row(0, named=True)
    assert row["start.awayTeamTimeouts"] == 1
    baylor = proc.json["timeouts"][239]
    assert baylor["OT2"] == [401112081105000234] and baylor["OT3"] == [401112081105000255]


def _run(game_id: int) -> CFBPlayProcess:
    path = FIX / "ot_timeouts" / f"summary_{game_id}_trimmed.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc


def _states(proc: CFBPlayProcess) -> dict[int, tuple[int, int, int, int]]:
    """(start home, start away, end home, end away) timeouts left, per overtime play id."""
    cols = ["start.homeTeamTimeouts", "start.awayTeamTimeouts", "end.homeTeamTimeouts", "end.awayTeamTimeouts"]
    ot = proc.plays_frame.filter(pl.col("period.number") >= 5)
    return {r["id"]: tuple(r[c] for c in cols) for r in ot.iter_rows(named=True)}


# Processing a default-odds capture warns DefaultOddsWarning incidentally;
# tests/cfb/test_cfb_pbp_offline.py::test_default_odds_warn asserts it.
@pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")
def test_interleaved_overtime_drives_pair_once() -> None:
    proc = _run(401754372)
    vt, cal = proc.json["timeouts"][259], proc.json["timeouts"][25]
    assert (vt["OT2"], cal["OT2"]) == ([401754372918], [401754372942])
    assert not {"OT1", "OT3"} & (vt.keys() | cal.keys())
    got = _states(proc)
    # (start home, start away, end home, end away); Virginia Tech is home. OT1 (period 5) has no timeout.
    assert got[401754372909] == (1, 1, 1, 1)  # Virginia Tech's touchdown
    assert got[401754372918] == (1, 1, 0, 1)  # Timeout Virginia Tech
    for play in (401754372922, 401754372930, 401754372934):  # Cal's OT2 snaps: Virginia Tech has none left
        assert got[play] == (0, 1, 0, 1)
    assert got[401754372942] == (0, 1, 0, 0)  # Timeout California
    assert proc.plays_frame.filter(pl.col("period.number") == 5)["start.homeTeamTimeouts"].to_list() == [1, 1, 1]


@pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")
def test_timeout_row_filed_under_a_regulation_drive_opens_no_period() -> None:
    proc = _run(401760426)
    unm, sdsu = proc.json["timeouts"][167], proc.json["timeouts"][21]
    assert (unm["OT2"], sdsu["OT2"]) == ([401760426747], [401760426794])
    assert not {"OT1", "OT3"} & (unm.keys() | sdsu.keys())
    got = _states(proc)  # New Mexico is home
    assert got[401760426747] == (1, 1, 0, 1)  # Timeout New Mexico, after its OT2 touchdown
    for play in (401760426755, 401760426762, 401760426788, 401760426792):  # San Diego State's OT2 snaps
        assert got[play] == (0, 1, 0, 1)
    assert got[401760426794] == (0, 1, 0, 0)  # Timeout San Diego State
