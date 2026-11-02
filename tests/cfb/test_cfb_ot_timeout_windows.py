"""Overtime timeout windows (NCAA Rule 3-1-3-h): one timeout per team per overtime period through 2024; from 2025
(rules change of 2025-04-16) one for the first, one for the second, and one from the third overtime until the end.

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

401866418 (Jacksonville State @ Ohio, 2026 season, 4OT; the whole-game capture at the fixtures root): the third and
fourth overtimes share one allotment.
"""

from __future__ import annotations

import functools
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


def _summary(game_id: int, folder: str = "ot_timeouts") -> dict:
    path = (FIX / folder if folder else FIX) / f"summary_{game_id}_trimmed.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


@functools.cache
def _run(game_id: int) -> CFBPlayProcess:
    return _process(game_id, _summary(game_id))


def _process(game_id: int, summary: dict) -> CFBPlayProcess:
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
    # San Diego State's OT2 snaps before its own timeout (...794, wall clock 00:01:43), in either row order
    for play in (401760426755, 401760426762, 401760426788):
        assert got[play] == (0, 1, 0, 1)
    assert got[401760426794] == (0, 1, 0, 0)  # Timeout San Diego State


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="row-order defect: the processor files ...792 (wall clock 00:03:27) before San Diego State's "
    "timeout ...794 (00:01:43), so it still reads one San Diego State timeout left",
)
@pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")
def test_snap_after_a_timeout_reads_it_used() -> None:
    assert _states(_run(401760426))[401760426792] == (0, 0, 0, 0)


def _ot_rows(proc: CFBPlayProcess, period: int) -> list[tuple[int, int, int, int]]:
    return [
        v for k, v in _states(proc).items() if proc.plays_frame.filter(pl.col("id") == k)["period.number"][0] == period
    ]


@pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")
def test_third_and_later_overtimes_share_one_timeout_from_2025() -> None:
    proc = _process(401866418, _summary(401866418, folder=""))
    assert proc.json["season"]["year"] >= 2025
    # no timeout in OT3 or OT4: both teams hold the single OT3-onward timeout throughout
    for period in (7, 8):
        assert set(_ot_rows(proc, period)) == {(1, 1, 1, 1)}


def _ohio_timeout_at_end_of_ot3(summary: dict, season: int) -> dict:
    """RULE TEST, NOT REAL DATA: OT3's "End of OT." row (...871) becomes "Timeout Ohio" after Ohio's OT3 try.

    No real game in the corpus spends an OT3 timeout and plays on into OT4, so one is made here.
    """
    edited = json.loads(json.dumps(summary))
    edited["header"]["season"]["year"] = season
    for drive in edited["drives"]["previous"]:
        for play in drive["plays"]:
            if play["id"] == "401866418871":
                play["type"] = {"id": "21", "text": "Timeout", "abbreviation": "TO"}
                play["text"] = "Timeout Ohio, clock 00:00"
                play["sequenceNumber"] = "174"  # a timeout shares the sequence number of the play before it
    return edited


@pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")
@pytest.mark.parametrize(("season", "ohio_left_in_ot4"), [(2026, 0), (2024, 1)])
def test_ot3_timeout_carries_into_ot4_from_2025(season: int, ohio_left_in_ot4: int) -> None:
    proc = _process(401866418, _ohio_timeout_at_end_of_ot3(_summary(401866418, folder=""), season))
    assert _states(proc)[401866418871] == (1, 1, 0, 1)  # Ohio (home) spends its timeout
    ot4 = _ot_rows(proc, 8)
    assert ot4 and {row[0] for row in ot4} == {ohio_left_in_ot4}
    assert {row[1] for row in ot4} == {1}
