"""#505: a feed that numbers every overtime 5 (401112081, Baylor @ TCU 2019, 3OT) still gets one timeout per OT.

Baylor's OT2 timeout (play ...234) and OT3 timeout (...255) shared one window, so OT3 read 0 left.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.cfb import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures" / "summary_401112081.json"


def test_each_overtime_allots_its_own_timeout() -> None:
    proc = CFBPlayProcess(gameId=401112081)
    proc.espn_cfb_pbp(summary=json.loads(FIX.read_text(encoding="utf-8")))
    proc.run_processing_pipeline()
    row = proc.plays_frame.filter(pl.col("id") == 401112081105000255).row(0, named=True)
    assert row["start.awayTeamTimeouts"] == 1
    baylor = proc.json["timeouts"][239]
    assert baylor["OT2"] == [401112081105000234] and baylor["OT3"] == [401112081105000255]
