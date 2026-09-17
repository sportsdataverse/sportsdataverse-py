"""CFBPlayProcess defects found while mapping the processor contract (football-sources program, S1-CFB).

Every case runs the real pipeline, offline, on a stored ESPN summary:

* ``summary_401856682.json`` -- Ohio State @ Texas, 2026 week 1 (vendor text).
* ``summary_401636889.json`` -- Iowa State vs Baylor, 2024 (classic text, a ``PAT MISSED`` TD).
* ``summary_401858435.json`` -- Indiana State @ Purdue, 2026 (a failed vendor kick, a
  two-word kicker surname, a two-man sack).
* ``summary_401858439.json`` -- Howard @ Indiana, 2026 (a returner written ``J.Washington lll``).
* ``summary_401868326.json`` -- Alabama State @ Troy, 2026 (one timeout logged twice).
* ``summary_401677179.json`` -- Indiana @ Notre Dame, 2024 ("Timeout Indiana" holds "nd").
* ``summary_401112081.json`` -- Baylor @ TCU, 2019 (triple overtime, every OT period numbered 5).

The 2026 summaries are copied verbatim from ``cfbfastR-cfb-raw/cfb/json/raw``.
"""

from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _summary(game_id: int) -> dict:
    return json.loads((FIX / f"summary_{game_id}.json").read_text())


@lru_cache(maxsize=None)
def _processed(game_id: int, blank_mascots: bool = False):
    summary = _summary(game_id)
    if blank_mascots:
        for comp in summary["header"]["competitions"][0]["competitors"]:
            comp["team"]["name"] = ""
    snapshot = copy.deepcopy(summary)
    proc = CFBPlayProcess(gameId=game_id)
    proc.espn_cfb_pbp(summary=summary)
    result = proc.run_processing_pipeline()
    return proc, result, summary, snapshot


def _plays(game_id: int, **kw) -> pl.DataFrame:
    return _processed(game_id, **kw)[0].plays_frame


def _row(plays: pl.DataFrame, needle: str) -> dict:
    hits = plays.filter(pl.col("text").str.contains(needle, literal=True))
    assert hits.height == 1, (needle, hits.height)
    return hits.row(0, named=True)


# --- C3: a missed / blocked / failed try scores the touchdown row at 6 -------------------------


def test_missed_pat_touchdown_ep_end_is_six():
    classic = _row(_plays(401636889), "(Kyle Konrardy PAT MISSED)")
    vendor = _row(_plays(401858435), "#37 S.Lopez kick attempt failed")
    assert classic["EP_end"] == 6
    assert vendor["EP_end"] == 6


# --- C4: a made vendor-text try ("kick attempt good") scores the touchdown row at 7 ------------


def test_vendor_text_made_pat_touchdown_ep_end_is_seven():
    plays = _plays(401856682)
    tds = plays.filter(pl.col("type.text").str.contains("Touchdown"))
    assert tds.height == 5
    assert tds["text"].str.contains("kick attempt good").all()
    assert tds["EP_end"].to_list() == [7.0] * 5
