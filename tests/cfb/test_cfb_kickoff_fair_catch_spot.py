"""A fair-caught kickoff is a touchback only inside the receiver's 25 (NCAA Rule 6-5-1, from 2018).

Rule 6-5-1: a fair-caught kick is dead where caught and belongs to the receiver at that spot; the 2018
exception puts a free kick fair-caught behind the receiver's 25 at the 25. sdv-py applied the exception to
every 2018+ fair catch, so a catch at the NCSt 30 was published at the 25.

Oracle: the feed's next real snap (the next play that is not a Timeout / End Period / End of row), read
from the payload, not from the processor, with possession unchanged. Only 401757282331 carries a flag: a
15-yard foul enforced from the catch spot, which the next snap confirms. 401761641 is the 2025 vendor feed,
which files all six of its fair catches with end.yardsToEndzone 35 whatever the spot.
"""

from __future__ import annotations

import functools
import gzip
import json
import re
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"
ODDS = {"gameSpread": 1.5, "overUnder": 50.5, "homeFavorite": True, "gameSpreadAvailable": True}
_ADMIN = re.compile(r"(?i)^(?:timeout|end period|end of)")

#: play id -> (game id, capture, next-snap yards to the end zone, kickoff_tb, why)
ROWS = {
    # 2022 NC State @ Wake Forest, ESPN's 2014-2024 text form; ESPN's own end is right on all three
    401411154104934503: (401411154, "fair_catch/401411154_trimmed.json.gz", 70, False, "at the NCSt 30: the spot"),
    401411154101919502: (401411154, "fair_catch/401411154_trimmed.json.gz", 75, True, "at the WFrst 1: the 25"),
    401411154103849901: (401411154, "fair_catch/401411154_trimmed.json.gz", 75, True, "at the WFrst 0: the 25"),
    # 2025 ODU @ ULM, the vendor form; ESPN's end is 35 on all three
    4017616413: (401761641, "supp002/401761641_trimmed.json.gz", 71, False, "at ODU29: the spot"),
    401761641156: (401761641, "supp002/401761641_trimmed.json.gz", 75, True, "at ULM07: the 25"),
    401761641195: (401761641, "supp002/401761641_trimmed.json.gz", 75, True, "at ULM03: the 25"),
    # 2025 New Mexico State @ Liberty: "at LU27 PENALTY NMST UNR ... 15 yards from LU27 to LU42"; ESPN's end is 80
    401757282331: (401757282, "fair_catch/401757282_trimmed.json.gz", 58, False, "at LU27, then 15 yards: the LU42"),
    # 2017 Auburn @ Georgia, before the rule: every fair catch is dead at the spot
    400933913104899302: (400933913, "fair_catch/400933913_trimmed.json.gz", 79, False, "2017, at the Geo 21: the spot"),
    400933913102959003: (400933913, "fair_catch/400933913_trimmed.json.gz", 73, False, "2017, at the Geo 27: the spot"),
}

pytestmark = pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")


def _summary(name: str) -> dict:
    with gzip.open(FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _next_snap_y2ez(summary: dict, play_id: int) -> int:
    plays = [p for d in (summary.get("drives") or {}).get("previous", []) for p in d.get("plays", [])]
    i = next(k for k, p in enumerate(plays) if int(p["id"]) == play_id)
    nxt = next(p for p in plays[i + 1 :] if not _ADMIN.search((p.get("type") or {}).get("text") or ""))
    assert nxt["start"]["team"]["id"] == plays[i]["end"]["team"]["id"], (play_id, nxt["id"])
    return int(nxt["start"]["yardsToEndzone"])


@functools.cache
def _frame(game_id: int, name: str) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id, join_participants=False, odds_override=ODDS)
    proc.espn_cfb_pbp(summary=_summary(name))
    proc.run_processing_pipeline()
    return proc.plays_frame.with_columns(pl.col("id").cast(pl.Int64))


@pytest.mark.parametrize(
    "play_id", [pytest.param(p, marks=pytest.mark.xdist_group(f"cfb-fc-{v[0]}"), id=str(p)) for p, v in ROWS.items()]
)
def test_fair_caught_kickoff_ends_where_the_next_snap_starts(play_id):
    game_id, name, ytez, touchback, why = ROWS[play_id]
    assert _next_snap_y2ez(_summary(name), play_id) == ytez, why
    row = _frame(game_id, name).filter(pl.col("id") == play_id).row(0, named=True)
    assert row["kickoff_fair_catch"] is True, why
    assert (row["kickoff_tb"], row["end.yardsToEndzone"]) == (touchback, ytez), (why, row["end.yardsToEndzone"])
    assert (row["end.down"], row["end.distance"]) == (1, 10), why
    # a touchback credits the 25-yard convention; a fair catch at the spot is a return of no yards
    assert row["yds_kickoff_return"] == (25 if touchback else 0), why
