"""EX-3.4-a / EX-3.4-c (quality program task 3.4 follow-ups): an end state ESPN did not state takes the next snap.

EX-3.4-c: ESPN's degenerate end block ``{down 0, yardLine 0, yardsToEndzone 0}`` on a rush, a pass, a sack or
a penalty (supp-002's rule covers fumble plays only). The processor kept down 0 and, where nothing else
filled the spot, the ``<= 0`` clamp's own 99: 401754585's 2nd & 5 incompletion at the 70 scored -1.85 where
the 3rd & 5 snapped there gives -0.95. EX-3.4-a: 401856797's Q3-closing "rush for 4 yards gain to the CIN01"
ships ``end.yardsToEndzone`` 99, the complement of the 1 where Q4 opens 2nd & 1; the mirror repair compared
it with the End Period row in between (which repeats the play's START), so EPA read -4.62.

Oracle: the ESPN feed's next real snap (the next play that is not a Timeout / End Period / End of row, read
from the payload, not from the processor), its start down, distance and yards to the end zone; the play's
text agrees with that spot on every row here (an incompletion or a declined flag leaves it, a gain or an
enforced flag moves it there). The EPA is held within 0.1 of the processor's own EP at that snap minus
``EP_start`` (observed 0.000 on the seven EX-3.4-c rows, 0.005 on 401856797637, whose clock runs from Q3
0:02 to Q4 14:15), and that snap's ``EP_between`` within 0.1 of 0.
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
#: the fixed EPA within 0.1 of the next-snap value (addendum W-M6), and that snap's EP_between within 0.1 of 0
EPA_ABS_TOL = 0.1
_ADMIN = re.compile(r"(?i)^(?:timeout|end period|end of)")

#: play id -> (game id, capture, why)
ROWS = {
    322590228207: (322590228, "summary_322590228_trimmed.json.gz", "EX-3.4-c Rush, end team dropped"),
    400869270101928104: (400869270, "summary_400869270.json", "EX-3.4-c declined Penalty, end team dropped"),
    401752744102995801: (401752744, "summary_401752744_trimmed.json.gz", "EX-3.4-c declined Penalty"),
    401754585114: (401754585, "summary_401754585_trimmed.json.gz", "EX-3.4-c Pass Incompletion"),
    401754598144: (401754598, "summary_401754598.json", "EX-3.4-c Pass Reception"),
    401761622102957402: (401761622, "summary_401761622_trimmed.json.gz", "EX-3.4-c Penalty, PI enforced"),
    401858435289: (401858435, "summary_401858435.json", "EX-3.4-c Pass Reception, holding enforced"),
    401856797637: (401856797, "supp002/401856797_trimmed.json.gz", "EX-3.4-a mirrored end, End Period between"),
}

pytestmark = pytest.mark.whole_game


def _summary(name: str) -> dict:
    path = FIX / name
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(path.read_text(encoding="utf-8"))


def _next_snap(summary: dict, play_id: int) -> tuple[int, int, int, int]:
    """The feed's next real play after ``play_id``: (id, start.down, start.distance, start.yardsToEndzone)."""
    plays = [p for d in (summary.get("drives") or {}).get("previous", []) for p in d.get("plays", [])]
    i = next(k for k, p in enumerate(plays) if int(p["id"]) == play_id)
    nxt = next(p for p in plays[i + 1 :] if not _ADMIN.search((p.get("type") or {}).get("text") or ""))
    assert nxt["start"]["team"]["id"] == plays[i]["start"]["team"]["id"], (play_id, nxt["id"])
    start = nxt["start"]
    return int(nxt["id"]), int(start["down"]), int(start["distance"]), int(start["yardsToEndzone"])


@functools.cache
def _frame(game_id: int, name: str) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id, join_participants=False, odds_override=ODDS)
    proc.espn_cfb_pbp(summary=_summary(name))
    proc.run_processing_pipeline()
    return proc.plays_frame.with_columns(pl.col("id").cast(pl.Int64))


@pytest.mark.parametrize(
    "play_id", [pytest.param(p, marks=pytest.mark.xdist_group(f"cfb-ex34-{v[0]}"), id=str(p)) for p, v in ROWS.items()]
)
def test_unstated_end_takes_the_next_snap(play_id):
    game_id, name, why = ROWS[play_id]
    nid, down, distance, ytez = _next_snap(_summary(name), play_id)
    frame = _frame(game_id, name)
    row = frame.filter(pl.col("id") == play_id).row(0, named=True)
    nxt = frame.filter(pl.col("id") == nid).row(0, named=True)
    assert (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) == (down, distance, ytez), (
        why,
        row["end.down"],
        row["end.distance"],
        row["end.yardsToEndzone"],
    )
    assert row["change_of_pos_team"] is False, why
    oracle = nxt["EP_start"] - row["EP_start"]
    assert abs(row["EPA"] - oracle) <= EPA_ABS_TOL, f"{why}: EPA {row['EPA']} vs next-snap {oracle}"
    assert abs(nxt["EP_between"]) <= EPA_ABS_TOL, f"{nid}: EP_between {nxt['EP_between']}"


#: play id -> (game id, capture): a play followed by an admin row whose end ESPN states right, while the next
#: real snap's start is the complement (a mirrored start of its own, or a sequence glitch across the break)
KEPT = {
    401282772102995901: (401282772, "ex34/401282772_trimmed.json.gz"),  # "run for 2 yds to the WMich 11"; Timeout
    401761589103995002: (401761589, "ex34/401761589_trimmed.json.gz"),  # "run for no gain to the CLT 48"; End Period
}


@pytest.mark.parametrize(
    "play_id", [pytest.param(p, marks=pytest.mark.xdist_group(f"cfb-ex34-{v[0]}"), id=str(p)) for p, v in KEPT.items()]
)
def test_mirror_repair_across_an_admin_row_needs_the_stated_yardage(play_id):
    """EX-3.4-a's guard: across an admin row the mirror repair moves an end only to where statYardage puts it.

    Oracle: the feed's own start, statYardage and text for the play. Without the guard the end is rewritten
    to the next snap's (mirrored) start: 401282772 -> 89, 401761589 -> 48.
    """
    game_id, name = KEPT[play_id]
    plays = [p for d in _summary(name)["drives"]["previous"] for p in d["plays"]]
    raw = next(p for p in plays if int(p["id"]) == play_id)
    stated = int(raw["start"]["yardsToEndzone"]) - int(raw["statYardage"])
    row = _frame(game_id, name).filter(pl.col("id") == play_id).row(0, named=True)
    assert row["end.yardsToEndzone"] == stated == int(raw["end"]["yardsToEndzone"]), (row["end.yardsToEndzone"], stated)


#: play id -> (game id, capture, end.down, end.yardsToEndzone, EPA) on origin/devel-cfb dd424de3e2: a declined flag
#: filed after a turnover (an interception; a failed 4th down), so the next snap is the OTHER team's. Not EX-3.4-c's
#: class (possession changed); a byte-stability pin, NOT an oracle (end.down 0 is the feed's, unrepaired)
OTHER_TEAM_NEXT = {
    401403973103977202: (401403973, "supp002/401403973_trimmed.json.gz", 0, 95, -5.249223709106445),
    401520162104938303: (401520162, "supp002/401520162_trimmed.json.gz", 0, 87, -2.45228910446167),
}


@pytest.mark.parametrize(
    "play_id",
    [pytest.param(p, marks=pytest.mark.xdist_group(f"cfb-ex34-{v[0]}"), id=str(p)) for p, v in OTHER_TEAM_NEXT.items()],
)
def test_degenerate_end_before_the_other_teams_snap_is_left_alone(play_id):
    game_id, name, down, ytez, epa = OTHER_TEAM_NEXT[play_id]
    row = _frame(game_id, name).filter(pl.col("id") == play_id).row(0, named=True)
    assert (row["end.down"], row["end.yardsToEndzone"]) == (down, ytez), (row["end.down"], row["end.yardsToEndzone"])
    assert abs(row["EPA"] - epa) <= 1e-5, row["EPA"]
