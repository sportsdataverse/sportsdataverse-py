"""Fumble plays keep their rush / pass flag in ESPN's 2025 play-text format.

ESPN's 2025 feed writes a run as "rush right for 6 yards gain" and files fumbles that
go out of bounds under a new ``Fumble`` type. The rush flag only read "run for" and
neither flag knew ``Fumble``. Together those left 435 of 1,401 FBS scrimmage fumbles in
2025 with ``pass`` and ``rush`` both False, outside every pass/rush aggregate (havoc,
EPA/play, success rate). Every text below is a real play (game id in the comment).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"

# name-mangled private
_FLAGS = CFBPlayProcess._CFBPlayProcess__add_rush_pass_flags

# (type.text, text, rush, pass)
CASES = [
    # 401767129: new-format run, fumble lost
    (
        "Fumble Recovery (Opponent)",
        "(05:33) No Huddle-Shotgun #24 D.Thompson rush right for 6 yards gain to the PSU45 fumbled by "
        "#24 D.Thompson at PSU45 forced by #56 D.Hampsten recovered by SAC #94 M.Mosley at PSU45, End Of Play",
        True,
        False,
    ),
    # 401752749: new-format run, fumble recovered by the offense
    (
        "Fumble Recovery (Own)",
        "(02:30) No Huddle-Shotgun #5 Q.Wisner rush middle for 6 yards loss to the TEX19 fumbled by "
        "#5 Q.Wisner at TEX20 recovered by TEX #5 Q.Wisner at TEX19 (#88 J.Burroughs)",
        True,
        False,
    ),
    # 401752946: new ``Fumble`` type, run fumbled out of bounds
    (
        "Fumble",
        "(01:02) No Huddle-Shotgun #10 T.Ti'a rush right for 4 yards loss to the WSU44 fumbled by "
        "#10 T.Ti'a at WSU45, out of bounds at WSU44, TURNOVER ON DOWNS",
        True,
        False,
    ),
    # 401761640: new ``Fumble`` type, completion fumbled out of bounds
    (
        "Fumble",
        "No Huddle-Shotgun #7 C.Del Rio-Wilson pass complete short right to #4 D.Lacey caught at Herd29, "
        "for 3 yards loss to the Herd29 fumbled by #4 D.Lacey at Herd29 forced by #5 D.Thomas, out of bounds at Herd29",
        False,
        True,
    ),
    # 401757224: stats-crew format with no "for"
    (
        "Fumble Recovery (Own)",
        "Mettauer,Mabrey rush middle , fumble by Mettauer,Mabrey recovered by SAM Joseph,Mich'le, "
        "Joseph,Mich'le rush. Mabrey Mettauer fumbled, recovered by SHSU Mich'le Joseph",
        True,
        False,
    ),
    # 401634180 (2024): the older "run for" format, unchanged
    (
        "Fumble Recovery (Own)",
        "Keali'i Ah Yat run for 18 yds to the MONT 8 Keali'i Ah Yat fumbled, recovered by MONT Keali'i Ah Yat",
        True,
        False,
    ),
    # Return fumbles are not scrimmage plays: 401777325 kickoff, 401754598 punt
    (
        "Fumble Recovery (Opponent)",
        "(12:33) #37 G.Rippa kickoff 65 yards to the KSU15 #9 D.Bryson return 33 yards to the KSU40 fumbled by "
        "#9 D.Bryson at KSU40 forced by #37 G.Rippa recovered by Jax St #24 J.Jones at KSU48, End Of Play",
        False,
        False,
    ),
    (
        "Fumble Recovery (Opponent)",
        "(02:28) #98 C.Noonkester punt 42 yards to the FSU14 #4 S.White return 2 yards to the FSU14 fumbled by "
        "#4 S.White at FSU14 recovered by NCSU #4 T.Thomas at FSU16, End Of Play",
        False,
        False,
    ),
    # 401756927: a defensive score whose appended two-point try is a run: still not a rush
    (
        "Fumble Return Touchdown",
        "Keaton Thomas 24 Yd Fumble Return (Sawyer Robertson Run for Two-Point Conversion)",
        False,
        False,
    ),
]


@pytest.mark.parametrize(("type_text", "text", "rush", "pass_"), CASES)
def test_fumble_play_rush_pass_flags(type_text: str, text: str, rush: bool, pass_: bool) -> None:
    df = pl.DataFrame({"type.text": [type_text], "text": [text]})
    out = _FLAGS(CFBPlayProcess.__new__(CFBPlayProcess), df)
    assert (out["rush"][0], out["pass"][0]) == (rush, pass_)


def _offline_plays(summary: dict, game_id: int) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame


@pytest.mark.parametrize(
    ("game_id", "name"),
    [
        (401754571, "summary_401754571.json"),  # Georgia Tech @ Syracuse, 2025
        (401752946, "summary_401752946_trimmed.json.gz"),  # Washington State @ Oregon State, 2025
    ],
)
def test_scrimmage_fumbles_are_pass_or_rush_end_to_end(game_id: int, name: str) -> None:
    """Every fumble-typed scrimmage play is a pass or a rush, and a completion stays one."""
    path = FIX / name
    if name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            summary = json.load(fh)
    else:
        summary = json.loads(path.read_text(encoding="utf-8"))
    plays = _offline_plays(summary, game_id)
    scrimmage = plays.filter(
        pl.col("type.text").str.starts_with("Fumble")
        & pl.col("text").str.contains(r"rush \w+ for|run for|pass complete|pass incomplete|sacked")
    )
    assert scrimmage.height >= 2
    assert (scrimmage["rush"] != scrimmage["pass"]).all(), scrimmage.select("type.text", "text", "rush", "pass")
    completions = scrimmage.filter(pl.col("text").str.contains("pass complete"))
    assert completions["completion"].all()


def test_intercepted_pass_fumbled_out_of_bounds_keeps_the_interception() -> None:
    """A ``Fumble``-typed pick whose returner fumbles out of bounds is an interception, not a lost fumble.

    Once the pass flag reads ``Fumble`` rows, the strip-sack rule would retype this row
    "Fumble Recovery (Opponent)" (a pass, a fumble, a change of possession) and ``int`` would miss it.
    """
    with gzip.open(FIX / "summary_401761657_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)  # Old Dominion @ Georgia Southern, 2025
    plays = _offline_plays(summary, 401761657)
    row = plays.filter(pl.col("text").str.contains("J.French IV pass intercepted by #12 J.Carter", literal=True))
    assert row.height == 1
    assert row.select("type.text", "pass", "int").row(0) == ("Interception Return", True, True)
