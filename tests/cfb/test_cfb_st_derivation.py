"""Special-teams values the play text does not state, derived from ESPN field position.

Every kick row below is a real ESPN play with the yardlines and parsed yardage
captured from processed games (team ids are placeholders, a distance the text does
state is hidden where noted, and the "Rush for 2 yards" rows only stand in for the
next snap's starting spot), fed to
``cfb_pbp._derive_special_teams_from_field_position`` with the flag and yardage
columns the processor has set by then. 2004 text carries no kick distances at all
("Punt by Vinnie Burns (VT) returned 15 yards by Reggie Bush (USC) to the Trojans
21."), and 29% of 2023 punts are only "Flynn Appleby punt for 40 yds".
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_pbp import _derive_special_teams_from_field_position as derive

KICKING, RECEIVING = 1, 2

_SCHEMA = {
    "text": pl.Utf8,
    "type.text": pl.Utf8,
    "season": pl.Int32,
    "start.yardsToEndzone": pl.Int64,
    "end.yardsToEndzone": pl.Int64,
    "start.pos_team.id": pl.Int64,
    "end.pos_team.id": pl.Int64,
    "punt": pl.Boolean,
    "punt_blocked": pl.Boolean,
    "punt_tb": pl.Boolean,
    "punt_oob": pl.Boolean,
    "kickoff_play": pl.Boolean,
    "kickoff_tb": pl.Boolean,
    "kickoff_oob": pl.Boolean,
    "yds_punted": pl.Int32,
    "yds_punt_return": pl.Int32,
    "yds_kickoff": pl.Int32,
    "yds_kickoff_return": pl.Int32,
}
_DEFAULTS = {
    "type.text": "Rush",
    "start.pos_team.id": RECEIVING,
    "end.pos_team.id": RECEIVING,
    "punt": False,
    "punt_blocked": False,
    "punt_tb": False,
    "punt_oob": False,
    "kickoff_play": False,
    "kickoff_tb": False,
    "kickoff_oob": False,
}


def _punt(text, season, start, end, *, ret=None, dist=None, **kw):
    return {
        "text": text,
        "type.text": "Punt",
        "season": season,
        "start.yardsToEndzone": start,
        "end.yardsToEndzone": end,
        "start.pos_team.id": KICKING,
        "end.pos_team.id": RECEIVING,
        "punt": True,
        "yds_punt_return": ret,
        "yds_punted": dist,
        **kw,
    }


def _kickoff(text, season, start, end, *, ret=None, dist=None, **kw):
    # ESPN's kickoff rows carry the RECEIVING team as start and end possession
    return {
        "text": text,
        "type.text": "Kickoff",
        "season": season,
        "start.yardsToEndzone": start,
        "end.yardsToEndzone": end,
        "kickoff_play": True,
        "yds_kickoff_return": ret,
        "yds_kickoff": dist,
        **kw,
    }


def _snap(season, start, text="Rush for 2 yards", **kw):
    return {"text": text, "season": season, "start.yardsToEndzone": start, "end.yardsToEndzone": start - 2, **kw}


def _run(*rows):
    frame = pl.DataFrame([{**_DEFAULTS, **r} for r in rows], schema=_SCHEMA)
    return derive(frame)


def _cells(out, col, i=0):
    return out[col][i], out[f"{col}_source"][i]


# ------------------------------------------------------------------ punt distance
def test_2004_returned_punt_distance_from_the_landing_spot():
    # receiver ends on his 21 after a 15-yard return: the ball came down on his 6,
    # 52 yards from the punter's line at the VT 42 (58 to go)
    out = _run(
        _punt(
            "Punt by Vinnie Burns (VT) returned 15 yards by Reggie Bush (USC) to the Trojans 21.", 2004, 58, 79, ret=15
        ),
        _snap(2004, 79),
    )
    assert _cells(out, "yds_punted") == (52, "derived")
    assert _cells(out, "yds_punt_return") == (15, "text")


def test_2004_no_return_punt_distance():
    out = _run(_punt("Punt by Brian Monroe (MIA) no return by Tigers.", 2004, 75, 75, ret=0), _snap(2004, 75))
    assert _cells(out, "yds_punted") == (50, "derived")


def test_touchback_punt_distance_is_the_distance_to_the_goal_line():
    out = _run(_punt("Bailey Cate punt for a touchback", 2016, 79, 80, ret=20, punt_tb=True), _snap(2016, 80))
    assert _cells(out, "yds_punted") == (79, "derived")


def test_parsed_punt_distance_is_never_overwritten():
    # geometry says 45; the text's 40 stands and is labelled text
    out = _run(
        _punt("Wilson Berry punt for 40 yds, fair catch by Zavion Thomas at the MSST 5", 2023, 50, 95, ret=0, dist=40),
        _snap(2023, 95),
    )
    assert _cells(out, "yds_punted") == (40, "text")


def test_no_return_punt_ending_at_the_20_is_not_derived():
    # a 2004 "no return" touchback reads exactly like a punt downed at the 20
    out = _run(_punt("Punt by Chris Jackson (LSU) no return by Commodores.", 2004, 37, 80, ret=0), _snap(2004, 80))
    assert _cells(out, "yds_punted") == (None, None)


def test_punt_exclusions_leave_the_distance_null():
    # distances the text does state are hidden (dist=None), as the validation did
    cases = [
        # touchdown on the return
        _punt(
            "Punt by Dan Jordan (GSU) returned 72 yards by Tyson Browning (GA) for a touchdown.", 2004, 28, 99, ret=72
        ),
        # blocked (the processor already stores 0 as a flag convention)
        _punt(
            "Bulldogs punt blocked, recovered by Mike Adams (CIT).",
            2004,
            73,
            100,
            dist=0,
            punt_blocked=True,
            **{"end.pos_team.id": KICKING},
        ),
        # penalty
        _punt(
            "Jim Hall punt for 49 yards, returned by Israel Route for 2 yards to the Tulan 42, "
            "Tulane penalty 10 yard IB accepted.",
            2005,
            89,
            68,
            ret=2,
        ),
        # fumble
        _punt(
            "Cody Freeby punt for 30 yards, fair catch by Jermain Moreira at the Okla 46, "
            "fumbled at the Okla 46, recovered by Okla.",
            2005,
            84,
            54,
            ret=0,
        ),
        # muff
        _punt(
            "#88 E.Pulliam punt 51 yards to the Deacs19 muffed by #8 C.Hernandez at Deacs19, out of bounds at Deacs23",
            2025,
            70,
            77,
            ret=0,
        ),
        # possession did not change (ids set so; the yardlines would otherwise give 40)
        _punt("Punt by Jake Hendy (TEM) no return by Eagles.", 2004, 47, 93, ret=0, **{"end.pos_team.id": KICKING}),
        # the ball would have come down 21 yards deep: not a real landing
        _punt(
            "Punt by Chris Kindred (TUL) returned 36 yards by Darrent Williams (OSU) to the Cowboys 15.",
            2004,
            85,
            85,
            ret=36,
        ),
        # 99 yards: over the 80-yard bound
        _punt(
            "Punt by Jeremy Kapinos (PSU) returned 25 yards by Jim Leonhard (WIS) to the Badgers 23.",
            2004,
            97,
            77,
            ret=25,
        ),
        # negative: the field position contradicts a punt
        _punt("Punt by John Braziel (TCU) no return by Wildcats.", 2004, 25, 58, ret=0),
        # in range (76) but landing 11 yards deep: the text's 47 shows the end spot is off
        _punt(
            "Anthony Venneri punt for 47 yds , Elijah Kennedy returns for 20 yds to the NCAT 9", 2025, 65, 91, ret=20
        ),
    ]
    out = _run(*[x for c in cases for x in (c, _snap(2004, 60))])
    punts = out.filter(pl.col("punt") == True)
    assert punts["yds_punted_source"].to_list() == [None, "text"] + [None] * 8
    assert punts["yds_punted"].to_list() == [None, 0] + [None] * 8


# ------------------------------------------------------------------ kickoff distance
def test_2004_kickoff_assumes_the_35_and_checks_the_catch_spot():
    # 2004 stores the catch spot (the 5) in start.yardsToEndzone; 14-yard return to the 19
    out = _run(_kickoff("Kickoff returned by Ryan Gilbert (HOU) for 14 yards.", 2004, 5, 81, ret=14), _snap(2004, 81))
    assert _cells(out, "yds_kickoff") == (60, "derived")
    assert out["yds_kickoff_return"][0] == 14


def test_2004_kickoff_touchback_is_65():
    out = _run(
        _kickoff("Trojans kickoff, touchback by Sun Devils.", 2004, 20, 75, ret=20, kickoff_tb=True), _snap(2004, 75)
    )
    assert _cells(out, "yds_kickoff") == (65, "derived")


def test_2004_kickoff_whose_landing_disagrees_with_the_catch_spot_is_not_derived():
    # "for -2 yards" is parsed as +2 by the return regex; the catch-spot check catches it
    out = _run(_kickoff("Kickoff returned by Damien Rhodes (SYR) for -2 yards.", 2004, 11, 91, ret=2), _snap(2004, 91))
    assert _cells(out, "yds_kickoff") == (None, None)


def test_2004_kickoff_after_a_flag_is_not_assumed_from_the_35():
    out = _run(
        {**_snap(2004, 100, text="15 yard penalty on Falcons."), "type.text": "Penalty"},
        _kickoff("Falcons kickoff, touchback by Golden Bears.", 2004, 20, 80, ret=20, kickoff_tb=True),
        _snap(2004, 80),
    )
    assert _cells(out, "yds_kickoff", 1) == (None, None)


def test_2009_kickoff_uses_espns_spot_the_30():
    out = _run(
        _kickoff(
            "Alex Tejada kickoff for 64 yards returned by Miguel Warren, tackled by Jerico Nelson for 18 yards "
            "to the MoSt 24, tackled by Jerico Nelson.",
            2009,
            70,
            76,
            ret=18,  # the text's 64 hidden
        ),
        _snap(2009, 76),
    )
    assert _cells(out, "yds_kickoff") == (64, "derived")


def test_kickoff_touchback_text_without_a_distance():
    # "kick for", not "kickoff for": the distance regex misses it
    out = _run(
        _kickoff("Blake Glessner kick for 65 yds for a touchback", 2023, 65, 75, ret=25, kickoff_tb=True),
        _snap(2023, 75),
    )
    assert _cells(out, "yds_kickoff") == (65, "derived")


def test_kickoff_exclusions_leave_the_distance_null():
    rows = [
        _kickoff("Kevin Lovell on-side kick recovered by PENN ST at the 50 yard line.", 2005, 65, 50),
        # out of bounds: the receiver's 35 is a placement, not a landing (the 42 hidden)
        _kickoff("Todd Soderquist kickoff for 42 yards out-of-bounds.", 2005, 65, 65, ret=40, kickoff_oob=True),
        # 2018+: a fair catch inside the 25 is flagged a touchback; the ball was not kicked 65
        _kickoff(
            "Logan Ward kickoff for 40 yds fair catch by Keilan Robinson at the TEX 4",
            2023,
            65,
            96,
            ret=25,
            kickoff_tb=True,
        ),
        # a bare "kickoff" row the processor also flags as a touchback
        _kickoff("Atticus Sappington kickoff", 2023, 65, 75, ret=25, kickoff_tb=True),
        _kickoff("Demond Claiborne 96 Yd Kickoff Return (Matthew Dennis Kick)", 2023, 65, 99),
    ]
    out = _run(*[x for r in rows for x in (r, _snap(2023, 70))]).filter(pl.col("kickoff_play") == True)
    assert out["yds_kickoff"].to_list() == [None] * 5
    assert out["yds_kickoff_source"].to_list() == [None] * 5


# ------------------------------------------------------------------ bare-punt returns
def test_bare_punt_return_from_the_distance_and_end_spot():
    # from the 30, 40 yards to the receiver's 30; receiver ends on the 55 -> 25-yard return
    out = _run(_punt("Flynn Appleby punt for 40 yds", 2023, 70, 45, dist=40), _snap(2023, 45))
    assert _cells(out, "yds_punt_return") == (25, "derived")
    assert _cells(out, "yds_punted") == (40, "text")


def test_bare_punt_return_needs_the_next_snap_at_the_end_spot():
    # 2025 feed: end.yardsToEndzone written from the kicking side (11) while the next
    # snap starts at 89 -- the end spot is not trustworthy, so nothing is derived
    out = _run(_punt("(00:40) #19 T.Doman punt 33 yards to the UK11", 2025, 44, 11, dist=33), _snap(2025, 89))
    assert _cells(out, "yds_punt_return") == (None, None)


def test_bare_punt_return_exclusions():
    rows = [
        # reached the end zone, receiver at the 20: an unmarked touchback, not a 20-yard return
        _punt("Emilio Duran punt for 55 yds", 2024, 55, 80, dist=55),
        # exactly 5: indistinguishable from a flag enforced at the end of the play
        _punt("Kenny Pham punt for 50 yds", 2024, 78, 67, dist=50),
        # geometry says no return
        _punt("Grant Burkett punt for 34 yds", 2023, 68, 66, dist=34),
        # the text describes a return the parser missed -- not a bare punt
        _punt(
            "(00:54) #41 J.Stonehouse punt 44 yards to the GT31 #7 B.Stockton return 1 yard to the GT32 (#82 J.Tremble)",
            2025,
            75,
            68,
            dist=44,
        ),
        # a first down on a punt is a flag or a fake
        _punt("Emilio Duran punt for 30 yds for a 1ST down", 2023, 40, 5, dist=30),
    ]
    out = _run(*[x for r in rows for x in (r, _snap(2023, r["end.yardsToEndzone"]))]).filter(pl.col("punt") == True)
    assert out["yds_punt_return"].to_list() == [None] * 5
    assert out["yds_punt_return_source"].to_list() == [None] * 5


def test_parsed_zero_return_is_kept_and_labelled_text():
    out = _run(
        _punt("Wilson Berry punt for 40 yds, fair catch by Zavion Thomas at the MSST 5", 2023, 50, 95, ret=0, dist=40),
        _snap(2023, 95),
    )
    assert _cells(out, "yds_punt_return") == (0, "text")


def test_non_kick_rows_carry_null_provenance_and_dtypes_hold():
    out = _run(_snap(2023, 60), _punt("Flynn Appleby punt for 40 yds", 2023, 70, 45, dist=40), _snap(2023, 45))
    assert out.row(0, named=True)["yds_punted_source"] is None
    assert out.row(0, named=True)["yds_kickoff_source"] is None
    assert out.row(0, named=True)["yds_punt_return_source"] is None
    for col in ("yds_punted", "yds_kickoff", "yds_punt_return"):
        assert out.schema[col] == pl.Int32
        assert out.schema[f"{col}_source"] == pl.Utf8


def test_punt_out_of_bounds_is_derived_unlike_a_kickoff_out_of_bounds():
    # a punt out of bounds is dead where it crossed the sideline, so ESPN's end spot IS
    # the landing spot (the processor already stores the 0 return): 2005 Boston College,
    # with the text's distance hidden. A kickoff out of bounds is spotted by rule and
    # stays excluded -- see test_kickoff_exclusions_leave_the_distance_null.
    out = _run(
        _punt("Johnny Ayers punt for 41 yards out-of-bounds.", 2005, 69, 72, ret=0, punt_oob=True),
        _snap(2005, 72),
    )
    assert _cells(out, "yds_punted") == (41, "derived")


def test_negative_parsed_values_are_not_treated_as_missing():
    # "punt for a loss of N" stores -N and a punt returned backwards stores a negative
    # return (both real, parsed upstream): a negative is a value, not a null, so it is
    # kept, labelled "text", and the distance derived from it stays right.
    out = _run(
        _punt("Johnny Ayers punt for a loss of 12 yards", 2005, 90, 98, ret=0, dist=-12),
        _snap(2005, 98),
        _punt(
            "Punt by Brandon Fields (MSU) returned -2 yards by Steve Breaston (MICH) to the Wolverines 13.",
            2004,
            45,
            87,
            ret=-2,
        ),
        _snap(2004, 87),
    )
    assert _cells(out, "yds_punted") == (-12, "text")
    assert _cells(out, "yds_punt_return") == (0, "text")
    # the 2004 row: the ball came down on the receiver's 15 and was returned BACK to the
    # 13, so the punt was 30 yards -- reading -2 as missing would give 26
    assert _cells(out, "yds_punted", 2) == (30, "derived")
    assert _cells(out, "yds_punt_return", 2) == (-2, "text")


# ------------------------------------------------------------------ through the pipeline
def test_pipeline_derives_2004_kick_distances_on_a_stored_game():
    """The production path: `CFBPlayProcess` on a committed 2004 summary.

    2004 text states no kick distance, so this is the season the derivation actually
    fires on -- and the only end-to-end check that a *derived* value reaches
    ``plays_frame`` (``test_cfb_jersey_special_teams`` covers a game where every
    distance is stated). Michigan State @ Michigan, 2004.
    """
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    summary = json.loads((Path(__file__).parent / "fixtures" / "summary_243040130.json").read_text())
    proc = CFBPlayProcess(gameId=243040130)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    plays = proc.plays_frame

    for col in ("yds_punted", "yds_kickoff", "yds_punt_return"):
        assert plays.schema[col] == pl.Int32
        assert plays.schema[f"{col}_source"] == pl.Utf8
    counts = {
        c: dict(zip(*plays.group_by(f"{c}_source").len().sort(f"{c}_source").to_dict(as_series=False).values()))
        for c in ("yds_punted", "yds_kickoff", "yds_punt_return")
    }
    # 10 of the 14 punts and all 9 kickoffs are derived; one punt distance is stated
    assert counts["yds_punted"].get("derived") == 10
    assert counts["yds_punted"].get("text") == 1
    assert counts["yds_kickoff"].get("derived") == 9
    # returns are all parsed from the text here: nothing derived
    assert counts["yds_punt_return"].get("derived") is None
    assert counts["yds_punt_return"].get("text") == 13

    # a NEGATIVE parsed return (real: the returner lost 2) is not treated as missing,
    # and the distance derived from it is the true 30, not 26
    row = plays.filter(pl.col("text").str.contains("returned -2 yards by Steve Breaston", literal=True)).row(
        0, named=True
    )
    assert (row["yds_punt_return"], row["yds_punt_return_source"]) == (-2, "text")
    assert (row["yds_punted"], row["yds_punted_source"]) == (30, "derived")
