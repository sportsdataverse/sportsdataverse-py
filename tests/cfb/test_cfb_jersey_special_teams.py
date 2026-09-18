"""ESPN's 2025 jersey-style special-teams text in the CFB processor.

Fixture: ``tests/cfb/fixtures/summary_401856682.json`` is the ESPN summary for
Ohio State @ Texas, 2025-08-30 (``videos`` / ``news`` / ``article`` stripped),
with ``participants_401856682.json`` the wide per-play participants frame from
``espn_cfb_play_participants`` (captured 2026-09-15). The 2025 vendor template
writes kicks in the NFL-like jersey style -- "(04:07) #43 M.Chiumento punt 43
yards to the OSU36 #0 B.Inniss return 16 yards to the TEX48 (#81 N.Townsend),
out of bounds" -- which none of the "punt for N yards" / "returned by" branches
read: every punt, kickoff, field-goal and return yardage came out null on those
games, along with every returner and fair-catcher name.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from sportsdataverse.football import espn_text
from sportsdataverse.football.usage_box import create_usage_box

FIX = Path(__file__).parent / "fixtures"
GAME_ID = 401856682

PUNT_RETURN_OOB = (
    "(04:07) #43 M.Chiumento punt 43 yards to the OSU36 #0 B.Inniss return 16 yards "
    "to the TEX48 (#81 N.Townsend), out of bounds"
)
PUNT_FAIR_CATCH = "(12:00) #42 J.McGuire punt 26 yards to the TEX13 fair catch by #21 R.Niblett at TEX13"
PUNT_RETURN_ZERO = (
    "(10:09) #43 M.Chiumento punt 43 yards to the OSU27 #0 B.Inniss return 0 yards to the OSU27 "
    "(#24 W.Roberson). #81 N.Townsend injured on the play"
)
KICKOFF_RETURN = (
    "(15:00) #49 M.Diomede kickoff 59 yards to the TEX06 #21 R.Niblett return 16 yards to the TEX22 (#11 T.Moore)"
)
KICKOFF_TOUCHBACK = "(12:04) #49 M.Diomede kickoff 65 yards to the TEX00, Touchback"
FG_GOOD = "(12:07) #96 C.Hawkins field goal attempt from 26 yards GOOD (H: #42 J.McGuire, LS: #55 D.Riggs), clock 12:04"
FG_NO_GOOD = (
    "(04:21) #96 C.Hawkins field goal attempt from 45 yards NO GOOD (H: #42 J.McGuire, LS: #55 D.Riggs), clock 04:16"
)
# pre-2025 phrasings the jersey patterns must leave alone
OLD_PUNT = "Scott Harding punt for 41 yards, John Doe returns for 5 yards to the 30."
OLD_KICKOFF = "Tyler Hadden kickoff for 64 yards returned by Arthur Jaffee for 4 yards to the 30."
OLD_FG = "Brett Baer 47 Yd Field Goal Good"


def test_jersey_grammar_reads_the_six_texts_and_ignores_the_old_style():
    df = pl.DataFrame(
        {
            "text": [
                PUNT_RETURN_OOB,
                PUNT_FAIR_CATCH,
                PUNT_RETURN_ZERO,
                KICKOFF_RETURN,
                KICKOFF_TOUCHBACK,
                FG_GOOD,
                FG_NO_GOOD,
                OLD_PUNT,
                OLD_KICKOFF,
                OLD_FG,
            ]
        }
    ).with_columns(
        punt=espn_text.jersey_punt_yards(),
        kickoff=espn_text.jersey_kickoff_yards(),
        fg=espn_text.jersey_fg_yards(),
        fg_result=espn_text.jersey_fg_result(),
        ret=espn_text.jersey_return_yards(),
        returner=espn_text.jersey_returner(),
        punter=espn_text.jersey_punter(),
        kicker=espn_text.jersey_kicker(),
        fg_kicker=espn_text.jersey_fg_kicker(),
        has_return=espn_text.has_jersey_return(),
    )
    assert df["punt"].to_list() == [43, 26, 43, None, None, None, None, None, None, None]
    assert df["kickoff"].to_list() == [None, None, None, 59, 65, None, None, None, None, None]
    assert df["fg"].to_list() == [None, None, None, None, None, 26, 45, None, None, None]
    assert df["fg_result"].to_list() == [None] * 5 + ["GOOD", "NO GOOD", None, None, None]
    assert df["ret"].to_list() == [16, None, 0, 16, None, None, None, None, None, None]
    assert df["returner"].to_list() == ["B.Inniss", "R.Niblett", "B.Inniss", "R.Niblett"] + [None] * 6
    assert df["punter"].to_list() == ["M.Chiumento", "J.McGuire", "M.Chiumento"] + [None] * 7
    assert df["kicker"].to_list() == [None, None, None, "M.Diomede", "M.Diomede"] + [None] * 5
    assert df["fg_kicker"].to_list() == [None] * 5 + ["C.Hawkins", "C.Hawkins", None, None, None]
    assert df["has_return"].to_list() == [True, False, True, True, False, False, False, False, False, False]


def _player_cols(rows):
    proc = CFBPlayProcess(gameId=1)
    df = pl.DataFrame(
        [
            {
                "text": text,
                "rush": False,
                "pass": False,
                "sack_vec": False,
                "sack": False,
                "fumble_vec": False,
                "type.text": type_text,
            }
            for type_text, text in rows
        ]
    )
    return proc._CFBPlayProcess__add_player_cols(df)


def test_add_player_cols_reads_jersey_names_and_keeps_the_old_style():
    out = _player_cols(
        [
            ("Punt Return", PUNT_RETURN_OOB),
            ("Punt", PUNT_FAIR_CATCH),
            ("Punt Return", PUNT_RETURN_ZERO),
            ("Kickoff", KICKOFF_RETURN),
            ("Kickoff", KICKOFF_TOUCHBACK),
            ("Field Goal Good", FG_GOOD),
            ("Field Goal Missed", FG_NO_GOOD),
            ("Punt", OLD_PUNT),
            ("Kickoff Return", OLD_KICKOFF),
            ("Field Goal Good", OLD_FG),
        ]
    )
    assert out["punter_player_name"].to_list()[:3] == ["M.Chiumento", "J.McGuire", "M.Chiumento"]
    assert out["punt_return_player_name"].to_list()[:3] == ["B.Inniss", "R.Niblett", "B.Inniss"]
    assert out["kickoff_player_name"].to_list()[3:5] == ["M.Diomede", "M.Diomede"]
    assert out["kickoff_return_player_name"].to_list()[3:5] == ["R.Niblett", None]
    assert out["fg_kicker_player_name"].to_list()[5:7] == ["C.Hawkins", "C.Hawkins"]
    # the pre-2025 phrasings resolve exactly as before
    assert out["punter_player_name"][7] == "Scott Harding"
    assert out["punt_return_player_name"][7] == "John Doe"
    assert out["kickoff_return_player_name"][8] == "Arthur Jaffee"
    assert out["fg_kicker_player_name"][9] == "Brett Baer"


@pytest.fixture(scope="module")
def summary():
    return json.loads((FIX / f"summary_{GAME_ID}.json").read_text())


@pytest.fixture(scope="module")
def participants():
    return pl.read_json(FIX / f"participants_{GAME_ID}.json")


def _run(summary, participants=None):
    proc = CFBPlayProcess(gameId=GAME_ID, participants=participants, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    out = proc.run_processing_pipeline()
    return proc, pl.from_dicts(out["plays"], infer_schema_length=None), out


@pytest.fixture(scope="module")
def processed(summary):
    return _run(summary)


@pytest.fixture(scope="module")
def processed_with_participants(summary, participants):
    return _run(summary, participants)


def _row(plays, text):
    hits = plays.filter(pl.col("text") == text)
    assert hits.height == 1, text
    return hits.row(0, named=True)


def test_pipeline_yardage_on_the_six_texts(processed):
    _, plays, _ = processed
    r = _row(plays, PUNT_RETURN_OOB)
    assert (r["yds_punted"], r["yds_punt_return"]) == (43, 16)
    assert r["punt_oob"] is False, "the returner stepped out, the punt did not"
    r = _row(plays, PUNT_FAIR_CATCH)
    assert (r["yds_punted"], r["yds_punt_return"], r["punt_fair_catch"]) == (26, 0, True)
    r = _row(plays, PUNT_RETURN_ZERO)
    assert (r["yds_punted"], r["yds_punt_return"]) == (43, 0)
    r = _row(plays, KICKOFF_RETURN)
    assert (r["yds_kickoff"], r["yds_kickoff_return"], r["kickoff_tb"]) == (59, 16, False)
    r = _row(plays, KICKOFF_TOUCHBACK)
    assert (r["yds_kickoff"], r["yds_kickoff_return"], r["kickoff_tb"]) == (65, 25, True)
    r = _row(plays, FG_GOOD)
    assert (r["yds_fg"], r["fg_made"], r["fg_attempt"]) == (26, True, True)
    r = _row(plays, FG_NO_GOOD)
    assert (r["yds_fg"], r["fg_made"], r["fg_attempt"]) == (45, False, True)
    # the whole game: no null kick / punt / field-goal distance remains
    assert plays.filter(pl.col("kickoff_play") == True)["yds_kickoff"].null_count() == 0
    assert plays.filter(pl.col("punt") == True)["yds_punted"].null_count() == 0
    assert plays.filter(pl.col("fg_attempt") == True)["yds_fg"].null_count() == 0


def test_pipeline_text_names_are_the_fallback(processed):
    _, plays, _ = processed
    assert _row(plays, PUNT_RETURN_OOB)["punt_return_player_name"] == "B.Inniss"
    assert _row(plays, PUNT_FAIR_CATCH)["punt_return_player_name"] == "R.Niblett"
    assert _row(plays, KICKOFF_RETURN)["kickoff_return_player_name"] == "R.Niblett"
    assert _row(plays, KICKOFF_TOUCHBACK)["kickoff_return_player_name"] is None
    assert _row(plays, PUNT_RETURN_OOB)["punter_player_name"] == "M.Chiumento"
    assert _row(plays, KICKOFF_RETURN)["kickoff_player_name"] == "M.Diomede"
    assert _row(plays, FG_GOOD)["fg_kicker_player_name"] == "C.Hawkins"


def test_participants_names_win_over_the_text_names(processed_with_participants, processed):
    _, plays, _ = processed_with_participants
    assert _row(plays, PUNT_RETURN_OOB)["punt_return_player_name"] == "Brandon Inniss"
    assert _row(plays, KICKOFF_RETURN)["kickoff_return_player_name"] == "Ryan Niblett"
    # ESPN files no returner participant on a fair catch: the text name stands
    assert _row(plays, PUNT_FAIR_CATCH)["punt_return_player_name"] == "R.Niblett"
    assert _row(plays, PUNT_FAIR_CATCH)["punter_player_name"] == "Joe McGuire"
    assert _row(plays, PUNT_RETURN_OOB)["punter_player_name"] == "Mac Chiumento"
    assert _row(plays, FG_GOOD)["fg_kicker_player_name"] == "Connor Hawkins"
    # the kickoff kicker has no participant mapping (only the FG kicker does): text name
    assert _row(plays, KICKOFF_RETURN)["kickoff_player_name"] == "M.Diomede"
    # the yardage does not depend on the participants
    _, text_only, _ = processed
    cols = ["text", "yds_punted", "yds_kickoff", "yds_fg", "yds_punt_return", "yds_kickoff_return"]
    st = pl.col("text").str.contains(r"#\d+ \S+ (?:punt|kickoff|field goal)")
    assert plays.filter(st).select(cols).sort("text").equals(text_only.filter(st).select(cols).sort("text"))


def test_special_teams_box_sums_the_jersey_yardage(processed):
    _, plays, out = processed
    box = create_usage_box(plays, None, league="cfb")
    punters = {r["player_name"]: r for r in box["st_punters"]}
    chiumento = punters["M.Chiumento"]
    assert (chiumento["punts"], chiumento["punt_yards"], chiumento["punt_long"]) == (3, 122, 43)
    assert (chiumento["punt_returns_allowed"], chiumento["punt_return_yards_allowed"]) == (2, 16)
    assert chiumento["punt_out_of_bounds"] == 0
    kickers = {r["player_name"]: r for r in box["st_kickers"]}
    assert (kickers["M.Diomede"]["kickoffs"], kickers["M.Diomede"]["kickoff_yards"]) == (6, 380)
    assert (kickers["C.Hawkins"]["fg_attempts"], kickers["C.Hawkins"]["fg_made"], kickers["C.Hawkins"]["fg_long"]) == (
        4,
        3,
        48,
    )
    returners = {r["player_name"]: r for r in box["st_returners"]}
    assert (returners["B.Inniss"]["punt_returns"], returners["B.Inniss"]["punt_return_yards"]) == (2, 16)
    assert (returners["R.Niblett"]["kick_returns"], returners["R.Niblett"]["kick_return_yards"]) == (1, 16)
    # the same sections ride on the processor's advanced box
    assert out["advBoxScore"]["st_punters"]


def test_pipeline_special_teams_provenance(processed):
    # every kick distance in this game is stated in the text: nothing derived, and the
    # provenance columns ride on the processed plays (null away from kicks)
    _, plays, _ = processed
    kicks = pl.col("punt") == True
    assert plays.filter(kicks)["yds_punted_source"].to_list() == ["text"] * 6
    assert plays.filter(pl.col("kickoff_play") == True)["yds_kickoff_source"].to_list() == ["text"] * 11
    assert plays.filter(kicks)["yds_punt_return_source"].drop_nulls().to_list() == ["text"] * 4
    other = plays.filter((pl.col("punt") == False) & (pl.col("kickoff_play") == False))
    for col in ("yds_punted_source", "yds_kickoff_source", "yds_punt_return_source"):
        assert other[col].null_count() == other.height
