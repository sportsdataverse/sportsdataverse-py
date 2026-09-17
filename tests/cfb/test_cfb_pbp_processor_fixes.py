"""CFBPlayProcess defects found while mapping the processor contract (football-sources program, S1-CFB).

Every case runs the real pipeline, offline, on a stored ESPN summary:

* ``summary_401856682.json`` -- Ohio State @ Texas, 2026 week 1 (vendor text).
* ``summary_401636889.json`` -- Iowa State vs Baylor, 2024 (classic text, a ``PAT MISSED`` TD).
* ``summary_401858435.json`` -- Indiana State @ Purdue, 2026 (a failed vendor kick, a
  two-word kicker surname, a two-man sack).
* ``summary_401858439.json`` -- Howard @ Indiana, 2026 (a returner written ``J.Washington lll``).
* ``summary_401858213.json`` -- Florida A&M @ Miami, 2026 (a timeout logged twice).
* ``summary_401677179.json`` -- Indiana @ Notre Dame, 2024 ("Timeout Indiana" holds "nd").
* ``summary_401112081.json`` -- Baylor @ TCU, 2019 (triple overtime, every OT period numbered 5).
* ``summary_401858426.json`` -- Northern Illinois @ Iowa, 2026 (a punt returner written ``R.Vander Zee``).
* ``summary_400869270.json`` -- Central Michigan @ Oklahoma State, 2016 ("Timeout CENTRAL MICH").
* ``summary_401032062.json`` -- Western Michigan @ BYU, 2018 ("Timeout WESTRN MICHIGAN").
* ``summary_401752746.json`` -- Auburn @ Arkansas, 2025 ("Timeout , clock" names no team).
* ``summary_401762858.json`` -- Buffalo @ Central Michigan, 2025 ("sacked  by", "fumbled,  return  for 85 yds").

The 2026 summaries are copied verbatim from ``cfbfastR-cfb-raw/cfb/json/raw``.
"""

from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _summary(game_id: int) -> dict:
    return json.loads((FIX / f"summary_{game_id}.json").read_text())


@lru_cache(maxsize=None)
def _processed(game_id: int, mascot: str | None = "keep"):
    summary = _summary(game_id)
    if mascot != "keep":
        for comp in summary["header"]["competitions"][0]["competitors"]:
            comp["team"]["name"] = mascot
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


# --- C5: the sacker is the parenthetical after the spot ------------------------------------------


def test_sack_player_from_parenthetical():
    plays = _plays(401856682)
    sacks = plays.filter(pl.col("text").str.contains("sacked for loss"))
    assert sacks["sack_player_name"].to_list() == ["R.Pettijohn", "K.Jackson Jr.", "L.Jackson", "C.Simmons"]
    two = _row(_plays(401858435), "#2 E.Owens sacked for loss of 3 yards to the PUR33 (#23 T.Smith, #94 R.Lora)")
    assert (two["sack_player_name"], two["sack_player_name2"]) == ("T.Smith", "R.Lora")


# --- C6: kicker / returner names that are not "X.Surname" ---------------------------------------


def test_fg_kicker_and_kickoff_returner_beyond_abbreviated_names():
    fgs = _plays(401858435).filter(pl.col("text").str.contains("field goal attempt"))
    lozano = fgs.filter(pl.col("text").str.contains("#92 J.Echeverria Lozano"))
    assert lozano.height >= 2
    assert set(lozano["fg_kicker_player_name"].to_list()) == {"J.Echeverria Lozano"}
    kr = _row(_plays(401858439), "#21 J.Washington lll return 23 yards")
    assert kr["kickoff_return_player_name"] == "J.Washington lll"


# --- C7: timeouts remaining stay within the allotment ---------------------------------------------

_TIMEOUT_COLS = [
    "start.homeTeamTimeouts",
    "start.awayTeamTimeouts",
    "end.homeTeamTimeouts",
    "end.awayTeamTimeouts",
    "start.posTeamTimeouts",
    "start.defPosTeamTimeouts",
    "end.posTeamTimeouts",
    "end.defPosTeamTimeouts",
]


def test_timeouts_remaining_never_negative():
    # ESPN logs more "Timeout <team>" rows than a team can call (Texas: four in the 2nd quarter)
    plays = _plays(401856682)
    for col in _TIMEOUT_COLS:
        assert plays[col].min() >= 0, col
        assert plays[col].max() <= 3, col


def test_timeout_logged_twice_is_charged_once():
    # Florida A&M's first timeout of the game, on two consecutive rows
    plays = _plays(401858213)
    twice = plays.filter(pl.col("text") == "Timeout Florida A&M, clock 12:02")
    assert twice.height == 2
    assert twice["end.awayTeamTimeouts"].to_list() == [2, 2]


def test_overtime_timeouts_reset_to_one():
    # an overtime period allots one timeout per team; the fourth quarter's count must not carry over
    plays = _plays(401112081).filter(pl.col("period.number") >= 5)
    assert plays.height > 0
    for col in _TIMEOUT_COLS[:4]:
        assert plays[col].max() <= 1, col
        assert plays[col].min() >= 0, col


# --- C8: a timeout is charged to the team it names, even with an empty mascot -------------------


def test_timeout_team_matching_empty_mascot_and_substrings():
    # "Timeout Indiana" contains Notre Dame's abbreviation "nd"; an empty mascot is contained in
    # every string. Either way the timeout used to be charged to both teams.
    plays = _plays(401677179, mascot="").filter(pl.col("type.text") == "Timeout")
    assert plays.height == 7
    both = plays.filter(pl.col("homeTimeoutCalled") & pl.col("awayTimeoutCalled"))
    assert both.height == 0, both["text"].to_list()
    home = plays.filter(pl.col("homeTimeoutCalled"))["text"]  # Notre Dame
    away = plays.filter(pl.col("awayTimeoutCalled"))["text"]  # Indiana
    assert home.str.contains("Notre Dame").all() and away.str.contains("Indiana").all()
    assert home.len() + away.len() == 7


# --- C9: a second run returns the same payload; the caller's summary is not mutated -------------


def test_rerun_returns_payload_and_input_untouched():
    proc, first, summary, snapshot = _processed(401856682)
    assert summary == snapshot
    again = proc.run_processing_pipeline()
    assert again is not None
    assert again["plays"] == first["plays"]


def test_cleaning_rerun_returns_payload_and_input_untouched():
    summary = _summary(401856682)
    snapshot = copy.deepcopy(summary)
    proc = CFBPlayProcess(gameId=401856682)
    proc.espn_cfb_pbp(summary=summary)
    first = proc.run_cleaning_pipeline()
    assert summary == snapshot
    assert proc.run_cleaning_pipeline() is first


# --- C10: odds provenance is part of the returned payload ---------------------------------------


def test_odds_source_returned():
    proc, result, _, _ = _processed(401856682)
    assert result["odds_source"] == proc.odds_source
    assert result["odds_source"] in {"summary_pickcenter", "core_odds_api", "default", "injected"}


# --- C16: punt returner names that are not "X.Surname" -------------------------------------------


def test_punt_returner_beyond_abbreviated_names():
    plays = _plays(401858426).filter(pl.col("text").str.contains("#2 R.Vander Zee return", literal=True))
    assert plays.height == 2
    assert plays["punt_return_player_name"].to_list() == ["R.Vander Zee", "R.Vander Zee"]


# --- C17: a doubled space in "sacked  by" / "return  for" -------------------------------------------


def test_double_space_sack_and_return_text():
    row = _row(_plays(401762858), "T. Roberson sacked  by K. Demma for -3 yds")
    assert (row["sack_player_name"], row["passer_player_name"]) == ("K. Demma", "T. Roberson")
    assert row["yds_fumble_return"] == 85


# --- C18: a null mascot is processed exactly like an empty one ------------------------------------


def test_null_mascot_is_empty_not_the_string_none():
    null_mascot = _plays(401677179, mascot=None)
    assert null_mascot["homeTeamMascot"].unique().to_list() == [""]
    assert null_mascot["awayTeamMascot"].unique().to_list() == [""]
    assert_frame_equal(null_mascot, _plays(401677179, mascot=""))


# --- C19: cfb_pbp_json() returns the attached payload ---------------------------------------------


def test_cfb_pbp_json_returns_the_attached_summary():
    proc = CFBPlayProcess(gameId=401856682)
    loaded = proc.espn_cfb_pbp(summary=_summary(401856682))
    assert proc.cfb_pbp_json() is loaded
    assert proc.json is loaded


# --- C20: the payload's timeout lists agree with the capped counts --------------------------------


def test_timeout_lists_agree_with_counts():
    period = pl.col("period.number")
    window = (
        pl.when(period <= 2)
        .then(pl.lit("1"))
        .when(period <= 4)
        .then(pl.lit("2"))
        .otherwise(pl.format("OT{}", period - 4))
    )
    for game_id in (401856682, 401112081):
        proc, result, _, _ = _processed(game_id)
        plays = proc.plays_frame.with_columns(window=window)
        for side, team_id in (("home", proc.homeTeamId), ("away", proc.awayTeamId)):
            lists = result["timeouts"][team_id]
            last = plays.group_by("window", maintain_order=True).agg(
                pl.col(f"end.{side}TeamTimeouts").last(), pl.col("period.number").first()
            )
            for key, remaining, first_period in last.iter_rows():
                allotted = 3 if first_period <= 4 else 1
                assert len(lists.get(key, [])) == allotted - remaining, (game_id, side, key)
            ids = [i for key_ids in lists.values() for i in key_ids]
            charged = plays.filter(pl.col("id").is_in(ids))
            assert charged.height == len(ids) and charged[f"{side}TimeoutCalled"].all()
    # Texas logged four timeouts in the first half of 2026 Ohio State @ Texas; Baylor's overtime
    # timeouts of 2019 Baylor @ TCU used to be listed under the second half.
    assert len(_processed(401856682)[1]["timeouts"][251]["1"]) == 3
    baylor = _processed(401112081)[1]["timeouts"][239]
    assert (len(baylor["2"]), len(baylor["OT1"])) == (3, 1)


# --- C21: shortened team names in timeout rows ------------------------------------------------------


def test_timeout_shortened_team_names():
    for game_id, short, team_side in (
        (400869270, "Timeout CENTRAL MICH", "away"),
        (401032062, "Timeout WESTRN MICHIGAN", "away"),
    ):
        timeouts = _plays(game_id).filter(pl.col("type.text") == "Timeout")
        assert (timeouts["homeTimeoutCalled"] != timeouts["awayTimeoutCalled"]).all(), game_id
        named = timeouts.filter(pl.col("text").str.starts_with(short))
        assert named.height >= 3 and named[f"{team_side}TimeoutCalled"].all(), game_id
    # a row that names no team is charged to nobody
    blank = _plays(401752746).filter(pl.col("text").str.starts_with("Timeout , clock"))
    assert blank.height == 14
    assert not (blank["homeTimeoutCalled"] | blank["awayTimeoutCalled"]).any()
