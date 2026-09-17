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
    plays = _plays(401677179, blank_mascots=True).filter(pl.col("type.text") == "Timeout")
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
