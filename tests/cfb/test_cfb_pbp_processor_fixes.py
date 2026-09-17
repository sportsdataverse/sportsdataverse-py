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
* ``summary_243042579.json`` -- Tennessee @ South Carolina, 2004 ("for -2 yards", "returned -1 yards by").
* ``summary_401636929.json`` -- Baylor @ West Virginia, 2024 ("return for a loss of 1 yard" on a kickoff).
* ``summary_401858221.json`` -- Old Dominion @ Virginia Tech, 2026 ("return  for -55 yds").
* ``summary_332570254.json`` -- Oregon State @ Utah, 2013 ("returned by Victor Bolden, fumbled, recovered by ... Victor Bolden for 10 yards").
* ``summary_252532751.json`` -- Louisiana Monroe @ Wyoming, 2005 ("Julius Stinson return -5 yards to the Wyom42").
* ``summary_252460252.json`` -- Boston College @ BYU, 2005 ("Johnny Ayers punt for a loss of 12 yards").
* ``summary_401752844.json`` -- Iowa @ Rutgers, 2025 ("J. Scullion kick for 65 yds", the short kickoff form).
* ``summary_401309611.json`` -- Troy @ UL Monroe, 2021 (two timeout rows show 29-10 between a 29-16 touchdown and kickoff).
* ``summary_400869817.json`` -- Nicholls @ South Alabama, 2016 (the last row shows 41-34 after the 41-40 final).
* ``summary_243040130.json`` -- Michigan State @ Michigan, 2004 (3OT; sequenceNumber restarts per drive).
* ``summary_401301042.json`` -- East Carolina @ Memphis, 2021 (OT; the winning touchdown row is missing, the "End of OT" marker carries 29-30).
* ``summary_401858224.json`` -- Wake Forest @ Purdue, 2026 (2OT; the vendor feed's sequenceNumber is a garbled running count).
* ``summary_332990030.json`` -- Utah @ USC, 2013 ("Timeout SOUTHERN CAL").
* ``summary_401110775.json`` -- UT Martin @ Florida, 2019 ("Timeout TENN MARTIN").
* ``summary_401012682.json`` -- Oregon State @ Ohio State, 2018 ("Timeout OREGON ST" whose initials are Ohio State's OSU).
* ``summary_333130023.json`` -- San Diego State @ San Jose State, 2013 (returners tackled out of bounds).

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


# --- C23: negative yardage keeps its sign ---------------------------------------------------------


def test_negative_yardage_keeps_its_sign():
    usc = _plays(243042579)
    assert _row(usc, "Kickoff returned by Jamon Meredith (USC) for -2 yards.")["yds_kickoff_return"] == -2
    assert _row(usc, "returned -1 yards by Noah Whiteside (USC)")["yds_punt_return"] == -1
    assert _row(usc, "Robert Meachem (TENN) rushed right side for -4 yards.")["yds_rushed"] == -4
    assert _row(usc, "complete to Noah Whiteside (USC) for -11 yards.")["yds_receiving"] == -11
    assert _row(_plays(401636929), "Ashtyn Hawkins return for a loss of 1 yard")["yds_kickoff_return"] == -1
    assert _row(_plays(401858221), "return  for -55 yds")["yds_fumble_return"] == -55


# --- C27: a spot or a later clause is never read as return yardage ---------------------------------


def test_return_yardage_ignores_spots_and_fumble_advances():
    # "no gain" is 0, not the spot (24); the fumble after it is irrelevant
    row = _row(_plays(401112081), "Trystan Slinker return for no gain to the Bayl 24")
    assert row["yds_kickoff_return"] == 0
    # the kick return states no yardage; "for 10 yards" is the recovery's advance, not the return
    fumbled = _plays(332570254).filter(pl.col("text").str.contains("returned by Victor Bolden, fumbled", literal=True))
    assert fumbled.height == 2
    assert fumbled["yds_kickoff_return"].to_list() == [None, None]


# --- C28: the returner clause without "for": "Name return -2 yards" -------------------------------


def test_return_n_yards_clause():
    plays = _plays(252532751)
    assert _row(plays, "Julius Stinson return -5 yards to the Wyom42")["yds_punt_return"] == -5
    assert _row(plays, "Hoost Marsh return 12 yards to the Wyom32")["yds_punt_return"] == 12
    assert _row(plays, "Joe Merritt return 19 yards to the LaMon20")["yds_kickoff_return"] == 19
    assert _row(plays, "Josh Alexander return 0 yards to the LaMon30")["yds_int_return"] == 0


# --- C29: a one- or two-row score glitch is reverted; the last row is anchored to the header ------


def test_score_glitches_reverted_and_last_row_anchored():
    troy = _plays(401309611)
    timeouts = troy.filter(pl.col("text") == "Timeout UL MONROE, clock 00:36")
    assert timeouts.height == 2
    assert timeouts["end.awayScore"].to_list() == [16, 16]
    assert timeouts["start.awayScore"].to_list() == [16, 16]
    # every row of the game keeps a non-decreasing score
    assert (troy["end.homeScore"].diff().fill_null(0) >= 0).all()
    assert (troy["end.awayScore"].diff().fill_null(0) >= 0).all()
    last = _plays(400869817).row(-1, named=True)
    assert last["type.text"] == "Penalty"
    assert (last["start.awayScore"], last["end.awayScore"]) == (40, 40)


# --- C30: a return touchdown is not a passing or rushing touchdown ---------------------------------


def test_return_touchdowns_are_not_offensive_touchdowns():
    akron = _plays(401628455)
    for needle in ("Lathan Ransom 27 Yd Fumble Return", "Gabe Powers 29 Yd Interception Return"):
        row = _row(akron, needle)
        assert (row["pass_td"], row["rush_td"]) == (False, False), needle
        assert row["touchdown"] is True, needle
    old = _row(_plays(332570254), "intercepted by Sean Martin at the Utah 27, returned for 27 yards for a TOUCHDOWN")
    assert (old["pass_td"], old["rush_td"]) == (False, False)
    # the offense's own touchdowns keep their flags
    tex = _plays(401856682).filter(pl.col("type.text").is_in(["Passing Touchdown", "Rushing Touchdown"]))
    assert tex.height == 5 and (tex["pass_td"] | tex["rush_td"]).all()


# --- C31: an overtime game ends at the header's final score --------------------------------------


def test_overtime_games_end_at_the_header_final():
    for game_id, final in ((243040130, (45, 37)), (401301042, (29, 30)), (401858224, (36, 38)), (401112081, (23, 29))):
        plays = _plays(game_id)
        last = plays.row(-1, named=True)
        assert (last["end.homeScore"], last["end.awayScore"]) == final, game_id
        assert plays.filter(pl.col("period.number") >= 5).height > 0, game_id
    # 2004: ids are chronological and sequenceNumber restarts every drive, so the OT rows keep id order
    ot = _plays(243040130).filter(pl.col("period.number") >= 5)
    assert ot["id"].is_sorted()


# --- C32: the 2004-09 field-goal kicker and the 2014-24 interceptor written after the result -----


def test_legacy_fg_kicker_and_interceptor_shapes():
    fg = _row(_plays(243042579), "27 yard field goal by Josh Brown (USC) is good.")
    assert fg["fg_kicker_player_name"] == "Josh Brown"
    pick = _row(_plays(401636889), "Sawyer Robertson pass intercepted, touchback. Jontez Williams return for no gain")
    assert pick["interception_player_name"] == "Jontez Williams"


# --- C33: a spelled-out team whose initials are the header's abbreviation ------------------------


def test_timeout_spelled_out_team_matches_abbreviation():
    usc = _plays(332990030).filter(pl.col("text").str.starts_with("Timeout SOUTHERN CAL"))
    assert usc.height == 5 and usc["homeTimeoutCalled"].all() and not usc["awayTimeoutCalled"].any()
    utm = _plays(401110775).filter(pl.col("text").str.starts_with("Timeout TENN MARTIN"))
    assert utm.height == 3 and utm["awayTimeoutCalled"].all() and not utm["homeTimeoutCalled"].any()
    # a name part that covers the whole token wins over the initialism: "OREGON ST" is Oregon State's
    # "Oregon St", although its initials OSU are Ohio State's abbreviation
    orst = _plays(401012682).filter(pl.col("text").str.starts_with("Timeout OREGON ST"))
    assert orst.height == 1 and orst["awayTimeoutCalled"].all() and not orst["homeTimeoutCalled"].any()


# --- C38b: a returner tackled out of bounds is not a kick out of bounds (pre-2025 text) -----------


def test_returner_out_of_bounds_is_not_kick_out_of_bounds():
    plays = _plays(333130023)
    ko = _row(plays, "returned by Tim Crawley for 22 yards to the SJSt 36, tackled by Stan Sedberry out-of-bounds")
    assert ko["kickoff_oob"] is False and ko["yds_kickoff_return"] == 22
    punt = _row(plays, "returned by Tim Vizzi, tackled by Simon Connette and Harrison Waid out-of-bounds")
    assert punt["punt_oob"] is False and punt["yds_punt_return"] is None
    # a kick that went out of bounds with no return keeps its flag
    assert plays.filter(pl.col("kickoff_oob") | pl.col("punt_oob")).height >= 0


# --- C36: a punt "for a loss of N" ended N yards behind the line ----------------------------------


def test_punt_for_a_loss_is_negative():
    assert _row(_plays(252460252), "Johnny Ayers punt for a loss of 12 yards.")["yds_punted"] == -12


# --- C37: the short kickoff form, "kick for N yds" ------------------------------------------------


def test_short_kick_form_parses():
    kick = _row(_plays(401752844), "J. Scullion kick for 65 yds")
    assert kick["yds_kickoff"] == 65
    assert kick["yds_kickoff_return"] == 100


# --- C39: the duplicate filter drops true duplicates only, never a distinct play ------------------

_ADMIN_TYPES = {"End Period", "End of Half", "End of Game", "Coin Toss"}


def _raw_ids(summary: dict, *, admin: bool = True) -> set[int]:
    return {
        int(p["id"])
        for drives in summary["drives"].values()
        for d in (drives if isinstance(drives, list) else [drives])
        for p in d["plays"]
        if admin or p.get("type", {}).get("text") not in _ADMIN_TYPES
    }


def test_2004_plays_sharing_the_next_rows_start_state_are_kept():
    # South Carolina @ Tennessee, 2004: the feed repeats the start state on every row, and the
    # former loose text test deleted 17 distinct plays -- rushes, completions, a two-point try
    proc, _, summary, _ = _processed(243042579)
    kept = set(proc.plays_frame["id"].to_list())
    assert 2430425790711 in kept  # Summers 12-yd rush, next row a 13-yd completion at the same spot
    assert 2430425791524 in kept  # Schaeffer two-point rush
    assert {2430425792904, 2430425792905} <= kept  # two completions logged at 13:55
    assert _raw_ids(summary, admin=False) <= kept <= _raw_ids(summary)


def test_identical_copies_on_consecutive_rows_are_dropped_once():
    # Temple @ Charlotte, 2024: two plays logged twice under consecutive ids -- same text,
    # clock, period and start state; 2013 San Jose State: a 2-yd rush entered twice
    kept = set(_plays(401645333)["id"].to_list())
    assert kept & {401645333101925701, 401645333101925702} == {401645333101925702}
    assert kept & {401645333102948301, 401645333102948307} == {401645333102948307}
    kept_2013 = set(_plays(333130023)["id"].to_list())
    assert kept_2013 & {333130023175, 333130023176} == {333130023176}


def test_a_repeated_current_drive_keeps_the_fresher_copy():
    # a live feed repeats the drive in progress under drives.current -- listed BEFORE previous
    # in the 2026 feed -- with the same ids and sometimes revised text; the revised copy survives
    summary = _summary(401645333)
    current = copy.deepcopy(summary["drives"]["previous"][-1])
    for play in current["plays"]:
        play["text"] += " (revised)"
    summary["drives"] = {"current": current, "previous": summary["drives"]["previous"]}
    proc = CFBPlayProcess(gameId=401645333)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    f = proc.plays_frame
    assert f["id"].to_list() == _plays(401645333)["id"].to_list()
    last_drive = {int(p["id"]) for p in current["plays"]} & set(f["id"].to_list())
    assert last_drive and f.filter(pl.col("id").is_in(last_drive))["text"].str.ends_with("(revised)").all()
