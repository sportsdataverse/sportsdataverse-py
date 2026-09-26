"""Round-3 fixes from the processor-invariant sweep (S1-SWEEP, PR #510), on real ESPN data.

Fixtures are nfl-raw summaries trimmed to the keys the processor reads (header,
drives, boxscore, gameInfo, pickcenter), processed offline through
``espn_nfl_pbp(summary=...)``:

* ``summary_221013007_trimmed.json.gz`` -- MIA @ DEN, 2002 week 6 ("First Last (TM)" text)
* ``summary_251113015_trimmed.json.gz`` -- NE @ MIA, 2005 week 10 (score drops, "Sack" stubs)
* ``summary_290927014_trimmed.json.gz`` -- GB @ STL, 2009 week 3 (blocked FG, "Josh.Brown")
* ``summary_291018018_trimmed.json.gz`` -- NYG @ NO, 2009 week 6 (``&apos;`` in the text)
* ``summary_400791508_trimmed.json.gz`` -- WSH @ PHI, 2015 week 16 (end.team flips, "PAT failed")
* ``summary_400874485_trimmed.json.gz`` -- NYG @ PHI, 2016 week 16 ("34 Yrd Interception Return")
* ``summary_261022011_trimmed.json.gz`` -- WSH @ IND, 2006 week 7 (the try booked on the kickoff row)
* ``summary_291115014_trimmed.json.gz`` -- NO @ STL, 2009 week 10 (end.team flipped on the final play)
* ``summary_271209010_trimmed.json.gz`` -- TEN @ SD, 2007 week 14 (the per-row score leads by a row)
* ``summary_240919004_trimmed.json.gz`` -- MIA @ CIN, 2004 week 2 ("A.J. Feeley", "T.J.
  Houshmandzadeh", and two untyped two-minute-warning rows)
* ``summary_400951676_trimmed.json.gz`` -- SF @ SEA, 2017 week 2 (six home points carried by no
  scoring row; the header's final anchors the repair)
* ``summary_400791590_trimmed.json.gz`` -- DAL @ NO, 2015 week 4 (a pick-six ESPN types
  "Interception Return", not "... Touchdown")
* ``summary_401547449_trimmed.json.gz`` -- LA @ IND, 2023 week 4 (two touchdowns with a
  successful two-point conversion, each followed by a clock-stoppage row)
* ``summary_401128075_trimmed.json.gz`` -- JAX @ IND, 2019 week 11 (IND up 31-13 returns a
  JAX try for a defensive two-point conversion)
* ``summary_401030824_trimmed.json.gz`` -- LAC @ DEN, 2018 week 17 (DEN's failed two-point
  try returned by LAC; the text carries both results)
* ``summary_401671740_trimmed.json.gz`` -- PHI @ TB, 2024 week 4 (a blocked extra point
  returned for two inside the touchdown row, with no standalone try row)
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _process(game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = NFLPlayProcess(gameId=game_id)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame.sort("game_play_number")


@pytest.fixture(scope="module")
def mia_den_2002() -> pl.DataFrame:
    return _process(221013007)


@pytest.fixture(scope="module")
def ne_mia_2005() -> pl.DataFrame:
    return _process(251113015)


@pytest.fixture(scope="module")
def gb_stl_2009() -> pl.DataFrame:
    return _process(290927014)


@pytest.fixture(scope="module")
def nyg_no_2009() -> pl.DataFrame:
    return _process(291018018)


@pytest.fixture(scope="module")
def wsh_phi_2015() -> pl.DataFrame:
    return _process(400791508)


def _row(frame: pl.DataFrame, play_id: int) -> dict:
    return frame.filter(pl.col("id") == play_id).row(0, named=True)


# ---------------------------------------------------------------------------
# N18 -- home/away WP after a play is the next play's home/away WP before
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("which", ["wsh_phi_2015", "mia_den_2002"])
def test_home_wp_after_is_never_the_complement_of_the_next_play(request, which):
    f = request.getfixturevalue(which).with_columns(pl.col("home_wp_before").shift(-1).alias("_next"))
    changed = f.filter(
        (pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
        & pl.col("home_wp_after").is_not_null()
        & pl.col("_next").is_not_null()
    )
    assert changed.height >= 10
    complemented = changed.filter(
        ((pl.col("home_wp_after") - (1 - pl.col("_next"))).abs() < 0.01)
        & ((pl.col("home_wp_after") - pl.col("_next")).abs() > 0.05)
    )
    assert complemented.select("id", "type.text").rows() == []
    assert (changed["home_wp_after"] + changed["away_wp_after"] - 1).abs().max() < 1e-6


# ---------------------------------------------------------------------------
# N19 -- an ESPN end.team flip on a play that kept the ball does not flip wp_after
# ---------------------------------------------------------------------------


def test_end_team_flip_on_a_kept_possession_keeps_the_next_plays_perspective(wsh_phi_2015):
    f = wsh_phi_2015.with_columns(
        pl.col("start.pos_team.id").shift(-1).alias("_next_pos"),
        pl.col("wp_before").shift(-1).alias("_lead_wp_before"),
    )
    flipped = f.filter(
        (pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
        & (pl.col("_next_pos") == pl.col("start.pos_team.id"))
        & (pl.col("scoringPlay") == False)  # noqa: E712
        & pl.col("type.text").is_in(["Rush", "Pass Reception", "Pass Incompletion", "Sack"])
    )
    assert flipped.height >= 5  # ESPN's end.team is wrong on eight rows of this game
    assert (flipped["wp_after"] - flipped["_lead_wp_before"]).abs().max() < 1e-6
    assert flipped["wpa"].abs().max() < 0.25


@pytest.fixture(scope="module")
def phi_nyg_2006() -> pl.DataFrame:
    return _process(260917021)


@pytest.fixture(scope="module")
def nyg_phi_2016() -> pl.DataFrame:
    return _process(400874485)


@pytest.fixture(scope="module")
def no_nyj_2013() -> pl.DataFrame:
    return _process(331103020)


@pytest.fixture(scope="module")
def stl_no_2009() -> pl.DataFrame:
    return _process(291115014)


def test_final_row_home_wp_matches_the_result(stl_no_2009, wsh_phi_2015, mia_den_2002):
    # NO 28 @ STL 23: ESPN flips end.team to NO on M.Bulger's last incompletion; the
    # game-over wp_after is judged for the team that threw it, not for ESPN's end team
    last = _row(stl_no_2009, 2911150143853)
    assert last["wp_after"] == 0.0 and last["home_wp_after"] == 0.0 and last["away_wp_after"] == 1.0
    for f, home_won in ((stl_no_2009, False), (wsh_phi_2015, False), (mia_den_2002, False)):
        tail = f.filter(pl.col("home_wp_after").is_not_null()).tail(1).row(0, named=True)
        assert tail["home_wp_after"] == (1.0 if home_won else 0.0)


# ---------------------------------------------------------------------------
# N21 -- a touchdown whose try is written in the text ends at 6, 7 or 8
# ---------------------------------------------------------------------------


def test_touchdown_end_ep_reads_the_inline_try(wsh_phi_2015):
    f = wsh_phi_2015
    assert _row(f, 400791508224)["EP_end"] == 7  # "Ryan Mathews 1 Yard Rush C.Sturgis extra point is GOOD"
    assert _row(f, 400791508947)["EP_end"] == 7  # "... for 12 Yrds D.Hopkins extra point is GOOD"
    assert (
        _row(f, 400791508641)["EP_end"] == 6
    )  # "Jordan Reed 22 Yd pass from Kirk Cousins (Dustin Hopkins PAT failed)"
    assert _row(f, 4007915084040)["EP_end"] == 8  # "... TWO-POINT CONVERSION ATTEMPT. ... ATTEMPT SUCCEEDS."
    tds = f.filter(
        pl.col("type.text").is_in(["Passing Touchdown", "Rushing Touchdown"])
        & pl.col("text").str.contains(r"extra point is|PAT|Kick\)|TWO-POINT")
    )
    assert tds.height >= 6
    assert set(tds["EP_end"].to_list()) <= {6.0, 7.0, 8.0}


# ---------------------------------------------------------------------------
# N22 -- a blocked field goal stays a blocked field goal
# ---------------------------------------------------------------------------


def test_blocked_field_goal_is_not_relabeled_an_extra_point(gb_stl_2009):
    r = _row(gb_stl_2009, 2909270140220)  # "Josh.Brown 48 yard field goal is BLOCKED (J.Jolly), ... RECOVERED by GB"
    assert r["type.text"] == "Blocked Field Goal"
    assert r["fg_attempt"] is True and r["fg_made"] is False
    assert r["EP_end"] < 0 and r["EPA"] < -1.5  # a turnover swing, not the -0.92 of a missed PAT


# ---------------------------------------------------------------------------
# N24 -- a team's score never drops (a feed error is repaired)
# ---------------------------------------------------------------------------


def test_score_drops_are_repaired(ne_mia_2005):
    f = ne_mia_2005
    for col in ("homeScore", "awayScore", "end.homeScore", "end.awayScore"):
        assert (f[col].diff().fill_null(0) >= 0).all(), col
    # ESPN wrote 7-12 on two non-scoring plays of a 7-6 drive, then 7-6 again
    assert [_row(f, i)["end.awayScore"] for i in (2511130152735, 2511130152756, 2511130152798)] == [6, 6, 6]
    assert _row(f, 2511130153777)["end.homeScore"] == 16  # ESPN wrote 10 after a scoring play made it 16
    assert f.tail(1).select("end.homeScore", "end.awayScore").row(0) == (16, 23)  # the header's final


@pytest.fixture(scope="module")
def ind_wsh_2006() -> pl.DataFrame:
    return _process(261022011)


def test_a_try_recorded_on_the_next_row_is_kept(ind_wsh_2006):
    # WSH's two-point try at 0:25 is booked on the kickoff row (36-20 -> 36-22,
    # scoringPlay False) and the game ends 36-22; a rise the feed never takes back is real
    f = ind_wsh_2006
    assert _row(f, 2610220114009)["end.awayScore"] == 22
    assert f.tail(1).select("end.homeScore", "end.awayScore").row(0) == (36, 22)  # the header's final
    assert (f["end.awayScore"].diff().fill_null(0) >= 0).all()


@pytest.fixture(scope="module")
def sd_ten_2007() -> pl.DataFrame:
    return _process(271209010)


def test_a_score_booked_on_the_row_before_the_kick_stays_on_the_kick(sd_ten_2007):
    # the 2007 feed's per-row score leads by a row: the incompletion before Nate
    # Kaeding's 20-yarder already reads 3; the kick is the scoring row
    f = sd_ten_2007
    assert _row(f, 2712090102153)["end.awayScore"] == 0
    fg = _row(f, 2712090102175)
    assert (fg["start.awayScore"], fg["end.awayScore"]) == (0, 3)


# ---------------------------------------------------------------------------
# N25 -- a defensive return touchdown is not an offensive pass_td / rush_td
# ---------------------------------------------------------------------------


def test_interception_return_touchdown_is_not_a_passing_touchdown(mia_den_2002):
    r = _row(
        mia_den_2002, 2210130072701
    )  # "... intercepted by Patrick Surtain (MIA). Returned for a 40 yard touchdown."
    assert r["type.text"] == "Interception Return Touchdown" and r["int"] is True and r["td_play"] is True
    assert r["pass_td"] is False and r["rush_td"] is False
    assert not r["yds_receiving"]


# ---------------------------------------------------------------------------
# N26 -- a sack is never a pass attempt, whatever the text says
# ---------------------------------------------------------------------------


def test_sack_stubs_are_not_pass_attempts(phi_nyg_2006):
    sacks = phi_nyg_2006.filter(pl.col("type.text") == "Sack")
    assert sacks.height >= 7 and (sacks["text"] == "Sack").all()
    assert sacks["sack"].all()
    assert not sacks["pass_attempt"].any() and not sacks["target"].any()


# ---------------------------------------------------------------------------
# N20 -- ESPN's pre-2015 "Pass Interception" rows are pass plays
# ---------------------------------------------------------------------------


def test_pre_2015_interceptions_are_pass_plays(mia_den_2002):
    ints = mia_den_2002.filter(pl.col("id").is_in([2210130071212, 2210130072309]))
    assert ints["type.text"].to_list() == ["Interception Return", "Interception Return"]
    assert ints["int"].all() and ints["pass"].all() and ints["pass_attempt"].all()
    assert not mia_den_2002.filter((pl.col("int") == True) & (pl.col("pass") == False)).height  # noqa: E712


# ---------------------------------------------------------------------------
# N28 -- scoring-summary text ("51 Yd Field Goal", "Pass From X for N Yrds") is parsed
# ---------------------------------------------------------------------------


def test_scoring_summary_kickers_passers_and_receivers(wsh_phi_2015):
    f = wsh_phi_2015
    assert _row(f, 4007915081769)["fg_kicker_player_name"] == "C.Sturgis"  # "Caleb Sturgis 34 Yd Field Goal"
    assert _row(f, 4007915082166)["fg_kicker_player_name"] == "D.Hopkins"
    reed = _row(f, 400791508947)  # "Jordan Reed Pass From Kirk Cousins for 12 Yrds D.Hopkins extra point is GOOD"
    assert (reed["passer_player_name"], reed["receiver_player_name"]) == ("K.Cousins", "J.Reed")
    thompson = _row(f, 4007915083091)
    assert (thompson["passer_player_name"], thompson["receiver_player_name"]) == ("K.Cousins", "C.Thompson")
    # "Pierre Garcon Pass From Kirk Cousins for 13 Yrds TWO-POINT CONVERSION ATTEMPT.
    # K.Cousins pass to J.Crowder is complete. ATTEMPT SUCCEEDS.": the try's target is not the receiver
    garcon = _row(f, 4007915084040)
    assert (garcon["passer_player_name"], garcon["receiver_player_name"]) == ("K.Cousins", "P.Garcon")


def test_scoring_summary_interceptor_is_named(nyg_phi_2016):
    f = nyg_phi_2016
    r = _row(f, 400874485377)  # "Malcolm Jenkins 34 Yrd Interception Return C.Sturgis extra point is GOOD, ..."
    assert r["int_td"] is True and r["interception_player_name"] == "M.Jenkins"
    assert _row(f, 400874485857)["fg_kicker_player_name"] == "R.Gould"  # "Robbie Gould 35 Yd Field Goal"


@pytest.mark.parametrize(
    ("text", "kicker", "interceptor"),
    [
        # verbatim ESPN scoring-summary rows (nfl-raw 2015-16); the spelled-out name
        # folds to the "X.Surname" form the player's other rows use
        ("Caleb Sturgis 34 Yd Field Goal ", "C.Sturgis", None),
        ("Malcolm Jenkins 34 Yd Interception Return (Caleb Sturgis Kick)", None, "M.Jenkins"),
        ("Marcus Cooper 60 Yd Interception Return  ", None, "M.Cooper"),
        (
            "Malcolm Jenkins 34 Yrd Interception Return C.Sturgis extra point is GOOD, Center-R.Lovato",
            None,
            "M.Jenkins",
        ),
    ],
)
def test_scoring_summary_kicker_and_interceptor_fold_to_the_abbreviated_form(text, kicker, interceptor):
    from sportsdataverse.nfl.nfl_pbp import _NFL_LEGACY_FG_KICKER_RE, _NFL_LEGACY_INTERCEPTOR_RE, _abbreviated_name

    r = (
        pl.DataFrame({"text": [text]})
        .with_columns(k=_abbreviated_name(_NFL_LEGACY_FG_KICKER_RE), i=_abbreviated_name(_NFL_LEGACY_INTERCEPTOR_RE))
        .row(0, named=True)
    )
    assert (r["k"], r["i"]) == (kicker, interceptor)


# ---------------------------------------------------------------------------
# N31 -- a field goal starts at ESPN's own line of scrimmage
# ---------------------------------------------------------------------------


def test_field_goals_start_at_the_snap_spot(wsh_phi_2015, ne_mia_2005, mia_den_2002, gb_stl_2009):
    assert _row(wsh_phi_2015, 4007915081769)["start.yardsToEndzone"] == 16  # "34 Yd Field Goal" from the PHI 16
    assert _row(wsh_phi_2015, 4007915082166)["start.yardsToEndzone"] == 10  # "28 Yd Field Goal" from the 10
    # ESPN's own yard line wins over the kick distance: "pushed ob at SL 8" then a
    # 25-yard kick on 4th-and-6 from the 8 (a 7-yard hold), not from the 7
    assert _row(gb_stl_2009, 2909270140918)["start.yardsToEndzone"] == 8
    assert _row(gb_stl_2009, 2909270140220)["start.yardsToEndzone"] == 31  # 48-yard attempt, 4th-and-8 from the 31
    # 2005: the text is empty (no kick distance), ESPN's yard line is the only spot
    fgs = ne_mia_2005.filter(pl.col("fg_attempt") == True)  # noqa: E712
    assert fgs.height == 5 and fgs["start.yardsToEndzone"].null_count() == 0
    assert _row(ne_mia_2005, 2511130151894)["start.yardsToEndzone"] == 17
    # 2002: ESPN's yardsToEndzone is 0 on every play; its yard line is right
    assert _row(mia_den_2002, 2210130070109)["start.yardsToEndzone"] == 15  # "33 yard field goal by Jason Elam"
    assert _row(mia_den_2002, 2210130073209)["start.yardsToEndzone"] == 37  # "55 yard field goal by Jason Elam"


# ---------------------------------------------------------------------------
# N15 -- "Direct snap to X." is not the receiver
# ---------------------------------------------------------------------------


def test_direct_snap_taker_is_not_the_receiver(no_nyj_2013):
    r = _row(no_nyj_2013, 3311030201335)  # "Direct snap to J.Cribbs.  J.Cribbs pass deep right to Z.Sudfeld to NO 15"
    assert (r["passer_player_name"], r["receiver_player_name"]) == ("J.Cribbs", "Z.Sudfeld")


# ---------------------------------------------------------------------------
# N16 -- ids resolve for four-letter initials and every name suffix is stripped
# ---------------------------------------------------------------------------


def test_four_letter_initials_resolve_to_an_id():
    with gzip.open(FIX / "summary_290927014_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = NFLPlayProcess(gameId=290927014)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    # ESPN writes the kicker "Josh.Brown"; the box lists "Josh Brown" (STL)
    f = proc.plays_frame.filter(pl.col("id").is_in([2909270142420, 2909270141570])).with_columns(
        fg_kicker_player_name=pl.lit("Josh.Brown"), kickoff_player_name=pl.lit("Josh.Brown")
    )
    out = proc._NFLPlayProcess__attach_player_ids(f)
    assert out["fg_kicker_player_id"].to_list()[0] == out["kickoff_player_id"].to_list()[1]
    assert out["fg_kicker_player_id"].to_list()[0] is not None


@pytest.mark.parametrize(
    ("name", "stripped"),
    [
        ("D.Jones Jr.", "D.Jones"),
        ("J.Ruffin, Jr.", "J.Ruffin"),
        ("R.Royal, III", "R.Royal"),
        ("W.Snead IV", "W.Snead"),
        ("K.Williams V", "K.Williams"),
        ("M.Smith lll", "M.Smith"),
        ("A.St. Brown", "A.St. Brown"),
    ],
)
def test_name_suffixes_are_stripped_like_the_shared_grammar(name, stripped):
    from sportsdataverse.nfl.nfl_pbp import _NFL_NAME_SUFFIX_RE

    assert _NFL_NAME_SUFFIX_RE.sub("", name) == stripped


# ---------------------------------------------------------------------------
# N17 -- the 2008-10 feed's HTML entities are unescaped at the boundary
# ---------------------------------------------------------------------------


def test_html_entities_are_unescaped_at_load(nyg_no_2009):
    f = nyg_no_2009
    assert not f["text"].str.contains("&apos;").any()
    ko = _row(f, 2910180182224)  # "T.Morstead kicks 49 yards ... W.Beatty (didn't try to advance) to NYG 21"
    assert ko["kickoff_return_player_name"] == "W.Beatty"
    assert "S.O'Hara" in _row(f, 2910180184340)["text"]


# ---------------------------------------------------------------------------
# N27 -- the 2002-09 "First Last (TM) verb" grammar
# ---------------------------------------------------------------------------


def test_legacy_team_tagged_text_names_players(mia_den_2002):
    f = mia_den_2002
    assert (
        _row(f, 2210130070105)["rusher_player_name"] == "C.Portis"
    )  # "Clinton Portis (DEN) rushed left side for 1 yard."
    pas = _row(f, 2210130070101)  # "Brian Griese (DEN) pass left side complete to Clinton Portis (DEN) for 9 yards."
    assert (pas["passer_player_name"], pas["receiver_player_name"]) == ("B.Griese", "C.Portis")
    assert (
        _row(f, 2210130070409)["passer_player_name"] == "B.Griese"
    )  # "Brian Griese (DEN) sacked for a loss of 17 yards."
    punt = _row(
        f, 2210130070304
    )  # "Punt by Mark Royals (MIA) returned 15 yards by Deltha O'Neal (DEN) to the Miami 26."
    assert (punt["punter_player_name"], punt["punt_return_player_name"]) == ("M.Royals", "D.O'Neal")
    assert (
        _row(f, 2210130070201)["kickoff_return_player_name"] == "T.Minor"
    )  # "Kickoff returned by Travis Minor (MIA) for 28 yards."
    assert (
        _row(f, 2210130070109)["fg_kicker_player_name"] == "J.Elam"
    )  # "33 yard field goal by Jason Elam (DEN) is good."
    assert _row(f, 2210130071212)["interception_player_name"] == "M.Reagor"  # "... intercepted by Montae Reagor (DEN)."
    fum = _row(
        f, 2210130071105
    )  # "Denver fumble by Mike Anderson (DEN), recovered by Larry Chester (MIA), returned for no gain."
    assert (fum["fumble_player_name"], fum["fumble_recovered_player_name"]) == ("M.Anderson", "L.Chester")
    assert _row(f, 2210130070909)["xp_kicker_player_name"] == "O.Mare"  # "Extra point by Olindo Mare (MIA) is good."
    assert _row(f, 2210130072915)["xp_kicker_player_name"] == "J.Elam"  # "Extra point by Jason Elam (DEN) is good."
    for flag, col in (
        ("rush", "rusher_player_name"),
        ("punt", "punter_player_name"),
        ("pass_attempt", "passer_player_name"),
    ):
        rows = f.filter(pl.col(flag) == True)  # noqa: E712
        assert rows[col].null_count() <= 0.05 * rows.height, (col, rows[col].null_count(), rows.height)


# ---------------------------------------------------------------------------
# N29 -- the 4th-down surface hands sdv's EP/WP the game roof, not nfl4th's model_roof
# ---------------------------------------------------------------------------


def test_fourth_down_prepare_passes_the_game_roof_to_sdv_models(monkeypatch):
    import pandas as pd

    import sportsdataverse.nfl.nfl_fourth_down as fd

    seen = []

    def spy(fn):
        def wrapper(df, *a, **k):
            seen.append(df["roof"].to_list())
            return fn(df, *a, **k)

        return wrapper

    monkeypatch.setattr(fd, "calculate_expected_points", spy(fd.calculate_expected_points))
    monkeypatch.setattr(fd, "calculate_win_probability", spy(fd.calculate_win_probability))
    view = pd.DataFrame(
        {
            "play_id": [1, 2, 3],
            "game_id": [401671835] * 3,
            "season": [2024] * 3,
            "posteam": ["4", "7", "4"],
            "defteam": ["7", "4", "7"],
            "home_team": ["4"] * 3,
            "away_team": ["7"] * 3,
            "roof": ["closed", "open", "outdoors"],
            "qtr": [2, 3, 4],
            "quarter_seconds_remaining": [120.0, 600.0, 30.0],
            "ydstogo": [3.0, 8.0, 1.0],
            "yardline_100": [40.0, 55.0, 2.0],
            "score_differential": [-3.0, 7.0, 0.0],
            "posteam_timeouts_remaining": [3, 2, 1],
            "defteam_timeouts_remaining": [2, 3, 1],
            "home_opening_kickoff": [1, 1, 1],
            "spread_line": [-2.5] * 3,
            "total_line": [44.5] * 3,
        }
    )
    d = fd._prepare(view)
    fd._calc_wp(d)  # nfl4th's WP averages its own model with sdv's EP and WP scorers
    assert seen and all(roofs == ["closed", "open", "outdoors"] for roofs in seen)
    # nfl4th's own fd / 2pt features keep nfl4th's mapping (open/closed -> retractable)
    assert d["retractable"].tolist() == [1, 1, 0] and d["dome"].tolist() == [0, 0, 0]


# ---------------------------------------------------------------------------
# Stage-1 tail: FG-formation penalties, mistyped admin rows, double initials
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mia_cin_2004() -> pl.DataFrame:
    return _process(240919004)


def test_a_penalty_in_field_goal_formation_is_not_a_field_goal_attempt(nyg_phi_2016, wsh_phi_2015):
    """A "(Field Goal formation)" clause is a formation annotation, not an attempt."""
    pen = _row(nyg_phi_2016, 4008744853519)
    # "(5:36) (Field Goal formation) PENALTY on NYG-B.Jones, False Start, ... - No Play."
    assert pen["type.text"] == "Penalty"
    assert pen["fg_attempt"] is False
    assert pen["fg_kicker_player_name"] is None
    # a real kick in the same game still reads as an attempt
    fg = _row(nyg_phi_2016, 400874485857)  # "Robbie Gould 35 Yd Field Goal"
    assert fg["fg_attempt"] is True and fg["fg_kicker_player_name"] == "R.Gould"
    # the punt twin needs no guard: ``punt`` reads type.text only, never the text
    punt_pen = wsh_phi_2015.filter(pl.col("text").str.contains(r"\(Punt formation\) PENALTY"))
    assert punt_pen.height == 1
    assert punt_pen["punt"].to_list() == [False]
    assert punt_pen["type.text"].to_list() == ["Penalty"]


def test_mistyped_two_minute_warning_rows_are_admin_rows(mia_den_2002, mia_cin_2004):
    """ESPN types the 2002-04 two-minute-warning rows as a play; retype them."""
    # 2002: ESPN labelled them "Missed Field Goal Return", which booked fg_attempt
    for play_id in (2210130071207, 2210130073201):
        r = _row(mia_den_2002, play_id)
        assert r["text"].strip() == "2 minute warning."
        assert r["type.text"] == "Two-minute warning"
        assert r["fg_attempt"] is False and r["play"] is False
    assert mia_den_2002.filter(pl.col("type.text") == "Missed Field Goal Return").height == 0
    # the game's real missed field goal is untouched
    missed = _row(mia_den_2002, 2210130070604)  # "48 yard field goal by Olindo Mare (MIA) is no good."
    assert missed["fg_attempt"] is True and missed["fg_kicker_player_name"] == "O.Mare"
    # 2003-04: ESPN ships no type at all on those rows
    for play_id in (2409190041608, 2409190043804):
        r = _row(mia_cin_2004, play_id)
        assert r["type.text"] == "Two-minute warning" and r["play"] is False


def test_double_initial_names_fold_like_the_abbreviated_grammar(mia_cin_2004):
    """Double initials ("A.J. Feeley", "T.J. Houshmandzadeh") fold to the box's key."""
    pas = _row(mia_cin_2004, 2409190040204)  # "A.J. Feeley (MIA) pass left side complete to ..."
    assert pas["passer_player_name"] == "A.J.Feeley" and pas["passer_player_id"] == "2704"
    rush = _row(mia_cin_2004, 2409190040805)  # "A.J. Feeley (MIA) rushed left side for 3 yards."
    assert rush["rusher_player_name"] == "A.J.Feeley" and rush["rusher_player_id"] == "2704"
    ret = _row(mia_cin_2004, 2409190041101)  # "Kickoff returned by T.J. Houshmandzadeh (CIN) for 19 yards."
    assert ret["kickoff_return_player_name"] == "T.J.Houshmandzadeh"
    assert ret["kickoff_return_player_id"] == "2753"
    # a single-initial name in the same game is unchanged, and no fold doubles the dot
    single = _row(mia_cin_2004, 2409190040403)
    assert single["receiver_player_name"] == "L.Gordon"
    for col in ("passer_player_name", "rusher_player_name", "receiver_player_name"):
        assert mia_cin_2004.filter(pl.col(col).str.contains(r"\.\.")).height == 0


# ---------------------------------------------------------------------------
# N36 -- the header's final score anchors the repair (CFB's C29)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sf_sea_2017() -> pl.DataFrame:
    return _process(400951676)


def test_a_rise_to_the_header_final_that_the_game_keeps_is_kept(sf_sea_2017):
    """SEA's second field goal exists only in the header: 6 -> 12 on a non-scoring row.

    Without the anchor the persistence rule reverts that rise (it is neither a scoring
    row nor the row after one), so the game ended 6-9 -- a SEA loss on a SEA win, which
    also left the last row's ``home_wp_after`` at 0.0.
    """
    f = sf_sea_2017
    assert f.tail(1).select("end.homeScore", "end.awayScore").row(0) == (12, 9)  # the header's final
    # the rise lands on the kickoff after the kick ("B.Walsh kicks 63 yards from SEA 35 ...")
    assert _row(f, 4009516763785)["end.homeScore"] == 12
    for col in ("homeScore", "awayScore"):
        assert (f[col].diff().fill_null(0) >= 0).all(), col
    assert float(f["home_wp_after"].drop_nulls()[-1]) > 0.9  # SEA won


# --- O3: a defensive return TD is not the offense's touchdown -------------------------------------


def test_pick_six_typed_interception_return_is_not_a_passing_touchdown():
    # "T.Romo pass short left intended for D.Street INTERCEPTED by T.McBride ... T.McBride for 20
    # yards, TOUCHDOWN." ESPN types the row "Interception Return", which is not in
    # ``defense_score_vec``, so ``pass & td_play`` booked it as Romo's passing touchdown.
    row = _row(_process(400791590), 4007915903306)
    assert row["type.text"] == "Interception Return"
    assert row["td_play"] and row["int"]
    assert not row["pass_td"] and not row["rush_td"]
    assert row["yds_receiving"] == 0
    assert row["yds_int_return"] == 20


# --- N38: an admin row after a score carries the post-score EP, not the score's -------------------


def test_clock_stoppage_after_a_score_does_not_inherit_the_realized_ep():
    # The timeout ESPN emits between a touchdown and the kickoff inherits ``lag_EP_end``, which
    # after "TWO-POINT CONVERSION ATTEMPT ... ATTEMPT SUCCEEDS" is the realized 8.0 rather than a
    # field-position expectation, so EP_start/EP_end published 8.0 -- outside the [-7, 7] band the
    # invariants assert.  Both stoppages take the post-score 0.92 the WP path already uses.
    f = _process(401547449)
    scored = f.filter(pl.col("EP_end") == 8.0)
    assert scored["type.text"].to_list() == ["Passing Touchdown", "Passing Touchdown"]

    # every clock-stoppage row that follows a score takes the post-score 0.92 the WP path
    # already uses, whatever the score was worth
    after_score = f.filter(
        pl.col("type.text").is_in(["Official Timeout", "Timeout", "Two-minute warning"])
        & (pl.col("lag_scoringPlay") == True)  # noqa: E712
    )
    assert after_score.height == 8
    assert after_score["EP_start"].to_list() == [0.92] * 8
    assert after_score["EP_end"].to_list() == [0.92] * 8
    assert after_score["EPA"].to_list() == [0.0] * 8

    # nothing else is out of band, and EPA on the admin rows is still zero
    assert f["EP_start"].drop_nulls().abs().max() <= 7.0
    non_scoring = f.filter(pl.col("EP_end").is_not_null() & (pl.col("scoring_play") == False))  # noqa: E712
    assert non_scoring["EP_end"].abs().max() <= 7.0


# --- a defensive two-point conversion is scored as a try -------------------------------------------


def test_defensive_two_point_conversion_is_scored_as_a_try() -> None:
    """401128075: IND returns a JAX try for two, IND up 31-13 in the fourth.

    The kicking team goes from a try's expected value to -2, so EPA is about -2.9, and an
    18 -> 20 point deficit this late cannot swing win probability. It published EPA -7.26:
    "Defensive 2pt Conversion" was missing from the fixed-EP try list, so the model scored
    ESPN's down-0 start state as a scrimmage snap (EP 5.26). The CFB twin is #571.
    """
    plays = _process(401128075).with_row_index("i")
    d2p = plays.filter(pl.col("type.text") == "Defensive 2pt Conversion")
    assert d2p.height == 1
    r = d2p.row(0, named=True)
    assert r["EP_start"] == 0.92
    assert -4 <= r["EPA"] <= -2, r["EPA"]
    assert abs(r["wpa"]) < 0.05, r["wpa"]

    td, _, kickoff = plays.filter(pl.col("i").is_between(r["i"] - 1, r["i"] + 1)).iter_rows(named=True)
    assert (td["type.text"], kickoff["type.text"]) == ("Passing Touchdown", "Kickoff")
    # JAX (30) scores and tries; IND (11) receives the kickoff JAX then makes
    assert [td["pos_team"], r["pos_team"], kickoff["pos_team"]] == [30, 30, 11]
    # WP hands over across the three rows: the try starts where the TD ended, and ends
    # where the kickoff starts, restated from the receiver's frame into JAX's
    assert abs(td["wp_after"] - r["wp_before"]) < 0.01
    assert r["wp_after"] == pytest.approx(1 - kickoff["wp_before"])
    assert r["home_wp_after"] == pytest.approx(kickoff["home_wp_before"])


# --- two_point_conv_result is the offence's try ---------------------------------------------------


def test_two_point_result_is_the_offences_when_the_defence_returns_the_try() -> None:
    """401030824: DEN's try fails and LAC returns it for two (Q4 10:10, DEN down 3-14).

    The text carries both results -- "ATTEMPT FAILS. DEFENSIVE TWO-POINT ATTEMPT. C.Hayward
    intercepted the try attempt. ATTEMPT SUCCEEDS." -- and the SUCCEEDS is the defence's.
    Reading it booked DEN a made two: two_point_conv_result "success" and a touchdown EP_end
    of 8. The standalone "Defensive 2pt Conversion" row that follows carries the -2, so the
    touchdown realises 6.
    """
    plays = _process(401030824)
    td = plays.filter(
        (pl.col("type.text") == "Passing Touchdown") & pl.col("text").str.contains("DEFENSIVE TWO-POINT")
    ).row(0, named=True)
    d2p = plays.filter(pl.col("type.text") == "Defensive 2pt Conversion").row(0, named=True)
    assert td["two_point_conv_result"] == d2p["two_point_conv_result"] == "failure"
    assert td["EP_end"] == 6
    assert d2p["EP_end"] == -2
    # the two points are LAC's (away): 14 -> 16, and DEN (home) 3 -> 9
    assert (d2p["awayScore"], d2p["homeScore"]) == (16, 9)


# --- a defensive two folded into the touchdown row -----------------------------------------------


def test_defensive_two_folded_into_the_touchdown_row_nets_four() -> None:
    """401671740: PHI blocks TB's extra point and K.Ringo returns it for two (Q3 6:49, TB 24-14).

    ESPN emits no "Defensive 2pt Conversion" row here: the return rides in the touchdown row's
    text. That row published EP_end 6 (a missed kick) and no row carried the defence's -2. It
    now realises 6 - 2 = 4 and carries defensive_two_point_conv, as nflverse flags the try row.
    Where ESPN does emit the standalone row (401030824), the flag and the -2 stay on it alone.
    """
    plays = _process(401671740)
    assert plays.filter(pl.col("type.text") == "Defensive 2pt Conversion").height == 0
    flagged = plays.filter(pl.col("defensive_two_point_conv") == True)  # noqa: E712
    assert flagged.height == 1
    td = flagged.row(0, named=True)
    assert td["type.text"] == "Rushing Touchdown"
    assert td["defensive_two_point_attempt"] is True
    assert td["EP_end"] == 4

    plays = _process(401030824)
    flagged = plays.filter(pl.col("defensive_two_point_conv") == True)  # noqa: E712
    assert flagged["type.text"].to_list() == ["Defensive 2pt Conversion"]
    td = plays.filter(pl.col("text").str.contains("DEFENSIVE TWO-POINT") & (pl.col("type.text") == "Passing Touchdown"))
    assert td["EP_end"].to_list() == [6]

