"""Offline regression tests for ESPN's NFL play-text grammar (2026 week 1).

Fixture: ``tests/nfl/fixtures/summary_401872922.json`` is the ESPN summary for
CLE @ JAX, 2026-09-13 (``videos`` / ``news`` / ``article`` stripped). Before this
grammar landed the processor ran the CFB text extractors over NFL text and
produced, on every 2026 week 1 game: a null rusher on every rush, formation
prefixes in passer names, sentence tails in receiver names, zero rushing /
receiving yards, and -0.9 to -18.2 EPA per team-game leaking in from
"Official Timeout" rows that arrive with ``down=-1``.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_pbp as nfl_pbp_mod
from sportsdataverse.nfl.model_vars import clock_stoppage_vec
from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures" / "summary_401872922.json"
GAME_ID = 401872922
_NAME_SHAPE = r"^[A-Z][a-z]{0,2}\.[A-Za-z'\-\. ]+$"


@pytest.fixture(scope="module")
def processed(monkeypatch_module):
    summary = json.loads(FIX.read_text())

    class _Resp:
        def json(self):
            return summary

    monkeypatch_module.setattr(nfl_pbp_mod, "download", lambda *a, **k: _Resp())
    # offline: no participants / sidecar fetch; ids resolve from the fixture's boxscore
    proc = NFLPlayProcess(gameId=GAME_ID, join_participants=False)
    proc.espn_nfl_pbp()
    out = proc.run_processing_pipeline()
    return proc, out


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def _plays(out):
    return out["plays"]


def test_clock_stoppage_rows_are_not_plays_and_carry_no_epa_or_wpa(processed):
    _, out = processed
    clock = [p for p in _plays(out) if p["type.text"] in clock_stoppage_vec]
    assert len(clock) > 10, "fixture should carry Official Timeout / Two-minute warning rows"
    assert {p["type.text"] for p in clock} >= {"Official Timeout", "Two-minute warning"}
    assert not any(p["scrimmage_play"] for p in clock)
    assert not any(p["play"] for p in clock)
    assert max(abs(p["EPA"] or 0.0) for p in clock) == 0.0
    assert max(abs(p["wpa"] or 0.0) for p in clock) == 0.0
    assert max(abs(p["EP_between"] or 0.0) for p in clock) == 0.0


def test_team_offensive_epa_equals_scrimmage_sum(processed):
    _, out = processed
    plays = _plays(out)
    for team in out["advBoxScore"]["team"]:
        tid = team["pos_team"]
        scrim = [p["EPA"] for p in plays if p["scrimmage_play"] and p["pos_team"] == tid]
        assert math.isclose(team["EPA_overall_off"], sum(scrim), abs_tol=1e-6)
        # every scrimmage row is a rush, a pass, or a fumble-recovery row -- never a clock stoppage
        assert not any(p["type.text"] in clock_stoppage_vec for p in plays if p["scrimmage_play"])


def test_rusher_passer_receiver_names_use_nfl_grammar(processed):
    _, out = processed
    plays = _plays(out)
    rush = [p for p in plays if p["rush"]]
    pas = [p for p in plays if p["pass"]]
    assert rush and pas
    assert all(p["rusher_player_name"] for p in rush)
    assert all(pl.Series([p["rusher_player_name"] for p in rush]).str.contains(_NAME_SHAPE))
    assert all(pl.Series([p["passer_player_name"] for p in pas]).str.contains(_NAME_SHAPE))
    assert "TEAM" not in {p["passer_player_name"] for p in pas}
    # sacks / interceptions keep the deliberate null receiver; everything else resolves
    catchable = [p for p in pas if not p["sack"] and not p["int"] and " to " in p["text"]]
    names = [p["receiver_player_name"] for p in catchable]
    assert all(names)
    assert all(pl.Series(names).str.contains(_NAME_SHAPE))
    assert not any(" ran ob" in n or " to " in n or "(" in n for n in names)
    # the two QBs, once each (no "(Shotgun) D.Watson", no "Deshaun Watson." split)
    assert {p["passer_player_name"] for p in pas} == {"D.Watson", "T.Lawrence"}
    # ESPN's scoring-summary form folds to the abbreviated name of the same player
    td = next(p for p in pas if "Yd pass from" in p["text"])
    assert td["passer_player_name"] == "D.Watson"
    assert td["receiver_player_name"] == "D.Boston"
    # three-letter disambiguation initials
    assert "Bri.Thomas" in {p["receiver_player_name"] for p in pas}


def test_yards_come_from_the_play_text(processed):
    _, out = processed
    plays = _plays(out)
    rush = [p for p in plays if p["rush"]]
    assert all(p["yds_rushed"] is not None for p in rush)
    # a plain rush's text yardage is ESPN's statYardage (the one exception in
    # this game is a double-text row that also carries an incomplete pass)
    mismatch = [p for p in rush if p["yds_rushed"] != p["statYardage"] and "incomplete" not in p["text"]]
    assert mismatch == []
    # penalty yardage is the number, not the ", 5" fragment the CFB capture returned
    pens = [p["yds_penalty"] for p in plays if p["penalty_flag"]]
    assert pens and all(str(y).isdigit() for y in pens)
    comp = [p for p in plays if p["pass"] and p["completion"]]
    assert all(p["yds_receiving"] is not None for p in comp)
    for p in plays:
        if p["sack"]:
            # "sacked at PHI 42 for -1 yards" is -1, not -42 (the yard line)
            assert p["yds_sacked"] is not None and -20 <= p["yds_sacked"] <= 0
    # team totals reconcile with ESPN's official box (CLE 87 / JAX 126 rushing)
    espn = {e["team_id"]: e for e in out["advBoxScore"]["espn_team"]}
    for team in out["advBoxScore"]["team"]:
        e = espn[team["pos_team"]]
        assert team["rush_yards"] == e["rushingYards"]
        assert team["rushes"] == e["rushingAttempts"]
        # gross passing yards vs ESPN net + sacks lost ("5-20" -> 20)
        sacks_lost = int(str(e["sacksYardsLost"]).split("-")[-1])
        assert abs(team["pass_yards"] - (e["netPassingYards"] + sacks_lost)) <= 8


def test_player_boxes_group_by_real_players(processed):
    _, out = processed
    box = out["advBoxScore"]
    assert len(box["pass"]) == 2
    assert {r["passer_player_name"] for r in box["pass"]} == {"D.Watson", "T.Lawrence"}
    assert all(r["Yds"] > 0 for r in box["pass"])
    assert 8 <= len(box["rush"]) <= 14 and all(r["rusher_player_name"] for r in box["rush"])
    # one unnamed row is allowed: throwaways ("pass incomplete short right.") have no target
    named = [r for r in box["receiver"] if r["receiver_player_name"]]
    assert 12 <= len(named) <= 22
    assert all(r["Rec"] == 0 for r in box["receiver"] if not r["receiver_player_name"])
    # completions / attempts match ESPN's box exactly (16/22 CLE, 18/23 JAX)
    espn = {e["team_id"]: e for e in box["espn_team"]}
    for r in box["pass"]:
        assert (r["Comp"], r["Att"]) == (espn[r["pos_team"]]["completions"], espn[r["pos_team"]]["pass_attempts"])


def test_plays_frame_and_espn_team_box_are_exposed(processed):
    proc, out = processed
    assert isinstance(proc.plays_frame, pl.DataFrame)
    assert proc.plays_frame.height == len(out["plays"])
    # the windowed re-aggregation Game on Paper does must work on the frame
    q1 = proc.plays_frame.filter(pl.col("period") == 1)
    assert len(proc.create_box_score(q1)["team"]) == 2
    espn = out["advBoxScore"]["espn_team"]
    assert {e["team_id"] for e in espn} == {5, 30}
    assert all(
        {"rushingYards", "netPassingYards", "completions", "pass_attempts", "firstDowns"} <= e.keys() for e in espn
    )


# --- processor parity: the shared football attribution / series / box builders ---


def test_box_sections_match_the_cfb_processor(processed):
    _, out = processed
    assert set(out["advBoxScore"].keys()) == {
        "pass",
        "rush",
        "receiver",
        "team",
        "situational",
        "defensive",
        "defensive_players",
        "specialists",
        "turnover",
        "drives",
        "espn_team",
        "espn_players",
        # the shared usage / situational / special-teams box (both processors)
        "player_usage",
        "position_group_usage",
        "tackles",
        "position_group_tackles",
        "team_usage",
        "drive_scripting",
        "st_kickers",
        "st_punters",
        "st_returners",
        "st_blocks",
        "st_team",
    }
    # the CFB passer / receiver air-yards and CPOE keys are present
    assert {"AirYds", "aDOT", "CompAirYds", "YAC", "AirYdsPct", "CompPct", "xCompPct", "CPOE"} <= out["advBoxScore"][
        "pass"
    ][0].keys()
    assert {"AirYds", "aDOT", "YAC"} <= out["advBoxScore"]["receiver"][0].keys()
    assert len(out["advBoxScore"]["espn_players"]) > 50


def test_turnovers_and_penalties_are_attributed_and_match_espn(processed):
    proc, out = processed
    box = out["advBoxScore"]
    espn = {e["team_id"]: e for e in box["espn_team"]}
    for t in box["turnover"]:
        e = espn[t["pos_team"]]
        assert (t["turnovers"], t["Int"], t["fumbles_lost"]) == (e["turnovers"], e["interceptions"], e["fumblesLost"])
        # ESPN-sourced totals and the play-by-play derivation agree on this game
        assert t["turnovers_pbp"] == t["turnovers"]
        assert t["espn_sourced"] is True
    for t in box["team"]:
        e = espn[t["pos_team"]]
        assert (t["penalties"], t["penalty_yards"]) == (e["penalties"], e["penalty_yards"])
    f = proc.plays_frame
    pen = f.filter(pl.col("penalty_flag") == True)  # noqa: E712
    assert pen.height > 10
    assert pen["penalized_team"].is_not_null().all()
    assert pen["penalty_team_id"].is_not_null().all()
    # ESPN's text codes (CLV) resolve to the ESPN abbreviation's team id (CLE = 5)
    assert set(pen["penalized_team"].unique().to_list()) == {5, 30}
    tos = f.filter(pl.col("is_turnover") == True)  # noqa: E712
    assert tos.height == 2 and set(tos["turnover_team"].to_list()) == {5}


def test_series_and_first_down_families_populate(processed):
    proc, _ = processed
    f = proc.plays_frame
    for c in (
        "firstD_by_yards",
        "firstD_by_poss",
        "firstD_by_kickoff",
        "first_down_earned",
        "first_down_yards",
        "new_series",
    ):
        assert f[c].dtype == pl.Boolean and int(f[c].sum()) > 0, c
    assert "firstD_by_penalty" in f.columns and "first_down_penalty" in f.columns
    assert int(f["kneel_down"].sum()) >= 1
    assert int(f["xp_attempt"].sum()) >= 4 and int(f["xp_made"].sum()) >= 3
    assert f.filter(pl.col("xp_attempt") == True)["xp_kicker_player_name"].is_not_null().all()  # noqa: E712


def test_air_yards_from_yards_after_catch(processed):
    proc, _ = processed
    f = proc.plays_frame
    comp = f.filter((pl.col("pass") == True) & (pl.col("completion") == True))  # noqa: E712
    assert comp["air_yards"].is_not_null().all()
    assert comp["yards_after_catch"].is_not_null().all()
    assert (comp["air_yards"] + comp["yards_after_catch"] == comp["yds_receiving"]).all()
    assert (comp["air_yardsToEndzone"] == comp["start.yardsToEndzone"] - comp["air_yards"]).all()


def test_defensive_and_special_teams_players_use_nfl_grammar(processed):
    _, out = processed
    box = out["advBoxScore"]
    dp = box["defensive_players"]
    assert dp and all(pl.Series([r["player_name"] for r in dp]).str.contains(_NAME_SHAPE))
    # "(sack split by J.Hines-Allen and T.Walker)" credits half a sack each
    by_name = {r["player_name"]: r for r in dp}
    assert by_name["T.Walker"]["sacks"] == 1.5 and by_name["J.Hines-Allen"]["sacks"] == 1.5
    assert sum(r["sacks"] for r in dp) == sum(1 for p in out["plays"] if p["sack"])
    assert by_name["F.Oluokun"]["interceptions"] == 1 and by_name["F.Oluokun"]["interceptions_yards"] > 0
    sp = box["specialists"]
    assert sp and all(pl.Series([r["player_name"] for r in sp]).str.contains(_NAME_SHAPE))
    punters = [r for r in sp if r["punts"] > 0]
    assert punters and all(r["punts_yards"] / r["punts"] > 30 for r in punters)
    assert any(r["kick_returns"] > 0 and r["kick_returns_yards"] > 0 for r in sp)


def test_shared_cfb_consumers_run_on_the_nfl_frame(processed):
    """Game on Paper's drive summary and situational block are CFB modules that read
    the attribution / series / air-yards columns; both must now produce output for
    an NFL frame instead of returning None."""
    from sportsdataverse.cfb import cfb_drive_summary, cfb_situational_stats

    proc, out = processed
    f = proc.plays_frame
    hid, aid = f["homeTeamId"][0], f["awayTeamId"][0]
    drives = (out.get("drives") or {}).get("previous") or []
    ds = cfb_drive_summary.create_drive_summary(drives, f, hid, aid)
    assert ds and {"chart", "teams"} <= set(ds.keys())
    ss = cfb_situational_stats.create_situational_stats(f, hid, aid)
    assert ss and "teams" in ss and len(ss["teams"]) == 2


def test_player_ids_resolve_from_the_boxscore(processed):
    proc, _ = processed
    f = proc.plays_frame
    for name_col, id_col in (
        ("passer_player_name", "passer_player_id"),
        ("rusher_player_name", "rusher_player_id"),
        ("receiver_player_name", "receiver_player_id"),
        ("sack_player_name", "sack_player_id"),
        ("interception_player_name", "interception_player_id"),
        ("punter_player_name", "punter_player_id"),
        ("kickoff_return_player_name", "kickoff_return_player_id"),
        ("fg_kicker_player_name", "fg_kicker_player_id"),
    ):
        named = f.filter(pl.col(name_col).is_not_null())
        assert named.height > 0, name_col
        assert named[id_col].is_not_null().mean() >= 0.95, (name_col, named[id_col].null_count())
    # the same athlete resolves to one id whichever column he appears in
    watson = f.filter(pl.col("passer_player_name") == "D.Watson")["passer_player_id"].unique().to_list()
    assert watson == ["3122840"]
    assert f.filter(pl.col("rusher_player_name") == "D.Watson")["rusher_player_id"].unique().to_list() == ["3122840"]


def test_penalty_side_direct_epa_and_naive_wp(processed):
    proc, _ = processed
    f = proc.plays_frame
    pen = f.filter(pl.col("penalty_flag") == True)  # noqa: E712
    # ESPN's structural foul label is the detail, and the text-named team settles the side
    assert pen["penalty_side"].is_not_null().all()
    assert set(pen["penalty_side"].unique().to_list()) <= {"off", "def"}
    assert "Neutral Zone Infraction" in pen["penalty_detail"].to_list()
    # accepted foul on a scrimmage play: the counterfactual EP and the direct penalty EPA
    assert f["EP_penalty_cf"].is_not_null().sum() >= 1
    assert f["EPA_penalty_direct"].is_not_null().sum() >= 10
    cf = f.filter(pl.col("EP_penalty_cf").is_not_null())
    assert (cf["EPA_penalty_direct"] == cf["EP_end"] - cf["EP_penalty_cf"]).all()
    assert "penalty_assessed_on_kickoff" in f.columns
    # spread-free WP under the CFB names, bounded, with a matching perspective flip
    for c in (
        "wp_before_naive",
        "wp_after_naive",
        "wpa_naive",
        "home_wp_before_naive",
        "away_wp_after_naive",
        "def_wp_before_naive",
        "lead_wp_before_naive",
        "wp_touchback_naive",
    ):
        assert c in f.columns, c
    assert f["wp_before_naive"].drop_nulls().is_between(0, 1).all()
    assert f["wp_after_naive"].drop_nulls().is_between(0, 1).all()
    assert ((f["home_wp_before_naive"] + f["away_wp_before_naive"]).round(6) == 1.0).all()
    assert f.filter(pl.col("type.text") == "Official Timeout")["wpa_naive"].abs().max() == 0.0
    # folded two-point tries carry their own flags (none in this game, columns present)
    assert {"two_point_attempt", "two_point_conv_result", "two_point_pass", "two_point_rush"} <= set(f.columns)


def test_2026_plays_under_the_2025_rulebook_without_an_era_warning(processed):
    """The era bump is a coverage assertion: 2026 must score without
    EraCoverageWarning, and the data must still show the 2025 kickoff rule (the
    touchback spotted at the 35 -> 65 yards to the end zone, or the 20 -> 80 when
    the kick lands short of the landing zone) while the EP substitution keeps the
    nflverse-parity spot (75). If a 2026 kickoff lands anywhere else this fails
    and the era needs a real review, not a bump."""
    import warnings as _w

    from sportsdataverse.errors import EraCoverageWarning
    from sportsdataverse.nfl.model_vars import ERA_MAX_KNOWN_SEASON, TOUCHBACK_YARDLINE_POST_2016

    proc, _ = processed
    assert ERA_MAX_KNOWN_SEASON >= 2026
    f = proc.plays_frame
    assert f["season"].unique().to_list() == [2026]
    tb = f.filter((pl.col("kickoff_play") == True) & (pl.col("kickoff_tb") == True))  # noqa: E712
    assert tb.height >= 1
    assert set(tb["start.yardsToEndzone.touchback"].to_list()) == {TOUCHBACK_YARDLINE_POST_2016}
    summary = json.loads(FIX.read_text())
    # the feed's own post-touchback spot (the processor substitutes the parity spot above)
    raw_spots = {
        p["end"]["yardsToEndzone"]
        for d in summary["drives"]["previous"]
        for p in d["plays"]
        if p["type"]["text"] == "Kickoff" and "Touchback" in p.get("text", "")
    }
    assert raw_spots and raw_spots <= {65, 80}, raw_spots

    class _Resp:
        def json(self):
            return summary

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(nfl_pbp_mod, "download", lambda *a, **k: _Resp())
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            again = NFLPlayProcess(gameId=GAME_ID, join_participants=False)
            again.espn_nfl_pbp()
            again.run_processing_pipeline()
    assert not [c for c in caught if issubclass(c.category, EraCoverageWarning)]


def test_two_point_decision_columns_on_touchdown_rows(processed):
    proc, _ = processed
    f = proc.plays_frame
    for c in ("two_pt_wp", "xp_wp", "prob_2pt", "two_pt_recommendation", "two_pt_wp_diff"):
        assert c in f.columns, c
    td = f.filter(
        (pl.col("td_play") == True) & ((pl.col("xp_attempt") == True) | (pl.col("two_point_attempt") == True))
    )  # noqa: E712
    assert td.height >= 5  # 4 JAX touchdowns + the legacy "(Andre Szmyt Kick)" CLE row
    scored = td.filter(pl.col("prob_2pt").is_not_null())
    if scored.height == 0:
        pytest.skip("nfl4th WP model not available offline; decision columns are null by contract")
    assert scored.height == td.height
    assert scored["prob_2pt"].is_between(0.3, 0.7).all()
    assert scored["xp_wp"].is_between(0, 1).all() and scored["two_pt_wp"].is_between(0, 1).all()
    assert ((scored["two_pt_wp"] - scored["xp_wp"]).round(9) == scored["two_pt_wp_diff"].round(9)).all()
    assert set(scored["two_pt_recommendation"].unique().to_list()) <= {"go_for_2", "kick_xp"}
    assert f.filter(pl.col("td_play") == False)["prob_2pt"].is_null().all()  # noqa: E712
    # the legacy try text is a made kick by the named kicker
    legacy = f.filter(pl.col("text").str.contains(r"\(Andre Szmyt Kick\)"))
    assert legacy.height == 1 and legacy["xp_made"][0] is True and legacy["xp_kicker_player_name"][0] == "A.Szmyt"


def test_participants_join_prefers_espn_ids_and_names(processed):
    """Injected participants (the wide frame espn_nfl_play_participants returns)
    overwrite the text-extracted name and take precedence over the box-resolved id."""
    from sportsdataverse.football.play_participants import coalesce_participants

    proc, _ = processed
    f = proc.plays_frame
    row = f.filter(pl.col("passer_player_name") == "D.Watson").head(1)
    parts = pl.DataFrame(
        {
            "play_id": [row["id"][0]],
            "passer_player_name": ["Deshaun Watson"],
            "passer_player_id": ["3122840"],
            "receiver_player_name": ["Jerry Jeudy"],
            "receiver_player_id": ["4241463"],
        },
    )
    out = coalesce_participants(f, parts, prefer_ids=True)
    got = out.filter(pl.col("id") == row["id"][0])
    assert got["passer_player_name"][0] == "Deshaun Watson" and got["passer_player_id"][0] == "3122840"
    assert got["receiver_player_name"][0] == "Jerry Jeudy" and got["receiver_player_id"][0] == "4241463"
    # every other row is untouched
    assert (
        out.filter(pl.col("id") != row["id"][0])["passer_player_name"].to_list()
        == f.filter(pl.col("id") != row["id"][0])["passer_player_name"].to_list()
    )


def test_offensive_foul_credits_the_run_to_the_spot_of_the_foul(processed):
    """ESPN's box scores a run that drew an offensive foul enforced from behind
    the end of the run with the yards up to the spot of the foul (Swift 17 -> 9
    on "to CHI 37 ... enforced at CHI 29"); a foul enforced at or beyond the end
    leaves the yardage standing; "No Play" rows stay at zero."""
    proc, _ = processed
    base = {
        "text": [
            "D.Swift right tackle to CHI 37 for 17 yards (Ja.Horn).PENALTY on CHI-R.Odunze, Offensive Holding, 10 yards, enforced at CHI 29.",
            "T.Lawrence left end to CLV 44 for 2 yards.PENALTY on JAX-T.Lawrence, Illegal Forward Pass, 5 yards, enforced at CLV 44.",
            "(Shotgun) J.Goff pass short left to A.St. Brown to DET 27 for 5 yards.PENALTY on DET-P.Sewell, Offensive Holding, 10 yards, enforced at DET 24.",
            "J.Gibbs up the middle to DET 30 for 8 yards (K.Elliss).",
            "J.Cook left guard to BUF 16 for 12 yards (D.Stingley). FUMBLES (D.Stingley), recovered by BUF-O.Torrence at BUF 19.",
            "(Shotgun) T.Lawrence pass deep right to Bri.Thomas to CLV 36 for 17 yards (D.Ward). FUMBLES (D.Ward), recovered by JAX-B.Tuten at CLV 41.",
            "C.Skattebo left end to DAL 31 for 7 yards (C.Durant). FUMBLES (C.Durant), and recovers at DAL 29. C.Skattebo to DAL 28 for 1 yard.",
            "(Shotgun) J.Allen pass short left to K.Coleman to HST 33 for 1 yard. Lateral to K.Shakir pushed ob at HST 23 for 10 yards (R.Blankenship).",
        ],
        "rush": [True, True, False, True, True, False, True, False],
        "pass": [False, False, True, False, False, True, False, True],
        "completion": [False, False, True, False, False, True, False, True],
        "yds_rushed": [17, 2, None, 8, 12, None, 10, None],
        "yds_receiving": [None, None, 5, None, None, 17, None, 11],
        "start.yardsToEndzone": [80, 58, 78, 78, 96, 53, 38, 34],
        "statYardage": [-11, 2, 5, 8, 15, 12, 10, 11],
        "type.text": [
            "Rush",
            "Rush",
            "Pass Reception",
            "Rush",
            "Fumble Recovery (Own)",
            "Fumble Recovery (Own)",
            "Fumble Recovery (Own)",
            "Pass Reception",
        ],
        "penalty_flag": [True, True, True, False, False, False, False, False],
        "penalty_side": ["off", "off", "off", None, None, None, None, None],
        "penalty_declined": [False] * 8,
        "penalty_offset": [False] * 8,
        "penalty_no_play": [False] * 8,
        "fumble_vec": [False, False, False, False, True, True, True, False],
        "pos_team": [3, 30, 8, 8, 2, 30, 19, 2],
        "homeTeamId": [3, 30, 8, 8, 34, 30, 19, 34],
        "homeTeamAbbrev": ["CHI", "JAX", "DET", "DET", "HOU", "JAX", "NYG", "HOU"],
        "awayTeamAbbrev": ["CAR", "CLE", "NO", "NO", "BUF", "CLE", "DAL", "BUF"],
    }
    df = pl.DataFrame(base)
    out = proc._NFLPlayProcess__credit_to_spot_of_foul(df)
    # spot of the foul (9), stands (2), stands (8); fumble recovered by a teammate
    # ahead keeps the carrier's 12, a teammate's recovery behind credits to the
    # spot (17 -> 12), the carrier's own recovery keeps the whole advance (10)
    assert out["yds_rushed"].to_list() == [9, 2, None, 8, 12, None, 10, None]
    assert out["yds_receiving"].to_list() == [None, None, 2, None, None, 12, None, 11]
    # the lateral is split off for the receiver box: catcher 1, lateral recipient 10
    assert out["lateral_player_name"].to_list()[-1] == "K.Shakir" and out["yds_lateral"].to_list()[-1] == 10


def test_scoring_opp_is_yards_to_endzone_not_the_raw_yardline(processed):
    """scoring_opp = the offense is within 40 yards of the END ZONE (the CFB
    rule). It used to read ESPN's home-oriented ``start.yardLine``, which flagged
    two thirds of NFL drives as scoring opportunities."""
    _, out = processed
    df = pl.from_dicts(out["plays"], infer_schema_length=None).filter(
        pl.col("scrimmage_play") == True  # noqa: E712
    )
    assert df.height > 100
    expected = df["start.yardsToEndzone"] <= 40
    assert (df["scoring_opp"] == expected).all()
    share = df["scoring_opp"].mean()
    assert 0.15 < share < 0.45, share
