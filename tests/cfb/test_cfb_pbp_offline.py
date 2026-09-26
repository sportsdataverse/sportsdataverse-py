import gzip
import json
from collections import Counter
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _load_summary(name="summary_401628455.json"):
    return json.loads((FIX / name).read_text())


def test_raw_allowlist_includes_injuries_and_gamenotes(monkeypatch):
    summary = _load_summary()
    summary["injuries"] = [{"team": {"id": "333"}, "injuries": []}]
    summary["gameNotes"] = [{"type": "note", "headline": "Week 1"}]

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    raw = CFBPlayProcess(gameId=401628455, raw=True).espn_cfb_pbp()
    assert "injuries" in raw and raw["injuries"], "injuries dropped by raw allowlist"
    assert "gameNotes" in raw and raw["gameNotes"], "gameNotes dropped by raw allowlist"


def test_odds_source_tag_summary_path():
    proc = CFBPlayProcess(gameId=401628455)
    pbp_txt = {
        "pickcenter": [
            {"provider": {"id": "58"}, "spread": -7.5, "overUnder": 52.5, "homeTeamOdds": {"favorite": True}},
            {"provider": {"id": "1002"}, "spread": -7.0, "overUnder": 52.0, "homeTeamOdds": {"favorite": True}},
        ],
    }
    proc._CFBPlayProcess__helper_cfb_pickcenter(pbp_txt)
    assert proc.odds_source == "summary_pickcenter"
    assert proc.gameSpreadAvailable is True


def test_injected_odds_bypasses_network(monkeypatch):
    proc = CFBPlayProcess(
        gameId=401628455,
        odds_override={"gameSpread": -10.5, "overUnder": 60.0, "homeFavorite": True, "gameSpreadAvailable": True},
    )
    # If the override path regressed into the live cascade, this would raise.
    # The method uses dunder-both-sides naming so no Python name mangling applies;
    # patch the class to cover all instances.
    monkeypatch.setattr(
        CFBPlayProcess,
        "__helper__espn_cfb_odds_information__",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("live odds endpoint must not be called")),
    )
    proc._CFBPlayProcess__helper_cfb_pickcenter({"pickcenter": []})
    assert proc.gameSpread == -10.5
    assert proc.overUnder == 60.0
    assert proc.odds_source == "injected"


def test_join_participants_constructor_arg():
    """``join_participants`` is accepted as a constructor arg, not only as a
    post-construction attribute. ``CFBPlayProcess(gameId=..., join_participants=False)``
    selects the fetch-free fast path (skips the participants join + roster fetch)
    at construction. Previously the kwarg was swallowed by ``**kwargs`` and the
    pipeline read it via ``getattr(self, "join_participants", True)``, so passing
    it to the constructor silently no-op'd."""
    assert CFBPlayProcess(gameId=1).join_participants is True  # default: lookups on
    assert CFBPlayProcess(gameId=1, join_participants=False).join_participants is False
    assert CFBPlayProcess(gameId=1, join_participants=True).join_participants is True


def test_odds_override_validation():
    with pytest.raises(ValueError):
        CFBPlayProcess(gameId=1, odds_override={"gameSpread": -3.5})  # missing keys
    with pytest.raises(ValueError):
        CFBPlayProcess(gameId=1, odds_override=[1, 2, 3])  # not a dict


def _stub_competitor():
    """ESPN's placeholder competitor, verbatim key set from game 401256142 (2020).

    A negative team id with "TBD" location/abbreviation; note the ABSENT `name`,
    `color`, `logos` and `groups` that a real team carries.
    """
    return {
        "id": "-2",
        "uid": "s:20~l:23~t:-2",
        "location": "TBD",
        "abbreviation": "TBD",
        "displayName": "TBD",
        "nickname": "TBD",
        "links": [],
    }


def test_placeholder_competitor_raises_nodata(monkeypatch):
    """A TBD opponent has no play-by-play; fail catchably, not on KeyError: 'name'.

    2020 is full of these (COVID cancellations) -- 28 games in that season alone
    died deep inside __helper_cfb_game_data before this guard existed.
    """
    from sportsdataverse.errors import NoDataError

    summary = _load_summary()
    summary["header"]["competitions"][0]["competitors"][1]["team"] = _stub_competitor()

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())

    game = CFBPlayProcess(gameId=401256142)
    with pytest.raises(NoDataError, match="placeholder"):
        game.espn_cfb_pbp()
        game.run_processing_pipeline()


def test_missing_team_name_does_not_keyerror(monkeypatch):
    """A real team (positive id) missing only `name` must not raise KeyError.

    The stub guard above catches the TBD case; this pins the belt-and-braces
    .get("name", "") so any other payload shape degrades instead of crashing.
    """
    summary = _load_summary()
    team = summary["header"]["competitions"][0]["competitors"][1]["team"]
    team.pop("name", None)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())

    game = CFBPlayProcess(gameId=401628455)
    try:
        game.espn_cfb_pbp()
        game.run_processing_pipeline()
    except KeyError as exc:  # pragma: no cover - the regression we are pinning
        pytest.fail(f"missing team key should not raise KeyError: {exc}")


# --- ESPN duplicate records whose clocks differ (C39 follow-up) -------------------------------


def _trimmed(game_id: int) -> dict:
    """Load a stored ESPN summary trimmed to the keys the processor reads.

    Provenance: copied from ``cfbfastR-cfb-raw/cfb/json/raw/{game_id}.json`` and
    reduced to ``boxscore``, ``drives``, ``gameInfo``, ``header``, ``pickcenter``
    and ``scoringPlays`` (the whole game is kept -- the processor short-circuits
    before the dedupe on a completed game with fewer than 50 plays, so a slice of
    a handful of plays would never reach the code under test).

    * ``summary_401411109_trimmed.json.gz`` -- Florida State @ Louisville, 2022 week 3.
      ESPN re-entered ten play records with a clock seconds apart from the copy they
      duplicate, including the two canonical pairs: "Jordan Travis pass intercepted"
      (3rd-and-9, 74 to go) at 9:59 and 9:51 and "Malik Cunningham pass incomplete to
      Tyler Hudson" (4th-and-2, 45 to go) at 5:18 and 4:42.
    * ``summary_242972641_trimmed.json.gz`` -- Texas @ Texas Tech, 2004 week 9. The
      2004 feed repeats the start state on the next row 49 times; none of those rows
      is a duplicate, and all of them must survive.
    * ``summary_401858228_trimmed.json.gz`` -- Mercer @ Georgia Tech, 2026 week 3. GT
      leads 30-6 in the third when Mercer returns a blocked kick try for a defensive
      two-point conversion.
    * ``summary_401234597_trimmed.json.gz`` -- North Carolina @ Boston College, 2020
      week 5. BC's touchdown with 0:45 left makes it 22-24, and UNC returns the two-point
      try for 26-22.
    * ``summary_401628559_trimmed.json.gz`` -- Penn State @ Minnesota, 2024 week 12.
      Minnesota returns PSU's blocked kick try for two with 0:18 left in the half; ESPN
      filed the return with id ...104999903 and sequence 102998955, both past the next
      drive's kickoff.
    * ``summary_401778334_trimmed.json.gz`` -- Wake Forest @ Mississippi State, 2025 week 1.
    * ``summary_401756960_trimmed.json.gz`` -- Kansas State @ Utah, 2025 week 13.
    * ``summary_401525860_trimmed.json.gz`` -- UCF @ Kansas, 2023 week 6. Each carries a
      two-point touchdown whose text names no try result (see the test below).
    * ``summary_272650152_trimmed.json.gz`` -- Clemson @ NC State, 2007 week 4. Clemson
      intercepts NC State's two-point try and returns it for two; ESPN typed the row
      "Extra Point Good".
    * ``summary_243040265_trimmed.json.gz`` -- USC @ Washington State, 2004. USC returns a
      punt for a touchdown and kicks the extra point (0-14 -> 0-21); late on, Washington
      State returns an interception for a touchdown, calls a timeout and tries for two.
    * ``summary_400547980_trimmed.json.gz`` -- Northwestern @ Notre Dame, 2014. Notre Dame
      returns a fumble for a touchdown and Northwestern returns the try for two.
    * ``summary_401756930_trimmed.json.gz`` -- Baylor @ Cincinnati, 2025 week 9. Two Baylor
      touchdowns whose appended two-point tries carry a no-play penalty.
    * ``summary_322430041_trimmed.json.gz`` -- UMass @ UConn, 2012 week 1. A pick-six ESPN
      typed "Pass Interception".
    * ``summary_282430151_trimmed.json.gz`` -- East Carolina @ Virginia Tech, 2008. Virginia
      Tech returns a blocked ECU extra point "for 2 defensive point conversion".
    * ``summary_243392572_trimmed.json.gz`` -- California @ Southern Miss, 2004. Cal returns
      Southern Miss's missed extra point for two (16-17 -> 16-19); the row has no type.
    * ``summary_400548425_trimmed.json.gz`` -- Idaho @ Georgia Southern, 2014 week 7. Idaho scores
      a touchdown and a two-point pass in one row, in the 2014 "(Two pt pass, ...)" wording.
    * ``summary_401831583_trimmed.json.gz`` -- Arizona @ SMU, 2025 bowl. Two Arizona
      touchdowns whose appended two-point tries fail, one of them re-tried after a penalty.
    * ``summary_401628439_trimmed.json.gz`` -- Georgia Tech @ Georgia, 2024 week 14. Eight
      overtimes; from the third on, alternating two-point attempts.
    * ``summary_400548250_trimmed.json.gz`` -- Utah State @ Boise State, 2014 week 14. Utah
      State returns Boise State's blocked extra point for two; ESPN files the return after
      the kickoff that follows.
    """
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _offline_plays(game_id: int) -> pl.DataFrame:
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=_trimmed(game_id))
    proc.run_processing_pipeline()
    return proc.plays_frame


def _feed_plays(summary: dict) -> list[dict]:
    return [play for drive in summary["drives"]["previous"] for play in drive["plays"]]


def test_espn_duplicate_records_with_differing_clocks_collapse():
    """401411109: the duplicate records ESPN logged with a different clock dedupe to one row."""
    plays = _offline_plays(401411109)
    for text, down, distance, yards_to_endzone in (
        ("Jordan Travis pass intercepted Rance Conner return for no gain to the FlaSt 45", 3, 9, 74),
        ("Malik Cunningham pass incomplete to Tyler Hudson", 4, 2, 45),
    ):
        hits = plays.filter(
            (pl.col("text") == text)
            & (pl.col("start.down") == down)
            & (pl.col("start.distance") == distance)
            & (pl.col("start.yardsToEndzone") == yards_to_endzone),
        )
        assert hits.height == 1, (text, hits.height)
    # a punt whose text is unique keeps its single row (the pre-C39 rule deleted rows like it)
    assert (
        plays.filter(
            pl.col("text") == "Alex Mastromanno punt for 52 yds , Braden Smith returns for no gain to the Lvile 37",
        ).height
        == 1
    )
    # the feed's 198 rows less the 4 quarter-end markers the pipeline drops and 10 duplicates
    lost = Counter(p["text"] for p in _feed_plays(_trimmed(401411109))) - Counter(plays["text"].to_list())
    markers = {text: n for text, n in lost.items() if text.startswith("End of")}
    dupes = {text: n for text, n in lost.items() if text not in markers}
    assert sum(markers.values()) == 4
    assert sum(dupes.values()) == 10
    # only the extra copy went: the feed's other rows carrying that text all survive
    # ("Malik Cunningham pass incomplete to Tyler Hudson" is 6 feed rows, 5 of them real)
    feed_counts = Counter(p["text"] for p in _feed_plays(_trimmed(401411109)))
    assert all(plays.filter(pl.col("text") == text).height == feed_counts[text] - n for text, n in dupes.items())
    assert plays.height == 184


def test_repeated_start_state_rows_all_survive():
    """242972641: the 2004 feed repeats the start state on the next row 49 times, none a duplicate."""
    summary = _trimmed(242972641)
    feed = _feed_plays(summary)

    def start_state(play: dict) -> tuple:
        start = play.get("start", {})
        return (
            start.get("team", {}).get("id"),
            start.get("down"),
            start.get("distance"),
            start.get("yardsToEndzone"),
        )

    assert sum(start_state(a) == start_state(b) for a, b in zip(feed, feed[1:])) == 49

    plays = _offline_plays(242972641)
    lost = Counter(p["text"] for p in feed) - Counter(plays["text"].to_list())
    # only the quarter-end markers the pipeline drops downstream -- no real play
    assert all(text.startswith("End of the") for text in lost), lost
    assert plays.height == 217


def test_defensive_two_point_conversion_is_scored_as_a_try():
    """401858228: Mercer returns a blocked GT kick try for two, GT up 30-6 at 8:20 of Q3.

    The kicking team goes from a try's expected value to -2, so EPA is about -2.9, and
    a 24 -> 22 point lead cannot swing win probability. It published EPA -7.70 (the
    model scored the try as first-and-goal from the 3) and WPA -0.996 (wp_after took
    the kickoff row's wp_before -- Mercer's -- without flipping it into GT's frame).
    """
    plays = _offline_plays(401858228).with_row_index("i")
    d2p = plays.filter(pl.col("type.text") == "Defensive 2pt Conversion")
    assert d2p.height == 1
    r = d2p.row(0, named=True)
    assert -4 <= r["EPA"] <= -2, r["EPA"]
    assert abs(r["wpa"]) < 0.05, r["wpa"]

    td, _, kickoff = plays.filter(pl.col("i").is_between(r["i"] - 1, r["i"] + 1)).iter_rows(named=True)
    assert (td["type.text"], kickoff["type.text"]) == ("Rushing Touchdown", "Kickoff")
    # GT (59) scores and tries; Mercer (2382) receives the kickoff GT then makes
    assert [td["pos_team"], r["pos_team"], kickoff["pos_team"]] == [59, 59, 2382]
    # WP hands over across the three rows: the try starts where the TD ended, and ends
    # where the kickoff starts, restated from the receiver's frame into GT's
    assert abs(td["wp_after"] - r["wp_before"]) < 0.01
    assert r["wp_after"] == pytest.approx(1 - kickoff["wp_before"])
    assert r["home_wp_after"] == pytest.approx(kickoff["home_wp_before"])


def test_a_try_row_starts_where_the_touchdown_ended() -> None:
    """401234597: BC scores to trail UNC 22-24 with 0:45 left; UNC returns the two for 26-22.

    Row N's wp_after is row N+1's wp_before within a possession. The try row's wp_before was
    the WP model on its placeholder start (BC's snap from the 3 again), 0.305 against the
    touchdown's 0.167 -- the TD row's end state is the board with the TD counted and UNC
    about to receive. The try row now starts from the touchdown's wp_after.
    """
    plays = _offline_plays(401234597).with_row_index("i")
    i = plays.filter(pl.col("type.text") == "Defensive 2pt Conversion")["i"][0]
    td, d2p = plays.filter(pl.col("i").is_between(i - 1, i)).iter_rows(named=True)
    assert td["start.pos_team.id"] == d2p["start.pos_team.id"] == 103
    assert d2p["wp_before"] == pytest.approx(td["wp_after"], abs=1e-6)
    assert -0.2 < d2p["wpa"] < -0.05, d2p["wpa"]


def test_a_late_filed_defensive_two_follows_its_touchdown() -> None:
    """401628559: ESPN filed Minnesota's return of PSU's try for two at the end of the game.

    Its id (...104999903) sorts after the final whistle and its sequence (102998955) after
    the next drive's kickoff and kneel, so the late-insert pass could not place it: the row
    sat last, in period 2, and took the end-of-game WP (wp_before 0.97, wp_after 1.0). A
    late try row now goes right after the last touchdown sequenced before it.
    """
    plays = _offline_plays(401628559).with_row_index("i")
    i = plays.filter(pl.col("type.text") == "Defensive 2pt Conversion")["i"][0]
    td, d2p, *nxt = plays.filter(pl.col("i").is_between(i - 1, i + 1)).iter_rows(named=True)
    assert td["type.text"] == "Rushing Touchdown", td["type.text"]
    assert [r["type.text"] for r in nxt] == ["Timeout"]
    assert td["period"] == d2p["period"] == 2
    assert d2p["wp_before"] == pytest.approx(td["wp_after"], abs=1e-6)
    assert abs(d2p["wpa"]) < 0.05, d2p["wpa"]


def test_a_two_point_touchdown_reads_espns_structured_result() -> None:
    """The try ESPN folds into a touchdown row, when the text names no result.

    ``two_point_conv_result`` comes from ESPN's pointAfterAttempt; the EP_end overlay read
    only the text ("conversion" plus "failed"), so these rows took the 6.92 unknown.

    * 401778334 "... to the MSU00 TOUCHDOWN, clock 14:06, 1ST DOWN": Two Point Rush, 2 -> 8.
    * 401756960 "J. Jackson run for 24 yds, for a TD (Av. Johnson pass Failed)": 0 -> 6, and
      the defence's return on the next row keeps its own -2.
    * 401525860 "Dylan McDuffie 1 Yd Run": Two Point Rush, value 2 -- UCF's two, not Kansas's:
      the score moves by 6 and UCF's "Defensive 2pt Conversion" follows. The offence's
      result is "failure" and the touchdown realises 6.
    """
    for game_id, row_id, result, ep_end in (
        (401778334, 401778334456, "success", 8),
        (401756960, 401756960729, "failure", 6),
        (401525860, 401525860103929701, "failure", 6),
    ):
        plays = _offline_plays(game_id).with_row_index("i")
        r = plays.filter(pl.col("id") == row_id).row(0, named=True)
        assert (r["two_point_conv_result"], r["EP_end"]) == (result, ep_end), game_id
        if result == "failure":
            d2p = plays.row(r["i"] + 1, named=True)
            assert (d2p["type.text"], d2p["EP_end"]) == ("Defensive 2pt Conversion", -2), game_id


def test_a_try_the_defence_returned_is_typed_a_defensive_two() -> None:
    """272650152: "Evans, D. pass attempt failed (intercepted), returned by Hamlin, M for
    defensive PAT." -- typed "Extra Point Good", so it realised +1 for NC State (EPA +0.08)
    while Clemson's score went 37 -> 39. 2007-13 carry 36 such rows, 35 of them typed
    "Extra Point Missed" (0 instead of -2).
    """
    plays = _offline_plays(272650152).with_row_index("i")
    r = plays.filter(pl.col("text").str.contains("for defensive PAT")).row(0, named=True)
    assert (r["orig_play_type"], r["type.text"]) == ("Extra Point Good", "Defensive 2pt Conversion")
    assert (r["EP_start"], r["EP_end"]) == (pytest.approx(0.92), -2)
    td = plays.row(r["i"] - 1, named=True)
    assert td["pos_team"] == r["pos_team"] and td["type.text"] == "Passing Touchdown"
    assert r["wp_before"] == pytest.approx(td["wp_after"], abs=1e-6)


@pytest.mark.parametrize(
    "text",
    [
        "Ryan Griffith extra point BLOCKED returned for 2-point defensive conversion by Leon McFadden.",
        "Paul Young extra point BLOCKED; Damion Owens return of blocked extra point for two-point defensive conversion.",
        "Two-point conversion attempt, Zac Robinson fumble recovered by Frank Alexander and returned for a "
        "defensive two-point conversion.",
        "KEENUM, Case pass attempt failed (intercepted), returned by FERGUSON, Josh for defensive PAT.",
        "Cole Way rush attempt failed  (fumbled), returned  for defensive PAT..",
        "Ben Ryan extra point BLOCKED returned for 2 defensive point conversion by Stephan Virgil.",
        "Blocked PAT returned by Brandon Flowers for a TWO-POINT CONVERSION.",
        "Two-point conversion attempt, Ricky Dobbs pass FAILED.  Pass intercepted by Brian Rolle and returned for "
        "two-points.",
        "Kelvin Hayden (MSU), missed PAT returned.",
    ],
)
def test_defensive_try_return_text_shapes(text: str) -> None:
    """Every 2004-13 shape of a returned try (the corpus has these nine)."""
    from sportsdataverse.cfb.cfb_pbp import _DEFENSIVE_TRY_RETURN

    assert pl.Series([text]).str.contains(_DEFENSIVE_TRY_RETURN).item()


def test_a_blocked_kick_returned_for_2_defensive_point_conversion_is_a_defensive_two() -> None:
    """282430151: "Ben Ryan extra point BLOCKED returned for 2 defensive point conversion by
    Stephan Virgil." (ECU 20 -> VT +2), typed "Extra Point Missed".

    The 2008 wording ("2 defensive point") and two others ("Blocked PAT returned by X for a
    TWO-POINT CONVERSION", "... returned for two-points.") missed the retype: the row realised
    0 for ECU (EPA -0.92) while Virginia Tech's score rose by 2.
    """
    plays = _offline_plays(282430151).with_row_index("i")
    r = plays.filter(pl.col("text").str.contains("defensive point conversion")).row(0, named=True)
    assert (r["orig_play_type"], r["type.text"]) == ("Extra Point Missed", "Defensive 2pt Conversion")
    assert (r["EP_start"], r["EP_end"]) == (pytest.approx(0.92), -2)
    td = plays.row(r["i"] - 1, named=True)
    assert td["pos_team"] == r["pos_team"] and td["type.text"] == "Passing Touchdown"
    assert r["wp_before"] == pytest.approx(td["wp_after"], abs=1e-6)


def test_a_2004_missed_pat_returned_is_a_defensive_two() -> None:
    """243392572: "Wendell Hunter (USM), missed PAT returned." (Southern Miss 16-17 -> 16-19).

    2004 files the three returned tries it has with no type. The row became "Unknown" before
    the defensive-return retype ran, which only looked at try types, so the model scored it
    as a scrimmage snap (EP_start 2.01 here) and its placeholder win probability cost Southern
    Miss 0.29, where the two points are worth 0.13.
    """
    plays = _offline_plays(243392572).with_row_index("i")
    r = plays.filter(pl.col("text").str.contains("missed PAT returned")).row(0, named=True)
    assert r["type.text"] == "Defensive 2pt Conversion"
    assert (r["EP_start"], r["EP_end"]) == (pytest.approx(0.92), -2)
    td = plays.row(r["i"] - 1, named=True)
    assert td["pos_team"] == r["pos_team"] and td["type.text"] == "Rushing Touchdown"
    assert r["wp_before"] == pytest.approx(td["wp_after"], abs=1e-6)


def test_a_2014_two_pt_clause_scores_the_two() -> None:
    """400548425: "Elijhaa Penny run for 4 yds for a TD, (Two pt pass, Matt Linehan pass  to
    Deon Watson GOOD)" (Idaho trails 10-33 -> 18-33).

    The two-point text test wanted the word "conversion", which 2014's "(Two pt pass|rush,
    ... GOOD)" never uses (37 rows), and in 2014 ESPN rarely sends pointAfterAttempt: the
    touchdown realised the 6.92 unknown instead of 8.
    """
    plays = _offline_plays(400548425)
    r = plays.filter(pl.col("id") == 400548425103968901).row(0, named=True)
    assert r["type.text"] == "Rushing Touchdown"
    assert r["EP_end"] == 8


def test_a_vendor_touchdowns_last_two_point_attempt_decides() -> None:
    """401831583: "... to the SMU00 TOUCHDOWN, clock 02:37, 1ST DOWN #1 N.Fifita pass attempt
    failed #1 N.Fifita pass attempt failed PENALTY SMU Pass Interference ... NO PLAY #24 Q.Craig
    rush attempt failed" (Arizona 0-24 -> 6-24).

    The 2025+ vendor template names no "conversion", and ESPN's pointAfterAttempt on this row
    is "NA", value 0, so the touchdown took the 6.92 unknown. The last attempt is the one that
    stood; the second touchdown's single "pass attempt failed" (pointAfterAttempt "Two Point
    Pass", 0) already realised 6.
    """
    plays = _offline_plays(401831583)
    for row_id in (401831583536, 401831583650):
        r = plays.filter(pl.col("id") == row_id).row(0, named=True)
        assert (r["type.text"], r["EP_end"]) == ("Passing Touchdown", 6), row_id


def test_a_failed_overtime_two_point_attempt_realises_nothing() -> None:
    """401628439: Georgia and Georgia Tech trade two-point attempts from the third overtime.

    ESPN types each attempt "Two Point Pass" / "Two Point Rush", and the EP_end branch for
    those types read only the 2004 wording "is no good": the nine "Two-Point Conversion
    failed" rows realised 2 (EPA +1.08, the value of a made two) instead of 0.
    """
    plays = _offline_plays(401628439)
    tries = plays.filter(pl.col("type.text").is_in(["Two Point Pass", "Two Point Rush"]))
    failed = tries.filter(pl.col("text") == "Two-Point Conversion failed")
    assert failed.height == 9
    assert failed["EP_end"].to_list() == [0] * 9
    assert failed["EPA"].to_list() == pytest.approx([-0.92] * 9)
    # the attempts ESPN scored (no text) are the made ones
    made = tries.filter(pl.col("scoringPlay") == True)  # noqa: E712
    assert made.height and made["EP_end"].to_list() == [2] * made.height


def test_a_try_filed_after_the_next_kickoff_follows_its_touchdown() -> None:
    """400548250: "Jay Ajayi 19 Yd Run (Dan Goodale PAT blocked)" at 2:05 of the first
    quarter, then the kickoff (2:04), then "Jalen Davis Defensive PAT Conversion" (2:05).

    The return's id and sequence both sort after the kickoff, and no clock steps back, so
    the late-insert pass left it there: the kickoff took the touchdown's end state and the
    return scored its own placeholder (wp_before 0.946 against the touchdown's 0.928).
    """
    plays = _offline_plays(400548250).with_row_index("i")
    i = plays.filter(pl.col("type.text") == "Defensive 2pt Conversion")["i"][0]
    td, d2p, kickoff = plays.filter(pl.col("i").is_between(i - 1, i + 1)).iter_rows(named=True)
    assert (td["type.text"], kickoff["type.text"]) == ("Rushing Touchdown", "Kickoff")
    assert td["pos_team"] == d2p["pos_team"] != kickoff["pos_team"]
    assert d2p["wp_before"] == pytest.approx(td["wp_after"], abs=1e-6)
    assert d2p["wp_after"] == pytest.approx(1 - kickoff["wp_before"], abs=1e-6)


def _rows_after_flipped_touchdowns(plays: pl.DataFrame) -> list[tuple[dict, list[dict]]]:
    """Each scoring play whose end team ESPN flipped to the scorer, with the rows up to its try."""
    out = []
    for td in plays.filter(
        (pl.col("scoringPlay") == True) & (pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
    ).iter_rows(named=True):
        nxt = []
        for r in plays.slice(td["i"] + 1, 3).iter_rows(named=True):
            nxt.append(r)
            if r["type.text"] != "Timeout":
                break
        out.append((td, nxt))
    return out


def test_a_try_after_a_return_touchdown_starts_where_the_touchdown_ended() -> None:
    """243040265 / 400547980: a try after a punt return or a fumble return touchdown.

    ESPN flips the touchdown's end team to the scorer, so #577's handover (touchdown kept
    its frame) skipped these: the try kept the WP model's value for its placeholder start,
    the touchdown borrowed its wp_after from it, and USC's made extra point after its punt
    return cost USC 0.036. The try now starts from the touchdown's end state scored for the
    scorer, and the touchdown's wp_after is that value restated for the punting team.
    """
    for game_id, td_type, try_type in (
        (243040265, "Punt Return Touchdown", "Extra Point Good"),
        (400547980, "Fumble Return Touchdown", "Defensive 2pt Conversion"),
    ):
        plays = _offline_plays(game_id).with_row_index("i")
        (td, (tr,)), *_ = _rows_after_flipped_touchdowns(plays)
        assert (td["type.text"], tr["type.text"]) == (td_type, try_type), game_id
        assert tr["start.pos_team.id"] == td["end.pos_team.id"] != td["start.pos_team.id"]
        assert tr["wp_before"] == pytest.approx(1 - td["wp_after"], abs=1e-6), game_id
        assert tr["home_wp_before"] == pytest.approx(td["home_wp_after"], abs=1e-6), game_id
        assert tr["wp_before_naive"] == pytest.approx(1 - td["wp_after_naive"], abs=1e-6), game_id
        if try_type == "Extra Point Good":
            # one point on top of the 0.92 the try was worth: a small gain, not a loss
            assert 0 < tr["wpa"] < 0.02, tr["wpa"]


def test_a_return_touchdown_hands_over_the_scorers_board() -> None:
    """243040265: USC's punt return touchdown, 0-14 -> 0-20 with 26:31 left.

    What the try inherits is the touchdown's end view in the scorer's frame: USC's lead
    and, before 2014, the try it is about to attempt, as an offensive touchdown's
    end.ExpScoreDiff carries. The plain end view states the lead for the punting team, and
    the pre-2014 defensive-touchdown branch subtracts the try from the scorer's lead.
    """
    from xgboost import DMatrix

    from sportsdataverse.cfb.cfb_pbp import wp_model
    from sportsdataverse.cfb.model_vars import wp_end_columns, wp_final_names

    plays = _offline_plays(243040265).with_row_index("i")
    (td, (tr,)), *_ = _rows_after_flipped_touchdowns(plays)
    lead = -td["end.pos_score_diff"]
    assert lead == 20
    scorer = (
        pl.DataFrame([td])
        .select(wp_end_columns)
        .with_columns(
            pl.lit(lead).alias("end.pos_score_diff"),
            pl.lit((lead + 0.92) / (td["end.adj_TimeSecsRem"] + 1)).alias("end.ExpScoreDiff_Time_Ratio"),
        )
    )
    scorer.columns = wp_final_names
    expected = float(wp_model.predict(DMatrix(scorer.to_pandas()))[0])
    assert tr["wp_before"] == pytest.approx(expected, abs=1e-6)


def test_a_timeout_between_a_return_touchdown_and_its_try_keeps_the_board() -> None:
    """243040265: Washington State's interception return, a timeout, then its two-point try.

    The timeout scored ESPN's placeholder (0.0074) and the try another (0.0062). Both now
    carry the touchdown's end state for Washington State, and the timeout does not move it.
    """
    plays = _offline_plays(243040265).with_row_index("i")
    _, (td, (timeout, tr)) = _rows_after_flipped_touchdowns(plays)
    assert (timeout["type.text"], tr["type.text"]) == ("Timeout", "Two Point Pass")
    assert timeout["start.pos_team.id"] == tr["start.pos_team.id"] == td["end.pos_team.id"]
    assert timeout["wp_before"] == pytest.approx(1 - td["wp_after"], abs=1e-6)
    assert timeout["wp_after"] == pytest.approx(timeout["wp_before"], abs=1e-6)
    assert tr["wp_before"] == pytest.approx(timeout["wp_after"], abs=1e-6)


def test_a_no_play_on_the_try_does_not_wipe_out_the_touchdown() -> None:
    """401756930: "... rush left for 1 yard gain to the CIN00 TOUCHDOWN, clock 13:34 #13
    S.Robertson pass attempt failed ... PENALTY CIN Face Mask (#2 D.Corleone). NO PLAY #23
    M.Turner rush attempt Successful" (Baylor 12-27 -> 20-27).

    The 2025+ vendor template appends the try, penalties and all, to the touchdown row, and
    ESPN's no-play marker there is the try's. It negated the touchdown: the row stayed
    "Rush" and realised EP_end -0.29 (EPA -6.13), and the "Passing Touchdown" row of the same
    shape did not count as a passing touchdown.
    """
    plays = _offline_plays(401756930)
    rush = plays.filter(pl.col("id") == 401756930616).row(0, named=True)
    assert (rush["orig_play_type"], rush["type.text"]) == ("Rush", "Rushing Touchdown")
    assert rush["td_play"] and rush["rush_td"]
    assert (rush["two_point_conv_result"], rush["EP_end"]) == ("success", 8)
    catch = plays.filter(pl.col("id") == 401756930467).row(0, named=True)
    assert catch["type.text"] == "Passing Touchdown" and catch["pass_td"]


@pytest.mark.parametrize(
    ("text", "scoring", "negated"),
    [
        # the try's no-play marker, on a touchdown ESPN scored
        (
            "No Huddle-Shotgun #13 S.Robertson rush left for 1 yard gain to the CIN00 TOUCHDOWN, clock 13:34 "
            "#13 S.Robertson pass attempt failed PENALTY CIN Pass Interference (#8 O.Arnold) 1 yard from CIN03 "
            "to CIN02. NO PLAY #23 M.Turner rush attempt Successful",
            True,
            False,
        ),
        # the same text on a row ESPN did not score is read whole
        (
            "No Huddle-Shotgun #13 S.Robertson rush left for 1 yard gain to the CIN00 TOUCHDOWN, clock 13:34 "
            "PENALTY CIN Holding 10 yards from CIN10 to CIN20. NO PLAY",
            False,
            True,
        ),
        # a touchdown that did not stand
        (
            "No Huddle-Shotgun #18 C.Coppock rush left for 27 yards gain to the FIU00 TOUCHDOWN nullified by "
            "penalty, clock 06:01 PENALTY KSU Holding (#17 G.Bullock Jr.) 10 yards from FIU27 to FIU37. NO PLAY",
            False,
            True,
        ),
        ("Jordan Travis pass complete to Ontaria Wilson for 20 yds for a TD (NO PLAY)", True, True),
    ],
)
def test_touchdown_negated_reads_the_try_tail_as_the_trys(text: str, scoring: bool, negated: bool) -> None:
    from sportsdataverse.cfb.cfb_pbp import _touchdown_negated

    frame = pl.DataFrame({"text": [text], "scoringPlay": [scoring]})
    assert frame.select(_touchdown_negated()).item() is negated


def test_an_interception_returned_for_a_touchdown_is_typed_one() -> None:
    """322430041: "Mike Wegzyn pass intercepted by Dwayne Gratz at the UMass 37, returned for 37
    yards for a TOUCHDOWN." (UConn 13-0 -> 19-0), typed "Pass Interception".

    Only "pass intercepted for a TD" was retyped, so the row stayed "Interception Return" and
    realised UMass's own EP at the end of a turnover (EPA +0.46).
    """
    plays = _offline_plays(322430041)
    r = plays.filter(pl.col("id") == 322430041105).row(0, named=True)
    assert (r["orig_play_type"], r["type.text"]) == ("Pass Interception", "Interception Return Touchdown")
    assert r["int_td"]
    assert r["EP_end"] == pytest.approx(-6.92)
    assert r["EPA"] < -6
