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
