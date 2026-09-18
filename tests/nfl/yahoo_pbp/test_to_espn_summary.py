"""Yahoo NFL -> ESPN-summary adapter: finals parity, the in-progress path, and the feed eras.

One test per gate class of the Stage 2 evidence run
(``background-research/2026-09-17-football-sources-program/s2-yahoo-nfl/``), on real payloads
only -- **no test here reaches the network**: every Yahoo payload is a committed fixture handed
to dispatch through ``payloads=``, which is also why the adapter resolves the Yahoo game id only
on the fetch path. The pinned floors are the values that run **observed**; they are never
lowered, and a regression below one is a failure, not a re-pin.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.football.sources.dispatch import (
    SourceContext,
    SourceUnavailable,
    _adapter_for,
    _process_game,
)
from sportsdataverse.football.sources.parity import GOP_HARD_COLUMNS, _compare_plays
from sportsdataverse.nfl.yahoo_pbp.fetch import _resolve_row, _yahoo_nfl_game_id
from sportsdataverse.nfl.yahoo_pbp.to_espn_summary import _STOPPAGE, _yahoo_to_espn_summary

FIX = Path(__file__).resolve().parents[1] / "fixtures"
YAHOO_FIX = FIX / "yahoo_nfl"

#: Cleveland at Jacksonville, 2026 week 1 -- the one game with a committed payload on both
#: feeds (``summary_401872922.json`` is the ESPN side, already in the repo).
CLE_JAX_ESPN_ID = 401872922
CLE_JAX_YAHOO_ID = "nfl.g.20260913030"
CLE_JAX_ROW = {
    "league": "nfl",
    "espn_event_id": str(CLE_JAX_ESPN_ID),
    "kickoff_utc": "2026-09-14T00:15Z",
    "home_espn_team_id": "30",
    "away_espn_team_id": "5",
    "yahoo_game_id": CLE_JAX_YAHOO_ID,
}
#: Both books had this game at JAX -8.5 / 40.5 (ESPN pickcenter, Yahoo `gameOddsSummary` and
#: BetMGM all agree). Injected into BOTH paths: the spread is an EP/WP *input*, so comparing a
#: run that read ESPN's pickcenter against one that read Yahoo's would measure the odds, not
#: the adapter.
CLE_JAX_ODDS = {"gameSpread": 8.5, "overUnder": 40.5, "homeFavorite": True, "gameSpreadAvailable": True}

#: Tampa Bay at Cincinnati, 2026 week 1 -- carries a try that does **not** follow its touchdown
#: (a PENALTY row sits between them), which is what makes the PAT-anchor mutation fail. On a
#: fixture whose tries are all adjacent the mutation passes, which is exactly how that defect
#: shipped green in #540.
TB_CIN_ESPN_ID = 401872925
TB_CIN_YAHOO_ID = "nfl.g.20260913004"
TB_CIN_ROW = {
    "league": "nfl",
    "espn_event_id": str(TB_CIN_ESPN_ID),
    "home_espn_team_id": "4",
    "away_espn_team_id": "27",
    "yahoo_game_id": TB_CIN_YAHOO_ID,
}
#: Buffalo at Pittsburgh, 2013 -- the pre-2020 feed era: no drive chart, the try written inside
#: the touchdown's own text, and a scoreboard stated only on scoring plays.
BUF_PIT_ESPN_ID = 331110023
BUF_PIT_YAHOO_ID = "nfl.g.20131110023"
BUF_PIT_ROW = {
    "league": "nfl",
    "espn_event_id": str(BUF_PIT_ESPN_ID),
    "home_espn_team_id": "23",
    "away_espn_team_id": "2",
    "yahoo_game_id": BUF_PIT_YAHOO_ID,
}
#: The 2025 Pro Bowl: Yahoo answers HTTP 200 with a well-formed envelope and an **empty**
#: ``games`` list. Deliberately without ESPN team ids -- "Yahoo does not cover this game" must
#: be reported before anything about the id-map row.
NO_COVERAGE_YAHOO_ID = "nfl.g.20250202032"
NO_COVERAGE_ESPN_ID = 401701113
NO_COVERAGE_ROW = {"league": "nfl", "espn_event_id": str(NO_COVERAGE_ESPN_ID), "yahoo_game_id": NO_COVERAGE_YAHOO_ID}

#: The join is on game state, not on the play id: Yahoo's ``playId`` is its own sequence. The
#: clock IS in the key for the NFL (unlike the CFB adapter, where ESPN repeats one clock across
#: consecutive plays and a clock-bearing key pairs ~8% of the rows) -- ESPN's NFL feed stamps
#: every play, and keeping it changes the pairing by at most a few rows. It is left out anyway
#: because Yahoo rounds the scoring plays' clock to the snap rather than to the score.
KEY = ("period", "start.down", "start.distance", "start.yardsToEndzone", "start.pos_team.id")
#: Clock-stoppage rows carry the PRECEDING snap's state on both feeds, so on a clock-free key
#: they collide with that snap; and a state key that occurs twice in a game cannot identify a
#: play at all. Both are dropped from both sides before the join rather than paired wrongly.
_ADMIN = tuple(_STOPPAGE)


def _snaps(frame: pl.DataFrame) -> pl.DataFrame:
    frame = frame.filter(~pl.col("type.text").is_in(_ADMIN))
    return frame.filter(pl.struct(list(KEY)).count().over(list(KEY)) == 1)


#: Observed on CLE @ JAX: 158 ESPN plays / 150 Yahoo, 121 paired unambiguous snaps; EPA r
#: .9999, EP_start .9999, EP_end .9999, WP 1.0000. Floors, never re-pinned down.
PINNED = {
    "paired_share": 0.90,
    "agreement": {
        "type.text": 0.96,
        "start.yardLine": 1.0,
        "start.posTeamTimeouts": 1.0,
        "start.defPosTeamTimeouts": 1.0,
        "start.homeScore": 1.0,
        "start.awayScore": 1.0,
        "end.yardsToEndzone": 0.95,
        "end.pos_team.id": 1.0,
    },
    "correlation": {"EPA": 0.99, "EP_start": 0.998, "EP_end": 0.99, "wp_before": 0.998, "wp_after": 0.998},
    "required_columns": list(GOP_HARD_COLUMNS),
}


def _payload(yahoo_id: str) -> dict:
    with gzip.open(YAHOO_FIX / f"{yahoo_id}.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _game(yahoo_id: str) -> dict:
    return _payload(yahoo_id)["data"]["games"][0]


@pytest.fixture(scope="module")
def cle_jax_espn() -> dict:
    with open(FIX / "summary_401872922.json", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def cle_jax_pair(cle_jax_espn):
    reference = _process_game(
        "nfl",
        CLE_JAX_ESPN_ID,
        source="espn",
        fallthrough=False,
        payloads={"espn": cle_jax_espn},
        idmap_row=CLE_JAX_ROW,
        odds_override=CLE_JAX_ODDS,
    )
    candidate = _process_game(
        "nfl",
        CLE_JAX_ESPN_ID,
        source="yahoo",
        fallthrough=False,
        payloads={"yahoo": _payload(CLE_JAX_YAHOO_ID)},
        idmap_row=CLE_JAX_ROW,
        odds_override=CLE_JAX_ODDS,
    )
    return reference, candidate


@pytest.fixture(scope="module")
def cle_jax_summary() -> dict:
    summary, _ = _yahoo_to_espn_summary(_payload(CLE_JAX_YAHOO_ID), CLE_JAX_ROW)
    return summary


# ---------------------------------------------------------------------------------------
# GATE 1 -- finals parity against the real ESPN summary
# ---------------------------------------------------------------------------------------
def test_finals_parity_against_the_real_espn_summary(cle_jax_pair):
    reference, candidate = cle_jax_pair
    report = _compare_plays(_snaps(reference.plays_frame), _snaps(candidate.plays_frame), key=KEY)
    assert report.n_paired >= 110, report.gates()
    assert report.check(PINNED) == [], report.gates()


def test_dispatch_serves_the_yahoo_source_with_provenance(cle_jax_pair):
    _, candidate = cle_jax_pair
    assert candidate.provenance["served"] == "yahoo"
    assert candidate.provenance["requested"] == "yahoo"
    assert candidate.provenance["native_ids"]["yahoo_game_id"] == CLE_JAX_YAHOO_ID
    assert candidate.provenance["contract"]["ok"] is True
    assert "air_yards" in candidate.provenance["lossy_columns"]
    assert _adapter_for("nfl", "yahoo") is not None


def test_contract_and_gop_fields(cle_jax_summary, cle_jax_pair):
    report = _validate_summary(cle_jax_summary, "nfl")
    assert report.ok, report.summary()
    _, candidate = cle_jax_pair
    missing = [c for c in GOP_HARD_COLUMNS if c not in candidate.plays_frame.columns]
    assert missing == []


def test_the_header_carries_espns_club_codes_not_nflverses():
    """``LAR`` / ``WSH``, which is what ``nfl_pbp._nfl_side_of_abbrev`` compares against.

    Written with an id-map row in the **Data API's** shape (``GAME_SCHEMA`` columns only, no
    ``home_team`` sub-row), which is the shape that made the Shield adapter invent ``BIL`` /
    ``LIO`` and charge 0 of 8 timeouts.
    """
    game = copy.deepcopy(_game(CLE_JAX_YAHOO_ID))
    row = dict(CLE_JAX_ROW, home_espn_team_id="14", away_espn_team_id="28")
    summary, _ = _yahoo_to_espn_summary(game, row)
    abbrs = {c["team"]["id"]: c["team"]["abbreviation"] for c in summary["header"]["competitions"][0]["competitors"]}
    assert abbrs == {"14": "LAR", "28": "WSH"}
    assert all(c["team"]["name"] for c in summary["header"]["competitions"][0]["competitors"])


def test_timeout_rows_are_charged_to_one_club(cle_jax_pair):
    """A Timeout row must name exactly one club, and an unattributed stoppage none.

    Yahoo's bare ``"Timeout"`` rows are the TV stoppages -- the ``TIMEOUT`` sub-play carries a
    ``teamId`` on them anyway (whoever had the ball), so reading it charged 13-14 official
    timeouts a game to a club that never called one.
    """
    _, candidate = cle_jax_pair
    frame = candidate.plays_frame
    charged = frame.filter(pl.col("type.text") == "Timeout")
    assert charged.height > 0
    official = frame.filter(pl.col("type.text") == "Official Timeout")
    assert all("Timeout #" in t for t in charged["text"].to_list())
    assert all("Timeout #" not in (t or "") for t in official["text"].to_list())
    assert frame["start.posTeamTimeouts"].min() >= 0


# ---------------------------------------------------------------------------------------
# the PAT fold -- mutation target #1
# ---------------------------------------------------------------------------------------
def test_pat_is_folded_into_its_touchdown(cle_jax_summary):
    plays = [p for d in cle_jax_summary["drives"]["previous"] for p in d["plays"]]
    tries = [p for p in plays if p.get("pointAfterAttempt")]
    assert tries, "no try folded at all"
    for play in tries:
        assert play["type"]["abbreviation"] == "TD", play["type"]
        assert play["pointAfterAttempt"]["abbreviation"] in (
            "Extra Point Good",
            "Extra Point Missed",
            "Two Point Conversion Good",
            "Two Point Attempt Failed",
        )
    assert not any(p["text"].strip().endswith("made PAT") for p in plays)


def test_pat_anchors_on_its_touchdown_across_an_intervening_row():
    """The try is folded onto the **scoring play**, never onto the row that precedes it.

    Tampa Bay at Cincinnati has a ``PENALTY`` row between a touchdown and its try. Anchoring on
    ``emitted[-1]`` hangs ``pointAfterAttempt`` off that penalty and leaves the touchdown
    stepping the scoreboard by 6 -- the #540 finding #1 class. This test is red under that
    mutation; a fixture whose tries are all adjacent is not.
    """
    summary, _ = _yahoo_to_espn_summary(_payload(TB_CIN_YAHOO_ID), TB_CIN_ROW)
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    carriers = [p for p in plays if p.get("pointAfterAttempt")]
    assert carriers
    for play in carriers:
        assert play["scoringPlay"] is True, play["text"]
        assert play["type"]["abbreviation"] == "TD", play["type"]
        assert play["type"]["text"] != "Penalty"
    # the fixture really does carry the non-adjacent case the mutation needs
    game = _game(TB_CIN_YAHOO_ID)
    ordered = sorted(game["playByPlay"], key=lambda p: p["playId"])
    assert any(
        p["playTypeId"] == "EXTRA_POINT_ATTEMPT" and not ordered[i - 1].get("isScoring")
        for i, p in enumerate(ordered)
        if i
    )


# ---------------------------------------------------------------------------------------
# the trailing end state -- mutation target #2
# ---------------------------------------------------------------------------------------
def test_the_newest_play_ends_where_its_own_yardage_puts_it():
    """A play with no next snap derives its end from its own yardage, never from its own start.

    Truncating a real final after a scrimmage gain is the live shape: it fires on the newest
    row of every poll, which is the row Game on Paper renders at the top of the page. Taking
    the play's own start says the ball never moved.
    """
    game = copy.deepcopy(_game(TB_CIN_YAHOO_ID))
    ordered = sorted(game["playByPlay"], key=lambda p: p["playId"])
    cut = next(
        i
        for i, p in enumerate(ordered)
        if p["playTypeId"] in ("RUSH", "PASS") and int(p.get("yards") or 0) > 3 and p.get("yardsToEndzone")
    )
    last = ordered[cut]
    game["playByPlay"] = ordered[: cut + 1]
    keep = {p["playId"] for p in game["playByPlay"]}
    game["drives"] = [
        {**d, "playList": [x for x in (d.get("playList") or []) if x in keep]}
        for d in (game.get("drives") or [])
        if any(x in keep for x in (d.get("playList") or []))
    ]
    game["status"] = "IN_PROGRESS"
    summary, _ = _yahoo_to_espn_summary(game, TB_CIN_ROW)
    newest = (summary["drives"].get("current") or summary["drives"]["previous"][-1])["plays"][-1]
    expected = int(last["yardsToEndzone"]) - int(last["yards"])
    assert newest["end"]["yardsToEndzone"] == expected
    assert newest["end"]["yardsToEndzone"] != newest["start"]["yardsToEndzone"]


def test_a_touchdown_ends_at_the_goal_line_of_the_team_that_scored(cle_jax_summary):
    plays = [p for d in cle_jax_summary["drives"]["previous"] for p in d["plays"]]
    tds = [p for p in plays if p["scoringPlay"] and p["type"]["abbreviation"] == "TD"]
    assert tds
    for play in tds:
        assert play["end"]["yardsToEndzone"] == 0
        assert play["end"]["down"] == -1


def test_a_made_field_goal_ends_where_it_was_kicked_from(cle_jax_summary):
    plays = [p for d in cle_jax_summary["drives"]["previous"] for p in d["plays"]]
    made = [p for p in plays if p["type"]["id"] == "59"]
    assert made
    for play in made:
        assert play["end"]["yardsToEndzone"] == play["start"]["yardsToEndzone"]
        assert play["end"]["down"] == -1 and play["end"]["distance"] == -1
        assert play["statYardage"] > 0  # the kick's distance, not Yahoo's zero


# ---------------------------------------------------------------------------------------
# GATE 2 -- in progress
# ---------------------------------------------------------------------------------------
@pytest.mark.parametrize("through", [2, 14, 88])
def test_an_in_progress_payload_synthesizes_the_open_drive(through):
    game = copy.deepcopy(_game(CLE_JAX_YAHOO_ID))
    ordered = sorted(game["playByPlay"], key=lambda p: p["playId"])[:through]
    keep = {p["playId"] for p in ordered}
    game["playByPlay"] = ordered
    game["drives"] = [
        {**d, "playList": [x for x in (d.get("playList") or []) if x in keep]}
        for d in (game.get("drives") or [])
        if any(x in keep for x in (d.get("playList") or []))
    ]
    game["status"] = "IN_PROGRESS"
    summary, notes = _yahoo_to_espn_summary(game, CLE_JAX_ROW)
    assert _validate_summary(summary, "nfl").ok
    current = summary["drives"].get("current")
    assert current is not None and current["plays"], notes
    assert current["result"] == current["displayResult"] == current["shortDisplayResult"] == "In Progress"
    assert current["isScore"] is False
    assert summary["header"]["competitions"][0]["status"]["type"]["completed"] is False


def test_the_opening_drive_of_a_live_game_is_servable():
    """A game whose only drive is the open one must be SERVED, not refused as empty.

    ``drives.previous`` is empty at that point; a play-presence guard that reads it alone
    refuses every live game for the whole of its first drive (#540 finding #2).
    """
    game = copy.deepcopy(_game(CLE_JAX_YAHOO_ID))
    ordered = sorted(game["playByPlay"], key=lambda p: p["playId"])[:4]
    keep = {p["playId"] for p in ordered}
    game["playByPlay"] = ordered
    game["drives"] = [
        {**d, "playList": [x for x in (d.get("playList") or []) if x in keep]}
        for d in (game.get("drives") or [])
        if any(x in keep for x in (d.get("playList") or []))
    ]
    game["status"] = "IN_PROGRESS"
    summary, _ = _yahoo_to_espn_summary(game, CLE_JAX_ROW)
    assert summary["drives"]["previous"] == []
    processed = _process_game(
        "nfl", CLE_JAX_ESPN_ID, source="yahoo", fallthrough=False, payloads={"yahoo": game}, idmap_row=CLE_JAX_ROW
    )
    assert processed.plays_frame.height == len(ordered)


# ---------------------------------------------------------------------------------------
# GATE 3 -- the pre-2020 feed era
# ---------------------------------------------------------------------------------------
def test_a_2013_payload_with_no_drive_chart_is_segmented_into_drives():
    summary, notes = _yahoo_to_espn_summary(_payload(BUF_PIT_YAHOO_ID), BUF_PIT_ROW)
    assert _validate_summary(summary, "nfl").ok
    assert any("no drive chart" in n for n in notes), notes
    drives = summary["drives"]["previous"]
    assert len(drives) > 15
    # a synthesized drive states real numbers: the box score SUMS them, and an all-null column
    # arrives as Utf8 through json_normalize, which is an error and not a null row
    assert all(isinstance(d["yards"], int) and isinstance(d["offensivePlays"], int) for d in drives)
    processed = _process_game(
        "nfl",
        BUF_PIT_ESPN_ID,
        source="yahoo",
        fallthrough=False,
        payloads={"yahoo": _payload(BUF_PIT_YAHOO_ID)},
        idmap_row=BUF_PIT_ROW,
    )
    assert processed.plays_frame.height > 140
    assert processed.plays_frame["EPA"].null_count() < processed.plays_frame.height


def test_a_2013_scoreboard_is_reconstructed_from_the_scoring_plays():
    """The pre-2020 feed states the score only on scoring plays; every other row reads 0-0.

    Taken literally that is a game tied from the opening kickoff to the final whistle, which
    wrecks every score-differential EP/WP feature.
    """
    summary, notes = _yahoo_to_espn_summary(_payload(BUF_PIT_YAHOO_ID), BUF_PIT_ROW)
    assert any("only on scoring plays" in n for n in notes), notes
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    totals = {(p["homeScore"], p["awayScore"]) for p in plays}
    assert len(totals) > 3, totals
    assert max(h for h, _ in totals) == 23 and max(a for _, a in totals) == 10
    # the scoreboard never steps backwards
    running = [(p["homeScore"], p["awayScore"]) for p in plays]
    assert all(b[0] >= a[0] and b[1] >= a[1] for a, b in zip(running, running[1:]))


def test_a_2013_touchdown_carries_the_try_written_inside_its_own_text():
    summary, _ = _yahoo_to_espn_summary(_payload(BUF_PIT_YAHOO_ID), BUF_PIT_ROW)
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    tds = [p for p in plays if p["scoringPlay"] and p["type"]["abbreviation"] == "TD"]
    assert tds
    assert any(p.get("pointAfterAttempt") for p in tds)
    assert not any(p["text"].strip().endswith("made PAT") for p in plays)


# ---------------------------------------------------------------------------------------
# failure paths -- every one hands over, none invents anything
# ---------------------------------------------------------------------------------------
def _adapt(espn_id: int, payload, row):
    """Call the registered adapter directly: dispatch would wrap its error in AllSourcesFailed."""
    adapter = _adapter_for("nfl", "yahoo")
    assert adapter is not None
    return adapter("nfl", espn_id, SourceContext(idmap_row=row, payload=payload))


def test_a_game_yahoo_does_not_cover_is_reported_as_no_coverage_not_a_rate_limit():
    with pytest.raises(SourceUnavailable, match="carries no game for this event"):
        _adapt(NO_COVERAGE_ESPN_ID, _payload(NO_COVERAGE_YAHOO_ID), NO_COVERAGE_ROW)


def test_a_rate_limited_body_is_a_retryable_failure_not_an_empty_game():
    """Yahoo's edge answers a rate limit with a 23-byte ``text/html`` body, not JSON."""
    for body in ("<html><body></body></html>", {}, {"data": None}, {"data": {"games": None}}):
        with pytest.raises(SourceUnavailable, match="no shangrila game object"):
            _adapt(CLE_JAX_ESPN_ID, body, CLE_JAX_ROW)


def test_a_payload_for_a_different_game_is_refused():
    payload = copy.deepcopy(_payload(CLE_JAX_YAHOO_ID))
    payload["data"]["games"][0]["gameId"] = "nfl.g.20260913099"
    with pytest.raises(SourceUnavailable, match="served game"):
        _adapt(CLE_JAX_ESPN_ID, payload, CLE_JAX_ROW)


def test_a_payload_with_null_team_ids_is_refused():
    game = copy.deepcopy(_game(CLE_JAX_YAHOO_ID))
    game["homeTeamId"] = game["awayTeamId"] = None
    with pytest.raises(ValueError, match="possession cannot be attributed"):
        _yahoo_to_espn_summary(game, {"espn_event_id": str(CLE_JAX_ESPN_ID)})


def test_espn_team_ids_are_read_off_yahoos_own_numbers_when_the_row_has_none():
    """Yahoo's ``nfl.t.N`` **is** the ESPN team id (32/32) -- but only for the NFL."""
    summary, _ = _yahoo_to_espn_summary(_game(CLE_JAX_YAHOO_ID), {"espn_event_id": str(CLE_JAX_ESPN_ID)})
    ids = {c["homeAway"]: c["team"]["id"] for c in summary["header"]["competitions"][0]["competitors"]}
    assert ids == {"home": "30", "away": "5"}


# ---------------------------------------------------------------------------------------
# id resolution -- computed, never invented
# ---------------------------------------------------------------------------------------
def test_the_yahoo_game_id_is_computed_from_the_kickoff_and_the_home_team():
    assert _yahoo_nfl_game_id("2026-09-14T00:15Z", "30") == "nfl.g.20260913030"
    assert _yahoo_nfl_game_id("2026-09-10T00:20Z", 26) == "nfl.g.20260909026"  # the Wednesday opener's date roll
    assert _yahoo_nfl_game_id("2026-02-08T23:30Z", "17") == "nfl.g.20260208017"  # neutral-site Super Bowl LX
    assert _yahoo_nfl_game_id(None, "30", et_date="2026-09-13") == "nfl.g.20260913030"
    assert _yahoo_nfl_game_id(None, "30") is None
    assert _yahoo_nfl_game_id("2026-09-14T00:15Z", None) is None


def test_id_resolution_prefers_the_stored_id_and_never_invents_one(monkeypatch):
    import sportsdataverse.nfl.yahoo_pbp.fetch as fetch

    monkeypatch.setattr(fetch, "_row_from_nflverse_schedule", lambda espn_id: None)
    row, game_id, how = _resolve_row(CLE_JAX_ESPN_ID, CLE_JAX_ROW)
    assert game_id == CLE_JAX_YAHOO_ID and how["yahoo_id_resolved_by"] == "idmap"

    computed_row = {k: v for k, v in CLE_JAX_ROW.items() if k != "yahoo_game_id"}
    row, game_id, how = _resolve_row(CLE_JAX_ESPN_ID, computed_row)
    assert game_id == CLE_JAX_YAHOO_ID and how["yahoo_id_resolved_by"] == "computed"

    row, game_id, how = _resolve_row(CLE_JAX_ESPN_ID, {"league": "nfl"})
    assert game_id is None and how["yahoo_id_resolved_by"] == "unresolved"


def test_an_unresolvable_id_hands_over_without_fetching(monkeypatch):
    import sportsdataverse.nfl.yahoo_pbp.to_espn_summary as adapter

    def boom(*args, **kwargs):  # pragma: no cover -- the point is that it is never reached
        raise AssertionError("the adapter fetched despite having no id")

    monkeypatch.setattr(adapter, "_fetch_playbook_boxscore", boom)
    monkeypatch.setattr("sportsdataverse.nfl.yahoo_pbp.fetch._row_from_nflverse_schedule", lambda espn_id: None)
    with pytest.raises(SourceUnavailable, match="none computable"):
        _adapt(1, None, {"league": "nfl"})
