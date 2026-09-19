"""CBS -> ESPN-summary adapter: parity on a final, the in-progress path, and no coverage.

One test per gate class of the Stage 2 evidence run
(``background-research/2026-09-17-football-sources-program/s2-cbs-cfb/``), on real payloads
only -- **no test here reaches the network**: every CBS payload is a committed fixture handed
to dispatch through ``payloads=``, which is also why the adapter resolves the CBS game id only
on the fetch path, and why the one id-resolution test injects its own transport. The pinned
floors are the values that run **observed**; they are never lowered, and a regression below one
is a failure, not a re-pin.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pytest

from sportsdataverse.cfb.cbs_pbp.game_id import _CARD_RE, _resolve_cbs_game_id
from sportsdataverse.football.cbs_common import _parse_scoreboard
from sportsdataverse.cfb.cbs_pbp.to_espn_summary import (
    _cbs_cfb_to_espn_summary,
    _has_coverage,
    _napi_list,
)
from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.football.sources.dispatch import SourceUnavailable, _adapter_for, _process_game
from sportsdataverse.football.sources.parity import _compare_plays

CBS_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cbs_cfb"
ESPN_FIX = Path(__file__).resolve().parents[1] / "fixtures"

#: Ohio State at Texas, 2026 week 2 -- the one game with a committed payload on both feeds.
OSU_TEX_ESPN_ID = 401856682
OSU_TEX_ROW = {
    "league": "cfb",
    "season": 2026,
    "season_type": 2,
    "week": 3,
    "espn_event_id": str(OSU_TEX_ESPN_ID),
    "kickoff_utc": "2026-09-12T23:30Z",
    "home_espn_team_id": "251",
    "away_espn_team_id": "194",
    "spread_line": -1.5,
    "total_line": 44.5,
    "odds_source": "cfbd_lines",
}
#: Penn State at UCLA, 2025 week 6 -- its drive-13 touchdown is followed by a **penalty** row
#: and only then by the try, which is the shape that makes ``emitted[-1]`` the wrong anchor.
PSU_UCLA_ROW = {
    "league": "cfb",
    "season": 2025,
    "season_type": 2,
    "week": 6,
    "espn_event_id": "401752855",
    "home_espn_team_id": "26",
    "away_espn_team_id": "213",
}
#: Tennessee State at Georgia, 2026 week 2 -- drive 20 carries **two adjacent tries** for one
#: touchdown (a nullified one, then the retry); both must land on it and only one may score.
TNST_UGA_ROW = {
    "league": "cfb",
    "season": 2026,
    "season_type": 2,
    "week": 2,
    "espn_event_id": "401856658",
    "home_espn_team_id": "61",
    "away_espn_team_id": "2634",
}
#: Arizona at USC, 2023 week 6 -- CBS types a two-point try as the plain ``CompletePass`` it
#: was and marks it only in the text.
ARIZ_USC_ROW = {
    "league": "cfb",
    "season": 2023,
    "season_type": 2,
    "week": 6,
    "espn_event_id": "401524025",
    "home_espn_team_id": "30",
    "away_espn_team_id": "12",
}
#: Michigan at Indiana, 2022 week 6 -- two **blocked** field goals whose subplay names the
#: recovering club while still stating the kicking team's yardage.
MICH_IU_ROW = {
    "league": "cfb",
    "season": 2022,
    "season_type": 2,
    "week": 6,
    "espn_event_id": "401405108",
    "home_espn_team_id": "84",
    "away_espn_team_id": "130",
}
#: Kent State at Oklahoma, 2025 week 6 -- a strip-sack returned for a touchdown. CBS's subplays
#: name the club that **lost** the ball as the offence; only the drive's outcome says otherwise.
KENT_OU_ROW = {
    "league": "cfb",
    "season": 2025,
    "season_type": 2,
    "week": 6,
    "espn_event_id": "401752729",
    "home_espn_team_id": "201",
    "away_espn_team_id": "2309",
}
#: Elon at Rhode Island, 2026 week 2 -- FCS-hosted. CBS answers **HTTP 200** with a 404
#: envelope and no ``plays`` key. Deliberately without ESPN team ids: "CBS does not cover this
#: game" must be reported before anything about the id-map row.
FCS_ROW = {"league": "cfb", "espn_event_id": "401866628"}

#: Observed on OSU @ TEX against the ESPN summary already committed for the Yahoo gate: 185
#: ESPN rows / 175 CBS, 141 paired (93.4% of the unambiguous ESPN snaps). The clock is
#: deliberately NOT in the key -- ESPN's CFB feed repeats one clock across several consecutive
#: plays, so a clock-bearing key pairs about 8% of the rows.
KEY = ("period", "start.down", "start.distance", "start.yardsToEndzone", "start.pos_team.id")
#: Clock-stoppage rows carry the PRECEDING snap's state on both feeds, so on a clock-free key
#: they collide with that snap; and a state key that occurs twice in a game cannot identify a
#: play at all. Both are dropped from both sides before the join rather than paired wrongly.
_ADMIN = (
    "Timeout",
    "End Period",
    "End of Half",
    "End of Game",
    "End of Regulation",
    "Official Timeout",
    "Two-minute warning",
)


def _snaps(frame):
    import polars as pl

    frame = frame.filter(~pl.col("type.text").is_in(_ADMIN))
    return frame.filter(pl.struct(list(KEY)).count().over(list(KEY)) == 1)


#: Floors recorded from what this pair actually produces (paired .9338, rush / pass / td_play
#: 1.0, both spot columns .9929, type.text .9787 / type.id .9716 -- ESPN's 2026 feed types
#: some returned punts ``Punt Return`` where every 2022-2025 season types them ``Punt`` -- and
#: EPA r .9947, EP_start .9996, EP_end .9988, WP .9994 / .9992. Never lowered: a regression
#: below one of these is a failure, not a re-pin.
PINNED = {
    "paired_share": 0.93,
    # the five KEY columns are the join and are therefore not scored; what is scored is
    # everything the join does NOT force to agree.
    "agreement": {
        "start.yardLine": 0.99,
        "end.pos_team.id": 0.99,
        "type.id": 0.97,
        "type.text": 0.97,
        "type.abbreviation": 0.97,
        "rush": 1.0,
        "pass": 1.0,
        "td_play": 1.0,
    },
    "correlation": {"EP_start": 0.999, "EP_end": 0.998, "EPA": 0.99, "wp_before": 0.999, "wp_after": 0.999},
    "required_columns": ["type.id", "type.abbreviation", "start.yardsToEndzone", "drive.id"],
}
COMPARE_COLUMNS = (
    "type.id",
    "type.text",
    "type.abbreviation",
    "start.down",
    "start.distance",
    "start.yardsToEndzone",
    "start.yardLine",
    "start.pos_team.id",
    "end.pos_team.id",
    "period",
    "EPA",
    "EP_start",
    "EP_end",
    "wp_before",
    "wp_after",
    "rush",
    "pass",
    "td_play",
)


@pytest.fixture(autouse=True)
def _no_cached_pages():
    """The scoreboard page cache is process-wide; a test must never inherit another's page."""
    from sportsdataverse.football.cbs_common import _PAGE_CACHE, _PAGE_MISSES

    _PAGE_CACHE.clear()
    _PAGE_MISSES.clear()
    yield
    _PAGE_CACHE.clear()
    _PAGE_MISSES.clear()


def _load(name: str) -> dict:
    with gzip.open(CBS_FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _adapt(bundle: dict, row: dict):
    return _cbs_cfb_to_espn_summary(
        _napi_list(bundle.get("plays"), "plays") or [],
        _napi_list(bundle.get("drives"), "drives"),
        bundle["scoreboard"]["scoreboard"],
        row,
        game=bundle.get("game"),
    )


def _flat(summary: dict) -> list[dict]:
    """Every play in the summary, in served order."""
    drives = summary["drives"]["previous"] + [d for d in (summary["drives"].get("current"),) if d]
    return [play for drive in drives for play in drive["plays"]]


@pytest.fixture(scope="module")
def osu_tex_cbs() -> dict:
    return _load("cbs_cfb_401856682.json.gz")


@pytest.fixture(scope="module")
def osu_tex_summary(osu_tex_cbs) -> tuple[dict, list[str]]:
    return _adapt(osu_tex_cbs, OSU_TEX_ROW)


@pytest.fixture(scope="module")
def osu_tex_pair(osu_tex_cbs):
    """Both paths through the unmodified processor, **one** closing line (two pipeline runs)."""
    espn = json.loads((ESPN_FIX / f"summary_{OSU_TEX_ESPN_ID}.json").read_text(encoding="utf-8"))
    odds = {"gameSpread": 1.5, "overUnder": 44.5, "homeFavorite": True, "gameSpreadAvailable": True}
    reference = _process_game(
        "cfb",
        OSU_TEX_ESPN_ID,
        source="espn",
        fallthrough=False,
        payloads={"espn": espn},
        idmap_row=OSU_TEX_ROW,
        odds_override=odds,
    )
    candidate = _process_game(
        "cfb",
        OSU_TEX_ESPN_ID,
        source="cbs",
        fallthrough=False,
        payloads={"cbs": osu_tex_cbs},
        idmap_row=OSU_TEX_ROW,
        odds_override=odds,
    )
    return reference, candidate


# --------------------------------------------------------------------------- gate 1: finals


def test_finals_parity_against_the_real_espn_summary(osu_tex_pair):
    reference, candidate = osu_tex_pair
    assert candidate.provenance["served"] == "cbs"
    report = _compare_plays(
        _snaps(reference.plays_frame),
        _snaps(candidate.plays_frame),
        key=KEY,
        columns=COMPARE_COLUMNS,
        numeric=("EPA", "EP_start", "EP_end", "wp_before", "wp_after"),
    )
    assert report.check(PINNED) == []


def test_dispatch_resolves_the_cbs_source(osu_tex_cbs):
    assert _adapter_for("cfb", "cbs") is not None
    processed = _process_game(
        "cfb", OSU_TEX_ESPN_ID, source="cbs", fallthrough=False, payloads={"cbs": osu_tex_cbs}, idmap_row=OSU_TEX_ROW
    )
    assert processed.provenance["served"] == "cbs"
    assert processed.provenance["native_ids"]["cbs_game_id"] == "50027666"
    assert processed.provenance["native_ids"]["cbs_game_id_resolved_by"] == "payload"
    # the stored closing line reached the processor -- not the 2.5 / 55.5 default
    assert processed.provenance["odds"] == {"source": "injected", "default": False, "from_idmap": True}
    assert processed.provenance["lossy_columns"], "the known-lossy list must reach provenance"


def test_the_adapted_summary_satisfies_the_contract(osu_tex_summary):
    summary, _notes = osu_tex_summary
    assert _validate_summary(summary, "cfb").ok


def test_the_running_score_rebuilt_from_the_plays_reaches_cbs_own_final(osu_tex_summary):
    """CBS states no per-play running score, so the adapter accumulates one. 24-23, no note."""
    summary, notes = osu_tex_summary
    last = _flat(summary)[-1]
    assert (last["homeScore"], last["awayScore"]) == (24, 23)
    competitors = summary["header"]["competitions"][0]["competitors"]
    assert [c["score"] for c in competitors] == ["24", "23"]
    assert not [n for n in notes if "does not match" in n]


def test_every_try_is_folded_into_a_touchdown_and_no_try_row_survives(osu_tex_summary):
    summary, notes = osu_tex_summary
    plays = _flat(summary)
    assert not [p for p in plays if "extra point" in p["text"].lower() and not p.get("pointAfterAttempt")]
    folded = [p for p in plays if p.get("pointAfterAttempt")]
    assert len(folded) == 5, "the game's five tries fold onto five touchdowns"
    assert all(p["type"]["abbreviation"] == "TD" for p in folded)
    assert not [n for n in notes if "no touchdown to fold into" in n]


# ------------------------------------------------- mutation: the PAT fold's anchor


def test_the_try_anchors_on_its_touchdown_across_an_intervening_row():
    """A penalty row sits between the touchdown and its try; the try must skip it.

    Red under ``target = emitted[-1]``: the penalty row would take ``pointAfterAttempt`` and
    the touchdown would be left stepping the scoreboard by 6.
    """
    summary, _notes = _adapt(_load("cbs_cfb_401752855_pat_gap.json.gz"), PSU_UCLA_ROW)
    plays = _flat(summary)
    carriers = [p for p in plays if p.get("pointAfterAttempt")]
    assert carriers, "the fixture carries a try"
    for play in carriers:
        assert play["type"]["abbreviation"] == "TD", play["text"]
    assert not [p for p in plays if p["type"]["text"] == "Penalty" and p.get("pointAfterAttempt")]
    touchdown = next(p for p in plays if p["type"]["text"] == "Rushing Touchdown" and p.get("pointAfterAttempt"))
    penalty = next(p for p in plays if p["type"]["text"] == "Penalty" and p["id"] > touchdown["id"])
    # the post-try score is carried onto the rows between the touchdown and the try, so the
    # scoreboard never steps backwards across the gap
    assert (penalty["homeScore"], penalty["awayScore"]) == (touchdown["homeScore"], touchdown["awayScore"])


def test_two_adjacent_tries_for_one_touchdown_both_land_on_it_and_only_one_scores():
    """A nullified try and its retry: the first must not consume the touchdown."""
    summary, notes = _adapt(_load("cbs_cfb_401856658_two_tries.json.gz"), TNST_UGA_ROW)
    plays = _flat(summary)
    assert not [n for n in notes if "no touchdown to fold into" in n]
    scored = [p for p in plays if p.get("pointAfterAttempt")]
    assert len(scored) == 1, "two try rows, one touchdown, one folded row"
    touchdown = scored[0]
    assert touchdown["pointAfterAttempt"]["value"] == 1
    before = [p for p in plays if p["id"] < touchdown["id"]]
    step = touchdown["homeScore"] - (before[-1]["homeScore"] if before else 0)
    assert step == 7, "six for the touchdown, one for the good try, the nullified try nothing"


def test_a_two_point_try_is_folded_and_scores_two():
    """CBS types a two-point try as the plain pass it was; only the text says otherwise."""
    summary, _notes = _adapt(_load("cbs_cfb_401524025_two_point.json.gz"), ARIZ_USC_ROW)
    plays = _flat(summary)
    assert not [p for p in plays if "TWO-POINT CONVERSION ATTEMPT" in p["text"] and not p.get("pointAfterAttempt")]
    folded = [p for p in plays if p.get("pointAfterAttempt")]
    assert len(folded) == 1
    assert folded[0]["pointAfterAttempt"]["value"] == 2
    assert folded[0]["type"]["abbreviation"] == "TD"
    before = [p for p in plays if p["id"] < folded[0]["id"]]
    assert folded[0]["awayScore"] - before[-1]["awayScore"] == 8


def test_a_penalty_against_the_defence_keeps_the_offences_frame():
    """A CBS penalty subplay names the **penalised** club; its yardage is still the offence's.

    Red when the penalty row is flipped like any other row whose subplay names another club:
    the two flagged rows here would come out 15 and 20 yards from the end zone instead of 85
    and 80 -- and, through the next row's end state, drag the play before them with them.
    """
    summary, _notes = _adapt(_load("cbs_cfb_401524025_two_point.json.gz"), ARIZ_USC_ROW)
    flagged = [p for p in _flat(summary) if p["type"]["text"] == "Penalty" and "PENALTY on ARI-" in p["text"]]
    assert len(flagged) == 2, "the fixture carries two flags against the defence"
    assert [p["start"]["yardsToEndzone"] for p in flagged] == [85, 80]


def test_a_blocked_field_goal_keeps_the_kicking_teams_frame():
    """A blocked kick's subplay names the **recovering** club; its yardage is still the kicker's.

    Red when the kick row is flipped like any other row whose subplay names another club: a
    blocked 26-yarder comes out 92 yards from the end zone instead of 8, and drags the play
    before it with it through that row's end state.
    """
    summary, _notes = _adapt(_load("cbs_cfb_401405108_blocked_fg.json.gz"), MICH_IU_ROW)
    kicks = [p for p in _flat(summary) if p["type"]["text"] == "Blocked Field Goal"]
    assert len(kicks) == 2, "the fixture carries both clubs' blocked kicks"
    # the snap is the kick's own distance less ESPN's college offset (26 - 18, 24 - 18)
    assert [(p["start"]["yardsToEndzone"], p["start"]["team"]["id"]) for p in kicks] == [(8, "130"), (6, "84")]


def test_a_strip_sack_touchdown_is_credited_to_the_defence():
    """CBS types a strip-sack return as the offence's own play; the drive's outcome says no.

    Red when the scorer comes from the subplay rather than the drive's result: the row is typed
    a sack/rushing touchdown, credited to the club that **lost** the ball, and
    ``_fill_end_state`` then puts its end 100 yards from where the ball actually crossed --
    about 14 EPA on the row.
    """
    summary, _notes = _adapt(_load("cbs_cfb_401752729_strip_sack_td.json.gz"), KENT_OU_ROW)
    scores = [p for p in _flat(summary) if p["scoringPlay"]]
    assert len(scores) == 1
    score = scores[0]
    assert score["type"]["text"] == "Fumble Return Touchdown"
    assert score["start"]["team"]["id"] == "2309", "the offence is still the club that snapped it"
    assert score["end"]["team"]["id"] == "201", "the defence scored, so the end is the defence's goal line"
    assert (score["end"]["down"], score["end"]["yardsToEndzone"]) == (-1, 0)


def test_a_folded_try_is_written_in_espns_own_grammar(osu_tex_summary):
    """``CFBPlayProcess`` reads the **text** for a try, not ``pointAfterAttempt``.

    ESPN's college feed folds the try into the touchdown as ``(Name KICK)`` /
    ``(Name PAT MISSED)``; CBS's own wording matches neither, so a try left in CBS's grammar
    leaves the touchdown scored as a bare 6 (``__add_xp_suffix_cols``, ``cfb_pbp.py:6310+``).
    """
    summary, _notes = osu_tex_summary
    folded = [p for p in _flat(summary) if p.get("pointAfterAttempt")]
    assert len(folded) == 5
    for play in folded:
        assert play["pointAfterAttempt"]["text"] == "Extra Point Good"
        assert play["text"].endswith(" KICK)"), play["text"]
        assert "extra point" not in play["text"].lower(), "CBS's own wording must not survive"


# ------------------------------------- mutation: the play the feed states no next snap for


def test_the_newest_play_ends_where_its_own_yardage_puts_it(osu_tex_cbs):
    """Red when the no-next-snap fallback is ``play["start"]``: the newest row of every poll.

    Truncated after a 9-yard rush from 18 yards out, so the end spot is 9, not 18.
    """
    live = copy.deepcopy(osu_tex_cbs)
    plays = sorted(live["plays"]["plays"], key=lambda p: int(p["id"]))
    live["plays"] = {"plays": plays[:3]}
    live["drives"] = {
        "drives": [d for d in live["drives"]["drives"] if str(d["id"]) in {str(p["drive_id"]) for p in plays[:3]}]
    }
    live["scoreboard"]["scoreboard"]["game_status"]["status"] = "INPROGRESS"
    summary, _notes = _adapt(live, OSU_TEX_ROW)
    top = _flat(summary)[-1]
    assert top["type"]["text"] == "Rush"
    assert top["start"]["yardsToEndzone"] == 18
    assert top["statYardage"] == 9
    assert top["end"]["yardsToEndzone"] == 9, "the ball moved; its own start is not its end"


def test_a_failed_fourth_down_with_no_next_snap_turns_the_ball_over(osu_tex_cbs):
    """The newest row of a live poll on 4th & 2 short: the ball changes hands where it lies.

    Red when ``_trailing_end`` keeps the offence's frame: the top of the page would say the
    offence still has it 12 yards out, instead of the defence 88 from its own end zone. ESPN's
    own captured summaries write the defence at 1st & 10 on 50 of 50 failed fourth downs.
    """
    live = copy.deepcopy(osu_tex_cbs)
    plays = sorted(live["plays"]["plays"], key=lambda p: int(p["id"]))[:39]
    live["plays"] = {"plays": plays}
    kept = {str(p["drive_id"]) for p in plays}
    live["drives"] = {"drives": [d for d in live["drives"]["drives"] if str(d["id"]) in kept]}
    live["scoreboard"]["scoreboard"]["game_status"]["status"] = "INPROGRESS"
    summary, _notes = _adapt(live, OSU_TEX_ROW)
    top = _flat(summary)[-1]
    assert (top["start"]["down"], top["start"]["distance"]) == (4, 2)
    assert top["start"]["yardsToEndzone"] == 13 and top["statYardage"] == 1
    assert top["end"]["team"]["id"] != top["start"]["team"]["id"], "fourth and short: the ball turned over"
    assert (top["end"]["down"], top["end"]["distance"]) == (1, 10)
    assert top["end"]["yardsToEndzone"] == 88, "100 - (13 - 1), in the defence's frame"


def test_the_last_snap_of_a_half_keeps_its_own_end_spot(osu_tex_summary):
    """The next snap after a half is a kickoff in the other direction, 65 yards out.

    Reading through the boundary gave every clock-killing kneel the ensuing kickoff's spot.
    """
    summary, _notes = osu_tex_summary
    plays = _flat(summary)
    boundary = next(i for i, p in enumerate(plays) if p["type"]["text"] == "End of Half")
    last_of_half = plays[boundary - 1]
    following = plays[boundary + 1]
    assert following["type"]["text"] == "Kickoff" and following["start"]["yardsToEndzone"] == 65
    assert last_of_half["end"]["yardsToEndzone"] != 65


# --------------------------------------------------------------------- gate 2: in progress


def test_the_opening_drive_of_a_live_game_is_servable(osu_tex_cbs):
    """Two plays in: everything is in ``drives.current`` and ``previous`` is empty."""
    live = copy.deepcopy(osu_tex_cbs)
    plays = sorted(live["plays"]["plays"], key=lambda p: int(p["id"]))[:2]
    live["plays"] = {"plays": plays}
    live["drives"] = {"drives": [d for d in live["drives"]["drives"] if str(d["id"]) == str(plays[0]["drive_id"])]}
    live["scoreboard"]["scoreboard"]["game_status"]["status"] = "INPROGRESS"
    processed = _process_game(
        "cfb", OSU_TEX_ESPN_ID, source="cbs", fallthrough=False, payloads={"cbs": live}, idmap_row=OSU_TEX_ROW
    )
    assert processed.provenance["served"] == "cbs"
    assert processed.provenance["contract"]["ok"]
    summary, notes = _adapt(live, OSU_TEX_ROW)
    assert summary["drives"]["previous"] == []
    assert summary["drives"]["current"]["plays"]
    assert [n for n in notes if "drives.current" in n]


def test_a_live_payload_states_no_outcome_and_no_end_of_game(osu_tex_cbs):
    live = copy.deepcopy(osu_tex_cbs)
    plays = sorted(live["plays"]["plays"], key=lambda p: int(p["id"]))[:88]
    live["plays"] = {"plays": plays}
    kept = {str(p["drive_id"]) for p in plays}
    live["drives"] = {"drives": [d for d in live["drives"]["drives"] if str(d["id"]) in kept]}
    live["scoreboard"]["scoreboard"]["game_status"]["status"] = "INPROGRESS"
    summary, _notes = _adapt(live, OSU_TEX_ROW)
    current = summary["drives"]["current"]
    assert (current["result"], current["displayResult"], current["shortDisplayResult"]) == (
        "In Progress",
        "In Progress",
        "In Progress",
    )
    assert current["isScore"] is False
    assert summary["header"]["competitions"][0]["status"]["type"]["state"] == "in"
    assert not [p for p in _flat(summary) if p["type"]["text"] == "End of Game"]


# ------------------------------------------------------------------- gate 3: no coverage


def test_an_fcs_hosted_game_reports_no_coverage_rather_than_an_empty_frame():
    """CBS answers 200 with a 404 envelope; the row deliberately carries no ESPN team ids.

    ``match=`` pins the **ordering** too: moving the shape check after the id-map check turns
    this red, because the row would fail on the team ids first.
    """
    bundle = _load("cbs_cfb_fcs_50027625.json.gz")
    adapter = _adapter_for("cfb", "cbs")
    from sportsdataverse.football.sources.dispatch import SourceContext

    with pytest.raises(SourceUnavailable, match="no play-by-play"):
        adapter("cfb", 401866628, SourceContext(idmap_row=FCS_ROW, payload=bundle))


def test_a_404_envelope_is_never_read_as_coverage():
    """CBS writes the same absence under ``errors`` and under ``warnings``, both with a 200."""
    envelope = {"code": 404, "type": "NotFoundException", "message": "No scoring plays data for that game."}
    assert _has_coverage(_load("cbs_cfb_fcs_50027625.json.gz")) is False
    assert _has_coverage({"plays": {"errors": [envelope]}}) is False
    assert _has_coverage({"plays": {"warnings": [envelope]}}) is False
    assert _has_coverage({"plays": {"plays": []}}) is False
    assert _has_coverage({"plays": {"plays": [{"id": "1"}]}}) is True


def test_a_payload_that_is_not_a_bundle_hands_over(osu_tex_cbs):
    adapter = _adapter_for("cfb", "cbs")
    from sportsdataverse.football.sources.dispatch import SourceContext

    with pytest.raises(SourceUnavailable, match="not a NAPI bundle"):
        adapter("cfb", OSU_TEX_ESPN_ID, SourceContext(idmap_row=OSU_TEX_ROW, payload="<html>429</html>"))


def test_a_bundle_for_a_different_game_is_refused(osu_tex_cbs):
    """CBS echoes the id it served, so a wrong id is checkable rather than filed silently."""
    adapter = _adapter_for("cfb", "cbs")
    from sportsdataverse.football.sources.dispatch import SourceContext

    other = copy.deepcopy(osu_tex_cbs)
    other["cbs_game_id"] = "50027600"
    with pytest.raises(SourceUnavailable, match="not 50027600"):
        adapter("cfb", OSU_TEX_ESPN_ID, SourceContext(idmap_row=OSU_TEX_ROW, payload=other))


# ------------------------------------------------------------- id resolution (no network)


_CARD = (
    '<div id="game-50027666" data-enhanced="true" data-abbrev="NCAAF_20260912_OHIOST@TEXAS" class="x">'
    '<a href="/college-football/teams/OHIOST/ohio-state-buckeyes/">'
    '<img src="https://sports.cbsimg.net/fly/images/team-logos/alt/758.svg"/></a>'
    '<a href="/college-football/teams/TEXAS/texas-longhorns/">'
    '<img src="https://sports.cbsimg.net/fly/images/team-logos/alt/853.svg"/></a></div>'
)


class _Page:
    def __init__(self, text):
        self.text = text


def test_the_cbs_game_id_comes_from_the_week_scoreboard_card():
    calls = []

    def transport(url, **kwargs):
        calls.append(url)
        return _Page(_CARD)

    game_id, provenance = _resolve_cbs_game_id(
        2026, 3, "251", "194", kickoff_utc="2026-09-12T23:30Z", transport=transport
    )
    assert game_id == "50027666"
    assert provenance["enhanced"] is True
    assert "/college-football/scoreboard/FBS/2026/regular/" in provenance["how"]
    assert calls, "the page was read"


def test_an_unknown_team_resolves_to_nothing_rather_than_a_guess():
    def transport(url, **kwargs):  # pragma: no cover -- must not be reached
        raise AssertionError("no page should be read for a team with no CBS id")

    game_id, provenance = _resolve_cbs_game_id(2026, 3, "999999", "194", transport=transport)
    assert game_id is None
    assert provenance["how"] == "unresolved"


def test_an_unreachable_scoreboard_page_is_a_miss_not_a_raise():
    def transport(url, **kwargs):
        raise OSError("connection reset")

    game_id, provenance = _resolve_cbs_game_id(2026, 3, "251", "194", transport=transport)
    assert game_id is None
    assert provenance["weeks_tried"], "every candidate week was attempted"


def test_a_failing_week_page_is_read_once_per_process_on_a_bounded_retry_budget():
    """Red without the miss cache: every game re-bills the same dead page, three weeks deep.

    ``dl_utils.download`` retries 15 times by default, and each game tries three weeks, so an
    unreachable CBS page cost ~45 requests **per game** on Game on Paper's request path. The
    NFL twin took this fix in #542; this module was copied from it before that landed.
    """
    from sportsdataverse.football.cbs_common import _SCOREBOARD_RETRIES

    calls = []

    def transport(url, **kwargs):
        calls.append(kwargs.get("num_retries"))
        raise OSError("connection reset")

    for _ in range(4):
        assert _resolve_cbs_game_id(2026, 3, "251", "194", transport=transport)[0] is None
    assert len(calls) == 3, "one read per candidate week, for the whole process"
    assert calls == [_SCOREBOARD_RETRIES] * 3, "a best-effort lookup does not take the 15-retry budget"


def test_a_week_page_that_parses_to_no_cards_is_also_remembered():
    """A 200 that carries no card is a miss too -- otherwise it is re-fetched per game."""
    calls = []

    def transport(url, **kwargs):
        calls.append(url)
        return _Page("<html>no games</html>")

    for _ in range(3):
        assert _resolve_cbs_game_id(2026, 3, "251", "194", transport=transport)[0] is None
    assert len(calls) == 3


def test_the_card_parser_reads_both_team_ids_in_page_order():
    cards = _parse_scoreboard(_CARD, _CARD_RE)
    assert cards == [
        {
            "cbs_game_id": "50027666",
            "enhanced": True,
            "date": "20260912",
            "away": "OHIOST",
            "home": "TEXAS",
            "away_cbs_team_id": "758",
            "home_cbs_team_id": "853",
        }
    ]
