"""Yahoo -> ESPN-summary adapter: parity on a final, the in-progress path, and no coverage.

One test per gate class of the Stage 2 evidence run
(``background-research/2026-09-17-football-sources-program/s2-yahoo-cfb/``), on real payloads
only -- **no test here reaches the network**: every Yahoo payload is a committed fixture handed
to dispatch through ``payloads=``, which is also why the adapter resolves the Yahoo game id
only on the fetch path. The pinned floors are the values that run **observed**; they are never
lowered, and a regression below one is a failure, not a re-pin.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pytest

from sportsdataverse.cfb.yahoo_pbp.fetch import _has_plays, _resolve_game_id, _yahoo_cfb_game_id
from sportsdataverse.cfb.yahoo_pbp.to_espn_summary import _yahoo_to_espn_summary
from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.football.sources.dispatch import SourceUnavailable, _adapter_for, _process_game
from sportsdataverse.football.sources.parity import _compare_plays

YAHOO_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "yahoo_cfb"
ESPN_FIX = Path(__file__).resolve().parents[1] / "fixtures"

#: Ohio State at Texas, 2026 week 2 -- the one 2026 game with a committed payload on both feeds.
OSU_TEX_ESPN_ID = 401856682
OSU_TEX_YAHOO_ID = "ncaaf.g.202609120083"
OSU_TEX_ROW = {
    "league": "cfb",
    "espn_event_id": str(OSU_TEX_ESPN_ID),
    "kickoff_utc": "2026-09-12T23:30Z",
    "home_espn_team_id": "251",
    "away_espn_team_id": "194",
    "yahoo_game_id": OSU_TEX_YAHOO_ID,
    "spread_line": -1.5,
    "total_line": 44.5,
    "odds_source": "cfbd_lines",
}
#: Louisville at Ole Miss, 2026 week 1 -- a second final, used for the in-progress truncations.
LOU_MISS_ESPN_ID = 401856661
LOU_MISS_YAHOO_ID = "ncaaf.g.202609060077"
LOU_MISS_ROW = {
    "league": "cfb",
    "espn_event_id": str(LOU_MISS_ESPN_ID),
    "home_espn_team_id": "145",
    "away_espn_team_id": "97",
    "yahoo_game_id": LOU_MISS_YAHOO_ID,
}
#: Elon at Rhode Island, 2026 week 2 -- FCS-hosted. Yahoo answers **HTTP 200** with a real game
#: object (final score 31-10, a win-probability stub) and an empty ``playByPlay``.
FCS_YAHOO_FIXTURE = "ncaaf.g.202609120206_fcs.json.gz"
FCS_ESPN_ID = 401866628
#: Deliberately without ESPN team ids: "Yahoo does not cover this game" must be reported before
#: anything about the id-map row, so the fall-through log says why for every FCS-hosted game.
FCS_ROW = {"league": "cfb", "espn_event_id": str(FCS_ESPN_ID), "yahoo_game_id": "ncaaf.g.202609120206"}

#: Observed on OSU @ TEX: 185 ESPN plays / 177 Yahoo, 151 / 152 unambiguous snaps, 134 paired;
#: EPA r .9948, EP_start .9997, EP_end .9987, WP .9995. Floors, never re-pinned down. The clock
#: is deliberately NOT in the key: ESPN's CFB feed repeats one clock across several consecutive
#: plays (15:00 x4, then 13:52 x3) while Yahoo stamps every play, so a clock-bearing key pairs
#: about 8% of the rows.
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


PINNED = {
    "paired_share": 0.85,
    # the five KEY columns are the join and are therefore not scored; what is scored is
    # everything the join does NOT force to agree.
    "agreement": {
        "start.yardLine": 0.99,
        "end.pos_team.id": 0.98,
        "type.id": 0.96,
        "type.text": 0.97,
        "type.abbreviation": 0.96,
        "rush": 0.99,
        "pass": 0.97,
        "td_play": 0.99,
    },
    "correlation": {"EP_start": 0.999, "EP_end": 0.995, "EPA": 0.99, "wp_before": 0.999, "wp_after": 0.999},
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


def _load(name: str) -> dict:
    with gzip.open(YAHOO_FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def osu_tex_yahoo() -> dict:
    return _load(f"{OSU_TEX_YAHOO_ID}.json.gz")


@pytest.fixture(scope="module")
def osu_tex_summary(osu_tex_yahoo) -> tuple[dict, list[str]]:
    return _yahoo_to_espn_summary(osu_tex_yahoo, OSU_TEX_ROW)


@pytest.fixture(scope="module")
def osu_tex_pair(osu_tex_yahoo):
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
        source="yahoo",
        fallthrough=False,
        payloads={"yahoo": osu_tex_yahoo},
        idmap_row=OSU_TEX_ROW,
        odds_override=odds,
    )
    return reference, candidate


# --------------------------------------------------------------------------- gate 1: finals


def test_finals_parity_against_the_real_espn_summary(osu_tex_pair):
    reference, candidate = osu_tex_pair
    assert candidate.provenance["served"] == "yahoo"
    report = _compare_plays(
        _snaps(reference.plays_frame),
        _snaps(candidate.plays_frame),
        key=KEY,
        columns=COMPARE_COLUMNS,
        numeric=("EPA", "EP_start", "EP_end", "wp_before", "wp_after"),
    )
    assert report.check(PINNED) == []


def test_dispatch_resolves_the_yahoo_source(osu_tex_yahoo):
    assert _adapter_for("cfb", "yahoo") is not None
    processed = _process_game(
        "cfb",
        OSU_TEX_ESPN_ID,
        source="yahoo",
        fallthrough=False,
        payloads={"yahoo": osu_tex_yahoo},
        idmap_row=OSU_TEX_ROW,
    )
    assert processed.provenance["served"] == "yahoo"
    assert processed.provenance["native_ids"]["yahoo_game_id"] == OSU_TEX_YAHOO_ID
    # the stored closing line reached the processor -- not the 2.5 / 55.5 default
    assert processed.provenance["odds"] == {"source": "injected", "default": False, "from_idmap": True}
    assert processed.provenance["lossy_columns"], "the known-lossy list must reach provenance"


def test_contract_and_gop_fields(osu_tex_summary):
    summary, _notes = osu_tex_summary
    report = _validate_summary(summary, "cfb")
    assert report.missing == [] and report.invalid == [] and report.gop_missing == []
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    # GOP bracket-reads type.id / type.text / type.abbreviation
    assert all("id" in p["type"] and "text" in p["type"] and "abbreviation" in p["type"] for p in plays)
    assert all(p["type"]["id"] and p["type"]["text"] for p in plays)
    # every state column is an int on every row: one null floats the whole json_normalize column
    for key in ("down", "distance", "yardLine", "yardsToEndzone"):
        assert all(isinstance(p["start"][key], int) for p in plays), key
    assert all(isinstance(p["homeScore"], int) and isinstance(p["awayScore"], int) for p in plays)
    # the competitors carry ESPN team ids and a non-empty mascot (an empty one charges every
    # Timeout row to both clubs)
    competitors = summary["header"]["competitions"][0]["competitors"]
    assert [c["team"]["id"] for c in competitors] == ["251", "194"]
    assert all(c["team"]["name"] for c in competitors)
    # the odds a caller supplies become pickcenter
    supplied = _yahoo_to_espn_summary(
        _load(f"{OSU_TEX_YAHOO_ID}.json.gz"),
        OSU_TEX_ROW,
        odds={"gameSpread": 1.5, "overUnder": 44.5, "homeFavorite": True, "gameSpreadAvailable": True},
    )[0]
    assert supplied["pickcenter"][0]["overUnder"] == 44.5


def test_timeout_rows_are_charged_to_one_club(osu_tex_pair):
    """Yahoo writes "Texas timeout"; the processor's own matcher must resolve a side.

    Mutated out (an empty ``team.name`` in the header, or a timeout row typed as anything but
    ``"Timeout"``), every timeout row matches both clubs or neither and the timeouts-remaining
    cumsum -- a WP model input -- stops moving.
    """
    _reference, candidate = osu_tex_pair
    frame = candidate.plays_frame
    timeouts = frame.filter(frame["type.text"] == "Timeout")
    assert timeouts.height, "fixture has no timeout row"
    # a two-minute warning names no club and must be charged to neither
    named = timeouts.filter(~timeouts["text"].str.contains("(?i)two-minute"))
    charged = named.filter(named["homeTimeoutCalled"] | named["awayTimeoutCalled"])
    assert charged.height == named.height == 7
    assert not (timeouts["homeTimeoutCalled"] & timeouts["awayTimeoutCalled"]).any()


# ------------------------------------------------- the pieces with a mutation check


def test_pat_is_folded_into_its_touchdown(osu_tex_summary):
    """No standalone PAT row; the try's point and text ride on the touchdown play.

    Mutated out (emit ``EXTRA_POINT_ATTEMPT`` as its own row, or anchor the fold on
    ``emitted[-1]`` instead of the newest touchdown), ``pointAfterAttempt`` lands on a
    non-touchdown row and the touchdown's score step is 6 rather than 7 -- both assertions
    below fail.
    """
    summary, _notes = osu_tex_summary
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    assert not any("made PAT" == p["text"].strip() for p in plays)
    tries = [p for p in plays if "pointAfterAttempt" in p]
    assert tries, "fixture has no touchdown with a try"
    assert all(p["type"]["abbreviation"] == "TD" for p in tries)
    # the score step across a converted try is 7, not 6
    steps = []
    for i, play in enumerate(plays):
        if play["type"]["abbreviation"] == "TD" and (play.get("pointAfterAttempt") or {}).get("value") == 1:
            previous = plays[i - 1]
            steps.append((play["homeScore"] - previous["homeScore"]) + (play["awayScore"] - previous["awayScore"]))
    assert steps and all(s == 7 for s in steps), steps


def test_pat_anchors_on_its_touchdown_across_an_intervening_row():
    """The try is not always the row after its touchdown, and the fold must survive that.

    Louisville at Ole Miss carries both shapes: a false start on the try (play 119, a PENALTY
    row between the touchdown and "made PAT") and a timeout before a two-point try (play 152).
    Mutated out (``target = emitted[-1]``), ``pointAfterAttempt`` hangs off the Penalty and
    Timeout rows and the touchdowns step the scoreboard by 6 -- both assertions fail, and the
    same mutation passes on a fixture whose tries are all adjacent, which is how this class of
    defect shipped green in the Shield adapter.
    """
    summary, _notes = _yahoo_to_espn_summary(_load(f"{LOU_MISS_YAHOO_ID}.json.gz"), LOU_MISS_ROW)
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    tries = [p for p in plays if "pointAfterAttempt" in p]
    assert len(tries) >= 6
    assert all(p["type"]["abbreviation"] == "TD" for p in tries), [
        (p["type"]["text"], p["text"][:40]) for p in tries if p["type"]["abbreviation"] != "TD"
    ]
    # the intervening rows themselves carry no try and no score step of their own
    assert not any(p["type"]["text"] in ("Penalty", "Timeout") and "pointAfterAttempt" in p for p in plays)


def test_an_fcs_hosted_game_reports_no_coverage_rather_than_an_empty_frame():
    """Yahoo answers HTTP 200 for a game it does not cover; the shape is the only signal.

    Mutated out (drop the ``_has_plays`` guard), the adapter builds a summary with zero plays:
    dispatch then records a *contract* failure whose message names ``drives[].plays[]`` instead
    of "no play-by-play", and an adapter that got as far as the processor would hand Game on
    Paper an empty frame for a game another source can serve.
    """
    payload = _load(FCS_YAHOO_FIXTURE)
    game = payload["data"]["games"][0]
    assert game["status"] == "FINAL" and game["homeScore"] is not None  # a real game, not an error body
    assert not _has_plays(game)
    with pytest.raises(SourceUnavailable, match="no play-by-play"):
        _adapter_for("cfb", "yahoo")("cfb", FCS_ESPN_ID, _ctx(idmap_row=FCS_ROW, payload=payload))


def test_a_rate_limited_body_is_a_retryable_failure_not_an_empty_game():
    """Yahoo's edge answers a rate limit with a 23-byte ``text/html`` body, never JSON."""
    with pytest.raises(SourceUnavailable, match="no shangrila game object"):
        _adapter_for("cfb", "yahoo")(
            "cfb", OSU_TEX_ESPN_ID, _ctx(idmap_row=OSU_TEX_ROW, payload={"error": "Edge: Too Many Requests"})
        )


# ------------------------------------------------------------- gate 2: in-progress payloads


def _truncate(payload: dict, through_play_id: int) -> dict:
    """A real final trimmed at ``through_play_id`` -- the shape of a live payload.

    The drive chart is cut with the plays and the status set to Yahoo's in-progress word, so
    the last drive is genuinely open.
    """
    out = copy.deepcopy(payload)
    game = out["data"]["games"][0]
    plays = [p for p in sorted(game["playByPlay"], key=lambda p: p["playId"]) if p["playId"] <= through_play_id]
    kept = {p["playId"] for p in plays}
    game["playByPlay"] = plays
    drives = []
    for drive in game["drives"]:
        listed = [pid for pid in drive["playList"] if pid in kept]
        if listed:
            drives.append({**drive, "playList": listed})
    game["drives"] = drives
    game["status"] = "IN_PROGRESS"
    game["currentPeriod"] = plays[-1]["gamePeriod"]
    return out


@pytest.mark.parametrize("through_play_id", [9, 40, 100])
def test_an_in_progress_payload_synthesizes_the_open_drive(osu_tex_yahoo, through_play_id):
    payload = _truncate(osu_tex_yahoo, through_play_id)
    summary, notes = _yahoo_to_espn_summary(payload, OSU_TEX_ROW)
    assert summary["drives"].get("current"), "the open drive must be served as drives.current"
    assert any("drives.current" in n for n in notes)
    status = summary["header"]["competitions"][0]["status"]
    assert status["type"]["completed"] is False and status["type"]["state"] == "in"
    # the outcome columns a finished game carries are absent while the game is live
    competitors = summary["header"]["competitions"][0]["competitors"]
    assert not any(c["winner"] for c in competitors)
    assert summary["drives"]["current"]["result"] == "In Progress"
    assert summary["drives"]["current"]["isScore"] is False
    assert _validate_summary(summary, "cfb").ok
    processed = _process_game(
        "cfb",
        OSU_TEX_ESPN_ID,
        source="yahoo",
        fallthrough=False,
        payloads={"yahoo": payload},
        idmap_row=OSU_TEX_ROW,
    )
    assert processed.plays_frame.height > 0


def test_the_opening_drive_of_a_live_game_is_servable(osu_tex_yahoo):
    """A live game whose only drive is the open one must not be refused as "no plays".

    The play-presence guard reads ``drives.previous`` **and** ``drives.current``; reading only
    ``previous`` refuses every live game for the whole of its opening drive (measured on the
    Shield adapter: 14 of 223 snapshots).
    """
    first_drive = sorted(osu_tex_yahoo["data"]["games"][0]["drives"], key=lambda d: d["driveId"])[0]
    payload = _truncate(osu_tex_yahoo, max(first_drive["playList"]))  # plays 2-3: the opening drive
    summary, _notes = _yahoo_to_espn_summary(payload, OSU_TEX_ROW)
    assert summary["drives"]["previous"] == []
    assert summary["drives"]["current"]["plays"]
    processed = _process_game(
        "cfb",
        OSU_TEX_ESPN_ID,
        source="yahoo",
        fallthrough=False,
        payloads={"yahoo": payload},
        idmap_row=OSU_TEX_ROW,
    )
    assert processed.provenance["served"] == "yahoo"


def test_a_payload_with_no_plays_at_all_hands_over(osu_tex_yahoo):
    payload = copy.deepcopy(osu_tex_yahoo)
    payload["data"]["games"][0]["playByPlay"] = []
    with pytest.raises(SourceUnavailable, match="no play-by-play"):
        _adapter_for("cfb", "yahoo")("cfb", OSU_TEX_ESPN_ID, _ctx(idmap_row=OSU_TEX_ROW, payload=payload))


# ------------------------------------------------------------------ the computable game id


def test_the_yahoo_game_id_is_computed_from_the_kickoff_and_the_home_team():
    # Alabama at Kentucky, 2026-09-12 19:30Z -- Kentucky is Yahoo 69
    assert _yahoo_cfb_game_id("2026-09-12T19:30Z", "96") == "ncaaf.g.202609120069"
    # a late West-coast kickoff rolls past midnight UTC but stays on its US-Eastern date
    assert _yahoo_cfb_game_id("2026-09-13T02:30Z", "264") == "ncaaf.g.202609120065"
    # a team Yahoo has never listed resolves to nothing rather than to a wrong id
    assert _yahoo_cfb_game_id("2026-09-12T19:30Z", "999999") is None
    # the id map's own Yahoo team number outranks the committed table
    assert _yahoo_cfb_game_id("2026-09-12T19:30Z", "96", "ncaaf.t.4242") == "ncaaf.g.202609124242"


def test_id_resolution_prefers_the_stored_id_and_never_invents_one():
    stored = _resolve_game_id(401856674, {"yahoo_game_id": "ncaaf.g.202609120069"}, ())
    assert stored == ("ncaaf.g.202609120069", "idmap")
    computed = _resolve_game_id(401856674, {"kickoff_utc": "2026-09-12T19:30Z", "home_espn_team_id": "96"}, ())
    assert computed == ("ncaaf.g.202609120069", "computed")
    # no row, and no crosswalk seasons to read: a miss, never a fabricated id
    assert _resolve_game_id(401856674, None, ()) == (None, "unresolved")


def test_an_unresolvable_id_hands_over_without_fetching(monkeypatch):
    import sportsdataverse.cfb.yahoo_pbp.to_espn_summary as module

    def _boom(*_args, **_kwargs):
        raise AssertionError("the adapter must not fetch when it has no id")

    monkeypatch.setattr(module, "_fetch_playbook_boxscore", _boom)
    monkeypatch.setattr(module, "_resolve_game_id", lambda *_a, **_k: (None, "unresolved"))
    with pytest.raises(SourceUnavailable, match="none computable"):
        _adapter_for("cfb", "yahoo")("cfb", 401856674, _ctx(idmap_row={"espn_event_id": "401856674"}))


def test_a_row_without_espn_team_ids_hands_over(osu_tex_yahoo):
    with pytest.raises(SourceUnavailable, match="no ESPN team ids"):
        _adapter_for("cfb", "yahoo")(
            "cfb",
            OSU_TEX_ESPN_ID,
            _ctx(idmap_row={"espn_event_id": str(OSU_TEX_ESPN_ID)}, payload=osu_tex_yahoo),
        )


def _ctx(**kwargs):
    from sportsdataverse.football.sources.dispatch import SourceContext

    return SourceContext(**kwargs)
