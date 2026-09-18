"""CBS -> ESPN-summary adapter: parity on a final, the in-progress path, the pre-GSIS era.

One test per gate class of the CBS evidence run
(``background-research/2026-09-17-football-sources-program/s2-cbs-nfl/``), on real trimmed
payloads only. The pinned floors are the values that run **observed**; they are never
lowered, and a regression below one is a failure, not a re-pin. **No test here reaches the
network**: every fetch path is either injected through ``payloads=`` or stubbed.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.football.sources.dispatch import SourceUnavailable, _adapter_for, _process_game
from sportsdataverse.football.sources.parity import _compare_plays
from sportsdataverse.nfl.cbs_pbp.game_id import _parse_scoreboard, _resolve_cbs_game_id
from sportsdataverse.nfl.cbs_pbp.to_espn_summary import _cbs_nfl_to_espn_summary, _norm_text

CBS_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_cbs"
ESPN_FIX = Path(__file__).resolve().parents[1] / "fixtures"

#: CLE @ JAX, 2026 week 1 -- the one game with a committed payload on both feeds.
CLE_JAX_ESPN_ID = 401872922
CLE_JAX_ROW = {
    "league": "nfl",
    "espn_event_id": str(CLE_JAX_ESPN_ID),
    "season": 2026,
    "season_type": 2,
    "week": 1,
    "kickoff_utc": "2026-09-13T17:00Z",
    "neutral_site": False,
    "home_espn_team_id": "30",
    "away_espn_team_id": "5",
    "cbs_game_id": "50029216",
    "nflverse_game_id": "2026_01_CLE_JAX",
}

#: Observed on 2026_01_CLE_JAX (154 ESPN rows, 138 CBS rows, 135 paired on the play id).
#: Floors, never re-pinned down.
PINNED = {
    "paired_share": 0.87,
    "agreement": {
        "type.id": 1.0,
        "type.text": 1.0,
        "type.abbreviation": 1.0,
        "start.down": 1.0,
        "start.distance": 1.0,
        "start.yardsToEndzone": 1.0,
        "start.yardLine": 1.0,
        "start.pos_team.id": 1.0,
        "start.posTeamTimeouts": 1.0,
        "period": 1.0,
        "homeScore": 1.0,
        "awayScore": 1.0,
        "rush": 1.0,
        "pass": 1.0,
        "sack": 1.0,
        "completion": 1.0,
        "int": 1.0,
        "td_play": 1.0,
        "penalty_flag": 1.0,
        "fg_made": 1.0,
        "yds_rushed": 1.0,
        "yds_receiving": 1.0,
        "yds_punted": 1.0,
        "drive.id": 1.0,
        "statYardage": 0.95,
    },
    "correlation": {"EP_start": 0.999, "EP_end": 0.999, "EPA": 0.999, "wp_before": 0.999, "wp_after": 0.999},
    "required_columns": ["type.id", "type.abbreviation", "start.yardsToEndzone", "drive.id"],
}


def _load(name: str = "2026_01_CLE_JAX.json.gz") -> dict:
    with gzip.open(CBS_FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _bodies(payload: dict) -> tuple:
    return (
        payload["plays"]["plays"],
        (payload.get("drives") or {}).get("drives"),
        payload["scoreboard"]["scoreboard"],
    )


@pytest.fixture(scope="module")
def cle_jax_payload() -> dict:
    return _load()


@pytest.fixture(scope="module")
def cle_jax_summary(cle_jax_payload) -> tuple:
    plays, drives, scoreboard = _bodies(cle_jax_payload)
    return _cbs_nfl_to_espn_summary(plays, drives, scoreboard, CLE_JAX_ROW)


@pytest.fixture(scope="module")
def cle_jax_pair(cle_jax_payload):
    """Both paths through the unmodified processor, same id-map row (two pipeline runs)."""
    espn = json.loads((ESPN_FIX / f"summary_{CLE_JAX_ESPN_ID}.json").read_text(encoding="utf-8"))
    reference = _process_game(
        "nfl", CLE_JAX_ESPN_ID, source="espn", fallthrough=False, payloads={"espn": espn}, idmap_row=CLE_JAX_ROW
    )
    candidate = _process_game(
        "nfl",
        CLE_JAX_ESPN_ID,
        source="cbs",
        fallthrough=False,
        payloads={"cbs": cle_jax_payload},
        idmap_row=CLE_JAX_ROW,
    )
    return reference, candidate


# --------------------------------------------------------------------------- gate 1: finals


def test_finals_parity_against_the_real_espn_summary(cle_jax_pair):
    reference, candidate = cle_jax_pair
    assert candidate.provenance["served"] == "cbs"
    report = _compare_plays(
        reference.plays_frame,
        candidate.plays_frame,
        columns=tuple(PINNED["agreement"]) + tuple(PINNED["correlation"]),
    )
    assert report.check(PINNED) == []


def test_dispatch_resolves_the_cbs_source(cle_jax_payload):
    assert _adapter_for("nfl", "cbs") is not None
    processed = _process_game(
        "nfl",
        CLE_JAX_ESPN_ID,
        source="cbs",
        fallthrough=False,
        payloads={"cbs": cle_jax_payload},
        idmap_row=CLE_JAX_ROW,
    )
    assert processed.provenance["served"] == "cbs"
    assert processed.provenance["native_ids"]["cbs_game_id"] == "50029216"
    assert processed.provenance["native_ids"]["cbs_game_id_source"] == "payload"
    # the source's own consensus line reached the processor, not the 2.5 / 55.5 default
    assert processed.provenance["odds"]["default"] is False


def test_contract_and_gop_fields(cle_jax_summary):
    summary, _notes = cle_jax_summary
    report = _validate_summary(summary, "nfl")
    assert report.missing == [] and report.invalid == [] and report.gop_missing == []
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    # Game on Paper bracket-reads type.id / type.text / type.abbreviation (python/app.py:204-208)
    assert all("id" in p["type"] and "text" in p["type"] and "abbreviation" in p["type"] for p in plays)
    assert all(p["type"]["id"] and p["type"]["text"] for p in plays)
    header = summary["header"]["competitions"][0]
    # no invented club label: the mascot is never empty (an empty one matches every Timeout row)
    assert [c["team"]["abbreviation"] for c in header["competitors"]] == ["JAX", "CLE"]
    assert all(c["team"]["name"] for c in header["competitors"])


# ------------------------------------------------ the two rules with a mutation check


def test_reversed_text_keeps_only_the_ruling(cle_jax_summary):
    """CBS keeps the whole overturned narrative; only the text after ``was REVERSED.`` stands.

    Mutated out (keep CBS's ``description`` verbatim), the overturned fumble is read back as
    a real one: ``_norm_text`` leaves ``FUMBLES`` / ``RECOVERED by`` in the text and the
    assertion below fails on the reversal play.
    """
    summary, _ = cle_jax_summary
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    reversed_plays = [p for p in plays if p["id"].endswith("3130")]
    assert reversed_plays, "fixture has no replay reversal"
    assert not any("FUMBLES" in p["text"] or "REVERSED" in p["text"] for p in reversed_plays)
    assert "D.Watson scrambles left end to JAX 22 for 7 yards" in reversed_plays[0]["text"]
    # the rule itself, in isolation: the ruling that stands is the tail
    assert _norm_text("A FUMBLES. B RECOVERED. the play was REVERSED. 4-D.Watson scrambles.") == "D.Watson scrambles."
    # idempotent on text with no reversal, and jersey-prefix stripping does not eat a score
    assert _norm_text("39-C.Little kicks 62 yards from JAC 35.") == "C.Little kicks 62 yards from JAX 35."


def test_pat_is_folded_into_its_touchdown(cle_jax_summary):
    """No standalone PAT row; the try's text and its point ride on the touchdown play.

    Mutated out (emit ``PointAfterTouchdown`` as its own row), the touchdown's score step is
    6 rather than 7 and a bare "extra point is GOOD" row appears -- both assertions fail.
    """
    summary, _ = cle_jax_summary
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    assert not any(p["text"].lower().startswith("extra point") for p in plays)
    touchdowns = [p for p in plays if "pointAfterAttempt" in p]
    assert touchdowns, "fixture has no touchdown with a try"
    assert all(p["type"]["abbreviation"] == "TD" for p in touchdowns)
    for play in touchdowns:
        index = plays.index(play)
        before = plays[index - 1] if index else {"homeScore": 0, "awayScore": 0}
        step = (play["homeScore"] - before["homeScore"]) + (play["awayScore"] - before["awayScore"])
        assert step == 6 + play["pointAfterAttempt"]["value"], play["id"]


def test_the_pat_anchors_on_its_touchdown_not_on_the_previous_row():
    """A stoppage between the touchdown and the try must not take the ``pointAfterAttempt``."""
    payload = _load()
    plays, drives, scoreboard = _bodies(payload)
    ordered = sorted(plays, key=lambda p: int(p["id"]))
    try_index = next(
        i for i, p in enumerate(ordered) if (p["subplays"]["subplay"] or [{}])[0].get("type") == "PointAfterTouchdown"
    )
    # splice a penalty-only row in between, exactly the shape that sits between a touchdown
    # and its try on 735 of the 30,279 PAT rows in nfl-raw
    filler = json.loads(json.dumps(ordered[try_index - 1]))
    filler["id"] = str(int(ordered[try_index]["id"]) - 1)
    filler["score_on_play"], filler["score_type"] = "No", None
    filler["subplays"]["subplay"] = [
        {
            "type": "Penalty",
            "order": "1",
            "penalty": {
                "yards_to_endzone": "2",
                "yards_on_play": "5",
                "team_penalized": filler["team_in_possession"],
                "team_in_possession": filler["team_in_possession"],
            },
        }
    ]
    filler["description"] = "PENALTY on CLE-54-S.Williams Neutral Zone Infraction 5 yards enforced at JAC 2 - No Play."
    summary, _ = _cbs_nfl_to_espn_summary(
        ordered[:try_index] + [filler] + ordered[try_index:], drives, scoreboard, CLE_JAX_ROW
    )
    spliced = [p for d in summary["drives"]["previous"] for p in d["plays"] if p["id"].endswith(filler["id"])]
    assert spliced and "pointAfterAttempt" not in spliced[0]
    holder = [p for d in summary["drives"]["previous"] for p in d["plays"] if "pointAfterAttempt" in p]
    assert all(p["type"]["abbreviation"] == "TD" for p in holder)


def test_timeouts_are_synthesized_and_an_implausible_drop_is_ignored(cle_jax_summary):
    """CBS emits no admin row; a decrement of exactly 1 is a timeout, a bigger one is a glitch."""
    summary, notes = cle_jax_summary
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    timeouts = [p for p in plays if p["type"]["text"] == "Timeout"]
    assert len(timeouts) == 3, [p["text"] for p in timeouts]
    assert all(p["homeTimeoutCalled"] or p["awayTimeoutCalled"] for p in timeouts)
    assert all(p["start"]["down"] == 0 for p in plays if p["type"]["text"].startswith("End"))
    assert any("implausible timeouts-remaining drops" in n for n in notes)


# ---------------------------------------------------------------- gate 2: in progress


@pytest.mark.parametrize("keep", [20, 70, 120])
def test_an_in_progress_payload_serves_an_open_drive(cle_jax_payload, keep):
    payload = json.loads(json.dumps(cle_jax_payload))
    ordered = sorted(payload["plays"]["plays"], key=lambda p: int(p["id"]))[:keep]
    payload["plays"]["plays"] = ordered
    open_drive = int(ordered[-1]["drive_id"])
    payload["drives"]["drives"] = [d for d in payload["drives"]["drives"] if int(d["id"]) <= open_drive]
    payload["drives"]["drives"][-1]["result"] = None
    status = payload["scoreboard"]["scoreboard"]["game_status"]
    status["status"], status["quarter"] = "INPROGRESS", ordered[-1]["quarter"]
    processed = _process_game(
        "nfl", CLE_JAX_ESPN_ID, source="cbs", fallthrough=False, payloads={"cbs": payload}, idmap_row=CLE_JAX_ROW
    )
    assert processed.provenance["served"] == "cbs"
    assert processed.provenance["contract"]["ok"]
    assert processed.plays_frame.height > 0
    assert processed.plays_frame["type.text"].null_count() == 0
    assert any("in progress" in n for n in processed.provenance["notes"])


def test_the_open_drive_carries_no_invented_outcome(cle_jax_payload):
    payload = json.loads(json.dumps(cle_jax_payload))
    ordered = sorted(payload["plays"]["plays"], key=lambda p: int(p["id"]))[:70]
    payload["plays"]["plays"] = ordered
    open_drive = int(ordered[-1]["drive_id"])
    payload["drives"]["drives"] = [d for d in payload["drives"]["drives"] if int(d["id"]) <= open_drive]
    payload["drives"]["drives"][-1]["result"] = None
    payload["scoreboard"]["scoreboard"]["game_status"]["status"] = "INPROGRESS"
    plays, drives, scoreboard = _bodies(payload)
    summary, _ = _cbs_nfl_to_espn_summary(plays, drives, scoreboard, CLE_JAX_ROW)
    assert "current" in summary["drives"]
    assert summary["drives"]["current"]["displayResult"] is None
    assert summary["drives"]["current"]["result"] is None
    assert summary["header"]["competitions"][0]["status"]["type"]["completed"] is False


# --------------------------------------------------------- the CBS game id resolution


@pytest.fixture(autouse=True)
def _clear_scoreboard_cache():
    """The page cache is per process and would leak between tests (and across a stub)."""
    from sportsdataverse.nfl.cbs_pbp.game_id import _PAGE_CACHE

    _PAGE_CACHE.clear()
    yield
    _PAGE_CACHE.clear()


def _stub_transport(html: str):
    class _Resp:
        text = html

    def transport(url, **kwargs):
        transport.urls.append(url)
        return _Resp()

    transport.urls = []
    return transport


def test_the_cbs_game_id_comes_from_the_week_scoreboard():
    html = (CBS_FIX / "scoreboard_2019_regular_1_cards.html").read_text(encoding="utf-8")
    assert len(_parse_scoreboard(html)) == 16
    transport = _stub_transport(html)
    # HOU (34) at NO (18), 2019 week 1
    cbs_id, provenance = _resolve_cbs_game_id(
        2019, 2, 1, "18", "34", kickoff_utc="2019-09-09T17:10Z", transport=transport
    )
    assert cbs_id == "3115586"
    assert provenance["enhanced"] is True
    assert transport.urls == ["https://www.cbssports.com/nfl/scoreboard/2019/regular/1/"]


def test_an_unresolvable_game_returns_none_rather_than_inventing_an_id():
    transport = _stub_transport("<html></html>")
    cbs_id, provenance = _resolve_cbs_game_id(2019, 2, 1, "18", "34", transport=transport)
    assert cbs_id is None and provenance["how"] == "unresolved"


def test_the_adapter_hands_over_when_the_id_cannot_be_resolved(monkeypatch):
    from sportsdataverse.nfl.cbs_pbp import game_id as game_id_module
    from sportsdataverse.nfl.cbs_pbp import to_espn_summary as adapter_module

    monkeypatch.setattr(game_id_module, "_resolve_cbs_game_id", lambda *a, **k: (None, {"how": "unresolved"}))
    monkeypatch.setattr(adapter_module, "_fetch_cbs_game", lambda *a, **k: pytest.fail("must not fetch"))
    row = {k: v for k, v in CLE_JAX_ROW.items() if k != "cbs_game_id"}
    adapter = _adapter_for("nfl", "cbs")

    class _Ctx:
        idmap_row = row
        payload = None
        participants = None
        odds_override = None

    with pytest.raises(SourceUnavailable):
        adapter("nfl", CLE_JAX_ESPN_ID, _Ctx())


def test_a_green_but_empty_napi_body_hands_over():
    """NAPI answers an absent resource with HTTP 200 + a ``warnings`` envelope, never a 404."""
    adapter = _adapter_for("nfl", "cbs")

    class _Ctx:
        idmap_row = CLE_JAX_ROW
        payload = {
            "cbs_game_id": "50029216",
            "plays": {"warnings": [{"code": 404, "message": "No scoring plays data for that game."}]},
            "scoreboard": {},
            "drives": None,
            "odds": None,
        }
        participants = None
        odds_override = None

    with pytest.raises(SourceUnavailable, match="carries no plays"):
        adapter("nfl", CLE_JAX_ESPN_ID, _Ctx())
