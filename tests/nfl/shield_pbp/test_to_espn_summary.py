"""Shield -> ESPN-summary adapter: parity on a final, the live path, and the old-era path.

One test per gate class of the Stage 2 Phase 3 evidence run
(``background-research/2026-09-17-football-sources-program/s2-shield-adapter/``), on real
payloads only. The pinned floors are the values that run **observed**; they are never
lowered, and a regression below one is a failure, not a re-pin.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.football.sources.dispatch import SourceUnavailable, _adapter_for, _process_game
from sportsdataverse.football.sources.parity import GOP_BOX_SECTIONS, _compare_processed
from sportsdataverse.nfl.shield_pbp import shield_nfl_pbp, shield_to_espn_summary

SHIELD_FIX = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield"
ESPN_FIX = Path(__file__).resolve().parents[1] / "fixtures"

#: CLE @ JAX, 2026 week 1 -- the one game that has a committed payload on both feeds.
CLE_JAX_ESPN_ID = 401872922
CLE_JAX_ROW = {
    "league": "nfl",
    "espn_event_id": str(CLE_JAX_ESPN_ID),
    "home_espn_team_id": "30",
    "away_espn_team_id": "5",
    "shield_game_id": "ef4d0e9a-4feb-11f1-abca-2c54536568a9",
    "nflverse_game_id": "2026_01_CLE_JAX",
    "spread_line": -1.5,
    "total_line": 46.5,
    "odds_source": "nflverse_schedule",
    "home_team": {"espn_abbr": "JAX"},
    "away_team": {"espn_abbr": "CLE"},
}
DET_BUF_ROW = {
    "league": "nfl",
    "espn_event_id": "401872932",
    "home_espn_team_id": "2",
    "away_espn_team_id": "8",
    "shield_game_id": "a9a8944e-4feb-11f1-abca-2c54536568a9",
    "home_team": {"espn_abbr": "BUF"},
    "away_team": {"espn_abbr": "DET"},
}
CIN_CLE_ROW = {
    "league": "nfl",
    "espn_event_id": "250911005",
    "home_espn_team_id": "5",
    "away_espn_team_id": "4",
    "shield_game_id": "2005-cin-cle",
    "home_team": {"espn_abbr": "CLE"},
    "away_team": {"espn_abbr": "CIN"},
}

#: Observed on 2026_01_CLE_JAX (154 plays each side, 135 paired). Floors, never re-pinned down.
PINNED = {
    "paired_share": 0.87,
    "agreement": {
        "type.id": 1.0,
        "type.text": 1.0,
        "type.abbreviation": 1.0,
        "start.down": 1.0,
        "start.distance": 1.0,
        "start.yardsToEndzone": 1.0,
        "start.pos_team.id": 1.0,
        "end.pos_team.id": 1.0,
        "period": 1.0,
        "pos_score_diff_start": 1.0,
        "td_play": 1.0,
        "pass": 1.0,
        "rush": 1.0,
        "turnover_vec": 1.0,
        "penalty_flag": 1.0,
        "fg_made": 1.0,
        "drive.id": 1.0,
        "statYardage": 0.95,
        "drive.result": 1.0,
    },
    "correlation": {"EP_start": 0.999, "EP_end": 0.999, "EPA": 0.995, "wp_before": 0.999, "wp_after": 0.999},
    "required_columns": ["type.id", "type.abbreviation", "start.yardsToEndzone", "drive.id"],
}


def _load(name: str) -> dict:
    with gzip.open(SHIELD_FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def cle_jax_shield() -> dict:
    return _load("2026_01_CLE_JAX.json.gz")


@pytest.fixture(scope="module")
def cle_jax_summary(cle_jax_shield) -> tuple[dict, list[str]]:
    return shield_to_espn_summary(cle_jax_shield, CLE_JAX_ROW)


@pytest.fixture(scope="module")
def cle_jax_pair(cle_jax_shield):
    """Both paths through the unmodified processor, same stored closing line (two pipeline runs)."""
    espn = json.loads((ESPN_FIX / f"summary_{CLE_JAX_ESPN_ID}.json").read_text(encoding="utf-8"))
    reference = _process_game(
        "nfl", CLE_JAX_ESPN_ID, source="espn", fallthrough=False, payloads={"espn": espn}, idmap_row=CLE_JAX_ROW
    )
    candidate = _process_game(
        "nfl",
        CLE_JAX_ESPN_ID,
        source="shield",
        fallthrough=False,
        payloads={"shield": cle_jax_shield},
        idmap_row=CLE_JAX_ROW,
    )
    return reference, candidate


# --------------------------------------------------------------------------- gate 1: finals


def test_finals_parity_against_the_real_espn_summary(cle_jax_pair):
    reference, candidate = cle_jax_pair
    assert candidate.provenance["served"] == "shield"
    assert reference.plays_frame.height == candidate.plays_frame.height
    report = _compare_processed(
        reference,
        candidate,
        columns=tuple(PINNED["agreement"]) + tuple(PINNED["correlation"]),
    )
    assert report.check(PINNED) == []


def test_dispatch_resolves_the_shield_source(cle_jax_shield):
    assert _adapter_for("nfl", "shield") is not None
    processed = _process_game(
        "nfl",
        CLE_JAX_ESPN_ID,
        source="shield",
        fallthrough=False,
        payloads={"shield": cle_jax_shield},
        idmap_row=CLE_JAX_ROW,
    )
    assert processed.provenance["served"] == "shield"
    assert processed.provenance["native_ids"]["shield_game_id"] == CLE_JAX_ROW["shield_game_id"]
    # the stored closing line reached the processor -- not the 2.5 / 55.5 default
    assert processed.provenance["odds"] == {"source": "injected", "default": False, "from_idmap": True}


def test_contract_and_gop_fields(cle_jax_summary):
    summary, notes = cle_jax_summary
    report = _validate_summary(summary, "nfl")
    assert report.missing == [] and report.invalid == [] and report.gop_missing == []
    assert notes == []
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    # GOP bracket-reads type.id / type.text / type.abbreviation (python/app.py:204-208)
    assert all("id" in p["type"] and "text" in p["type"] and "abbreviation" in p["type"] for p in plays)
    assert all(p["type"]["id"] and p["type"]["text"] for p in plays)
    # pickcenter rides the stored closing line when one is given
    assert (
        shield_to_espn_summary(
            _load("2026_01_CLE_JAX.json.gz"),
            CLE_JAX_ROW,
            odds={"gameSpread": 1.5, "overUnder": 46.5, "homeFavorite": False, "gameSpreadAvailable": True},
        )[0]["pickcenter"][0]["overUnder"]
        == 46.5
    )


# --------------------------------------------------- the two pieces with a mutation check


def test_pat_is_folded_into_its_touchdown(cle_jax_summary):
    """No standalone PAT row; the try's text and its point ride on the touchdown play.

    Mutated out (emit ``XP_KICK`` as its own row), the play count no longer matches ESPN's
    and the touchdown's score step is 6 rather than 7 -- both assertions below fail.
    """
    summary, _ = cle_jax_summary
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    assert not any(p["text"].startswith(("extra point", "Extra Point")) for p in plays)
    touchdowns = [p for p in plays if p["type"]["abbreviation"] == "TD" and "pointAfterAttempt" in p]
    assert touchdowns, "fixture has no touchdown with a try"
    for play in touchdowns:
        assert "extra point" in play["text"].lower() or "two-point" in play["text"].lower()
        assert play["pointAfterAttempt"]["abbreviation"]
    # a good extra point steps the scoreboard by 7, never 6
    ordered = sorted(plays, key=lambda p: int(p["id"]))
    for i, play in enumerate(ordered):
        after = play["pointAfterAttempt"] if "pointAfterAttempt" in play else None
        if not (after and after["value"] == 1 and play["type"]["abbreviation"] == "TD" and i):
            continue
        before = ordered[i - 1]
        step = (play["homeScore"] - before["homeScore"]) + (play["awayScore"] - before["awayScore"])
        assert step == 7, f"play {play['id']} stepped the score by {step}, not 7"


def test_end_state_crosses_the_drive_boundary(cle_jax_summary):
    """A punt's end state is the receiving team's next snap, on the *next* drive.

    Mutated out (stop at the drive boundary), the punt's ``end.team.id`` equals its own
    ``start.team.id`` and the assertion below fails.
    """
    summary, _ = cle_jax_summary
    drives = summary["drives"]["previous"]
    checked = 0
    for i, drive in enumerate(drives[:-1]):
        plays = drive["plays"]
        if not plays or plays[-1]["type"]["text"] != "Punt":
            continue
        nxt = next((p for d in drives[i + 1 :] for p in d["plays"] if p["type"]["text"] not in ("Timeout",)), None)
        assert nxt is not None
        assert plays[-1]["end"]["team"]["id"] == nxt["start"]["team"]["id"] != plays[-1]["start"]["team"]["id"]
        checked += 1
    assert checked >= 2, f"only {checked} punts ended a drive in the fixture"


def test_admin_rows_are_present_and_charged(cle_jax_summary):
    """Timeout / end-of-period rows survive with ``down = 0`` and a timeout side."""
    summary, _ = cle_jax_summary
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    stoppages = [p for p in plays if p["type"]["text"] in ("Timeout", "Official Timeout", "End Period")]
    assert len(stoppages) >= 10
    assert all(p["start"]["down"] == 0 for p in stoppages)
    charged = [p for p in plays if p["type"]["text"] == "Timeout"]
    assert charged and all("homeTimeoutCalled" in p and "awayTimeoutCalled" in p for p in charged)
    assert any(p["homeTimeoutCalled"] for p in charged) or any(p["awayTimeoutCalled"] for p in charged)


# ------------------------------------------------------------------------- gate 2: live


@pytest.mark.parametrize("fixture", ["2026_02_DET_BUF_ingame_q2.json.gz", "2026_02_DET_BUF_ingame_q4.json.gz"])
def test_live_snapshot_processes(fixture):
    payload = _load(fixture)
    parsed = shield_nfl_pbp(game_detail=payload, enrich=False)
    summary, _notes = shield_to_espn_summary(payload, DET_BUF_ROW, parsed=parsed)
    report = _validate_summary(summary, "nfl")
    assert report.missing == [] and report.invalid == []
    status = summary["header"]["competitions"][0]["status"]
    assert status["type"]["state"] == "in" and status["type"]["completed"] is False
    assert status["shieldPhase"] == "INGAME" and status["period"]
    assert "current" in summary["drives"]  # the open drive is served separately, as ESPN does
    processed = _process_game(
        "nfl", 401872932, source="shield", fallthrough=False, payloads={"shield": payload}, idmap_row=DET_BUF_ROW
    )
    box = processed.game.get("advBoxScore") or {}
    assert [s for s in GOP_BOX_SECTIONS if not box.get(s)] == []
    frame = processed.plays_frame
    assert frame.select((pl.col("type.id").is_null() | pl.col("type.text").is_null()).sum()).item() == 0


def test_pregame_payload_hands_over(monkeypatch):
    """A payload with only the GAME_START marker is not servable -- it says so, not 'contract failed'."""
    from sportsdataverse.football.sources.dispatch import SourceContext

    adapter = _adapter_for("nfl", "shield")
    ctx = SourceContext(idmap_row=DET_BUF_ROW, payload=_load("2026_02_DET_BUF_pregame.json.gz"))
    with pytest.raises(SourceUnavailable):
        adapter("nfl", 401872932, ctx)


# -------------------------------------------------------------------- gate 3: old seasons


def test_oldest_era_game_adapts():
    """2005: gamebook ``CLV`` in the yard line, no ``specialTeamsPlayType``, no drive chart extras."""
    payload = _load("2005_01_CIN_CLE.json.gz")
    summary, notes = shield_to_espn_summary(payload, CIN_CLE_ROW)
    report = _validate_summary(summary, "nfl")
    assert report.missing == [] and report.invalid == []
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    assert len(plays) > 120
    # the CLV/CLE gamebook rename must not push the ball into the wrong half of the field
    assert all(p["start"]["yardLine"] is None or 0 <= p["start"]["yardLine"] <= 100 for p in plays)
    assert all(p["start"]["yardsToEndzone"] is None or 0 <= p["start"]["yardsToEndzone"] <= 100 for p in plays)
    # Stage 3 coverage study: pre-2014 ids do not join ESPN's own, and provenance must say so
    assert any("< 2014" in n for n in notes), notes


def test_modern_game_carries_no_era_note():
    """The id-join caveat is era-specific: a 2026 game must not carry it."""
    _summary, notes = shield_to_espn_summary(_load("2026_01_CLE_JAX.json.gz"), CLE_JAX_ROW)
    assert not any("< 2014" in n for n in notes)


# ------------------------------------------------- GOP's call shape: the ESPN id and nothing else


def test_gop_call_shape_resolves_the_idmap_itself(cle_jax_shield, monkeypatch):
    """``_process_game(league, espn_id, source=...)`` with no id map still serves Shield.

    Game on Paper's Flask calls exactly this (GOP #260): no ``idmap_row``, no ``odds_override``.
    Dispatch resolves the row once, and the adapter closes the rest from the nflverse schedule.
    """
    from sportsdataverse.football.sources import dispatch as dispatch_mod

    monkeypatch.delenv(dispatch_mod.IDMAP_BASE_URL_ENV, raising=False)
    monkeypatch.delenv(dispatch_mod.IDMAP_DIR_ENV, raising=False)
    processed = _process_game(
        "nfl", CLE_JAX_ESPN_ID, source="shield", fallthrough=False, payloads={"shield": cle_jax_shield}
    )
    assert processed.provenance["served"] == "shield"
    # dispatch could not resolve a row, and says so rather than pretending it did
    assert processed.provenance["idmap"] == {"source": "unresolved", "resolved": False}
    # the adapter closed the gap from the schedule, and stamps that in provenance
    assert processed.provenance["native_ids"]["idmap_resolved_by"] == "nflverse_schedule"
    frame = processed.plays_frame
    assert frame.height > 100
    assert frame.select(pl.col("start.pos_team.id").is_null().sum()).item() == 0


def test_unmapped_game_hands_over_rather_than_inventing_an_id(monkeypatch):
    """No id map, no schedule row, no payload -> ``SourceUnavailable``, never a fabricated id."""
    from sportsdataverse.football.sources import dispatch as dispatch_mod
    from sportsdataverse.nfl.shield_pbp import to_espn_summary as adapter_mod

    monkeypatch.delenv(dispatch_mod.IDMAP_BASE_URL_ENV, raising=False)
    monkeypatch.delenv(dispatch_mod.IDMAP_DIR_ENV, raising=False)
    monkeypatch.setattr(adapter_mod, "_idmap_row_from_schedule", lambda espn_id: None)
    ctx = dispatch_mod.SourceContext()
    with pytest.raises(SourceUnavailable):
        _adapter_for("nfl", "shield")("nfl", 1, ctx)


def test_idmap_lookup_failure_is_a_miss_not_a_raise(monkeypatch):
    """A 404 / timeout / missing asset degrades to ``unresolved`` so fallthrough reaches ESPN."""
    from sportsdataverse.football.sources import dispatch as dispatch_mod

    monkeypatch.setenv(dispatch_mod.IDMAP_BASE_URL_ENV, "http://127.0.0.1:1/never")
    monkeypatch.setenv(dispatch_mod.IDMAP_DIR_ENV, "/nonexistent/idmap")
    with pytest.warns(RuntimeWarning):
        row, how = dispatch_mod._resolve_idmap_row("nfl", CLE_JAX_ESPN_ID)
    assert row is None and how == "unresolved"


def test_missing_timeouts_is_surfaced_as_a_note():
    """``summary.timeouts`` is what a live consumer's timeouts-remaining comes from; say when it is gone."""
    payload = _load("2026_02_DET_BUF_ingame_q2.json.gz")
    stripped = json.loads(json.dumps(payload))
    for side in ("homeTeam", "awayTeam"):
        stripped["summary"][side].pop("timeouts", None)
    _summary, notes = shield_to_espn_summary(stripped, DET_BUF_ROW)
    assert any("summary.timeouts" in n for n in notes)
