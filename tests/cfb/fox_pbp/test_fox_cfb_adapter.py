"""CFB-side Fox adapter: geometry, drive regrouping, no-coverage and dispatch. Zero network."""

from __future__ import annotations

import json
import pathlib

import pytest

from sportsdataverse.cfb.fox_pbp.to_espn_summary import (
    ESPN_PLAY_TYPES,
    _fox_adapter,
    _fox_cfb_to_espn_summary,
    _resolve_fox_event_id,
)
from sportsdataverse.football.sources.contract import _validate_summary
from sportsdataverse.football.sources.dispatch import SourceUnavailable, _process_game

FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "fox"
ROW = {
    "espn_event_id": "401856679",
    "season": 2026,
    "season_type": 2,
    "week": 2,
    "home_espn_team_id": "130",  # Michigan
    "away_espn_team_id": "201",  # Oklahoma
}


def _load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class _Ctx:
    def __init__(self, payload=None, idmap_row=None, odds_override=None):
        self.payload = payload
        self.idmap_row = idmap_row
        self.participants = None
        self.odds_override = odds_override


@pytest.fixture(scope="module")
def final():
    return _load("fox_cfb_401856679.json")


@pytest.fixture(scope="module")
def adapted(final):
    return _fox_cfb_to_espn_summary(final, ROW)


def _plays(summary):
    drives = summary["drives"]
    groupings = list(drives["previous"]) + ([drives["current"]] if drives.get("current") else [])
    return [p for d in groupings for p in d["plays"]]


def test_the_adapted_summary_satisfies_the_contract(adapted):
    summary, _ = adapted
    report = _validate_summary(summary, "cfb")
    assert report.ok, (report.missing, report.invalid)
    assert report.gop_ok, report.gop_missing
    assert report.n_plays > 150


def test_every_emitted_type_id_is_in_the_espn_college_vocabulary(adapted):
    summary, _ = adapted
    for play in _plays(summary):
        assert play["type"]["id"] in ESPN_PLAY_TYPES
        assert play["type"]["text"] == ESPN_PLAY_TYPES[play["type"]["id"]][0]
        assert "abbreviation" in play["type"]  # Game on Paper bracket-reads it


def test_field_position_is_absolute_not_yards_to_goal(adapted):
    """The defect in the shipped ``cfb_pbp_fox``: it reads ``yardStart`` as yards-to-goal, which
    mirrors the field on every away-offense play (50.0% / 53.7% of spots right)."""
    summary, _ = adapted
    home = ROW["home_espn_team_id"]
    scrimmage = [p for p in _plays(summary) if p["start"]["down"] and p["start"]["yardLine"] is not None]
    home_rows = [p for p in scrimmage if str(p["start"]["team"]["id"]) == home]
    away_rows = [p for p in scrimmage if str(p["start"]["team"]["id"]) != home]
    assert home_rows and away_rows
    assert all(p["start"]["yardsToEndzone"] == 100 - p["start"]["yardLine"] for p in home_rows)
    assert all(p["start"]["yardsToEndzone"] == p["start"]["yardLine"] for p in away_rows)


def test_a_row_grouped_with_the_possession_that_just_ended_is_flipped(adapted):
    """Fox files the receiving team's first stoppage under the punting team's drive; taking the
    group's team there put the ball 98 yards from where it was (6.1 EPA on the punt)."""
    summary, notes = adapted
    assert any("possession flipped" in n for n in notes)
    plays = _plays(summary)
    punts = [(i, p) for i, p in enumerate(plays) if p["type"]["id"] == "52"]
    assert punts
    for index, punt in punts:
        nxt = next((p for p in plays[index + 1 :] if p["type"]["id"] not in ("2", "21", "65", "66", "74", "75")), None)
        if nxt is not None:
            assert str(nxt["start"]["team"]["id"]) != str(punt["start"]["team"]["id"])


def test_a_made_field_goal_ends_where_it_was_kicked_from(adapted):
    summary, _ = adapted
    made = [p for p in _plays(summary) if p["type"]["id"] == "59"]
    assert made
    for play in made:
        assert play["end"]["yardsToEndzone"] == play["start"]["yardsToEndzone"]
        assert play["end"]["down"] == -1


def test_an_fcs_hosted_game_hands_over_by_shape():
    """Fox, CBS and Yahoo all serve FCS-hosted games as HTTP 200 with no plays, in every era."""
    payload = _load("fox_cfb_no_coverage.json")
    meta = _load("fox_cfb_no_coverage_meta.json")
    assert payload["header"]["leftTeam"]["name"], "the fixture must carry a real header"
    with pytest.raises(SourceUnavailable, match="carries no play-by-play"):
        _fox_adapter("cfb", 0, _Ctx(payload=payload, idmap_row={**ROW, "espn_event_id": "0"}))
    assert meta["n_plays"] == 0


def test_the_crosswalk_is_the_only_fallback_and_never_invents_an_id(monkeypatch):
    assert _resolve_fox_event_id("401856679", {"fox_event_id": "43065"}) == ("43065", "idmap")
    import sportsdataverse.cfb.fox_pbp.to_espn_summary as module

    monkeypatch.setattr(module, "_crosswalk_fox_id", lambda espn_id, seasons: None)
    assert _resolve_fox_event_id("401856679", {"season": 2026})[0] is None
    with pytest.raises(SourceUnavailable, match="not computable"):
        _fox_adapter("cfb", 401856679, _Ctx(idmap_row={"season": 2026}))


def test_dispatch_serves_the_game_from_fox(final):
    served = _process_game(
        "cfb",
        int(ROW["espn_event_id"]),
        source="fox",
        fallthrough=False,
        payloads={"fox": final},
        idmap_row=ROW,
        odds_override={"gameSpread": 5.5, "overUnder": 43.5, "homeFavorite": False, "gameSpreadAvailable": True},
    )
    assert served.provenance["served"] == "fox"
    assert served.provenance["contract"]["ok"]
    assert served.provenance["lossy_columns"]
    assert served.plays_frame.height > 150
    assert served.plays_frame["EPA"].null_count() < served.plays_frame.height
