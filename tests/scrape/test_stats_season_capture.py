"""capture_season resume vs refresh: presence-on-disk must not freeze the current season."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from sportsdataverse.scrape.stats import season_capture as sc


def _gamelog(*game_ids: str) -> dict:
    """A leaguegamelog payload with one row per game id (none -> a valid zero-row envelope)."""
    return {"resultSets": [{"name": "LeagueGameLog", "headers": ["GAME_ID"], "rowSet": [[g] for g in game_ids]}]}


@pytest.fixture
def plan(monkeypatch):
    """Plan exactly one season-level capture, so the tests need no stats module."""
    monkeypatch.setattr(
        sc, "plan_season", lambda *_a: iter([("leaguegamelog", "regular-season", {"season_type": "Regular Season"})])
    )


def _run(root, answer, *, refresh):
    """One capture_season pass whose every fetch returns ``answer`` (or raises it)."""

    def fetch(_endpoint, _kwargs):
        """Stand-in transport: raise an Exception answer, return anything else."""
        if isinstance(answer, Exception):
            raise answer
        return answer

    return sc.capture_season(2026, root, fetch, SimpleNamespace(), "wnba_stats", "10", refresh=refresh)


def test_resume_skips_existing_but_refresh_refetches(tmp_path, plan):
    """Resume ignores a newer answer for an existing file; refresh lands it."""
    path = sc.payload_path(tmp_path, "leaguegamelog", 2026, "regular-season")
    assert _run(tmp_path, _gamelog("1"), refresh=False) == (1, 0, 0)

    # Resume: a newer answer is ignored because the file exists -- the Sept 2026 freeze.
    assert _run(tmp_path, _gamelog("1", "2"), refresh=False) == (0, 1, 0)
    assert sc.game_ids_from_gamelog(json.loads(path.read_text())) == ["0000000001"]

    # Refresh: the new games land.
    assert _run(tmp_path, _gamelog("1", "2"), refresh=True) == (1, 0, 0)
    assert sc.game_ids_from_gamelog(json.loads(path.read_text())) == ["0000000001", "0000000002"]


@pytest.mark.parametrize(
    ("answer", "counts"),
    [
        (RuntimeError("proxy died"), (0, 0, 1)),  # failed fetch
        (_gamelog(), (0, 1, 0)),  # valid envelope, zero rows
        ({}, (0, 0, 1)),  # contentless
        # v2 with no tables: list-valued parameters must not read as rows
        ({"resultSets": [], "parameters": {"TeamIDs": [1, 2]}}, (0, 1, 0)),
    ],
)
def test_refresh_never_downgrades_a_populated_capture(tmp_path, plan, answer, counts):
    """A failed fetch, a zero-row envelope or ``{}`` keeps the populated file."""
    path = sc.payload_path(tmp_path, "leaguegamelog", 2026, "regular-season")
    _run(tmp_path, _gamelog("1", "2"), refresh=False)
    assert _run(tmp_path, answer, refresh=True) == counts
    assert len(json.loads(path.read_text())["resultSets"][0]["rowSet"]) == 2


def test_refresh_keeps_populated_v3_capture_and_survives_bad_bytes(tmp_path, plan):
    """v3 payloads carry no result tables; an empty entity list must not replace rows."""
    path = sc.payload_path(tmp_path, "leaguegamelog", 2026, "regular-season")
    full = {"meta": {"version": 1}, "games": {"list": [{"gameId": "1"}, {"gameId": "2"}]}}
    empty = {"meta": {"version": 1}, "games": {"list": []}}
    _run(tmp_path, full, refresh=False)
    assert _run(tmp_path, empty, refresh=True) == (0, 1, 0)
    assert json.loads(path.read_text()) == full

    path.write_bytes(b"\xff\xfe not utf-8")  # an unreadable capture is replaced, never fatal
    assert _run(tmp_path, _gamelog(), refresh=True) == (1, 0, 0)  # zero rows forces the read


def test_rosters_enumerate_from_disk_when_team_stats_refresh_fails(tmp_path, monkeypatch):
    """A failed team-stats refresh still yields rosters, from the capture on disk."""
    team_kwargs = {
        "season_type_all_star": "Regular Season",
        "measure_type_detailed_defense": "Base",
        "per_mode_detailed": "Totals",
    }
    monkeypatch.setattr(
        sc, "plan_season", lambda *_a: iter([("leaguedashteamstats", "regular-season_base_totals", team_kwargs)])
    )
    teams = {
        "resultSets": [{"name": "LeagueDashTeamStats", "headers": ["TEAM_ID"], "rowSet": [[1611661313], [1611661319]]}]
    }
    sc.write_payload(sc.payload_path(tmp_path, "leaguedashteamstats", 2026, "regular-season_base_totals"), teams)
    rosters = []

    def fetch(endpoint, kwargs):
        """Team stats fail; rosters answer with one player row."""
        if endpoint == "leaguedashteamstats":
            raise RuntimeError("proxy died")
        rosters.append(kwargs["team_id"])
        return {"resultSets": [{"name": "CommonTeamRoster", "headers": ["PLAYER_ID"], "rowSet": [[1]]}]}

    module = SimpleNamespace(wnba_stats_commonteamroster=None)
    assert sc.capture_season(2026, tmp_path, fetch, module, "wnba_stats", "10", refresh=True) == (2, 0, 1)
    assert sorted(rosters) == ["1611661313", "1611661319"]
