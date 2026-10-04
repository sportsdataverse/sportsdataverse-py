"""capture_season resume vs refresh: presence-on-disk must not freeze the current season."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from sportsdataverse.scrape.stats import season_capture as sc


def _gamelog(*game_ids: str) -> dict:
    """A leaguegamelog payload with one row per game id (none -> a valid zero-row envelope)."""
    return {"resultSets": [{"name": "LeagueGameLog", "headers": ["GAME_ID"], "rowSet": [[g] for g in game_ids]}]}


@pytest.fixture(autouse=True)
def _no_retry_pause(monkeypatch):
    """The empty-refresh retries wait between attempts; the tests do not."""
    monkeypatch.setattr(sc, "_EMPTY_REFRESH_PAUSE_S", 0)


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


def test_nba_files_season_level_captures_under_the_end_year(tmp_path, plan):
    """NBA asks the API for the START year (2025 = 2025-26) but files the payload under
    the END year, beside ``playbyplayv3/2026/``; WNBA (calendar year) is unshifted."""
    fetch = lambda _endpoint, _kwargs: _gamelog("1")  # noqa: E731 - one-line stand-in transport
    assert sc.capture_season(2025, tmp_path, fetch, SimpleNamespace(), "nba_stats", "00") == (1, 0, 0)
    assert (tmp_path / "leaguegamelog" / "2026" / "regular-season.json").exists()
    assert not (tmp_path / "leaguegamelog" / "2025").exists()

    assert sc.capture_season(2026, tmp_path / "w", fetch, SimpleNamespace(), "wnba_stats", "10") == (1, 0, 0)
    assert (tmp_path / "w" / "leaguegamelog" / "2026" / "regular-season.json").exists()


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


def _run_sequence(root, answers, *, log=None):
    """One refresh pass whose fetches return ``answers`` in order (an Exception is raised)."""
    calls: list[str] = []

    def fetch(endpoint, _kwargs):
        calls.append(endpoint)
        answer = answers[min(len(calls), len(answers)) - 1]
        if isinstance(answer, Exception):
            raise answer
        return answer

    kwargs = {"log": log} if log else {}
    counts = sc.capture_season(2026, root, fetch, SimpleNamespace(), "wnba_stats", "10", refresh=True, **kwargs)
    return counts, calls


@pytest.mark.parametrize(
    "answers",
    [
        [_gamelog(), _gamelog("1", "2", "3")],  # one empty answer, then the real one
        [_gamelog(), RuntimeError("proxy died"), _gamelog("1", "2", "3")],  # a retry may fail too
    ],
)
def test_an_empty_refresh_is_asked_again_before_the_old_capture_is_kept(tmp_path, plan, answers):
    """The 2026-10-04 WNBA case: the game index answered empty once and the daily run
    indexed no new playoff game. The capture now asks again and lands the new games."""
    path = sc.payload_path(tmp_path, "leaguegamelog", 2026, "regular-season")
    _run(tmp_path, _gamelog("1", "2"), refresh=False)
    messages: list[str] = []
    counts, calls = _run_sequence(tmp_path, answers, log=messages.append)
    assert counts == (1, 0, 0)
    assert len(calls) == len(answers)
    assert sc.game_ids_from_gamelog(json.loads(path.read_text())) == ["0000000001", "0000000002", "0000000003"]
    assert any("retry" in m and "returned some" in m for m in messages)


def test_a_refresh_that_stays_empty_keeps_the_capture_after_the_retries(tmp_path, plan):
    """Still empty after every retry: the populated capture stays, and the log says so."""
    path = sc.payload_path(tmp_path, "leaguegamelog", 2026, "regular-season")
    _run(tmp_path, _gamelog("1", "2"), refresh=False)
    messages: list[str] = []
    counts, calls = _run_sequence(tmp_path, [_gamelog()], log=messages.append)
    assert counts == (0, 1, 0)
    assert len(calls) == 1 + sc._EMPTY_REFRESH_RETRIES
    assert sc.game_ids_from_gamelog(json.loads(path.read_text())) == ["0000000001", "0000000002"]
    assert any("kept previous capture" in m for m in messages)


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
