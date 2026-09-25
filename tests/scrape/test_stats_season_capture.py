"""capture_season resume vs refresh: presence-on-disk must not freeze the current season."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from sportsdataverse.scrape.stats import season_capture as sc


def _gamelog(*game_ids: str) -> dict:
    return {"resultSets": [{"name": "LeagueGameLog", "headers": ["GAME_ID"], "rowSet": [[g] for g in game_ids]}]}


@pytest.fixture
def plan(monkeypatch):
    monkeypatch.setattr(
        sc, "plan_season", lambda *_a: iter([("leaguegamelog", "regular-season", {"season_type": "Regular Season"})])
    )


def _run(root, answer, *, refresh):
    def fetch(_endpoint, _kwargs):
        if isinstance(answer, Exception):
            raise answer
        return answer

    return sc.capture_season(2026, root, fetch, SimpleNamespace(), "wnba_stats", "10", refresh=refresh)


def test_resume_skips_existing_but_refresh_refetches(tmp_path, plan):
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
    ],
)
def test_refresh_never_downgrades_a_populated_capture(tmp_path, plan, answer, counts):
    path = sc.payload_path(tmp_path, "leaguegamelog", 2026, "regular-season")
    _run(tmp_path, _gamelog("1", "2"), refresh=False)
    assert _run(tmp_path, answer, refresh=True) == counts
    assert len(json.loads(path.read_text())["resultSets"][0]["rowSet"]) == 2
