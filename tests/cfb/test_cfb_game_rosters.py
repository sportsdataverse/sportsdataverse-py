"""Offline regression tests for espn_cfb_game_rosters long-tail robustness.

Two failure modes surfaced by the 22-year CFB backfill, both fixed in
``sportsdataverse/cfb/cfb_game_rosters.py``:

1. Older games (e.g. pre-2021) omit the team-level ``statistics`` ``$ref`` in
   the competitors payload, so ``statistics_href`` is absent and the strict
   ``items.rename({..., "statistics_href": "team_statistics_href"})`` raised
   ``polars.exceptions.ColumnNotFoundError`` for the whole game.
2. A single team's ``/roster`` sub-endpoint can 404 (``NoESPNDataError``) while
   the other team's roster exists; the per-team loop used to let that one 404
   fail the entire game, discarding the recoverable team's roster too.

These are unit tests of the pure helpers (no network) — ``download`` is
monkeypatched so the team-roster branches are exercised deterministically.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.cfb import cfb_game_rosters
from sportsdataverse.cfb.cfb_game_rosters import helper_cfb_game_items, helper_cfb_roster_items
from sportsdataverse.errors import NoESPNDataError


def _competitor(team_id: int, *, with_statistics: bool) -> dict:
    item = {
        "id": str(team_id),
        "uid": f"s:20~l:23~t:{team_id}",
        "order": 0,
        "homeAway": "home",
        "winner": True,
        "team": {"$ref": f"http://core/teams/{team_id}"},
    }
    if with_statistics:
        item["statistics"] = {"$ref": f"http://core/teams/{team_id}/statistics"}
    return item


def test_game_items_without_statistics_ref_does_not_raise():
    """Regression #1: a competitors payload missing the ``statistics`` ``$ref``
    must flatten cleanly (no ColumnNotFoundError) and simply omit
    ``team_statistics_href``."""
    summary = {"items": [_competitor(52, with_statistics=False), _competitor(2641, with_statistics=False)]}
    items = helper_cfb_game_items(summary)
    assert isinstance(items, pl.DataFrame)
    assert items.height == 2
    assert "team_id" in items.columns and "team_uid" in items.columns
    assert "team_statistics_href" not in items.columns
    assert sorted(items["team_id"].to_list()) == [52, 2641]


def test_game_items_with_statistics_ref_renames():
    """When the ``statistics`` ``$ref`` is present it is renamed to
    ``team_statistics_href`` (unchanged happy-path behavior)."""
    summary = {"items": [_competitor(52, with_statistics=True)]}
    items = helper_cfb_game_items(summary)
    assert "team_statistics_href" in items.columns


class _FakeResp:
    def __init__(self, payload: dict):
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def _roster_entry(player_id: int) -> dict:
    return {
        "playerId": player_id,
        "period": 0,
        "active": True,
        "forPlayerId": 0,
        "starter": True,
        "athlete": {"$ref": f"http://core/athletes/{player_id}"},
    }


def test_roster_items_one_team_404_recovers_other(monkeypatch):
    """Regression #2: when one team's roster 404s, the other team's roster is
    still recovered instead of failing the whole game."""
    items = pl.DataFrame({"team_id": [52, 2641]})

    def fake_download(url, **kwargs):
        if url.endswith("/2641/roster"):
            raise NoESPNDataError("NoESPNDataError: No data found for " + url)
        return _FakeResp({"entries": [_roster_entry(101), _roster_entry(102)]})

    monkeypatch.setattr(cfb_game_rosters, "download", fake_download)
    out = helper_cfb_roster_items(items=items, summary_url="http://core/competitors")
    assert isinstance(out, pl.DataFrame)
    assert out.height == 2  # only team 52's two entries
    assert out["team_id"].unique().to_list() == [52]


def test_roster_items_all_teams_404_raises(monkeypatch):
    """When every team's roster 404s the game genuinely has no roster data, so a
    single NoESPNDataError is raised (caught upstream and degraded to empty)."""
    items = pl.DataFrame({"team_id": [52, 2641]})

    def fake_download(url, **kwargs):
        raise NoESPNDataError("NoESPNDataError: No data found for " + url)

    monkeypatch.setattr(cfb_game_rosters, "download", fake_download)
    with pytest.raises(NoESPNDataError):
        helper_cfb_roster_items(items=items, summary_url="http://core/competitors")
