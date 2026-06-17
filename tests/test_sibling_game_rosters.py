"""Offline regression tests: the espn_*_game_rosters long-tail robustness fixes
applied across all six ESPN sibling modules (wbb, wnba, nba, mbb, nhl, nfl),
mirroring tests/cfb/test_cfb_game_rosters.py.

Both fixes (statistics_href strict-rename guard; per-team roster 404 tolerance)
were templated identically into every module — these tests exercise each
module's pure helpers with synthetic payloads + a monkeypatched ``download`` so
no network is required.
"""

from __future__ import annotations

import importlib

import polars as pl
import pytest

from sportsdataverse.errors import NoESPNDataError

SPORTS = ["wbb", "wnba", "nba", "mbb", "nhl", "nfl"]


def _mod(sport: str):
    return importlib.import_module(f"sportsdataverse.{sport}.{sport}_game_rosters")


def _competitor(team_id: int, *, with_statistics: bool) -> dict:
    item = {
        "id": str(team_id),
        "uid": f"s:00~l:00~t:{team_id}",
        "order": 0,
        "homeAway": "home",
        "winner": True,
        "team": {"$ref": f"http://core/teams/{team_id}"},
    }
    if with_statistics:
        item["statistics"] = {"$ref": f"http://core/teams/{team_id}/statistics"}
    return item


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


@pytest.mark.parametrize("sport", SPORTS)
def test_game_items_without_statistics_ref_does_not_raise(sport):
    """A competitors payload missing the team-level ``statistics`` ``$ref`` must
    flatten cleanly (no ColumnNotFoundError) and omit ``team_statistics_href``."""
    fn = getattr(_mod(sport), f"helper_{sport}_game_items")
    items = fn({"items": [_competitor(52, with_statistics=False), _competitor(2641, with_statistics=False)]})
    assert isinstance(items, pl.DataFrame)
    assert items.height == 2
    assert "team_id" in items.columns
    assert "team_statistics_href" not in items.columns


@pytest.mark.parametrize("sport", SPORTS)
def test_game_items_with_statistics_ref_renames(sport):
    """When the ``statistics`` ``$ref`` is present it is renamed to
    ``team_statistics_href`` (unchanged happy-path behavior)."""
    fn = getattr(_mod(sport), f"helper_{sport}_game_items")
    items = fn({"items": [_competitor(52, with_statistics=True)]})
    assert "team_statistics_href" in items.columns


@pytest.mark.parametrize("sport", SPORTS)
def test_roster_items_one_team_404_recovers_other(sport, monkeypatch):
    """When one team's roster 404s, the other team's roster is still recovered."""
    mod = _mod(sport)
    items = pl.DataFrame({"team_id": [52, 2641]})

    def fake_download(url, **kwargs):
        if url.endswith("/2641/roster"):
            raise NoESPNDataError("NoESPNDataError: No data found for " + url)
        return _FakeResp({"entries": [_roster_entry(101), _roster_entry(102)]})

    monkeypatch.setattr(mod, "download", fake_download)
    out = getattr(mod, f"helper_{sport}_roster_items")(items=items, summary_url="http://core/competitors")
    assert isinstance(out, pl.DataFrame)
    assert out.height == 2
    assert out["team_id"].unique().to_list() == [52]


@pytest.mark.parametrize("sport", SPORTS)
def test_roster_items_all_teams_404_raises(sport, monkeypatch):
    """When every team's roster 404s the game genuinely has no roster data, so a
    single NoESPNDataError is raised (caught upstream and degraded to empty)."""
    mod = _mod(sport)
    items = pl.DataFrame({"team_id": [52, 2641]})

    def fake_download(url, **kwargs):
        raise NoESPNDataError("NoESPNDataError: No data found for " + url)

    monkeypatch.setattr(mod, "download", fake_download)
    with pytest.raises(NoESPNDataError):
        getattr(mod, f"helper_{sport}_roster_items")(items=items, summary_url="http://core/competitors")


@pytest.mark.parametrize("sport", SPORTS)
def test_roster_items_differing_team_columns_concat(sport, monkeypatch):
    """The two teams' roster-entry payloads can carry different column sets; the
    per-team concat must be diagonal (union + null-fill), not vertical (which
    raised polars ShapeError and lost the whole game)."""
    mod = _mod(sport)
    items = pl.DataFrame({"team_id": [52, 2641]})

    def _entry(pid, **extra):
        e = {
            "playerId": pid,
            "period": 0,
            "active": True,
            "forPlayerId": 0,
            "starter": True,
            "athlete": {"$ref": f"http://core/athletes/{pid}"},
        }
        e.update(extra)
        return e

    def fake_download(url, **kwargs):
        if url.endswith("/52/roster"):
            return _FakeResp({"entries": [_entry(101, jersey="7"), _entry(102, jersey="9")]})
        return _FakeResp({"entries": [_entry(201, didNotPlay=True), _entry(202, didNotPlay=False)]})

    monkeypatch.setattr(mod, "download", fake_download)
    out = getattr(mod, f"helper_{sport}_roster_items")(items=items, summary_url="http://core/competitors")
    assert out.height == 4
    assert set(out["team_id"].unique().to_list()) == {52, 2641}
