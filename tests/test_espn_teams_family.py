"""Offline contract of the 8 hand-written ``espn_<lg>_teams`` functions (task 2.11).

Every payload is the real Site v2 capture ``tests/fixtures/espn/teams_<lg>.json`` (provenance in
that directory's README) served through the real ``dl_utils.download`` (only
``requests.Session.get`` is replaced); expected values are read from the payload itself.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest
import requests

from sportsdataverse import _espn_teams
from sportsdataverse.errors import AssetFetchError, NoDataError
from tests.codegen.test_runtime_errors import _route
from tests.espn_family_offline import _documented

ESPN = Path(__file__).parent / "fixtures" / "espn"
LEAGUES = ["mbb", "wbb", "nba", "wnba", "nfl", "nhl", "cfb", "mlb"]
_404 = (ESPN / "error_404_body.json").read_text(encoding="utf-8")


def _fn(league: str):
    return getattr(importlib.import_module(f"sportsdataverse.{league}.{league}_teams"), f"espn_{league}_teams")


def _body(league: str) -> Dict[str, Any]:
    return json.loads((ESPN / f"teams_{league}.json").read_text(encoding="utf-8"))


def _serve(monkeypatch, body: Any, status: int = 200):
    return _route(
        monkeypatch, lambda url: (status, json.dumps(body) if not isinstance(body, str) else body, "application/json")
    )


# --------------------------------------------------------------------------------------
# B-M23: no lru_cache -- kwargs work, each call is a fresh frame and a fresh fetch
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("league", LEAGUES)
def test_teams_kwargs(league, monkeypatch):
    """``headers={...}`` reached the lru_cache key and raised ``TypeError: unhashable type: 'dict'``."""
    body = _body(league)
    calls = _serve(monkeypatch, body)
    df = _fn(league)(headers={"User-Agent": "sdv-py-test"})
    assert df.height == len(body["sports"][0]["leagues"][0]["teams"]) > 0
    assert len(calls) == 1


@pytest.mark.parametrize("league", LEAGUES)
def test_teams_copy_on_return(league, monkeypatch):
    """A caller mutating its frame never changes what the next caller gets, and each call fetches."""
    body = _body(league)
    calls = _serve(monkeypatch, body)
    first = _fn(league)(return_as_pandas=True)
    first["team_id"] = "mutated"
    first["added_by_caller"] = 1
    second = _fn(league)(return_as_pandas=True)
    assert second is not first
    assert "added_by_caller" not in second.columns
    assert second["team_id"].tolist() == [t["team"]["id"] for t in body["sports"][0]["leagues"][0]["teams"]]
    assert len(calls) == 2  # never served from a process-lifetime cache


# --------------------------------------------------------------------------------------
# B-L1: a payload without teams is a zero-row frame with the documented columns
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload", [{}, {"sports": []}, {"sports": [{"leagues": []}]}, {"sports": [{"leagues": [{}]}]}]
)
@pytest.mark.parametrize("league", LEAGUES)
def test_teams_no_teams_is_schemad_empty(league, payload, monkeypatch):
    _serve(monkeypatch, payload)
    df = _fn(league)()
    assert df.height == 0
    assert df.columns == _documented(league, "teams")
    assert df.schema == pl.Schema(_espn_teams.TEAMS_SCHEMA)
    assert list(_fn(league)(return_as_pandas=True).columns) == df.columns


@pytest.mark.parametrize("league", LEAGUES)
def test_teams_schema_matches_a_real_capture(league, monkeypatch):
    """The empty frame's dtypes are the ones the real capture produces, so the two concat."""
    _serve(monkeypatch, _body(league))
    assert dict(_fn(league)().schema) == _espn_teams.TEAMS_SCHEMA


def test_teams_payload_not_mutated(monkeypatch):
    body = _body("nba")
    monkeypatch.setattr(_espn_teams, "_download_json", lambda *a, **k: body)  # the SAME object, twice
    assert _fn("nba")().equals(_fn("nba")())
    assert body == _body("nba")


# --------------------------------------------------------------------------------------
# error vocabulary: a FAILED fetch is never "no teams"
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("failure", [403, 429, 500, 503, "connection"])
@pytest.mark.parametrize("league", LEAGUES)
def test_teams_failed_fetch_raises(league, failure, monkeypatch):
    """403/429/5xx/connection raise AssetFetchError, never an empty frame.

    The body is neutral on purpose: an ESPN ``code: 404`` body under a non-404 status is read as
    "no data" by the 200-with-code-404 rule, which would not test the failed-fetch branch.
    """
    monkeypatch.setenv("SDV_PY_HTTP_RETRIES", "2")
    if failure == "connection":
        _route(monkeypatch, lambda url: requests.exceptions.ConnectionError("connection reset"))
    else:
        _serve(monkeypatch, json.dumps({"message": "upstream failure"}), status=failure)
    with pytest.raises(AssetFetchError):
        _fn(league)()


@pytest.mark.parametrize("league", LEAGUES)
def test_teams_404_raises_no_data(league, monkeypatch):
    _serve(monkeypatch, _404, status=404)
    with pytest.raises(NoDataError):
        _fn(league)()


# --------------------------------------------------------------------------------------
# B-L11 / one deprecation cycle: cache_clear() still exists, warns, and names its removal
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("league", LEAGUES)
def test_teams_cache_clear_shim_warns(league):
    fn = _fn(league)
    with pytest.warns(DeprecationWarning, match=rf"espn_{league}_teams\.cache_clear is deprecated.*0\.2\.0"):
        assert fn.cache_clear() is None
    assert not hasattr(fn, "clear_cache")  # the name the old docstrings gave never existed
