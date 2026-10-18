"""Offline contract of the 7 Core v2 ``espn_<lg>_game_rosters`` chains + mlb (task 2.10).

Every payload is a real capture (``tests/fixtures/espn_game_rosters/README.md``; nba reuses
``tests/fixtures/espn``) served through the real ``dl_utils.download`` (only
``requests.Session.get`` is replaced); expected values are read from the payload itself.
"""

from __future__ import annotations

import copy
import importlib
import json
import re
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest

from sportsdataverse.errors import NoDataError
from tests.codegen.test_runtime_errors import _route

FIX = Path(__file__).parent / "fixtures"
LEAGUES = ["mbb", "wbb", "nba", "wnba", "nfl", "nhl", "cfb"]
_404 = (FIX / "espn" / "error_404_body.json").read_text(encoding="utf-8")


def _payload(kind: str, league: str) -> Dict[str, Any]:
    path = FIX / "espn" / f"{kind}_nba.json" if league == "nba" else FIX / "espn_game_rosters" / f"{kind}_{league}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _mod(league: str):
    return importlib.import_module(f"sportsdataverse.{league}.{league}_game_rosters")


def _serve(monkeypatch, routes: Dict[str, Any]):
    """Answer each URL whose path (query dropped) is a key of ``routes``; every other URL is ESPN's real 404."""

    def answer(url: str):
        body = routes.get(url.split("?", 1)[0].replace("https://", "http://"))
        return (200, json.dumps(body), "application/json") if body is not None else (404, _404, "application/json")

    return _route(monkeypatch, answer)


def _key(href: str) -> str:
    return href.split("?", 1)[0].replace("https://", "http://")


# --------------------------------------------------------------------------------------
# athletes: position kept (B-M26), one 404 athlete dropped alone (B-L5)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_keep_position(league, monkeypatch):
    athlete = _payload("athlete_core", league)
    _serve(monkeypatch, {_key(athlete["$ref"]): athlete})
    helper = getattr(_mod(league), f"helper_{league}_athlete_items")
    df = helper(teams_rosters=pl.DataFrame({"athlete_href": [athlete["$ref"]]}))
    pos = athlete["position"]
    assert df["position_id"].to_list() == [int(pos["id"])] and df.schema["position_id"] == pl.Int32
    assert df["position_name"].to_list() == [pos["name"]]
    assert df["position_display_name"].to_list() == [pos["displayName"]]
    assert df["position_abbreviation"].to_list() == [pos["abbreviation"]]
    assert df["position_leaf"].to_list() == [pos["leaf"]]
    assert "position_href" not in df.columns  # only the position's own $ref is dropped (hoopR / wehoop)
    if "parent" in pos:
        assert df["position_parent_href"].to_list() == [pos["parent"]["$ref"]]


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_athlete_404_drops_only_that_athlete(league, monkeypatch):
    athlete = _payload("athlete_core", league)
    missing = re.sub(r"/athletes/\d+", "/athletes/1", athlete["$ref"])
    _serve(monkeypatch, {_key(athlete["$ref"]): athlete})
    helper = getattr(_mod(league), f"helper_{league}_athlete_items")
    with pytest.warns(UserWarning, match="no athlete resource"):
        df = helper(teams_rosters=pl.DataFrame({"athlete_href": [missing, athlete["$ref"]]}))
    assert df["athlete_id"].to_list() == [int(athlete["id"])]
    with pytest.warns(UserWarning, match="no athlete resource"), pytest.raises(NoDataError):
        helper(teams_rosters=pl.DataFrame({"athlete_href": [missing]}))


# --------------------------------------------------------------------------------------
# raw= honoured (B-GB-06); mlb's empty competitors (B-L25)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_raw_payload(league, monkeypatch):
    body = _payload("event_competitors", league)
    game_id = int(re.search(r"/events/(\d+)/", body["items"][0]["$ref"]).group(1))
    url = re.sub(r"/competitors/.*", "/competitors", body["items"][0]["$ref"])
    calls = _serve(monkeypatch, {_key(url): body})
    out = getattr(_mod(league), f"espn_{league}_game_rosters")(game_id=game_id, raw=True)
    assert out == body
    assert len(calls) == 1  # the roster, team and athlete $refs are not followed


def test_mlb_game_rosters_empty_competitors_is_a_schemad_frame(monkeypatch):
    body = _payload("event_competitors", "mlb")
    game_id = int(re.search(r"/events/(\d+)/", body["items"][0]["$ref"]).group(1))
    url = re.sub(r"/competitors/.*", "/competitors", body["items"][0]["$ref"])
    _serve(monkeypatch, {_key(url): {**body, "items": [], "count": 0}})
    mod = _mod("mlb")
    df = mod.espn_mlb_game_rosters(game_id=game_id)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert df.columns == list(mod._COLUMNS)
    assert df.schema["athlete_starter"] == pl.Boolean
    assert mod.espn_mlb_game_rosters(game_id=game_id, return_as_pandas=True).columns.tolist() == list(mod._COLUMNS)


# --------------------------------------------------------------------------------------
# teams: one pop list (B-GB-32), logo block (B-GB-19)
# --------------------------------------------------------------------------------------


def _team_frame(monkeypatch, league: str, team: Dict[str, Any]) -> pl.DataFrame:
    href = team["$ref"]
    _serve(monkeypatch, {_key(href): team})
    return getattr(_mod(league), f"helper_{league}_team_items")(items=pl.DataFrame({"team_href": [href]}))


def test_game_rosters_college_team_frame_is_the_canonical_team_columns(monkeypatch):
    team = _payload("team_core", "mbb")
    assert {"awards", "coaches"} <= set(team)  # the college keys B-GB-32 found leaking
    df = _team_frame(monkeypatch, "mbb", team)
    assert not [c for c in df.columns if c.endswith("_href") and not c.startswith("logo")], df.columns
    assert df["team_id"].to_list() == [int(team["id"])]


@pytest.mark.parametrize("league", ["nba", "mbb"])
def test_game_rosters_team_with_one_logo_keeps_it(league, monkeypatch):
    team = copy.deepcopy(_payload("team_core", league))
    team["logos"] = team["logos"][:1]
    df = _team_frame(monkeypatch, league, team)
    assert df["logo_href"].to_list() == [team["logos"][0]["href"]]
    assert df["logo_dark_href"].to_list() == [""]


def test_game_rosters_unreadable_logos_warn(monkeypatch):
    team = copy.deepcopy(_payload("team_core", "nba"))
    for logo in team["logos"]:
        logo.pop("href")
    with pytest.warns(UserWarning, match="logos have a shape"):
        df = _team_frame(monkeypatch, "nba", team)
    assert df["logo_href"].to_list() == [""] and df["logo_dark_href"].to_list() == [""]
