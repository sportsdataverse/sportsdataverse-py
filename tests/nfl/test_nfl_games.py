"""Offline tests for the ``/experience/v1/gamedetails`` wrappers in ``nfl_games``.

The fixture is a trimmed real body (first 5 plays, 1 drive, 1 scoring summary) of
CLE @ JAX, 2026 REG week 1, Shield id ``a8fc1728-4feb-11f1-abca-2c54536568a9``,
captured anonymously on 2026-09-17. The live v1 body nests the game under
``data.viewer.gameDetail``; the OpenAPI note "everything under ``data``" means the
outer envelope, not that ``data`` is the game itself.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sportsdataverse.nfl import nfl_games

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_api" / "game_details_v1.json"
GAME = "a8fc1728-4feb-11f1-abca-2c54536568a9"


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


@pytest.fixture
def seen_urls(monkeypatch) -> list:
    payload = json.loads(FIX.read_text(encoding="utf-8"))
    seen: list = []

    def fake_get(url, headers=None, timeout=None, **kwargs):
        seen.append(url)
        return _Resp(payload)

    monkeypatch.setattr(nfl_games.requests, "get", fake_get)
    return seen


def test_game_details_unwraps_viewer_game_detail(seen_urls):
    detail = nfl_games.nfl_game_details(game_id=GAME, headers={})
    assert seen_urls == [f"{nfl_games.API_HOST}/experience/v1/gamedetails/{GAME}"]
    assert detail["id"] == GAME
    assert [p["playId"] for p in detail["plays"]] == [1, 40, 63, 88, 110]
    assert detail["homeTeam"]["abbreviation"] == "JAX"


def test_game_details_raw_returns_envelope(seen_urls):
    raw = nfl_games.nfl_game_details(game_id=GAME, headers={}, raw=True)
    assert list(raw) == ["data"]
    assert list(raw["data"]) == ["viewer"]


def test_game_pbp_one_row_per_play_with_game_context(seen_urls):
    df = nfl_games.nfl_game_pbp(game_id=GAME, headers={})
    assert df.height == 5
    assert df.columns[:3] == ["game_id", "home_team", "visitor_team"]
    assert df["game_id"].unique().to_list() == [GAME]
    assert (df["home_team"].unique().to_list(), df["visitor_team"].unique().to_list()) == (["JAX"], ["CLE"])
    assert df["playType"].to_list() == ["GAME_START", "KICK_OFF", "PASS", "RUSH", "PASS"]
