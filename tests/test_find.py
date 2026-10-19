"""``sportsdataverse.find`` resolvers under the error vocabulary (C-M16, C-L48).

The payloads are the committed real ESPN Site v2 captures (``tests/fixtures/espn/teams_nba.json``: the 30
NBA teams; ``team_roster_nba.json``: ``teams/13/roster``, the Lakers, LeBron James = 1966; provenance in
that directory's README). Only the transport is stubbed: ``_codegen_runtime.download`` answers each URL
with the capture or with a status, so every call runs the real wrapper -> ``_get`` -> vocabulary path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import sportsdataverse._codegen_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.find import clear_team_cache, find_athlete, find_event

ESPN = Path(__file__).resolve().parent / "fixtures" / "espn"
TEAMS = json.loads((ESPN / "teams_nba.json").read_text(encoding="utf-8"))
LAKERS_ROSTER = json.loads((ESPN / "team_roster_nba.json").read_text(encoding="utf-8"))


class _Resp:
    def __init__(self, status_code: int, body: object = None):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body) if body is not None else "Forbidden"

    def json(self):
        if self._body is None:
            raise ValueError("not json")
        return self._body


@pytest.fixture
def espn(monkeypatch):
    """Serve the real captures; ``status[team_id]`` overrides one roster's answer."""
    status: dict[str, int] = {}

    def fake_download(url, params=None, **kw):
        if "/roster" in url:
            team_id = url.rsplit("/teams/", 1)[1].split("/", 1)[0]
            code = status.get(team_id, 200 if team_id == "13" else 404)
            if code == 404:
                raise NoDataError(f"{url} answered HTTP 404")  # what download() raises on a 404
            return _Resp(code, LAKERS_ROSTER if code == 200 else None)
        if url.rstrip("/").endswith("/teams"):
            return _Resp(200, TEAMS)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(rt, "download", fake_download)
    clear_team_cache()
    yield status
    clear_team_cache()


def test_find_athlete_resolves_on_the_real_roster(espn):
    assert find_athlete("lebron", league="nba", team="lakers")["id"] == "1966"
    # the sweep skips the 29 rosters that answer 404 and still finds him
    assert find_athlete("lebron", league="nba")["id"] == "1966"


@pytest.mark.parametrize("team", ["lakers", None], ids=["team", "sweep"])
def test_find_athlete_forbidden_roster_raises(espn, team):
    """A 403 is an unknown answer, never "no such player" (the original returned None / [])."""
    espn["13"] = 403
    with pytest.raises(AssetFetchError, match="403"):
        find_athlete("lebron", league="nba", team=team)


@pytest.mark.parametrize("call", [find_athlete, find_event], ids=["athlete", "event"])
def test_unknown_league_is_a_value_error(call):
    with pytest.raises(ValueError, match="Unknown league 'xfl'"):
        call("x", league="xfl")
