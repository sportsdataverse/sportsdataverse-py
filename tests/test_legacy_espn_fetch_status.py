"""The hand-written ESPN scrapers classify a failed fetch instead of parsing it as data.

Each family routes its ``download()`` through ``_codegen_runtime._download_json``
(CLAUDE.md "Error vocabulary"). Driven through the REAL ``dl_utils.download`` (retry
loop, ESPN ``code: 404`` probe) with only ``requests.Session.get`` replaced, serving the
real ESPN bodies under ``tests/fixtures/espn/`` and ``tests/fixtures/runtime_errors/``.
Short synthetic bodies stand in where no capture exists (a 403/429/5xx error body, the
core-v2 roster / plays / athletes pages).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from tests.codegen.test_runtime_errors import _body, _route, _serve

ESPN = Path(__file__).resolve().parent / "fixtures" / "espn"


def _espn(name: str) -> str:
    return (ESPN / name).read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _two_retries(monkeypatch):
    # nfl_players' page walk takes no kwargs, so cap the budget for every family the same way
    monkeypatch.setenv("SDV_PY_HTTP_RETRIES", "2")


def _schedule() -> Any:
    from sportsdataverse.nba.nba_schedule import espn_nba_schedule

    return espn_nba_schedule(dates=20240115)


def _teams() -> Any:
    from sportsdataverse.nhl.nhl_teams import espn_nhl_teams

    return espn_nhl_teams()


def _pbp() -> Any:
    from sportsdataverse.wbb.wbb_pbp import espn_wbb_pbp

    return espn_wbb_pbp(401528028, raw=True)


def _game_rosters() -> Any:
    from sportsdataverse.nfl.nfl_game_rosters import espn_nfl_game_rosters

    return espn_nfl_game_rosters(401220403)


def _play_participants() -> Any:
    from sportsdataverse.cfb.cfb_play_participants import espn_cfb_play_participants

    return espn_cfb_play_participants(401628334)


def _nfl_players() -> Any:
    from sportsdataverse.nfl.nfl_players import build_nfl_players

    return build_nfl_players()


FAMILIES: Dict[str, Callable[[], Any]] = {
    "schedule": _schedule,
    "teams": _teams,
    "pbp": _pbp,
    "game_rosters": _game_rosters,
    "play_participants": _play_participants,
    "nfl_players": _nfl_players,
}


# --------------------------------------------------------------------------------------
# a failed fetch raises in every family -- before, the error body reached the parser
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("status", [403, 429, 500, 503])
def test_retryable_status_after_retries_raises(monkeypatch, family, status):
    # A JSON error body parses cleanly, which is exactly how it used to pass as data.
    calls = _serve(monkeypatch, status, '{"code": %d, "message": "rate limited"}' % status)
    with pytest.raises(AssetFetchError, match=f"answered HTTP {status}"):
        FAMILIES[family]()
    assert len(calls) == 3  # retried twice, then surfaced -- one request, never a second


@pytest.mark.parametrize("family", FAMILIES)
def test_empty_200_raises(monkeypatch, family):
    _serve(monkeypatch, 200, "")
    with pytest.raises(AssetFetchError, match="answered HTTP 200 with an empty body"):
        FAMILIES[family]()


@pytest.mark.parametrize("family", FAMILIES)
def test_non_json_200_raises_without_the_decode_error(monkeypatch, family):
    _serve(monkeypatch, 200, "<html><body>Access Denied</body></html>", "text/html")
    with pytest.raises(AssetFetchError, match="answered HTTP 200 with a non-JSON body") as ei:
        FAMILIES[family]()
    assert ei.value.__context__ is None  # a JSONDecodeError's .doc is the whole body


@pytest.mark.parametrize("family", FAMILIES)
def test_rejected_request_is_a_value_error(monkeypatch, family):
    calls = _serve(monkeypatch, 400, _body("espn_site_roster_400.json"))
    with pytest.raises(ValueError, match="rejected the request: HTTP 400"):
        FAMILIES[family]()
    assert len(calls) == 1


@pytest.mark.parametrize("family", FAMILIES)
def test_404_stays_no_data(monkeypatch, family):
    _serve(monkeypatch, 404, _body("espn_core_event_404.json"))
    with pytest.raises(NoDataError):
        FAMILIES[family]()


def test_connection_failure_after_retries_raises(monkeypatch):
    import requests

    calls = _route(monkeypatch, lambda url: requests.ConnectionError("reset"))
    with pytest.raises(AssetFetchError, match="fetch failed after retries: ConnectionError"):
        _schedule()
    assert len(calls) == 3


# --------------------------------------------------------------------------------------
# a 200 JSON body still parses
# --------------------------------------------------------------------------------------


def test_200_schedule_parses_the_real_scoreboard(monkeypatch):
    _serve(monkeypatch, 200, _espn("scoreboard_nba.json"))
    df = _schedule()
    assert df.height == len(json.loads(_espn("scoreboard_nba.json"))["events"]) > 0


def test_200_teams_parses_the_real_teams_page(monkeypatch):
    _serve(monkeypatch, 200, _espn("teams_nhl.json"))
    assert _teams().height > 0


def test_200_pbp_returns_the_real_summary(monkeypatch):
    _serve(monkeypatch, 200, _espn("summary_wbb.json"))
    assert set(_pbp()) >= {"boxscore", "header"}


def test_200_play_participants_follows_page_count(monkeypatch):
    from sportsdataverse.football.play_participants import _download_plays

    def answer(url: str) -> Any:
        page = 2 if "page=2" in url else 1
        body = {"items": [{"id": f"p{page}"}], "pageCount": 2, "pageIndex": page}
        return (200, json.dumps(body), "application/json")

    calls = _route(monkeypatch, answer)
    assert [p["id"] for p in _download_plays(401628334)] == ["p1", "p2"]
    assert len(calls) == 2


# --------------------------------------------------------------------------------------
# loops: which ones propagate, which skip and tally
# --------------------------------------------------------------------------------------


def _athletes_router(fail: set) -> Callable[[str], Any]:
    base = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes"

    def answer(url: str) -> Any:
        if "page=" in url:
            refs = [{"$ref": f"{base}/{i}"} for i in (1, 2, 3)]
            return (200, json.dumps({"items": refs, "pageCount": 1}), "application/json")
        aid = int(url.rsplit("/", 1)[-1])
        if aid in fail:
            return (503, '{"message": "Service Unavailable"}', "application/json")
        return (200, json.dumps({"id": str(aid), "fullName": f"Player {aid}"}), "application/json")

    return answer


def test_nfl_players_skips_and_logs_an_isolated_athlete_failure(monkeypatch, caplog):
    from sportsdataverse.nfl.nfl_players import _fetch_athletes

    _route(monkeypatch, _athletes_router(fail={2}))
    with caplog.at_level("WARNING"):
        got = _fetch_athletes()
    assert sorted(a["id"] for a in got) == ["1", "3"]
    assert "espn_nfl_athlete: skipped 1 of 3 items" in caplog.text


def test_nfl_players_raises_when_every_athlete_failed(monkeypatch):
    from sportsdataverse._crosswalk_basketball_sources import CrosswalkSourceError
    from sportsdataverse.nfl.nfl_players import _fetch_athletes

    _route(monkeypatch, _athletes_router(fail={1, 2, 3}))
    with pytest.raises(CrosswalkSourceError, match="all 3 per-item fetches failed") as ei:
        _fetch_athletes()
    assert isinstance(ei.value.__cause__, AssetFetchError)


def test_game_rosters_404_team_roster_is_still_skipped(monkeypatch):
    # the pre-existing 404 skip survives: only a FAILED fetch now raises
    from sportsdataverse.nfl.nfl_game_rosters import helper_nfl_roster_items

    summary_url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/1/competitions/1/competitors"

    def answer(url: str) -> Any:
        if "/1/roster" in url:
            return (404, _body("espn_core_event_404.json"), "application/json")
        return (200, json.dumps({"entries": [{"playerId": 7, "athlete": {"$ref": "x"}}]}), "application/json")

    _route(monkeypatch, answer)
    out = helper_nfl_roster_items(items=pl.DataFrame({"team_id": [1, 2]}), summary_url=summary_url)
    assert out["team_id"].to_list() == [2]


def test_game_rosters_failed_team_roster_propagates(monkeypatch):
    from sportsdataverse.nfl.nfl_game_rosters import helper_nfl_roster_items

    _serve(monkeypatch, 503, '{"message": "Service Unavailable"}')
    with pytest.raises(AssetFetchError):
        helper_nfl_roster_items(items=pl.DataFrame({"team_id": [1, 2]}), summary_url="https://x.test/c")


def test_best_effort_sidecar_degrades_on_a_failed_fetch(monkeypatch):
    # the cdn sidecar was always best-effort; a 503 now degrades instead of being read as a hash
    from sportsdataverse.football.play_participants import _download_athlete_lookup

    _serve(monkeypatch, 503, '{"__gamepackage__": {"playerHash": {"9": {"json": {"athlete": {"displayName": "X"}}}}}}')
    assert _download_athlete_lookup(401628334) == {}


# --------------------------------------------------------------------------------------
# model download-on-demand: an error body never lands in the cache
# --------------------------------------------------------------------------------------


def test_failed_model_download_is_not_cached(monkeypatch, tmp_path):
    from sportsdataverse.nfl import ep_wp

    monkeypatch.setattr(ep_wp, "_model_cache_dir", lambda: tmp_path)
    _serve(monkeypatch, 503, "<html>Service Unavailable</html>", "text/html")
    with pytest.raises(FileNotFoundError, match="answered HTTP 503"):
        ep_wp._load_model("xyac_model.ubj")
    # before: the 503 page was written as the model, so every later load failed on it
    assert list(tmp_path.iterdir()) == []
