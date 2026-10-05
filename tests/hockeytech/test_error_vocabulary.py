"""Every HockeyTech caller keeps the error vocabulary (PY-4).

``hockeytech_api`` raises ``AssetFetchError`` for a failed fetch and
``NoDataError`` for a 404 (pinned in ``test_client.py``). These tests check that
no caller turns either back into an empty frame: the 13 family callables in all
20 leagues, the PWHL surface, the season helpers, and the analytics. The one
graceful empty (MJHL's access-denied ``gc`` reply) is driven end to end.
"""

from __future__ import annotations

import importlib
import pathlib

import pytest
import requests

from sportsdataverse.errors import AssetFetchError, NoDataError
from tests.conftest import load_fixture

LEAGUES = [
    "ahl", "ohl", "whl", "qmjhl", "echl", "sphl", "chl", "ushl", "bchl", "ajhl",
    "sjhl", "ojhl", "cchl", "gojhl", "mhl", "nojhl", "vijhl", "kijhl", "mjhl",
]  # fmt: skip

# (stem, args): every callable build_family() mints, called with no season so the
# season lookup goes through the family's own (patched) hockeytech_api.
FAMILY_CALLS = [
    ("season_id", ()),
    ("schedule", ()),
    ("pbp", (1,)),
    ("standings", ()),
    ("teams", ()),
    ("team_roster", (1,)),
    ("player_stats", (1,)),
    ("leaders", ()),
    ("game_summary", (1,)),
    ("game_shifts", (1,)),
    ("player_toi", (1,)),
    ("game_corsi", (1,)),
]

PWHL_CALLS = [
    ("pwhl_api", "pwhl_season_id", ()),
    ("pwhl_api", "most_recent_pwhl_season", ()),
    ("pwhl_api", "pwhl_schedule", ()),
    ("pwhl_api", "pwhl_scorebar", ()),
    ("pwhl_api", "pwhl_pbp", (1,)),
    ("pwhl_api", "pwhl_standings", ()),
    ("pwhl_api", "pwhl_teams", ()),
    ("pwhl_api", "pwhl_team_roster", (1,)),
    ("pwhl_api", "pwhl_player_stats", (1,)),
    ("pwhl_api", "pwhl_leaders", ()),
    ("pwhl_api", "pwhl_game_summary", (1,)),
    ("pwhl_api", "pwhl_game_info", (1,)),
    ("pwhl_api", "pwhl_player_box", (1,)),
    ("pwhl_api", "pwhl_player_info", (1,)),
    ("pwhl_api", "pwhl_player_game_log", (1,)),
    ("pwhl_api", "pwhl_player_search", ("poulin",)),
    ("pwhl_api", "pwhl_stats", ()),
    ("pwhl_api", "pwhl_transactions", ()),
    ("pwhl_api", "pwhl_playoff_bracket", ()),
    ("pwhl_analytics", "pwhl_game_shifts", (1,)),
    ("pwhl_analytics", "pwhl_player_toi", (1,)),
    ("pwhl_analytics", "pwhl_game_corsi", (1,)),
]

_FIX = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "hockeytech"


def _raiser(exc):
    def fake(*_a, **_k):
        raise exc

    return fake


def _patch_all(monkeypatch, fake):
    """Point every module-level ``hockeytech_api`` reference at ``fake``."""
    from sportsdataverse.hockeytech import _client, _family
    from sportsdataverse.pwhl import pwhl_analytics, pwhl_api

    for mod in (_client, _family, pwhl_api, pwhl_analytics):
        monkeypatch.setattr(mod, "hockeytech_api", fake)


# ---------------------------------------------------------------------------
# A failed fetch reaches the caller of every public function.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("exc", [AssetFetchError("HTTP 503"), NoDataError("HTTP 404")])
@pytest.mark.parametrize("stem, args", FAMILY_CALLS)
@pytest.mark.parametrize("lg", LEAGUES)
def test_family_callable_propagates_fetch_errors(monkeypatch, lg, stem, args, exc):
    _patch_all(monkeypatch, _raiser(exc))
    fn = getattr(importlib.import_module(f"sportsdataverse.hockey.{lg}"), f"{lg}_{stem}")
    with pytest.raises(type(exc)):
        fn(*args)


@pytest.mark.parametrize("lg", LEAGUES)
def test_family_most_recent_propagates_fetch_errors(monkeypatch, lg):
    _patch_all(monkeypatch, _raiser(AssetFetchError("HTTP 503")))
    with pytest.raises(AssetFetchError):
        getattr(importlib.import_module(f"sportsdataverse.hockey.{lg}"), f"most_recent_{lg}_season")()


@pytest.mark.parametrize("exc", [AssetFetchError("HTTP 503"), NoDataError("HTTP 404")])
@pytest.mark.parametrize("module, name, args", PWHL_CALLS)
def test_pwhl_callable_propagates_fetch_errors(monkeypatch, module, name, args, exc):
    _patch_all(monkeypatch, _raiser(exc))
    fn = getattr(importlib.import_module(f"sportsdataverse.pwhl.{module}"), name)
    with pytest.raises(type(exc)):
        fn(*args)


def test_enrich_pbp_fetch_errors_propagate(monkeypatch):
    from sportsdataverse.hockeytech import _parsers as P
    from sportsdataverse.hockeytech._analytics import enrich_pbp

    _patch_all(monkeypatch, _raiser(AssetFetchError("HTTP 503")))
    df = P.parse_pbp(load_fixture("hockeytech", "pwhl_pbp_42"), pbp_style="hockeytech_a", game_id=42)
    with pytest.raises(AssetFetchError):
        enrich_pbp(df, "pwhl", 42)  # meta fetched live
    meta = load_fixture("hockeytech", "pwhl_game_summary_42")
    with pytest.raises(AssetFetchError):
        enrich_pbp(df, "pwhl", 42, meta_payload=meta)  # shifts fetched live


def test_pwhl_streaks_makes_no_request(monkeypatch):
    """The view does not exist; the deprecated wrapper returns empty without asking."""
    from sportsdataverse.pwhl import pwhl_api

    _patch_all(monkeypatch, _raiser(AssertionError("no request expected")))
    with pytest.warns(DeprecationWarning):
        assert pwhl_api.pwhl_streaks().height == 0


# ---------------------------------------------------------------------------
# Season helpers.
# ---------------------------------------------------------------------------


def _seasons_fake(payload):
    return lambda league, feed, view, *a, **k: payload if view == "seasons" else {}


@pytest.mark.parametrize("lg", ["pwhl", "ahl", "mjhl"])
def test_most_recent_season_is_the_max_listed(monkeypatch, lg):
    """The real PWHL feed lists 2026-27, so the old hard-coded 2026 was stale."""
    _patch_all(monkeypatch, _seasons_fake(load_fixture("hockeytech", "pwhl_seasons")))
    mod = importlib.import_module("sportsdataverse.pwhl.pwhl_api" if lg == "pwhl" else f"sportsdataverse.hockey.{lg}")
    assert getattr(mod, f"most_recent_{lg}_season")() == 2027


@pytest.mark.parametrize("lg", ["pwhl", "ahl"])
def test_most_recent_season_answered_empty_is_no_data(monkeypatch, lg):
    _patch_all(monkeypatch, _seasons_fake({"SiteKit": {"Seasons": []}}))
    mod = importlib.import_module("sportsdataverse.pwhl.pwhl_api" if lg == "pwhl" else f"sportsdataverse.hockey.{lg}")
    with pytest.raises(NoDataError, match="lists no season"):
        getattr(mod, f"most_recent_{lg}_season")()


def test_resolve_season_id_reads_the_live_list(monkeypatch):
    from sportsdataverse.hockeytech._leagues import resolve_season_id

    _patch_all(monkeypatch, _seasons_fake(load_fixture("hockeytech", "pwhl_seasons")))
    assert resolve_season_id("ahl", season=2026) == 8
    assert resolve_season_id("ahl", season=2026, game_type="playoffs") == 9


@pytest.mark.parametrize("exc", [AssetFetchError("HTTP 503"), NoDataError("HTTP 404")])
def test_resolve_season_id_non_pwhl_reraises(monkeypatch, exc):
    from sportsdataverse.hockeytech._leagues import resolve_season_id

    _patch_all(monkeypatch, _raiser(exc))
    with pytest.raises(type(exc)):
        resolve_season_id("echl", season=2025)


def test_resolve_season_id_pwhl_falls_back_on_failed_fetch(monkeypatch):
    from sportsdataverse.hockeytech._leagues import resolve_season_id

    _patch_all(monkeypatch, _raiser(AssetFetchError("HTTP 503")))
    assert resolve_season_id("pwhl", season=2025) == 5
    with pytest.raises(ValueError, match="No pwhl season"):
        resolve_season_id("pwhl", season=2031)


def test_resolve_season_id_pwhl_falls_back_when_list_lacks_season(monkeypatch):
    from sportsdataverse.hockeytech._leagues import resolve_season_id

    seasons = load_fixture("hockeytech", "pwhl_seasons")
    seasons["SiteKit"]["Seasons"] = [s for s in seasons["SiteKit"]["Seasons"] if s["season_id"] != "1"]
    _patch_all(monkeypatch, _seasons_fake(seasons))
    assert resolve_season_id("pwhl", season=2024) == 1


def test_resolve_season_id_pwhl_does_not_swallow_programming_errors(monkeypatch):
    from sportsdataverse.hockeytech._leagues import resolve_season_id

    _patch_all(monkeypatch, _raiser(TypeError("bug")))
    with pytest.raises(TypeError):
        resolve_season_id("pwhl", season=2025)


# Every wrapper that defaults its season: (module, function, extra positional args).
SEASON_DEFAULTING = [("family", stem, args) for stem, args in (
    ("standings", ()), ("teams", ()), ("team_roster", (1,)), ("leaders", ()),
)] + [("pwhl_api", name, args) for name, args in (
    ("pwhl_standings", ()), ("pwhl_teams", ()), ("pwhl_team_roster", (1,)),
    ("pwhl_leaders", ()), ("pwhl_stats", ()), ("pwhl_playoff_bracket", ()),
)]  # fmt: skip


@pytest.mark.parametrize("seasons_reply", ["fails", "answered_empty"])
@pytest.mark.parametrize("module, name, args", SEASON_DEFAULTING)
def test_explicit_season_id_never_asks_for_seasons(monkeypatch, module, name, args, seasons_reply):
    """An explicit season_id is all the call needs; a dead seasons feed must not break it."""
    views = []

    def fake(league, feed, view, *a, **k):
        views.append(view)
        if view == "seasons":
            if seasons_reply == "fails":
                raise AssetFetchError("HTTP 503")
            return {"SiteKit": {"Seasons": []}}
        return {}

    _patch_all(monkeypatch, fake)
    if module == "family":
        fn = getattr(importlib.import_module("sportsdataverse.hockey.ahl"), f"ahl_{name}")
    else:
        fn = getattr(importlib.import_module(f"sportsdataverse.pwhl.{module}"), name)
    fn(*args, season_id=11)
    assert views and "seasons" not in views


# ---------------------------------------------------------------------------
# The documented graceful empty, end to end through the real client.
# ---------------------------------------------------------------------------


def _serve_by_view(monkeypatch, bodies):
    """Fake the HTTP session: answer with ``bodies[view-or-tab]`` (status 200)."""
    from urllib.parse import parse_qs, urlparse

    from sportsdataverse import dl_utils
    from sportsdataverse.hockeytech import _client

    def fake_get(url, **_k):
        q = parse_qs(urlparse(url).query)
        r = requests.Response()
        r.status_code = 200
        r._content = bodies[(q.get("view") or q.get("tab"))[0]]
        r.encoding = "utf-8"
        r.url = url
        return r

    monkeypatch.setattr(dl_utils._SHARED_SESSION, "get", fake_get)
    monkeypatch.setattr(_client.time, "sleep", lambda *_a, **_k: None)


def test_mjhl_game_summary_access_denied_is_empty_frames(monkeypatch):
    from sportsdataverse.hockey.mjhl import mjhl_game_summary

    _serve_by_view(monkeypatch, {"gamesummary": (_FIX / "mjhl_gamesummary_7301_access_denied.txt").read_bytes()})
    out = mjhl_game_summary(7301)
    assert all(out[k].height == 0 for k in ("goals", "penalties", "shots_by_period", "three_stars"))
    game = out["game"]  # the game_id stub row the parser always emits; no metadata
    assert game.height == 1 and game["game_id"][0] == 7301 and game["date"][0] is None


def test_mjhl_pbp_survives_access_denied_meta(monkeypatch):
    """PBP + shifts answer; only gamesummary is denied, and it is fetched once."""
    import json

    from sportsdataverse.hockeytech import _client
    from sportsdataverse.hockey.mjhl import mjhl_pbp

    real = _client.hockeytech_api
    seen = []

    def counting(*a, **k):
        seen.append(a[2])
        return real(*a, **k)

    _serve_by_view(
        monkeypatch,
        {
            "gameCenterPlayByPlay": ("(" + json.dumps(load_fixture("hockeytech", "ohl_pbp_27225")) + ")").encode(),
            "gamesummary": (_FIX / "mjhl_gamesummary_7301_access_denied.txt").read_bytes(),
            "gameshifts": json.dumps({"SiteKit": {"Gameshifts": {"home": [], "visitor": []}}}).encode(),
        },
    )
    from sportsdataverse.hockeytech import _family

    monkeypatch.setattr(_family, "hockeytech_api", counting)
    df = mjhl_pbp(7301)
    assert df.height > 0
    assert seen.count("gamesummary") == 1
