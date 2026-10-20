"""The NFL hand-rolled getters speak the error vocabulary (task 4.1b; A-M4, B-M21, B-M22, B-GB-03, A-L67, A-L70).

404 -> ``NoDataError``, 400 / 422 -> ``ValueError``, any other non-2xx, an empty / HTML / scalar 2xx body
or a dead connection -> ``AssetFetchError``. The answers are served below ``requests.Session.request``, so
``dl_utils.download``, the chokepoints and the getters all run for real. Success bodies are real captures:
``tests/fixtures/nfl_api/game_details_v1.json`` and ``tests/fixtures/nfl_ngs/plays_isHighlight.json``; the NGS
403 is the gateway's recorded deny body (``tests/fixtures/nfl_ngs/README.md``). pro.nfl.com rows ride the
discovered matrix in ``tests/codegen/test_runtime_errors.py``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Union

import pytest
import requests

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nfl import nfl_api_runtime, nfl_games, nfl_ngs, nflpro_runtime

FIX = Path(__file__).resolve().parents[1] / "fixtures"
GAME = "a8fc1728-4feb-11f1-abca-2c54536568a9"
DETAILS = (FIX / "nfl_api" / "game_details_v1.json").read_text(encoding="utf-8")
HIGHLIGHT = (FIX / "nfl_ngs" / "plays_isHighlight.json").read_text(encoding="utf-8")
DENY = (FIX / "nfl_ngs" / "api_gateway_deny_403.json").read_text(encoding="utf-8")

Answer = Union[tuple, Exception]


@pytest.fixture
def serve(monkeypatch) -> Callable[[Callable[[str], Answer]], List[Dict[str, Any]]]:
    """Answer every request with ``answer(url)`` -> ``(status, body)`` or an exception; return the calls."""
    import sportsdataverse.cache as cache

    monkeypatch.setattr(cache, "get_cache_mode", lambda: "off")
    monkeypatch.setattr("time.sleep", lambda *a, **k: None)
    monkeypatch.setenv("SDV_PY_HTTP_RETRIES", "1")
    monkeypatch.delenv("SDV_PY_HTTP_TIMEOUT", raising=False)
    monkeypatch.delenv("NFL_ACCESS_TOKEN", raising=False)
    monkeypatch.setattr(nfl_ngs, "_SESSION", None)
    nfl_games.nfl_clear_token_cache()

    def install(answer: Callable[[str], Answer]) -> List[Dict[str, Any]]:
        calls: List[Dict[str, Any]] = []

        def request(self, method, url, *args, **kwargs):
            url = requests.Request(method, url, params=kwargs.get("params")).prepare().url  # with the query
            calls.append({"method": method, "url": url, "timeout": kwargs.get("timeout")})
            got = answer(url)
            if isinstance(got, Exception):
                raise got
            resp = requests.Response()
            resp.status_code, body = got
            resp._content = body.encode("utf-8")
            resp.encoding = "utf-8"
            resp.headers["Content-Type"] = "text/html" if body.startswith("<") else "application/json"
            resp.url = url
            return resp

        monkeypatch.setattr(requests.Session, "request", request)
        return calls

    yield install
    nfl_games.nfl_clear_token_cache()


CASES = {
    "400": ((400, '{"errors": [{"field": "playId"}]}'), ValueError),
    "422": ((422, '{"message": "unprocessable"}'), ValueError),
    "404": ((404, ""), NoDataError),
    "403": ((403, DENY), AssetFetchError),
    "429": ((429, '{"error": "rate limited"}'), AssetFetchError),
    "503": ((503, "<html>Service Unavailable</html>"), AssetFetchError),
    "empty_200": ((200, ""), AssetFetchError),
    "html_200": ((200, "<html>Just a moment...</html>"), AssetFetchError),
    "scalar_200": ((200, "42"), AssetFetchError),
    "dead": (requests.ConnectionError("reset"), AssetFetchError),
}
GETTERS = {
    "mint": lambda: nfl_games._mint_token("k", "s"),
    "schedule": lambda: nfl_games.nfl_game_schedule(headers={}),
    "details": lambda: nfl_games.nfl_game_details(game_id=GAME, headers={}),
    "ngs": lambda: nfl_ngs.nfl_ngs_play_is_highlight(game_id=2025090708, play_id=2244),
    "nflpro": lambda: nflpro_runtime._get("https://pro.nfl.com/api/secured/stats/x", headers={}),  # + the matrix
}


@pytest.mark.parametrize("case", list(CASES))
@pytest.mark.parametrize("getter", list(GETTERS))
def test_nfl_getter_speaks_the_vocabulary(serve, getter, case):
    answer, want = CASES[case]
    calls = serve(lambda url: (200, "<html>home</html>") if url == nfl_ngs._HOME else answer)
    with pytest.raises(want):
        GETTERS[getter]()
    assert [c for c in calls if c["url"] != nfl_ngs._HOME]


def test_a_failed_mint_surfaces_through_the_nfl_api_wrapper_getter(serve):
    """B-M21: ``nfl_api_runtime._get`` mints outside its own guard; a mint 503 leaked ``HTTPError``."""
    calls = serve(lambda url: (503, '{"message": "down"}'))
    with pytest.raises(AssetFetchError, match="api.nfl.com/identity/v3/token answered HTTP 503"):
        nfl_api_runtime._get("https://api.nfl.com/football/v2/x")
    assert [c["method"].upper() for c in calls] == ["POST"]


def test_mint_returns_the_access_token_and_refuses_a_body_without_one(serve):
    serve(lambda url: (200, '{"accessToken": "eyJ.not-a-real.token", "expiresIn": 3600}'))
    assert nfl_games._mint_token("k", "s") == "eyJ.not-a-real.token"
    serve(lambda url: (200, '{"refreshToken": "r"}'))
    with pytest.raises(AssetFetchError, match="without an accessToken"):
        nfl_games._mint_token("k", "s")


def test_game_details_real_body_unwraps(serve):
    serve(lambda url: (200, DETAILS))
    detail = nfl_games.nfl_game_details(game_id=GAME, headers={})
    assert detail["id"] == GAME and len(detail["plays"]) == 5


def _envelope(**over: Any) -> str:
    body = json.loads(DETAILS)
    body.update(over)
    return json.dumps(body)


def test_game_details_graphql_errors_envelope_is_a_failed_fetch(serve):
    """B-GB-03: the real body with ``gameDetail`` nulled and an ``errors`` list unwrapped to {} -> (0, 0) pbp."""
    body = _envelope(data={"viewer": {"gameDetail": None}}, errors=[{"message": "Internal server error"}])
    serve(lambda url: (200, body))
    with pytest.raises(AssetFetchError, match="Internal server error"):
        nfl_games.nfl_game_pbp(game_id=GAME, headers={})
    with pytest.raises(AssetFetchError):  # the raw envelope is not data either
        nfl_games.nfl_game_details(game_id=GAME, headers={}, raw=True)


def test_game_details_without_game_detail_is_a_failed_fetch(serve):
    serve(lambda url: (200, _envelope(data={"viewer": {}})))
    with pytest.raises(AssetFetchError, match="without data.viewer.gameDetail"):
        nfl_games.nfl_game_details(game_id=GAME, headers={})


def test_game_details_null_game_detail_is_an_empty_game(serve):
    serve(lambda url: (200, _envelope(data={"viewer": {"gameDetail": None}})))
    assert nfl_games.nfl_game_details(game_id=GAME, headers={}) == {}


def test_ngs_real_body_parses(serve):
    serve(lambda url: (200, "<html>home</html>") if url == nfl_ngs._HOME else (200, HIGHLIGHT))
    df = nfl_ngs.nfl_ngs_play_is_highlight(game_id=2025090708, play_id=2244)
    assert df.height == 1 and df["isHighlight"].to_list() == [True]


def test_ngs_deny_is_answered_once(serve):
    """The gateway's 403 is an IAM deny: ``download``'s default would retry it."""
    calls = serve(lambda url: (200, "<html>home</html>") if url == nfl_ngs._HOME else (403, DENY))
    with pytest.raises(AssetFetchError, match="explicit deny"):
        nfl_ngs.nfl_ngs_play_is_highlight(game_id=2025090708, play_id=2244)
    assert len([c for c in calls if c["url"] != nfl_ngs._HOME]) == 1


@pytest.mark.parametrize(
    "getter",
    [
        lambda: nfl_games._mint_token("k", "s"),
        lambda: nfl_ngs.nfl_ngs_play_is_highlight(game_id=2025090708, play_id=2244),
        lambda: nfl_api_runtime._get("https://api.nfl.com/football/v2/x", headers={}),
        lambda: nflpro_runtime._get("https://pro.nfl.com/x", headers={}),
        lambda: nfl_games.nfl_game_details(game_id=GAME, headers={}),
    ],
    ids=["mint", "ngs", "nfl_api", "nflpro", "details"],
)
def test_getters_honour_sdv_py_http_timeout(serve, monkeypatch, getter):
    """A-L67: the NFL getters hard-set their timeout and ignored ``SDV_PY_HTTP_TIMEOUT``."""
    calls = serve(lambda url: (200, '{"accessToken": "t"}') if "token" in url else (200, HIGHLIGHT))
    monkeypatch.setenv("SDV_PY_HTTP_TIMEOUT", "7")
    try:
        getter()
    except AssetFetchError:  # details refuses the isHighlight body; the request was still made
        pass
    assert {c["timeout"] for c in calls if c["url"] != nfl_ngs._HOME} == {7}


def test_nflpro_auth_error_is_an_asset_fetch_error_and_still_a_runtime_error():
    assert issubclass(nflpro_runtime.NFLProAuthError, AssetFetchError)
    assert issubclass(nflpro_runtime.NFLProAuthError, RuntimeError)


def test_nflpro_string_limit_still_pages(serve):
    """A-L70: ``limit="5"`` read as None, so a full page with no ``total`` looked complete."""
    rows = [{"i": i} for i in range(12)]

    def answer(url: str) -> Answer:
        off = int(url.split("offset=")[1].split("&")[0]) if "offset=" in url else 0
        return (200, json.dumps({"passers": rows[off : off + 5]}))

    serve(answer)
    body = nflpro_runtime._get("https://pro.nfl.com/x", {"limit": "5"}, headers={})
    assert len(body["passers"]) == 12


def test_an_undecodable_nflpro_token_is_logged_not_silent(caplog):
    """A-L70: ``except Exception: return False`` made a token that never decodes re-log-in silently."""
    secret = "not-a-jwt-secret"
    with caplog.at_level(logging.WARNING, logger=nflpro_runtime.__name__):
        assert nflpro_runtime._fresh(secret) is False
        assert nflpro_runtime._entitled(secret) is False
    assert [r.getMessage() for r in caplog.records] == [
        "NFL Pro token does not decode (IndexError); treating it as unusable"
    ] * 2
    assert secret not in caplog.text


def test_an_unexpected_error_in_the_token_check_is_not_swallowed(monkeypatch):
    def boom(token: str) -> dict:
        raise RuntimeError("bug")

    monkeypatch.setattr(nflpro_runtime, "_claims", boom)
    with pytest.raises(RuntimeError, match="bug"):
        nflpro_runtime._fresh("x.y.z")
