"""The provider getters outside the NFL family speak the error vocabulary (task 4.1b, PR 2).

Findings: C-NF-7 / B-GB-07 (idmap), B-L91 (The Odds API), A-L69 (pff_api), B-L75 and A-L67 (pff_runtime),
A-L67 (the 247Sports curl_cffi getters), B-L88 (the On3 rankings shims), B-M10 / B-GB-04 (statcast and torvik
on a cache hit). 404 -> ``NoDataError``, 400 / 422 -> ``ValueError``, any other non-2xx, an empty / HTML /
scalar 2xx body or a dead connection -> ``AssetFetchError``; a 204 / 205 is a deliberate nothing.

Answers are served below ``requests.Session.request``, so ``dl_utils.download``, the chokepoints and the getters
all run for real. Success bodies are real committed captures (``tests/fixtures/mlb_statcast/schedule.json``,
``tests/fixtures/torvik/2025_getgamestats_head.json``) or PFF's own published spec examples
(``tests/fixtures/pff_api/``); the error rows are literal status / body pairs, as in the 1.3 matrix.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Union

import pytest
import requests

from sportsdataverse.errors import AssetFetchError, NoDataError

FIX = Path(__file__).resolve().parent / "fixtures"
Answer = Union[tuple, Exception]


@pytest.fixture
def serve(monkeypatch) -> Callable[..., List[Dict[str, Any]]]:
    """Answer every request with ``answer(url)`` -> ``(status, body[, content type])`` or an exception."""
    import sportsdataverse.cache as cache

    monkeypatch.setattr(cache, "get_cache_mode", lambda: "off")
    monkeypatch.setattr("time.sleep", lambda *a, **k: None)
    monkeypatch.setenv("SDV_PY_HTTP_RETRIES", "1")
    monkeypatch.delenv("SDV_PY_HTTP_TIMEOUT", raising=False)

    def install(answer: Callable[[str], Answer]) -> List[Dict[str, Any]]:
        calls: List[Dict[str, Any]] = []

        def request(self, method, url, *args, **kwargs):
            calls.append({"url": url, "timeout": kwargs.get("timeout")})
            got = answer(url)
            if isinstance(got, Exception):
                raise got
            status, body, *ctype = got
            resp = requests.Response()
            resp.status_code = status
            resp._content = body.encode("utf-8")
            resp.encoding = "utf-8"
            resp.headers["Content-Type"] = (
                ctype[0] if ctype else ("text/html" if body.startswith("<") else "application/json")
            )
            resp.url = url
            return resp

        monkeypatch.setattr(requests.Session, "request", request)
        return calls

    return install


CASES = {
    "400": ((400, '{"message": "bad request"}'), ValueError),
    "422": ((422, '{"message": "unprocessable"}'), ValueError),
    "404": ((404, ""), NoDataError),
    "403": ((403, '{"detail": "forbidden"}'), AssetFetchError),
    "500": ((500, '{"error": "upstream failed"}'), AssetFetchError),
    "502": ((502, "<html>Bad Gateway</html>"), AssetFetchError),
    "empty_200": ((200, ""), AssetFetchError),
    "html_200": ((200, "<html>Just a moment...</html>"), AssetFetchError),
    "scalar_200": ((200, "42"), AssetFetchError),
    "dead": (requests.ConnectionError("reset"), AssetFetchError),
}


def _idmap(**kw: Any) -> Any:
    from sportsdataverse.football.sources.idmap import _fetch_idmap_row

    return _fetch_idmap_row("nfl", 401872922, base_url="https://api.example", **kw)


def _odds() -> Any:
    from sportsdataverse.odds import the_odds_api as toa

    return toa.toa_sports(return_parsed=False, api_key="test-key-0123456789")


GETTERS = {"idmap": _idmap, "odds": _odds}


@pytest.mark.parametrize("case", list(CASES))
@pytest.mark.parametrize("getter", list(GETTERS))
def test_provider_getter_speaks_the_vocabulary(serve, getter, case):
    answer, want = CASES[case]
    calls = serve(lambda url: answer)
    if getter == "idmap" and want is NoDataError:  # a 404 is "game not mapped"
        assert GETTERS[getter]() is None
    else:
        with pytest.raises(want):
            GETTERS[getter]()
    assert calls


def test_idmap_error_body_is_never_a_row(serve):
    # C-NF-7's probe: a 500 whose JSON body came back as the mapping row {'error': 'upstream failed'}.
    serve(lambda url: (500, '{"error": "upstream failed"}'))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        _idmap()


@pytest.mark.parametrize(("answer", "want"), [((200, "[]"), AssetFetchError), ((204, ""), None), ((200, "{}"), None)])
def test_idmap_non_row_bodies(serve, answer, want):
    serve(lambda url: answer)
    if want is None:
        assert _idmap() is None
    else:
        with pytest.raises(want, match="not an id-map row"):
            _idmap()


def test_idmap_lookup_falls_back_on_a_failed_fetch(serve, monkeypatch):
    # dispatch catches it and warns; before, the error body WAS the row and dispatch used it.
    from sportsdataverse.football.sources import dispatch

    monkeypatch.setenv("SDV_DATA_API_URL", "https://api.example")
    monkeypatch.delenv("SDV_IDMAP_DIR", raising=False)
    serve(lambda url: (500, '{"error": "upstream failed"}'))
    with pytest.warns(RuntimeWarning, match="AssetFetchError"):
        row, how = dispatch._resolve_idmap_row("nfl", 401872922)
    assert how != "data_api" and row is None


def test_odds_204_is_nothing(serve):
    serve(lambda url: (204, ""))
    assert _odds() == {}


# --- pff_api: an injected transport hands back the status download would have raised on (A-L69) ---------


def _pff_api(status: int, text: str) -> Any:
    from sportsdataverse.nfl import pff_api_runtime as rt

    return rt._get("https://api.pff.com/v1/teams", api_key="k", transport=lambda u, p, h: (status, text))


@pytest.mark.parametrize("status", [204, 205])
def test_pff_api_no_content_is_nothing(status):
    assert _pff_api(status, "") == {}


def test_pff_api_injected_404_is_no_data():
    with pytest.raises(NoDataError, match="HTTP 404"):
        _pff_api(404, '{"error": {"code": "not_found", "message": "no such team"}}')


def test_pff_api_any_2xx_object_is_data():
    body = (FIX / "pff_api" / "team_directory.json").read_text(encoding="utf-8")  # PFF's own spec example
    assert _pff_api(201, body) == json.loads(body)


# --- pff_runtime (LEGACY premium.pff.com): B-L75 and the A-L67 timeout ------------------------------------


def _pff_legacy(**kw: Any) -> Any:
    from sportsdataverse.nfl import pff_runtime as rt

    return rt._get("https://premium.pff.com/api/v1/leagues", {}, cookies={"_premium_key": "PK"}, **kw)


def test_pff_legacy_array_body_is_a_failed_fetch_not_an_empty_dict():
    with pytest.raises(AssetFetchError, match="JSON array, not an object"):
        _pff_legacy(transport=lambda u, p, h, c: (200, "[1, 2]"))


def test_pff_legacy_object_and_no_content():
    body = (FIX / "pff_api" / "team_directory.json").read_text(encoding="utf-8")  # /v1 serves the same reports
    assert _pff_legacy(transport=lambda u, p, h, c: (200, body)) == json.loads(body)
    assert _pff_legacy(transport=lambda u, p, h, c: (204, "")) == {}


@pytest.mark.parametrize(("env", "want"), [(None, 30), ("7", 7)])
def test_pff_legacy_honours_sdv_py_http_timeout(serve, monkeypatch, env, want):
    calls = serve(lambda url: (200, '{"leagues": []}'))
    if env is not None:
        monkeypatch.setenv("SDV_PY_HTTP_TIMEOUT", env)
    _pff_legacy()
    assert calls[-1]["timeout"] == want


# --- 247Sports curl_cffi getters: SDV_PY_HTTP_TIMEOUT (A-L67) ----------------------------------------------


@pytest.mark.parametrize("mod", ["sports247_runtime", "sports247_site_pages_runtime"])
@pytest.mark.parametrize(("env", "want"), [(None, 30), ("7", 7)])
def test_247_curl_transport_honours_sdv_py_http_timeout(monkeypatch, mod, env, want):
    import importlib

    curl = pytest.importorskip("curl_cffi.requests")
    seen: Dict[str, Any] = {}

    class _R:
        status_code, text = 200, "{}"

    def get(url, **kwargs):
        seen.update(kwargs)
        return _R()

    monkeypatch.setattr(curl, "get", get)
    monkeypatch.delenv("SDV_PY_HTTP_TIMEOUT", raising=False)
    if env is not None:
        monkeypatch.setenv("SDV_PY_HTTP_TIMEOUT", env)
    importlib.import_module(f"sportsdataverse.cfb.{mod}")._curl_transport("https://x.test/", {}, {}, None)
    assert seen["timeout"] == want


@pytest.mark.parametrize(("env", "want"), [(None, 30), ("7", 7)])
def test_247_guest_jwt_mint_honours_sdv_py_http_timeout(monkeypatch, env, want):
    from sportsdataverse.cfb import sports247_runtime

    curl = pytest.importorskip("curl_cffi.requests")
    seen: Dict[str, Any] = {}

    class _Session:
        cookies = {"JWT": "guest.jwt.placeholder"}

        def get(self, url, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(curl, "Session", _Session)
    monkeypatch.delenv("SDV_PY_HTTP_TIMEOUT", raising=False)
    if env is not None:
        monkeypatch.setenv("SDV_PY_HTTP_TIMEOUT", env)
    assert sports247_runtime._mint_guest_jwt() == "guest.jwt.placeholder"
    assert seen["timeout"] == want


# --- On3 deprecated rankings shims: a malformed year / sport_slug is a wrong request (B-L88) ---------------


@pytest.mark.parametrize(("year", "sport"), [("26", "football"), (2026, "Foot Ball"), ("2026x", "football")])
def test_on3_shim_malformed_args_raise_value_error_not_empty(year, sport):
    from sportsdataverse.cfb import on3_rankings

    with pytest.warns(DeprecationWarning), pytest.raises(ValueError, match="not an On3 rankings route"):
        on3_rankings.on3_player_rankings(year, sport, return_parsed=False)


# --- statcast / torvik: the second (cached) call returns the shape the first did (B-M10, B-GB-04) ----------


@pytest.mark.parametrize(
    ("mod", "url", "fixture"),
    [
        (
            "sportsdataverse.mlb.mlb_statcast_runtime",
            "https://baseballsavant.mlb.com/schedule",
            "mlb_statcast/schedule.json",
        ),
        (
            "sportsdataverse.mbb.torvik_runtime",
            "https://barttorvik.com/2025_getgamestats.json",
            "torvik/2025_getgamestats_head.json",
        ),
    ],
)
def test_content_type_getters_return_json_on_a_cache_hit(serve, monkeypatch, mod, url, fixture):
    import importlib

    import sportsdataverse.cache as cache

    body = (FIX / fixture).read_text(encoding="utf-8")
    calls = serve(lambda u: (200, body, "application/json"))
    monkeypatch.setattr(cache, "get_cache_mode", lambda: "memory")
    cache.set_cache_mode("memory")
    cache.clear_cache()
    try:
        get = importlib.import_module(mod)._get
        first = get(url, cache_ttl=3600)
        second = get(url, cache_ttl=3600)
    finally:
        cache.clear_cache()
        cache.set_cache_mode("off")
    assert len(calls) == 1, "the second call must be a cache hit"
    assert first == second == json.loads(body)
