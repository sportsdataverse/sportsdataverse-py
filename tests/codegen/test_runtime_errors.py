"""A failed fetch raises; it never reaches a parser as data (CLAUDE.md "Error vocabulary").

Every getter here is driven through the REAL ``dl_utils.download`` (retry loop, ESPN
``code: 404`` probe) with only ``requests.Session.get`` replaced, serving the real error
bodies in ``tests/fixtures/runtime_errors/`` where one could be captured.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, List, Tuple

import pytest
import requests

from sportsdataverse._codegen_runtime import _get
from sportsdataverse.errors import AssetFetchError, NoDataError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "runtime_errors"
FOX_KEY = "jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq"


def _body(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def _serve(monkeypatch, status: int, body: str = "", ctype: str = "application/json") -> List[str]:
    """Answer every ``requests.Session.get`` with ``status``/``body``; return the URLs hit."""
    import sportsdataverse.cache as _cache

    calls: List[str] = []

    def get(self, url, params=None, **kwargs):
        resp = requests.Response()
        resp.status_code = status
        resp._content = body.encode("utf-8")
        resp.encoding = "utf-8"
        resp.headers["Content-Type"] = ctype
        resp.url = requests.Request("GET", url, params=params).prepare().url
        calls.append(resp.url)
        return resp

    monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
    monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)
    monkeypatch.setattr(requests.Session, "get", get)
    return calls


# --------------------------------------------------------------------------------------
# the shared getter, through generated wrappers of three families
# --------------------------------------------------------------------------------------


def test_2xx_json_is_returned_unchanged(monkeypatch):
    _serve(monkeypatch, 200, '{"team": {"id": "13"}, "athletes": []}')
    from sportsdataverse.nba import espn_nba_team_roster

    assert espn_nba_team_roster(team_id=13, return_parsed=False) == {"team": {"id": "13"}, "athletes": []}


def test_2xx_json_array_is_returned_unchanged(monkeypatch):
    _serve(monkeypatch, 200, "[1, 2]")
    assert _get("https://example.test/x") == [1, 2]


@pytest.mark.parametrize("status", [200, 204])
def test_empty_2xx_is_a_deliberate_nothing(monkeypatch, status):
    _serve(monkeypatch, status, "")
    assert _get("https://example.test/x") == {}


@pytest.mark.parametrize(
    ("fixture", "ctype", "call"),
    [
        ("cbs_napi_404.json", "application/json", lambda: _get("https://api.cbssports.com/napi/resource/nope")),
        ("nhl_api_web_404.html", "text/html", lambda: __import__("sportsdataverse.nhl", fromlist=["x"]).nhl_web_pbp(1)),
        ("espn_core_event_404.json", "application/json", lambda: _get("https://sports.core.api.espn.com/v2/x")),
    ],
)
def test_404_is_no_data(monkeypatch, fixture, ctype, call):
    calls = _serve(monkeypatch, 404, _body(fixture), ctype)
    with pytest.raises(NoDataError):
        call()
    assert len(calls) == 1  # a definitive answer: never retried


def test_espn_200_with_code_404_body_is_no_data(monkeypatch):
    _serve(monkeypatch, 200, _body("espn_site_summary_404.json"))
    from sportsdataverse.nfl import espn_nfl_summary

    with pytest.raises(NoDataError):
        espn_nfl_summary(event_id=1)


def test_fox_401_error_body_is_a_failed_fetch(monkeypatch):
    # Before: {"fault": {"faultstring": "Invalid ApiKey", ...}} came back as the payload.
    calls = _serve(monkeypatch, 401, _body("fox_bifrost_401_invalid_apikey.json"))
    from sportsdataverse.fox import fox_api_scoreboard

    with pytest.raises(AssetFetchError) as ei:
        fox_api_scoreboard(sport="nfl", return_parsed=False)
    msg = str(ei.value)
    assert "api.foxsports.com/bifrost/v1/nfl/scoreboard/main" in msg
    assert "HTTP 401" in msg and "Invalid ApiKey" in msg
    assert FOX_KEY not in msg
    assert len(calls) == 1  # 401 is not retried
    assert FOX_KEY in calls[0]  # ...and the key really was on the request


def test_espn_400_error_body_is_a_failed_fetch(monkeypatch):
    _serve(monkeypatch, 400, _body("espn_site_roster_400.json"))
    from sportsdataverse.nba import espn_nba_team_roster

    with pytest.raises(AssetFetchError, match=r"site\.api\.espn\.com/.*/teams/99999/roster answered HTTP 400"):
        espn_nba_team_roster(team_id=99999)


@pytest.mark.parametrize("status", [403, 429, 503])
def test_retryable_status_after_retries_is_a_failed_fetch(monkeypatch, status):
    calls = _serve(monkeypatch, status, '{"error": "rate limited", "token": "s3cr3t-value-123"}')
    from sportsdataverse.fox import fox_api_scoreboard

    with pytest.raises(AssetFetchError, match=f"HTTP {status}") as ei:
        fox_api_scoreboard(sport="nfl", num_retries=2)  # parsed path: no empty frame either
    assert len(calls) == 3  # retried, then surfaced
    assert "s3cr3t-value-123" not in str(ei.value)


def test_non_json_2xx_is_a_failed_fetch_without_the_decode_error(monkeypatch):
    _serve(monkeypatch, 200, "<html>Service Unavailable</html>", "text/html")
    with pytest.raises(AssetFetchError, match="example.test/x answered HTTP 200 with a non-JSON body") as ei:
        _get("https://example.test/x", params={"apikey": FOX_KEY})
    # Not chained at all: a JSONDecodeError's ``.doc`` is the whole body.
    assert ei.value.__context__ is None and ei.value.__cause__ is None
    assert FOX_KEY not in str(ei.value)


def test_failed_fetch_is_never_an_empty_frame(monkeypatch):
    _serve(monkeypatch, 503, '{"message": "Service Unavailable"}')
    from sportsdataverse.nhl import nhl_web_pbp

    with pytest.raises(AssetFetchError):  # before: a zero-row frame indistinguishable from "no plays"
        nhl_web_pbp(2023030417, num_retries=0)


# --------------------------------------------------------------------------------------
# every other runtime getter that had the bug
# --------------------------------------------------------------------------------------


def _download_getters() -> List[Tuple[str, Callable[[], Any]]]:
    from sportsdataverse import _subscription_http
    from sportsdataverse.cfb import on3_runtime
    from sportsdataverse.mbb import kenpom_runtime, torvik_runtime
    from sportsdataverse.mlb import mlb_api_extra, mlb_statcast_runtime
    from sportsdataverse.nfl import nfl_api_runtime
    from sportsdataverse.soccer.mls import mls_api_runtime
    from sportsdataverse.soccer.nwsl import nwsl_api_runtime
    from sportsdataverse.yahoo import yahoo_shangrila_runtime

    return [
        ("mlb_api_extra", lambda: mlb_api_extra._get("/api/v1/teams")),
        ("mls", lambda: mls_api_runtime._get("https://stats-api.mlssoccer.com/competitions")),
        ("nwsl", lambda: nwsl_api_runtime._get("https://api-sdp.nwslsoccer.com/v1/x")),
        ("statcast", lambda: mlb_statcast_runtime._get("https://baseballsavant.mlb.com/leaderboard/x")),
        ("torvik", lambda: torvik_runtime._get("https://barttorvik.com/x.json")),
        ("yahoo", lambda: yahoo_shangrila_runtime._get("https://graphite-secure.sports.yahoo.com/v1/query/x")),
        ("on3", lambda: on3_runtime._get("https://api.on3.com/public/rdb/v1/x")),
        ("nfl_api", lambda: nfl_api_runtime._get("https://api.nfl.com/football/v2/x", headers={})),
        (
            "kenpom",
            lambda: _subscription_http.get_html(
                kenpom_runtime.KENPOM, "https://kenpom.com/index.php", session=requests.Session()
            ),
        ),
    ]


@pytest.mark.parametrize("idx", range(9))
@pytest.mark.parametrize("status", [401, 503])
def test_other_download_getters_raise_on_a_failed_fetch(monkeypatch, idx, status):
    name, call = _download_getters()[idx]
    _serve(monkeypatch, status, '{"errors": [{"message": "nope"}]}')
    with pytest.raises(AssetFetchError, match=f"HTTP {status}"):
        call()


@pytest.mark.parametrize("idx", [0, 1, 2, 5, 6, 7])  # the JSON getters (not text/HTML ones)
def test_other_json_getters_raise_on_a_non_json_2xx(monkeypatch, idx):
    name, call = _download_getters()[idx]
    _serve(monkeypatch, 200, "<html>Access Denied</html>", "text/html")
    with pytest.raises(AssetFetchError, match="non-JSON body") as ei:
        call()
    assert ei.value.__context__ is None


def test_on3_scrape_get_raises_when_the_page_fails(monkeypatch):
    from sportsdataverse.cfb import on3_runtime

    monkeypatch.setattr(on3_runtime, "_build_id", None)
    _serve(monkeypatch, 403, "<html>Forbidden</html>", "text/html")
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        on3_runtime._scrape_get("https://www.on3.com/rivals/rankings/player/football/2026.json", num_retries=0)


def _tuple_transport(status: int, text: str) -> Callable[..., Tuple[int, str]]:
    return lambda *a, **k: (status, text)


@pytest.mark.parametrize(
    ("status", "text", "err"),
    [
        (404, "", NoDataError),
        (403, "Forbidden", AssetFetchError),
        (500, "{}", AssetFetchError),
        (200, "<html>", AssetFetchError),
    ],
)
def test_tuple_transport_getters(status, text, err):
    from sportsdataverse.cfb import sports247_runtime, sports247_site_pages_runtime
    from sportsdataverse.nfl import pff_runtime

    t = _tuple_transport(status, text)
    for call in (
        lambda: sports247_runtime._get("https://ipa.247sports.com/rdb/v1/x/", transport=t, auth=False),
        lambda: sports247_site_pages_runtime._get("https://247sports.com/Institution/1.json", transport=t),
        lambda: pff_runtime._get("https://premium.pff.com/api/v1/x", {}, cookies={"_premium_key": "PK"}, transport=t),
    ):
        with pytest.raises(err):
            call()


def test_tuple_transport_getters_keep_json_and_blank():
    from sportsdataverse.cfb import sports247_site_pages_runtime

    ok = sports247_site_pages_runtime._get(
        "https://247sports.com/x.json", transport=_tuple_transport(200, '{"Key": 1}')
    )
    assert ok == {"Key": 1}
    blank = sports247_site_pages_runtime._get("https://247sports.com/x.json", transport=_tuple_transport(200, " "))
    assert blank == {}


def test_247_refused_after_a_failed_remint_is_a_failed_fetch(monkeypatch):
    from sportsdataverse.cfb import sports247_runtime as rt

    monkeypatch.setattr(rt, "_jwt", "expired")
    monkeypatch.setattr(rt, "_mint_guest_jwt", lambda *a, **k: None)
    with pytest.raises(AssetFetchError, match="HTTP 401"):
        rt._get("https://ipa.247sports.com/rdb/v1/x/", transport=_tuple_transport(401, '{"message": "expired"}'))


def test_the_two_errors_stay_distinct():
    assert not issubclass(NoDataError, AssetFetchError)
    assert not issubclass(AssetFetchError, NoDataError)
