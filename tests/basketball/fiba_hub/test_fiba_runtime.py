"""The FIBA runtime keeps the key out of sight, refuses answers that are not about the request and spaces every attempt.

Every refusal case of ``sdv-internal-refs/fiba/tools/capture.py`` is exercised through a mocked transport
(``_codegen_runtime.download`` for GDAP, ``fiba_runtime._page_transport`` for game pages), measured live on
2026-10-12:

* GDAP: a missing key is 401, a missing parameter HTTP 500 with an empty body, an unknown id HTTP 200 with ``{}`` /
  ``[]``; bodies are checked for scope (type-strict ids, the date window, the family and season).
* game pages: a wrong slug, team-code order or competition answers HTTP 200 with the 120-150 KB not-found shell.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List

import pytest
import requests
from requests.adapters import BaseAdapter

from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime as base
from sportsdataverse.basketball.fiba_hub import fiba_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

GDAP = Path(__file__).parents[2] / "fixtures" / "fiba_gdap"
PAGES = Path(__file__).parents[2] / "fixtures" / "fiba_gamepage"
URL = f"{rt.GDAP_URL}/getgdapgamesbycompetitionid"
KEY = "0123456789abcdef0123456789abcdef"
OVR = json.loads((PAGES / "full" / "page__wc2023-final.json").read_text(encoding="utf-8"))
WC = json.loads((GDAP / "gdap__getGdapCompetitionById__wc2023.json").read_text(encoding="utf-8"))
SCHEDULE = [
    {
        "gameId": 100720,
        "teamA": {"code": "ger", "teamId": 279569},
        "teamB": {"code": "SRB", "teamId": 279961},
        "competition": {"competitionId": 208182},
    }
]


def _gdap(name: str) -> Any:
    return json.loads((GDAP / name).read_text(encoding="utf-8"))


class _Resp:
    def __init__(
        self,
        body: Any = None,
        status: int = 200,
        ctype: str = "application/json",
        headers: Dict[str, str] | None = None,
    ):
        self.status_code = status
        self.text = body if isinstance(body, str) else ("" if body is None else json.dumps(body))
        self.headers = {"content-type": ctype, **(headers or {})}
        self.url = "u"

    def json(self) -> Any:
        return json.loads(self.text)


class Seq(list):
    """Answers consumed one per request (a plain list is a JSON array body)."""


class Net:
    """The mocked transport: GDAP answers by method (a value, a list consumed in order, or a callable)."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.answers: Dict[str, Any] = {}
        self.key_page = (
            'self.__next_f.push([1,"{\\"NEXT_CLIENT_APIM_SUBSCRIPTION_KEY\\":\\"' + KEY + '\\",\\"x\\":1}"])'
        )

    def download(self, url: str, params: Any = None, headers: Any = None, **kw: Any) -> _Resp:
        self.calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {}), **kw})
        if url == rt.KEY_PAGE:
            return _Resp(self.key_page, ctype="text/html")
        answer = self.answers[url.rsplit("/", 1)[-1]]
        if isinstance(answer, Seq):
            answer = answer.pop(0)
        if callable(answer):
            answer = answer(params)
        return answer if isinstance(answer, _Resp) else _Resp(answer)

    def gdap_calls(self) -> List[Dict[str, Any]]:
        return [c for c in self.calls if c["url"] != rt.KEY_PAGE]


@pytest.fixture
def net(monkeypatch):
    n = Net()
    monkeypatch.setattr(rt._rt, "download", n.download)
    rt._api_key.cache_clear()
    rt._codes.clear()
    rt._competition_slug.cache_clear()
    yield n
    rt._api_key.cache_clear()
    rt._codes.clear()
    rt._competition_slug.cache_clear()


def _call(params: Dict[str, Any] | None = None, url: str = URL) -> Any:
    return rt._get(url, {"gdapCompetitionId": 208182} if params is None else params)


def _rows(competition_id: Any = 208182, n: int = 2) -> List[Dict[str, Any]]:
    return [{"gameId": 100720 + i, "competition": {"competitionId": competition_id}} for i in range(n)]


# --- the key -------------------------------------------------------------------------------------------------------------------


def test_the_key_is_read_from_the_live_page_once_and_sent_as_the_apim_header(net):
    net.answers["getgdapgamesbycompetitionid"] = _rows()
    _call()
    _call()
    assert [c["url"] for c in net.calls].count(rt.KEY_PAGE) == 1  # cached per process
    sent = net.gdap_calls()[0]
    assert sent["headers"][rt.KEY_HEADER] == KEY and sent["session"] is base.SESSION
    assert sent["params"] == {"gdapCompetitionId": 208182}


def test_the_documented_key_is_used_when_the_page_cannot_be_read(net):
    net.key_page = "<html>no client env here</html>"
    net.answers["getgdapgamesbycompetitionid"] = _rows()
    _call()
    assert net.gdap_calls()[0]["headers"][rt.KEY_HEADER] == rt._FALLBACK_KEY


def test_the_key_page_not_found_falls_back_without_raising(net, monkeypatch):
    def boom(url, **kw):
        if url == rt.KEY_PAGE:
            return _Resp("nope", status=404, ctype="text/html")
        return net.download(url, **kw)

    monkeypatch.setattr(rt._rt, "download", boom)
    net.answers["getgdapgamesbycompetitionid"] = _rows()
    assert _call()


def test_a_401_re_reads_the_key_once_and_retries(net):
    net.answers["getgdapgamesbycompetitionid"] = Seq(
        [_Resp({"statusCode": 401, "message": "Access denied"}, 401), _rows()]
    )
    assert len(_call()) == 2
    assert [c["url"] for c in net.calls].count(rt.KEY_PAGE) == 2  # the cache was cleared and read again


def test_two_401s_raise_and_the_key_is_never_in_the_message(net):
    net.answers["getgdapgamesbycompetitionid"] = lambda p: _Resp({"statusCode": 401, "message": "denied"}, 401)
    with pytest.raises(AssetFetchError) as err:
        _call()
    assert KEY not in str(err.value) and rt._FALLBACK_KEY not in str(err.value) and "401" in str(err.value)


# --- error vocabulary ----------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("answer", "exc"),
    [
        (_Resp("", status=404), NoDataError),
        ({}, NoDataError),  # an unknown id answers 200 {}
        ([], NoDataError),
        (_Resp("null"), NoDataError),
        (_Resp("", status=500), AssetFetchError),  # a missing parameter answers 500 with an empty body
        (_Resp("<html>blocked</html>", status=403, ctype="text/html"), AssetFetchError),
        (_Resp("<html>oops</html>", ctype="text/html"), AssetFetchError),  # 200, not JSON
        (_Resp("", status=200), AssetFetchError),  # 200, empty body
        (_Resp({"statusCode": 500, "message": "boom"}, status=200), AssetFetchError),  # an error envelope
        ({"message": "x"}, AssetFetchError),
        ({"Error": "unknown competition"}, AssetFetchError),  # the site JS treats {"Error": ...} as a failure
        (_Resp({"message": "bad"}, status=400), ValueError),
        (_Resp({"message": "bad"}, status=422), ValueError),
        (["a", {"gameId": 1}], AssetFetchError),
    ],
)
def test_error_vocabulary(net, answer, exc):
    net.answers["getgdapgamesbycompetitionid"] = answer
    with pytest.raises(exc):
        _call()


def test_http_500_is_not_retried(net):
    net.answers["getgdapgamesbycompetitionid"] = _Resp("", status=500)
    with pytest.raises(AssetFetchError):
        _call()
    assert _Resp  # download is mocked, so the retry policy is the argument handed to it
    assert 500 not in net.gdap_calls()[0]["retry_statuses"] and 429 in net.gdap_calls()[0]["retry_statuses"]


# --- wrong scope ----------------------------------------------------------------------------------------------------------------


def test_rows_of_another_competition_are_refused_not_returned(net):
    net.answers["getgdapgamesbycompetitionid"] = _rows(competition_id=208722)
    with pytest.raises(AssetFetchError, match="outside the request"):
        _call()


@pytest.mark.parametrize("wrong", ["208182", 208182.0, True, None])
def test_scope_ids_are_compared_type_strictly(net, wrong):
    net.answers["getgdapgamesbycompetitionid"] = _rows(competition_id=wrong)
    with pytest.raises(AssetFetchError):
        _call()


def test_one_row_of_another_competition_among_good_ones_is_refused(net):
    rows = _rows() + _rows(competition_id=1, n=1)
    net.answers["getgdapgamesbycompetitionid"] = rows
    with pytest.raises(AssetFetchError):
        _call()


def test_a_row_without_the_field_to_check_is_refused_never_skipped(net):
    net.answers["getgdapgamesbycompetitionid"] = [{"gameId": 1}]
    with pytest.raises(AssetFetchError):
        _call()


@pytest.mark.parametrize(
    ("method", "params", "body"),
    [
        ("getgdapgamebyid", {"gdapGameId": 100720}, {"gameId": 999}),
        ("getGdapCompetitionById", {"gdapCompetitionId": 208182}, {"competitionId": 1}),
        ("getgdapcompetitionteamrosterbyteamid", {"gdapTeamId": 279569}, {"teamId": 1, "competitionId": 1}),
        (
            "getgdapplayergamestatisticsbyplayerid",
            {"gdapPlayerId": 211501, "gdapCompetitionId": 208182},
            {"playerId": 1, "competitionId": 208182},
        ),
        (
            "getgdapplayerinfobycompetitionid",
            {"gdapPlayerId": 211501, "gdapCompetitionId": 208182},
            {"playerId": 211501, "competitionId": 1},
        ),
        (
            "getgdapfederationgamesbyfederationid",
            {"gdapFederationId": 64},
            [{"teamA": {"organisationId": 1}, "teamB": {"organisationId": 2}}],
        ),
        (
            "getgdapgameswithleaderdetailsbyteamid",
            {"gdapTeamId": 279569},
            [{"teamA": {"teamId": 1}, "teamB": {"teamId": 2}}],
        ),
        ("getgdapcompetitionteamsbycompetitionid", {"gdapCompetitionId": 208182}, [{"profile": {"competitionId": 1}}]),
        ("getgdapcompetitionnominationsbyid", {"gdapCompetitionId": 208182}, [{"competitionLevelId": 1}]),
        ("getgdapcompetitionawardsbyid", {"gdapCompetitionId": 208182}, [{"competitionId": 1}]),
    ],
)
def test_every_scoped_route_refuses_a_body_for_another_entity(net, method, params, body):
    net.answers[method] = body
    with pytest.raises(AssetFetchError, match="outside the request"):
        rt._get(f"{rt.GDAP_URL}/{method}", params)


def test_a_federation_game_where_either_side_is_the_asked_federation_is_accepted(net):
    net.answers["getgdapfederationgamesbyfederationid"] = [
        {"teamA": {"organisationId": 64}, "teamB": {"organisationId": 2}},
        {"teamA": {"organisationId": 3}, "teamB": {"organisationId": 64}},
    ]
    assert len(rt._get(f"{rt.GDAP_URL}/getgdapfederationgamesbyfederationid", {"gdapFederationId": 64})) == 2


def test_the_date_window_is_checked_on_the_venue_local_time_and_dateTo_is_exclusive(net):
    url = f"{rt.GDAP_URL}/getgdapgamesbetweentwodates"
    params = {"dateFrom": "2023-09-08T00:00:00Z", "dateTo": "2023-09-11T00:00:00Z"}
    net.answers["getgdapgamesbetweentwodates"] = [
        {"gameDateTime": "2023-09-08T00:00:00"},
        {"gameDateTime": "2023-09-10T22:00:00"},
    ]
    assert len(rt._get(url, params)) == 2
    for bad in ("2023-09-11T00:00:00", "2023-09-07T23:59:59", None, 5):
        net.answers["getgdapgamesbetweentwodates"] = [{"gameDateTime": "2023-09-09T10:00:00"}, {"gameDateTime": bad}]
        with pytest.raises(AssetFetchError, match="outside"):
            rt._get(url, params)


def test_editions_must_be_of_the_asked_family_and_inside_the_season_range(net):
    url = f"{rt.GDAP_URL}/getgdapcompetitionsbyseasonsandcompetitionmarketingname"
    ok = {"season": 2025, "competitionMarketingName": {"competitionMarketingNameId": 112}}
    net.answers["getgdapcompetitionsbyseasonsandcompetitionmarketingname"] = [ok]
    assert rt._get(url, {"competitionMarketingNameId": 112, "fromSeason": 2025, "toSeason": 2025})
    for bad in (
        {**ok, "season": 2024},
        {**ok, "competitionMarketingName": {"competitionMarketingNameId": 201}},
        {**ok, "season": "2025"},
    ):
        net.answers["getgdapcompetitionsbyseasonsandcompetitionmarketingname"] = [bad]
        with pytest.raises(AssetFetchError):
            rt._get(url, {"competitionMarketingNameId": 112, "fromSeason": 2025, "toSeason": 2025})


def test_one_season_asked_means_that_season_only(net):
    url = f"{rt.GDAP_URL}/getgdapcompetitionsbyseasonsandcompetitionmarketingname"
    net.answers["getgdapcompetitionsbyseasonsandcompetitionmarketingname"] = [
        {"season": 2025, "competitionMarketingName": {"competitionMarketingNameId": 112}}
    ]
    rt._get(url, {"competitionMarketingNameId": 112, "fromSeason": 2025})
    assert net.gdap_calls()[0]["params"]["toSeason"] == 2025


def test_unscoped_routes_pass_but_are_still_checked_for_emptiness(net):
    net.answers["getgdapnationalteams"] = [{"nationalFederationId": 64}]
    assert rt._get(f"{rt.GDAP_URL}/getgdapnationalteams", {})
    net.answers["getgdapnationalteams"] = []
    with pytest.raises(NoDataError):
        rt._get(f"{rt.GDAP_URL}/getgdapnationalteams", {})


def test_a_ranking_date_between_two_rankings_is_not_a_wrong_scope(net):
    # the API answers the latest ranking published on or before the date: asOfDate may differ
    net.answers["getgdapfederationsranking"] = {"asOfDate": "2026-03-03T00:00:00", "items": [{"worldRank": 1}]}
    body = rt._get(f"{rt.GDAP_URL}/getgdapfederationsranking", {"gdapCategory": "men", "asOfDate": "2026-03-10"})
    assert body["items"]


# --- personal data ---------------------------------------------------------------------------------------------------------------


def test_the_runtime_drops_contact_details_and_staff_dates_of_birth(net):
    raw = _gdap("gdap__getgdapcompetitionnominationsbyid__wc2023.json")
    assert "contactDetails" in raw[0] and "dateOfBirth" in raw[0]
    net.answers["getgdapcompetitionnominationsbyid"] = raw
    out = rt._get(
        f"{rt.GDAP_URL}/getgdapcompetitionnominationsbyid", {"gdapCompetitionId": 208182, "level": "competition"}
    )
    text = json.dumps(out)
    assert "contactDetails" not in text and "dateOfBirth" not in text and "@" not in text
    assert out[0]["lastName"] == raw[0]["lastName"]  # the rest of the row survives


def test_players_dates_of_birth_survive_but_coaches_do_not(net):
    raw = json.loads(
        (GDAP / "full" / "getgdapcompetitionteamrosterbyteamid__wc2023-ger.json").read_text(encoding="utf-8")
    )
    raw["coaches"][0]["dateOfBirth"] = "1970-01-01"  # the fixture was scrubbed on capture; put one back
    net.answers["getgdapcompetitionteamrosterbyteamid"] = raw
    out = rt._get(f"{rt.GDAP_URL}/getgdapcompetitionteamrosterbyteamid", {"gdapTeamId": 279569})
    assert all(p.get("dateOfBirth") for p in out["players"])
    assert all("dateOfBirth" not in c for c in out["coaches"])


def test_a_game_header_loses_its_officials_birth_dates(net):
    raw = _gdap("gdap__getgdapgamebyid__wc2023-final.json")
    net.answers["getgdapgamebyid"] = raw
    out = rt._get(f"{rt.GDAP_URL}/getgdapgamebyid", {"gdapGameId": 100720})
    assert out["nominations"] and all("dateOfBirth" not in n for n in out["nominations"])


# --- argument transforms and the season mapping --------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", [208182, "208182", " 208182 "])
def test_int_id_accepts_integers_and_digit_strings(value):
    assert rt._int_id(value) == 208182


@pytest.mark.parametrize("value", [208182.0, "208182.0", True, "x", "", -1.5, [1]])
def test_int_id_refuses_a_float_a_bool_or_text(value):
    with pytest.raises(ValueError, match="integer"):
        rt._int_id(value)


def test_int_id_passes_none_through():
    assert rt._int_id(None) is None


@pytest.mark.parametrize(("given", "sent"), [(2024, 2024), ("2025", 2025), (2023, 2023), (1950, 1950), (None, None)])
def test_season_is_sent_unchanged_club_end_year_and_national_event_year(given, sent):
    # BCL 2024-25 is season 2025 (END year); World Cup 2023 is 2023 (event year): both are already the sdv key
    assert rt._season(given) == sent


@pytest.mark.parametrize("bad", [1949, 3000, 2024.5, "twenty", True])
def test_season_refuses_what_is_not_a_year(bad):
    with pytest.raises(ValueError):
        rt._season(bad)


def test_window_date_builds_the_api_timestamp():
    import datetime

    assert rt._window_date("2023-09-08") == "2023-09-08T00:00:00Z"
    assert rt._window_date(datetime.date(2023, 9, 8)) == "2023-09-08T00:00:00Z"
    assert rt._window_date(datetime.datetime(2023, 9, 8, 15, 30)) == "2023-09-08T00:00:00Z"
    assert rt._window_date("2023-09-08T10:00:00Z") == "2023-09-08T00:00:00Z"
    assert rt._window_date(None) is None
    for bad in ("09/08/2023", "2023-13-40", 20230908):
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            rt._window_date(bad)


# --- throttling -------------------------------------------------------------------------------------------------------------------------


class _Adapter(BaseAdapter):
    """Answers the queued statuses, one per request, so a retry loop is real but offline."""

    def __init__(self, statuses: List[int]) -> None:
        super().__init__()
        self.statuses = list(statuses)
        self.sent = 0

    def send(self, request, **kwargs):  # noqa: ANN001
        self.sent += 1
        resp = requests.Response()
        resp.status_code = self.statuses.pop(0)
        resp._content = b"[]" if resp.status_code == 200 else b""
        resp.headers["Retry-After"] = "0"
        resp.url = request.url
        resp.request = request
        return resp

    def close(self) -> None:
        pass


def test_every_attempt_of_a_retried_gdap_request_waits_its_turn(monkeypatch):
    waits: List[str] = []
    monkeypatch.setattr(base, "_polite_wait", lambda url, *a, **k: waits.append(url))
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        rt, "_api_key", type("K", (), {"__call__": lambda self: KEY, "cache_clear": lambda self: None})()
    )
    session = base.PoliteSession()
    adapter = _Adapter([429, 503, 200])
    session.mount("https://", adapter)
    with pytest.raises(NoDataError):  # the final 200 body is [] : nothing there, after three attempts
        rt._get(URL, {"gdapCompetitionId": 208182}, session=session)
    assert adapter.sent == 3 and len(waits) == 3  # one wait per attempt, retries included


def test_the_shared_session_is_the_polite_one():
    assert isinstance(base.SESSION, base.PoliteSession) and rt.SESSION is base.SESSION


def test_each_game_page_attempt_waits_and_retry_after_is_honoured(net, monkeypatch):
    waits, sleeps = [], []
    monkeypatch.setattr(rt, "_polite_wait", lambda url: waits.append(url))
    monkeypatch.setattr(rt.time, "sleep", sleeps.append)
    answers = [
        _Resp("", 429, "text/html", {"retry-after": "7"}),
        _Resp("", 503, "text/html"),
        _Resp(_flight(OVR), ctype="text/x-component"),
    ]
    monkeypatch.setattr(rt, "_page_transport", lambda url, headers, follow=True: answers.pop(0))
    _wire_gdap(net)
    page = rt._get(PAGE_URL, {"comp": 208182})
    assert page["game"]["gameId"] == 100720
    page_waits = [w for w in waits if "/games/" in w]
    assert len(page_waits) == 3 and sleeps[0] >= 7 and sleeps[1] >= 4  # Retry-After 7 s; the 503 backs off


def test_a_connection_failure_outliving_the_attempts_is_an_asset_fetch_error(net, monkeypatch):
    monkeypatch.setattr(rt, "_polite_wait", lambda url: None)
    monkeypatch.setattr(rt.time, "sleep", lambda s: None)
    calls = []

    def dead(url, headers, follow=True):
        calls.append(url)
        raise ConnectionError("reset")

    monkeypatch.setattr(rt, "_page_transport", dead)
    _wire_gdap(net)
    with pytest.raises(AssetFetchError, match="after 4 attempts"):
        rt._get(PAGE_URL, {"comp": 208182})
    assert len(calls) == rt._PAGE_ATTEMPTS


# --- game pages ----------------------------------------------------------------------------------------------------------------------------

PAGE_URL = "https://www.fiba.basketball/en/games/100720"
GOOD = "https://www.fiba.basketball/en/history/201-fiba-basketball-world-cup/208182/games/100720-GER-SRB"


def _flight(page: Dict[str, Any], **override: Any) -> str:
    """A text/x-component stand-in: the page's objects as ``"key":{...}`` literals among flight noise."""
    body = {**copy.deepcopy(page), **override}
    parts = [f'"{key}":{json.dumps(value)}' for key, value in body.items() if value is not ... and key != "_extra"]
    return '0:["$","div",null,{"children":[' + ",".join(parts) + '],"ui":{"playByPlay":{"timeout":5}}}]\n'


def _wire_gdap(net: Net) -> None:
    net.answers["getGdapCompetitionById"] = copy.deepcopy(WC)
    net.answers["getgdapgamesbycompetitionid"] = copy.deepcopy(SCHEDULE)


def _transport(monkeypatch, answers: Dict[str, Any]) -> List[str]:
    monkeypatch.setattr(rt, "_polite_wait", lambda url: None)
    monkeypatch.setattr(rt.time, "sleep", lambda s: None)
    seen: List[str] = []

    def fake(url, headers, follow=True):
        seen.append(url)
        got = answers[url] if url in answers else answers["*"]
        return got.pop(0) if isinstance(got, list) else got

    monkeypatch.setattr(rt, "_page_transport", fake)
    return seen


def _ok(page: Dict[str, Any] = OVR, **override: Any) -> _Resp:
    return _Resp(_flight(page, **override), ctype="text/x-component; charset=utf-8")


def test_the_page_url_is_built_from_gdap_and_the_rsc_header_is_sent(net, monkeypatch):
    _wire_gdap(net)
    seen = _transport(monkeypatch, {"*": _ok()})
    headers: List[Dict[str, str]] = []
    real = rt._page_transport
    monkeypatch.setattr(rt, "_page_transport", lambda url, h, follow=True: headers.append(h) or real(url, h, follow))
    page = rt._get(PAGE_URL, {"comp": 208182})
    assert seen == [GOOD]  # team codes upper-cased from the schedule, history slug from the marketing name
    assert headers[0]["RSC"] == "1"
    assert set(page) == {"game", "playByPlay", "gameDetails", "playersTeamA", "playersTeamB"}
    assert page["gameDetails"]["id"] == 100720 and page["playByPlay"]["items"]["Q1"]["items"]


def test_the_schedule_is_fetched_once_per_competition(net, monkeypatch):
    _wire_gdap(net)
    _transport(monkeypatch, {"*": [_ok(), _ok()]})
    rt._get(PAGE_URL, {"comp": 208182})
    rt._get(PAGE_URL, {"comp": 208182})
    methods = [c["url"].rsplit("/", 1)[-1] for c in net.gdap_calls()]
    assert methods.count("getgdapgamesbycompetitionid") == 1 and methods.count("getGdapCompetitionById") == 1


@pytest.mark.parametrize(
    ("name", "slug"),
    [
        ("FIBA Basketball World Cup", "fiba-basketball-world-cup"),
        ("FIBA Men’s European Club Competitions – Tier 1", "fiba-mens-european-club-competitions-tier-1"),
        ("Men's Olympic Basketball Tournament", "mens-olympic-basketball-tournament"),
        ("FIBA Women’s Basketball World Cup", "fiba-womens-basketball-world-cup"),
    ],
)
def test_history_slugs_follow_the_documented_rule(name, slug):
    assert rt._slugify(name) == slug


def test_a_page_needs_comp(net):
    with pytest.raises(ValueError, match="comp"):
        rt._get(PAGE_URL, {})
    with pytest.raises(ValueError, match="integer"):
        rt._get(PAGE_URL, {"comp": 208182.0})


def test_a_game_the_competition_does_not_list_is_no_data(net):
    _wire_gdap(net)
    with pytest.raises(NoDataError, match="lists no game"):
        rt._get("https://www.fiba.basketball/en/games/999", {"comp": 208182})


def test_not_a_fiba_route_is_a_value_error():
    with pytest.raises(ValueError, match="not a FIBA route"):
        rt._get("https://example.com/x", {})


SHELL = '0:["$","html",null,{"ui":{"playByPlay":{"timeout":5}},"children":"' + "x" * 120000 + '"}]\n'


def test_the_not_found_shell_is_refused_not_returned_as_an_empty_page(net, monkeypatch):
    _wire_gdap(net)
    seen = _transport(monkeypatch, {"*": _Resp(SHELL, ctype="text/x-component")})
    with pytest.raises(AssetFetchError, match="not-found shell"):
        rt._get(PAGE_URL, {"comp": 208182})
    assert any("championsleague" in u for u in seen)  # history, no event page, then the Champions League host


def test_a_shell_at_the_archive_url_falls_back_to_the_event_page(net, monkeypatch):
    _wire_gdap(net)
    event = "https://www.fiba.basketball/en/events/world-cup-2023/games/100720-GER-SRB"
    redirect = _Resp("", 307, "text/html", {"location": "https://www.fiba.basketball/en/events/world-cup-2023"})
    seen = _transport(
        monkeypatch,
        {
            GOOD: _Resp(SHELL, ctype="text/x-component"),
            "https://www.fiba.basketball/en/history/201-fiba-basketball-world-cup/208182": redirect,
            event: _ok(),
        },
    )
    page = rt._get(PAGE_URL, {"comp": 208182})
    assert page["game"]["gameId"] == 100720
    assert seen[-1] == event and len(seen) == 3


def test_a_page_for_another_game_is_the_shell(net, monkeypatch):
    _wire_gdap(net)
    other = copy.deepcopy(OVR)
    other["game"]["gameId"] = 555
    _transport(monkeypatch, {"*": _Resp(_flight(other), ctype="text/x-component")})
    with pytest.raises(AssetFetchError, match="not-found shell"):
        rt._get(PAGE_URL, {"comp": 208182})


def test_a_page_of_another_competition_is_refused(net, monkeypatch):
    _wire_gdap(net)
    other = copy.deepcopy(OVR)
    other["game"]["competition"]["competitionId"] = 208722
    _transport(monkeypatch, {"*": _Resp(_flight(other), ctype="text/x-component")})
    with pytest.raises(AssetFetchError, match="competition"):
        rt._get(PAGE_URL, {"comp": 208182})


def test_a_box_score_for_another_game_is_refused(net, monkeypatch):
    _wire_gdap(net)
    other = copy.deepcopy(OVR)
    other["gameDetails"]["id"] = "100720"  # a string is not the int the page sends
    _transport(monkeypatch, {"*": _Resp(_flight(other), ctype="text/x-component")})
    with pytest.raises(AssetFetchError, match="box score is for game"):
        rt._get(PAGE_URL, {"comp": 208182})


def test_a_header_only_page_is_no_data(net, monkeypatch):
    _wire_gdap(net)
    header_only = {k: v for k, v in OVR.items() if k in ("game", "playersTeamA", "playersTeamB")}
    _transport(monkeypatch, {"*": _Resp(_flight(header_only), ctype="text/x-component")})
    with pytest.raises(NoDataError, match="no box score"):
        rt._get(PAGE_URL, {"comp": 208182})


def test_a_game_still_live_is_refused(net, monkeypatch):
    _wire_gdap(net)
    live = copy.deepcopy(OVR)
    live["game"]["isLive"] = True
    _transport(monkeypatch, {"*": _Resp(_flight(live), ctype="text/x-component")})
    with pytest.raises(AssetFetchError, match="still live"):
        rt._get(PAGE_URL, {"comp": 208182})


@pytest.mark.parametrize(
    ("resp", "exc"),
    [
        (_Resp("<html>denied</html>", 403, "text/html"), AssetFetchError),
        (_Resp("", 404, "text/html"), NoDataError),
        (
            _Resp("<html>page</html>", 200, "text/html"),
            AssetFetchError,
        ),  # RSC header ignored: HTML, not a flight payload
        (_Resp("", 200, "text/x-component"), AssetFetchError),
        (_Resp("x", 400, "text/html"), ValueError),
    ],
)
def test_page_http_failures_use_the_error_vocabulary(net, monkeypatch, resp, exc):
    _wire_gdap(net)
    _transport(monkeypatch, {"*": resp})
    with pytest.raises(exc):
        rt._get(PAGE_URL, {"comp": 208182})


def test_flight_tokens_are_resolved_and_back_references_are_copied(net, monkeypatch):
    _wire_gdap(net)
    page = copy.deepcopy(OVR)
    page["gameDetails"]["teamA"] = "$1a:props:gameDetails:c:0"  # a flight back-reference to the first team
    page["playByPlay"]["items"]["Q1"]["items"][0]["txt"] = "$undefined"
    page["game"]["note"] = "$$literal"
    page["game"]["when"] = "$D2023-09-10T20:00:00"
    _transport(monkeypatch, {"*": _Resp(_flight(page), ctype="text/x-component")})
    out = rt._get(PAGE_URL, {"comp": 208182})
    assert out["gameDetails"]["teamA"] == OVR["gameDetails"]["c"][0]
    assert out["playByPlay"]["items"]["Q1"]["items"][0]["txt"] is None
    assert out["game"]["note"] == "$literal" and out["game"]["when"] == "2023-09-10T20:00:00"


@pytest.mark.parametrize(
    "token", ["$Zweird", "$L12", "$1a:props:nowhere:x", "$1a:props:gameDetails:c:99", "$1a:props:gameDetails:nope"]
)
def test_an_unknown_or_dangling_flight_token_fails_closed(net, monkeypatch, token):
    _wire_gdap(net)
    page = copy.deepcopy(OVR)
    page["game"]["note"] = token
    _transport(monkeypatch, {"*": _Resp(_flight(page), ctype="text/x-component")})
    with pytest.raises(AssetFetchError, match="flight"):
        rt._get(PAGE_URL, {"comp": 208182})


def test_the_playbyplay_config_object_is_not_taken_for_the_play_by_play(net, monkeypatch):
    # the UI ships {"playByPlay":{"timeout":5}} before the data; only {"playByPlay":{"items":...}} counts
    _wire_gdap(net)
    _transport(monkeypatch, {"*": _ok()})
    out = rt._get(PAGE_URL, {"comp": 208182})
    assert "items" in out["playByPlay"] and "timeout" not in out["playByPlay"]


def test_a_box_only_page_keeps_its_box_score(net, monkeypatch):
    _wire_gdap(net)
    box_only = {k: v for k, v in OVR.items() if k != "playByPlay"}
    _transport(monkeypatch, {"*": _Resp(_flight(box_only), ctype="text/x-component")})
    out = rt._get(PAGE_URL, {"comp": 208182})
    assert "playByPlay" not in out and out["gameDetails"]["c"]


def test_the_proxy_environment_is_not_used_and_no_secret_reaches_an_error(net, monkeypatch):
    monkeypatch.setenv("GENIUS_PROXY", "http://user:secret@proxy.invalid:1")
    _wire_gdap(net)
    _transport(monkeypatch, {"*": _Resp("<html>denied</html>", 403, "text/html")})
    with pytest.raises(AssetFetchError) as err:
        rt._get(PAGE_URL, {"comp": 208182})
    assert "secret" not in str(err.value) and KEY not in str(err.value)


def test_a_wrapper_url_is_the_pseudo_route_the_getter_rewrites():
    assert rt._PAGE_PATH.match("/en/games/100720") and not rt._PAGE_PATH.match("/en/games/100720-GER-SRB")
