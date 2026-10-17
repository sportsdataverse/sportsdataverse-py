"""The SIHF runtime builds the scoped query SIHF needs and refuses an answer it did not ask for.

Measured live on 2026-10-08 (see ``sportsdataverse.hockey.sihf.sihf_runtime``):

* the league must travel in ``searchQuery=1//<league>``; as a ``League`` filter the current
  season answers ``400 MissingLeagueId``;
* ``results`` without a ``Date`` range answers only today's game day for the current season;
* an unknown season / phase is answered with ANOTHER one, visible only in
  ``filters[].selected``, and an unknown league with one ``["Keine Tabelle gefunden!"]`` row;
* the error shapes drift within a day (2026-10-12): an unknown alias answered a 200
  ``{"stub": true}`` and later an empty 501, a missing ``searchQuery`` an empty 501 and later
  400 ``InvalidSearchQuery``; a game body must echo the requested ``gameId``.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey.sihf import sihf_runtime as rt

URL = "https://data.sihf.ch/statistic/api/cms/table"


def _body(rows: Any = None, **selected: str) -> Dict[str, Any]:
    # Like a real body, each filter lists its own selected value among its entries.
    return {
        "filters": [{"alias": k, "selected": v, "entries": [{"alias": v}]} for k, v in selected.items()],
        "header": [{"alias": "rank"}, {"alias": "team"}],
        "data": [["1", "x"]] if rows is None else rows,
    }


def _patch(monkeypatch: pytest.MonkeyPatch, body: Any) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []

    def fake(url: str, params: Any = None, **kw: Any) -> Any:
        calls.append(dict(params or {}))
        return body

    monkeypatch.setattr(rt._rt, "_get", fake)
    return calls


def test_league_moves_into_the_search_scope(monkeypatch):
    calls = _patch(monkeypatch, _body(Season="2027"))
    rt._get(URL, {"alias": "standing", "searchQuery": "1//", "Season": 2027, "League": 1, "language": "en"})
    assert calls == [
        {"alias": "standing", "searchQuery": "1//1", "language": "en", "filterBy": "Season", "filterQuery": "2027"}
    ]


def test_phase_folds_after_season(monkeypatch):
    calls = _patch(monkeypatch, _body(Season="2026", Phase="4940"))
    rt._get(URL, {"alias": "standing", "searchQuery": "1//", "Phase": 4940, "Season": 2026, "League": 2})
    assert calls[0]["searchQuery"] == "1//2"
    assert (calls[0]["filterBy"], calls[0]["filterQuery"]) == ("Season,Phase", "2026/4940")


def test_standings_content_type_folds_last(monkeypatch):
    # Without ContentType, standing answers the away-games table and does not echo it (measured 2026-10-12).
    calls = _patch(monkeypatch, _body(Season="2026", Phase="4940", ContentType="All"))
    rt._get(URL, {"alias": "standing", "searchQuery": "1//", "Season": 2026, "Phase": 4940, "ContentType": "All"})
    assert (calls[0]["filterBy"], calls[0]["filterQuery"]) == ("Season,Phase,ContentType", "2026/4940/All")


@pytest.mark.parametrize(
    ("echo", "match"),
    [({}, "dropped the ContentType filter"), ({"ContentType": "Away"}, "answered ContentType=Away")],
)
def test_a_dropped_or_swapped_content_type_is_a_failed_fetch(monkeypatch, echo, match):
    # Every season has an All table: an answer without it is broken, not empty (never NoDataError).
    _patch(monkeypatch, _body(Season="2026", **echo))
    with pytest.raises(AssetFetchError, match=match):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026, "ContentType": "All"})


def test_results_always_send_the_season_date_range(monkeypatch):
    calls = _patch(monkeypatch, _body(Season="2027", Date="01.07.2026-30.06.2027"))
    rt._get(URL, {"alias": "results", "searchQuery": "1//", "Season": 2027, "League": 1})
    assert (calls[0]["filterBy"], calls[0]["filterQuery"]) == ("Season,Date", "2027/01.07.2026-30.06.2027")


def test_season_date_range_spans_july_to_june():
    assert rt._season_date_range(2026) == "01.07.2025-30.06.2026"


def test_unset_filters_are_not_sent(monkeypatch):
    calls = _patch(monkeypatch, _body(Season="2026"))
    rt._get(URL, {"alias": "player", "searchQuery": "1//", "Season": 2026, "League": 1, "Phase": None})
    assert calls[0]["filterBy"] == "Season"
    assert "Phase" not in calls[0] and "League" not in calls[0]


def test_game_route_is_untouched(monkeypatch):
    calls = _patch(monkeypatch, {"gameId": "1"})
    url = "https://data.sihf.ch/statistic/api/cms/gameoverview"
    assert rt._get(url, {"alias": "gameDetail", "searchQuery": 1}) == {"gameId": "1"}
    assert calls == [{"alias": "gameDetail", "searchQuery": 1}]


@pytest.mark.parametrize(
    ("sent", "echo", "name"),
    [
        ({"Season": 1890}, {"Season": "2027"}, "Season"),
        ({"Season": 2026, "Phase": 1}, {"Season": "2026", "Phase": "4940"}, "Phase"),
    ],
)
def test_a_fallback_answer_raises_no_data(monkeypatch, sent, echo, name):
    _patch(monkeypatch, _body(**echo))
    with pytest.raises(NoDataError, match=f"{name}="):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, **sent})


@pytest.mark.parametrize(
    ("echo", "name"),
    [({"Season": "2026"}, "Phase"), ({"Phase": "4940"}, "Season")],
)
def test_a_filter_missing_from_the_echo_is_a_failed_fetch(monkeypatch, echo, name):
    # The body has an echo but leaves the requested filter out: SIHF ignored it, so the answer is
    # unverifiable (AssetFetchError), not "no data" -- like a body with no echo at all.
    _patch(monkeypatch, _body(**echo))
    with pytest.raises(AssetFetchError, match=f"dropped the {name} filter"):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026, "Phase": 4940})


def test_an_echoed_but_unlisted_phase_raises_no_data(monkeypatch):
    # results?Phase=0 echoes selected "0" and serves the Playout Final (measured 2026-10-10).
    body = _body(Season="2026", Phase="0", Date="01.07.2025-30.06.2026")
    body["filters"][1]["entries"] = [{"alias": "all"}, {"alias": "5294"}, {"alias": "4940"}]
    _patch(monkeypatch, body)
    with pytest.raises(NoDataError, match="lists no Phase=0"):
        rt._get(URL, {"alias": "results", "searchQuery": "1//", "League": 1, "Season": 2026, "Phase": 0})


def test_a_phase_echo_without_entries_cannot_be_verified(monkeypatch):
    body = _body(Season="2026", Phase="4940")
    del body["filters"][1]["entries"]
    _patch(monkeypatch, body)
    with pytest.raises(AssetFetchError, match="Phase=4940 cannot be verified"):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026, "Phase": 4940})


def test_an_empty_phase_asks_for_the_list_without_checking_the_echo(monkeypatch):
    # sihf_phases sends Phase="": SIHF picks its default phase and lists every one.
    calls = _patch(monkeypatch, _body(Season="2026", Phase="5297"))
    rt._get(URL, {"alias": "player", "searchQuery": "1//", "League": 1, "Season": 2026, "Phase": ""})
    assert (calls[0]["filterBy"], calls[0]["filterQuery"]) == ("Season,Phase", "2026/")


def test_an_empty_phase_still_checks_the_season(monkeypatch):
    _patch(monkeypatch, _body(Season="2027", Phase="657"))
    with pytest.raises(NoDataError, match="Season=1890"):
        rt._get(URL, {"alias": "player", "searchQuery": "1//", "League": 1, "Season": 1890, "Phase": ""})


def test_an_empty_phase_needs_the_entries_list(monkeypatch):
    body = _body(Season="2026", Phase="5297")
    del body["filters"][1]["entries"]
    _patch(monkeypatch, body)
    with pytest.raises(AssetFetchError, match="no Phase entries"):
        rt._get(URL, {"alias": "player", "searchQuery": "1//", "League": 1, "Season": 2026, "Phase": ""})


def test_unknown_league_sentinel_raises_no_data(monkeypatch):
    _patch(monkeypatch, _body([["Keine Tabelle gefunden!"]], Season="2027"))
    with pytest.raises(NoDataError, match="Keine Tabelle gefunden"):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 999, "Season": 2027})


def test_a_body_without_its_echo_cannot_be_verified(monkeypatch):
    _patch(monkeypatch, {"header": [], "data": []})
    with pytest.raises(AssetFetchError, match="no filter echo"):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026})


def test_a_truncated_table_raises_asset_fetch_error(monkeypatch):
    # ``pages`` is the total row count (results 414 / 414, goalkeeper 35 / 35, measured 2026-10-12).
    body = _body(Season="2026", Date="01.07.2025-30.06.2026")
    body["pages"] = 414
    _patch(monkeypatch, body)
    with pytest.raises(AssetFetchError, match="announces 414 rows but serves 1"):
        rt._get(URL, {"alias": "results", "searchQuery": "1//", "League": 1, "Season": 2026})


@pytest.mark.parametrize("pages", [1, 0, None])
def test_a_whole_or_unpaged_table_passes(monkeypatch, pages):
    body = _body(Season="2026")  # one row
    body["pages"] = pages  # standing answers 0
    _patch(monkeypatch, body)
    assert rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026}) is body


def test_a_null_selected_is_a_failed_fetch_not_no_data(monkeypatch):
    # ``selected: null`` echoes nothing, like a dropped filter: unverifiable, never "no data".
    body = _body(Season="2026")
    body["filters"][0]["selected"] = None
    _patch(monkeypatch, body)
    with pytest.raises(AssetFetchError, match="dropped the Season filter"):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026})


GAME_URL = "https://data.sihf.ch/statistic/api/cms/gameoverview"


@pytest.mark.parametrize(
    "body",
    [
        {"gameId": "20261105000009"},  # another game
        {"gameId": None},
        {"stub": True, "data": [], "generatedAt": "2026-10-12T07:45:00"},  # /cache300?alias=gameDetail
        {},
        [],
    ],
)
def test_a_game_body_must_echo_the_requested_game(monkeypatch, body):
    _patch(monkeypatch, body)
    with pytest.raises(AssetFetchError):
        rt._get(GAME_URL, {"alias": "gameDetail", "searchQuery": 20261105000325, "language": "en"})


def test_a_game_body_echoing_the_game_as_a_string_passes(monkeypatch):
    body = {"gameId": "20261105000325", "summary": {}}
    _patch(monkeypatch, body)
    assert rt._get(GAME_URL, {"alias": "gameDetail", "searchQuery": 20261105000325}) is body


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ({"stub": True, "data": [], "generatedAt": "2026-10-12T07:45:00"}, "stub"),
        ({"action": "Redirect", "redirectTo": {"alias": "teamGoal"}}, "redirect to alias 'teamGoal'"),
    ],
)
def test_a_stub_or_redirect_table_body_is_a_failed_fetch(monkeypatch, body, match):
    _patch(monkeypatch, body)
    with pytest.raises(AssetFetchError, match=match):
        rt._get(URL, {"alias": "team", "searchQuery": "1//", "League": 1, "Season": 2026})


def test_every_request_goes_over_the_throttled_session(monkeypatch):
    """``SESSION`` waits for the host's slot before every attempt, retries included (no extra explicit wait)."""
    seen: List[Any] = []
    monkeypatch.setattr(
        rt._rt, "_get", lambda url, params=None, **kw: seen.append(kw.get("session")) or _body(Season="2026")
    )
    rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026})
    assert seen == [rt._eh.SESSION]


def test_a_matching_echo_returns_the_body(monkeypatch):
    body = _body(Season="2026")
    _patch(monkeypatch, body)
    assert rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026}) is body


class _Resp:
    def __init__(self, text: str, status_code: int) -> None:
        self.text = text
        self.status_code = status_code

    def json(self) -> Any:
        return json.loads(self.text)


@pytest.mark.parametrize(
    ("status", "text", "exc"),
    [
        (404, '{"error":"GameNotFound"}', NoDataError),
        (400, '{"error":"MissingLeagueId"}', ValueError),
        (501, "", AssetFetchError),  # an unknown table alias (09:35 EDT 2026-10-12) or no searchQuery (07:45)
        (400, '{"error":"InvalidSearchQuery"}', ValueError),  # no searchQuery (09:35 EDT 2026-10-12)
        # an unknown table alias at ~07:45 EDT 2026-10-12: the same request, a 200 stub instead of the 501
        (200, '{"stub":true,"data":[],"generatedAt":"2026-10-12T07:45:00+02:00"}', AssetFetchError),
    ],
)
def test_error_vocabulary_through_the_shared_getter(monkeypatch, status, text, exc):
    def download(url, **kw):
        if status == 404:
            raise NoDataError(f"No data found for {url}")
        return _Resp(text, status)

    monkeypatch.setattr(rt._rt, "download", download)
    with pytest.raises(exc):
        rt._get(URL, {"alias": "nosuch", "searchQuery": "1//"})


def test_a_stub_game_body_through_the_shared_getter(monkeypatch):
    # /cache300?alias=gameDetail answers a 200 stub: through the real getter it raises, never parses empty.
    monkeypatch.setattr(rt._rt, "download", lambda url, **kw: _Resp('{"stub":true,"data":[]}', 200))
    with pytest.raises(AssetFetchError, match="stub"):
        rt._get(GAME_URL, {"alias": "gameDetail", "searchQuery": 20261105000325})
