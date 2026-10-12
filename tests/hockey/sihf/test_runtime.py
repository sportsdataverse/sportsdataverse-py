"""The SIHF runtime builds the scoped query SIHF needs and refuses an answer it did not ask for.

Measured live on 2026-10-08 (see ``sportsdataverse.hockey.sihf.sihf_runtime``):

* the league must travel in ``searchQuery=1//<league>``; as a ``League`` filter the current
  season answers ``400 MissingLeagueId``;
* ``results`` without a ``Date`` range answers only today's game day for the current season;
* an unknown season / phase is answered with ANOTHER one, visible only in
  ``filters[].selected``, and an unknown league with one ``["Keine Tabelle gefunden!"]`` row.
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


def test_a_dropped_content_type_raises_no_data(monkeypatch):
    _patch(monkeypatch, _body(Season="2026"))
    with pytest.raises(NoDataError, match="dropped the ContentType filter"):
        rt._get(URL, {"alias": "standing", "searchQuery": "1//", "League": 1, "Season": 2026, "ContentType": "All"})


def test_results_always_send_the_season_date_range(monkeypatch):
    calls = _patch(monkeypatch, _body(Season="2027", Date="01.07.2026-30.06.2027"))
    rt._get(URL, {"alias": "results", "searchQuery": "1//", "Season": 2027, "League": 1})
    assert (calls[0]["filterBy"], calls[0]["filterQuery"]) == ("Season,Date", "2027/01.07.2026-30.06.2027")


def test_season_date_range_spans_july_to_june():
    assert rt.season_date_range(2026) == "01.07.2025-30.06.2026"


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


def test_a_dropped_filter_raises_no_data(monkeypatch):
    _patch(monkeypatch, _body(Season="2026"))
    with pytest.raises(NoDataError, match="dropped the Phase filter"):
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
        (501, "", AssetFetchError),  # an unknown table alias
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
