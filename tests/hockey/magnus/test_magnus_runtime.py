"""The Ligue Magnus runtime maps the END-year season, extracts the game sheet and refuses bad answers.

Every refusal case of ``sdv-internal-refs/ligue-magnus/tools/capture.py`` is exercised through a
mocked transport (``_codegen_runtime.download``), measured live on 2026-10-12:

* admin-ajax errors arrive as HTTP 200: ``success: false``, an empty ``data`` stub, a Laravel
  validation dict; plus zero rows, non-object rows, out-of-scope rows and partial pages;
* the game page embeds its sheet in ``<live-rencontre-container :data>``; an unknown id renders
  ``{"message": ...}``, an unplayed game has no events, and the sheet names its own game id.
"""

from __future__ import annotations

import copy
import html
import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as ehr
from sportsdataverse.hockey.magnus import magnus_runtime as rt

FIXTURES = Path(__file__).parents[2] / "fixtures" / "magnus"
GAME_URL = "https://liguemagnus.com/rencontre/77779/"


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class _Resp:
    def __init__(self, text: str, status_code: int = 200, content_type: str = "application/json") -> None:
        self.text = text
        self.status_code = status_code
        self.headers = {"content-type": content_type}

    def json(self) -> Any:
        return json.loads(self.text)


class _Transport:
    """The mocked transport: ``calls`` logs each request, ``waits`` each one sent over the throttled session."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.waits: List[str] = []
        self._answer: Any = None

    def download(self, url: str, params: Any = None, **kw: Any) -> Any:
        self.calls.append({"url": url, "params": dict(params or {})})
        if kw.get("session") is ehr.SESSION:  # every attempt on SESSION waits for the host's slot
            self.waits.append(url)
        if isinstance(self._answer, BaseException):
            raise self._answer
        return self._answer

    def answer(self, value: Any) -> None:
        self._answer = value

    def json(self, body: Any, status: int = 200) -> None:
        self.answer(_Resp(json.dumps(body), status))

    def page(self, sheet: Any, decoy: bool = True) -> None:
        """A game page; ``decoy`` puts another element's ``:data`` attribute first."""
        attr = html.escape(json.dumps(sheet), quote=True)
        head = '<div :data="{&quot;evenements&quot;: []}"></div>' if decoy else ""
        body = f'<html>{head}<live-rencontre-container id="x" :data="{attr}"></live-rencontre-container></html>'
        self.answer(_Resp(body, 200, "text/html"))


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> _Transport:
    t = _Transport()
    monkeypatch.setattr(rt._rt, "download", t.download)
    return t


def _ajax(params: Dict[str, Any]) -> Any:
    return rt._get(rt.AJAX_URL, params)


# --- season -> competition_id -------------------------------------------------------


@pytest.mark.parametrize(("season", "cid"), [(2021, 1), (2022, 34), (2026, 197), (2027, 240), ("2027", 240)])
def test_seeded_seasons_map_without_a_request(transport, season, cid):
    assert rt._competition_id(season) == cid
    assert transport.calls == []


def test_none_season_passes_through(transport):
    assert rt._competition_id(None) is None


def test_a_season_before_2020_21_raises_no_data_without_a_request(transport):
    with pytest.raises(NoDataError, match="2020-21"):
        rt._competition_id(2020)
    assert transport.calls == []


@pytest.mark.parametrize("season", ["2026-27", True, 2026.5])
def test_a_non_year_season_is_a_value_error(transport, season):
    with pytest.raises(ValueError, match="season must be a year"):
        rt._competition_id(season)
    assert transport.calls == []


def test_an_unseeded_season_is_looked_up_once(monkeypatch, transport):
    monkeypatch.setattr(rt, "_SEASONS", dict(rt._SEASONS))
    rt._live_seasons.cache_clear()
    body = _load("ajax__get_competition_by_type.json")
    body["data"]["data"].append({**body["data"]["data"][0], "id": 281, "saison": 2028})
    transport.json(body)
    try:
        assert rt._competition_id(2028) == 281
        assert rt._competition_id(2028) == 281
        assert len(transport.calls) == 1
        assert transport.calls[0]["params"] == {"action": "get_competition_by_type", "id_type": 1}
        with pytest.raises(NoDataError, match="2031"):
            rt._competition_id(2031)  # the per-process lookup is not repeated
        assert len(transport.calls) == 1
    finally:
        rt._live_seasons.cache_clear()


# --- admin-ajax envelope -------------------------------------------------------------


def test_every_request_waits_for_the_host_throttle(transport):
    transport.json(_load("ajax__get_phases.json"))
    _ajax({"action": "get_phases", "competition_id": 240})
    assert transport.waits == [rt.AJAX_URL]


def test_a_good_body_is_returned_unchanged_and_none_params_are_dropped(transport):
    body = _load("ajax__get_rencontres@round2.json")
    transport.json(body)
    out = _ajax({"action": "get_rencontres", "competition_id": 240, "phase_id": 714, "journee": 2, "equipe_id": None})
    assert out == body
    assert "equipe_id" not in transport.calls[0]["params"]


@pytest.mark.parametrize(
    ("body", "exc", "match"),
    [
        ({"success": False, "data": "Aucune donnée disponible"}, NoDataError, "success=false"),
        ({"success": False, "data": "  AUCUNE   DONNEE\tdisponible. "}, NoDataError, "success=false"),
        ({"success": False, "data": "aucune donnée disponible pour cette phase"}, NoDataError, "success=false"),
        ({"success": False, "data": "Erreur interne du serveur"}, AssetFetchError, "unknown message"),
        ({"success": False, "data": {"code": 500}}, AssetFetchError, "unknown message"),
        ({"success": False, "data": ""}, AssetFetchError, "unknown message"),
        ({"success": True, "data": ""}, NoDataError, "empty data"),
        (
            {"success": True, "data": {"competition_id": ["Le champ competition id est invalide."]}},
            ValueError,
            "rejected",
        ),
        ({"success": True, "data": {"data": [], "time": "1"}}, NoDataError, "zero rows"),
        ({"success": True, "data": {"data": [1, 2], "time": "1"}}, AssetFetchError, "non-object"),
        ({"success": True, "data": {"time": "1"}}, AssetFetchError, "no row list"),
        ({"data": {"data": [{"id": 1}]}}, AssetFetchError, "envelope"),
        ([{"id": 1}], AssetFetchError, "envelope"),
    ],
)
def test_error_envelopes_are_refused(transport, body, exc, match):
    transport.json(body)
    with pytest.raises(exc, match=match):
        _ajax({"action": "get_phases", "competition_id": 240})


def test_schedule_rows_of_another_phase_are_refused(transport):
    body = _load("ajax__get_rencontres@round2.json")
    body["data"]["data"][0]["phase_id"] = 560
    transport.json(body)
    with pytest.raises(AssetFetchError, match="outside phase_id=714"):
        _ajax({"action": "get_rencontres", "competition_id": 240, "phase_id": 714, "par_page": 700})


def test_schedule_rows_of_another_season_are_refused(transport):
    body = _load("ajax__get_rencontres@round2.json")
    body["data"]["data"][2]["competition"]["id"] = 197
    transport.json(body)
    with pytest.raises(AssetFetchError, match="outside competition_id=240"):
        _ajax({"action": "get_rencontres", "competition_id": 240, "par_page": 700})


def test_a_row_without_the_scope_field_is_refused_not_skipped(transport):
    body = _load("ajax__get_rencontres@round2.json")
    del body["data"]["data"][0]["phase_id"]
    transport.json(body)
    with pytest.raises(AssetFetchError, match="outside phase_id"):
        _ajax({"action": "get_rencontres", "competition_id": 240, "phase_id": 714})


def test_competitions_of_another_type_are_refused(transport):
    transport.json(_load("ajax__get_competition_by_type.json"))
    with pytest.raises(AssetFetchError, match="outside id_type=2"):
        _ajax({"action": "get_competition_by_type", "id_type": 2})


def test_rounds_of_another_phase_are_refused(transport):
    transport.json(_load("ajax__get_liste_tours.json"))
    with pytest.raises(AssetFetchError, match="outside phase_id=560"):
        _ajax({"action": "get_liste_tours", "phase_id": 560})


def test_a_round_of_another_number_is_refused(transport):
    transport.json(_load("ajax__get_rencontrestours.json"))
    _ajax({"action": "get_rencontrestours", "phase_id": 714, "journee": 1})
    with pytest.raises(AssetFetchError, match="phase/round"):
        _ajax({"action": "get_rencontrestours", "phase_id": 714, "journee": 2})


def test_a_schedule_without_paging_meta_is_unverifiable(transport):
    body = _load("ajax__get_rencontres@round2.json")
    del body["data"]["meta"]
    transport.json(body)
    with pytest.raises(AssetFetchError, match="no paging meta"):
        _ajax({"action": "get_rencontres", "competition_id": 240})


def test_a_partial_schedule_page_is_refused(transport):
    body = _load("ajax__get_rencontres@round2.json")
    body["data"]["meta"].update({"last_page": 14, "total": 264, "current_page": 1})
    transport.json(body)
    with pytest.raises(AssetFetchError, match="partial schedule"):
        _ajax({"action": "get_rencontres", "competition_id": 240})


def test_a_single_page_with_a_matching_total_passes(transport):
    body = _load("ajax__get_rencontres@round2.json")
    assert body["data"]["meta"]["last_page"] == 1 and body["data"]["meta"]["total"] == 6
    transport.json(body)
    assert _ajax({"action": "get_rencontres", "competition_id": 240}) == body


@pytest.mark.parametrize(
    "meta",
    [
        {"last_page": 2, "total": 6},  # rows match the total, but it is not the only page: both must hold
        {"last_page": 1, "total": 7},  # one page, but it does not hold every game
        {"last_page": True, "total": 6},  # True == 1 in Python; not a page count
        {"last_page": "1", "total": 6},
        {"last_page": 1.0, "total": 6},
        {"last_page": 1, "total": "6"},
        {"last_page": 1, "total": None},
    ],
)
def test_the_paging_check_is_type_strict_and_needs_both_conditions(transport, meta):
    body = _load("ajax__get_rencontres@round2.json")
    body["data"]["meta"].update(meta)
    transport.json(body)
    with pytest.raises(AssetFetchError, match="partial schedule"):
        _ajax({"action": "get_rencontres", "competition_id": 240})


@pytest.mark.parametrize(
    ("answer", "exc"),
    [
        (NoDataError("No data found for https://liguemagnus.com/wp-admin/admin-ajax.php"), NoDataError),
        (_Resp("0", 400, "text/html"), ValueError),  # an unknown action
        (_Resp("", 503), AssetFetchError),
        (_Resp("<html>challenge</html>", 200, "text/html"), AssetFetchError),
        (ConnectionError("reset"), AssetFetchError),
    ],
)
def test_error_vocabulary_through_the_shared_getter(transport, answer, exc):
    transport.answer(answer)
    with pytest.raises(exc):
        _ajax({"action": "get_nothing"})


# --- game sheet page -----------------------------------------------------------------


def _sheet() -> Dict[str, Any]:
    return copy.deepcopy(_load("full/rencontre__77779.json"))


def test_the_game_sheet_is_extracted_from_its_element_only(transport):
    sheet = _sheet()
    transport.page(sheet)  # a decoy :data on another element comes first
    out = rt._get(GAME_URL, {})
    assert len(out["evenements"]) == 159
    assert transport.calls[0]["url"] == GAME_URL
    assert transport.waits == [GAME_URL]


def test_html_entities_in_the_attribute_are_unescaped(transport):
    sheet = _sheet()
    sheet["patinoire"] = 'L\'Île & "Glace"'
    transport.page(sheet)
    assert rt._get(GAME_URL)["patinoire"] == 'L\'Île & "Glace"'


def test_staff_birth_dates_are_never_returned(transport):
    sheet = _sheet()
    assert all("ddn" in s for s in sheet["staffs"])
    transport.page(sheet)
    out = rt._get(GAME_URL)
    assert out["staffs"] and not any("ddn" in s for s in out["staffs"])


def test_a_page_without_the_game_element_cannot_be_read(transport):
    transport.answer(_Resp('<html><div :data="{&quot;x&quot;: 1}"></div></html>', 200, "text/html"))
    with pytest.raises(AssetFetchError, match="no <live-rencontre-container"):
        rt._get(GAME_URL)


def test_an_unknown_game_id_raises_no_data(transport):
    transport.page({"message": "Rencontre introuvable"}, decoy=False)
    with pytest.raises(NoDataError, match="no game sheet"):
        rt._get(GAME_URL)


def test_an_unplayed_game_raises_no_data(transport):
    sheet = _sheet()
    sheet["evenements"] = []
    transport.page(sheet)
    with pytest.raises(NoDataError, match="unplayed"):
        rt._get(GAME_URL)


@pytest.mark.parametrize("ids", [[77779, 77780], [77780, 77780], []])
def test_a_sheet_for_another_game_is_refused(transport, ids):
    sheet = _sheet()
    sheet["scores"] = [{**sheet["scores"][0], "sportif_rencontre_id": i} for i in ids]
    transport.page(sheet)
    with pytest.raises(AssetFetchError, match="asked 77779"):
        rt._get(GAME_URL)


def test_a_missing_game_page_raises_no_data(transport):
    transport.answer(NoDataError(f"No data found for {GAME_URL}"))
    with pytest.raises(NoDataError):
        rt._get(GAME_URL)


def test_a_blocked_game_page_is_a_failed_fetch(transport):
    transport.answer(_Resp("Just a moment...", 403, "text/html"))
    with pytest.raises(AssetFetchError, match="403"):
        rt._get(GAME_URL)
