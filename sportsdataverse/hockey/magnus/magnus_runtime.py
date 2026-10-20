"""Runtime for the generated Ligue Magnus wrappers (``liguemagnus.com``, keyless).

Two sources sit on one host, both measured live (2026-10-12):

* **WordPress admin-ajax JSON.** ``GET /wp-admin/admin-ajax.php?action=<x>&...`` is one
  physical endpoint dispatched by ``action``; the wrappers send ``action`` as a fixed
  query parameter. Every answer is ``{"success": true, "data": ...}`` with HTTP 200, errors
  included, so :func:`_get` reads the envelope:

  * ``{"success": false, "data": "Aucune donnée disponible"}`` -> :class:`NoDataError` (the
    message is matched ignoring accents, case and whitespace); ``success: false`` with any other ``data`` ->
    :class:`AssetFetchError` (the request failed and the answer is unknown);
  * ``{"success": true, "data": ""}`` (a stub: a required parameter missing) -> :class:`NoDataError`;
  * ``{"success": true, "data": {"<param>": ["Le champ ... est invalide."]}}`` (a Laravel
    validation error naming the rejected parameter) -> :class:`ValueError`, like a 422;
  * a zero-row list -> :class:`NoDataError`;
  * rows outside the requested scope (a ``get_rencontres`` row of another phase or
    competition, a ``get_competition_by_type`` row of another type, a ``get_liste_tours``
    row of another phase, a ``get_rencontrestours`` round of another phase or round),
    non-object rows, or a ``get_rencontres`` page that is not the whole list (``meta``
    missing, or not both an int ``last_page`` of 1 and an int ``total`` equal to the rows
    served) -> :class:`AssetFetchError`. A row without the field to check is refused, never skipped.

* **The game sheet page.** ``GET /rencontre/{id}/`` is text/html; the whole game sheet is
  the HTML-escaped JSON value of the Vue ``:data`` attribute on ``<live-rencontre-container>``.
  :func:`_get` extracts it (regex anchored on that element -> ``html.unescape`` ->
  ``json.loads`` -> the first JSON object) and returns the object, so a game wrapper's
  ``return_parsed=False`` is the game sheet, not the page. A page without that element is an
  :class:`AssetFetchError`; an unknown id (its ``:data`` is ``{"message": ...}``) and a game
  with no events yet (unplayed) are :class:`NoDataError`; a sheet whose
  ``scores[].sportif_rencontre_id`` is not the requested id is an :class:`AssetFetchError`.
  Staff birth dates (``staffs[].ddn``) are dropped before the sheet is returned.

**Season key = END year** (2027 = 2026-27). The API keys a season by ``competition_id``;
:func:`_competition_id` (the ``season`` transform of the generated wrappers) maps it through
``get_competition_by_type&id_type=1``. The ids known on 2026-10-12 are seeded so the common
case costs no request; an unseeded season triggers one lookup per process. Coverage starts in
2020-21 (season 2021): an earlier season raises :class:`NoDataError` before any request.

Every request, each retry included, goes through
:data:`~sportsdataverse.hockey.euro_hockey_runtime.SESSION`, whose every attempt runs inside
:func:`~sportsdataverse.hockey.euro_hockey_runtime._polite` (at least 1 s after the host's last answer).
Everything else is the package error vocabulary of the shared getter: 404 ->
:class:`NoDataError`, 400 / 422 (an unknown ``action`` answers 400 ``0``) -> :class:`ValueError`,
any other non-2xx, an empty or non-JSON 200 or a dead connection -> :class:`AssetFetchError`.
"""

from __future__ import annotations

import functools
import html
import json
import re
import unicodedata
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as _eh

__all__: List[str] = []

HOST = "https://liguemagnus.com"
AJAX_URL = f"{HOST}/wp-admin/admin-ajax.php"
#: ``get_competition_by_type`` ``id_type`` of the Ligue Magnus (2 = Division 1).
LIGUE_MAGNUS_TYPE = 1
#: The first season (END year) this backend holds: 2020-21.
FIRST_SEASON = 2021
#: season (END year) -> ``competition_id``, from ``get_competition_by_type&id_type=1`` on 2026-10-12.
_SEASONS: Dict[int, int] = {2021: 1, 2022: 34, 2023: 79, 2024: 120, 2025: 157, 2026: 197, 2027: 240}

_GAME_PATH = re.compile(r"^/rencontre/(\d+)/?$")
# Anchored on the custom element so a :data attribute elsewhere on the page is never taken for
# the game sheet (sdv-internal-refs ligue-magnus/tools/capture.py ``_DATA_ATTR``).
_DATA_ATTR = re.compile(r'<live-rencontre-container\b[^>]*?\s:data="([^"]*)"')
_ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status"})
#: The ``success: false`` message that means "no data" (accent-, case- and whitespace-folded).
_NO_DATA_MESSAGE = "aucune donnee disponible"


def _at(obj: Any, *keys: str) -> Any:
    """``obj[k1][k2]...``; ``None`` as soon as a level is not an object."""
    for key in keys:
        obj = obj.get(key) if isinstance(obj, dict) else None
    return obj


def _int(value: Any) -> Optional[int]:
    """An int or a digit string as ``int``; anything else (a bool, a float, an object) -> ``None``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


# ---------------------------------------------------------------------------
# season -> competition_id
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _live_seasons() -> Dict[int, int]:
    """``{season: competition_id}`` from one ``get_competition_by_type`` request (cached per process)."""
    body = _get(AJAX_URL, {"action": "get_competition_by_type", "id_type": LIGUE_MAGNUS_TYPE})
    out: Dict[int, int] = {}
    for row in _at(body, "data", "data") or []:
        season, cid = _int(_at(row, "saison")), _int(_at(row, "id"))
        if season is not None and cid is not None:
            out[season] = cid
    return out


def _competition_id(season: Any) -> Optional[int]:
    """The ``competition_id`` of a Ligue Magnus season given by its END year (2027 = 2026-27).

    Args:
        season: END year of the season; ``None`` passes through (no competition filter).

    Returns:
        The competition id, or ``None`` when ``season`` is ``None``.

    Raises:
        ValueError: ``season`` is not a year.
        NoDataError: the season is before 2020-21 or the federation lists no Ligue Magnus
            competition for it.
    """
    if season is None:
        return None
    end = _int(season)
    if end is None:
        raise ValueError(f"season must be a year (END year, 2027 = 2026-27); got {season!r}")
    if end < FIRST_SEASON:
        raise NoDataError(f"Ligue Magnus data on liguemagnus.com starts in 2020-21 (season {FIRST_SEASON}); got {end}")
    if end not in _SEASONS:
        _SEASONS.update(_live_seasons())
    if end not in _SEASONS:
        raise NoDataError(f"liguemagnus.com lists no Ligue Magnus competition for season {end}")
    return _SEASONS[end]


# ---------------------------------------------------------------------------
# admin-ajax
# ---------------------------------------------------------------------------


def _rows(action: Any, data: Any) -> Any:
    """The row list of an admin-ajax ``data`` object (``None`` when it is missing)."""
    inner = _at(data, "data")
    if action == "get_rencontrestours":
        return _at(inner, "rencontres")
    if action == "get_classementsphase":
        return _at(inner, "positions")
    return inner


#: action -> [(request param, row -> the value that param scopes)].
_SCOPE: Dict[str, List[Any]] = {
    "get_competition_by_type": [("id_type", lambda r: _at(r, "type", "id"))],
    "get_rencontres": [
        ("phase_id", lambda r: _at(r, "phase_id")),
        ("competition_id", lambda r: _at(r, "competition", "id")),
    ],
    "get_liste_tours": [("phase_id", lambda r: _at(r, "phase_id"))],
}


def _fold(text: str) -> str:
    """``text`` without accents, case-folded, whitespace runs collapsed to one space."""
    plain = "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))
    return " ".join(plain.casefold().split())


def _is_no_data_message(data: Any) -> bool:
    """True for the ``success: false`` "Aucune donnée disponible" message, however it is spelled."""
    return isinstance(data, str) and _NO_DATA_MESSAGE in _fold(data)


def _check_ajax(url: str, body: Any, params: Mapping[str, Any]) -> Any:
    """Return ``body`` when it is a complete, in-scope admin-ajax answer; raise otherwise."""
    where, action = _rt._where(url), params.get("action")
    if not isinstance(body, dict) or not isinstance(body.get("success"), bool):
        raise AssetFetchError(f"{where} ({action}): the body is not a {{success, data}} envelope")
    data = body.get("data")
    if body["success"] is not True:
        detail = _rt._excerpt(data if isinstance(data, str) else json.dumps(data, ensure_ascii=False))
        if _is_no_data_message(data):
            raise NoDataError(f"{where} ({action}) answered success=false: {detail}")
        raise AssetFetchError(f"{where} ({action}) answered success=false with an unknown message: {detail}")
    if data in ("", None, [], {}):
        raise NoDataError(f"{where} ({action}) answered an empty data stub (a required parameter missing or unknown)")
    if isinstance(data, dict) and set(data) <= set(params) and all(isinstance(v, list) for v in data.values()):
        raise ValueError(
            f"{where} ({action}) rejected the request: {_rt._excerpt(json.dumps(data, ensure_ascii=False))}"
        )
    rows = _rows(action, data)
    if not isinstance(rows, list):
        raise AssetFetchError(f"{where} ({action}): the body carries no row list")
    if not rows:
        raise NoDataError(f"{where} ({action}) answered zero rows")
    if not all(isinstance(r, dict) for r in rows):
        raise AssetFetchError(f"{where} ({action}): the body has non-object rows")
    for param, get in _SCOPE.get(str(action), []):
        if params.get(param) is None:
            continue
        want = _int(params[param])
        bad = [r.get("id") for r in rows if _int(get(r)) is None or _int(get(r)) != want]
        if bad:
            raise AssetFetchError(f"{where} ({action}): {len(bad)} of {len(rows)} rows are outside {param}={want}")
    if action == "get_rencontrestours":
        rnd = _at(data, "data")
        got = (_int(_at(rnd, "phase_id")), _int(_at(rnd, "tour")))
        want_round = (_int(params.get("phase_id")), _int(params.get("journee")))
        if None in got or got != want_round:
            raise AssetFetchError(f"{where} ({action}): served phase/round {got}, asked {want_round}")
    if action == "get_rencontres":
        meta = _at(data, "meta")
        if not isinstance(meta, dict):
            raise AssetFetchError(f"{where} ({action}): no paging meta, so the list cannot be verified complete")
        last_page, total = meta.get("last_page"), meta.get("total")
        # strict types: a bool, a float or a digit string is not a page count (True == 1 in Python).
        if not (type(last_page) is int and last_page == 1 and type(total) is int and total == len(rows)):
            raise AssetFetchError(
                f"{where} ({action}): page {meta.get('current_page')}/{meta.get('last_page')} of "
                f"{meta.get('total')} games served {len(rows)}; a partial schedule is not returned as a whole one"
            )
    return body


# ---------------------------------------------------------------------------
# game sheet page
# ---------------------------------------------------------------------------


def _extract_game_sheet(page: str) -> Optional[Dict[str, Any]]:
    """The first JSON object in a ``<live-rencontre-container :data="...">`` attribute, unescaped."""
    for match in _DATA_ATTR.finditer(page or ""):
        try:
            body = json.loads(html.unescape(match.group(1)))
        except ValueError:
            continue
        if isinstance(body, dict):
            return body
    return None


def _game_sheet(url: str, game_id: str, **kwargs: Any) -> Dict[str, Any]:
    """Fetch ``/rencontre/{id}/`` and return its checked game sheet (see the module docstring)."""
    where = _rt._where(url)
    sheet = _extract_game_sheet(_eh.get_text(url, **kwargs))
    if sheet is None:
        raise AssetFetchError(f"{where}: no <live-rencontre-container :data> game sheet in the page")
    if (sheet and set(sheet) <= _ERROR_KEYS) or "evenements" not in sheet:
        raise NoDataError(
            f"{where}: no game sheet for game {game_id}: {_rt._excerpt(json.dumps(sheet, ensure_ascii=False))}"
        )
    events = sheet.get("evenements")
    if not isinstance(events, list):
        raise AssetFetchError(f"{where}: the game sheet's evenements is not a list")
    if not events:
        raise NoDataError(f"{where}: game {game_id} has no events yet (unplayed)")
    scores = sheet.get("scores")
    ids = {_int(_at(s, "sportif_rencontre_id")) for s in scores} if isinstance(scores, list) else set()
    if ids != {int(game_id)}:
        raise AssetFetchError(f"{where}: the sheet's scores are for games {sorted(map(str, ids))}, asked {game_id}")
    for staff in sheet.get("staffs") or []:
        if isinstance(staff, dict):
            staff.pop("ddn", None)  # staff birth dates are personal data the package never returns
    return sheet


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a Ligue Magnus route: an admin-ajax action as checked JSON, or a game page's game sheet.

    Args:
        url: fully-built request URL (``.../wp-admin/admin-ajax.php`` or ``.../rencontre/{id}/``).
        params: query-string parameters (``action`` included); ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The admin-ajax ``{"success": true, "data": ...}`` body, or the game sheet object.

    Raises:
        NoDataError: 404; ``success: false`` "Aucune donnée disponible"; an empty ``data``
            stub; zero rows; a season outside the coverage; an unknown game id; a game without
            events.
        ValueError: 400 / 422, or a Laravel validation error naming a rejected parameter.
        AssetFetchError: any other non-2xx after retries, a dead connection, an empty or
            non-JSON body, ``success: false`` with any other message, a body outside the
            requested scope, a partial schedule page, or a game page without its embedded
            game sheet.
    """
    match = _GAME_PATH.match(urlsplit(url).path)
    if match:
        return _game_sheet(url, match.group(1), **kwargs)
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    return _check_ajax(url, _eh._fetch_json(url, clean, **kwargs), clean)
