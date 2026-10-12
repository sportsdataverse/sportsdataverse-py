"""Runtime for the generated KHL site REST wrappers (``POST https://www.khl.ru/rest/<route>/``).

The khl.ru front end loads its data blocks by POSTing a form to ``/rest/<route>/``; this module is
that client. Measured live (2026-10-12, ``sdv-internal-refs/khl``):

* **Session.** One ``curl_cffi`` session per host (Chrome TLS, a cookie jar), kept in-process. It is
  primed by a GET of ``/`` that follows redirects (the first answer is a **307** that sets the
  ServicePipe ``spid`` / ``spsc`` cookies) and scrapes the Bitrix ``sessid`` with
  ``sessid":"([a-f0-9]{32})``. Never a headless browser: ServicePipe answers one with a 403.
* **POST.** Form-encoded ``sessid`` plus the wrapper's ``values[...]`` / ``nav[...]`` / ``page``
  fields, with ``X-Requested-With: XMLHttpRequest`` and ``Origin`` / ``Referer`` set to the site.
  The generated wrappers hand those fields over as ``params``: the codegen calls every getter
  ``_get(url, params=...)``, so this ``_get`` sends ``params`` as the POST form, not a query string.
* **307 = rotated WAF cookie.** A POST that answers 307 re-primes the session once (at least one
  second later) and is retried once; a second 307 raises
  :class:`~sportsdataverse.errors.AssetFetchError`.
* **Errors in a 200.** Bad or missing fields still answer HTTP 200
  ``{"status": "error", "data": null, "errors": [{"message": ...}]}`` (``code: invalid_csrf`` for a
  stale ``sessid``). ``status != "success"`` or a non-empty ``errors`` raises ``AssetFetchError``;
  an ``invalid_csrf`` also drops the cached session, so the next call primes a fresh one.
* **Season -> tournament.** No route takes a year. ``season`` (the END year, 2026 = 2025/26) and
  ``stage`` (``regular`` / ``playoff``) become ``values[tournament]`` (the ``tnId``) through the
  webcaster season table (:func:`sportsdataverse.hockey.khl.khl_webcaster_runtime.stage_for`, cached
  in-process), unless ``values[tournament]`` is given (``tournament_id``).
* **Refusals.** An empty ``data`` (``players/stats`` without a player answers ``success`` with
  ``data: []``) or an empty row container raises :class:`~sportsdataverse.errors.NoDataError`; so does
  a game page that names another game or tournament than the one requested, a player page of
  another player, or a schedule whose rows are not (90 %+) of the requested tournament (the All-Star
  games ride along under their own tournament id, so 100 % is not required).
* **Redactions.** ``fonbet`` (the betting-widget flag of ``game/text``) and ``bets`` are removed.

Status and decoding go through the shared vocabulary (404 -> ``NoDataError``, 400/422 ->
``ValueError``, any other non-2xx, a non-JSON body or a dead connection -> ``AssetFetchError``), with
at least one second between two requests to the host
(:func:`sportsdataverse.hockey.euro_hockey_runtime._polite_wait`).
"""

from __future__ import annotations

import re
import threading
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey.euro_hockey_runtime import _polite_wait
from sportsdataverse.hockey.khl import khl_webcaster_runtime as _wc

__all__: List[str] = []

SESSID_RE = re.compile(r'sessid":"([a-f0-9]{32})')
TIMEOUT = 60
TOURNAMENT = "values[tournament]"
#: Routes that answer an error envelope without a tournament.
NEEDS_TOURNAMENT = frozenset({"/game/header/", "/game/text/", "/game/protocol/", "/standings/playoff/"})
#: Where each route keeps its rows: an empty container there is "no data".
ROWS: Dict[str, Tuple[Any, ...]] = {
    "/calendar/list/": ("GAMES",),
    "/game/header/": ("game",),
    "/game/text/": ("game", "players"),
    "/game/protocol/": ("teams",),
    "/standings/regular/": ("json", "league"),
    "/standings/playoff/": ("LIST",),
    "/stat/players/": ("list", 0, "list"),
    "/players/profile/": ("DATA",),
    "/players/stats/": ("tournament_summary",),
    "/clubs/team/": ("players",),
    "/filters/statplayers/": ("LIST",),
}

_sessions: Dict[str, List[Any]] = {}  # site -> [curl_cffi session, sessid]
_lock = threading.Lock()


def _new_http() -> Any:
    """A Chrome-impersonating ``curl_cffi`` session (lazy import: ``curl_cffi`` is an optional extra)."""
    try:
        from curl_cffi import requests as creq
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "The khl.ru REST wrappers need curl_cffi (the site's WAF fingerprints TLS). "
            "Install with: pip install curl_cffi or pip install sportsdataverse[all]"
        ) from exc
    return creq.Session(impersonate="chrome")


def _site(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def _headers(site: str) -> Dict[str, str]:
    return {
        "X-Requested-With": "XMLHttpRequest",
        "Origin": site,
        "Referer": site + "/",
        "Accept": "application/json, */*",
    }


def _prime(http: Any, site: str, timeout: Any) -> str:
    """GET ``site/`` (following the cookie-setting 307) and return the page's ``sessid``."""
    home = site + "/"
    _polite_wait(home)
    with _rt._transport_errors(home):
        resp = http.get(home, timeout=timeout)
    status = getattr(resp, "status_code", None)
    _rt._check_status(home, status, "")
    match = SESSID_RE.search(getattr(resp, "text", "") or "")
    if match is None:
        raise AssetFetchError(
            f"{_rt._where(home)} answered HTTP {status} without a sessid; the session cannot be primed"
        )
    return match.group(1)


def _form(route: str, params: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """The POST form: ``None`` dropped, ``season`` / ``stage`` resolved to ``values[tournament]``."""
    form = {k: v for k, v in (params or {}).items() if v is not None}
    season, stage = form.pop("season", None), form.pop("stage", None)
    if season is not None and TOURNAMENT not in form:
        form[TOURNAMENT] = _wc.stage_for(season, stage)["tournament_id"]
    if route in NEEDS_TOURNAMENT and TOURNAMENT not in form:
        raise ValueError(f"{route} needs a season (END year, with stage) or a tournament_id")
    return {k: str(v) for k, v in form.items()}


def _dig(obj: Any, path: Tuple[Any, ...]) -> Any:
    for step in path:
        if isinstance(step, int):
            obj = obj[step] if isinstance(obj, list) and len(obj) > step else None
        else:
            obj = obj.get(step) if isinstance(obj, dict) else None
    return obj


def _check(url: str, route: str, raw: Any, form: Dict[str, str]) -> None:
    """Raise on an error envelope, an empty answer, or one scoped to something not requested."""
    where = _rt._where(url)
    if not isinstance(raw, dict) or "status" not in raw:
        raise AssetFetchError(f"{where}: the body is not a khl.ru envelope")
    errors = raw.get("errors")
    if str(raw.get("status")).lower() != "success" or errors:
        first = errors[0] if isinstance(errors, list) and errors and isinstance(errors[0], dict) else {}
        detail = " ".join(str(first.get(k)) for k in ("code", "message") if first.get(k))
        raise AssetFetchError(f"{where}: khl.ru answered status={raw.get('status')!r}: {_rt._excerpt(detail)}")
    data = raw.get("data")
    if not isinstance(data, (dict, list)) or not data:
        raise NoDataError(f"{where}: khl.ru answered success with no data")
    rows = _dig(data, ROWS[route]) if route in ROWS else data
    if not isinstance(rows, (dict, list)) or not rows:
        raise NoDataError(f"{where}: khl.ru answered success with no rows")
    page: Dict[str, Any] = data if isinstance(data, dict) else {}
    game, tn = form.get("values[gameid]"), form.get(TOURNAMENT)
    if route in ("/game/header/", "/game/text/"):
        echo = _dig(page, ("game",) if route == "/game/header/" else ("game", "info", "data"))
        echo = echo if isinstance(echo, dict) else {}
        got = (str(echo.get("id")), str(echo.get("tnId")))
        if got != (game, tn):
            raise NoDataError(f"{where}: asked for game {game} of tournament {tn}, answered game {got[0]} of {got[1]}")
    if route == "/players/stats/" and str(page.get("id")) != form.get("values[id]"):
        raise NoDataError(f"{where}: asked for player {form.get('values[id]')}, answered player {page.get('id')}")
    if route == "/players/profile/":
        link = str(_dig(page, ("DATA", "pagelink")) or "")
        if f"/players/{form.get('values[id]')}/" != link:
            raise NoDataError(
                f"{where}: asked for player {form.get('values[id]')}, answered {link or 'no player link'}"
            )
    if route == "/calendar/list/" and tn is not None:
        ids = [
            str(g.get("tnId"))
            for week in page.get("GAMES") or []
            if isinstance(week, dict)
            for day in week.values()
            if isinstance(day, list)
            for g in day
            if isinstance(g, dict)
        ]
        if not ids or sum(i == tn for i in ids) / len(ids) < 0.9:
            raise NoDataError(f"{where}: asked for tournament {tn}, but most of its games are of another tournament")


def _get(url: str, params: Optional[dict] = None, headers: Optional[Dict[str, str]] = None, **kwargs: Any) -> Any:
    """POST a khl.ru REST route (the form is ``params``) within the primed session, then verify it.

    Args:
        url: the route URL (``https://www.khl.ru/rest/<a>/<b>/``; ``en.khl.ru`` gets its own session).
        params: the form fields; ``season`` (END year) + ``stage`` become ``values[tournament]``.
        headers: extra request headers, merged over ``X-Requested-With`` / ``Origin`` / ``Referer``.
        **kwargs: ``timeout`` (seconds, default 60); anything else goes to ``curl_cffi``'s ``post``.

    Returns:
        The envelope ``{"status": "success", "data": ..., "errors": []}``, betting fields removed.

    Raises:
        ValueError: a route that needs a tournament got neither ``season`` nor ``tournament_id``, a
            malformed ``season`` / ``stage``, or HTTP 400 / 422.
        NoDataError: HTTP 404, an empty answer, an unknown season, or an answer for another game /
            player / tournament than the one requested.
        AssetFetchError: an error envelope, a second 307 after re-priming, a page without a sessid,
            any other non-2xx, a non-JSON body or a dead connection.
    """
    route = urlsplit(url).path.removeprefix("/rest")
    form = _form(route, params)
    timeout = kwargs.pop("timeout", TIMEOUT)
    site = _site(url)
    with _lock:
        session = _sessions.get(site)
        if session is None:
            http = _new_http()
            session = _sessions[site] = [http, _prime(http, site, timeout)]
        http = session[0]
        for attempt in (0, 1):
            _polite_wait(url)
            with _rt._transport_errors(url):
                resp = http.post(
                    url,
                    data={"sessid": session[1], **form},
                    headers={**_headers(site), **(headers or {})},
                    allow_redirects=False,
                    timeout=timeout,
                    **kwargs,
                )
            if getattr(resp, "status_code", None) != 307:
                break
            if attempt:
                raise AssetFetchError(f"{_rt._where(url)} answered 307 again after re-priming the session")
            session[1] = _prime(http, site, timeout)  # its _polite_wait keeps >= 1 s after the 307
    raw = _rt._json_text(url, getattr(resp, "status_code", None), getattr(resp, "text", "") or "")
    if isinstance(raw, dict) and "invalid_csrf" in str(raw.get("errors")):
        _sessions.pop(site, None)  # the sessid expired: the next call primes a fresh session
    _check(url, route, raw, form)
    return _wc._strip(raw)
