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
* **Retries.** Every attempt (the priming GET and the POST alike) waits for its host slot first
  (:func:`sportsdataverse.hockey.euro_hockey_runtime._polite_wait`). A dead connection, a 429 or a
  5xx is retried up to :data:`ATTEMPTS` times in all, with an exponential back-off that honours a
  ``Retry-After`` header (capped at two minutes); the last failure raises
  :class:`~sportsdataverse.errors.AssetFetchError`.
* **307 = rotated WAF cookie.** A POST that answers 307 re-primes the session and is retried once; a
  second 307 raises :class:`~sportsdataverse.errors.AssetFetchError`.
* **Errors in a 200.** Bad or missing fields still answer HTTP 200
  ``{"status": "error", "data": null, "errors": [{"message": ...}]}`` (``code: invalid_csrf`` for a
  stale ``sessid``). ``status != "success"`` or a non-empty ``errors`` raises ``AssetFetchError``.
  An ``invalid_csrf`` first drops the cached session, primes a fresh one and retries the POST once;
  only a second ``invalid_csrf`` raises.
* **Season -> tournament.** No route takes a year. ``season`` (the END year, 2026 = 2025/26) and
  ``stage`` (``regular`` / ``playoff``) become ``values[tournament]`` (the ``tnId``) through the
  webcaster season table (:func:`sportsdataverse.hockey.khl.khl_webcaster_runtime.stage_for`, cached
  in-process), unless ``values[tournament]`` is given (``tournament_id``).
* **Refusals.** An empty ``data`` (``players/stats`` without a player answers ``success`` with
  ``data: []``) or an empty row container raises :class:`~sportsdataverse.errors.NoDataError`. A
  WRONG answer is a failed fetch, :class:`~sportsdataverse.errors.AssetFetchError`: a game page that
  names another game or tournament than the one requested, a player page of another player, or a
  schedule whose rows are not (90 %+) of the requested tournament (the All-Star games ride along
  under their own tournament id, so 100 % is not required).
* **Redactions.** ``fonbet`` (the betting-widget flag of ``game/text``) and ``bets`` are removed.

Status and decoding go through the shared vocabulary (404 -> ``NoDataError``, 400/422 ->
``ValueError``, any other non-2xx, a non-JSON body or a dead connection -> ``AssetFetchError``), with
at least one second between two requests to the host
(:func:`sportsdataverse.hockey.euro_hockey_runtime._polite_wait`).
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey.euro_hockey_runtime import _polite_wait
from sportsdataverse.hockey.khl import khl_webcaster_runtime as _wc

__all__: List[str] = []

SESSID_RE = re.compile(r'sessid":"([a-f0-9]{32})')
TIMEOUT = 60
#: Statuses worth another attempt, and the attempt budget per request (first try included).
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
ATTEMPTS = 3
_MAX_RETRY_AFTER = 120.0
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


def _retry_after(resp: Any) -> float:
    try:
        return min(float((getattr(resp, "headers", None) or {}).get("retry-after", 0)), _MAX_RETRY_AFTER)
    except (TypeError, ValueError):  # an HTTP-date Retry-After falls back to the exponential delay
        return 0.0


def _send(url: str, call: Callable[[], Any]) -> Any:
    """``call()`` (one request to ``url``), each attempt throttled, retried on a dead connection / 429 / 5xx.

    Returns the last response (a retryable status only once the budget is spent); a connection that
    fails every attempt raises :class:`~sportsdataverse.errors.AssetFetchError`.
    """
    for attempt in range(ATTEMPTS):
        _polite_wait(url)
        delay = 2.0 * 2**attempt
        try:
            resp = call()
        except OSError as exc:  # curl_cffi's RequestException subclasses OSError
            if attempt == ATTEMPTS - 1:
                raise AssetFetchError(
                    f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}"
                ) from exc
        else:
            if getattr(resp, "status_code", None) not in RETRY_STATUSES or attempt == ATTEMPTS - 1:
                return resp
            delay = max(delay, _retry_after(resp))
        time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def _prime(http: Any, site: str, timeout: Any) -> str:
    """GET ``site/`` (following the cookie-setting 307) and return the page's ``sessid``."""
    home = site + "/"
    resp = _send(home, lambda: http.get(home, timeout=timeout))
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
            raise AssetFetchError(
                f"{where}: asked for game {game} of tournament {tn}, answered game {got[0]} of {got[1]}"
            )
    if route == "/players/stats/" and str(page.get("id")) != form.get("values[id]"):
        raise AssetFetchError(f"{where}: asked for player {form.get('values[id]')}, answered player {page.get('id')}")
    if route == "/players/profile/":
        link = str(_dig(page, ("DATA", "pagelink")) or "")
        if f"/players/{form.get('values[id]')}/" != link:
            raise AssetFetchError(
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
        if not ids:
            raise NoDataError(f"{where}: khl.ru answered no games for tournament {tn}")
        if sum(i == tn for i in ids) / len(ids) < 0.9:
            raise AssetFetchError(
                f"{where}: asked for tournament {tn}, but most of its games are of another tournament"
            )


def _get(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[Dict[str, str]] = None,
    *,
    body: Optional[dict] = None,
    **kwargs: Any,
) -> Any:
    """POST a khl.ru REST route (the form is ``body``) within the primed session, then verify it.

    Args:
        url: the route URL (``https://www.khl.ru/rest/<a>/<b>/``; ``en.khl.ru`` gets its own session).
        params: the form fields when ``body`` is not given (the pre-``body:`` calling convention).
        headers: extra request headers, merged over ``X-Requested-With`` / ``Origin`` / ``Referer``.
        body: the form fields the generated wrappers send; ``season`` (END year) + ``stage`` become
            ``values[tournament]``.
        **kwargs: ``timeout`` (seconds, default 60); anything else goes to ``curl_cffi``'s ``post``.

    Returns:
        The envelope ``{"status": "success", "data": ..., "errors": []}``, betting fields removed.

    Raises:
        ValueError: a route that needs a tournament got neither ``season`` nor ``tournament_id``, a
            malformed ``season`` / ``stage``, or HTTP 400 / 422.
        NoDataError: HTTP 404, an empty answer or an unknown season.
        AssetFetchError: an answer for another game / player / tournament than the one requested, an
            error envelope (a second ``invalid_csrf`` included), a second 307 after re-priming, a page
            without a sessid, any other non-2xx (429 / 5xx once the retries are spent), a non-JSON body
            or a dead connection.
    """
    route = urlsplit(url).path.removeprefix("/rest")
    form = _form(route, params if body is None else body)
    timeout = kwargs.pop("timeout", TIMEOUT)
    site = _site(url)
    # ponytail: one global lock (sessions, re-primes and retries all serialise); per-site locks if it ever matters
    with _lock:
        session = _sessions.get(site)
        if session is None:
            http = _new_http()
            session = _sessions[site] = [http, _prime(http, site, timeout)]
        for attempt in (0, 1):  # one recovery: a re-prime after a 307 or after an invalid_csrf
            http, sessid = session
            resp = _send(
                url,
                lambda: http.post(
                    url,
                    data={"sessid": sessid, **form},
                    headers={**_headers(site), **(headers or {})},
                    allow_redirects=False,
                    timeout=timeout,
                    **kwargs,
                ),
            )
            if getattr(resp, "status_code", None) == 307:
                if attempt:
                    raise AssetFetchError(f"{_rt._where(url)} answered 307 again after re-priming the session")
                session[1] = _prime(http, site, timeout)  # its _polite_wait keeps >= 1 s after the 307
                continue
            raw = _rt._json_text(url, getattr(resp, "status_code", None), getattr(resp, "text", "") or "")
            if not (isinstance(raw, dict) and "invalid_csrf" in str(raw.get("errors"))):
                break
            _sessions.pop(site, None)  # the sessid expired: drop the session, prime a fresh one, retry once
            if attempt:
                break  # a second invalid_csrf: _check raises it; the next call primes afresh
            http = _new_http()
            session = _sessions[site] = [http, _prime(http, site, timeout)]
    _check(url, route, raw, form)
    return _wc._strip(raw)
