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
* **Retries.** Every attempt (the priming GET and the POST alike) runs inside
  :func:`sportsdataverse.hockey.euro_hockey_runtime._polite`: it starts at least one second after the
  host's previous answer finished. A dead connection, a 429 or a
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
* **Goalies.** ``/stat/players/`` with ``position=goalies`` fetches every page (``nav.max`` of the
  first) and returns them as one list sorted by games played, then wins (both descending), then name:
  the site orders goalies by goals-against average ascending and ignores sort fields. A partial list
  (a missing page, a row count that does not fit the page count, a player on two pages, more than
  :data:`MAX_PAGES` pages) raises :class:`~sportsdataverse.errors.AssetFetchError`.
* **Redactions.** ``fonbet`` (the betting-widget flag of ``game/text``) and ``bets`` are removed.

Status and decoding go through the shared vocabulary (404 -> ``NoDataError``, 400/422 ->
``ValueError``, any other non-2xx, a non-JSON body or a dead connection -> ``AssetFetchError``), with
at least one second from the end of one answer to the start of the next request to the host
(:func:`sportsdataverse.hockey.euro_hockey_runtime._polite`).
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as _eh
from sportsdataverse.hockey.khl import khl_webcaster_runtime as _wc

__all__: List[str] = []

SESSID_RE = re.compile(r'sessid":"([a-f0-9]{32})')
TIMEOUT = 60
#: Statuses worth another attempt, and the attempt budget per request (first try included).
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
ATTEMPTS = 3
_MAX_RETRY_AFTER = 120.0
TOURNAMENT = "values[tournament]"
POSITION = "values[amplua_full]"
#: Runaway-paging guard for the all-pages goalie list (khl.ru has about 7 pages of 20).
MAX_PAGES = 30
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
        delay = 2.0 * 2**attempt
        try:
            with _eh._polite(url):  # starts >= 1 s after the host's last answer finished
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
    _rt._check_status(home, status, "")  # a 204 here has no sessid to read: AssetFetchError below (R4.1b-d-1)
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


@_rt._no_content_is_empty
def _get(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[dict] = None,
    **kwargs: Any,
) -> Any:
    """POST a khl.ru REST route (the form is ``data``) within the primed session, then verify it.

    Args:
        url: the route URL (``https://www.khl.ru/rest/<a>/<b>/``; ``en.khl.ru`` gets its own session).
        params: the form fields when ``data`` is omitted, else extra query-string fields (the generated wrappers send an empty dict).
        data: the form fields; ``season`` (END year) + ``stage`` become ``values[tournament]``.
        headers: extra request headers, merged over ``X-Requested-With`` / ``Origin`` / ``Referer``.
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
    if data is None:  # a direct call: the params ARE the form fields
        form, query = _form(route, params), {}
    else:  # the generated wrappers: form in ``data``, caller ``params`` stay on the query string
        form, query = _form(route, data), {k: v for k, v in (params or {}).items() if v is not None}
    if query:
        kwargs["params"] = query
    timeout = kwargs.pop("timeout", TIMEOUT)
    if route == "/stat/players/" and form.get(POSITION) == "goalies":
        return _all_goalies(url, form, headers, timeout, kwargs)
    return _post(url, route, form, headers, timeout, kwargs)


def _post(
    url: str, route: str, form: Dict[str, str], headers: Optional[Dict[str, str]], timeout: Any, kwargs: Dict[str, Any]
) -> Any:
    """One checked POST of ``form`` to ``url`` within the primed session (see :func:`_get`)."""
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
                session[1] = _prime(http, site, timeout)  # its _polite starts >= 1 s after the 307 answered
                continue
            _rt._check_no_content(getattr(resp, "status_code", None))  # the data POST's 204 / 205 is {} (EX-SYNC-a)
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


def _goalie_key(row: Any) -> Tuple[float, float, str, str]:
    """Sort key: games played desc, wins desc, then name and player link (deterministic)."""

    def num(v: Any) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    cols = row.get("cols") if isinstance(row, dict) else None
    cols = cols if isinstance(cols, dict) else {}
    cells = row.get("first") if isinstance(row, dict) else None
    who = next((c for c in cells or [] if isinstance(c, dict) and c.get("link")), {})
    return (-num(cols.get("gp")), -num(cols.get("w")), str(who.get("val") or ""), str(who.get("link") or ""))


def _all_goalies(
    url: str, form: Dict[str, str], headers: Optional[Dict[str, str]], timeout: Any, kwargs: Dict[str, Any]
) -> Any:
    """Every page of ``position=goalies``, merged into page 1's envelope, sorted by gp desc, w desc, name.

    khl.ru orders goalies by goals-against average ascending and ignores every sort field, so page 1 is
    the goalies without a game. The page count is the body's ``nav.max``; paging stops there or on an
    empty page (without a ``nav.max``, on a short page). Never a partial result: more than
    :data:`MAX_PAGES` pages, a row count that does not fill ``nav.max`` pages (a page missing), or a
    player on two pages raises ``AssetFetchError``.
    """
    where = _rt._where(url)
    route = "/stat/players/"
    per_page = int(form.get("nav[count]") or 20)
    first: Any = None
    rows: List[Any] = []
    total: Optional[int] = None
    for page in range(1, MAX_PAGES + 2):
        if page > MAX_PAGES:
            raise AssetFetchError(f"{where}: still paging goalies after {MAX_PAGES} pages")
        try:
            raw = _post(url, route, {**form, "nav[page]": str(page)}, headers, timeout, dict(kwargs))
        except _rt._NoContent:  # a NoDataError subclass: caught first, so a 204 never reads as "past the end"
            if page == 1:
                raise  # the route's own answer: ``_get`` returns {} (an empty list)
            raise AssetFetchError(
                f"{where}: page {page} answered HTTP 204 mid-list; the list would be partial"
            ) from None
        except NoDataError:
            if page == 1:
                raise
            break  # an empty page past the end
        group = _dig(raw, ("data", "list", 0))
        got = group.get("list") or [] if isinstance(group, dict) else []
        nav = group.get("nav") if isinstance(group, dict) else None
        if total is None and isinstance(nav, dict):
            try:
                total = int(nav["max"])
            except (KeyError, TypeError, ValueError):
                total = None
            if total is not None and total > MAX_PAGES:
                raise AssetFetchError(f"{where}: {total} goalie pages, more than the {MAX_PAGES}-page guard")
        first = first if first is not None else raw
        rows.extend(got)
        if (total is not None and page >= total) or (total is None and len(got) < per_page):
            break
    if total is not None and not (total - 1) * per_page < len(rows) <= total * per_page:
        raise AssetFetchError(
            f"{where}: {len(rows)} goalies do not fill {total} pages of {per_page}; refusing a partial list"
        )
    links = [_goalie_key(r)[3] for r in rows]
    if len(set(links)) != len(links):
        raise AssetFetchError(f"{where}: a goalie is on two pages (the list shifted while paging)")
    merged = _dig(first, ("data", "list", 0))
    merged["list"] = sorted(rows, key=_goalie_key)
    merged["sort"] = {"gp": "desc", "w": "desc", "name": "asc"}
    return first
