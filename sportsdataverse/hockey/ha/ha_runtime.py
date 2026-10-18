"""Runtime for the generated HockeyAllsvenskan wrappers (``hockeyallsvenskan.se``, keyless).

The site serves its data through its own Next.js routes, and the flat-API codegen only
renders ``_get(url, params)`` GET calls. This module's :func:`_get` is the one getter the
YAML names (``getter_module``); it dispatches on the route:

* ``GET /pages/matcher`` (text/html): the current season's game list, decoded from the React
  Server Components flight data (:func:`extract_game_list`, the sdv-internal-refs
  ``hockeyallsvenskan/tools/capture.py`` rule).
* ``GET /api/game?slug=``: plain JSON.
* every other route is a ``POST`` with a JSON body. The wrapper's ``params`` are the body
  fields; they are laid over the route's default body (:data:`BODIES`, the bodies the refs
  capture sends), so a ``None`` keeps the default.

Every request waits on :func:`sportsdataverse.hockey.euro_hockey_runtime._polite_wait`
(at least 1 s per host) and goes through the ``_codegen_runtime`` decoders, so the error
vocabulary holds: 404 -> ``NoDataError`` (an unknown play-by-play game), 400 / 422 ->
``ValueError`` (a missing required field: the site answers ``{"error": "..."}`` with 400),
any other non-2xx, a dead connection, or an empty / non-JSON / truncated 200 ->
``AssetFetchError``.

**Seasons.** The public ``season`` is the END year (2026 = 2025/26). The site keys seasons
by START year (body ``season: "2025"`` = 2025/26), so :func:`body_season` sends
``season - 1``. ``None`` means the season in progress (:func:`current_season`: the END year
of the July-June season containing today). Stats and standings start in 2005/06
(END 2006); an earlier season raises ``NoDataError`` without a request.

**Refusals**, measured against the live site on 2026-10-12:

* ``categories`` outside the route's list answers 200 with zero rows, so it is checked
  before sending (:data:`CATEGORIES`, ``ValueError``); so are ``phase`` (:data:`PHASES`)
  and the leaderboard ``metric`` (a non-empty string).
* a body without rows (a season before 2005/06, an unknown slug or a pre-2025/26 one, a
  phase not played) -> ``NoDataError``.
* an error envelope in a 200 (``{"error": ...}``) -> ``AssetFetchError``.
* a truncated page: the paged tables must say ``total_pages == 1`` and ``count == rows``,
  the leaderboard ``totalPages == 1`` and ``totalCount == rows`` (type-strict) ->
  ``AssetFetchError``.
* ``/api/all-players`` pages unreliably league-wide (page edges overlap and skip), so it
  needs ``teamShortName`` (``ValueError`` without one) and refuses any answer that is not
  one complete page of that team's players.
* out of scope: games-table rows dated outside the requested season, ``/api/game`` rows for
  another slug, play-by-play events of a team that is not in the request, leaderboard rows
  of another ``metric`` (type-strict), schedule games of more than one season ->
  ``AssetFetchError``.

Unplayed games are NOT refused: the games table lists every scheduled game of the season in
progress (``decided_in == ""``, score 0-0, attendance null) and the parser flags them with
``is_played``.

Several bodies do not say what they answer (the stats tables carry no season, play-by-play
no game), so the runtime stamps the request into the body under ``_request``
(``{"season": ...}`` or ``{"statnet_game_number": ..., "home_statnet_id": ...,
"away_statnet_id": ...}``) and the parsers turn it into columns. The raw body
(``return_parsed=False``) carries that key too.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.dl_utils import _MAX_RETRY_AFTER, _SHARED_SESSION, _parse_retry_after
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime

__all__: list = []

HOST = "https://hockeyallsvenskan.se"
#: Game types (``phase``): regular season, playoffs, playout (relegation series).
PHASES = ("HA", "HAPlayoff", "HAkval")
#: ``categories`` (column sets) per stats route, from the stats pages' filter config. Anything else: zero rows.
CATEGORIES: Dict[str, Tuple[str, ...]] = {
    "/api/all-time-skaters-stats": (
        "summary",
        "points",
        "goals",
        "time_on_ice",
        "power_play",
        "penalty_kill",
        "equal_strength",
        "penalties",
        "plus_minus",
        "faceoffs",
        "offensive",
        "defensive",
        "shootouts",
        "corsi_fenwick_pdo",
    ),
    "/api/all-time-goalkeepers-stats": ("summary", "results", "game_strength", "defensive", "shootouts"),
    "/api/all-time-teams-stats": (
        "results",
        "goals",
        "power_play",
        "penalty_kill",
        "penalties",
        "offensive",
        "defensive",
        "faceoffs",
        "shootouts",
        "corsi_fenwick_pdo",
        "stoppage_reasons",
        "attendance",
    ),
}
#: First season (END year) with stats and standings: 2005/06.
FIRST_SEASON = 2006
#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"
#: Statuses a POST is retried on (2 s, 4 s backoff), as the refs capture does.
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

_FILTERS = {
    "league": "HA",
    "phase": "HA",
    "team_side": "all",
    "team": "all",
    "against_team": "all",
    "all_results": "all",
    "game_status": "all",
    "decided_in": "all",
}
#: Default JSON body per POST route (the refs ``capture.ROUTES`` bodies, minus ``season``).
BODIES: Dict[str, Dict[str, Any]] = {
    "/api/play-by-play": {},
    "/api/all-time-games-stats": {**_FILTERS, "categories": "all", "page": 1, "page_size": 1000},
    "/api/league-standings-all-time": {"league": "HA", "phase": "HA", "location": "all"},
    "/api/all-time-skaters-stats": {
        **_FILTERS,
        "categories": "summary",
        "position": "all",
        "ages": "all",
        "page": 1,
        "page_size": 1000,
    },
    "/api/all-time-goalkeepers-stats": {**_FILTERS, "categories": "summary", "page": 1, "page_size": 1000},
    "/api/all-time-teams-stats": {**_FILTERS, "categories": "results"},
    "/api/player-leaderboard": {
        "league": "HA",
        "team": None,
        "phase": "HA",
        "metric": "TP",
        "isGoalkeeper": False,
        "page": 1,
        "pageSize": 100,
    },
    "/api/all-players": {
        "scope": "league",
        "orderBy": "familyName",
        "teamShortName": None,
        "leagueShortName": "HA",
        "searchQuery": "",
        "page": 1,
        "pageSize": 100,
    },
}
#: Routes whose body takes a ``season`` (START year on the wire).
SEASONED = frozenset(
    {
        "/api/all-time-games-stats",
        "/api/league-standings-all-time",
        "/api/all-time-skaters-stats",
        "/api/all-time-goalkeepers-stats",
        "/api/all-time-teams-stats",
    }
)
_PAGED = frozenset({"/api/all-time-games-stats", "/api/all-time-skaters-stats", "/api/all-time-goalkeepers-stats"})
_HEADERS = {"User-Agent": "sportsdataverse-py", "Accept": "application/json"}
_SESSION = _SHARED_SESSION
_GAME_LIST_KEY = re.compile(r'"(games\.game-calendar-[^"]*)",')
_PUSH = re.compile(r'self\.__next_f\.push\(\[1,("(?:[^"\\]|\\.)*")\]\)')


# --- seasons ------------------------------------------------------------------------------------


def current_season(today: Optional[date] = None) -> int:
    """END year of the July-June season containing ``today`` (2026-10-12 -> 2027)."""
    today = today or date.today()
    return today.year + 1 if today.month >= 7 else today.year


def body_season(season: Optional[int], today: Optional[date] = None) -> Tuple[int, str]:
    """``(END year, wire START year)`` for a public ``season`` (``2026`` -> ``(2026, "2025")``).

    Raises:
        NoDataError: the season is before 2005/06 (:data:`FIRST_SEASON`); nothing is sent.
        ValueError: ``season`` is not an integer year.
    """
    end = current_season(today) if season is None else season
    if isinstance(end, bool) or not isinstance(end, int):
        if not (isinstance(end, str) and end.isdigit()):
            raise ValueError(f"season must be an END year such as 2026; got {season!r}")
        end = int(end)
    if end < FIRST_SEASON:
        raise NoDataError(f"HockeyAllsvenskan stats start in 2005/06 (season {FIRST_SEASON}); got season {end}")
    return end, str(end - 1)


# --- the HTML schedule page ---------------------------------------------------------------------


def flight_text(page: str) -> str:
    """The RSC flight data of a Next.js page: every ``self.__next_f.push([1,"..."])`` string literal,
    decoded and concatenated in document order (an element can span chunks).

    Raises:
        AssetFetchError: no flight data, or a chunk that is not a JSON string literal.
    """
    out = []
    for lit in _PUSH.findall(page):
        try:
            out.append(json.loads(lit))
        except ValueError:
            raise AssetFetchError("hockeyallsvenskan.se/pages/matcher: undecodable __next_f chunk") from None
    if not out:
        raise AssetFetchError("hockeyallsvenskan.se/pages/matcher: no self.__next_f.push flight data in the page")
    return "".join(out)


def extract_game_list(page: str) -> Dict[str, Any]:
    """Props of the ``games.game-calendar-*`` RSC element: ``{season, team, games: [...], ...}``.

    In the flight text the element is ``["$","$L4c","games.game-calendar-265-0",{props}]`` (the
    ``$L4c`` / ``-265-0`` parts vary between deploys); the props are decoded right after the key.

    Raises:
        AssetFetchError: the page has no such element, or its props are not an object with a
            ``games`` list.
    """
    text = flight_text(page)
    m = _GAME_LIST_KEY.search(text)
    if not m:
        raise AssetFetchError("hockeyallsvenskan.se/pages/matcher: no games.game-calendar element in the page")
    try:
        props, _ = json.JSONDecoder().raw_decode(text, m.end())
    except ValueError:
        raise AssetFetchError("hockeyallsvenskan.se/pages/matcher: game-calendar props are not JSON") from None
    if not isinstance(props, dict) or not isinstance(props.get("games"), list):
        raise AssetFetchError("hockeyallsvenskan.se/pages/matcher: game-calendar props carry no games list")
    return props


def _get_page(url: str, **kwargs: Any) -> Dict[str, Any]:
    kwargs.setdefault("session", euro_hockey_runtime.SESSION)  # waits per request, retries included
    with _rt._transport_errors(url):
        resp = _rt.download(url=url, **kwargs)
    text = _rt._text_body(resp, url)
    ctype = (getattr(resp, "headers", None) or {}).get("content-type", "")
    if "html" not in ctype.lower():
        raise AssetFetchError(f"{_rt._where(url)} answered {ctype or 'no content type'}, not text/html")
    return extract_game_list(text)


# --- POST ---------------------------------------------------------------------------------------


def _post(url: str, json: Any = None, timeout: float = 30, attempts: int = 3, **_: Any) -> Any:
    """POST ``json`` with retries (2 s, 4 s) on :data:`RETRY_STATUSES` and transport errors.

    Each attempt waits on the shared throttle. A ``Retry-After`` header on a retried status
    (429 and 503 alike) lengthens the wait to what the server asks, capped at
    ``dl_utils._MAX_RETRY_AFTER``. Any other status returns at once (the caller decodes it
    under the error vocabulary); the last transport error is re-raised.
    """
    for i in range(attempts):
        euro_hockey_runtime._polite_wait(url)
        retry_after = None
        try:
            resp = _SESSION.post(url, json=json, headers=_HEADERS, timeout=timeout)
        except OSError:  # requests.RequestException
            if i == attempts - 1:
                raise
        else:
            if resp.status_code not in RETRY_STATUSES or i == attempts - 1:
                return resp
            header = (getattr(resp, "headers", None) or {}).get("Retry-After")
            retry_after = _parse_retry_after(str(header)) if header else None
        time.sleep(max(2.0 * 2**i, min(retry_after or 0.0, _MAX_RETRY_AFTER)))
    raise AssertionError("unreachable")  # pragma: no cover


def build_body(path: str, params: Optional[Mapping[str, Any]], today: Optional[date] = None) -> Tuple[Dict, Dict]:
    """``(JSON body, request stamp)`` for a POST route: the route's default body, overlaid with the
    non-``None`` params, the END-year season mapped to the START-year wire key, and the
    enumerated fields checked.

    Raises:
        ValueError: an unknown route, ``categories`` / ``phase`` outside its list, a leaderboard
            without a ``metric``, or an ``/api/all-players`` call without ``teamShortName``.
        NoDataError: a season before 2005/06.
    """
    if path not in BODIES:
        raise ValueError(f"not a HockeyAllsvenskan POST route: {path!r}")
    body = {**BODIES[path], **{k: v for k, v in (params or {}).items() if v is not None}}
    stamp: Dict[str, Any] = {}
    if path in SEASONED:
        stamp["season"], body["season"] = body_season(body.get("season"), today)
    if "phase" in body and body["phase"] not in PHASES:
        raise ValueError(f"phase must be one of {PHASES!r}; got {body['phase']!r}")
    allowed = CATEGORIES.get(path)
    if allowed and body.get("categories") not in allowed:
        raise ValueError(
            f"categories must be one of {allowed!r}; got {body.get('categories')!r} (anything else: 0 rows)"
        )
    if path == "/api/player-leaderboard" and not _nonempty_str(body.get("metric")):
        raise ValueError(f"metric must be a non-empty string such as 'TP'; got {body.get('metric')!r}")
    if path == "/api/all-players" and not _nonempty_str(body.get("teamShortName")):
        raise ValueError(
            "team is required: /api/all-players pages unreliably league-wide, so ask one team at a time "
            "(its CMS short name, e.g. 'AIK', 'MORA', 'ÖIK')"
        )
    if path == "/api/play-by-play":
        body = {k: (str(v) if k == "statNetGameNumber" else v) for k, v in body.items()}
        stamp = {
            "statnet_game_number": body.get("statNetGameNumber"),
            "home_statnet_id": body.get("homeStatNetId"),
            "away_statnet_id": body.get("awayStatNetId"),
        }
    return body, stamp


# --- checks -------------------------------------------------------------------------------------


def _nonempty_str(x: Any) -> bool:
    return type(x) is str and x != ""


def _is_int(x: Any, value: Optional[int] = None) -> bool:
    """Type-strict integer test: ``True`` and ``1.0`` are not ``1``."""
    return type(x) is int and (value is None or x == value)


def _rows(body: Any, key: str) -> List[Dict[str, Any]]:
    """``body[key]`` as a list of objects; ``[]`` when missing or empty; refuses any other shape."""
    rows = body.get(key) if isinstance(body, dict) else None
    if rows is None or rows == [] or rows == {}:
        return []
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise AssetFetchError(f"{key} is not a list of objects ({type(rows).__name__})")
    return rows


def _season_window(end: int) -> Tuple[date, date]:
    return date(end - 1, 7, 1), date(end, 6, 30)


def _check(path: str, body: Any, request: Mapping[str, Any], stamp: Mapping[str, Any]) -> None:
    """Raise unless ``body`` is a complete, in-scope answer to the request (see the module docstring)."""
    if not isinstance(body, dict):
        raise AssetFetchError(f"answered {type(body).__name__}, not an object")
    if body.get("error"):
        raise AssetFetchError(f"answered an error envelope: {str(body.get('error'))[:120]}")
    if path == "/pages/matcher":
        games = _rows(body, "games")
        if not games:
            raise NoDataError("the schedule page lists no games")
        seasons = {g.get("season") for g in games}
        if len(seasons) != 1 or not all(_nonempty_str(s) for s in seasons):
            raise AssetFetchError(f"the schedule mixes seasons {sorted(map(repr, seasons))[:3]}")
        if not all(_nonempty_str(g.get("slug")) and _nonempty_str(g.get("statNetGameNumber")) for g in games):
            raise AssetFetchError("a schedule game has no slug or statNetGameNumber")
        return
    if path == "/api/game":
        rows = _rows(body, "data")
        if not rows:
            raise NoDataError(f"no game record for slug {request.get('slug')!r} (records start in 2025/26)")
        if not all(r.get("slug") == request.get("slug") for r in rows):
            raise AssetFetchError(f"rows outside slug={request.get('slug')!r}")
        return
    if path == "/api/play-by-play":
        events = [e for p in _rows(body, "game_events") for e in _rows(p, "Events")]
        if not events:
            raise NoDataError(f"no play-by-play events for game {request.get('statNetGameNumber')!r}")
        teams = {request.get("homeStatNetId"), request.get("awayStatNetId")}
        bad = [e.get("type") for e in events if (e.get("team") or {}).get("statNetId") not in teams]
        if bad:
            raise AssetFetchError(f"{len(bad)} events of a team outside {sorted(map(str, teams))}")
        return
    key = {
        "/api/league-standings-all-time": "standings",
        "/api/all-time-teams-stats": "data",
        "/api/player-leaderboard": "players",
        "/api/all-players": "players",
    }.get(path, "results")
    rows = _rows(body, key)
    if not rows:
        raise NoDataError("answered no rows (a season or phase without data, or an unknown filter value)")
    if path in _PAGED and not (_is_int(body.get("total_pages"), 1) and _is_int(body.get("count"), len(rows))):
        raise AssetFetchError(
            f"truncated page: {len(rows)} of {body.get('count')!r} rows, {body.get('total_pages')!r} pages"
        )
    if path == "/api/all-time-games-stats":
        lo, hi = _season_window(stamp["season"])
        for r in rows:
            try:
                d = date.fromisoformat(r["Date"])
            except (KeyError, TypeError, ValueError):
                d = None
            if d is None or not lo <= d <= hi:
                raise AssetFetchError(f"a game dated {r.get('Date')!r} is outside season {stamp['season']}")
            if type(r.get("decided_in")) is not str:
                raise AssetFetchError(f"a game without a string decided_in ({r.get('decided_in')!r})")
    elif path == "/api/player-leaderboard":
        if not (_is_int(body.get("totalPages"), 1) and _is_int(body.get("totalCount"), len(rows))):
            raise AssetFetchError(
                f"truncated board: {len(rows)} of {body.get('totalCount')!r} rows, {body.get('totalPages')!r} pages"
            )
        metric = request.get("metric")
        if not all(type(r.get("metric")) is str and r["metric"] == metric for r in rows):
            raise AssetFetchError(f"rows outside metric={metric!r}")
    elif path == "/api/all-players":
        page = body.get("pagination") or {}
        if not (_is_int(page.get("pageCount"), 1) and _is_int(page.get("total"), len(rows))):
            raise AssetFetchError(
                f"multi-page directory: {len(rows)} of {page.get('total')!r} players, {page.get('pageCount')!r} "
                "pages; league-wide paging overlaps and skips, so only one complete page is accepted"
            )
        team = request.get("teamShortName")
        if not all((r.get("team") or {}).get("shortName") == team for r in rows):
            raise AssetFetchError(f"rows outside teamShortName={team!r}")


def _stamped(body: Dict[str, Any], stamp: Mapping[str, Any]) -> Dict[str, Any]:
    if stamp:
        body[REQUEST_KEY] = dict(stamp)
    return body


def _guard(url: str, fn: Callable[[], None]) -> None:
    """Run a check, prefixing its message with ``host/path``."""
    try:
        fn()
    except (AssetFetchError, NoDataError) as exc:
        raise type(exc)(f"{_rt._where(url)}: {exc}") from None


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """Fetch one HockeyAllsvenskan route (GET page / GET JSON / POST JSON by path), checked and stamped.

    Args:
        url: fully-built request URL.
        params: the wrapper's parameters: the query string of ``/api/game``, the JSON body
            fields of a POST route (``None`` keeps the route default), unused by ``/pages/matcher``.
        **kwargs: forwarded to the transport (``timeout``).

    Returns:
        The decoded JSON body (the schedule page's game-list props for ``/pages/matcher``), with the
        request stamped under :data:`REQUEST_KEY` where the body does not say what it answers.

    Raises:
        NoDataError: the site answered 404 or no rows, or the season is before 2005/06.
        ValueError: a field outside its enumerated set, a missing team for ``/api/all-players``, or
            HTTP 400 / 422 (a missing required field).
        AssetFetchError: any other non-2xx, a connection failure after retries, an empty /
            non-JSON / error-shaped / truncated / out-of-scope answer.
    """
    path = urlsplit(url).path.rstrip("/") or "/"
    if path == "/pages/matcher":
        body = _get_page(url, **kwargs)
        _guard(url, lambda: _check(path, body, {}, {}))
        return body
    if path == "/api/game":
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        body = euro_hockey_runtime._get(url, clean, **kwargs)
        _guard(url, lambda: _check(path, body, clean, {}))
        return body
    request, stamp = build_body(path, params)
    body = _rt._download_json(_post, url, json=request, **kwargs)
    _guard(url, lambda: _check(path, body, request, stamp))
    return _stamped(body, stamp)
