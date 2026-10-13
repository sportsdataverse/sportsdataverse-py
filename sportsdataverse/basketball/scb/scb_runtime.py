"""Runtime for the generated BBL / DBBL wrappers (the SCB API on two hosts, one set of routes).

Both leagues run the same API: ``api.basketball-bundesliga.de`` (BBL, men) and
``api-1.dbbl.scb.world`` (DBBL, women). The generated wrappers are written against the BBL host;
a ``league`` argument (``"bbl"`` or ``"dbbl"``) reaches :func:`_get` as a parameter, which pops it
and swaps the host. Measured against the live API (sdv-internal-refs ``scb/``, 2026-10-12):

* **Two headers on every request.** ``x-api-key: publicWebUser`` (a constant) and ``x-api-secret``,
  which is the ``"key":"..."`` value embedded in an https://www.easycredit-bbl.de/ page, reversed,
  with every ``:`` turned into ``-``. The key rotates, so it is scraped once per process and again
  after a 401 / 403 (at most one re-derive per request). **Neither value is ever logged, printed,
  returned or put in an error message**; an echo of either in a body is replaced with
  ``<redacted>`` before the body is parsed. A page value that is not 32-256 characters of
  ``[A-Za-z0-9:_-]`` is not the API key (a React ``key``) and is skipped.
* **Throttle.** Every attempt, key-page fetch and ``download()`` retry included, goes through
  :data:`~sportsdataverse.basketball.basketball_runtime.SESSION`: at least 1 s per host, and
  ``Retry-After`` on a 429 / 503 is honoured by ``dl_utils.download``. A 403 is not retried by the
  transport (it is an auth failure here, see above).
* **Seasons.** The API's ``seasonId`` is the START year (2026 = 2026-27); the public ``season``
  argument is the END year (2026 = 2025-26), mapped by :func:`season_id` (``season - 1``) before the
  URL is built.
* **Paging.** ``/games`` and ``/referees`` return ``{items, totalPages, currentPage, pageSize}``.
  The wrappers ask for page 1 with a large ``pageSize`` (the whole season), and a body whose
  ``totalPages`` is not 1 is refused (:class:`AssetFetchError`: raise ``page_size``) because a
  partial season would look complete.
* **Refusals.**

  * 404, an empty 204 (a game before 2002-03 has no box score; an unknown season-player), a 400
    ``"not a valid gameId"``, ``[]``, a page with no ``items``, an all-empty bracket: :class:`NoDataError`.
  * Any other 400 / 422: :class:`ValueError` (the request is wrong).
  * 401 / 403 after one re-derive, any other non-2xx, an HTML or non-JSON body, an error envelope
    (``{"status": "error", ...}`` or a bare string) and a truncated page: :class:`AssetFetchError`.
  * **Wrong scope** is an :class:`AssetFetchError`, never an empty frame: a game, club, player or
    season body whose id is not the one asked for, a ``/games`` or ``/standings`` row of another
    season, and any ``competition`` that belongs to the other host (the BBL secret also unlocks
    DBBL, so a mis-routed request answers with the wrong league's rows).
  * A box score of a game that has not started is :class:`NoDataError`; one still in play
    (``status`` ``LIVE``) is an :class:`AssetFetchError` because it is not final. ``POST`` (ended,
    not yet official) and ``OFFICIAL`` are returned.

* **Stamped request.** Game ids repeat between the hosts, and the player / team / box-score bodies
  do not say which host or season they answer, so the runtime adds the request to the body under
  ``_request`` (``{"league": "bbl", "season": 2026, ...}``; on each row of a list body, and at the
  top of an object body). The parsers turn it into ``league`` / ``season`` columns; the raw body
  (``return_parsed=False``) carries the key too.
"""

from __future__ import annotations

import re
import threading
from typing import Any, Dict, FrozenSet, Iterator, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import NOREDIRECT_SESSION, SESSION
from sportsdataverse.dl_utils import _RETRYABLE_STATUS, download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["HOSTS", "LEAGUES", "REQUEST_KEY", "season_id"]

#: The two hosts, by the ``league`` argument.
HOSTS: Dict[str, str] = {
    "bbl": "https://api.basketball-bundesliga.de",
    "dbbl": "https://api-1.dbbl.scb.world",
}
LEAGUES: Tuple[str, ...] = tuple(HOSTS)
#: The competitions each host may answer with; the other host's competition is a wrong-scope body.
HOST_COMPETITIONS: Dict[str, FrozenSet[str]] = {
    "bbl": frozenset({"BBL", "BBL_CUP", "ALLSTAR", "CHAMPIONS_CUP"}),
    "dbbl": frozenset({"DBBL", "DBBL_CUP"}),
}
#: The page whose embedded ``"key"`` value the secret is derived from.
KEY_PAGE = "https://www.easycredit-bbl.de/"
API_KEY = "publicWebUser"
#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"
REDACTED = "<redacted>"
#: Retried by ``download()``: the package default minus 403 (here a refused secret, handled below).
RETRY_STATUSES = frozenset(_RETRYABLE_STATUS) - {403}
#: Statuses that mean "the derived secret was refused".
_AUTH_FAILED = frozenset({401, 403})

_KEY_RE = re.compile(r'"key":"([^"]+)"')
_KEY_OK = re.compile(r"[A-Za-z0-9:_-]{32,256}")
_MAX_ROWS_DEPTH = 50

_lock = threading.Lock()
_secrets: Dict[str, str] = {}


def season_id(season: Any) -> Optional[int]:
    """The API's ``seasonId`` (START year) for an END-year ``season``; ``None`` stays ``None``.

    Args:
        season: the season END year (2026 = 2025-26), an ``int`` or a digit string.

    Returns:
        ``season - 1`` (2026 -> 2025, the 2025-26 season).

    Raises:
        ValueError: ``season`` is not a positive integer.
    """
    if season is None:
        return None
    if isinstance(season, bool) or not (isinstance(season, int) or (isinstance(season, str) and season.isdigit())):
        raise ValueError(f"season must be an END year such as 2026 (2025-26); got {season!r}")
    year = int(season)
    if year < 1:
        raise ValueError(f"season must be an END year such as 2026 (2025-26); got {season!r}")
    return year - 1


# --- credentials --------------------------------------------------------------------------------


def _derive_secret(key: str) -> str:
    """The ``x-api-secret`` for a page key: reversed, every ``:`` turned into ``-``."""
    if not _KEY_OK.fullmatch(key or ""):
        raise AssetFetchError("the easycredit-bbl.de page key has an unexpected length or character set")
    return key[::-1].replace(":", "-")


def _page_key(html: str) -> str:
    """The first ``"key":"..."`` value on the page that looks like the API key (not a React ``key``)."""
    for m in _KEY_RE.finditer(html or ""):
        if _KEY_OK.fullmatch(m.group(1)):
            return m.group(1)
    raise AssetFetchError(f"no API-shaped page key on {KEY_PAGE}: the page layout changed")


def _fetch_key() -> str:
    """Scrape the page key; ``SESSION`` throttles this request like any other (it carries no credential header)."""
    resp = download(url=KEY_PAGE, headers={"Accept": "text/html"}, session=SESSION, retry_statuses=RETRY_STATUSES)
    status = getattr(resp, "status_code", None)
    if not (isinstance(status, int) and 200 <= status < 300):
        raise AssetFetchError(f"{_rt._where(KEY_PAGE)} answered HTTP {status}: the API key could not be read")
    return _page_key(getattr(resp, "text", "") or "")


def _headers(refresh: bool = False) -> Dict[str, str]:
    """The two auth headers; the key is scraped once per process, and again when ``refresh``."""
    with _lock:
        if refresh or "key" not in _secrets:
            key = _fetch_key()
            _secrets["key"], _secrets["secret"] = key, _derive_secret(key)
        return {"x-api-key": API_KEY, "x-api-secret": _secrets["secret"], "Accept": "application/json"}


def _redact(text: str) -> str:
    """``text`` with the page key and the derived secret (if known) replaced by ``<redacted>``."""
    for value in (_secrets.get("key"), _secrets.get("secret")):
        if value:
            text = text.replace(value, REDACTED)
    return text


# --- request ------------------------------------------------------------------------------------

_ROUTES: Tuple[Tuple[str, "re.Pattern[str]"], ...] = (
    ("games_recent", re.compile(r"^/games/recent/?$")),
    ("game_stats", re.compile(r"^/games/([^/]+)/stats/?$")),
    ("game", re.compile(r"^/games/([^/]+)/?$")),
    ("games", re.compile(r"^/games/?$")),
    ("playoff_tree", re.compile(r"^/seasons/(\d+)/playoff-tree/?$")),
    ("player_statistics", re.compile(r"^/seasons/(\d+)/player-statistics/?$")),
    ("team_statistics", re.compile(r"^/seasons/(\d+)/team-statistics/?$")),
    ("season", re.compile(r"^/seasons/(\d+)/?$")),
    ("seasons", re.compile(r"^/seasons/?$")),
    ("standings", re.compile(r"^/standings/?$")),
    ("season_player", re.compile(r"^/season-players/([^/]+)/?$")),
    ("team", re.compile(r"^/teams/([^/]+)/?$")),
    ("referees", re.compile(r"^/referees/?$")),
    ("transfers_teams", re.compile(r"^/transfers/teams/?$")),
    ("transfers", re.compile(r"^/transfers/?$")),
    ("records_per_game", re.compile(r"^/records/per-game/(\d+)/?$")),
    ("records_season_averages", re.compile(r"^/records/season-averages/?$")),
)


def _route(url: str) -> Tuple[str, Optional[str]]:
    """``(route name, the path id)`` of a request path; ``("", None)`` for a path outside the table."""
    path = urlsplit(url).path
    for name, pattern in _ROUTES:
        m = pattern.match(path)
        if m:
            return name, (m.group(1) if m.groups() else None)
    return "", None


def _swap_host(url: str, league: str) -> str:
    base = HOSTS["bbl"]
    return HOSTS[league] + url[len(base) :] if url.startswith(base) else url


def _send(url: str, params: Mapping[str, Any], **kwargs: Any) -> Any:
    """GET with the auth headers; one re-derive of the secret when the host refuses it. Returns the response."""
    kwargs.setdefault("retry_statuses", RETRY_STATUSES)
    for attempt in range(2):
        with _rt._transport_errors(url):
            resp = download(
                url=url,
                params=dict(params),
                headers=_headers(refresh=attempt == 1),
                session=NOREDIRECT_SESSION,
                **kwargs,
            )
        if getattr(resp, "status_code", None) not in _AUTH_FAILED:
            return resp
    raise AssetFetchError(
        f"{_rt._where(url)} refused the derived x-api-secret (HTTP {resp.status_code}) after a re-derive: "
        "the page key may have rotated, or the host blocks this address"
    )


def _competitions(node: Any, depth: int = 0) -> Iterator[str]:
    """Every ``competition`` string in a body; the walk stops at depth 50."""
    if depth > _MAX_ROWS_DEPTH:
        return
    if isinstance(node, dict):
        if isinstance(node.get("competition"), str):
            yield node["competition"]
        for v in node.values():
            yield from _competitions(v, depth + 1)
    elif isinstance(node, list):
        for v in node:
            yield from _competitions(v, depth + 1)


def _is(value: Any, want: Any) -> bool:
    """Type-strict equality (``True == 1`` in Python): an ``int`` is compared to an ``int``, a ``str`` to a ``str``."""
    return type(value) is type(want) and value == want


def _error(url: str, body: Any) -> None:
    """Raise :class:`AssetFetchError` on one of the API's 200-with-an-error shapes."""
    where = _rt._where(url)
    if isinstance(body, str):
        raise AssetFetchError(f"{where} answered an error string: {body[:80]!r}")
    if isinstance(body, dict) and (
        body.get("status") == "error" or (body and set(body) <= {"status", "error", "message", "statusCode"})
    ):
        raise AssetFetchError(f"{where} answered an error envelope: {str(body)[:120]}")


def _is_empty(route: str, body: Any) -> bool:
    """No rows: ``[]``, a page without ``items``, a bracket or bucket set with nothing in it, a null object."""
    if isinstance(body, list):
        return not body
    if not isinstance(body, dict) or not body:
        return True
    if "items" in body:
        return not body["items"]
    if route == "games_recent":
        return not any(body.get(k) for k in ("present", "future", "past"))
    if route == "playoff_tree":
        return not (body.get("quarterFinals") or body.get("semiFinals") or body.get("finals"))
    return False


def _truncated(url: str, body: Any) -> None:
    """Refuse a paged body that is not the whole result (``totalPages`` must be exactly 1)."""
    if isinstance(body, dict) and "items" in body and not _is(body.get("totalPages"), 1):
        raise AssetFetchError(
            f"{_rt._where(url)}: truncated page (totalPages={body.get('totalPages')!r}, currentPage="
            f"{body.get('currentPage')!r}); raise page_size so the whole result fits one page"
        )


def _scope(url: str, route: str, path_id: Optional[str], params: Mapping[str, Any], league: str, body: Any) -> None:
    """Raise :class:`AssetFetchError` unless a body that names its scope answers the request."""
    where = _rt._where(url)
    seen = set(_competitions(body))
    if seen - HOST_COMPETITIONS[league]:
        raise AssetFetchError(
            f"{where}: out of scope: competition {sorted(seen - HOST_COMPETITIONS[league])} on the {league} host"
        )
    if route in ("games", "game", "game_stats", "standings") and not seen:
        raise AssetFetchError(f"{where}: out of scope: no competition field to check against the {league} host")
    rows: List[Any] = (
        body["items"] if isinstance(body, dict) and "items" in body else (body if isinstance(body, list) else [body])
    )
    asked_id = params.get("seasonId")
    start = int(asked_id) if asked_id is not None else None
    checks: List[Tuple[str, Any, Any]] = []  # (field, got, want), per row
    if route in ("games", "standings") and start is not None:
        checks = [("seasonId", r.get("seasonId") if isinstance(r, dict) else None, start) for r in rows]
    elif route == "game":
        checks = [("id", r.get("id") if isinstance(r, dict) else None, path_id) for r in rows]
    elif route == "game_stats":
        checks = [("gameId", r.get("gameId") if isinstance(r, dict) else None, path_id) for r in rows]
    elif route in ("season", "playoff_tree"):
        want = int(path_id or 0)
        key = "id" if route == "season" else "seasonId"
        checks = [(key, r.get(key) if isinstance(r, dict) else None, want) for r in rows]
    elif route == "team_statistics":
        want = int(path_id or 0)
        checks = [
            (
                "seasonTeam.seasonId",
                ((r.get("seasonTeam") or {}).get("seasonId") if isinstance(r, dict) else None),
                want,
            )
            for r in rows
        ]
    elif route in ("season_player", "team"):
        checks = [("id", r.get("id") if isinstance(r, dict) else None, path_id) for r in rows]
    elif route == "records_per_game":
        checks = [("seasonId", r.get("seasonId") if isinstance(r, dict) else None, path_id) for r in rows]
    for name, got, want in checks:
        if not _is(got, want):
            raise AssetFetchError(f"{where}: out of scope: {name}={got!r}, asked for {want!r}")


def _status_gate(url: str, route: str, body: Any) -> None:
    """A box score exists only once the game is over: PRE has nothing yet, LIVE is not final."""
    if route != "game_stats" or not isinstance(body, dict):
        return
    status = body.get("status")
    if status == "PRE":
        raise NoDataError(f"{_rt._where(url)}: the game has not started (status PRE), so it has no box score yet")
    if status == "LIVE":
        raise AssetFetchError(f"{_rt._where(url)}: the game is in play (status LIVE); its box score is not final")


def _stamp(body: Any, request: Mapping[str, Any]) -> Any:
    """Add the request under :data:`REQUEST_KEY` (on each row of a list body, at the top of an object)."""
    if isinstance(body, dict):
        body[REQUEST_KEY] = dict(request)
    elif isinstance(body, list):
        for row in body:
            if isinstance(row, dict):
                row[REQUEST_KEY] = dict(request)
    return body


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a BBL / DBBL route as JSON: authenticate, refuse the empty / error / out-of-scope answers, stamp the request.

    Args:
        url: the request URL built against the BBL host; with ``league="dbbl"`` the host is swapped.
        params: query-string parameters; ``None`` values are dropped. ``league`` (``"bbl"`` default or
            ``"dbbl"``) is read here and never sent.
        **kwargs: forwarded to ``dl_utils.download`` (``retry_statuses`` defaults to :data:`RETRY_STATUSES`).

    Returns:
        The parsed JSON body, with the request stamped under :data:`REQUEST_KEY`.

    Raises:
        NoDataError: 404, a 204, a 400 ``"not a valid gameId"``, a body without rows, or the box score of a
            game that has not started.
        ValueError: ``league`` is not ``"bbl"`` / ``"dbbl"``, or the host answered another 400 / 422.
        AssetFetchError: a refused secret (after one re-derive), any other non-2xx, an HTML / non-JSON / error-shaped
            body, a truncated page, a body for another game, season, club or league than the one asked for, or a
            box score still in play.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    league = clean.pop("league", "bbl")
    if league not in HOSTS:
        raise ValueError(f"league must be one of {LEAGUES!r}; got {league!r}")
    url = _swap_host(url, league)
    where = _rt._where(url)
    route, path_id = _route(url)
    resp = _send(url, clean, **kwargs)
    status = getattr(resp, "status_code", None)
    text = _redact(getattr(resp, "text", "") or "")
    if status == 204:
        raise NoDataError(f"{where} answered HTTP 204: no content for this id")
    if status == 400 and "not a valid" in text:
        raise NoDataError(f"{where} answered 400 {text.strip()[:60]}: no such record")
    body = _rt._json_text(url, status, text)
    _error(url, body)
    if _is_empty(route, body):
        raise NoDataError(f"{where}: the API answered no rows")
    _truncated(url, body)
    _scope(url, route, path_id, clean, league, body)
    _status_gate(url, route, body)
    request: Dict[str, Any] = {"league": league}
    start = clean.get(
        "seasonId",
        path_id
        if route in ("playoff_tree", "player_statistics", "team_statistics", "season", "records_per_game")
        else None,
    )
    if start is not None:
        request["season"] = int(start) + 1
    if "displayType" in clean:
        request["display_type"] = clean["displayType"]
    return _stamp(body, request)


def _build_query(qs: Dict[str, Any]) -> Dict[str, Any]:
    """The query as the API receives it: ``league`` picks the host and is never sent (docs example URLs)."""
    return {k: v for k, v in qs.items() if k != "league"}
