"""Runtime for the generated Liiga wrappers (``liiga.fi/api/v2``, keyless JSON).

Every request goes through the shared European hockey throttle
(:func:`sportsdataverse.hockey.euro_hockey_runtime._get`: at least 1 s per host, then the
package getter), so the error vocabulary holds: 404 -> ``NoDataError``, 400 / 422 ->
``ValueError``, any other non-2xx, a dead connection, or an empty or non-JSON 200 ->
``AssetFetchError``. On top of it, measured against the live API (sdv-internal-refs
``liiga/``, 2026-10-12):

* **A 500 is not retried.** Liiga answers an unknown ``tournament`` or ``dataType`` with
  HTTP 500 ``"Remote server error"``, every time, so a retry only repeats it. The 500 is
  dropped from the retry set and raises ``AssetFetchError`` at once. Both enums are
  checked before sending (``dataType`` by the wrappers, ``tournament`` here, since an
  omitted tournament is valid on some routes), so a typo never gets that far.
* **Error bodies arrive as 200s.** ``{"stats": "Remote server error"}`` is how
  ``/games/stats`` says the season has no such game: :class:`NoDataError`. A bare string
  body, the generic ``{"message": "Internal server error"}`` envelope or any other object
  whose top-level string names an error: :class:`AssetFetchError`.
* **A row-less body is "nothing here".** ``[]`` (a tournament not played that season, a
  shot map before 2016 or of the season in progress), ``{"teamStats": []}`` or an object
  whose values are all null raise :class:`NoDataError`.
* **Scope.** A body that names its season or game must be the one asked for, else
  :class:`AssetFetchError`: ``/games/{season}/{id}`` checks ``game.season`` and
  ``game.id``; ``/games``, ``/schedule`` and ``/gameweeks`` check ``season`` on every row
  when a season was sent; a player's game log checks that every game date falls inside
  the season (July to June). ``/games/stats`` and ``/shotmap`` carry neither, so they
  cannot be checked.

Seasons are END years natively (2026 = 2025-26), the sdv key, so no mapping happens.
Game ids repeat across seasons, and the box score, shot map, lineup-stats variant,
standings and player game log do not say which season (or game) they answer. The
runtime therefore stamps the request into the body under ``_request``
(``{"season": ..., "game_id": ..., "player_id": ...}``; on each row of a list body), and
the parsers turn it into ``season`` / ``game_id`` / ``player_id`` columns. The raw body
(``return_parsed=False``) carries that key too.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.dl_utils import _RETRYABLE_STATUS
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime

__all__: list = []

#: Statuses retried by the transport: the package default minus Liiga's deterministic 500.
RETRY_STATUSES = frozenset(_RETRYABLE_STATUS) - {500}

#: The ``tournament`` values Liiga accepts (probe 2026-10-12); anything else answers a 500.
TOURNAMENTS = ("runkosarja", "playoffs", "playout", "qualifications", "valmistavat_ottelut", "chl")

#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"

_ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status"})
_ERROR_WORD = re.compile(r"(?i)\berror\b")
#: Path -> the names of its numeric segments, in order.
_STAMPED: Tuple[Tuple["re.Pattern[str]", Tuple[str, ...]], ...] = (
    (re.compile(r"/games/stats/(\d+)/(\d+)/?$"), ("season", "game_id")),
    (re.compile(r"/games/(\d+)/(\d+)/?$"), ("season", "game_id")),
    (re.compile(r"/shotmap/(\d+)/(\d+)/?$"), ("season", "game_id")),
    (re.compile(r"/players/info/(\d+)/games/(\d+)/?$"), ("player_id", "season")),
)
_GAME = re.compile(r"/games/(\d+)/(\d+)/?$")
_GAME_LOG = re.compile(r"/players/info/\d+/games/(\d+)/?$")
_SEASON_ROWS = re.compile(r"/(games|schedule)/?$")


def _path(url: str) -> str:
    return urlsplit(url).path


def _is(value: Any, want: int) -> bool:
    """Type-strict equality: ``True == 1`` in Python, so ``id: true`` must not pass for game 1."""
    return type(value) is int and value == want


def _request_of(url: str, params: Optional[Mapping[str, Any]]) -> Dict[str, int]:
    """The ids the request names: path segments, plus the ``season`` query of ``/standings/``."""
    path = _path(url)
    for pattern, names in _STAMPED:
        m = pattern.search(path)
        if m:
            return {name: int(v) for name, v in zip(names, m.groups())}
    season = (params or {}).get("season")
    if path.rstrip("/").endswith("/standings") and season is not None:
        return {"season": int(season)}
    return {}


def _stamp(url: str, params: Optional[Mapping[str, Any]], body: Any) -> Any:
    """Add the request's ids under :data:`REQUEST_KEY` (on each row of a list body)."""
    req = _request_of(url, params)
    if not req:
        return body
    if isinstance(body, dict):
        body[REQUEST_KEY] = dict(req)
    elif isinstance(body, list):
        for row in body:
            if isinstance(row, dict):
                row[REQUEST_KEY] = dict(req)
    return body


def _error(url: str, body: Any) -> None:
    """Raise on a 200 whose body is one of Liiga's error shapes (see the module docstring)."""
    where = _rt._where(url)
    if isinstance(body, str):
        raise AssetFetchError(f"{where} answered an error string: {body[:80]!r}")
    if not isinstance(body, dict) or not body:
        return
    if set(body) <= _ERROR_KEYS:
        raise AssetFetchError(f"{where} answered an error envelope: {str(body)[:120]}")
    for key, value in body.items():
        if isinstance(value, str) and _ERROR_WORD.search(value):
            if key == "stats":
                raise NoDataError(f"{where}: Liiga has no such game ({{'stats': {value!r}}})")
            raise AssetFetchError(f"{where} answered an error body: {{{key!r}: {value!r}}}")
        if isinstance(value, dict) and value and set(value) <= _ERROR_KEYS:
            raise AssetFetchError(f"{where} answered an error envelope under {key!r}: {str(value)[:120]}")


def _is_empty(body: Any) -> bool:
    """No rows: a scalar, a list without one non-empty object, an all-null object, or one whose lists are all empty."""
    if isinstance(body, list):
        return not any(isinstance(r, dict) and r for r in body)
    if not isinstance(body, dict):
        return True
    if not body or all(v is None for v in body.values()):
        return True
    lists = [v for v in body.values() if isinstance(v, list)]
    return bool(lists) and not any(lists)


def _scope(url: str, params: Optional[Mapping[str, Any]], body: Any) -> None:
    """Raise :class:`AssetFetchError` unless a body that names its scope answers the request."""
    where = _rt._where(url)
    path = _path(url)
    params = params or {}
    game = _GAME.search(path)
    if game and "/games/stats/" not in path and not params.get("dataType"):
        want_season, want_game = (int(x) for x in game.groups())
        g = body.get("game") if isinstance(body, dict) else None
        if not isinstance(g, dict) or not (_is(g.get("season"), want_season) and _is(g.get("id"), want_game)):
            got = (g.get("season"), g.get("id")) if isinstance(g, dict) else None
            raise AssetFetchError(f"{where}: asked for game {want_season}/{want_game}, the body is {got!r}")
        return
    season = params.get("season")
    if season is not None and (_SEASON_ROWS.search(path) or path.rstrip("/").endswith("/gameweeks")):
        rows = body if isinstance(body, list) else [body]
        bad = [r for r in rows if not isinstance(r, dict) or not _is(r.get("season"), int(season))]
        if bad:
            got = bad[0].get("season") if isinstance(bad[0], dict) else bad[0]
            raise AssetFetchError(f"{where}: asked for season {season}, the body has season {got!r}")
        return
    log = _GAME_LOG.search(path)
    if log and isinstance(body, dict):
        end = int(log.group(1))
        lo, hi = f"{end - 1}-07-01", f"{end}-06-30T23:59:59Z"
        for rows in body.values():
            for r in rows if isinstance(rows, list) else []:
                date = r.get("date") if isinstance(r, dict) else None
                if not isinstance(date, str) or not lo <= date <= hi:
                    raise AssetFetchError(f"{where}: game date {date!r} is outside season {end} ({lo}..{hi})")


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a Liiga route as JSON, refuse the error / empty / out-of-scope answers, stamp the request.

    Args:
        url: fully-built request URL.
        params: query-string parameters; ``None`` values are dropped.
        **kwargs: forwarded to the HTTP getter (``retry_statuses`` defaults to
            :data:`RETRY_STATUSES`).

    Returns:
        The parsed JSON body, with the request stamped under :data:`REQUEST_KEY` where the
        body does not say which season / game / player it answers.

    Raises:
        NoDataError: the host answered 404, ``{"stats": "Remote server error"}`` (no such
            game) or a body without rows.
        ValueError: ``tournament`` is not one of :data:`TOURNAMENTS` (checked before sending), or
            the host answered 400 / 422.
        AssetFetchError: any other non-2xx (a 500 at once, unretried), a connection failure
            after retries, an empty / non-JSON / error-shaped 200, or a body for another
            season or game than the one asked for.
    """
    tournament = (params or {}).get("tournament")
    if tournament is not None and tournament not in TOURNAMENTS:
        raise ValueError(f"tournament must be one of {TOURNAMENTS!r}; got {tournament!r}")
    kwargs.setdefault("retry_statuses", RETRY_STATUSES)
    body = euro_hockey_runtime._get(url, params, **kwargs)
    _error(url, body)
    if _is_empty(body):
        raise NoDataError(f"{_rt._where(url)}: Liiga answered no rows")
    _scope(url, params, body)
    return _stamp(url, params, body)
