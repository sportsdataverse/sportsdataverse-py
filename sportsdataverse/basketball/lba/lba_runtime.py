"""Runtime for the generated Lega Basket Serie A wrappers (``www.legabasket.it/api``, keyless JSON).

Every request goes through :data:`SESSION`, a :class:`~sportsdataverse.basketball.basketball_runtime.PoliteSession`
that waits :data:`INTERVAL` (2 s, the ``Crawl-delay`` in the site's ``robots.txt``) before **every** attempt, retries
included, then through the shared ``basketball_runtime._get`` (404 -> ``NoDataError``, 400 / 422 -> ``ValueError``,
any other non-2xx, a dead connection or a non-JSON / empty 200 -> ``AssetFetchError``; ``Retry-After`` on a 429 /
503 is honoured by the transport).

Measured against the live site (sdv-internal-refs ``legabasket/``, 2026-10-12):

* **Seasons are START years on the wire** (``s`` / ``year``: 2026 = 2026-27). The public argument is the sdv END
  year, so ``_get`` subtracts one from ``s`` and ``year`` before sending, and stamps the END year it was asked for
  into the body under ``_request`` (``{"season": 2027}``) for the parsers.
* **An unknown id is HTTP 500 ``text/plain``**, every time, so it is not retried: the 500 is dropped from the retry
  set and raises ``AssetFetchError`` at once (the site cannot tell an unknown id from an outage).
* **Old seasons answer 200 with an empty payload**, not 404: a play-by-play before 2017-18 (``actions: []``), a
  finished championship's standings without a round (``standings: null``), an unknown season (``competitions:
  []``). That is "nothing here": ``NoDataError``.
* **Scope.** A body must be the one asked for, else ``AssetFetchError`` (never an empty frame): the season, the
  championship, game, team or player id, the round, and the leaderboard category are all checked, type-strictly, on
  every row. A standings body must sit on one round, with no team past that many games played.
* **A game that is not final** (``game_status`` other than ``"2"``) is refused by the box score, play-by-play and
  shot routes: a partial box score is not a smaller box score. The game header (``lba_game``) is exempt.
* **A truncated page** (``pagination.count`` above the rows returned, on the two list routes that ask for a whole
  season) is ``AssetFetchError``.
* **Error envelopes** (a JSON object that only says ``error`` / ``message`` / ``status``) are ``AssetFetchError``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.dl_utils import _RETRYABLE_STATUS
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

#: Seconds between two requests to legabasket.it (robots.txt ``Crawl-delay: 2``).
INTERVAL = 2.0

#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"

#: Statuses retried by the transport: the package default minus the site's deterministic 500.
RETRY_STATUSES = frozenset(_RETRYABLE_STATUS) - {500}

#: The one game status that means final (``0`` scheduled, ``1`` live, ``2`` final).
FINAL = "2"

#: Wire season parameters: the public END year becomes the wire START year (END - 1).
_SEASON_PARAMS = ("s", "year")


class _LbaSession(_bb.PoliteSession):
    """A polite session whose spacing is :data:`INTERVAL`, not the shared one-second default."""

    def request(self, method: str | bytes, url: str | bytes, *args: Any, **kwargs: Any) -> requests.Response:
        """Wait :data:`INTERVAL` for ``url``'s host, then send the request (every attempt, retries included)."""
        _bb._polite_wait(url if isinstance(url, str) else url.decode(), INTERVAL)
        return requests.Session.request(self, method, url, *args, **kwargs)


#: The pooled session every legabasket.it request goes through.
SESSION = _LbaSession()
for _scheme in ("https://", "http://"):
    SESSION.mount(_scheme, HTTPAdapter(pool_connections=4, pool_maxsize=8))

_MISSING = object()
_ERROR_KEYS = frozenset({"error", "errors", "message", "status", "statusCode", "code", "detail"})

#: Route -> the dotted path that must hold at least one row (a list or object). ``a.b`` descends.
ROWS: Dict[str, str] = {
    "/championships/get-championships": "competitions",
    "/championships/get-championships-calendar-by-id": "matches",
    "/championships/get-championships-matches-by-id": "match",
    "/championships/get-championships-matches-scores-by-id": "scores.ht.rows",
    "/championships/get-championships-matches-play-by-play": "pbp.actions",
    "/matches/get-championship-matches-shots-by-id": "shots.ht",
    "/championships/get-championship-ranking": "standings",
    "/teams/get-teams": "teams",
    "/teams/get-team-roster": "players",
    "/teams/get-team-players-stats": "players",
    "/players/get-players": "players",
    "/players/get-player-by-id": "player",
    "/players/get-player-stats": "stats.data.sum",
    "/players/get-player-matches": "matches_stats.data",
    "/statistics/get-players-statistics": "stats",
    "/statistics/get-teams-statistics-by-type": "stats",
}

#: Route -> (dotted path, request parameter) pairs: the body must echo the parameter, on every row (``a[].b``).
#: Checked only for a parameter that was sent. The season parameters are START years here (already mapped).
SCOPE: Dict[str, List[Tuple[str, str]]] = {
    "/championships/get-championships": [("competitions[].year", "s")],
    "/championships/get-championships-calendar-by-id": [
        ("competition.id", "id"),
        ("matches[].championship_id", "id"),
        ("matches[].number", "d"),  # `number` is the round (the `d` echo)
    ],
    "/championships/get-championships-matches-by-id": [("match.id", "id")],
    "/championships/get-championships-matches-scores-by-id": [("match.id", "id")],
    "/championships/get-championships-matches-play-by-play": [("match.id", "id")],
    "/matches/get-championship-matches-shots-by-id": [("match.id", "id")],
    "/championships/get-championship-ranking": [("competition.id", "id")],
    "/teams/get-teams": [("teams[].year", "year")],
    "/teams/get-team-roster": [("team.id", "id")],
    "/teams/get-team-players-stats": [("team.id", "id"), ("players[].player_season", "s")],
    "/players/get-players": [("players[].year", "year")],
    "/players/get-player-by-id": [("player.id", "id")],
    "/players/get-player-stats": [("player.id", "id")],
    "/players/get-player-matches": [("player.id", "id"), ("matches_stats.data[].player_id", "id")],
    "/statistics/get-players-statistics": [("stats[].year", "s"), ("stats[].category", "cat")],
    "/statistics/get-teams-statistics-by-type": [("stats[].year", "s")],
}

#: Routes whose rows carry the START year in a string prefix (``2026|TVU``) instead of a field.
_PREFIX: Dict[str, List[Tuple[str, str]]] = {
    "/players/get-player-matches": [("matches_stats.data[].player_team_code", "s")],
}

#: Routes that must answer a final game (status ``2``): the box score, play-by-play and shot routes.
_FINAL_ONLY = frozenset(
    {
        "/championships/get-championships-matches-scores-by-id",
        "/championships/get-championships-matches-play-by-play",
        "/matches/get-championship-matches-shots-by-id",
    }
)

#: List routes that ask for a whole season at once: a page shorter than ``pagination.count`` is truncated.
_WHOLE = frozenset({"/championships/get-championships", "/teams/get-teams"})


def _route(url: str) -> str:
    path = urlsplit(url).path
    return path.split("/api", 1)[1] if "/api/" in path else path


def _values(body: Any, path: str) -> List[Any]:
    """Values at a dotted path; ``name[]`` fans out over a list. A missing step yields ``_MISSING`` (fails closed)."""
    nodes: List[Any] = [body]
    for part in path.split("."):
        many = part.endswith("[]")
        key = part[:-2] if many else part
        out: List[Any] = []
        for node in nodes:
            value = node.get(key, _MISSING) if isinstance(node, dict) else _MISSING
            if many:
                if not isinstance(value, list) or not value:
                    out.append(_MISSING)
                else:
                    out.extend(value)
            else:
                out.append(value)
        nodes = out
    return nodes


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality (``True == 1`` in Python, so ``id: true`` must not pass for game 1)."""
    return type(got) is type(want) and got == want


def _to_start(params: Dict[str, Any]) -> Dict[str, int]:
    """Map the public END-year ``s`` / ``year`` to the wire START year in place; return ``{"season": END}`` sent."""
    sent: Dict[str, int] = {}
    for key in _SEASON_PARAMS:
        value = params.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().lstrip("-").isdigit():
            raise ValueError(f"season must be an END year such as 2027 (2026-27); got {value!r}")
        end = int(value)
        if not 1950 <= end <= 2100:
            raise ValueError(f"season must be an END year such as 2027 (2026-27); got {end}")
        params[key] = end - 1
        sent["season"] = end
    return sent


def _error(url: str, body: Any) -> None:
    where = _rt._where(url)
    if not isinstance(body, dict):
        raise AssetFetchError(f"{where} answered a {type(body).__name__}, not an object")
    if body and set(body) <= _ERROR_KEYS:
        raise AssetFetchError(f"{where} answered an error envelope: {str(body)[:120]}")


def _is_empty(value: Any) -> bool:
    if isinstance(value, (list, dict)):
        return not any(v not in (None, "", [], {}) for v in (value if isinstance(value, list) else value.values()))
    return value is None


def _check(url: str, route: str, params: Mapping[str, Any], body: Dict[str, Any]) -> None:
    """Raise ``NoDataError`` for a row-less body, ``AssetFetchError`` for a partial or out-of-scope one."""
    where = _rt._where(url)
    path = ROWS.get(route)
    if path is not None:
        (got,) = _values(body, path)
        if got is _MISSING or _is_empty(got):
            raise NoDataError(f"{where}: the site answered no rows at {path!r}")
    for dotted, key in SCOPE.get(route, []):
        if params.get(key) is None:
            continue
        for got in _values(body, dotted):
            want = params[key] if key == "cat" else int(params[key])
            if got is _MISSING or not _same(got, want):
                raise AssetFetchError(
                    f"{where}: asked for {key}={want!r}, the body has {dotted}={None if got is _MISSING else got!r}"
                )
    for dotted, key in _PREFIX.get(route, []):
        if params.get(key) is None:
            continue
        want = f"{params[key]}|"
        for got in _values(body, dotted):
            if not isinstance(got, str) or not got.startswith(want):
                raise AssetFetchError(
                    f"{where}: asked for season {params[key]}, the body has {dotted}={None if got is _MISSING else got!r}"
                )
    if route == "/championships/get-championship-ranking" and params.get("d") is not None:
        _round(where, body, int(params["d"]))
    if route in _FINAL_ONLY:
        status = _values(body, "match.game_status")[0]
        if status != FINAL:
            raise AssetFetchError(
                f"{where}: the game is not final (game_status={None if status is _MISSING else status!r})"
            )
    if route in _WHOLE and "page" not in params:
        pg = body.get("pagination")
        rows = _values(body, ROWS[route])[0]
        count = pg.get("count") if isinstance(pg, dict) else None
        if type(count) is not int or count > len(rows):
            raise AssetFetchError(f"{where}: truncated page ({len(rows)} rows of {count!r}); pass a larger page size")


def _round(where: str, body: Mapping[str, Any], day: int) -> None:
    """Standings on one round: one ``championships_day_id`` on every row and no team past ``day`` games played."""
    rows = [r for r in body.get("standings") or [] if isinstance(r, dict)]
    days = {r.get("championships_day_id", _MISSING) for r in rows}
    played = [r.get("game_played") for r in rows]
    if len(days) != 1 or _MISSING in days:
        raise AssetFetchError(f"{where}: the standings span several rounds")
    counts = [g for g in played if type(g) is int]
    # not `==`: a postponed game leaves a team one short
    if len(counts) != len(played) or max(counts) > day:
        raise AssetFetchError(f"{where}: games played {played} are past round {day}")


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a legabasket.it route as JSON, refuse the empty / partial / out-of-scope answers, stamp the season.

    Args:
        url: fully-built request URL.
        params: query-string parameters; ``None`` values are dropped. ``s`` and ``year`` are the sdv END year and
            are sent as the START year (END - 1).
        **kwargs: forwarded to the HTTP getter (``session`` defaults to :data:`SESSION`, ``retry_statuses`` to
            :data:`RETRY_STATUSES`).

    Returns:
        The parsed JSON body, with ``{"season": END}`` stamped under :data:`REQUEST_KEY` when a season was sent.

    Raises:
        NoDataError: the host answered 404, or a body with no rows (an unknown season, a play-by-play before
            2017-18, standings of a finished championship without a round).
        ValueError: the season is not an END year, or the host answered 400 / 422.
        AssetFetchError: any other non-2xx (a 500 at once, unretried: unknown id or outage), a connection
            failure after retries, an empty / non-JSON / error-shaped 200, a game that is not final on a stat
            route, a truncated page, or a body for another season, championship, game, team, player, round or
            category than the one asked for.
    """
    wire = {k: v for k, v in (params or {}).items() if v is not None}
    sent = _to_start(wire)
    kwargs.setdefault("session", SESSION)
    kwargs.setdefault("retry_statuses", RETRY_STATUSES)
    body = _bb._get(url, wire, **kwargs)
    _error(url, body)
    _check(url, _route(url), wire, body)
    if sent:
        body[REQUEST_KEY] = sent
    return body
