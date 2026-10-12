"""Runtime for the generated ``nationalleague`` wrappers (``www.nationalleague.ch/api``).

**Owner ruling.** ``www.nationalleague.ch/robots.txt`` disallows ``/api``. The owner approved
building on it on 2026-10-12, for polite, incremental use only: at least 1 s between requests
(:func:`sportsdataverse.hockey.euro_hockey_runtime._polite_wait`), one worker, and no bulk
backfill or published mirror without asking again. Fetch a finished game once, discover new
games from one ``/games`` call per run, and treat season tables as game-day snapshots.

The API is keyless JSON and already keys a season by its END year (``2026`` = 2025/26), the
package convention, so ``season`` goes on the wire unchanged. ``phase`` is a SIHF phase id
(``4940`` = 2025/26 Regular Season); ids change every season. Leaving ``phase`` out served the
regular season on ``/player`` (verified, byte-identical to ``phase=4940``); on the other routes
that default is assumed, not verified.

:func:`_get` is the shared throttled getter plus the checks the reference capture applies before
it banks a body (``sdv-internal-refs/nationalleague/tools/capture.py``), so a wrong answer never
reaches a parser as data:

* the package error vocabulary from the shared getter: 404 -> ``NoDataError``, 400 / 422 ->
  ``ValueError``, any other non-2xx, an empty or non-JSON body, or a dead connection ->
  ``AssetFetchError``;
* an error envelope (a 200 object holding only ``error`` / ``message`` / ``status`` ... keys),
  a list route answering a non-list, a row that is not an object or lacks its digit-string id
  (``playerId`` / ``teamId`` / ``gameId``), or a game / player / club page without the id it
  must echo -> ``AssetFetchError`` (the answer is broken, not empty);
* a wrong answer -- a game whose ``gameId`` does not start with the requested season, a team's
  schedule or player list holding another team, or a page echoing another game / player / club
  -> ``AssetFetchError`` (the fetch did not give what was asked; it is not "nothing here");
* an empty list (``/games?season=2016`` answers ``200 []``), an empty object, or a bracket with
  no games yet -> ``NoDataError`` (nothing for what was asked).

Ids are type-strict, as in the capture: the API sends them as digit strings, and an integer
where a digit string belongs is refused. ``/player*`` and ``/teams`` rows carry no season, so a
wrong-season answer from them cannot be detected.

Every body is returned without its api-key entries: ``/teams/{teamId}/info`` ships a club
backend key in ``fields.apiKey``, which is dropped here (and again by the parsers), so neither
the raw body nor a frame ever carries it.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as _euro
from sportsdataverse.hockey.nl.nationalleague_parsers import _scrub

__all__: List[str] = []

_ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status"})

# (path under /api, scope); named groups are the ids the body must echo. Most specific first.
_ROUTES: List[Tuple["re.Pattern[str]", str]] = [
    (re.compile(r"/games/(?:playoffs|playouts)$"), "bracket"),
    (re.compile(r"/games/current$"), "games"),
    (re.compile(r"/games/team/(?P<team>[^/]+)$"), "team_games"),
    (re.compile(r"/games/(?P<game>[^/]+)$"), "game"),
    (re.compile(r"/games$"), "games"),
    (re.compile(r"/player/topscorer$"), "player_rows"),
    (re.compile(r"/player/team/(?P<team>[^/]+)$"), "team_players"),
    (re.compile(r"/player/(?P<player>[^/]+)$"), "player"),
    (re.compile(r"/player$"), "player_rows"),
    (re.compile(r"/teams/(?P<team>[^/]+)/info$"), "team_info"),
    (re.compile(r"/teams$"), "team_rows"),
]


def _is_id(value: Any, want: Optional[str] = None) -> bool:
    """A non-empty digit string (equal to ``want`` when given); an integer is refused."""
    return isinstance(value, str) and value.isdigit() and (want is None or value == want)


def _games_of(url: str, body: List[Any]) -> List[Dict[str, Any]]:
    """Every game in a games list or a rounds -> series -> games tree."""
    out: List[Dict[str, Any]] = []
    for item in body:
        if not isinstance(item, dict):
            raise AssetFetchError(f"{url}: a {type(item).__name__} where a game, round or series belongs")
        if isinstance(item.get("games"), list):
            out += _games_of(url, item["games"])
        else:
            out.append(item)
    return out


def _check(url: str, params: Optional[dict], body: Any) -> None:
    """Raise unless ``body`` is a real answer to ``url`` + ``params`` (see the module docstring)."""
    path = urlsplit(url).path
    path = path[path.find("/api") + 4 :] if "/api" in path else path
    scope, want = next(((s, m.groupdict()) for rx, s in _ROUTES if (m := rx.search(path))), ("", {}))
    if isinstance(body, dict) and body and set(body) <= _ERROR_KEYS:
        raise AssetFetchError(f"{url} answered an error object: {sorted(body)}")
    if scope in ("games", "team_games", "bracket", "player_rows", "team_players", "team_rows"):
        if not isinstance(body, list):
            raise AssetFetchError(f"{url} answered a {type(body).__name__}, not a list")
        if not body:
            raise NoDataError(f"{url} answered an empty list")
    elif scope and not (isinstance(body, dict) and body):
        if isinstance(body, dict):
            raise NoDataError(f"{url} answered an empty object")
        raise AssetFetchError(f"{url} answered a {type(body).__name__}, not an object")

    if scope in ("player_rows", "team_players", "team_rows"):
        key = "teamId" if scope == "team_rows" else "playerId"
        for row in body:
            if not (isinstance(row, dict) and row and _is_id(row.get(key))):
                raise AssetFetchError(f"{url}: a row without a digit-string {key}: {str(row)[:60]}")
        if scope == "team_players" and any(row.get("teamId") != want["team"] for row in body):
            raise AssetFetchError(f"{url}: asked team {want['team']}, served a player of another team")
    elif scope in ("games", "team_games", "bracket"):
        games = _games_of(url, body)
        if not games:
            raise NoDataError(f"{url}: no games yet")
        if any(not _is_id(g.get("gameId")) for g in games):
            raise AssetFetchError(f"{url}: a game without a digit-string gameId")
        season = str((params or {}).get("season") or "")
        if season and (bad := next((g["gameId"] for g in games if not g["gameId"].startswith(season)), None)):
            raise AssetFetchError(f"{url}: asked season {season}, served game {bad}")
        if scope == "team_games":
            team = want["team"]
            if any(not (_is_id(g.get("homeTeamId"), team) or _is_id(g.get("awayTeamId"), team)) for g in games):
                raise AssetFetchError(f"{url}: asked team {team}, served a game without it")
    elif scope in ("game", "player", "team_info"):
        if scope == "game":
            got, asked = (body.get("overview") or {}).get("gameId"), want["game"]
        elif scope == "player":
            got, asked = body.get("playerId"), want["player"]
        else:
            got, asked = (body.get("fields") or {}).get("teamID"), want["team"]
        if not _is_id(got):
            raise AssetFetchError(f"{url}: the page carries no digit-string id to check ({got!r})")
        if got != asked:
            raise AssetFetchError(f"{url}: asked {asked}, served {got}")


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a National League API route as JSON: throttled, checked, api keys dropped.

    Args:
        url: fully-built request URL under ``https://www.nationalleague.ch/api``.
        params: query-string parameters; ``None`` values are dropped. ``season`` is the END year.
        **kwargs: forwarded to :func:`sportsdataverse._codegen_runtime._get`.

    Returns:
        The parsed JSON body, without any api-key entry.

    Raises:
        NoDataError: a 404, or an empty answer (see the module docstring).
        ValueError: a 400 / 422.
        AssetFetchError: any other failed fetch, an error envelope, a malformed body, or an
            answer for another season, team, game or player.
    """
    body = _euro._get(url, params, **kwargs)  # _polite_wait, then the shared getter
    _check(url, params, body)
    return _scrub(body)
