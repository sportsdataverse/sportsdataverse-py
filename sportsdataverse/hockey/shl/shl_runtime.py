"""Runtime for the generated SHL wrappers (``www.shl.se/api``): season mapping and answer checks.

The SHL site API is keyless JSON. Its contracts, all from the refs capture
(``sdv-internal-refs/shl/``, live-verified 2026-10-12):

* **Seasons are ssgts.** Tables and schedules key on an ssgt uuid (season + series + game
  type); season codes are START years (``2025`` = 2025/26). The wrappers take the season as
  its END year plus a ``game_type`` (regular, playoff, qualdown, playin). :func:`_get` maps
  the pair through ``/statistics-v2/layout-info`` (fetched once per process, cached) to
  ``ssgtUuid`` for ``stats-info``, to ``seasonSeriesGameType`` for the match-day routes, and
  to the three ``seasonUuid`` / ``seriesUuid`` / ``gameTypeUuid`` that ``game-schedule``
  requires. The athlete season game log takes the START year: its wrapper reduces ``season`` by
  one (``transform: {offset: -1}`` in ``shl.yaml``), so ``_get`` sends that route's ``season`` as
  given. A season or game type layout-info does not list raises
  :class:`~sportsdataverse.errors.NoDataError`. :func:`shl_ssgt_uuid` exposes the mapping.
* **stats-info truncates silently.** Without ``count=1000`` it serves 50 rows and its
  ``totalCount`` echoes 50. :func:`_get` always sends ``count=1000`` and raises
  :class:`~sportsdataverse.errors.AssetFetchError` when the rows reach the count or fall
  short of the body's ``totalCount`` (a cut page).
* **gameday/* starts in 2023/24.** Older games answer 200 with an EMPTY body, which raises
  ``NoDataError`` (the shared getter would call an empty body a failed fetch).
* **Refusals.** A 200 body that is only an error envelope, or a body that is not JSON, raises
  ``AssetFetchError``; an empty stub (``{}`` / ``[]``) or a table with zero rows raises
  ``NoDataError``. A body that names another game, team, athlete or ssgt than the one asked
  for (a wrong answer) raises ``AssetFetchError``, as does one without the field that would
  show it (it cannot be verified). ``player-h2h-v2`` is not checked: it answers
  the current season whatever ssgt the path names.
* **One text route.** ``/sports-v2/ssgt/{ssgt}/provider`` answers a bare word (``statnet``)
  as text/html; anything else there (the SPA shell) raises ``AssetFetchError``.

Every attempt, retries included, goes through
:data:`~sportsdataverse.hockey.euro_hockey_runtime.SESSION`, which runs it inside ``_polite``
(at least 1 s from the host's last answer to the next request). Status codes follow the package vocabulary
through :mod:`sportsdataverse._codegen_runtime`: 404 -> ``NoDataError``, 400 / 422 ->
``ValueError``, any other non-2xx or a dead connection -> ``AssetFetchError``.
"""

from __future__ import annotations

import functools
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlsplit

import pandas as pd
import polars as pl

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey.euro_hockey_runtime import SESSION
from sportsdataverse.hockey.shl.shl_parsers import parse_shl_player_crosswalk

__all__ = ["GAME_TYPES", "shl_player_crosswalk", "shl_ssgt_uuid"]

BASE = "https://www.shl.se/api"
#: Game type codes layout-info lists (``playin`` exists from 2023/24).
GAME_TYPES: Tuple[str, ...] = ("regular", "playoff", "qualdown", "playin")
#: stats-info page size; without it the API serves 50 rows and says 50 is the total.
STATS_COUNT = 1000

_ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status", "statusCode"})
# Route (path below /api, ids as [^/]+) -> pluck path of the rows that must be present.
_ROWS: Dict[str, str] = {
    r"/sports-v2/season-series-game-types-filter": "season[]",
    r"/statistics-v2/layout-info": "[].seasonSeriesGameTypeTeams[]",
    r"/sports-v2/game-schedule": "gameInfo[]",
    r"/sports-v2/game-info/[^/]+": "gameInfo",
    r"/sports-v2/teams/[^/]+": "",
    r"/sports-v2/athletes/by-team-uuid/[^/]+": "[].players[]",
    r"/sports-v2/athlete-details/[^/]+": "athleteData",
    r"/sports-v2/all-sites/[^/]+": "[]",
    r"/sports-v2/played-games/[^/]+": "playedGames[]",
    r"/sports-v2/upcoming-games/[^/]+": "upcomingGames[]",
    r"/sports-v2/last-game-day": "games[]",
    r"/sports-v2/next-game-day": "games[]",
    r"/gameday/play-by-play/[^/]+": "[]",
    r"/gameday/boxscore/[^/]+": "stats.*[]",
    r"/gameday/player-stats/[^/]+": "stats.*[]",
    r"/gameday/team-stats/[^/]+": "home",
    r"/gameday/game-info/[^/]+": "teamHead2Head[]",
    r"/gameday/game-overview/[^/]+": "homeTeam",
    r"/gameday/team-standings/[^/]+": "leagueStandings[]",
    r"/gameday/post-game-data/team-stats/[^/]+": "stats[]",
    r"/statistics-v2/stats-info/[^/]+": "[].stats[]",
    r"/statistics-v2/athlete/profile-page": "",
    r"/statistics-v2/athlete/seasonList": "[]",
    r"/statistics-v2/athlete/playerProfile_gameLog": "[].stats[]",
    r"/statistics-v2/athlete/playerProfile_seasonGameLog": "[].stats[]",
    r"/statistics-v2/player-h2h-v2/[^/]+/[^/]+/[^/]+": "players[]",
}
# Route -> (pluck path of the field that must echo the request, where the requested id is):
# "path" = the last path segment, "ssgt" = the ssgt this call resolved, "playerUuid" = that param.
_SCOPE: Dict[str, Tuple[str, str]] = {
    r"/sports-v2/game-schedule": ("gameInfo[].ssgtUuid", "ssgt"),
    r"/sports-v2/game-info/[^/]+": ("gameInfo.gameUuid", "path"),
    r"/sports-v2/teams/[^/]+": ("uuid", "path"),
    r"/sports-v2/athlete-details/[^/]+": ("athleteData.uuid", "path"),
    r"/sports-v2/last-game-day": ("games[].ssgtUuid", "ssgt"),
    r"/sports-v2/next-game-day": ("games[].ssgtUuid", "ssgt"),
    r"/gameday/play-by-play/[^/]+": ("[].gameUuid", "path"),
    r"/gameday/team-stats/[^/]+": ("home.gameUuid", "path"),
    r"/gameday/game-overview/[^/]+": ("gameUuid", "path"),
    r"/gameday/team-standings/[^/]+": ("ssgtUuid", "path"),
    r"/statistics-v2/athlete/profile-page": ("uuid", "playerUuid"),
}
_TEXT_ROUTE = re.compile(r"/sports-v2/ssgt/[^/]+/provider")
# Routes whose ``season`` (END year) + ``gameType`` the runtime maps to the provider's ssgt keys.
_SEASON_ROUTES = frozenset(
    {
        r"/statistics-v2/stats-info/[^/]+",
        r"/sports-v2/game-schedule",
        r"/sports-v2/last-game-day",
        r"/sports-v2/next-game-day",
    }
)


def _route(url: str, table: Dict[str, Any]) -> Optional[str]:
    """The ``table`` key whose pattern matches ``url``'s path below ``/api``."""
    path = urlsplit(url).path.split("/api", 1)[-1].rstrip("/")
    return next((k for k in table if re.fullmatch(k, path)), None)


def _pluck(body: Any, path: str) -> List[Any]:
    """Values at a dotted path: ``[]`` walks a list, ``*`` a map's values, ``""`` = the body; nulls skipped."""
    cur = [body]
    for part in [p for p in re.split(r"\.|(?=\[\])", path) if p]:
        nxt: List[Any] = []
        for c in cur:
            if part == "[]":
                nxt += c if isinstance(c, list) else []
            elif part == "*":
                nxt += list(c.values()) if isinstance(c, dict) else []
            elif isinstance(c, dict) and c.get(part) is not None:
                nxt.append(c[part])
        cur = nxt
    return cur


# --------------------------------------------------------------------------- transport


def _transport(url: str, **kwargs: Any) -> Any:
    """``download`` over the polite session; an empty 200 from a gameday route is "no data"."""
    kwargs.setdefault("session", SESSION)
    resp = download(url=url, **kwargs)
    status = getattr(resp, "status_code", 200)
    _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
    if "/gameday/" in url and status == 200 and not (getattr(resp, "text", "") or "").strip():
        raise NoDataError(f"{_rt._where(url)} answered an empty body: gameday data starts in 2023/24")
    return resp


def _fetch(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    return _rt._download_json(_transport, url, params=clean, **kwargs)


# --------------------------------------------------------------------------- season -> ssgt


@functools.lru_cache(maxsize=1)
def _directory() -> Tuple[Dict[Tuple[int, str], Dict[str, str]], str]:
    """``({(END season, game type): uuids}, active ssgt)`` from one layout-info call (cached)."""
    raw = _fetch(f"{BASE}/statistics-v2/layout-info")
    body = raw[0] if isinstance(raw, list) and raw and isinstance(raw[0], dict) else {}
    out: Dict[Tuple[int, str], Dict[str, str]] = {}
    for s in body.get("seasonSeriesGameTypeTeams") or []:
        if not isinstance(s, dict):
            continue
        code = str((s.get("season") or {}).get("code") or "")
        gtype = (s.get("gameType") or {}).get("code")
        if code.isdigit() and gtype and s.get("ssgtUuid"):
            out[(int(code) + 1, str(gtype))] = {
                "ssgt": str(s["ssgtUuid"]),
                "season": str((s.get("season") or {}).get("uuid") or ""),
                "series": str((s.get("series") or {}).get("uuid") or ""),
                "game_type": str((s.get("gameType") or {}).get("uuid") or ""),
            }
    if not out:
        raise AssetFetchError(f"{_rt._where(BASE + '/statistics-v2/layout-info')}: no ssgt directory in the body")
    active = str(((body.get("ssgtDefaultValue") or {}).get("ssgtUuid")) or "")
    return out, active


def _resolve(season: Any, game_type: Any) -> Dict[str, str]:
    """The uuids of (END-year ``season``, ``game_type``); ``NoDataError`` when layout-info has none."""
    gtype = str(game_type or "regular")
    try:
        key = (int(season), gtype)
    except (TypeError, ValueError):
        raise ValueError(f"season must be an END year such as 2026; got {season!r}") from None
    directory, _ = _directory()
    if key not in directory:
        seasons = sorted({s for s, _ in directory})
        raise NoDataError(
            f"SHL lists no {gtype} ssgt for season {season} (END year); layout-info covers "
            f"{seasons[0]}-{seasons[-1]} with game types {sorted({g for _, g in directory})}"
        )
    return directory[key]


def shl_ssgt_uuid(season: int, game_type: str = "regular") -> str:
    """The SHL ssgt uuid of a season (END year) and game type, from ``/statistics-v2/layout-info``.

    Use it for the routes that take an ssgt uuid directly (``shl_standings``, ``shl_sites``,
    ``shl_ssgt_provider``, ``shl_post_game_team_stats``, ``shl_player_h2h``). The directory is
    fetched once per process.

    Args:
        season (int): season END year (2026 = 2025/26).
        game_type (str): regular, playoff, qualdown or playin.

    Returns:
        str: the ssgt uuid (e.g. ``"iuzqg7dqk9"`` for the 2025/26 regular season).

    Raises:
        NoDataError: layout-info lists no ssgt for that season and game type.
        ValueError: ``season`` is not a year.
        AssetFetchError: layout-info could not be fetched or read.

    Example:
        Quick start::

            from sportsdataverse.hockey.shl import shl_ssgt_uuid
            shl_ssgt_uuid(2026)              # 2025/26 regular season
            shl_ssgt_uuid(2025, "playoff")

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_schedule` - a route this feeds.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return _resolve(season, game_type)["ssgt"]


def _map_query(route: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
    """Rewrite the wrappers' ``season`` / ``gameType`` into the provider's keys; also return the ssgt."""
    q = dict(params)
    if route == r"/statistics-v2/athlete/playerProfile_seasonGameLog":
        return q, None  # its ``season`` is already the START year: the wrapper declares ``transform: {offset: -1}``
    if route not in _SEASON_ROUTES:
        return q, None  # e.g. the filter route, whose ``season`` is a season uuid
    season, gtype = q.pop("season", None), q.pop("gameType", None)
    if route == r"/statistics-v2/stats-info/[^/]+":
        if season is not None:
            q["ssgtUuid"] = _resolve(season, gtype)["ssgt"]
        q["count"] = STATS_COUNT
        return q, q.get("ssgtUuid")
    if route == r"/sports-v2/game-schedule":
        if season is not None:
            ids = _resolve(season, gtype)
            q.update(seasonUuid=ids["season"], seriesUuid=ids["series"], gameTypeUuid=ids["game_type"])
            return q, ids["ssgt"]
        return q, None
    if route in (r"/sports-v2/last-game-day", r"/sports-v2/next-game-day"):
        if season is not None:
            q["seasonSeriesGameType"] = _resolve(season, gtype)["ssgt"]
        elif not q.get("seasonSeriesGameType"):
            q["seasonSeriesGameType"] = _directory()[1]  # the active ssgt
        return q, q.get("seasonSeriesGameType")
    return q, None


# --------------------------------------------------------------------------- answer checks


def _check(url: str, route: Optional[str], raw: Any, query: Dict[str, Any], ssgt: Optional[str]) -> None:
    where = _rt._where(url)
    if isinstance(raw, dict) and raw and set(raw) <= _ERROR_KEYS:
        raise AssetFetchError(f"{where} answered an error envelope {sorted(raw)}")
    if raw in ({}, [], None, ""):
        raise NoDataError(f"{where} answered an empty body")
    if route is None:
        return
    rows = _pluck(raw, _ROWS[route])
    if not any(isinstance(r, dict) and r for r in rows):
        raise NoDataError(f"{where} answered no rows at {_ROWS[route]!r}")
    if route == r"/statistics-v2/stats-info/[^/]+":
        if len(rows) >= STATS_COUNT:
            raise AssetFetchError(f"{where} answered {len(rows)} rows = count={STATS_COUNT}: the table may be cut")
        total = next(iter(_pluck(raw, "[].totalCount")), None)
        if isinstance(total, int) and not isinstance(total, bool) and len(rows) < total:
            raise AssetFetchError(f"{where} answered {len(rows)} of totalCount={total} rows: the table was cut")
    scope = _SCOPE.get(route)
    if scope is None:
        return
    field, source = scope
    expected: Optional[str]
    if source == "path":
        expected = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]
    else:
        expected = ssgt if source == "ssgt" else query.get(source)
    if not expected:
        return
    values = _pluck(raw, field)
    if not values or ("[]" in field and len(values) != len(_pluck(raw, field.rsplit(".", 1)[0]))):
        raise AssetFetchError(f"{where}: the body lacks {field!r}, so {expected!r} cannot be verified")
    wrong = [v for v in values if v != expected]
    if wrong:
        raise AssetFetchError(f"{where} answered {field}={wrong[0]!r} for {expected!r}: not the data asked for")


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET an SHL route: map the season, fetch politely, then refuse an unusable answer.

    Args:
        url: fully-built request URL.
        params: query parameters; ``season`` (END year) and ``gameType`` are mapped (see the
            module docstring), ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The parsed JSON body; the bare word of the ``provider`` text route.

    Raises:
        NoDataError: a 404; an empty gameday body (games before 2023/24); an empty stub or a
            table with no rows; a season or game type layout-info does not list.
        ValueError: a 400 / 422, or a season that is not a year.
        AssetFetchError: any other non-2xx or a dead connection after retries; a non-JSON or
            error-envelope 200; a stats-info page whose rows reach ``count`` or fall short of
            its ``totalCount``; an answer for another game / team / athlete / ssgt, or a body
            without the field that would show its scope.
    """
    if _TEXT_ROUTE.search(urlsplit(url).path):
        kwargs.setdefault("session", SESSION)
        with _rt._transport_errors(url):
            resp = download(url=url, **kwargs)
        text = _rt._text_body(resp, url).strip()
        if not re.fullmatch(r"[a-z_]{1,40}", text):
            raise AssetFetchError(f"{_rt._where(url)} answered {text[:40]!r}, not a provider name")
        return text
    route = _route(url, _ROWS)
    query, ssgt = _map_query(route or "", dict(params or {}))
    raw = _fetch(url, query, **kwargs)
    _check(url, route, raw, query, ssgt)
    return raw


Frame = Union[pl.DataFrame, pd.DataFrame]


def shl_player_crosswalk(season: int, game_type: str = "regular", *, return_as_pandas: bool = False) -> Frame:
    """isa player id -> athlete uuid for every player of an SHL season, from one stats-info call.

    The SHL has three player id systems: the **isa** (StatNet) id that play-by-play, box scores
    and stats tables key on, the platform **athlete uuid** that rosters and athlete routes key
    on, and the old ``ramses_legacy`` CMS id (athlete-details only). The ``players_summary``
    table's ``players{}`` map is keyed by isa and carries each player's uuid, skaters and
    goalies alike.

    Args:
        season (int): season END year (2026 = 2025/26).
        game_type (str): regular, playoff, qualdown or playin.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player: ``player_id`` (isa, ``Int64``),
        ``athlete_uuid`` (``Utf8``; null where SHL has none, common before about 2005),
        ``first_name``, ``last_name``, ``full_name``, ``date_of_birth`` (``YYYY-MM-DD``), ``nationality``,
        ``gender``, ``position``, ``shoots``, ``ep_url`` (all ``Utf8``), ``jersey_number``, ``weight`` (kg)
        and ``height`` (cm) (``Int64``).

    Raises:
        NoDataError: layout-info lists no such ssgt, or the table has no rows.
        AssetFetchError: the fetch failed or the table came back cut.

    Example:
        Quick start::

            from sportsdataverse.hockey.shl import shl_player_crosswalk
            xw = shl_player_crosswalk(2026)
            xw.filter(xw["player_id"] == 2427)

    See Also:
        * :func:`sportsdataverse.hockey.ha.ha_player_crosswalk` - the HA lookup built on this frame.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    raw = _get(f"{BASE}/statistics-v2/stats-info/players_summary", {"season": season, "gameType": game_type})
    return parse_shl_player_crosswalk(raw, return_as_pandas=return_as_pandas)
