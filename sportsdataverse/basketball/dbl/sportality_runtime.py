"""Runtime for the generated Basketligaen wrappers (``www.basketligaen.dk/api``): season mapping and answer checks.

Basketligaen's site is a Sportality instance (the vendor behind ``shl`` / ``ha``); the stats are
``genius-basketball``, a relay of Genius Sports LiveStats. The API is keyless JSON. Its contracts,
all from the refs capture (``sdv-internal-refs/sportality/``, live-verified 2026-10-12):

* **Seasons are ssgts.** Tables and schedules key on an ssgt uuid (season + series + game type);
  season codes are START years (``2025`` = 2025/26). The wrappers take the season as its END year
  plus a ``game_type`` (``regular``, or ``finals`` for the playoffs). :func:`_get` maps the pair
  through ``/statistics-v2/layout-info`` (fetched once per process, cached) to ``ssgtUuid`` for
  ``stats-info``, to ``seasonSeriesGameType`` for the match-day routes, and to the three
  ``seasonUuid`` / ``seriesUuid`` / ``gameTypeUuid`` that ``game-schedule`` requires. A season or
  game type layout-info does not list (the site starts in 2002/03) raises
  :class:`~sportsdataverse.errors.NoDataError`. :func:`sportality_ssgt_uuid` exposes the mapping.
* **stats-info truncates silently.** Without ``count=1000`` it serves 50 rows. :func:`_get` always
  sends ``count=1000`` and raises :class:`~sportsdataverse.errors.AssetFetchError` when the rows
  reach the count or differ from the body's ``totalCount`` (a cut page; the teams and standings
  tables carry a null total and are checked on the count alone). An unknown module answers the
  ``games`` table with HTTP 200, so a table whose columns are not the module's is refused.
* **gameday/* starts in 2023/24.** Older games, and unknown game uuids, answer 200 with an EMPTY
  body, which raises ``NoDataError`` (the shared getter would call an empty body a failed fetch).
  An unknown game uuid on ``sports-v2`` answers 200 with null fields: also ``NoDataError``.
* **Refusals, as the capture's gates.** A 200 body that is only an error envelope, or not JSON,
  raises ``AssetFetchError``; an empty stub (``{}`` / ``[]``) or a table with zero rows raises
  ``NoDataError``. A body that names another game, team, athlete or ssgt than the one asked for
  (a wrong answer) raises ``AssetFetchError``, as does one without the field that would show it.
  Play-by-play and the game overview additionally refuse an UNFINISHED game and a TRUNCATED feed:
  the newest numbered event must be the ``game``/``end`` line in state ``GameEnded`` (the source
  has games whose feed stops early, e.g. at 89-91 of a 92-93 game; the survey found six).
  Box scores must hold one team-totals row per side, every player row must be a player of its own
  side's ``players`` map, and no player may sit on both sides; a play-by-play feed may not put one
  player on both teams. Not mirrored, because each needs a second request: a play-by-play player
  who is not on the box score's roster (one survey game), points per period against the final
  score (one survey game), the standings / post-game routes against stats-info, and the
  schedule's finished-game count against ``games_games`` (the 2002/03 regular season lists 109
  finished games where its own standings count 110: a source gap, not a cut).
* **A 500 is this API's answer for an unknown team / athlete / ssgt / season uuid**, but a real
  server error looks the same, so it is not retried and raises ``AssetFetchError`` (the package
  vocabulary for an unknown answer).
* **One text route.** ``/sports-v2/ssgt/{ssgt}/provider`` answers a bare word
  (``genius-basketball``) as text; anything else there (the SPA shell) raises ``AssetFetchError``.
* **Personal data** (birth dates, e-mail, phone, address, contact blocks) is removed from every
  body before it is returned (the parsers' ``_scrub``).

Every attempt, retries included, goes through
:data:`~sportsdataverse.basketball.basketball_runtime.SESSION`, which waits for ``_polite_wait`` (at
least 1 s between two requests to the host); ``Retry-After`` on a 429 / 503 is honoured by the shared
``download``. Status codes follow the package vocabulary through :mod:`sportsdataverse._codegen_runtime`:
404 -> ``NoDataError``, 400 / 422 -> ``ValueError``, any other non-2xx or a dead connection ->
``AssetFetchError``.
"""

from __future__ import annotations

import functools
import re
from typing import Any, Callable, Dict, List, NoReturn, Optional, Set, Tuple, Union
from urllib.parse import urlsplit

import pandas as pd
import polars as pl

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.basketball.dbl.sportality_parsers import _scrub, parse_sportality_player_crosswalk
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["GAME_TYPES", "sportality_player_crosswalk", "sportality_ssgt_uuid"]

BASE = "https://www.basketligaen.dk/api"
#: Game type codes layout-info lists: the regular season and the playoffs ("finals", Slutspil).
GAME_TYPES: Tuple[str, ...] = ("regular", "finals")
#: stats-info page size; without it the API serves 50 rows.
STATS_COUNT = 1000
#: A 500 is the API's answer for an unknown uuid and is not retried (a 429 and the gateway errors are).
RETRY_STATUSES = frozenset({429, 502, 503, 504})
ATTEMPTS = 3
#: layout-info lists 49 ssgts of 25 seasons; far fewer means a cut or foreign body.
MIN_SSGTS = 40
MIN_FILTER_SEASONS = 20

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
    r"/gameday/game-info/[^/]+": "teamPeriod.stats[]",
    r"/gameday/game-overview/[^/]+": "homeTeam",
    r"/gameday/team-standings/[^/]+": "leagueStandings[]",
    r"/gameday/post-game-data/team-stats/[^/]+": "stats[]",
    r"/statistics-v2/stats-info/[^/]+": "[].stats[]",
    r"/statistics-v2/athlete/profile-page": "",
    r"/statistics-v2/athlete/playerProfile_gameLog": "[].stats[]",
    r"/statistics-v2/player-h2h-v2/[^/]+/[^/]+/[^/]+": "players[]",
}
# Route -> [(pluck path of a field that must echo the request, where the requested id is)]:
# "path" = the last path segment, "ssgt" = the ssgt this call resolved, "playerUuid" = that param,
# "path1" / "path2" = the second / third segment after player-h2h-v2.
_SCOPE: Dict[str, List[Tuple[str, str]]] = {
    r"/sports-v2/game-schedule": [("ssgtUuid", "ssgt"), ("gameInfo[].ssgtUuid", "ssgt")],
    r"/sports-v2/game-info/[^/]+": [("gameInfo.gameUuid", "path")],
    r"/sports-v2/teams/[^/]+": [("uuid", "path")],
    r"/sports-v2/athlete-details/[^/]+": [("athleteData.uuid", "path")],
    r"/sports-v2/last-game-day": [("games[].ssgtUuid", "ssgt")],
    r"/sports-v2/next-game-day": [("games[].ssgtUuid", "ssgt")],
    r"/gameday/play-by-play/[^/]+": [("[].gameUuid", "path")],
    r"/gameday/team-stats/[^/]+": [("home.gameUuid", "path"), ("away.gameUuid", "path")],
    r"/gameday/game-overview/[^/]+": [
        ("gameUuid", "path"),
        ("homeTeam.gameUuid", "path"),
        ("awayTeam.gameUuid", "path"),
    ],
    r"/statistics-v2/athlete/profile-page": [("uuid", "playerUuid")],
    r"/statistics-v2/player-h2h-v2/[^/]+/[^/]+/[^/]+": [("homeTeamUuid", "path1"), ("awayTeamUuid", "path2")],
}
# Columns each stats-info module group must carry (an unknown module answers the games table).
_STATS_NEED: Dict[str, Set[str]] = {
    "players": {"Rank", "Player", "Team"},
    "teams": {"Rank", "Team"},
    "standings": {"Rank", "GP", "W", "L"},
    "games": {"DateTime", "homeTeam", "awayTeam"},
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


def _strict(a: Any, b: Any) -> bool:
    """Equality that also requires equal types (``'5' != 5``, ``True != 1``)."""
    return type(a) is type(b) and a == b


def _dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


# --------------------------------------------------------------------------- transport


def _transport(url: str, **kwargs: Any) -> Any:
    """``download`` over the polite session; an empty 200 from a gameday route is "no data"."""
    kwargs.setdefault("session", SESSION)
    kwargs.setdefault("retry_statuses", RETRY_STATUSES)
    kwargs.setdefault("num_retries", ATTEMPTS - 1)
    resp = download(url=url, **kwargs)
    status = getattr(resp, "status_code", 200)
    _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
    if "/gameday/" in url and status == 200 and not (getattr(resp, "text", "") or "").strip():
        raise NoDataError(
            f"{_rt._where(url)} answered an empty body: an unknown game, or a game before 2023/24 (gameday data starts there)"
        )
    return resp


def _fetch(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    return _scrub(_rt._download_json(_transport, url, params=clean, **kwargs))


# --------------------------------------------------------------------------- season -> ssgt


@functools.lru_cache(maxsize=1)
def _directory() -> Tuple[Dict[Tuple[int, str], Dict[str, Any]], str]:
    """``({(END season, game type): ids + teams}, active ssgt)`` from one layout-info call (cached)."""
    raw = _fetch(f"{BASE}/statistics-v2/layout-info")
    body = raw[0] if isinstance(raw, list) and raw and isinstance(raw[0], dict) else {}
    out: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for s in body.get("seasonSeriesGameTypeTeams") or []:
        if not isinstance(s, dict):
            continue
        code = str((s.get("season") or {}).get("code") or "")
        gtype = (s.get("gameType") or {}).get("code")
        if code.isdigit() and gtype and s.get("ssgtUuid"):
            teams = [t for t in s.get("teams") or [] if isinstance(t, dict)]
            out[(int(code) + 1, str(gtype))] = {
                "ssgt": str(s["ssgtUuid"]),
                "season": str((s.get("season") or {}).get("uuid") or ""),
                "series": str((s.get("series") or {}).get("uuid") or ""),
                "game_type": str((s.get("gameType") or {}).get("uuid") or ""),
                "start_year": int(code),
                "teams": {str(t["uuid"]) for t in teams if t.get("uuid")},
                "instances": {str(t["instanceId"]) for t in teams if t.get("instanceId")},
            }
    if len(out) < MIN_SSGTS:
        raise AssetFetchError(
            f"{_rt._where(BASE + '/statistics-v2/layout-info')}: {len(out)} ssgts in the body, expected the "
            f"{MIN_SSGTS}+ of 25 seasons"
        )
    active = str(((body.get("ssgtDefaultValue") or {}).get("ssgtUuid")) or "")
    return out, active


def _by_ssgt(ssgt: Optional[str]) -> Optional[Dict[str, Any]]:
    """The directory entry of an ssgt uuid, or ``None``."""
    return next((v for v in _directory()[0].values() if v["ssgt"] == ssgt), None)


def _resolve(season: Any, game_type: Any) -> Dict[str, Any]:
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
            f"Basketligaen lists no {gtype} ssgt for season {season} (END year); layout-info covers "
            f"{seasons[0]}-{seasons[-1]} with game types {sorted({g for _, g in directory})}"
        )
    return directory[key]


def sportality_ssgt_uuid(season: int, game_type: str = "regular") -> str:
    """The Basketligaen ssgt uuid of a season (END year) and game type, from ``/statistics-v2/layout-info``.

    Use it for the routes that take an ssgt uuid directly (``sportality_standings``,
    ``sportality_sites``, ``sportality_ssgt_provider``, ``sportality_post_game_team_stats``,
    ``sportality_player_h2h``). The directory is fetched once per process.

    Args:
        season (int): season END year (2026 = 2025/26).
        game_type (str): ``regular`` or ``finals`` (the playoffs).

    Returns:
        str: the ssgt uuid (e.g. ``"qzw34da6jg"`` for the 2025/26 regular season).

    Raises:
        NoDataError: layout-info lists no ssgt for that season and game type.
        ValueError: ``season`` is not a year.
        AssetFetchError: layout-info could not be fetched or read.

    Example:
        Quick start::

            from sportsdataverse.basketball.dbl import sportality_ssgt_uuid
            sportality_ssgt_uuid(2026)              # 2025/26 regular season
            sportality_ssgt_uuid(2026, "finals")    # 2025/26 playoffs
    """
    return str(_resolve(season, game_type)["ssgt"])


def _map_query(route: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
    """Rewrite the wrappers' ``season`` / ``gameType`` into the provider's keys; also return the ssgt."""
    q = dict(params)
    if route not in _SEASON_ROUTES:
        return q, None
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
    if season is not None:  # last-game-day / next-game-day
        q["seasonSeriesGameType"] = _resolve(season, gtype)["ssgt"]
    elif not q.get("seasonSeriesGameType"):
        q["seasonSeriesGameType"] = _directory()[1]  # the active ssgt
    return q, q.get("seasonSeriesGameType")


# --------------------------------------------------------------------------- answer checks


def _refuse(where: str, why: str) -> NoReturn:
    raise AssetFetchError(f"{where}: {why}")


def _check_box(where: str, raw: Any) -> None:
    """One team-totals row per side, every player row in its own side's ``players`` map, nobody on both sides."""
    seen: Dict[str, Set[str]] = {}
    for key, side in (("homeTeamValue", "home"), ("awayTeamValue", "away")):
        rows = [r for r in _dict(_dict(raw).get("stats")).get(key) or [] if isinstance(r, dict)]
        totals = [r for r in rows if _dict(r.get("info")).get("label") == "sports.stats.team_totals"]
        if len(totals) != 1:
            _refuse(where, f"{key} has {len(totals)} team_totals rows, expected 1")
        if _dict(totals[0].get("info")).get("side") != side:
            _refuse(where, f"the {key} totals row is for side {_dict(totals[0].get('info')).get('side')!r}")
        roster = _dict(_dict(_dict(raw).get("players")).get(key))
        if not roster:
            _refuse(where, f"no players map for {key}")
        ids = {str(k) for k in roster}
        players = [r for r in rows if _dict(r.get("info")).get("playerId") not in ("", None)]
        if not players:
            _refuse(where, f"{key} has no player rows")
        for r in players:
            pid = _dict(r.get("info")).get("playerId")
            if str(pid) not in ids:
                _refuse(
                    where, f"a {key} row of player {pid!r} is not in the {key} players map: a different game's body"
                )
        seen[side] = ids
    if seen["home"] & seen["away"]:
        _refuse(where, "a player is on both sides of the box score")


def _check_pbp(where: str, raw: Any, game_uuid: str) -> None:
    """A whole, finished feed of this game: ids agree, no player on both teams, the game-end line is newest."""
    if not isinstance(raw, list):
        _refuse(where, "play-by-play is not a list of events")
    source_ids = {str(e.get("gameSourceId")) for e in raw if isinstance(e, dict)}
    if len(source_ids) != 1:
        _refuse(where, f"the events name {len(source_ids)} Genius match ids: not one game's feed")
    side_of: Dict[str, str] = {}
    for e in raw:
        et, pl_ = _dict(e.get("eventTeam")), _dict(e.get("player"))
        if not et:
            continue
        block = _dict(e.get("homeTeam" if et.get("place") == "home" else "awayTeam"))
        if not _strict(et.get("teamId"), block.get("teamId")):
            _refuse(
                where,
                f"eventTeam {et.get('place')!r} has teamId {et.get('teamId')!r}, the {et.get('place')} block {block.get('teamId')!r}",
            )
        pid = pl_.get("playerId")
        if pid is not None and side_of.setdefault(str(pid), str(et.get("place"))) != et.get("place"):
            _refuse(where, f"player {pid!r} plays for both teams: not one game's feed")
    numbered = [e for e in raw if isinstance(e, dict) and isinstance(e.get("eventId"), int)]
    if not numbered:
        _refuse(where, "no numbered events")
    last = max(numbered, key=lambda e: e["eventId"])
    ends = [e for e in raw if isinstance(e, dict) and e.get("type") == "game" and e.get("subType") == "end"]
    if ends != [last] or last.get("gameState") != "GameEnded":
        _refuse(
            where,
            f"unfinished game or truncated feed: the newest event is {last.get('type')}/{last.get('subType')} "
            f"at {_dict(last.get('homeTeam')).get('score')}-{_dict(last.get('awayTeam')).get('score')} "
            f"in state {last.get('gameState')!r}, not the game-end line",
        )


def _check_overview(where: str, raw: Any) -> None:
    """A finished game whose two team blocks name the same Genius match."""
    raw = _dict(raw)
    if raw.get("state") != "GameEnded":
        _refuse(where, f"unfinished game: state {raw.get('state')!r}, expected 'GameEnded'")
    ids = {_dict(raw.get(k)).get("gameSourceId") for k in ("homeTeam", "awayTeam")}
    if len(ids) != 1 or None in ids:
        _refuse(where, f"the team blocks name Genius match ids {sorted(map(str, ids))}")


def _check_team_stats(where: str, raw: Any) -> None:
    raw = _dict(raw)
    sides = {_dict(raw.get(k)).get("place"): _dict(raw.get(k)) for k in ("home", "away")}
    if set(sides) != {"home", "away"}:
        _refuse(where, f"the team blocks are placed {sorted(map(str, sides))}, expected home and away")
    if not _strict(sides["home"].get("gameSourceId"), sides["away"].get("gameSourceId")):
        _refuse(where, "the home and away blocks name different Genius match ids")


def _check_schedule(where: str, raw: Any, ssgt: Optional[str]) -> None:
    """Rows in the season window and a team list equal to layout-info's for the ssgt."""
    entry = _by_ssgt(ssgt)
    if entry is None:
        return
    lo, hi = f"{entry['start_year']}-07-01", f"{entry['start_year'] + 1}-07-31"
    for g in _dict(raw).get("gameInfo") or []:
        day = str(_dict(g).get("rawStartDateTime"))[:10]
        if not lo <= day <= hi:
            _refuse(where, f"game {_dict(g).get('uuid')!r} is on {day}, outside the season window {lo}..{hi}")
    teams = {str(t.get("uuid")) for t in _dict(raw).get("teamList") or [] if isinstance(t, dict)}
    if teams != entry["teams"]:
        _refuse(where, f"the team list ({len(teams)}) is not layout-info's ({len(entry['teams'])}) for this ssgt")


def _check_stats_info(where: str, url: str, raw: Any, ssgt: Optional[str]) -> None:
    rec = raw[0] if isinstance(raw, list) and len(raw) == 1 and isinstance(raw[0], dict) else None
    if rec is None:
        _refuse(where, "stats-info did not answer a one-item list")
    rows = rec.get("stats") or []
    if len(rows) >= STATS_COUNT:
        _refuse(where, f"answered {len(rows)} rows = count={STATS_COUNT}: the table may be cut")
    total = rec.get("totalCount")
    if isinstance(total, int) and not isinstance(total, bool) and len(rows) != total:
        _refuse(where, f"answered {len(rows)} of totalCount={total} rows: the table was cut")
    module = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]
    group = module.split("_")[0]
    cols = {c.get("name") for c in rec.get("dataColumns") or [] if isinstance(c, dict)}
    need = _STATS_NEED.get(group)
    if need is not None and not need <= cols:
        _refuse(
            where,
            f"module {module!r}: the columns lack {sorted(need - cols)} (an unknown module answers the games table)",
        )
    entry = _by_ssgt(ssgt)
    if entry is not None and group != "games":
        teams = {str(t.get("uuid")) for t in _dict(rec.get("teams")).values() if isinstance(t, dict)}
        if not teams or not teams <= entry["teams"]:
            _refuse(where, f"the teams {sorted(teams - entry['teams'])[:3]} are not teams of the ssgt asked for")


def _check_team_games(where: str, raw: Any, key: str, team: str) -> None:
    for g in _dict(raw).get(key) or []:
        if team not in (
            _dict(_dict(g).get("homeTeamInfo")).get("uuid"),
            _dict(_dict(g).get("awayTeamInfo")).get("uuid"),
        ):
            _refuse(where, f"game {_dict(g).get('uuid')!r} does not involve team {team!r}: not the data asked for")


def _check(url: str, route: Optional[str], raw: Any, query: Dict[str, Any], ssgt: Optional[str]) -> None:
    where = _rt._where(url)
    if isinstance(raw, dict) and raw and set(raw) <= _ERROR_KEYS:
        raise AssetFetchError(f"{where} answered an error envelope {sorted(raw)}")
    if raw in ({}, [], None, ""):
        raise NoDataError(f"{where} answered an empty body")
    if route is None:
        return
    rows = _pluck(raw, _ROWS[route])
    if not any(isinstance(r, dict) and any(v is not None for v in r.values()) for r in rows):
        raise NoDataError(f"{where} answered no rows at {_ROWS[route]!r}")
    segments = urlsplit(url).path.rstrip("/").split("/")
    path_id = segments[-1]
    if route == r"/statistics-v2/layout-info" and len(rows) < MIN_SSGTS:
        _refuse(where, f"{len(rows)} ssgts, expected the {MIN_SSGTS}+ of 25 seasons")
    if route == r"/sports-v2/season-series-game-types-filter" and len(rows) < MIN_FILTER_SEASONS:
        _refuse(where, f"{len(rows)} seasons, expected the {MIN_FILTER_SEASONS}+ of the site")
    expected = {
        "path": path_id,
        "ssgt": ssgt,
        "playerUuid": query.get("playerUuid"),
        "path1": segments[-2],
        "path2": segments[-1],
    }
    for field, source in _SCOPE.get(route, []):
        want = expected.get(source)
        if not want:
            continue
        values = _pluck(raw, field)
        if not values or ("[]" in field and len(values) != len(_pluck(raw, field.rsplit(".", 1)[0]))):
            _refuse(where, f"the body lacks {field!r}, so {want!r} cannot be verified")
        wrong = [v for v in values if v != want]
        if wrong:
            _refuse(where, f"answered {field}={wrong[0]!r} for {want!r}: not the data asked for")
    checks: Dict[str, Callable[[], None]] = {
        r"/gameday/boxscore/[^/]+": lambda: _check_box(where, raw),
        r"/gameday/player-stats/[^/]+": lambda: _check_box(where, raw),
        r"/gameday/play-by-play/[^/]+": lambda: _check_pbp(where, raw, path_id),
        r"/gameday/game-overview/[^/]+": lambda: _check_overview(where, raw),
        r"/gameday/team-stats/[^/]+": lambda: _check_team_stats(where, raw),
        r"/sports-v2/game-schedule": lambda: _check_schedule(where, raw, ssgt),
        r"/statistics-v2/stats-info/[^/]+": lambda: _check_stats_info(where, url, raw, ssgt),
        r"/sports-v2/played-games/[^/]+": lambda: _check_team_games(where, raw, "playedGames", path_id),
        r"/sports-v2/upcoming-games/[^/]+": lambda: _check_team_games(where, raw, "upcomingGames", path_id),
        r"/statistics-v2/athlete/playerProfile_gameLog": lambda: _check_career(where, raw, query.get("playerUuid")),
        r"/sports-v2/all-sites/[^/]+": lambda: _check_sites(where, raw, path_id),
    }
    if route in checks:
        checks[route]()


def _check_career(where: str, raw: Any, athlete: Optional[str]) -> None:
    players = _dict(_dict(raw[0] if isinstance(raw, list) and raw else None).get("players"))
    if athlete and {_dict(p).get("uuid") for p in players.values()} != {athlete}:
        _refuse(where, "the player map is not (only) the requested athlete: not the data asked for")


def _check_sites(where: str, raw: Any, ssgt: str) -> None:
    entry = _by_ssgt(ssgt)
    if entry is None:
        return
    got = {str(r.get("instanceId")) for r in raw if isinstance(r, dict)}
    if got != entry["instances"]:
        _refuse(where, f"the clubs {sorted(got)} are not layout-info's {sorted(entry['instances'])} for this ssgt")


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a Basketligaen route: map the season, fetch politely, then refuse an unusable answer.

    Args:
        url: fully-built request URL.
        params: query parameters; ``season`` (END year) and ``gameType`` are mapped (see the
            module docstring), ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The parsed JSON body without personal data; the bare word of the ``provider`` text route.

    Raises:
        NoDataError: a 404; an empty gameday body (games before 2023/24, or an unknown game); an
            empty stub or a table with no rows; a season or game type layout-info does not list.
        ValueError: a 400 / 422, or a season that is not a year.
        AssetFetchError: any other non-2xx or a dead connection after retries (an unknown team /
            athlete / ssgt uuid answers a 500); a non-JSON or error-envelope 200; a stats-info
            page whose rows reach ``count`` or differ from its ``totalCount``; an answer for
            another game / team / athlete / ssgt; an unfinished game or truncated play-by-play;
            a body without the field that would show its scope.
    """
    if _TEXT_ROUTE.search(urlsplit(url).path):
        kwargs.setdefault("session", SESSION)
        kwargs.setdefault("retry_statuses", RETRY_STATUSES)
        kwargs.setdefault("num_retries", ATTEMPTS - 1)
        with _rt._transport_errors(url):
            resp = download(url=url, **kwargs)
        text = _rt._text_body(resp, url).strip()
        if not re.fullmatch(r"[a-z_-]{1,40}", text):
            raise AssetFetchError(f"{_rt._where(url)} answered {text[:40]!r}, not a provider name")
        return text
    route = _route(url, _ROWS)
    query, ssgt = _map_query(route or "", dict(params or {}))
    raw = _fetch(url, query, **kwargs)
    _check(url, route, raw, query, ssgt)
    return raw


Frame = Union[pl.DataFrame, pd.DataFrame]


def sportality_player_crosswalk(season: int, game_type: str = "regular", *, return_as_pandas: bool = False) -> Frame:
    """Genius person id -> athlete uuid for every player of a Basketligaen season, from one stats-info call.

    The site has two player id systems: the **Genius person id** that play-by-play, box scores and
    stats tables key on (and that joins the ``genius`` family), and the platform **athlete uuid**
    that rosters and athlete routes key on. The ``players_summary`` table's ``players{}`` map is
    keyed by Genius id and carries each player's uuid.

    Args:
        season (int): season END year (2026 = 2025/26).
        game_type (str): ``regular`` or ``finals`` (the playoffs).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player: ``player_id`` (Genius, ``Int64``),
        ``athlete_uuid`` (null where the site has none, common before about 2005), names,
        nationality, gender, position, jersey number, height and weight. Birth dates are dropped.

    Raises:
        NoDataError: layout-info lists no such ssgt, or the table has no rows.
        AssetFetchError: the fetch failed or the table came back cut.

    Example:
        Quick start::

            from sportsdataverse.basketball.dbl import sportality_player_crosswalk
            xw = sportality_player_crosswalk(2026)
            xw.filter(xw["player_id"] == 1887576)
    """
    raw = _get(f"{BASE}/statistics-v2/stats-info/players_summary", {"season": season, "gameType": game_type})
    return parse_sportality_player_crosswalk(raw, return_as_pandas=return_as_pandas)
