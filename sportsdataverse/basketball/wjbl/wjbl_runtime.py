"""Runtime for the generated W League wrappers (Women's Japan Basketball League, ``api.wjbl.01core.app/api``).

Every request goes through the shared :data:`sportsdataverse.basketball.basketball_runtime.SESSION`, which waits at
least one second before **every** attempt, retries included (``Retry-After`` on a 429 / 503 is honoured by the shared
``dl_utils.download``). The API needs no key and no proxy. ``www.wjbl.org/robots.txt`` answers 404 (checked
2026-10-12), so no ``Crawl-delay`` is published; one request a second is the pace.

What the live API does (sdv-internal-refs ``wjbl/``, probed 2026-10-12):

* **Every answer is HTTP 200, errors included.** An unknown id is ``[]`` (or ``{"league_id": "999", "game_months":
  []}``) and a missing or non-numeric parameter is ``{"error": {"error_code": "E003", "error_message": ...}}``. The
  second is ``AssetFetchError``; the first is "nothing here" (``NoDataError``) unless the body can be compared with
  the request, in which case a body for another game, season, league edition, team or player is ``AssetFetchError``.
* **Seasons.** The public argument is the END year (``2026`` = 2025-26). The API keys a season edition by an opaque
  ``league_id`` (45 = ``'25-26 Wリーグ'``) and the runtime maps the END year with the live ``/leagues`` list (one
  extra, cached request; END = 2000 + the second ``YY`` of ``league_title``). Phases inside an edition
  (``season_id``: 169 = 2025-26 regular season Premier) are native ids and are passed as given.
* **Month paging.** ``/play-schedules`` answers ONE month (``game_month``, ``YYYY-MM``); without it the league's
  latest month only. Every game of the answer is checked against the requested month and team.
* **Game routes** (``/play-schedule/detail``, ``/play-schedule``, ``/boxscore`` and the three ``/pbp/*``) take only
  the global ``play_schedule_id``. The game must be final (``game_status`` ``'fixed'``): the header is fetched first
  (one extra request, cached once final) and the team ids of every other game route must be the header's two, in the
  header's home / away order. The play-by-play must end with the end-of-game line (``end_game_flg`` 1).
* **Nothing here** is ``NoDataError``: an empty list, a game without play-by-play (before 2018-19 the ``/pbp/*``
  routes answer ``[]``), a league edition without months.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

BASE = "https://api.wjbl.01core.app/api"

#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"

_ROUTES = (
    "/leagues",
    "/play-schedule-months",
    "/play-schedules",
    "/play-schedule/detail",
    "/play-schedule",
    "/boxscore",
    "/pbp/play-by-play",
    "/pbp/team-stats",
    "/pbp/player-stats",
    "/seasons/standings",
    "/team-standings",
    "/team-ranking",
    "/teams/base",
    "/team/roster",
    "/player",
)
_GAME_ROUTES = frozenset({"/play-schedule", "/boxscore", "/pbp/play-by-play", "/pbp/team-stats", "/pbp/player-stats"})
_PBP_ROUTES = frozenset({"/pbp/play-by-play", "/pbp/team-stats", "/pbp/player-stats"})
_INT_PARAMS = ("league_id", "play_schedule_id", "team_id", "season_id", "player_id")
_TITLE = re.compile(r"^\s*(\d{2})-(\d{2}) Wリーグ(?:\s|$)")
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

#: Dependency bodies (the league table, a FINAL game header), cached for the process. Only a successful, final answer
#: is cached. ``clear_cache()`` empties it.
_CACHE: Dict[Tuple[Any, ...], Any] = {}


def clear_cache() -> None:
    """Forget the cached dependency bodies (the league table and the final game headers)."""
    _CACHE.clear()


def season_of_title(title: Any) -> Optional[int]:
    """The sdv END year of a league edition title, or ``None`` for an all-star game, cup or camp.

    Args:
        title: ``league_title`` such as ``'25-26 Wリーグ'`` (END = 2000 + the second ``YY``; ``'99-00'`` is 2000).

    Returns:
        The END year (2026 for ``'25-26 Wリーグ'``), or ``None`` when the title is not a ``YY-YY Wリーグ`` season
        (``'Wリーグオールスター2025-26'``, ``'オータムカップ2021'``, ``'サマーキャンプ2019'``).
    """
    m = _TITLE.match(title) if isinstance(title, str) else None
    if not m:
        return None
    first, second = int(m.group(1)), int(m.group(2))
    return 2000 + second if (first + 1) % 100 == second else None


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality (``True == 1`` in Python, so ``id: true`` must not pass for id 1)."""
    return type(got) is type(want) and got == want


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _rows(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _int_param(value: Any, name: str) -> int:
    if isinstance(value, bool) or not str(value).strip().lstrip("-").isdigit():
        raise ValueError(f"{name} must be an integer id; got {value!r}")
    return int(value)


def _season_ids(value: Any) -> str:
    """``season_ids`` as the API wants it: a comma list of integer phase ids (``'169,170'``)."""
    parts = value if isinstance(value, (list, tuple, set)) else str(value).split(",")
    ids = [_int_param(p, "season_ids") for p in parts if str(p).strip() != ""]
    if not ids:
        raise ValueError("season_ids must name at least one season phase id (see wjbl_standing_seasons)")
    return ",".join(str(i) for i in ids)


def _route(url: str) -> str:
    path = urlsplit(url).path
    tail = (path[len("/api") :] if path.startswith("/api/") else path).rstrip("/") or "/"
    if tail not in _ROUTES:
        raise ValueError(f"not a W League route: {tail!r}")
    return tail


def _fetch(url: str, params: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
    """GET ``url`` through the polite session; an error envelope (HTTP 200) is ``AssetFetchError``."""
    kwargs.setdefault("session", _bb.SESSION)
    body = _bb._fetch_json(url, dict(params or {}), **kwargs)
    err = _obj(body).get("error") if isinstance(body, dict) else None
    if isinstance(err, dict):
        code = str(err.get("error_code"))[:20]
        raise AssetFetchError(
            f"{_rt._where(url)} answered an error envelope (error_code={code}): a missing or invalid parameter"
        )
    return body


# --- dependencies (one extra, cached request each) ------------------------------------------------------------------


def _leagues(**kwargs: Any) -> List[Mapping[str, Any]]:
    key = ("leagues",)
    if key not in _CACHE:
        rows = _rows(_fetch(f"{BASE}/leagues", {"target": "play_schedule"}, **kwargs))
        if not rows:
            raise AssetFetchError(
                "api.wjbl.01core.app/api/leagues answered no editions, so the season cannot be mapped"
            )
        _CACHE[key] = rows
    return _CACHE[key]


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-1)
def league_id(season: Any, **kwargs: Any) -> int:
    """The opaque ``league_id`` of the league edition whose END year is ``season``.

    Args:
        season: the sdv season, an END year (2026 = 2025-26).
        **kwargs: forwarded to the HTTP getter.

    Returns:
        The ``league_id`` (45 for 2026), read from the live ``/leagues`` list (cached).

    Raises:
        ValueError: ``season`` is not an END year, or no edition has that END year.
    """
    if isinstance(season, bool) or not str(season).strip().isdigit():
        raise ValueError(f"season must be an END year such as 2026 (2025-26); got {season!r}")
    end = int(season)
    by_end: Dict[int, int] = {}
    for r in _leagues(**kwargs):
        edition, lid = season_of_title(r.get("league_title")), r.get("league_id")
        if edition is not None and isinstance(lid, int):
            by_end[edition] = lid
    if end not in by_end:
        raise ValueError(f"no W League edition ends in {end} (seasons {min(by_end)}-{max(by_end)}; 2000 = 1999-2000)")
    return by_end[end]


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-8)
def _final_header(gid: int, **kwargs: Any) -> Mapping[str, Any]:
    """The ``/play-schedule/detail`` header of a game, which must be final (cached once it is)."""
    key = ("header", gid)
    if key not in _CACHE:
        head = _fetch(f"{BASE}/play-schedule/detail", {"play_schedule_id": gid}, **kwargs)
        _check_header("api.wjbl.01core.app/api/play-schedule/detail", gid, head)
        _CACHE[key] = head
    return _CACHE[key]


def _check_header(where: str, gid: int, head: Any) -> None:
    if head == [] or head == {}:
        raise NoDataError(f"{where}: the API has no game with this id")
    body = _obj(head)
    if not _same(body.get("play_schedule_id"), gid):
        raise AssetFetchError(
            f"{where}: asked for game {gid}, the body has play_schedule_id={body.get('play_schedule_id')!r}"
        )
    if not _same(body.get("game_status"), "fixed"):
        raise AssetFetchError(f"{where}: the game is not final (game_status={body.get('game_status')!r})")
    for side in ("home", "away"):
        if not isinstance(_obj(body.get(side)).get("team_id"), int):
            raise AssetFetchError(f"{where}: the header has no {side} team_id")


# --- checks ---------------------------------------------------------------------------------------------------------


def _teams(head: Mapping[str, Any]) -> Tuple[Any, Any]:
    return _obj(head.get("home")).get("team_id"), _obj(head.get("away")).get("team_id")


def _check_game(route: str, gid: int, data: Any, **kw: Any) -> None:
    where = f"api.wjbl.01core.app/api{route}"
    head = _final_header(gid, **kw)
    home, away = _teams(head)
    if data == [] or data == {}:
        if route in _PBP_ROUTES:
            raise NoDataError(f"{where}: no play-by-play for game {gid} (the /pbp routes start with 2018-19)")
        raise NoDataError(f"{where}: the API answered nothing for game {gid}")
    body = _obj(data)
    if not _same(body.get("play_schedule_id"), gid):
        raise AssetFetchError(
            f"{where}: asked for game {gid}, the body has play_schedule_id={body.get('play_schedule_id')!r}"
        )
    if "game_status" in body and not _same(body.get("game_status"), "fixed"):
        raise AssetFetchError(f"{where}: the game is not final (game_status={body.get('game_status')!r})")
    if route == "/boxscore" or route == "/play-schedule":
        got = (_obj(body.get("home")).get("team_id"), _obj(body.get("away")).get("team_id"))
        if got != (home, away):
            raise AssetFetchError(f"{where}: teams {got} are not game {gid}'s ({home}, {away}) in home/away order")
        if route == "/boxscore" and not all(_rows(_obj(body.get(s)).get("players")) for s in ("home", "away")):
            raise NoDataError(f"{where}: the game has no player lines")
    elif route in ("/pbp/team-stats", "/pbp/player-stats"):
        got = (body.get("home_team_id"), body.get("away_team_id"))
        if got != (home, away):
            raise AssetFetchError(f"{where}: teams {got} are not game {gid}'s ({home}, {away}) in home/away order")
        if not _obj(body.get("periods")):
            raise NoDataError(f"{where}: no period rows for game {gid}")
    elif route == "/pbp/play-by-play":
        plays = _rows(body.get("play_by_plays"))
        if not plays:
            raise NoDataError(f"{where}: no plays for game {gid}")
        sides = {1: home, 2: away}
        for p in plays:
            side, team = p.get("home_away"), p.get("team_id")
            if side is not None and (side not in sides or team != sides[side]):
                raise AssetFetchError(
                    f"{where}: play {p.get('no')!r} has team_id={team!r} on side {side!r}, not game {gid}'s ({home}, {away})"
                )
        if not _same(plays[-1].get("end_game_flg"), 1):
            raise AssetFetchError(f"{where}: truncated play-by-play (it does not end with the end-of-game line)")


def _check(route: str, wire: Mapping[str, Any], data: Any, **kw: Any) -> None:
    """Raise ``NoDataError`` for a row-less body, ``AssetFetchError`` for a partial or out-of-scope one."""
    where = f"api.wjbl.01core.app/api{route}"
    if route in _GAME_ROUTES:
        _check_game(route, wire["play_schedule_id"], data, **kw)
    elif route == "/play-schedule/detail":
        _check_header(where, wire["play_schedule_id"], data)
    elif route in ("/leagues", "/seasons/standings", "/teams/base"):
        if not _rows(data):
            raise NoDataError(f"{where}: the API answered no rows")
    elif route == "/play-schedule-months":
        body = _obj(data)
        if str(body.get("league_id")) != str(wire["league_id"]) or not isinstance(body.get("league_id"), str):
            raise AssetFetchError(f"{where}: asked for league_id={wire['league_id']}, got {body.get('league_id')!r}")
        if not _rows(body.get("game_months")):
            raise NoDataError(f"{where}: league {wire['league_id']} has no game months")
    elif route == "/play-schedules":
        _schedules(where, wire, data)
    elif route in ("/team-standings", "/team-ranking"):
        _seasons(where, route, wire["season_ids"], data)
    elif route == "/team/roster":
        body = _obj(data)
        if not body:
            raise NoDataError(f"{where}: no roster for team {wire['team_id']} in season {wire.get('season_id')}")
        if not _same(body.get("team_id"), wire["team_id"]) or (
            "season_id" in wire and not _same(body.get("season_id"), wire["season_id"])
        ):
            raise AssetFetchError(
                f"{where}: asked for team {wire['team_id']} season {wire.get('season_id')}, "
                f"got team {body.get('team_id')!r} season {body.get('season_id')!r}"
            )
    elif route == "/player":
        body = _obj(data)
        if not body:
            raise NoDataError(f"{where}: the API has no player {wire['player_id']}")
        if not _same(body.get("player_id"), wire["player_id"]):
            raise AssetFetchError(
                f"{where}: asked for player {wire['player_id']}, the body has {body.get('player_id')!r}"
            )


def _schedules(where: str, wire: Mapping[str, Any], data: Any) -> None:
    body = _obj(data)
    if not body:
        raise NoDataError(f"{where}: the API answered nothing for league {wire['league_id']}")
    if not _same(body.get("league_id"), wire["league_id"]):
        raise AssetFetchError(f"{where}: asked for league_id={wire['league_id']}, got {body.get('league_id')!r}")
    games = [g for ph in _obj(body.get("seasons")).values() for g in _rows(_obj(ph).get("play_schedules"))]
    if not games:
        raise NoDataError(
            f"{where}: no games in league {wire['league_id']} for {wire.get('game_month') or 'its latest month'}"
        )
    month, team = wire.get("game_month"), wire.get("team_id")
    for g in games:
        if month is not None:
            got = f"{g.get('game_year')}-{str(g.get('game_date'))[:2]}"
            if got != month:
                raise AssetFetchError(f"{where}: asked for {month}, game {g.get('play_schedule_id')!r} is in {got}")
        if team is not None and team not in (g.get("home_team_id"), g.get("away_team_id")):
            raise AssetFetchError(f"{where}: game {g.get('play_schedule_id')!r} does not involve team {team}")


def _seasons(where: str, route: str, ids: str, data: Any) -> None:
    want = {int(i) for i in ids.split(",")}
    if data == [] or data == {}:
        raise NoDataError(f"{where}: the API has no season phase in {ids}")
    if route == "/team-standings":
        got = {r.get("season_id") for r in _rows(data)}
    else:
        got = {_obj(ph).get("season_id") for ph in _obj(_obj(data).get("seasons")).values()}
    if got != want:
        raise AssetFetchError(f"{where}: asked for season_ids {sorted(want)}, got {sorted(map(str, got))}")


# --- the getter -----------------------------------------------------------------------------------------------------


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a W League route, refuse the empty, partial, non-final and out-of-scope answers and stamp the request.

    Args:
        url: fully-built request URL (``https://api.wjbl.01core.app/api/...``).
        params: query-string parameters; ``None`` values are dropped. ``season`` (the sdv END year) is sent as the
            opaque ``league_id``; an explicit ``league_id`` is sent as given. ``season_ids`` may be an int, a list
            or a comma string.
        **kwargs: forwarded to the HTTP getter (``session`` defaults to the shared polite session).

    Returns:
        The body (a bare list comes back as ``{"items": [...]}``) with ``{"play_schedule_id": ..., "league_id": ...,
        "season": END, ...}`` under :data:`REQUEST_KEY`.

    Raises:
        NoDataError: the API answered no rows (an empty list, a game without play-by-play, an edition without
            months or games, an id it does not know).
        ValueError: ``season`` is not an END year of a W League edition, an id is not an integer, ``game_month`` is
            not ``YYYY-MM``, or the API answered HTTP 400 / 422.
        AssetFetchError: any other failure: an error envelope (HTTP 200), any other non-2xx, an empty / non-JSON
            body, a game that is not final, a truncated play-by-play, or a body for another game, league edition,
            month, team, season phase or player than the one asked for.
    """
    route = _route(url)
    wire = {k: v for k, v in (params or {}).items() if v is not None}
    dep = {k: v for k, v in kwargs.items() if k in ("session", "retry_statuses")}
    stamp: Dict[str, Any] = {}
    if "season" in wire:
        end = wire.pop("season")
        wire.setdefault("league_id", league_id(end, **dep))
        stamp["season"] = int(end)
    for key in _INT_PARAMS:
        if key in wire:
            wire[key] = _int_param(wire[key], key)
    if "season_ids" in wire:
        wire["season_ids"] = _season_ids(wire["season_ids"])
    if "game_month" in wire and not _MONTH.match(str(wire["game_month"])):
        raise ValueError(f"game_month must be 'YYYY-MM' (see wjbl_game_months); got {wire['game_month']!r}")
    needs = {
        "/play-schedule-months": ("league_id",),
        "/team-standings": ("season_ids",),
        "/team-ranking": ("season_ids",),
        "/team/roster": ("team_id",),
        "/player": ("player_id",),
        **{r: ("play_schedule_id",) for r in (*_GAME_ROUTES, "/play-schedule/detail")},
    }.get(route, ())
    missing = [n for n in needs if n not in wire]
    if missing:
        raise ValueError(f"{route} needs {', '.join(missing)}")
    if route == "/play-schedules" and "league_id" not in wire:
        raise ValueError("/play-schedules needs a season (END year) or league_id")
    if route == "/leagues":
        wire.setdefault("target", "play_schedule")
    kwargs.setdefault("session", _bb.SESSION)
    if route in _GAME_ROUTES:
        _final_header(
            wire["play_schedule_id"], **dep
        )  # first: an unknown or live game is refused before the body is read
    data = _fetch(url, wire, **kwargs)
    _check(route, wire, data, **dep)
    if route == "/play-schedule/detail":
        _CACHE[("header", wire["play_schedule_id"])] = data
    shaped: Any = data if isinstance(data, dict) else {"items": data}
    out = dict(shaped)
    if route == "/leagues":
        _CACHE[("leagues",)] = _rows(data)
    out[REQUEST_KEY] = {**{k: v for k, v in wire.items() if k != "target"}, **stamp}
    return out
