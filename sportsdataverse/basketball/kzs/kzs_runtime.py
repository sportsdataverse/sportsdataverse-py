"""Runtime for the generated KZS wrappers (Basketball Federation of Slovenia, ``api.kzs.si/api/v1/public``).

Every request goes through the shared :data:`sportsdataverse.basketball.basketball_runtime.SESSION`, which waits at
least one second before **every** attempt, retries included. The API needs no key and no proxy. ``api.kzs.si/robots.txt``
answers 401 and ``www.kzs.si/robots.txt`` 404, so no ``Crawl-delay`` is published; one request a second is the pace.

What the live API does (sdv-internal-refs ``kzs/``, probed 2026-10-12):

* **Every answer is an envelope** ``{"status": "OK", "data": ...}``. ``_get`` returns ``data``: a dict gets the request
  stamped under ``_request`` (``competition_id``, ``match_id``, ``team_id``, ``player_id``, ``season_id``, ``season``,
  the leaderboard ``stats_category``), which the parsers read; a bare list (``playingDays``) is wrapped as
  ``{"items": [...], "_request": {...}}``.
* **Errors are HTTP 400**, never 404: ``INVALID_REQUEST`` (``Match with id 99999999 does not exist``) and
  ``VALIDATION_ERROR`` (a bad or missing parameter). Both are ``ValueError`` ("the request is wrong"), as everywhere
  in the package: an unknown id is a request the API cannot answer. A 200 whose ``status`` is not ``OK`` is
  ``AssetFetchError``.
* **Seasons are opaque ids** (27 = 2026/2027). The public argument is the END year (2027); ``_get`` maps it with
  :data:`SEASON_IDS` (the 39 rows of ``/seasons/``, verified against the live list) before sending ``seasonId``.
* **List routes default to 150 rows and say nothing**: ``count``, ``offset`` and ``limit`` are always 0. Every list
  wrapper sends a large ``limit`` and a page that comes back full (``len(items) == limit``) is ``AssetFetchError``.
* **Scope.** A body must be the one asked for, else ``AssetFetchError`` (never an empty frame): competition, match,
  team and player ids are checked type-strictly against the body; a leaderboard, a team list, the rounds and the
  playing days are checked against the competition (its standings or phases, one extra request, cached for the
  process); a match's play-by-play and box score are checked against the match header (one extra request, cached
  once the header says ``FINISHED``): the game must be final, the match-team ids must be the header's two, in the
  header's home/away order. ``/teams/{id}/general`` and ``/teamPlayers/`` echo nothing to compare with, so they are
  not scoped (the cap on the list is still enforced).
* **Nothing here** is ``NoDataError``: an empty list, a competition without standings, a final game with no
  play-by-play (a coverage gap: 2007-08 and some 2020-21 games) or no box score (2007-08 answers only scores).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

BASE = "https://api.kzs.si/api/v1/public"

#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"

#: Public END year -> opaque KZS season id: ids 1-30 are 2000/2001 ... 2029/2030 (END = 2000 + id), ids 31-39 are
#: 1991/1992 ... 1999/2000 (END = 1961 + id). Checked against the live ``/seasons/`` list on 2026-10-12.
SEASON_IDS: Dict[int, int] = {**{2000 + i: i for i in range(1, 31)}, **{1961 + i: i for i in range(31, 40)}}
_END_OF_ID = {v: k for k, v in SEASON_IDS.items()}

_MISSING = object()
_PATHS: List[Tuple[str, "re.Pattern[str]", Tuple[str, ...]]] = [
    (route, re.compile("^" + pattern + "/?$"), names)
    for route, pattern, names in [
        ("/seasons/", r"/seasons", ()),
        ("/competitions/", r"/competitions", ()),
        ("/competitions/{competition_id}", r"/competitions/(\d+)", ("competition_id",)),
        ("/competitions/{competition_id}/rounds", r"/competitions/(\d+)/rounds", ("competition_id",)),
        ("/competitions/{competition_id}/playerStats", r"/competitions/(\d+)/playerStats", ("competition_id",)),
        ("/competitions/{competition_id}/teamStats", r"/competitions/(\d+)/teamStats", ("competition_id",)),
        ("/matches/", r"/matches", ()),
        ("/matches/standings", r"/matches/standings", ()),
        ("/matches/{competition_id}/playingDays", r"/matches/(\d+)/playingDays", ("competition_id",)),
        ("/matches/{match_id}", r"/matches/(\d+)", ("match_id",)),
        ("/matches/{match_id}/playByPlay", r"/matches/(\d+)/playByPlay", ("match_id",)),
        ("/matches/{match_id}/stats", r"/matches/(\d+)/stats", ("match_id",)),
        ("/teams/", r"/teams", ()),
        ("/teams/{team_id}", r"/teams/(\d+)", ("team_id",)),
        ("/teams/{team_id}/general", r"/teams/(\d+)/general", ("team_id",)),
        ("/teamPlayers/", r"/teamPlayers", ()),
        ("/players/{player_id}", r"/players/(\d+)", ("player_id",)),
        ("/players/{player_id}/stats", r"/players/(\d+)/stats", ("player_id",)),
    ]
]

#: Leaderboard ``sortBy`` -> the row column that must be ordered (the only echo of ``sortBy`` / ``sort``).
SORT_COLUMN = {"avg": "average", "sum": "sum", "max": "max"}
_LIST_ROUTES = frozenset(
    {
        "/seasons/",
        "/competitions/",
        "/competitions/{competition_id}/playerStats",
        "/competitions/{competition_id}/teamStats",
        "/matches/",
        "/teams/",
        "/teamPlayers/",
    }
)
_DEFAULT_LIMIT = 150

#: Dependency bodies (a competition, its team set, a FINISHED header, a season's competition ids), cached for the
#: process. Only a successful, final answer is cached. ``clear_cache()`` empties it.
_CACHE: Dict[Tuple[Any, ...], Any] = {}


def clear_cache() -> None:
    """Forget the cached dependency bodies (competitions, standings team sets, finished match headers)."""
    _CACHE.clear()


def season_id(season: Any) -> int:
    """The opaque KZS season id of an END year.

    Args:
        season: the sdv season, an END year (2027 = 2026/2027).

    Returns:
        The KZS season id (27 for 2027).

    Raises:
        ValueError: ``season`` is not an END year between 1992 and 2030.
    """
    if isinstance(season, bool) or not isinstance(season, (int, str)) or not str(season).strip().lstrip("-").isdigit():
        raise ValueError(f"season must be an END year such as 2027 (2026/2027); got {season!r}")
    end = int(season)
    if end not in SEASON_IDS:
        raise ValueError(f"season must be an END year between 1992 and 2030 (2027 = 2026/2027); got {end}")
    return SEASON_IDS[end]


def _route(url: str) -> Tuple[str, Dict[str, int]]:
    """The route template of ``url`` and its path ids; ``ValueError`` for a path the family does not wrap."""
    path = urlsplit(url).path
    tail = path.split("/api/v1/public", 1)[1] if "/api/v1/public" in path else path
    for route, pattern, names in _PATHS:
        m = pattern.match(tail)
        if m:
            return route, {n: int(g) for n, g in zip(names, m.groups())}
    raise ValueError(f"not a KZS route: {tail!r}")


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


def _fetch(url: str, params: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
    """GET ``url`` through the polite session and unwrap the ``{status, data}`` envelope."""
    kwargs.setdefault("session", _bb.SESSION)
    # a 400 / 422 stays ValueError: the request is wrong (EX-SYNC-b, the repo-wide vocabulary)
    body = _bb._fetch_json(url, dict(params or {}), **kwargs)
    where = _rt._where(url)
    if not isinstance(body, dict):
        raise AssetFetchError(f"{where} answered a {type(body).__name__}, not an envelope")
    if not _same(body.get("status"), "OK") or "data" not in body:
        raise AssetFetchError(f"{where} answered status {str(body.get('status'))[:40]!r}, not OK")
    return body["data"]


# --- dependencies (one extra, cached request each) ------------------------------------------------------------------


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-8)
def _competition(competition_id: int, **kwargs: Any) -> Mapping[str, Any]:
    key = ("competition", competition_id)
    if key not in _CACHE:
        data = _obj(_fetch(f"{BASE}/competitions/{competition_id}", None, **kwargs))
        if not _same(data.get("id"), competition_id):
            raise AssetFetchError(f"competition {competition_id}: the header answered id {data.get('id')!r}")
        _CACHE[key] = data
    return _CACHE[key]


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-8)
def _team_ids(competition_id: int, **kwargs: Any) -> set:
    """Team ids in the standings of a competition (the competition's entrants)."""
    key = ("teams", competition_id)
    if key not in _CACHE:
        data = _obj(_fetch(f"{BASE}/matches/standings", {"competitionId": competition_id}, **kwargs))
        if not _same(_obj(data.get("competition")).get("id"), competition_id):
            raise AssetFetchError(f"competition {competition_id}: the standings dependency is another competition")
        _CACHE[key] = {
            r.get("teamId")
            for ph in _rows(data.get("phases"))
            for block in (ph, *_rows(ph.get("groups")))
            for r in _rows(block.get("standings"))
        }
    return _CACHE[key]


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-8)
def _header(match_id: int, **kwargs: Any) -> Mapping[str, Any]:
    """The match header, which must be FINISHED (cached once it is)."""
    key = ("header", match_id)
    if key not in _CACHE:
        head = _obj(_fetch(f"{BASE}/matches/{match_id}", None, **kwargs))
        _need_final(match_id, head)
        _CACHE[key] = head
    return _CACHE[key]


def _need_final(match_id: int, head: Mapping[str, Any]) -> None:
    if not _same(head.get("id"), match_id):
        raise AssetFetchError(f"match {match_id}: the header answered id {head.get('id')!r}")
    if not _same(head.get("status"), "FINISHED"):
        raise AssetFetchError(f"match {match_id}: the game is not final (status={head.get('status')!r})")


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-8)
def _season_competitions(sid: int, **kwargs: Any) -> set:
    key = ("season", sid)
    if key not in _CACHE:
        data = _obj(_fetch(f"{BASE}/competitions/", {"seasonId": sid, "limit": 10000}, **kwargs))
        items = _rows(data.get("items"))
        _whole(f"competitions of season {sid}", items, 10000)
        _CACHE[key] = {c.get("id") for c in items if _same(_obj(c.get("season")).get("id"), sid)}
    return _CACHE[key]


# --- checks ---------------------------------------------------------------------------------------------------------


def _whole(where: str, items: List[Any], limit: Optional[int]) -> None:
    """A page that comes back full was cut: the API gives no other sign."""
    cap = _DEFAULT_LIMIT if limit is None else limit
    if len(items) >= cap:
        raise AssetFetchError(f"{where}: truncated page ({len(items)} rows at limit={cap}); ask for a larger limit")


def _check(route: str, ids: Mapping[str, int], params: Mapping[str, Any], data: Any, **kw: Any) -> None:
    """Raise ``NoDataError`` for a row-less body, ``AssetFetchError`` for a partial or out-of-scope one."""
    where = f"api.kzs.si{route}"
    if route in _LIST_ROUTES:
        items = _obj(data).get("items")
        if not isinstance(items, list):
            raise AssetFetchError(f"{where}: a list route answered no items[]")
        _whole(where, items, params.get("limit"))
        if not items:
            raise NoDataError(f"{where}: the API answered no rows")
    if route == "/competitions/" and params.get("seasonId") is not None:
        for c in _rows(data["items"]):
            if not _same(_obj(c.get("season")).get("id"), params["seasonId"]):
                raise AssetFetchError(
                    f"{where}: asked for seasonId={params['seasonId']}, got competition {c.get('id')!r}"
                )
    elif route == "/competitions/{competition_id}":
        _id(where, data, ids["competition_id"], "competition")
    elif route == "/competitions/{competition_id}/rounds":
        want = {p.get("id") for p in _rows(_competition(ids["competition_id"], **kw).get("phases"))}
        got = {p.get("id") for p in _rows(_obj(data).get("phases"))}
        if not got:
            raise NoDataError(f"{where}: no rounds")
        if got != want:
            raise AssetFetchError(
                f"{where}: phases {sorted(map(str, got))} are not the competition's {sorted(map(str, want))}"
            )
    elif route.endswith(("/playerStats", "/teamStats")):
        _leaderboard(where, ids["competition_id"], params, _rows(data["items"]), **kw)
    elif route == "/matches/":
        _matches(where, params, _rows(data["items"]))
    elif route == "/matches/{match_id}":
        _id(where, data, ids["match_id"], "match")
    elif route.endswith("/playByPlay"):
        _pbp(where, ids["match_id"], data, **kw)
    elif route.endswith("/stats") and route.startswith("/matches/"):
        _box(where, ids["match_id"], data, **kw)
    elif route == "/matches/standings":
        if not _same(_obj(_obj(data).get("competition")).get("id"), params.get("competitionId")):
            raise AssetFetchError(f"{where}: asked for competitionId={params.get('competitionId')!r}, got another")
        if not any(
            _obj(p).get("standings") or any(_rows(g.get("standings")) for g in _rows(p.get("groups")))
            for p in _rows(_obj(data).get("phases"))
        ):
            raise NoDataError(f"{where}: the competition has no standings rows")
    elif route == "/matches/{competition_id}/playingDays":
        _days(where, ids["competition_id"], data, **kw)
    elif route == "/teams/":
        teams = _team_ids(params["competitionId"], **kw)
        bad = [r.get("id") for r in _rows(data["items"]) if r.get("id") not in teams]
        if bad:
            raise AssetFetchError(f"{where}: team ids {bad[:5]} are not in competition {params['competitionId']}")
    elif route == "/teams/{team_id}":
        _id(where, data, ids["team_id"], "team")
    elif route == "/players/{player_id}":
        _id(where, data, ids["player_id"], "player")
    elif route == "/players/{player_id}/stats":
        rows = _rows(_obj(data).get("competitionStats"))
        if not rows:
            raise NoDataError(f"{where}: the player has no competition rows in season {params['seasonId']}")
        comps = _season_competitions(params["seasonId"], **kw)
        bad = [r.get("competitionId") for r in rows if r.get("competitionId") not in comps]
        if bad:
            raise AssetFetchError(f"{where}: competitions {bad} are not in season {params['seasonId']}")


def _id(where: str, data: Any, want: int, what: str) -> None:
    got = _obj(data).get("id", _MISSING)
    if not _same(got, want):
        raise AssetFetchError(f"{where}: asked for {what} {want}, the body has id={None if got is _MISSING else got!r}")


def _leaderboard(where: str, cid: int, params: Mapping[str, Any], rows: List[Mapping[str, Any]], **kw: Any) -> None:
    teams = _team_ids(cid, **kw)
    bad = [r.get("teamId") for r in rows if r.get("teamId") not in teams]
    if bad:
        raise AssetFetchError(f"{where}: team ids {bad[:5]} are not in competition {cid}")
    col, direction = SORT_COLUMN.get(str(params.get("sortBy"))), params.get("sort")
    if col is None or direction not in ("ASC", "DESC"):
        return  # only a request that sent both sortBy and sort can be checked (the rows echo neither)
    vals: List[float] = []
    for r in rows:
        v = r.get(col)
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            raise AssetFetchError(f"{where}: leaderboard rows without a numeric {col}")
        vals.append(v)
    if vals != sorted(vals, reverse=direction == "DESC"):
        raise AssetFetchError(f"{where}: rows are not ordered {direction} by {col}: sortBy/sort ignored?")


def _matches(where: str, params: Mapping[str, Any], rows: List[Mapping[str, Any]]) -> None:
    cid = params.get("competitionId")
    lo, hi = params.get("from"), params.get("to")
    for m in rows:
        if cid is not None and not any(_same(c.get("competitionId"), cid) for c in _rows(m.get("competitions"))):
            raise AssetFetchError(f"{where}: match {m.get('id')!r} is not in competition {cid}")
        stamp = m.get("dateTime")
        if (lo or hi) and not isinstance(stamp, str):
            raise AssetFetchError(f"{where}: match {m.get('id')!r} has no dateTime to check against from/to")
        if isinstance(stamp, str) and (lo and stamp[:19] < str(lo)[:19] or hi and stamp[:19] > str(hi)[:19]):
            raise AssetFetchError(f"{where}: match {m.get('id')!r} at {stamp!r} is outside {lo}..{hi}")


def _pbp(where: str, mid: int, data: Any, **kw: Any) -> None:
    head = _header(mid, **kw)
    home, away = _obj(head.get("firstTeam")).get("id"), _obj(head.get("secondTeam")).get("id")
    acts = _rows(_obj(data).get("actions"))
    if not acts:
        raise NoDataError(f"{where}: the game has no play-by-play (a coverage gap: 2007-08 and some later games)")
    first = {p.get("id") for p in _rows(_obj(data).get("firstTeamPlayers"))}
    second = {p.get("id") for p in _rows(_obj(data).get("secondTeamPlayers"))}
    seen = set()
    for a in acts:
        team = a.get("matchTeamId")
        if team is None:
            continue
        if team not in (home, away):
            raise AssetFetchError(f"{where}: action matchTeamId {team!r} is not match {mid}'s ({home}, {away})")
        pid = a.get("playerId")
        if pid is not None and ((pid in first and team != home) or (pid in second and team != away)):
            raise AssetFetchError(f"{where}: player {pid} acts for matchTeamId {team}: home/away order is wrong")
        seen.add(team)
    if seen != {home, away}:
        raise AssetFetchError(f"{where}: actions carry matchTeamIds {sorted(seen)}, not both of match {mid}'s")


def _box(where: str, mid: int, data: Any, **kw: Any) -> None:
    body = _obj(data)
    if not _same(body.get("matchId"), mid):
        raise AssetFetchError(f"{where}: asked for match {mid}, the body has matchId={body.get('matchId')!r}")
    head = _header(mid, **kw)
    for side in ("firstTeam", "secondTeam"):
        block = _obj(body.get(side))
        if block and not _same(block.get("teamId"), _obj(head.get(side)).get("teamId")):
            raise AssetFetchError(
                f"{where}: {side}.teamId {block.get('teamId')!r} is not the header's (home/away order)"
            )
    if not all(_rows(_obj(body.get(s)).get("playerStats")) for s in ("firstTeam", "secondTeam")):
        raise NoDataError(f"{where}: the game has no box score (2007-08 answers only per-minute scores)")


def _days(where: str, cid: int, data: Any, **kw: Any) -> None:
    days = _rows(_obj(data).get("items"))
    if not days:
        raise NoDataError(f"{where}: no playing days")
    phases = _rows(_competition(cid, **kw).get("phases"))
    lo = min((str(p.get("startDate") or "")[:10] for p in phases), default="")
    hi = max((str(p.get("endDate") or "")[:10] for p in phases), default="")
    bad = [d.get("date") for d in days if not (lo and lo <= str(d.get("date")) <= hi)]
    if bad:
        raise AssetFetchError(f"{where}: days {bad[:3]} are outside competition {cid}'s {lo or '?'}..{hi or '?'}")


# --- the getter -----------------------------------------------------------------------------------------------------


def _context(ids: Mapping[str, int], params: Mapping[str, Any]) -> Dict[str, Any]:
    ctx: Dict[str, Any] = dict(ids)
    if params.get("competitionId") is not None:
        ctx["competition_id"] = params["competitionId"]
    if params.get("teamId") is not None:
        ctx["team_id"] = params["teamId"]
    if params.get("seasonId") is not None:
        ctx["season_id"] = params["seasonId"]
        if params["seasonId"] in _END_OF_ID:
            ctx["season"] = _END_OF_ID[params["seasonId"]]
    if params.get("statsCategory") is not None:
        ctx["stats_category"] = params["statsCategory"]
    return ctx


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a KZS route, unwrap the envelope, refuse the partial or out-of-scope answers and stamp the request.

    Args:
        url: fully-built request URL (``https://api.kzs.si/api/v1/public/...``).
        params: query-string parameters; ``None`` values are dropped. ``season`` (the sdv END year) is sent as the
            opaque ``seasonId``; an explicit ``seasonId`` is sent as given.
        **kwargs: forwarded to the HTTP getter (``session`` defaults to the shared polite session).

    Returns:
        ``data`` of the envelope with ``{"competition_id": ..., "season": END, ...}`` under :data:`REQUEST_KEY` (a
        bare list comes back as ``{"items": [...], "_request": {...}}``).

    Raises:
        NoDataError: the API answered no rows (an empty list, a competition without standings, a final game with no
            play-by-play or box score).
        ValueError: ``season`` is not an END year, an id is not an integer, or HTTP 400 (the API's only error
            status: an unknown id or a bad parameter).
        AssetFetchError: any other failure: any other non-2xx, an empty / non-JSON body, an envelope whose status is not ``OK``, a
            full list page (cut at the limit), a game that is not final, or a body for another competition, match,
            team, player or season than the one asked for.
    """
    wire = {k: v for k, v in (params or {}).items() if v is not None}
    if "season" in wire:
        wire["seasonId"] = season_id(wire.pop("season"))
    for key in ("competitionId", "teamId", "seasonId", "limit"):
        if key in wire:
            wire[key] = _int_param(wire[key], key)
    route, ids = _route(url)
    if route.endswith(("/playerStats", "/teamStats")):
        wire.setdefault("competitionId", ids["competition_id"])  # the API wants it in the query as well as the path
    kwargs.setdefault("session", _bb.SESSION)
    data = _fetch(url, wire, **kwargs)
    dep = {k: v for k, v in kwargs.items() if k in ("session", "retry_statuses")}
    if data is None or data == {} or data == []:
        raise NoDataError(f"{_rt._where(url)}: the API answered no data")
    shaped: Any = data if isinstance(data, dict) else {"items": data}
    _check(route, ids, wire, shaped, **dep)
    out = dict(shaped)
    out[REQUEST_KEY] = _context(ids, wire)
    return out
