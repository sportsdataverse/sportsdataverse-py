"""Runtime for the generated EASL wrappers (East Asia Super League, via an UNOFFICIAL third-party sync server).

**Unofficial intermediary.** ``https://easl.joshricha.com/api/v1`` is an Express server run by an individual. It is not
EASL's and not Sportradar's: it mirrors Sportradar DataCore (organisation ``b1e0z``) into SQLite and serves it keyless.
It can change or vanish without notice, and a first-party DataCore feed needs credentials this package does not have.

Every request goes through the shared :data:`sportsdataverse.basketball.basketball_runtime.SESSION`, which waits at
least one second before **every** attempt, retries included (the server advertises ``RateLimit: "100-in-1sec"``; one
request a second is far below it), and ``dl_utils.download`` honours ``Retry-After`` on 429 and 503. The host serves no
``robots.txt`` (404) and states no terms. No key, cookie or proxy is needed.

What the live server does (sdv-internal-refs ``easl/``, probed 2026-10-12):

* **Seasons.** The public argument is the END year (2027 = EASL 26-27). ``/metadata/seasons[].year`` is the START year,
  so the runtime reads that list (once per process; :func:`clear_cache`) and maps ``season`` to the DataCore
  ``seasonId``; ``season=None`` means the ACTIVE season. ``/matches``, ``/standings`` and ``/leaders`` default to the
  ACTIVE season on the server, but the runtime always sends the resolved ``seasonId``. An unknown season is a
  ``ValueError``.
* **Errors.** An unknown ``fixtureId`` is a bare HTTP 500 (never retried; only 429, 502, 503, 504 are); an unknown id on
  a sub-route is a 200 with an empty body, and an unknown ``seasonId`` is a 404 on ``/standings`` and an empty 200
  elsewhere. A malformed id is a ``ValueError`` before any request.
* **Finished games only.** A game route first reads ``/matches/{fixtureId}``: the game must be ``CONFIRMED`` with two
  scored teams, else ``AssetFetchError``. The box score must belong to the fixture and its player points must add up to
  each team's score (this refuses the one-team, four-row box the server serves for fixture ``2425016``); the
  play-by-play must carry the fixture's two teams, end on the final score on its LAST event, in its highest period
  (4 or more) and in the last period of the period table; the team totals and the period table must add up to the
  score.
* **Freshness.** For the ACTIVE season's fixture lists the runtime reads ``/health`` first: the server clock must be
  within a day of ours, and a fixture still ``SCHEDULED`` more than a day after its tip-off means the sync has stalled
  (``AssetFetchError``). This proves the mirror kept up with the games that were played; it does not prove a game is
  final upstream, and a mirror that stalled before the first stale tip-off passes. Past seasons are not checked.
* **Scope.** Seasons are tied by the ``seasonId`` on every row (fixtures, leaders), by the season dates (the standings'
  ``lastFiveMatches``) or, for a team's fixtures, by the row's own ``seasonId`` or the season half of its ``stage.id``
  (neither present: ``AssetFetchError``, fail closed). ``/players/{id}/stats`` accepts but IGNORES ``seasonId``; its
  body carries no id, so it is tied to the request only by a positive ``games`` count. ``/health``, ``/teams`` and
  ``/players`` are global lists with nothing to echo.
* **Nothing here** is ``NoDataError``: an empty list, an empty leaders object, a team with no games in a season.

The API has no paging (every list comes back whole); a body that carries a pager key is refused.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

BASE = "https://easl.joshricha.com/api/v1"

#: DataCore organisation id of the EASL on every season, team and player row.
ORG = "b1e0z"

#: 500 is the server's answer for an unknown fixtureId: deterministic, never retried.
RETRY_STATUSES = frozenset({429, 502, 503, 504})

#: Key the request is stamped under; the parsers read it.
REQUEST_KEY = "_request"

#: A fixture still SCHEDULED this long after its tip-off, or a server clock this far from ours, means a stalled sync.
STALE_AFTER = timedelta(days=1)

_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
_UUID_RE = re.compile(_UUID)
_PAGER_KEYS = frozenset({"next", "next_url", "nextPage", "totalPages", "pagination", "cursor"})

#: Process cache: the seasons list and the header of every FINAL game. ``clear_cache()`` empties it.
_CACHE: Dict[Tuple[Any, ...], Any] = {}


def clear_cache() -> None:
    """Forget the cached seasons list and game headers."""
    _CACHE.clear()


def _now() -> datetime:
    """The local UTC clock (a seam for the tests)."""
    return datetime.now(timezone.utc)


# --- routes ---------------------------------------------------------------------------------------------------------

#: (route name, compiled path, id names). Ids are UUIDs; a path with a malformed id is a ValueError.
_ROUTES: List[Tuple[str, "re.Pattern[str]", Tuple[str, ...]]] = [
    (name, re.compile("^" + pattern + "/?$"), ids)
    for name, pattern, ids in [
        ("health", r"/health", ()),
        ("seasons", r"/metadata/seasons", ()),
        ("matches", r"/matches", ()),
        ("match", rf"/matches/({_UUID})", ("fixture_id",)),
        ("boxscore", rf"/matches/({_UUID})/boxscore", ("fixture_id",)),
        ("playbyplay", rf"/matches/({_UUID})/playbyplay", ("fixture_id",)),
        ("teamstats", rf"/matches/({_UUID})/teamstats", ("fixture_id",)),
        ("period", rf"/matches/({_UUID})/teamstats/by/period", ("fixture_id",)),
        ("standings", r"/standings", ()),
        ("leaders", r"/leaders", ()),
        ("teams", r"/teams", ()),
        ("team", rf"/teams/({_UUID})", ("team_id",)),
        ("team_matches", rf"/teams/({_UUID})/matches", ("team_id",)),
        ("team_stats", rf"/teams/({_UUID})/stats", ("team_id",)),
        ("team_leaders", rf"/teams/({_UUID})/leaders", ("team_id",)),
        ("players", r"/players", ()),
        ("player", rf"/players/({_UUID})", ("player_id",)),
        ("player_stats", rf"/players/({_UUID})/stats", ("player_id",)),
    ]
]
_GAME_ROUTES = frozenset({"match", "boxscore", "playbyplay", "teamstats", "period"})


def _route(url: str) -> Tuple[str, Dict[str, str]]:
    """The route name of ``url`` and its path ids; ``ValueError`` for a path the family does not wrap."""
    path = url.split("://", 1)[-1].split("?", 1)[0]
    path = path[path.index("/") :] if "/" in path else "/"
    path = path[len("/api/v1") :] if path.startswith("/api/v1/") else path
    for name, pattern, ids in _ROUTES:
        m = pattern.match(path)
        if m:
            return name, dict(zip(ids, m.groups()))
    if re.match(r"^/(matches|teams|players)/[^/]+(/|$)", path):
        raise ValueError(f"the id in {path!r} must be a DataCore UUID such as 10af8fd4-106e-11f1-960b-5798087c7aa3")
    raise ValueError(f"not an EASL route: {path!r}")


# --- helpers --------------------------------------------------------------------------------------------------------


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality (``True == 1`` in Python)."""
    return type(got) is type(want) and got == want


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _rows(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _int(value: Any) -> Optional[int]:
    """An int as sent, or a digit string; ``None`` otherwise (bools are not ints)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return int(value) if isinstance(value, str) and value.isdigit() else None


def _utc(text: Any) -> Optional[datetime]:
    """``2026-10-13T00:16:10.324Z`` or ``2026-10-03T08:10:00`` (naive means UTC) as an aware datetime, else ``None``."""
    try:
        d = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _season_arg(value: Any) -> int:
    if isinstance(value, bool) or not str(value).strip().isdigit():
        raise ValueError(f"season must be an END year such as 2026 (EASL 25-26); got {value!r}")
    return int(value)


def _check_body(where: str, body: Any) -> Any:
    """Refuse an error envelope, a body with a pager key or a body that is not JSON data."""
    if body is None or body == "" or isinstance(body, (str, bool, int, float)):
        raise AssetFetchError(f"{where}: expected a JSON object or list, got {body!r}"[:200])
    if isinstance(body, dict):
        status = str(body.get("status", "")).lower()
        if "error" in body or body.get("success") is False or status in ("error", "fail", "failed"):
            raise AssetFetchError(f"{where}: answered an error envelope")
        if _PAGER_KEYS & set(body):
            raise AssetFetchError(f"{where}: the body carries a paging key; the API is documented as unpaged")
    return body


def _fetch(path: str, params: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Any:
    """GET ``BASE + path`` through the polite session; HTTP 400 / 422 is ``ValueError``, a 204 / 205 ``_rt._NoContent``."""
    kwargs.setdefault("session", _bb.SESSION)
    kwargs.setdefault("retry_statuses", RETRY_STATUSES)
    headers = {"Accept": "application/json"}
    headers.update(kwargs.pop("headers", None) or {})
    # a 400 / 422 stays ValueError: the request is wrong (EX-SYNC-b, the repo-wide vocabulary)
    body = _bb._fetch_json(BASE + path, dict(params or {}), headers=headers, **kwargs)
    return _check_body(f"easl.joshricha.com/api/v1{path}", body)


def _list(where: str, body: Any) -> List[Any]:
    if not isinstance(body, list):
        raise AssetFetchError(f"{where}: expected a JSON list, got {type(body).__name__}")
    return body


def _wrap(data: Any, stamp: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(data) if isinstance(data, dict) else {"items": data}
    out[REQUEST_KEY] = dict(stamp)
    return out


# --- seasons --------------------------------------------------------------------------------------------------------


def _seasons(**kw: Any) -> List[Mapping[str, Any]]:
    """The validated ``/metadata/seasons`` rows (cached for the process)."""
    if ("seasons",) not in _CACHE:
        where = "easl.joshricha.com/api/v1/metadata/seasons"
        rows = _rows(_list(where, _fetch("/metadata/seasons", None, **kw)))
        if not rows:
            raise NoDataError(f"{where}: the server lists no seasons")
        if any(r.get("organizationId") != ORG or not isinstance(r.get("seasonId"), str) for r in rows):
            raise AssetFetchError(f"{where}: a season row is not organisation {ORG} or has no seasonId")
        if not any(r.get("status") == "ACTIVE" for r in rows):
            raise AssetFetchError(f"{where}: no ACTIVE season")
        _CACHE[("seasons",)] = rows
    return _CACHE[("seasons",)]


def _season(season: Any, **kw: Any) -> Tuple[Mapping[str, Any], int]:
    """``(season row, END year)`` of a public ``season`` (``None`` = the ACTIVE season); ``ValueError`` if unknown."""
    rows = _seasons(**kw)
    if season is None:
        active = [r for r in rows if r.get("status") == "ACTIVE"]
        if len(active) != 1 or _int(active[0].get("year")) is None:
            raise AssetFetchError(
                "easl.joshricha.com/api/v1/metadata/seasons: not exactly one ACTIVE season with a year"
            )
        return active[0], _int(active[0].get("year")) + 1  # type: ignore[operator]
    end = _season_arg(season)
    for r in rows:
        if _int(r.get("year")) == end - 1:
            return r, end
    have = sorted((_int(r.get("year")) or 0) + 1 for r in rows)
    raise ValueError(f"no EASL season ending {end}; the server lists END years {have}")


def _fresh(**kw: Any) -> datetime:
    """Server clock from ``/health`` (must say OK and be within a day of ours); the basis of the stall check."""
    where = "easl.joshricha.com/api/v1/health"
    body = _obj(_fetch("/health", None, **kw))
    server = _utc(body.get("timestamp"))
    if body.get("status") != "OK" or server is None:
        raise AssetFetchError(f"{where}: not OK or no timestamp: freshness unverified")
    if abs(_now() - server) > STALE_AFTER:
        raise AssetFetchError(f"{where}: the server clock {server:%Y-%m-%d %H:%M}Z is more than a day from ours")
    return server


def _no_stale(rows: List[Mapping[str, Any]], **kw: Any) -> None:
    """Refuse an ACTIVE-season fixture list in which a game is still SCHEDULED a day after its tip-off."""
    server = _fresh(**kw)
    late = [
        r.get("fixtureId")
        for r in rows
        if r.get("status") == "SCHEDULED" and (_utc(r.get("startTimeUTC")) or server) < server - STALE_AFTER
    ]
    if late:
        raise AssetFetchError(
            f"easl.joshricha.com/api/v1/matches: {len(late)} fixtures are still SCHEDULED more than a day after "
            "tip-off: the mirror looks stalled"
        )


# --- games ----------------------------------------------------------------------------------------------------------


def _header(fid: str, **kw: Any) -> Dict[str, Any]:
    """The checked ``/matches/{fid}`` body plus ``teams`` (ids) and ``scores`` (team id -> final points)."""
    key = ("game", fid)
    if key not in _CACHE:
        where = f"easl.joshricha.com/api/v1/matches/{fid}"
        body = _obj(_fetch(f"/matches/{fid}", None, **kw))
        if not _same(body.get("fixtureId"), fid):
            raise AssetFetchError(f"{where}: asked for {fid!r}, the body has fixtureId={body.get('fixtureId')!r}")
        if body.get("status") != "CONFIRMED":
            raise AssetFetchError(
                f"{where}: fixture status {body.get('status')!r}, not CONFIRMED: only finished games are served"
            )
        comps = _rows(body.get("competitors"))
        scores = {c.get("teamId"): _int(c.get("score")) for c in comps}
        if len(comps) != 2 or len(scores) != 2 or any(not isinstance(t, str) or s is None for t, s in scores.items()):
            raise AssetFetchError(f"{where}: the fixture lacks two scored competitors")
        _CACHE[key] = {"body": body, "teams": list(scores), "scores": scores}
    return _CACHE[key]


def _periods(body: Any) -> List[int]:
    return sorted({int(k[7:]) for r in _rows(body) for k in r if re.fullmatch(r"period_\d+", k)})


def _period_table(fid: str, ctx: Mapping[str, Any], **kw: Any) -> List[Mapping[str, Any]]:
    where = f"easl.joshricha.com/api/v1/matches/{fid}/teamstats/by/period"
    rows = _rows(_list(where, _fetch(f"/matches/{fid}/teamstats/by/period", None, **kw)))
    if len(rows) != 2 or {_obj(r.get("team")).get("teamId") for r in rows} != set(ctx["teams"]):
        raise AssetFetchError(f"{where}: the rows are not this fixture's two teams")
    for r in rows:
        total = sum(_int(_obj(v).get("points")) or 0 for k, v in r.items() if re.fullmatch(r"period_\d+", k))
        want = ctx["scores"][_obj(r.get("team")).get("teamId")]
        if total != want:
            raise AssetFetchError(f"{where}: period points {total} != the fixture score {want}")
    return rows


def _game_route(name: str, fid: str, **kw: Any) -> Any:
    ctx = _header(fid, **kw)
    teams, scores = ctx["teams"], ctx["scores"]
    where = f"easl.joshricha.com/api/v1/matches/{fid}"
    if name == "match":
        return ctx["body"]
    if name == "boxscore":
        body = _obj(_fetch(f"/matches/{fid}/boxscore", None, **kw))
        rows, tms = _rows(body.get("boxScore")), _rows(body.get("teams"))
        if not rows or {t.get("teamId") for t in tms} != set(teams):
            raise AssetFetchError(f"{where}/boxscore: no boxScore, or teams that are not this fixture's")
        if any(not _same(r.get("fixtureId"), fid) for r in rows) or any(r.get("entityId") not in scores for r in rows):
            raise AssetFetchError(f"{where}/boxscore: a row belongs to another fixture or to a team outside it")
        for t in teams:
            pts = sum(
                _int(_obj(r.get("statistics")).get("points")) or 0
                for r in rows
                if r["entityId"] == t and r.get("isPlayer")
            )
            if pts != scores[t]:
                raise AssetFetchError(
                    f"{where}/boxscore: player points {pts} != the fixture score {scores[t]} (an incomplete box?)"
                )
        return body
    if name == "playbyplay":
        body = _obj(_fetch(f"/matches/{fid}/playbyplay", None, **kw))
        ev, tms = _rows(body.get("playbyplay")), _rows(body.get("teams"))
        if not ev or {t.get("teamId") for t in tms} != set(teams):
            raise AssetFetchError(f"{where}/playbyplay: no events, or teams that are not this fixture's")
        if any(set(_obj(e.get("scores"))) != set(teams) for e in ev):
            raise AssetFetchError(
                f"{where}/playbyplay: an event's running score is not keyed by the fixture's two teams"
            )
        last = ev[-1]
        if {t: last["scores"][t] for t in teams} != scores:
            raise AssetFetchError(
                f"{where}/playbyplay: the last event does not carry the final score: cut short or live"
            )
        pids = [_int(e.get("periodId")) if isinstance(e.get("periodId"), int) else None for e in ev]
        known = [p for p in pids if p is not None]
        if len(known) != len(pids) or pids[-1] != max(known) or max(known) < 4:
            raise AssetFetchError(
                f"{where}/playbyplay: the last event is in period {pids[-1]!r}, not the final period (4 or more)"
            )
        table = _period_table(fid, ctx, **kw)
        if max(_periods(table)) != max(known):
            raise AssetFetchError(
                f"{where}/playbyplay: ends in period {max(known)}, the period table in {max(_periods(table))}"
            )
        return body
    if name == "teamstats":
        rows = _rows(_list(where, _fetch(f"/matches/{fid}/teamstats", None, **kw)))
        if (
            len(rows) != 2
            or {r.get("entityId") for r in rows} != set(teams)
            or any(not _same(r.get("fixtureId"), fid) for r in rows)
        ):
            raise AssetFetchError(f"{where}/teamstats: the rows are not this fixture's two teams")
        if any(_int(_obj(r.get("statistics")).get("points")) != scores[r["entityId"]] for r in rows):
            raise AssetFetchError(f"{where}/teamstats: team points differ from the fixture score")
        return rows
    return _period_table(fid, ctx, **kw)


# --- seasons, teams, players ----------------------------------------------------------------------------------------


def _season_route(name: str, ids: Mapping[str, str], wire: Mapping[str, Any], **kw: Any) -> Tuple[Any, Dict[str, Any]]:
    row, end = _season(wire.get("season"), **kw)
    sid = row["seasonId"]
    stamp: Dict[str, Any] = {"season": end, "season_id": sid}
    where = f"easl.joshricha.com/api/v1/{name}"
    active = row.get("status") == "ACTIVE"
    tid = ids.get("team_id")
    if tid:
        stamp["team_id"] = tid
    if name == "matches":
        rows = _rows(_list(where, _fetch("/matches", {"seasonId": sid}, **kw)))
        if any(not _same(r.get("seasonId"), sid) for r in rows):
            raise AssetFetchError(f"{where}: a fixture belongs to another season")
        if rows and active:
            _no_stale(rows, **kw)
        return rows, stamp
    if name == "team_matches":
        rows = _rows(_list(where, _fetch(f"/teams/{tid}/matches", {"seasonId": sid}, **kw)))
        for r in rows:
            own = _obj(r.get("stage")).get("id")
            ok = _same(r["seasonId"], sid) if "seasonId" in r else (isinstance(own, str) and own.startswith(sid + ":"))
            if not ok:
                raise AssetFetchError(f"{where}: a fixture carries neither this season's seasonId nor a stage id of it")
            if tid not in {c.get("teamId") for c in _rows(r.get("competitors"))}:
                raise AssetFetchError(f"{where}: a fixture does not involve team {tid}")
        if rows and active:
            _no_stale(rows, **kw)
        return rows, stamp
    if name == "standings":
        rows = _rows(_list(where, _fetch("/standings", {"seasonId": sid}, **kw)))
        lo, hi = str(row.get("startDate")), str(row.get("endDate"))
        for r in rows:
            for m in _rows(r.get("lastFiveMatches")):
                if not lo <= str(m.get("startTimeUTC"))[:10] <= hi:
                    raise AssetFetchError(f"{where}: a last-five match is outside the season ({lo}..{hi})")
        return rows, stamp
    if name == "team_stats":
        body = _obj(_fetch(f"/teams/{tid}/stats", {"seasonId": sid}, **kw))
        games = body.get("games")
        if not body or not isinstance(games, int) or isinstance(games, bool):
            raise AssetFetchError(f"{where}: no integer games count to tie the body to team {tid}")
        if games == 0:
            raise NoDataError(f"{where}: team {tid} played no game in season {end}")
        return body, stamp
    # leaders and team_leaders
    body = _obj(_fetch(f"/teams/{tid}/leaders" if tid else "/leaders", {"seasonId": sid}, **kw))
    cats = {k: _rows(v) for k, v in body.items() if isinstance(v, list)}
    if not cats or any(not v for v in cats.values()):
        raise NoDataError(f"{where}: empty leader categories for season {end}")
    for r in (r for v in cats.values() for r in v):
        if not _same(r.get("seasonId"), sid) or (tid and _obj(r.get("team")).get("teamId") != tid):
            raise AssetFetchError(f"{where}: a leader row belongs to another season or team")
    return body, stamp


def _simple_route(name: str, ids: Mapping[str, str], wire: Mapping[str, Any], **kw: Any) -> Tuple[Any, Dict[str, Any]]:
    where = f"easl.joshricha.com/api/v1/{name}"
    if name == "health":
        body = _obj(_fetch("/health", None, **kw))
        if body.get("status") != "OK":
            raise AssetFetchError(f"{where}: health is not OK")
        return body, {}
    if name == "seasons":
        return _seasons(**kw), {}
    if name == "teams":
        rows = _rows(_list(where, _fetch("/teams", None, **kw)))
        if any(r.get("organizationId") != ORG for r in rows):
            raise AssetFetchError(f"{where}: a team row is not organisation {ORG}")
        return rows, {}
    if name == "team":
        body = _obj(_fetch(f"/teams/{ids['team_id']}", None, **kw))
        if not _same(body.get("teamId"), ids["team_id"]):
            raise AssetFetchError(f"{where}: asked for {ids['team_id']}, the body has teamId={body.get('teamId')!r}")
        return body, {"team_id": ids["team_id"]}
    if name == "player":
        body = _obj(_fetch(f"/players/{ids['player_id']}", None, **kw))
        if not _same(body.get("playerId"), ids["player_id"]):
            raise AssetFetchError(
                f"{where}: asked for {ids['player_id']}, the body has playerId={body.get('playerId')!r}"
            )
        return body, {"player_id": ids["player_id"]}
    if name == "player_stats":
        body = _obj(_fetch(f"/players/{ids['player_id']}/stats", None, **kw))
        games = body.get("games")
        if not body or not isinstance(games, int) or isinstance(games, bool):
            raise AssetFetchError(f"{where}: no integer games count to tie the body to player {ids['player_id']}")
        if games < 1:
            raise NoDataError(f"{where}: player {ids['player_id']} has no games (an unknown id answers the same way)")
        return body, {"player_id": ids["player_id"]}
    # players
    params: Dict[str, Any] = {}
    stamp: Dict[str, Any] = {}
    if wire.get("season") is not None:
        row, end = _season(wire["season"], **kw)
        params["seasonId"] = stamp["season_id"] = row["seasonId"]
        stamp["season"] = end
    if wire.get("teamId") is not None:
        if not _UUID_RE.fullmatch(str(wire["teamId"])):
            raise ValueError(f"team_id must be a DataCore team UUID; got {wire['teamId']!r}")
        params["teamId"] = stamp["team_id"] = str(wire["teamId"])
    rows = _rows(_list(where, _fetch("/players", params, **kw)))
    if any(not isinstance(r.get("playerId"), str) or not r["playerId"] for r in rows):
        raise AssetFetchError(f"{where}: a player row lacks playerId")
    return rows, stamp


# --- the getter -----------------------------------------------------------------------------------------------------


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET an EASL route, refuse the partial or out-of-scope answers and stamp the request.

    Args:
        url: the route URL (``https://easl.joshricha.com/api/v1/...``); ids are DataCore UUIDs.
        params: ``season`` (END year; ``None`` = the ACTIVE season) on the season routes (``/matches``,
            ``/standings``, ``/leaders``, ``/teams/{id}/matches|stats|leaders``) and, with ``teamId``, on
            ``/players``; ``None`` values are dropped. The runtime sends the resolved ``seasonId``.
        **kwargs: forwarded to the HTTP getter (``session`` defaults to the shared polite session).

    Returns:
        The body: a dict with the request stamped under :data:`REQUEST_KEY` (``fixture_id``, ``season``, ``season_id``,
        ``team_id``, ``player_id`` as they apply); a bare list comes back as ``{"items": [...], "_request": {...}}``.

    Raises:
        NoDataError: the server has nothing for the request (an empty list or leaders object, a team without games,
            a 404).
        ValueError: a malformed id, a season that is not an EASL END year, or a malformed ``teamId`` (raised before
            the request that would use it).
        AssetFetchError: any other failure: a non-2xx (the unknown-fixture 500 included), an error envelope, a
            game that is not finished, a stalled sync on the active season, or a body for another game, season,
            team or player.
    """
    wire = {k: v for k, v in (params or {}).items() if v is not None}
    name, ids = _route(url)
    if name in _GAME_ROUTES:
        fid = ids["fixture_id"]
        return _wrap(_game_route(name, fid, **kwargs), {"fixture_id": fid})
    if name in ("matches", "standings", "leaders", "team_matches", "team_stats", "team_leaders"):
        data, stamp = _season_route(name, ids, wire, **kwargs)
    else:
        data, stamp = _simple_route(name, ids, wire, **kwargs)
    if data is None or data == [] or data == {}:
        raise NoDataError(f"easl.joshricha.com/api/v1/{name}: the server answered no rows")
    return _wrap(data, stamp)
