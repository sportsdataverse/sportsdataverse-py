"""Runtime for the generated KBL wrappers (Korean Basketball League, ``api.kbl.or.kr``).

Every request goes through the shared :data:`sportsdataverse.basketball.basketball_runtime.SESSION`, which waits at
least one second before **every** attempt, retries included, and ``dl_utils.download`` honours ``Retry-After`` on 429
and 503. ``www.kbl.or.kr/robots.txt`` answers the SPA's HTML and ``api.kbl.or.kr/robots.txt`` a 500 error envelope, so
no policy or ``Crawl-delay`` is published (checked 2026-10-12); one request a second is the pace. No key, cookie or
proxy is needed.

What the live API does (sdv-internal-refs ``kbl/``, probed 2026-10-12):

* **Two headers must be present**, with any value: ``Channel`` and ``TeamCode`` (the site sends ``WEB`` and ``XX``).
  Without them the API answers HTTP 400 ``{"resultCode": "Fail", ...}``; the runtime always sends them. A 400 is
  ``ValueError`` all the same, as everywhere in the package (the error vocabulary).
* **Errors** are an envelope ``{"resultCode": "Fail", "message": ..., "errorCode": N}`` with HTTP 400 or 500. An
  unknown or malformed id is a **500** ("a server error occurred"), so 500 is never retried (only 429, 502, 503, 504).
  An unknown ``gameNo`` on ``player-stat`` is a 200 ``[]`` and on ``match-chart`` a 200 with empty arrays: the game
  header is fetched first (below), so an unknown game fails there.
* **Seasons.** The public argument is the END year (2026 = 2025-26). KBL numbers seasons **and** D-League editions with
  one sequential ``seasonCode`` (:mod:`kbl_codes`, ``SEASONS``); ``division`` picks the KBL (``"kbl"``) or the
  D-League (``"d_league"``) and ``game_code`` the competition inside the season (``"01"`` regular season). The
  runtime rewrites the public path (``/league/rank/2026``) to the API's ``glkey`` (``/league/rank/S47G01``) or
  ``seasonCode`` and never sends ``division`` or ``game_code``. ``/league/playoff/{season}`` is the old route below
  ``seasonCode`` 45 and the new route from it, as the site does, and both take the REGULAR-SEASON key.
* **Games.** ``gmkey`` (``S47G01N49``) is a string; a malformed one is a ``ValueError`` before any request. The four
  box routes and the header are served **only for final games**: ``isEnded`` is unreliable (0 on every finished game
  through 2005-06 and on some later ones), so a game is final when ``isEnded`` is 1, or when it is dated before today
  (KST), both teams have points, **and** its text-cast reached the end of the last period (a game-end row, ``a`` =
  ``009``). Anything else is ``AssetFetchError``. The first call for a game fetches its header (and, for a game whose
  ``isEnded`` is not 1, its text-cast) once; the answer is cached for the process (:func:`clear_cache`).
* **Scope.** A body must be the one asked for, else ``AssetFetchError`` (never an empty frame): header ``gmkey``;
  text-cast teams, the periods the header played and a game-end row; box points that add up to the header's; team
  totals; shot teams and a score chart that ends on the final score; standings ``seasonCode`` / ``gameCode``;
  bracket and round-MVP ``seasonCode``; schedule dates inside the window. ``/leagues/{glkey}/stats/players`` and
  ``team-tracks`` echo no season (team-tracks rounds are checked against the season's dates), and the current-season
  routes (``/league/rank/team``, ``/league/season-leader``, ``/players``) have no parameter to check.
* **Nothing here** is ``NoDataError``: an empty list, a final game with no box score, no bracket, no rounds, no
  schedule rows in a window. A final game with no shot chart (none before 2006-07) is not: it parses to zero rows.

The text-cast always asks for ``quarterList=Q1,Q2,Q3,Q4,EQ1,EQ2,EQ3,EQ4``: without it only the last period comes back.
"""

from __future__ import annotations

import datetime as dt
import re
from contextlib import nullcontext
from typing import Any, Dict, List, Mapping, Optional, Tuple

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.basketball.kbl import kbl_codes as codes
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

BASE = "https://api.kbl.or.kr"

#: The two headers the API insists on (any value); ``Accept`` is courtesy.
HEADERS = {"Channel": "WEB", "TeamCode": "XX", "Accept": "application/json"}

#: 500 is the API's answer for an unknown id: deterministic, never retried.
RETRY_STATUSES = frozenset({429, 502, 503, 504})

#: Key the request is stamped under; the parsers read it.
REQUEST_KEY = "_request"

#: Without ``quarterList`` the text-cast returns only the last period.
QUARTERS = "Q1,Q2,Q3,Q4,EQ1,EQ2,EQ3,EQ4"

_KST = dt.timezone(dt.timedelta(hours=9))
_GMKEY = re.compile(r"S\d+G\d{2}N\d+")
_DATE = re.compile(r"\d{8}")
_NEW_PLAYOFF_FROM = 45  # /league/playoff/new serves seasonCode >= 45, /league/playoff the earlier ones

#: Dependency cache: only a successful, FINAL game is stored. ``clear_cache()`` empties it.
_CACHE: Dict[Tuple[Any, ...], Any] = {}


def clear_cache() -> None:
    """Forget the cached game contexts (header-derived team codes, totals and finality)."""
    _CACHE.clear()


def _today_kst() -> str:
    """Today's date in Korea, ``YYYYMMDD`` (a seam for the tests)."""
    return dt.datetime.now(_KST).strftime("%Y%m%d")


# --- routes ---------------------------------------------------------------------------------------------------------

#: (route name, compiled path, path-id names). The public season routes carry the END year where the API has a key.
_ROUTES: List[Tuple[str, "re.Pattern[str]", Tuple[str, ...]]] = [
    (name, re.compile("^" + pattern + "/?$"), ids)
    for name, pattern, ids in [
        ("season_list", r"/season/list", ()),
        ("team_list", r"/common/teamList", ()),
        ("match_list", r"/match/list", ()),
        ("header", r"/match/(S\d+G\d{2}N\d+)", ("gmkey",)),
        ("text_cast", r"/match/(S\d+G\d{2}N\d+)/text-cast", ("gmkey",)),
        ("player_stat", r"/match/(S\d+G\d{2}N\d+)/player-stat", ("gmkey",)),
        ("team_record", r"/match/(S\d+G\d{2}N\d+)/team-record", ("gmkey",)),
        ("match_chart", r"/match/(S\d+G\d{2}N\d+)/match-chart", ("gmkey",)),
        ("season_players", r"/leagues/(\d+)/stats/players", ("season",)),
        ("rank", r"/league/rank/(\d+)", ("season",)),
        ("team_tracks", r"/league/rank/(\d+)/team-tracks", ("season",)),
        ("playoff", r"/league/playoff/(\d+)", ("season",)),
        ("round_mvp", r"/league/round-mvp/(\d+)", ("season",)),
        ("rank_team", r"/league/rank/team", ()),
        ("season_leader", r"/league/season-leader", ()),
        ("players", r"/players", ()),
    ]
]
_SEASON_ROUTES = frozenset({"season_players", "rank", "team_tracks", "playoff", "round_mvp"})
_GAME_ROUTES = frozenset({"header", "text_cast", "player_stat", "team_record", "match_chart"})


def _route(url: str) -> Tuple[str, Dict[str, str]]:
    """The route name of ``url`` and its path ids; ``ValueError`` for a path the family does not wrap."""
    path = url.split("://", 1)[-1].split("?", 1)[0]
    path = path[path.index("/") :] if "/" in path else "/"
    for name, pattern, ids in _ROUTES:
        m = pattern.match(path)
        if m:
            return name, dict(zip(ids, m.groups()))
    if re.match(r"^/match/[^/]+(/|$)", path):
        raise ValueError(f"gmkey must look like S47G01N49 (S<seasonCode>G<gameCode>N<gameNo>); got {path!r}")
    raise ValueError(f"not a KBL route: {path!r}")


# --- helpers --------------------------------------------------------------------------------------------------------


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality (``True == 1`` in Python, so ``isEnded: true`` must not pass for 1)."""
    return type(got) is type(want) and got == want


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _rows(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _int_of(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _season_arg(value: Any) -> int:
    if isinstance(value, bool) or not str(value).strip().lstrip("-").isdigit():
        raise ValueError(f"season must be an END year such as 2026 (2025-26); got {value!r}")
    return int(value)


def _glkey(season: Any, division: Any, game_code: Any) -> Tuple[int, str, str]:
    """``(seasonCode, gameCode, glkey)`` of a public season; ``ValueError`` before any request."""
    code = codes.season_code(_season_arg(season), str(division or "kbl"))
    gc = "01" if game_code is None else str(game_code)
    if not re.fullmatch(r"\d{2}", gc):
        raise ValueError(f"game_code must be a two-digit code such as '01' (regular season); got {game_code!r}")
    return code, gc, f"S{code}G{gc}"


def _fetch(path: str, params: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
    """GET ``BASE + path`` through the polite session with the two required headers; refuse the error envelope."""
    kwargs.setdefault("session", _bb.SESSION)
    kwargs.setdefault("retry_statuses", RETRY_STATUSES)
    headers = dict(HEADERS)
    headers.update(kwargs.pop("headers", None) or {})
    url = BASE + path
    # a 400 / 422 stays ValueError: the request is wrong (EX-SYNC-b, the repo-wide vocabulary)
    body = _bb._fetch_json(url, dict(params or {}), headers=headers, **kwargs)
    if isinstance(body, dict) and (body.get("resultCode") == "Fail" or ("errorCode" in body and "resultCode" in body)):
        raise AssetFetchError(
            f"{_rt._where(url)} answered the KBL error envelope (errorCode={body.get('errorCode')!r})"
        )
    return body


def _list(where: str, body: Any) -> List[Any]:
    if not isinstance(body, list):
        raise AssetFetchError(f"{where}: expected a JSON list, got {type(body).__name__}")
    return body


# --- games ----------------------------------------------------------------------------------------------------------


def _context(gmkey: str, body: Any) -> Dict[str, Any]:
    """What the per-game checks need from a ``/match/{gmkey}`` body (ids, team codes, totals, overtimes)."""
    where = f"api.kbl.or.kr/match/{gmkey}"
    game, records = _obj(_obj(body).get("game")), _obj(_obj(body).get("teamrecords"))
    if not _same(game.get("gmkey"), gmkey):
        raise AssetFetchError(f"{where}: asked for {gmkey!r}, the header has game.gmkey={game.get('gmkey')!r}")
    if not game or not records:
        raise AssetFetchError(f"{where}: the header has no game or teamrecords")
    totals: Dict[str, int] = {}
    ots = 0
    for side in ("home", "away"):
        rec = _obj(records.get(side))
        eq: List[Any] = rec["scoreeq"] if isinstance(rec.get("scoreeq"), list) else []
        totals[side] = sum(_int_of(rec.get(f"scoreq{i}")) for i in range(1, 5)) + sum(_int_of(x) for x in eq)
        ots = max(ots, len(eq))
    return {
        "gmkey": gmkey,
        "home": game.get("tcodeH"),
        "away": game.get("tcodeA"),
        "date": game.get("gameDate"),
        "is_ended": game.get("isEnded"),
        "totals": totals,
        "ots": ots,
    }


def _check_text_cast(where: str, ctx: Mapping[str, Any], body: Any) -> List[Mapping[str, Any]]:
    """The text-cast must be this game's whole feed and end with a game-end row; returns its rows."""
    rows = _list(where, body)
    if not rows:
        raise AssetFetchError(f"{where}: the text-cast is empty (a truncated page or an unknown game)")
    teams = {ctx["home"], ctx["away"]}
    want = {"Q1", "Q2", "Q3", "Q4"} | {f"X{i}" for i in range(1, ctx["ots"] + 1)}
    last = f"X{ctx['ots']}" if ctx["ots"] else "Q4"
    seen, acted, ended = set(), set(), False
    for row in rows:
        if not isinstance(row, dict):
            raise AssetFetchError(f"{where}: a text-cast row is not an object")
        if row.get("t") not in teams | {"", " "}:  # team-less events: '' (later seasons) or ' ' (1997-98)
            raise AssetFetchError(f"{where}: text-cast team {row.get('t')!r} is not in {sorted(map(str, teams))}")
        seen.add(row.get("q"))
        acted.add(row.get("t"))
        ended = ended or (row.get("a") == "009" and row.get("q") == last)
    if seen != want:
        raise AssetFetchError(
            f"{where}: text-cast periods {sorted(map(str, seen))} are not the game's {sorted(want)} (truncated or another game)"
        )
    if not teams <= acted:
        raise AssetFetchError(f"{where}: the text-cast has no row for team(s) {sorted(map(str, teams - acted))}")
    if not ended:
        raise AssetFetchError(f"{where}: the text-cast has no game-end row (a=009) in {last}: the game may not be over")
    return rows


def _game(gmkey: str, **kw: Any) -> Tuple[Dict[str, Any], Any, Any]:
    """``(context, header body or None, text-cast body or None)`` of a FINAL game.

    The header is fetched once per process; a game whose ``isEnded`` is not 1 is proven final by its text-cast, which
    is returned so the text-cast route need not fetch it again. A cached game returns ``(context, None, None)``.
    """
    key = ("game", gmkey)
    if key in _CACHE:
        return _CACHE[key], None, None
    where = f"api.kbl.or.kr/match/{gmkey}"
    header = _fetch(f"/match/{gmkey}", None, **kw)
    ctx = _context(gmkey, header)
    text = None
    if not _same(ctx["is_ended"], 1):
        date = ctx["date"]
        if not (isinstance(date, str) and date < _today_kst() and min(ctx["totals"].values()) > 0):
            raise AssetFetchError(
                f"{where}: the game is not final (isEnded={ctx['is_ended']!r}, gameDate={date!r}, points={ctx['totals']})"
            )
        with _rt._no_content_unknown(f"{where}/text-cast (the final-game proof)"):  # a lookup, even for the header
            text = _fetch(f"/match/{gmkey}/text-cast", {"quarterList": QUARTERS}, **kw)
        _check_text_cast(where, ctx, text)  # raises unless it reached the end of the last period
    _CACHE[key] = ctx
    return ctx, header, text


def _check_player_stat(where: str, ctx: Mapping[str, Any], rows: List[Any]) -> None:
    pts = {ctx["home"]: 0, ctx["away"]: 0}
    for row in rows:
        tcode = _obj(_obj(row).get("player")).get("tcode")
        if tcode not in pts:
            raise AssetFetchError(f"{where}: player team {tcode!r} is not in {sorted(map(str, pts))}")
        pts[tcode] += _int_of(_obj(_obj(row).get("records")).get("score"))
    got = {"home": pts[ctx["home"]], "away": pts[ctx["away"]]}
    if got != ctx["totals"]:
        raise AssetFetchError(f"{where}: player points {got} are not the header's {ctx['totals']}")


def _check_team_record(where: str, ctx: Mapping[str, Any], rows: List[Any]) -> None:
    got: Dict[Any, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise AssetFetchError(f"{where}: a team-record row is not an object")
        got[row.get("tcode")] = _int_of(_obj(row.get("records")).get("score"))
    want = {ctx["home"]: ctx["totals"]["home"], ctx["away"]: ctx["totals"]["away"]}
    if got != want:
        raise AssetFetchError(f"{where}: team scores {got} are not the header's {want}")


def _check_match_chart(where: str, ctx: Mapping[str, Any], body: Any) -> None:
    if not isinstance(body, dict):
        raise AssetFetchError(f"{where}: the match chart is not an object")
    teams = {ctx["home"], ctx["away"]}
    for row in _rows(body.get("shootLog")):
        if row.get("tcode") not in teams:
            raise AssetFetchError(f"{where}: shot team {row.get('tcode')!r} is not in {sorted(map(str, teams))}")
    chart = _obj(body.get("scoreChart"))
    for side in ("home", "away"):
        periods = _obj(chart.get(side))
        last = next((periods[p] for p in reversed(list(periods)) if isinstance(periods[p], list) and periods[p]), None)
        if not last or not _same(last[-1], ctx["totals"][side]):
            raise AssetFetchError(
                f"{where}: scoreChart {side} ends {last[-1] if last else None!r}, the header total is {ctx['totals'][side]}"
            )


def _game_route(name: str, gmkey: str, **kw: Any) -> Any:
    where = (
        f"api.kbl.or.kr/match/{gmkey}/{name.replace('_', '-')}" if name != "header" else f"api.kbl.or.kr/match/{gmkey}"
    )
    if name == "header":  # the header IS the answer here; on the other match routes the final check is a lookup
        ctx, header, text = _game(gmkey, **kw)
    else:
        with _rt._no_content_unknown(f"{where} (the final-game check)"):
            ctx, header, text = _game(gmkey, **kw)
    if name == "header":
        if header is None:
            header = _fetch(f"/match/{gmkey}", None, **kw)
            _context(gmkey, header)
        return header
    if name == "text_cast":
        if text is None:
            text = _fetch(f"/match/{gmkey}/text-cast", {"quarterList": QUARTERS}, **kw)
        return _check_text_cast(where, ctx, text)
    path = f"/match/{gmkey}/{name.replace('_', '-')}"
    body = _fetch(path, None, **kw)
    if name in ("player_stat", "team_record"):
        rows = _list(where, body)
        if not rows:
            raise NoDataError(f"{where}: the final game has no box score")
        (_check_player_stat if name == "player_stat" else _check_team_record)(where, ctx, rows)
        return rows
    _check_match_chart(where, ctx, body)
    return body


# --- seasons, leagues -----------------------------------------------------------------------------------------------


def _same_window(where: str, rows: List[Mapping[str, Any]], lo: str, hi: str) -> None:
    for row in rows:
        date = row.get("gameDate")
        if not isinstance(date, str) or not lo <= date <= hi:
            raise AssetFetchError(f"{where}: gameDate {date!r} is outside {lo}..{hi}")
        parts = codes.game_key_parts(row.get("gmkey"))  # type: ignore[arg-type]
        if parts is None or parts != (row.get("seasonCode"), row.get("gameCode"), row.get("gameNo")):
            raise AssetFetchError(f"{where}: gmkey {row.get('gmkey')!r} does not match its seasonCode/gameCode/gameNo")


def _windows(season: Optional[int], lo: Optional[str], hi: Optional[str]) -> List[Tuple[str, str]]:
    """Request windows: the given dates, or a season's first..last game split at New Year (one calendar year each)."""
    if season is not None:
        if lo or hi:
            raise ValueError("pass season, or from_date and to_date, not both")
        lo, hi = codes.season_dates(_season_arg(season))
    if not (isinstance(lo, str) and isinstance(hi, str) and _DATE.fullmatch(lo) and _DATE.fullmatch(hi)):
        raise ValueError("pass season (an END year) or both from_date and to_date as YYYYMMDD")
    if lo > hi:
        raise ValueError(f"from_date {lo} is after to_date {hi}")
    out, start = [], lo
    while start[:4] < hi[:4]:
        out.append((start, start[:4] + "1231"))
        start = str(int(start[:4]) + 1) + "0101"
    out.append((start, hi))
    return out


def _match_list(params: Mapping[str, Any], **kw: Any) -> Tuple[List[Any], Dict[str, Any]]:
    season = params.get("season")
    wins = _windows(None if season is None else _season_arg(season), params.get("fromDate"), params.get("toDate"))
    rows: List[Any] = []
    # one window is one request (its 204 is the route's {}); several windows leave a season incomplete on a 204
    with _rt._no_content_unknown("api.kbl.or.kr/match/list (a window of several)") if len(wins) > 1 else nullcontext():
        for lo, hi in wins:
            part = _list("api.kbl.or.kr/match/list", _fetch("/match/list", {"fromDate": lo, "toDate": hi}, **kw))
            _same_window("api.kbl.or.kr/match/list", _rows(part), lo, hi)
            rows.extend(part)
    stamp: Dict[str, Any] = {"from_date": wins[0][0], "to_date": wins[-1][1]}
    if season is not None:
        stamp["season"] = _season_arg(season)
    return rows, stamp


def _season_route(name: str, season: Any, params: Mapping[str, Any], **kw: Any) -> Tuple[Any, Dict[str, Any]]:
    division = params.get("division") or "kbl"
    if name in ("playoff", "round_mvp"):
        if division != "kbl":
            raise ValueError(f"the {name} route exists for the KBL only (division='kbl'); got {division!r}")
        game_code: Any = "01"  # the playoff routes take the REGULAR-SEASON key
    else:
        game_code = params.get("gameCode")
    code, gc, glkey = _glkey(season, division, game_code)
    end = _season_arg(season)
    stamp = {"season": end, "season_code": code, "game_code": gc, "glkey": glkey, "division": division}
    where = f"api.kbl.or.kr/{name}"
    if name == "season_players":
        rows = _list(where, _fetch(f"/leagues/{glkey}/stats/players", None, **kw))
    elif name == "rank":
        rows = _list(where, _fetch(f"/league/rank/{glkey}", None, **kw))
        for r in _rows(rows):
            if not (_same(r.get("seasonCode"), code) and _same(r.get("gameCode"), gc)):
                raise AssetFetchError(
                    f"{where}: asked for {glkey}, got seasonCode={r.get('seasonCode')!r} gameCode={r.get('gameCode')!r}"
                )
    elif name == "team_tracks":
        body = _fetch(f"/league/rank/{glkey}/team-tracks", None, **kw)
        lo, hi = next((r[3], r[4]) for r in codes.SEASONS if r[0] == code)
        rounds = _rows(_obj(body).get("rounds"))
        if not rounds:
            raise NoDataError(f"{where}: no rounds for {glkey}")
        for rd in rounds:
            if not (isinstance(rd.get("fromDate"), str) and lo <= rd["fromDate"] <= hi):
                raise AssetFetchError(f"{where}: round {rd!r} is outside season {code} ({lo}..{hi})")
        return body, stamp
    elif name == "playoff":
        new = code >= _NEW_PLAYOFF_FROM
        body = _fetch(f"/league/playoff/{'new/' if new else ''}{glkey}", None, **kw)
        seeds = _rows(_obj(body).get("defaultRank"))
        if not seeds:
            raise NoDataError(f"{where}: no playoff bracket for {glkey}")
        bad = [s.get("seasonCode") for s in seeds if not _same(s.get("seasonCode"), code)]
        for key, games in _obj(_obj(body).get("result")).items():
            if str(key).startswith("game") and isinstance(games, list):
                bad += [
                    g["gmkey"].get("seasonCode") for g in _rows(games) if _obj(g.get("gmkey")).get("seasonCode") != code
                ]
        if bad:
            raise AssetFetchError(f"{where}: asked for seasonCode {code}, the bracket has {bad[:3]}")
        stamp["format"] = "new" if new else "old"
        return body, stamp
    else:  # round_mvp
        rows = _list(where, _fetch(f"/league/round-mvp/{code}", None, **kw))
        for r in _rows(rows):
            if not _same(r.get("seasonCode"), code):
                raise AssetFetchError(f"{where}: asked for seasonCode {code}, got {r.get('seasonCode')!r}")
    if not rows:
        raise NoDataError(f"{where}: the API answered no rows for {glkey}")
    return rows, stamp


# --- the getter -----------------------------------------------------------------------------------------------------


def _wrap(data: Any, stamp: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(data) if isinstance(data, dict) else {"items": data}
    out[REQUEST_KEY] = dict(stamp)
    return out


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a KBL route, refuse the partial or out-of-scope answers and stamp the request.

    Args:
        url: the public route URL (``https://api.kbl.or.kr/...``); a season route carries the END year where the API
            has a key (``/league/rank/2026``) and a game route the ``gmkey`` (``/match/S47G01N49``).
        params: query-string parameters; ``None`` values are dropped. ``division`` (``"kbl"`` / ``"d_league"``) and
            ``game_code`` (default ``"01"``) pick the competition of a season route and are never sent;
            ``/match/list`` takes ``season`` (END year) or ``fromDate`` and ``toDate`` (``YYYYMMDD``).
        **kwargs: forwarded to the HTTP getter (``session`` defaults to the shared polite session).

    Returns:
        The body: a dict with ``{"season": END, "season_code": ..., "glkey": ...}`` / ``{"gmkey": ...}`` stamped under
        :data:`REQUEST_KEY`; a bare list comes back as ``{"items": [...], "_request": {...}}``.

    Raises:
        NoDataError: the API has nothing for the request (an empty list, a final game with no box score, no bracket,
            no rounds). An empty shot chart (before 2006-07) is not an error: it parses to zero rows.
        ValueError: a malformed ``gmkey``, a ``season`` that is not an END year between 1997 and 2027, an unknown
            ``division`` or ``game_code``, or bad ``/match/list`` dates (raised before any request).
        AssetFetchError: any other failure: a non-2xx (the unknown-id 500 included), an empty or non-JSON body, the
            KBL error envelope, a game that is not final, or a body for another game, season or window.
    """
    wire = {k: v for k, v in (params or {}).items() if v is not None}
    name, ids = _route(url)
    kw = {k: v for k, v in kwargs.items() if k in ("session", "retry_statuses", "headers")}
    kw.update({k: v for k, v in kwargs.items() if k not in kw})
    if name in _GAME_ROUTES:
        gmkey = ids["gmkey"]
        return _wrap(_game_route(name, gmkey, **kw), {"gmkey": gmkey})
    if name in _SEASON_ROUTES:
        data, stamp = _season_route(name, ids["season"], wire, **kw)
        return _wrap(data, stamp)
    if name == "match_list":
        rows, stamp = _match_list(wire, **kw)
        if not rows:
            raise NoDataError("api.kbl.or.kr/match/list: no games in the window")
        return _wrap(rows, stamp)
    path = {
        "season_list": "/season/list",
        "team_list": "/common/teamList",
        "rank_team": "/league/rank/team",
        "season_leader": "/league/season-leader",
        "players": "/players",
    }[name]
    data = _fetch(path, None, **kw)
    if data is None or data == [] or data == {}:
        raise NoDataError(f"api.kbl.or.kr{path}: the API answered no rows")
    return _wrap(data, {})
