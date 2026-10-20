"""Runtime for the generated TBF wrappers (Turkish Basketball Federation app API: BSL, TBL, KBSL).

One host, ``miniappapi.tbf.org.tr/webapi-service/api``: the official TBF app backend, plain nginx, no key, no cookie,
no ``Referer``. The vendor origin hosts (``*.nikayazilim.com``) serve the same API without Cloudflare but are never
used, so every request is sent with redirects disabled and a 3xx is a failed fetch. ``www.tbf.org.tr`` sits behind an
interactive Cloudflare Turnstile that this API does not touch and that is not automated. Measured against the live
API (sdv-internal-refs ``tbf/``, 2026-10-12):

* **Season and competition.** The public ``season`` is the END year (``"2026-2027"`` -> 2027) and ``competition`` is
  ``bsl`` (Basketbol Super Ligi), ``tbl`` (second tier; the top flight was named TBL until 2007-08, under ``bsl``) or
  ``kbsl`` (women). The API keys a league-season by an internal ``activityId`` and ``seasonId``; both are read from
  ``League/get-leagues-and-seasons-by-prefix`` (one request per competition per process) by the label's END year. A
  year the list lacks is ``NoDataError``.
* **Finished games only.** Every game wrapper first reads ``Match/mac-header`` (cached per game): its
  ``matchStatusId`` must be 1 (played), else ``AssetFetchError``. The header gives the final score, the team-season
  ids and the roster every other game route is checked against.
* **Silent wrong answers, all refused** (type-strict compares, fail closed): ``matchId`` echoed by play-by-play, shots,
  summary and leaders must be the asked game (the API echoes it as a string); the box score echoes nothing, so the
  team totals must equal the final score and every player must be on the header roster; play-by-play must end on
  ``Maç Sonu`` at the final score; period scores must sum to the final score; league, standings, schedule, player,
  team and daily routes are compared with the ids asked for; capped lists (20 seasons, 20 leaders) that are not
  exactly their cap, paged lists that are partial, and rows for another team are refused. Routes that echo nothing
  (weeks, rosters, staff, team statistics, the profile) can only be refused when empty.
* **Error vocabulary.** ``NoDataError``: HTTP 404 (unknown game or player), a season the list lacks, an empty list or
  zeroed answer for an unknown id, and a finished game without play-by-play or shots (TBF has them from 2025-26 only).
  ``ValueError``: an argument that is not an integer / known competition, HTTP 400 / 422. ``AssetFetchError``: any
  other non-200 (429 / 502 / 503 / 504 retried, ``Retry-After`` honoured; a 500 is not retried), a dead connection,
  a non-JSON or error-envelope body, a 3xx, an unfinished game, and every wrong-scope body above.

Every attempt, retries included, goes through a ``basketball_runtime.PoliteSession`` and so waits its turn (at
least 1 s per host). Personal fields (birth dates, ages, contact details; none are used) are replaced by
``<redacted>`` by key name in every body.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, FrozenSet, List, Optional, Tuple
from urllib.parse import urlsplit

from requests.adapters import HTTPAdapter

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import PoliteSession
from sportsdataverse.basketball.tbf.tbf_parsers import LEADER_CATEGORIES, _end_year, _i
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["REQUEST_KEY", "SESSION"]

HOST = "miniappapi.tbf.org.tr"
BASE = "/webapi-service/api"
API = f"https://{HOST}{BASE}"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 502, 503, 504})
COMPETITIONS = ("bsl", "tbl", "kbsl")
#: The server's caps: the 20 most recent seasons, the top 20 of every leaders list.
CAP_ROWS = 20
FINAL = 1  # matchStatusId 1 = played (Oynandı)
GAME_END = "Maç Sonu"
#: route suffix -> LEADER_CATEGORIES entry (the nine /Player/get-*-leaders routes)
_LEADER_ROUTES = dict(
    zip(
        ("point", "asist", "blok", "ribound", "top-calma", "duration", "3Point", "top-kaybi", "mvp"),
        LEADER_CATEGORIES,
    )
)
_PERSONAL = re.compile(r"(birth|dogum|doğum|^age$|e_?mail|phone|telefon|adres|address|websitesi)", re.I)


class _TbfSession(PoliteSession):
    """The polite session with redirects off: a redirect (to a vendor origin or anywhere) is never followed."""

    def request(self, method: Any, url: Any, *args: Any, **kwargs: Any) -> Any:
        kwargs["allow_redirects"] = False
        return super().request(method, url, *args, **kwargs)


#: The pooled, throttled, no-redirect session every TBF request goes through.
SESSION = _TbfSession()
for _scheme in ("https://", "http://"):
    SESSION.mount(_scheme, HTTPAdapter(pool_connections=4, pool_maxsize=8))

#: Process caches: competition -> season rows of the prefix list; match id -> verified finished-game context.
_PREFIX: Dict[str, List[Dict[str, Any]]] = {}
_GAMES: Dict[int, Dict[str, Any]] = {}


def _redact(obj: Any) -> Any:
    """``obj`` with personal values replaced by ``<redacted>`` by key name, at any depth."""
    if isinstance(obj, dict):
        return {k: ("<redacted>" if _PERSONAL.search(str(k)) else _redact(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def _same(a: Any, b: Any) -> bool:
    """Type-strict equality after the API's integral-float ids (``22214.0`` -> 22214); ``1 != "1"``, ``True != 1``."""
    if isinstance(a, float) and a == a and a == int(a):
        a = int(a)
    return type(a) is type(b) and a == b


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be an integer; got {value!r}")
    return int(value)


def _competition(value: Any) -> str:
    comp = str(value).lower() if value is not None else ""
    if comp not in COMPETITIONS:
        raise ValueError(f"competition must be one of {COMPETITIONS}; got {value!r}")
    return comp


def _rows(obj: Any) -> List[Dict[str, Any]]:
    return [r for r in obj if isinstance(r, dict)] if isinstance(obj, list) else []


def _where(path: str) -> str:
    return f"{HOST}{BASE}{path}"


# ----------------------------------------------------------------------------- transport
def _fetch(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET ``BASE + path`` through the throttled no-redirect session; the checked, redacted envelope.

    Raises:
        NoDataError: HTTP 404, or a 200 whose ``problemDetails`` says not found.
        ValueError: HTTP 400 / 422.
        AssetFetchError: any other failed fetch, a body that is not the TBF envelope, ``isSuccess`` false.
    """
    url = f"{API}{path}"
    body = _rt._download_json(
        _rt._no_content_transport(download),  # a 204 / 205 raises _rt._NoContent: ``_get`` answers {} (EX-SYNC-a)
        url,
        params={k: v for k, v in (params or {}).items() if v is not None},
        session=SESSION,
        timeout=TIMEOUT,
        num_retries=ATTEMPTS - 1,
        retry_statuses=RETRY_STATUSES,
        cache_ttl=0,
    )
    if not isinstance(body, dict) or "data" not in body or "isSuccess" not in body:
        raise AssetFetchError(f"{_where(path)} answered a body that is not the TBF envelope")
    problem = body.get("problemDetails")
    if body.get("isSuccess") is not True or problem is not None:
        if isinstance(problem, dict) and problem.get("type") == "NotFoundException":
            raise NoDataError(f"{_where(path)}: not found")
        raise AssetFetchError(f"{_where(path)} answered an error envelope (isSuccess {body.get('isSuccess')!r})")
    return _redact(body)


def _paged_complete(path: str, body: Dict[str, Any], rows: List[Any]) -> None:
    """A paged list must be whole: ``count`` equals the rows and no next page."""
    if body.get("hasNextPage") is not False or not _same(body.get("count"), len(rows)):
        raise AssetFetchError(
            f"{_where(path)}: partial page ({len(rows)} rows, count {body.get('count')!r}, "
            f"next page {body.get('hasNextPage')!r})"
        )


def _stamp(body: Dict[str, Any], **request: Any) -> Dict[str, Any]:
    body[REQUEST_KEY] = request
    return body


def _envelope(data: Any) -> Dict[str, Any]:
    return {"data": data, "isSuccess": True, "problemDetails": None}


def _need_rows(path: str, rows: List[Any], what: str) -> None:
    if not rows:
        raise NoDataError(f"{_where(path)}: no {what}")


# ----------------------------------------------------------------------- season / competition
def _prefix_rows(competition: str) -> List[Dict[str, Any]]:
    """The season rows of one competition (cached); every url must belong to the competition."""
    if competition not in _PREFIX:
        path = "/League/get-leagues-and-seasons-by-prefix"
        rows = _rows(_fetch(path, {"prefix": competition})["data"])
        _need_rows(path, rows, f"seasons for {competition}")
        bad = [r.get("url") for r in rows if not str(r.get("url") or "").startswith(competition + "-")]
        if bad:
            raise AssetFetchError(f"{_where(path)}: out of scope: url {bad[0]!r} is not a {competition}- league")
        _PREFIX[competition] = rows
    return _PREFIX[competition]


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-8)
def _league(season: Any, competition: Any) -> Tuple[int, int, str, int]:
    """``(season_id, activity_id, competition, season)`` of an END year; ``NoDataError`` when the list lacks it."""
    year, comp = _int_arg(season, "season"), _competition("bsl" if competition is None else competition)
    hits = [r for r in _prefix_rows(comp) if _end_year(r.get("sezon")) == year]
    if not hits:
        raise NoDataError(f"{HOST}: no {comp} season ending {year} in TBF's league list")
    if len(hits) > 1:
        raise AssetFetchError(f"{HOST}: {len(hits)} {comp} seasons end {year}; the season key is ambiguous")
    sid, aid = _i(hits[0].get("sezon_ID")), _i(hits[0].get("faaliyet_ID"))
    if sid is None or aid is None:
        raise AssetFetchError(f"{HOST}: {comp} {year} row without season / activity id")
    return sid, aid, comp, year


# -------------------------------------------------------------------------------- route handlers
def _seasons(params: Dict[str, Any]) -> Dict[str, Any]:
    path = "/Season/get-seasons"
    body = _fetch(path)
    rows = _rows(body["data"])
    _paged_complete(path, body, rows)
    if len(rows) != CAP_ROWS:
        raise AssetFetchError(f"{_where(path)}: capped list drifted: {len(rows)} rows, expected exactly {CAP_ROWS}")
    return body


def _leagues(params: Dict[str, Any]) -> Dict[str, Any]:
    sid = _league(params.get("season"), "bsl")[0]  # the bsl list holds every TBF season since 1966-67
    path = "/League/get-leagues-for-filter"
    body = _fetch(path, {"seasonId": sid})
    rows = _rows(body["data"])
    _need_rows(path, rows, "leagues")
    for r in rows:
        if not _same(r.get("seasonId"), sid):
            raise AssetFetchError(f"{_where(path)}: out of scope: seasonId {r.get('seasonId')!r}, asked for {sid}")
    return body


def _league_seasons(params: Dict[str, Any]) -> Dict[str, Any]:
    comp = _competition(params.get("competition"))
    return _stamp(_envelope(list(_prefix_rows(comp))), competition=comp)


def _weeks_list(sid: int, aid: int) -> List[Dict[str, Any]]:
    path = "/League/get-league-weeks"
    rows = _rows(_fetch(path, {"seasonId": sid, "leagueId": aid})["data"])
    _need_rows(path, rows, "weeks")
    return rows


def _weeks(params: Dict[str, Any]) -> Dict[str, Any]:
    sid, aid, comp, year = _league(params.get("season"), params.get("competition"))
    return _stamp(_envelope(_weeks_list(sid, aid)), season=year, competition=comp, activity_id=aid)


def _standings(params: Dict[str, Any]) -> Dict[str, Any]:
    sid, aid, comp, year = _league(params.get("season"), params.get("competition"))
    week = None if params.get("week") is None else _int_arg(params["week"], "week")
    path = "/League/get-standings-table"
    body = _fetch(path, {"seasonId": sid, "leagueId": aid, "hafta": week})
    rows = [s for g in _rows(body["data"]) for s in _rows(g.get("standings"))]
    _need_rows(path, rows, "standings rows")
    for s in rows:
        if not _same(s.get("faaliyetAltAlanId"), aid):
            raise AssetFetchError(
                f"{_where(path)}: out of scope: faaliyetAltAlanId {s.get('faaliyetAltAlanId')!r}, asked for {aid}"
            )
        if week is not None and not _same(s.get("hafta"), week):
            raise AssetFetchError(f"{_where(path)}: out of scope: hafta {s.get('hafta')!r}, asked for {week}")
    return _stamp(body, season=year, competition=comp)


def _matches_page(sid: int, aid: int, week: str) -> List[Dict[str, Any]]:
    path = "/Match/get-all-matches-for-filter"
    body = _fetch(path, {"ActivityId": aid, "WeekFilter": week, "Page": 1, "PageSize": -1})
    rows = _rows(body["data"])
    _paged_complete(path, body, rows)
    for r in rows:
        if not _same(r.get("activityId"), aid) or not _same(r.get("seasonId"), sid):
            raise AssetFetchError(
                f"{_where(path)}: out of scope: activity/season {r.get('activityId')!r}/{r.get('seasonId')!r}, "
                f"asked for {aid}/{sid}"
            )
    if len({r.get("week") for r in rows}) > 1:
        raise AssetFetchError(f"{_where(path)}: week {week}: rows from several weeks")
    return rows


def _schedule(params: Dict[str, Any]) -> Dict[str, Any]:
    sid, aid, comp, year = _league(params.get("season"), params.get("competition"))
    rows: List[Dict[str, Any]] = []
    if params.get("week") is not None:  # one request: its 204 is the route's {}
        rows += _matches_page(sid, aid, str(_int_arg(params["week"], "week")))
    else:  # the week list and one page per week: a 204 anywhere leaves the season incomplete, an unknown answer
        with _rt._no_content_unknown(f"{_where('/Match/get-all-matches-for-filter')} (a season's weeks)"):
            for w in [str(w.get("sezon_Hafta")) for w in _weeks_list(sid, aid)]:
                rows += _matches_page(sid, aid, w)
    _need_rows("/Match/get-all-matches-for-filter", rows, "games")
    return _stamp(_envelope(rows), season=year, competition=comp)


def _daily(params: Dict[str, Any]) -> Dict[str, Any]:
    day = params.get("match_date")
    try:
        day = date.fromisoformat(str(day)).isoformat()
    except ValueError:
        raise ValueError(f"match_date must be 'YYYY-MM-DD'; got {day!r}") from None
    aid = None
    if params.get("season") is not None or params.get("competition") is not None:
        aid = _league(params.get("season"), params.get("competition"))[1]
    path = "/Match/get-daily-matches"
    body = _fetch(path, {"MatchDate": day, "GroupByActivity": "true", "ActivityId": aid})
    groups = _rows(body["data"])
    games = [m for g in groups for m in _rows(g.get("matches"))]
    _need_rows(path, games, "games that day")
    for g in groups:
        if not _same(g.get("totalCount"), len(_rows(g.get("matches")))):
            raise AssetFetchError(
                f"{_where(path)}: truncated group {g.get('groupName')!r}: totalCount {g.get('totalCount')!r}"
            )
    for m in games:
        if m.get("matchDateOnly") != day:
            raise AssetFetchError(
                f"{_where(path)}: out of scope: matchDateOnly {m.get('matchDateOnly')!r}, asked for {day!r}"
            )
        if aid is not None and not _same(m.get("activityId"), aid):
            raise AssetFetchError(f"{_where(path)}: out of scope: activityId {m.get('activityId')!r}, asked for {aid}")
    return body


def _players(params: Dict[str, Any]) -> Dict[str, Any]:
    sid, aid, comp, year = _league(params.get("season"), params.get("competition"))
    path = "/Player/get-player-list"
    body = _fetch(path, {"seasonId": sid, "leagueId": aid, "page": 1, "pageSize": -1, "sortPlayersEnum": "AZ"})
    rows = _rows(body["data"])
    _paged_complete(path, body, rows)
    _need_rows(path, rows, "players")
    for r in rows:
        if not _same(r.get("seasonId"), sid) or r.get("prefix") != comp:
            raise AssetFetchError(
                f"{_where(path)}: out of scope: seasonId/prefix {r.get('seasonId')!r}/{r.get('prefix')!r}, "
                f"asked for {sid}/{comp}"
            )
    return _stamp(body, season=year, competition=comp)


def _profile(player_id: int) -> Dict[str, Any]:
    path = f"/Player/get-player-detail-profile-and-career-info/{player_id}"
    body = _fetch(path)
    data = body["data"] if isinstance(body["data"], dict) else {}
    if not data.get("playerProfile") or not _rows(data.get("careerInfo")):
        raise NoDataError(f"{_where('/Player/get-player-detail-profile-and-career-info')}: no profile for {player_id}")
    return _stamp(body, player_id=player_id)


def _leaders(route: str, category: str) -> Any:
    def handler(params: Dict[str, Any]) -> Dict[str, Any]:
        sid, aid, comp, year = _league(params.get("season"), params.get("competition"))
        path = f"/Player/get-{route}-leaders"
        body = _fetch(path, {"seasonId": sid, "leagueId": aid})
        data = body["data"] if isinstance(body["data"], dict) else {}
        total, avg = _rows(data.get("sum")), _rows(data.get("avg"))
        if not total and not avg:
            raise NoDataError(f"{_where(path)}: no leaders")
        if len(total) != CAP_ROWS or len(avg) != CAP_ROWS:
            raise AssetFetchError(
                f"{_where(path)}: capped list drifted: {len(total)} sum / {len(avg)} avg rows, "
                f"expected exactly {CAP_ROWS} each"
            )
        return _stamp(body, season=year, competition=comp, category=category)

    return handler


def _teams(params: Dict[str, Any]) -> Dict[str, Any]:
    sid, aid, comp, year = _league(params.get("season"), params.get("competition"))
    path = "/Team/get-teams-by-leauge"
    body = _fetch(path, {"leaugeId": aid})
    rows = _rows(body["data"])
    _need_rows(path, rows, "teams")
    for r in rows:
        if not _same(r.get("seasonId"), sid):
            raise AssetFetchError(
                f"{_where(path)}: out of scope: seasonId {r.get('seasonId')!r}, the league's season is {sid}"
            )
    return _stamp(body, season=year, competition=comp)


def _team(route: str, key: str) -> Any:
    """A team-season route; ``key`` names the row list inside ``data`` (``""``: ``data`` is the list)."""
    path = f"/Team/{route}"

    def handler(params: Dict[str, Any]) -> Dict[str, Any]:
        sid, aid, comp, year = _league(params.get("season"), params.get("competition"))
        tp = _int_arg(params.get("team_process_id"), "team_process_id")
        body = _fetch(path, {"LeagueId": aid, "SeasonId": sid, "TeamProcessId": tp})
        data = body["data"]
        rows = _rows(data.get(key) if key and isinstance(data, dict) else data)
        _need_rows(path, rows, "rows for this team-season")
        if key == "maclar":  # the one team route that names the teams: the asked team must play in every game
            for m in rows:
                if not (_same(m.get("takimAId"), tp) or _same(m.get("takimBId"), tp)):
                    raise AssetFetchError(f"{_where(path)}: out of scope: game {m.get('matchId')!r} is not team {tp}'s")
        return _stamp(body, season=year, competition=comp, team_process_id=tp)

    return handler


# ------------------------------------------------------------------------------------------- games
Roster = FrozenSet[Tuple[Any, Any, Any]]


def _roster(header: Dict[str, Any]) -> Roster:
    return frozenset(
        (p.get("isHome"), p.get("jerseyNumber"), p.get("playerName")) for p in _rows(header.get("players"))
    )


def _game(match_id: int) -> Dict[str, Any]:
    """The header context of a FINISHED game (cached); anything else raises.

    Raises:
        NoDataError: HTTP 404 (an unknown game) or an empty header.
        AssetFetchError: the game is not played yet, or the header lacks a roster, score or team-season id.
    """
    if match_id in _GAMES:
        return _GAMES[match_id]
    path = "/Match/mac-header"
    body = _fetch(path, {"matchId": match_id})
    h = body["data"] if isinstance(body["data"], dict) else {}
    if not h:
        raise NoDataError(f"{_where(path)}: no game {match_id}")
    if not _same(h.get("matchStatusId"), FINAL):
        raise AssetFetchError(
            f"{_where(path)}: game {match_id} is not finished (matchStatusId {h.get('matchStatusId')!r}); "
            "TBF game wrappers serve finished games only"
        )
    ids = [_i(h.get(k)) for k in ("homeTeamProcessId", "awayTeamProcessId", "homeTeamScore", "awayTeamScore")]
    if None in ids or not _roster(h):
        raise AssetFetchError(f"{_where(path)}: game {match_id}: header without team ids, score or roster")
    _GAMES[match_id] = {
        "body": body,
        "roster": _roster(h),
        "score": (ids[2], ids[3]),
        "request": {
            "match_id": match_id,
            "home_team_process_id": ids[0],
            "away_team_process_id": ids[1],
            "activity_id": _i(h.get("faaliyetId")),
            "season_id": _i(h.get("seasonId")),
        },
    }
    return _GAMES[match_id]


def _off_roster(path: str, rows: List[Tuple[Any, Any, Any]], roster: Roster) -> None:
    for r in rows:
        if r not in roster:
            raise AssetFetchError(f"{_where(path)}: out of scope: player {r!r} is not on this game's header roster")


def _persons(rows: List[Dict[str, Any]], named_only: bool = False) -> List[Tuple[Any, Any, Any]]:
    return [
        (r.get("isHome"), r.get("jerseyNumber"), r.get("playerName"))
        for r in rows
        if r.get("playerName") or not named_only
    ]


def _check_summary(path: str, d: Dict[str, Any], g: Dict[str, Any]) -> None:
    periods = {s: _rows(d.get(f"{s}TeamScore")) for s in ("home", "away")}
    _need_rows(path, periods["home"], "period scores")
    sums = tuple(sum(_i(p.get("score")) or 0 for p in periods[s]) for s in ("home", "away"))
    if sums != g["score"]:
        raise AssetFetchError(
            f"{_where(path)}: out of scope: period scores sum to {sums}, the final score is {g['score']}"
        )
    names = {(h, n) for h, _j, n in g["roster"]}
    for r in _rows(d.get("matchHighlightLeaders")):
        for side, key in ((True, "homePlayer"), (False, "awayPlayer")):
            if r.get(key) and (side, r[key]) not in names:
                raise AssetFetchError(
                    f"{_where(path)}: out of scope: {key} {r[key]!r} is not on this game's header roster"
                )


def _check_flow(path: str, d: Dict[str, Any], g: Dict[str, Any]) -> None:
    ev = _rows(d.get("events"))
    _need_rows(path, ev, "play-by-play events (TBF has them from 2025-26)")
    last, final = ev[-1], "%d-%d" % g["score"]
    if last.get("action") != GAME_END:
        raise AssetFetchError(f"{_where(path)}: game not final: last event {last.get('action')!r}, not {GAME_END!r}")
    if last.get("score") != final:
        raise AssetFetchError(
            f"{_where(path)}: out of scope: final event score {last.get('score')!r}, the game's is {final}"
        )
    _off_roster(path, _persons(ev, named_only=True), g["roster"])


def _check_stats(path: str, mid: int, d: Dict[str, Any], g: Dict[str, Any]) -> None:
    if d.get("matchId") is not None and d.get("matchId") != str(mid):
        raise AssetFetchError(f"{_where(path)}: out of scope: matchId {d.get('matchId')!r}, asked for {mid}")
    blocks = ("homeTopFive", "homeBenchPlayers", "awayTopFive", "awayBenchPlayers")
    players = [(b.startswith("home"), p) for b in blocks for p in _rows(d.get(b))]
    _need_rows(path, players, "player rows")
    _off_roster(path, [(h, p.get("jerseyNumber"), p.get("playerName")) for h, p in players], g["roster"])
    totals = tuple(_dict_get(d, f"{s}TeamTotal", "points") for s in ("home", "away"))
    if not all(_same(t, s) for t, s in zip(totals, g["score"])):
        raise AssetFetchError(
            f"{_where(path)}: out of scope: team totals {totals}, the game's final score is {g['score']}"
        )


def _dict_get(d: Dict[str, Any], key: str, sub: str) -> Any:
    inner = d.get(key)
    return inner.get(sub) if isinstance(inner, dict) else None


def _game_route(route: str, kind: str) -> Any:
    path = f"/Match/{route}"

    def handler(params: Dict[str, Any]) -> Dict[str, Any]:
        mid = _int_arg(params.get("match_id"), "match_id")
        if kind == "header":  # the header IS the answer here; on the other match routes it is a lookup
            g = _game(mid)
            return _stamp(dict(g["body"]), **g["request"])
        with _rt._no_content_unknown(f"{_where('/Match/mac-header')} (the finished-game check)"):
            g = _game(mid)
        body = _fetch(path, {"matchId": mid})
        d = body["data"] if isinstance(body["data"], dict) else {}
        if kind == "stats":
            _check_stats(path, mid, d, g)
            return _stamp(body, **g["request"])
        if d.get("matchId") != str(mid):  # play-by-play, shots, summary and leaders echo the id as a string
            raise AssetFetchError(f"{_where(path)}: out of scope: matchId {d.get('matchId')!r}, asked for {str(mid)!r}")
        if kind == "flow":
            _check_flow(path, d, g)
        elif kind == "shots":
            shots = _rows(d.get("shotInfos"))
            _need_rows(path, shots, "shots (TBF has them from 2025-26)")
            _off_roster(path, _persons(shots), g["roster"])
        elif kind == "summary":
            _check_summary(path, d, g)
        else:  # leaders
            cats = _rows(d.get("categories"))
            _need_rows(path, cats, "leader categories")
            _off_roster(path, _persons([p for c in cats for p in _rows(c.get("leaders"))]), g["roster"])
        return _stamp(body, **g["request"])

    return handler


_ROUTES: Dict[str, Any] = {
    "/Season/get-seasons": _seasons,
    "/League/get-leagues-for-filter": _leagues,
    "/League/get-leagues-and-seasons-by-prefix": _league_seasons,
    "/League/get-league-weeks": _weeks,
    "/League/get-standings-table": _standings,
    "/Match/get-all-matches-for-filter": _schedule,
    "/Match/get-daily-matches": _daily,
    "/Match/mac-header": _game_route("mac-header", "header"),
    "/Match/mac-istatistik": _game_route("mac-istatistik", "stats"),
    "/Match/game-flow": _game_route("game-flow", "flow"),
    "/Match/shot-chart": _game_route("shot-chart", "shots"),
    "/Match/mac-ozet": _game_route("mac-ozet", "summary"),
    "/Match/mac-leader": _game_route("mac-leader", "leaders"),
    "/Player/get-player-list": _players,
    "/Team/get-teams-by-leauge": _teams,
    "/Team/get-team-detail-statistic": _team("get-team-detail-statistic", "playersTotal"),
    "/Team/get-team-detail-matches-by-season-and-league": _team(
        "get-team-detail-matches-by-season-and-league", "maclar"
    ),
    "/Team/get-team-active-player-list-by-season-and-league": _team(
        "get-team-active-player-list-by-season-and-league", ""
    ),
    "/Team/get-team-detail-team-staff-by-season-and-league": _team(
        "get-team-detail-team-staff-by-season-and-league", ""
    ),
    **{f"/Player/get-{r}-leaders": _leaders(r, cat) for r, cat in _LEADER_ROUTES.items()},
}
_PROFILE = re.compile(r"^/Player/get-player-detail-profile-and-career-info/([^/]+)$")


@_rt._no_content_is_empty
def _get(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a TBF wrapper route: map season / competition, check the answer's scope, stamp the request.

    Args:
        url: the wrapper's fully-built URL on ``miniappapi.tbf.org.tr``.
        params: the wrapper's arguments by name (``season``, ``competition``, ``week``, ``match_id``,
            ``team_process_id``, ``match_date``); ``None`` values are dropped.
        **kwargs: accepted for wrapper compatibility; transport options are fixed.

    Returns:
        The redacted TBF envelope, with the request stamped under ``_request``.

    Raises:
        NoDataError: nothing there (see the module docstring).
        ValueError: an invalid argument, or HTTP 400 / 422.
        AssetFetchError: a failed fetch or a body for another game, season, league or team.
    """
    parts = urlsplit(url)
    if parts.netloc != HOST or not parts.path.startswith(BASE):
        raise ValueError(f"not a TBF app API URL: {_rt._where(url)}")
    route = parts.path[len(BASE) :]
    args = {k: v for k, v in (params or {}).items() if v is not None}
    m = _PROFILE.match(route)
    if m:
        return _profile(_int_arg(m.group(1), "player_id"))
    handler = _ROUTES.get(route)
    if handler is None:
        raise ValueError(f"not a wrapped TBF route: {route}")
    return handler(args)
