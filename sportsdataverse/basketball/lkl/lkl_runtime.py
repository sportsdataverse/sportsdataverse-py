"""Runtime for the generated LKL wrappers (``lkl.lt``: the livestream JSON API plus three HTML routes).

One host, keyless, no headers, and ``robots.txt`` is ``Disallow:`` (empty) with no crawl-delay. Measured against
the live site (sdv-internal-refs ``lkl/``, 2026-10-12):

* **Season keys.** The public ``season`` is the END year (2026 = 2025-26). The site keys a season by two opaque
  ids, a results id (``/loadResults?season=``) and a standings id (``/loadStandings/{id}``), neither a year. Both
  are read at run time from the site's own season selects (``/rezultatai`` ``#season-lkl`` and
  ``/turnyrine-lentele`` ``#season``), whose labels read ``YYYY-YYYY m. sezonas``: the id whose label ends in the
  requested year. No table to go stale; a year the selects lack is ``NoDataError``.
* **Finished games only.** Before any game route is fetched the game page is read and its ``game-status`` must be
  ``Pasibaigė`` (finished): a scheduled or live game is an ``AssetFetchError`` (its box score is null stats or
  partial). The page also gives the team ids, the roster (slug -> player id) and the date, which the other routes
  are checked against and which are stamped under ``_request``. A finished game's context is cached for the process.
* **Silent wrong answers, all refused** (fail closed, type-strict compares): a game page whose ``game-id`` is not
  the request; a box score whose ``gameStatistics`` teams, ``game_id`` / ``team_id`` or player slugs are not this
  game's; leaders, play-by-play and shot-chart rows by players of another game; a results list whose dates leave
  the season window (August 1 - July 31), whose pages are short or repeat, or whose paginator understates the list;
  a standings page whose tabs or headers drift. The standings fragment carries no echo of its season, so the select
  label is its only scope check.
* **Error vocabulary.** ``NoDataError``: HTTP 404 (an unknown game or standings season), a season the selects do
  not list, a season with no played games, and a finished game without play-by-play or shots (the site charts both
  from 2024-25 only). ``ValueError``: a game id or season that is not an integer, HTTP 400 / 422.
  ``AssetFetchError``: any other non-200 (429 / 5xx after three attempts, spaced by ``dl_utils.download`` and
  honouring ``Retry-After``), a dead connection, a non-JSON or non-HTML body, an empty or error body, an unfinished
  game, an unplayed game's page, and every wrong-scope body above.

Every attempt, retries included, goes through the shared ``basketball_runtime.SESSION`` and so waits its turn (at
least 1 s per host). Personal contact or birth-date fields (none are served today) are replaced by ``<redacted>`` by
key name in every raw body before it is returned or parsed.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.basketball.lkl.lkl_parsers import (
    FINISHED,
    PAGE_SIZE,
    _label_end_year,
    _NotParsed,
    _options,
    _page_record,
    _results_page,
    _same,
)
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["REQUEST_KEY"]

SITE = "https://lkl.lt"
API = f"{SITE}/api/livestream"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
#: Hard stop for a results walk (the longest season seen has 21 pages).
MAX_PAGES = 100

_PERSONAL = re.compile(r"(birth|^dob$|gimim|e_?mail|phone|^tel$|mobile|address|adresas)", re.I)
_ERROR_KEYS = frozenset({"message", "error", "errors", "status", "exception", "trace", "code"})
#: route -> kind
_API_ROUTES = {
    "boxscore": "boxscore",
    "summary": "summary",
    "game-score": "game_score",
    "game-stats": "game_stats",
    "leaders": "leaders",
    "play-by-play": "pbp",
    "shot-chart": "shots",
}
#: Finished games' page records (the context every other route is checked against), per process.
_GAMES: Dict[int, Dict[str, Any]] = {}


def _redact_personal(obj: Any) -> Any:
    """``obj`` with personal contact / birth-date values replaced by ``<redacted>`` by key name, any depth."""
    if isinstance(obj, dict):
        return {k: ("<redacted>" if _PERSONAL.search(str(k)) else _redact_personal(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact_personal(v) for v in obj]
    return obj


# ----------------------------------------------------------------------------- transport
def _fetch(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries 429 / 5xx up to :data:`ATTEMPTS` times with a ``Retry-After``-aware delay and
    raises ``NoDataError`` on a 404. A connection failure that outlives the retries becomes an ``AssetFetchError``.
    """
    try:
        return download(
            url,
            params=params,
            session=SESSION,
            timeout=TIMEOUT,
            num_retries=ATTEMPTS - 1,
            retry_statuses=RETRY_STATUSES,
            cache_ttl=0,
        )
    except OSError as exc:  # requests' RequestException subclasses OSError
        raise AssetFetchError(
            f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}"
        ) from None


def _check_status(url: str, resp: Any) -> None:
    status = getattr(resp, "status_code", None)
    _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
    if status == 200:
        return
    where = _rt._where(url)
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    raise AssetFetchError(f"{where} answered HTTP {status}, not 200")


def _html(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    """The text of an HTML route: HTTP 200, an HTML content type and a non-empty body."""
    resp = _fetch(url, params)
    _check_status(url, resp)
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    if "html" not in ctype:
        raise AssetFetchError(f"{_rt._where(url)} answered {ctype or 'no content type'}, not HTML")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise AssetFetchError(f"{_rt._where(url)} answered HTTP 200 with an empty body")
    return text


def _json(url: str) -> Any:
    resp = _fetch(url)
    _check_status(url, resp)
    where = _rt._where(url)
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    if "json" not in ctype:
        raise AssetFetchError(f"{where} answered {ctype or 'no content type'}, not JSON")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise AssetFetchError(f"{where} answered HTTP 200 with an empty body")
    try:
        return json.loads(text)
    except ValueError:
        raise AssetFetchError(f"{where} answered a body that is not JSON") from None


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be an integer; got {value!r}")
    return int(value)


# ---------------------------------------------------------------------------- game page
def _game(game_id: int) -> Dict[str, Any]:
    """The game page record of a FINISHED game (cached); anything else raises.

    Raises:
        NoDataError: HTTP 404 (an unknown game id).
        AssetFetchError: the page is not the game asked for, has no game component (an unplayed game), or the game
            is not finished.
    """
    if game_id in _GAMES:
        return _GAMES[game_id]
    url = f"{SITE}/rungtynes/{game_id}"
    text = _html(url)
    try:
        rec = _page_record("game", text, game_id)
    except _NotParsed as exc:
        raise AssetFetchError(f"{_rt._where(url)}: no finished game to read: {exc}") from None
    if not _same(rec.get("game_status"), FINISHED):
        raise AssetFetchError(
            f"{_rt._where(url)}: game {game_id} is not finished (game-status {rec.get('game_status')!r}); "
            "LKL game wrappers serve finished games only"
        )
    for side in ("home", "away"):
        players = (rec.get("players") or {}).get(side) if isinstance(rec.get("players"), dict) else None
        if not isinstance(_team(rec, side).get("id"), int) or not isinstance(players, list) or not players:
            raise AssetFetchError(f"{_rt._where(url)}: game page without a {side} team id or players")
    rec = _redact_personal(rec)
    _GAMES[game_id] = rec
    return rec


def _team(rec: Dict[str, Any], side: str) -> Dict[str, Any]:
    info = rec.get("teams_info")
    t = info.get(side) if isinstance(info, dict) else None
    return t if isinstance(t, dict) else {}


def _roster(rec: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    return [p for p in (rec.get("players") or {}).get(side, []) if isinstance(p, dict)]


def _request(rec: Dict[str, Any]) -> Dict[str, Any]:
    """What the parsers need from the game page: team ids and the slug -> player id map."""
    players = {p["slug"]: p["id"] for side in ("home", "away") for p in _roster(rec, side) if p.get("slug")}
    return {
        "game_id": rec["game_id"],
        "home_team_id": _team(rec, "home")["id"],
        "away_team_id": _team(rec, "away")["id"],
        "players": players,
    }


# ----------------------------------------------------------------------------- JSON routes
def _quarters(body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    q = body.get("quarters") if "quarters" in body else body
    return q if isinstance(q, dict) and set(q) == {"regular", "overtime"} else None


def _lines(q: Dict[str, Any]) -> List[Dict[str, Any]]:
    reg, ot = q["regular"], q["overtime"]
    rows = [
        r for v in (reg.values() if isinstance(reg, dict) else reg or []) for r in (v if isinstance(v, list) else [v])
    ]
    return [r for r in rows + list(ot if isinstance(ot, list) else []) if isinstance(r, dict)]


def _scope_error(kind: str, body: Dict[str, Any], rec: Dict[str, Any]) -> Optional[str]:
    """Why ``body`` is not this finished game's, or ``None``. Fails closed."""
    gid = rec["game_id"]
    slugs = {s: {p.get("slug") for p in _roster(rec, s)} for s in ("home", "away")}
    pids = {s: {p.get("id") for p in _roster(rec, s)} for s in ("home", "away")}
    if kind in ("boxscore", "summary"):
        gs = body.get("gameStatistics")
        want = {str(_team(rec, "home")["id"]), str(_team(rec, "away")["id"])}
        if not isinstance(gs, dict) or set(gs) != want:
            return f"out of scope: gameStatistics teams {sorted(gs) if isinstance(gs, dict) else gs!r}, game has {sorted(want)}"
        for tid, row in gs.items():
            if (
                not isinstance(row, dict)
                or not _same(row.get("game_id"), gid)
                or not _same(row.get("team_id"), int(tid))
            ):
                return f"out of scope: gameStatistics[{tid}] is not game {gid}'s row"
            if not isinstance(row.get("points"), int):
                return f"game not final: team {tid} has no points"
        if kind == "boxscore":
            box = body.get("boxscore")
            for side in ("home", "away"):
                players = (box.get(side) or {}).get("players") if isinstance(box, dict) else None
                pts = ((((box or {}).get(side) or {}).get("team") or {}).get("points") or {}).get("value")
                if not isinstance(players, list) or not players:
                    return f"boxscore without {side} players"
                if not _same(pts, body["gameStatistics"][str(_team(rec, side)["id"])]["points"]):
                    return f"out of scope: boxscore {side} points {pts!r} != gameStatistics"
                if any(not isinstance(p, dict) or p.get("slug") not in slugs[side] for p in players):
                    return f"out of scope: a {side} box row is not a {side} player of game {gid}"
        return None
    if kind == "game_score":
        if not (isinstance(body.get("home"), int) and isinstance(body.get("away"), int)):
            return "game not final: score line without two integer scores"
        return None
    if kind == "game_stats":
        comp = body.get("comparison")
        if (
            not isinstance(comp, dict)
            or not isinstance(comp.get("all"), list)
            or not isinstance(body.get("leads"), dict)
        ):
            return "game-stats without comparison.all / leads (layout drift)"
        return None
    if kind == "leaders":
        cats = body.get("leaders")
        if not isinstance(cats, dict) or not cats:
            return "leaders without categories"
        for cat in cats.values():
            for side in ("home", "away"):
                lead = cat.get(side) if isinstance(cat, dict) else None
                rows = [lead] + list(lead.get("others") or []) if isinstance(lead, dict) else [None]
                for r in rows:
                    if not isinstance(r, dict) or r.get("slug") not in slugs[side]:
                        return f"out of scope: a {side} leader is not a {side} player of game {gid}"
        return None
    q = _quarters(body)
    if q is None:
        return "no quarters.regular / quarters.overtime (layout drift)"
    rows = _lines(q)
    for r in rows:
        row_side = "home" if r.get("is_home") is True else "away" if r.get("is_home") is False else None
        if row_side is None:
            return "row without a boolean is_home"
        if kind == "shots":
            if r.get("player_id") not in pids[row_side]:
                return f"out of scope: a shot by player {r.get('player_id')!r}, not a {row_side} player of game {gid}"
        else:
            slug = (r.get("player") or {}).get("slug") if isinstance(r.get("player"), dict) else None
            if slug is not None and slug not in slugs[row_side]:
                return f"out of scope: a play by {slug!r}, not a {row_side} player of game {gid}"
    if kind == "pbp" and rows:
        end = [r for r in rows if r.get("type") == "game" and r.get("subtype") == "end"]
        if not end:
            return "play-by-play without a game-end line (unfinished?)"
    return None


def _api(route: str, game_id: int) -> Dict[str, Any]:
    kind = _API_ROUTES[route]
    rec = _game(game_id)  # NoDataError / AssetFetchError before the route is asked for
    url = f"{API}/{route}/{game_id}"
    body = _json(url)
    where = _rt._where(url)
    if not isinstance(body, dict) or not body:
        raise AssetFetchError(f"{where}: the body is empty or not an object")
    if set(body) <= _ERROR_KEYS:
        raise AssetFetchError(f"{where}: the body is an error envelope")
    if kind in ("pbp", "shots"):
        q = _quarters(body)
        if q is not None and not _lines(q):
            raise NoDataError(
                f"{where}: no {'play-by-play' if kind == 'pbp' else 'shots'} for game {game_id} (the site charts both from 2024-25)"
            )
    why = _scope_error(kind, body, rec)
    if why:
        raise AssetFetchError(f"{where}: {why}")
    body = _redact_personal(body)
    body[REQUEST_KEY] = _request(rec)
    return body


def _game_page(game_id: int) -> Dict[str, Any]:
    rec = dict(_game(game_id))
    rec[REQUEST_KEY] = {"game_id": game_id}
    return rec


# ------------------------------------------------------------------------ season pages
def _season_id(index_html: str, select_id: str, season: int, what: str) -> int:
    """The id whose label ends in ``season`` in ``select#select_id`` of the index page."""
    try:
        options = _options(index_html, select_id)
    except _NotParsed as exc:
        raise AssetFetchError(f"{what}: {exc}") from None
    ids = [o["season_id"] for o in options if _label_end_year(o["label"]) == season]
    if not ids:
        raise NoDataError(f"{what}: the season select lists no season ending in {season}")
    if len(ids) > 1:
        raise AssetFetchError(f"{what}: {len(ids)} seasons end in {season} (ambiguous)")
    return ids[0]


def _results(season: int) -> Dict[str, Any]:
    index_url = f"{SITE}/rezultatai"
    index = _html(index_url)
    sid = _season_id(index, "season-lkl", season, _rt._where(index_url))
    url = f"{SITE}/loadResults"
    q = {"team": "-", "opponent": "-", "month": "-", "season": sid}
    pages: List[str] = []
    last = 1
    while len(pages) < min(last, MAX_PAGES):
        pages.append(_html(url, {**q, "page": len(pages) + 1}))
        if len(pages) == 1:
            try:
                last = _results_page(pages[0])[1] or 1
            except _NotParsed as exc:
                raise AssetFetchError(f"{_rt._where(url)}: not the page asked for: {exc}") from None
    after = None
    try:
        if pages and len(_results_page(pages[-1])[0]) == PAGE_SIZE:  # a full last page: confirm nothing follows
            after = _html(url, {**q, "page": len(pages) + 1})
        rec = _page_record("results", sid, index, pages, after)
    except _NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{_rt._where(url)}: {exc}") from None
        raise AssetFetchError(f"{_rt._where(url)}: not the list asked for: {exc}") from None
    rec[REQUEST_KEY] = {"season": season, "season_id": sid}
    return _redact_personal(rec)


def _standings(season: int) -> Dict[str, Any]:
    index_url = f"{SITE}/turnyrine-lentele"
    index = _html(index_url)
    sid = _season_id(index, "season", season, _rt._where(index_url))
    url = f"{SITE}/loadStandings/{sid}"
    text = _html(url)
    try:
        rec = _page_record("standings", text, sid, index)
    except _NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{_rt._where(url)}: {exc}") from None
        raise AssetFetchError(f"{_rt._where(url)}: not the page asked for: {exc}") from None
    rec[REQUEST_KEY] = {"season": season, "season_id": sid}
    return _redact_personal(rec)


# ------------------------------------------------------------------------------ the getter
@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET an LKL route, refuse every wrong answer, return the body or page record with the request stamped.

    Args:
        url: the URL a generated wrapper built: ``lkl.lt/api/livestream/{route}/{gameId}``, ``lkl.lt/rungtynes/{gameId}``,
            ``lkl.lt/loadResults`` (with ``params["season"]``, the END year) or ``lkl.lt/loadStandings/{season}``
            (the END year in the path; the runtime sends the site's own standings-season id).
        params: query parameters; only ``/loadResults`` uses them. ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The parsed JSON body of a game route, the game page record, or the merged results / standings record, with
        ``_request`` stamped: ``{"game_id", "home_team_id", "away_team_id", "players"}`` for a game route (the
        slug -> player id map from the game roster) or ``{"season", "season_id"}`` for a season page.

    Raises:
        NoDataError: see the module docstring (HTTP 404, a season the site lacks, no plays or shots).
        ValueError: a game id or season that is not an integer, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer, including an unfinished game.
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for an LKL request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    parts = urlsplit(url)
    if parts.netloc != urlsplit(SITE).netloc:
        raise ValueError(f"not an LKL route: {_rt._where(url)}")
    segs: Tuple[str, ...] = tuple(s for s in parts.path.split("/") if s)
    if len(segs) == 3 and segs[:2] == ("api", "livestream") and segs[2] in _API_ROUTES:
        raise ValueError(f"an LKL game route needs a game id: {_rt._where(url)}")
    if len(segs) == 4 and segs[:2] == ("api", "livestream") and segs[2] in _API_ROUTES:
        return _api(segs[2], _int_arg(segs[3], "game id"))
    if len(segs) == 2 and segs[0] == "rungtynes":
        return _game_page(_int_arg(segs[1], "game id"))
    if segs == ("loadResults",):
        if q.get("season") is None:
            raise ValueError("lkl_results needs season (the END year, 2026 = 2025-26)")
        return _results(_int_arg(q["season"], "season"))
    if len(segs) == 2 and segs[0] == "loadStandings":
        return _standings(_int_arg(segs[1], "season"))
    raise ValueError(f"not an LKL route: {_rt._where(url)}")
