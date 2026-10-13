"""Runtime for the generated B.LEAGUE wrappers (public S3 game contexts + www.bleague.jp pages).

Two keyless hosts. Measured against the live site (sdv-internal-refs ``bleague/``, 2026-10-12):

* **Season key.** The public ``season`` is the END year (2026 = 2025-26); the site's ``Year`` / ``year=`` is the
  START year, so the runtime sends ``season - 1``. A game wrapper takes only the global ``schedule_key``; its
  ``season`` is an optional guard: when given, a context whose ``Year`` differs is refused.
* **Feed fallback (read at run time, no static table).** For a ``schedule_key`` the runtime asks
  ``v2_genius_contexts/{key}/latestid`` first (B1/B2 2020-21 on), then ``v1_contexts`` (B3 and B1/B2 2018-2020),
  then the ``www.bleague.jp/game_detail/`` page, whose embedded context is the only copy of 2016-17 and 2017-18.
  S3 answers ``403 AccessDenied`` for a key it does not hold, which moves to the next source; all three missing is
  ``NoDataError``. The context found is stamped ``_request = {"schedule_key", "feed"}`` (``feed`` is ``v1`` / ``v2``).
* **Refused (AssetFetchError), fail closed:** a body that is not the feed's context shape, an error envelope, a
  ``Game.ScheduleKey`` or any row's ``ScheduleKey`` that is not the request, a box row of a team that is not one
  of the game's two, a ``Year`` that differs from a given ``season``, an embed whose key is another game, a game
  with ``GameEndedFlg`` / ``RecordFixedFlg`` not both true (unfinished), a non-integer ``latestid``, a schedule
  page for another tab / season / event / day, a paged (truncated) schedule list, standings of another season,
  layout drift, and any non-200 other than the 403/404 miss.
* **Error vocabulary.** ``NoDataError``: no source holds the game, HTTP 404, a schedule day without games.
  ``ValueError``: a non-integer argument or HTTP 400 / 422. ``AssetFetchError``: everything else.

Every attempt, retries included (429 / 5xx, ``Retry-After`` honoured by ``dl_utils.download``), goes through the
shared ``basketball_runtime.SESSION`` and waits its turn (>= 1 s per host); ``robots.txt`` sets ``Crawl-delay``
only for msnbot (10) and bingbot (30). Contact and birth-date keys are replaced by ``<redacted>`` in every body.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.basketball.bleague.bleague_parsers import (
    FEED_KEYS,
    FEED_NAMES,
    _embed_context,
    _NotParsed,
    _same,
    _schedule_record,
    _standings_record,
)
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["REQUEST_KEY"]

S3 = "https://b-league.s3.amazonaws.com/web_json"
SITE = "https://www.bleague.jp"
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
#: Division tab -> the league-season competition type the schedule page defaults to.
DEFAULT_EVENT = {1: 2, 2: 7, 3: 15}

_CONTACT = re.compile(r"(?i)(e-?mail|phone|^tel$|tel_?no|address|^zip$)")
_BIRTH = re.compile(r"(?i)(birth|^dob$)")
_STAFF = re.compile(r"(?i)(referee|commissioner|coach|official|staff|umpire)")
_ERROR_KEYS = frozenset({"message", "error", "errors", "status", "exception", "trace", "code"})


def _redact(obj: Any, path: str = "") -> Any:
    """``obj`` with contact keys (any depth) and dates of birth under an official / staff key replaced."""
    if isinstance(obj, dict):
        return {
            k: (
                "<redacted>"
                if _CONTACT.search(k) or (_BIRTH.search(k) and _STAFF.search(path + "." + k))
                else _redact(v, f"{path}.{k}")
            )
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact(v, path) for v in obj]
    return obj


# ----------------------------------------------------------------------------- transport
def _fetch(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response."""
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


def _status(url: str, resp: Any) -> None:
    status = getattr(resp, "status_code", None)
    if status == 200:
        return
    where = _rt._where(url)
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    raise AssetFetchError(f"{where} answered HTTP {status}, not 200")


def _is_miss(resp: Any) -> bool:
    """S3's answer for a key it does not hold: 403 with an AccessDenied XML body."""
    return getattr(resp, "status_code", None) == 403 and "AccessDenied" in (getattr(resp, "text", "") or "")


def _text(url: str, params: Optional[Dict[str, Any]] = None, *, kind: str) -> str:
    resp = _fetch(url, params)
    _status(url, resp)
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    where = _rt._where(url)
    if kind == "html" and "html" not in ctype:
        raise AssetFetchError(f"{where} answered {ctype or 'no content type'}, not HTML")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise AssetFetchError(f"{where} answered HTTP 200 with an empty body")
    return text


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be an integer; got {value!r}")
    return int(value)


# ------------------------------------------------------------------------------- contexts
def _scope_error(body: Any, feed: str, key: int, season: Optional[int]) -> Optional[str]:
    """Why ``body`` is not a final, in-scope context of game ``key``, or ``None``. Fails closed."""
    if not isinstance(body, dict) or set(body) != FEED_KEYS[feed]:
        return f"not a {feed} context: keys {sorted(body) if isinstance(body, dict) else type(body).__name__}"
    if set(body) <= _ERROR_KEYS:
        return "error envelope"
    game = body["Game"] if isinstance(body["Game"], dict) else {}
    if not _same(game.get("ScheduleKey"), key):
        return f"out of scope: Game.ScheduleKey {game.get('ScheduleKey')!r}, asked for {key!r}"
    if season is not None and not _same(game.get("Year"), season - 1):
        return f"out of scope: Year {game.get('Year')!r}, asked for season {season} (Year {season - 1})"
    if game.get("GameEndedFlg") is not True or game.get("RecordFixedFlg") is not True:
        return "game not final (GameEndedFlg / RecordFixedFlg not both true)"
    teams = {game.get("HomeTeamID"), game.get("AwayTeamID")}
    if None in teams or len(teams) != 2:
        return "game header without two team ids"
    for name in ("PlayByPlays", "HomeBoxscores", "AwayBoxscores", "Summaries", "Leaders", "Points"):
        if name not in body:
            continue
        rows = body[name]
        if not isinstance(rows, list) or (
            name in ("PlayByPlays", "HomeBoxscores", "AwayBoxscores", "Summaries") and not rows
        ):
            return f"{name} empty or not a list"
        for r in rows:
            if not isinstance(r, dict) or not _same(r.get("ScheduleKey"), key):
                return f"out of scope: a {name} row of game {r.get('ScheduleKey') if isinstance(r, dict) else r!r}"
            if name.endswith("Boxscores"):
                totals = _same(r.get("Category"), 3) and r.get("TeamID") is None
                if not totals and r.get("TeamID") not in teams:
                    return f"out of scope: {name} row of team {r.get('TeamID')!r}"
    return None


def _embed_body(html: str, key: int) -> Tuple[Dict[str, Any], str]:
    rec = _embed_context(html)
    admin = rec.get("admin_name")
    feed = next((f for f, n in FEED_NAMES.items() if n == admin), None)
    if feed is None:
        raise _NotParsed(f"embed admin_name {admin!r} is not a known feed")
    if not _same(rec.get("ScheduleKey"), str(key)):
        raise _NotParsed(f"out of scope: embed ScheduleKey {rec.get('ScheduleKey')!r}, asked for {str(key)!r}")
    return {k: rec[k] for k in FEED_KEYS[feed] if k in rec}, feed


def _context(key: int, season: Optional[int]) -> Dict[str, Any]:
    """The final context of game ``key``: v2 feed, else v1 feed, else the game page's embed."""
    where = f"B.LEAGUE game {key}"
    for feed in ("v2", "v1"):
        base = f"{S3}/{FEED_NAMES[feed]}/{key}"
        resp = _fetch(f"{base}/latestid")
        if _is_miss(resp):
            continue
        _status(f"{base}/latestid", resp)
        latest = (getattr(resp, "text", "") or "").strip()
        if not re.fullmatch(r"\d{1,9}", latest):
            raise AssetFetchError(f"{_rt._where(base)}/latestid: not an integer snapshot id")
        url = f"{base}/{latest}.json"
        resp = _fetch(url)
        _status(url, resp)
        try:
            body = json.loads(getattr(resp, "text", "") or "")
        except ValueError:
            raise AssetFetchError(f"{_rt._where(url)} answered a body that is not JSON") from None
        why = _scope_error(body, feed, key, season)
        if why:
            raise AssetFetchError(f"{_rt._where(url)}: {why}")
        return _stamp(body, key, feed)
    page = f"{SITE}/game_detail/"
    html = _text(page, {"ScheduleKey": key, "tab": 4}, kind="html")
    try:
        body, feed = _embed_body(html, key)
    except _NotParsed as exc:
        if "no _contexts_s3id" in str(exc):
            raise NoDataError(f"{where}: no S3 context and no embedded context on the game page") from None
        raise AssetFetchError(f"{_rt._where(page)}: {exc}") from None
    why = _scope_error(body, feed, key, season)
    if why:
        raise AssetFetchError(f"{_rt._where(page)}: {why}")
    return _stamp(body, key, feed)


def _stamp(body: Dict[str, Any], key: int, feed: str) -> Dict[str, Any]:
    out = _redact(body)
    out[REQUEST_KEY] = {"schedule_key": key, "feed": feed}
    return out


# ---------------------------------------------------------------------------------- pages
def _schedule(q: Dict[str, Any]) -> Dict[str, Any]:
    season = _int_arg(q.get("season"), "season")
    tab = _int_arg(q.get("tab"), "tab")
    mon = _int_arg(q.get("month"), "month")
    day = _int_arg(q.get("day"), "day")
    event = _int_arg(q["event"], "event") if q.get("event") is not None else DEFAULT_EVENT.get(tab)
    if event is None or tab not in (1, 2, 3) or not 1 <= mon <= 12 or not 1 <= day <= 31:
        raise ValueError("bleague_schedule needs tab 1-3, month 1-12, day 1-31 (and event when tab is not 1-3)")
    year = season - 1
    html = _text(
        f"{SITE}/schedule/",
        {"tab": tab, "year": year, "event": event, "mon": f"{mon:02d}", "day": f"{day:02d}"},
        kind="html",
    )
    try:
        return _schedule_record(html, tab, year, event, mon, day)
    except _NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{SITE}/schedule/: {exc}") from None
        raise AssetFetchError(f"{SITE}/schedule/: not the page asked for: {exc}") from None


def _standings(q: Dict[str, Any]) -> Dict[str, Any]:
    season = _int_arg(q.get("season"), "season")
    tab = _int_arg(q.get("tab"), "tab")
    if tab not in (1, 2, 3):
        raise ValueError("bleague_standings needs tab 1-3 (B1, B2, B3)")
    year = season - 1
    html = _text(f"{SITE}/standings/", {"year": year, "tab": tab}, kind="html")
    try:
        return _standings_record(html, year, tab)
    except _NotParsed as exc:
        raise AssetFetchError(f"{SITE}/standings/: not the page asked for: {exc}") from None


# ------------------------------------------------------------------------------ the getter
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a B.LEAGUE route, refuse every wrong answer, return the context or page record.

    Args:
        url: the URL a generated wrapper built: ``.../web_json/contexts/{schedule_key}`` (game wrappers; optional
            ``params["season"]`` guard), ``www.bleague.jp/schedule/`` (``season``, ``tab``, ``month``, ``day``,
            optional ``event``) or ``www.bleague.jp/standings/`` (``season``, ``tab``).
        params: query parameters; ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The game context (``Game``, ``PlayByPlays``, ``HomeBoxscores``, ``AwayBoxscores``, ``Summaries`` and on the
        v1 feed ``Leaders``, ``Points``) stamped ``_request = {"schedule_key", "feed"}``, or the schedule /
        standings page record.

    Raises:
        NoDataError: no source holds the game; a schedule day without games; HTTP 404.
        ValueError: a non-integer or out-of-range argument, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer, including an unfinished game.
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a B.LEAGUE request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    parts = urlsplit(url)
    segs = tuple(s for s in parts.path.split("/") if s)
    if parts.netloc == urlsplit(S3).netloc and len(segs) == 3 and segs[:2] == ("web_json", "contexts"):
        season = _int_arg(q["season"], "season") if q.get("season") is not None else None
        return _context(_int_arg(segs[2], "schedule_key"), season)
    if parts.netloc == urlsplit(SITE).netloc and segs == ("schedule",):
        return _schedule(q)
    if parts.netloc == urlsplit(SITE).netloc and segs == ("standings",):
        return _standings(q)
    raise ValueError(f"not a B.LEAGUE route: {_rt._where(url)}")
