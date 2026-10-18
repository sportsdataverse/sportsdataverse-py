"""Runtime for the generated CEBL wrappers (``api.data.cebl.ca``, the Canadian Elite Basketball League's own stats API).

The API is the JSON backend of the CEBL Stats Hub (cebl-stats-hub.web.app). Measured against the live host
(sdv-internal-refs ``cebl/``, 2026-10-12 and 2026-10-13). **CEBL's own API, owner-approved gray area**: the key
below is CEBL's first-party key for its own hub, not issued to third parties.

* **Header key.** Every route needs ``X-Api-Key: <key>`` (401 ``{"detail": "Unauthorized"}`` without it). The
  key is public: it is hard-coded in the Stats Hub's page chunk (``/_next/static/chunks/app/league-leaders/
  page-<hash>.js``, the axios client of the hub). This runtime reads it from there at run time (two requests,
  once per process; accepted only if it is 20-64 letters / digits) and falls back to the documented
  :data:`FALLBACK_KEY` (the value of 2026-10-13) when the bundle cannot be read. On a 401 it re-reads the bundle
  once, then raises ``AssetFetchError`` with a "key rotated?" hint. The key is NEVER printed, logged or put in an
  error message: every message passes through :func:`_scrub`. ``Origin`` and ``Referer`` are not checked by the
  API; they are sent as the hub sends them.
* **Season key.** The calendar year (May to August) = the sdv END year, so the mapping is the identity. A
  season with no data (2018, 2099) answers ``200 []`` and is ``NoDataError``. The routes that carry a date
  (``games``, ``gamelog``) are checked against the requested year.
* **Unknown ids are silent.** An unknown season, team or player answers HTTP 200 ``[]`` (leaders: ``{"POINTS": []}``;
  the player profile: an object whose every field is null), never another object's body, so an empty answer is
  ``NoDataError``. A missing required query param answers 422 (``ValueError``).
* **Wrong-scope bodies are refused.** Most bodies echo nothing about the request, so what the body does carry is
  checked: game dates against the season, a team filter against ``home_team_id`` / ``away_team_id`` / ``team_id``,
  a player id against ``id``, the leaders' keys against the statistics asked for, a career row against
  ``career_only``, and a team table's single league row. The rest (standings, rosters, team and player tables
  without a team filter) have no echo and are checked for shape only.
* **Error vocabulary.** ``NoDataError``: HTTP 404, an empty list, an all-null profile. ``ValueError``: HTTP 400 /
  422, an argument outside the closed sets. ``AssetFetchError``: any other non-200 (429 / 5xx after three attempts,
  spaced by ``dl_utils.download`` and honouring ``Retry-After``), a dead connection, a rejected key, a non-JSON
  body, an error envelope, a changed envelope and any body for another season, team or player than the one asked.
* **No unfinished-game refusal.** This API has no per-game route (box score, play-by-play and shots are Genius
  LiveStats, see ``games`` ``genius_match_id``); the schedule legitimately lists unplayed and cancelled games.
* **Personal data.** Contact fields (e-mail, phone, address) are dropped from every body; the API serves none
  today. A player's name, nationality and birth date are the public bio and stay.

Every attempt, retries and the bundle reads included, goes through the shared ``basketball_runtime.SESSION`` and so
waits its turn (at least 1 s per host).
"""

from __future__ import annotations

import json
import re
import threading
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import NOREDIRECT_SESSION as SESSION
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["API", "FALLBACK_KEY", "REQUEST_KEY", "SEGMENTS", "STATISTICS"]

HUB = "https://cebl-stats-hub.web.app"
API = "https://api.data.cebl.ca"
#: Public key from the Stats Hub page chunk, read 2026-10-13 (also in the spec's ``securitySchemes``).
FALLBACK_KEY = "800chyzv2hvur3z0ogh39cve2zok0c"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 502, 503, 504})  # 500 is the API's answer to a bad id: not retried
MODES = ("TOTALS", "PER_GAME")
COMPETITIONS = ("REGULAR", "FINALS")
SEGMENTS = ("HOME", "AWAY", "L7D", "L5G", "L10G", "MAY", "JUNE", "JULY", "AUGUST")
CONFERENCES = ("EAST", "WEST")
STATISTICS = (
    "POINTS",
    "REBOUNDS",
    "ASSISTS",
    "BLOCKS",
    "STEALS",
    "TARGET_SCORES",
    "FIELD_GOAL_PERCENTAGE",
    "THREE_POINTS_MADE",
    "THREE_POINT_PERCENTAGE",
    "FREE_THROW_PERCENTAGE",
    "FOULS",
    "TURNOVERS",
    "MINUTES",
)

_KEY_RE = re.compile(r"[A-Za-z0-9]{20,64}")
_PAGE_CHUNK = re.compile(r"static/chunks/app/league-leaders/page-[0-9a-f]+\.js")
_BUNDLE_KEY = re.compile(r'"X-Api-Key"\s*:\s*"([^"]{1,80})"')
_ERROR_KEYS = frozenset({"error", "errors", "message", "exception", "trace", "detail"})
#: Contact keys (dropped); nothing else personal is served.
_PERSONAL = re.compile(r"(e_?mail|phone|mobile|^tel$|address|passport|salary)", re.IGNORECASE)

_key: Dict[str, Optional[str]] = {"value": None, "source": None}
_key_lock = threading.Lock()


# --------------------------------------------------------------------------------------- secrets
def _scrub(text: Any) -> str:
    """``text`` with the key in use and the documented fallback replaced by ``<key>``."""
    out = str(text)
    for k in {_key["value"], FALLBACK_KEY}:
        if k:
            out = out.replace(k, "<key>")
    return out


def _fail(msg: str) -> AssetFetchError:
    return AssetFetchError(_scrub(msg))


# --------------------------------------------------------------------------------------- transport
def _fetch(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries 429 / 502 / 503 / 504 up to :data:`ATTEMPTS` times with a ``Retry-After``-aware
    delay and raises ``NoDataError`` on a 404. A connection failure that outlives the retries becomes an
    ``AssetFetchError`` naming only the exception class (never the URL's query or any header).
    """
    try:
        return download(
            url,
            params=params,
            headers=headers,
            session=SESSION,
            timeout=TIMEOUT,
            num_retries=ATTEMPTS - 1,
            retry_statuses=RETRY_STATUSES,
            cache_ttl=0,
        )
    except OSError as exc:  # requests' RequestException subclasses OSError
        raise _fail(f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}") from None


def _read_key(*, renew: bool = False) -> str:
    """The ``X-Api-Key``: the hub bundle's, else :data:`FALLBACK_KEY`. Cached per process."""
    with _key_lock:
        if _key["value"] is not None and not renew:
            return str(_key["value"])
        found: Optional[str] = None
        try:
            page = _fetch(HUB + "/league-leaders")
            m = (
                _PAGE_CHUNK.search(getattr(page, "text", "") or "")
                if getattr(page, "status_code", None) == 200
                else None
            )
            if m:
                js = _fetch(f"{HUB}/_next/{m.group(0)}")
                km = (
                    _BUNDLE_KEY.search(getattr(js, "text", "") or "")
                    if getattr(js, "status_code", None) == 200
                    else None
                )
                if km and _KEY_RE.fullmatch(km.group(1)):
                    found = km.group(1)
        except (AssetFetchError, NoDataError):
            found = None  # the documented fallback answers any failure to read the bundle
        _key["value"], _key["source"] = (found, "bundle") if found else (FALLBACK_KEY, "fallback")
        return str(_key["value"])


def _call(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """GET ``API + path`` with the key; one key renewal on a 401. Returns the parsed JSON body.

    Raises:
        NoDataError: HTTP 404.
        ValueError: HTTP 400 / 422.
        AssetFetchError: a rejected key (after a renewal), any other non-200, a non-JSON or empty body.
    """
    url = API + path
    where = _rt._where(url)
    resp: Any = None
    used: Optional[str] = None
    for renew in (False, True):
        key = _read_key(renew=renew)
        if renew and key == used:
            break  # the bundle still holds the key the API just refused
        used = key
        headers = {"X-Api-Key": key, "Origin": HUB, "Referer": HUB + "/", "Accept": "application/json"}
        resp = _fetch(url, params, headers)
        if getattr(resp, "status_code", None) != 401:
            break
    status = getattr(resp, "status_code", None)
    if status == 401:
        raise _fail(
            f"{where} answered HTTP 401: the X-Api-Key was rejected (key rotated? re-read it from the "
            f"app/league-leaders page chunk linked from {HUB}/league-leaders and update FALLBACK_KEY)"
        )
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    if status != 200:
        raise _fail(f"{where} answered HTTP {status}, not 200")
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    text = getattr(resp, "text", "") or ""
    if "json" not in ctype:
        raise _fail(f"{where} answered {ctype or 'no content type'}, not JSON")
    if not text.strip():
        raise _fail(f"{where} answered HTTP 200 with an empty body")
    try:
        return json.loads(text)
    except ValueError:
        raise _fail(f"{where} answered a body that is not JSON") from None


# --------------------------------------------------------------------------------------- shape helpers
def _scrub_personal(obj: Any) -> Any:
    """``obj`` without contact keys."""
    if isinstance(obj, dict):
        return {k: _scrub_personal(v) for k, v in obj.items() if not _PERSONAL.search(str(k))}
    if isinstance(obj, list):
        return [_scrub_personal(v) for v in obj]
    return obj


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _rows(body: Any, where: str) -> List[Dict[str, Any]]:
    """``body`` as a non-empty list of objects (``NoDataError`` when empty, ``AssetFetchError`` on any other shape)."""
    if isinstance(body, dict) and _ERROR_KEYS & set(body):
        raise _fail(f"{where}: the body is an error envelope")
    if not isinstance(body, list):
        raise _fail(f"{where}: not a list of rows")
    if not body:
        raise NoDataError(f"{where}: no rows (an unknown season, team or player, or nothing played yet)")
    if not all(isinstance(r, dict) for r in body):
        raise _fail(f"{where}: a row is not an object")
    return body


def _year(stamp: Any) -> Optional[int]:
    m = re.match(r"(\d{4})-", stamp) if isinstance(stamp, str) else None
    return int(m.group(1)) if m else None


def _each(rows: List[Dict[str, Any]], ok: Any, where: str, what: str) -> None:
    if not all(ok(r) for r in rows):
        raise _fail(f"{where}: a row is for another {what} than the one asked for")


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be an integer; got {value!r}")
    return int(value)


def _season_arg(value: Any) -> int:
    season = _int_arg(value, "season")
    if not 1000 <= season <= 9999:
        raise ValueError(f"season must be a 4-digit calendar year; got {value!r}")
    return season


def _choice(value: Any, allowed: tuple, what: str) -> Optional[str]:
    if value is None:
        return None
    if value not in allowed:
        raise ValueError(f"{what} must be one of {allowed}; got {value!r}")
    return str(value)


def _team_ok(team_id: Optional[int]) -> Any:
    return (lambda r: r.get("team_id") == team_id) if team_id is not None else (lambda r: True)


# --------------------------------------------------------------------------------------- routes
_ROUTES = [
    ("seasons", re.compile(r"^/seasons/$")),
    ("standings", re.compile(r"^/teams/(?P<season>\d+)/$")),
    ("teams", re.compile(r"^/teams/$")),
    ("roster", re.compile(r"^/teams/(?P<team_id>\d+)/roster/(?P<season>\d+)/$")),
    ("team_stats", re.compile(r"^/teams/statistics/aggregated/$")),
    ("games", re.compile(r"^/games/(?P<season>\d+)/$")),
    ("player_stats", re.compile(r"^/players/statistics/aggregated/$")),
    ("leaders", re.compile(r"^/players/statistics/leaders/$")),
    ("player_seasons", re.compile(r"^/players/(?P<player_id>\d+)/statistics/$")),
    ("player", re.compile(r"^/players/(?P<player_id>\d+)/$")),
    ("gamelog", re.compile(r"^/players/(?P<player_id>\d+)/gamelog/$")),
    ("news", re.compile(r"^/news/articles/(?P<language>[A-Za-z_-]+)/$")),
]


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a CEBL route, refuse every wrong answer, return ``{"rows": <body>, "_request": <stamp>}``.

    Args:
        url: the URL a generated wrapper built (``https://api.data.cebl.ca/games/2025/`` ...). Path ids are
            checked to be integers and the season to be a 4-digit year.
        params: query parameters; ``None`` values are dropped. ``mode`` / ``competition`` / ``segment`` /
            ``vs_conference`` are checked against their closed sets and ``career_only`` is sent as ``true`` /
            ``false``.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        ``{"rows": body, "_request": {"kind", "season", "mode", "competition", "segment", "team_id", ...}}``;
        ``body`` is the API's list (or, for the leaders, its object keyed by statistic) with contact fields dropped.

    Raises:
        NoDataError: HTTP 404, an empty list, an all-null profile (an unknown id or season).
        ValueError: an argument outside its set, an id that is not an integer, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a CEBL request: {sorted(kwargs)}")
    parts = urlsplit(url)
    if f"{parts.scheme}://{parts.netloc}" != API:
        raise ValueError(f"not a CEBL route: {_rt._where(url)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    for kind, pattern in _ROUTES:
        m = pattern.match(parts.path)
        if m:
            break
    else:
        raise ValueError(f"not a CEBL route: {_rt._where(url)}")
    g = m.groupdict()
    where = _rt._where(url)
    stamp: Dict[str, Any] = {"kind": kind}
    send: Dict[str, Any] = {}
    team_id = player_id = None
    if "season" in g:
        stamp["season"] = _season_arg(g["season"])
    elif "season" in q:
        stamp["season"] = _season_arg(q["season"])
        send["season"] = stamp["season"]
    if "team_id" in g:
        stamp["team_id"] = _int_arg(g["team_id"], "team_id")
    elif "team_id" in q:
        team_id = stamp["team_id"] = send["team_id"] = _int_arg(q["team_id"], "team_id")
    if "player_id" in g:
        player_id = stamp["player_id"] = _int_arg(g["player_id"], "player_id")
    if kind in ("team_stats", "player_stats", "leaders", "player_seasons"):
        stamp["mode"] = send["mode"] = _choice(q.get("mode"), MODES, "mode")
        if stamp["mode"] is None:
            raise ValueError("mode is required: TOTALS or PER_GAME")
    if kind in ("team_stats", "player_stats", "leaders", "player_seasons", "gamelog"):
        stamp["competition"] = send["competition"] = _choice(q.get("competition"), COMPETITIONS, "competition")
    if kind in ("team_stats", "player_stats"):
        stamp["segment"] = send["segment"] = _choice(q.get("segment"), SEGMENTS, "segment")
    if kind == "team_stats":
        stamp["vs_conference"] = send["vs_conference"] = _choice(q.get("vs_conference"), CONFERENCES, "vs_conference")
        if "vs_team_id" in q:
            stamp["vs_team_id"] = send["vs_team_id"] = _int_arg(q["vs_team_id"], "vs_team_id")
    if kind == "leaders":
        names = [s.strip() for s in str(q.get("statistics", "")).split(",") if s.strip()]
        if not names:
            raise ValueError("statistics is required: a comma-separated list such as 'POINTS,REBOUNDS'")
        stamp["statistics"] = send["statistics"] = ",".join(names)
    if kind == "player_seasons":
        career = q.get("career_only", False)
        if not isinstance(career, bool):
            raise ValueError(f"career_only must be a bool; got {career!r}")
        stamp["career_only"] = career
        send["career_only"] = "true" if career else "false"
    if kind == "news":
        stamp["language"] = g["language"]
        if "team_source_id" in q:
            stamp["team_source_id"] = send["team_source_id"] = _int_arg(q["team_source_id"], "team_source_id")
    body = _scrub_personal(_call(parts.path, send or None))

    if kind == "leaders":
        if isinstance(body, dict) and _ERROR_KEYS & set(body):
            raise _fail(f"{where}: the body is an error envelope")
        if not isinstance(body, dict) or set(body) != set(stamp["statistics"].split(",")):
            raise _fail(f"{where}: the body is not an object keyed by the statistics asked for")
        lists = list(body.values())
        if not all(isinstance(v, list) and all(isinstance(r, dict) for r in v) for v in lists):
            raise _fail(f"{where}: a statistic is not a list of rows")
        if not any(lists):
            raise NoDataError(f"{where}: no leaders (an unknown season or team, or a statistic name the API lacks)")
        for rows in lists:
            _each(rows, _team_ok(team_id), where, "team")
        return {"rows": body, REQUEST_KEY: stamp}
    if kind == "player":
        if isinstance(body, dict) and _ERROR_KEYS & set(body):
            raise _fail(f"{where}: the body is an error envelope")
        if not isinstance(body, dict):
            raise _fail(f"{where}: not a player object")
        if body.get("id") is None and not any(v is not None for v in body.values()):
            raise NoDataError(f"{where}: an all-null profile (an unknown player id)")
        if body.get("id") != player_id:
            raise _fail(f"{where}: the profile is for another player than the one asked for")
        return {"rows": body, REQUEST_KEY: stamp}

    rows = _rows(body, where)
    season = stamp.get("season")
    if kind == "seasons":
        _each(rows, lambda r: _is_int(r.get("year")), where, "season")
    elif kind == "games":
        _each(rows, lambda r: _year(r.get("start_time_utc")) == season, where, "season")
        if team_id is not None:
            _each(rows, lambda r: team_id in (r.get("home_team_id"), r.get("away_team_id")), where, "team")
    elif kind == "gamelog" and season is not None:
        _each(rows, lambda r: _year(r.get("game_date")) == season, where, "season")
    elif kind == "team_stats":
        if sum(1 for r in rows if r.get("id") is None) != 1:
            raise _fail(f"{where}: a team table must hold exactly one league row")
    elif kind == "player_stats":
        _each(rows, _team_ok(team_id), where, "team")
    elif kind == "player_seasons":
        _each(rows, lambda r: r.get("id") == player_id, where, "player")
        if stamp["career_only"] and not (len(rows) == 1 and rows[0].get("season") is None):
            raise _fail(f"{where}: career_only must answer the one career row")
    return {"rows": rows, REQUEST_KEY: stamp}
