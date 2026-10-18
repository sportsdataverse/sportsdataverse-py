"""Runtime for the generated CBA wrappers (``data-server.cbaleague.com``, the men's Chinese Basketball Association).

Three things make this host unusual (sdv-internal-refs ``cba/``, live-verified 2026-10-12):

* **Every body is AES-128-ECB / PKCS7 encrypted** (a JSON string literal holding base64 of the ciphertext). The
  16-character key is a public constant in the site's front-end bundle (function ``TIe`` of
  ``cbaleague.com/data/assets/index.<hash>.js``); :mod:`cba_aes` is the stdlib implementation, so no dependency is
  added. ponytail: the key is a constant here; if the site rotates it every request fails with an
  ``AssetFetchError`` naming the decrypt step, and the fix is the new constant from the bundle.
  Anything that does not decrypt is REFUSED: the API's unencrypted HTTP 500 envelope, an empty 200 body, a plain body.
* **The host blocks datacenter IPs at TCP.** Set ``CBA_PROXY`` (a residential proxy URL; owner ruling: Decodo, small
  requests only). The value is read by :func:`basketball_runtime.proxy_from_env`, never logged and never put in an
  error. Without it the request goes direct and fails FAST from a datacenter: a 5 s connect timeout
  (:data:`FAST_CONNECT_TIMEOUT`) and no retry of a refused or timed-out CONNECT (a read timeout or a connection reset
  mid-response is still retried), so the ``AssetFetchError`` naming
  ``CBA_PROXY`` comes in seconds, not after three 60 s connect timeouts. On a network where direct access works (a
  residential IP), set ``CBA_DIRECT=1`` (:data:`DIRECT_ENV`): direct requests then get the full timeout and retries.
* **Several game routes answer team-season tables, not game data** (``schedule-shoot-list``,
  ``schedule-four-elements``): their rows cannot be scoped to the game (documented in the wrapper notes).

**Season key.** The public ``season`` is the END year (2026 = 2025-26); the API takes the START year, so the runtime
sends ``season - 1`` (:func:`wire_season`). The reference lists are not echoed (``getTeam``, ``team-count``); the
season player lists echo ``season`` on every row, which is checked.

**Finished games only.** Every game route first reads the game report header (``baseInfo``, cached per process): a
game without two integer scores and both team ids is not final and raises ``AssetFetchError``. Box, period, roster and
best-player bodies are then checked against it (player points sum to the score, period points sum to the score, team
ids match, advanced rows are box players). Play-by-play is requested one period at a time by the API; the runtime
fetches every period (from the period scores) and refuses a partial set, an unordered clock or a final score the
plays do not reach; a game without play-by-play (the site has none for 2009-10) is ``NoDataError``.

**Error vocabulary.** ``NoDataError``: HTTP 404, a game without play-by-play. ``ValueError``: a non-integer id or
season, HTTP 400 / 422. ``AssetFetchError``: any other non-200 (429 / 502 / 503 / 504 retried three times, spaced and
honouring ``Retry-After``; HTTP 500 is NOT retried: this API answers it for a body it dislikes), a dead connection, an
empty or undecryptable body, an unfinished game, and every wrong-scope body.

Every attempt, retries included, goes through ``basketball_runtime.SESSION`` and waits its turn (>= 1 s per host).
Operator ids (``createdBy`` / ``lastModifiedBy``) and any bare mobile-number string are removed from every body.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import requests
from urllib3.exceptions import NewConnectionError

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION, proxy_from_env
from sportsdataverse.basketball.cba.cba_aes import Undecryptable, decrypt_body
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["DIRECT_ENV", "PROXY_ENV", "REQUEST_KEY", "wire_season"]

PROXY_ENV = "CBA_PROXY"
#: Set to ``1`` where direct access works (a residential IP): no proxy, but the full timeout and retries.
DIRECT_ENV = "CBA_DIRECT"
REQUEST_KEY = "_request"
API = "https://data-server.cbaleague.com"
#: Public constant from the site bundle (see the module docstring).
_KEY = b"uVayqL4ONKjFbVzQ"
HEADERS = {"Referer": "https://www.cbaleague.com/data/"}
ATTEMPTS = 3
TIMEOUT = 60
#: Connect timeout of a direct request without :data:`DIRECT_ENV` (the host drops datacenter connections at TCP).
FAST_CONNECT_TIMEOUT = 5
RETRY_STATUSES = frozenset({429, 502, 503, 504})
MAX_RETRY_AFTER = 30.0
_POST = frozenset(
    {
        "player-match-datas/data-count",
        "player-match-datas/advance-count",
        "player-match-datas/schedule-other-data",
        "schedule-quarters/getQuarterInfo",
        "team-match-datas/schedule-shoot-list",
        "teams/schedule-four-elements",
        "schedule-eventses/filterPage",
        "teams/basic-data",
        "teams/four-elements",
        "team-match-datas/team-entirety-list",
        "team-match-datas/team-shoot-list",
        "player-base-list",
        "player-offensive-list",
        "player-defensive-list",
        "player-shoot-list",
    }
)
#: Game routes whose scheduleId is a path segment.
_PATH_ID = (
    "schedules/getPlayersByScheduleId",
    "team-match-datas/getMatchDataReport/baseInfo",
    "team-match-datas/getMatchDataReport/bestPlayer",
    "team-match-datas/getMatchDataReport/bestDataQuota",
    "team-match-datas/getMatchDataReportStatues",
)
#: Game routes whose scheduleId is a body field.
_BODY_ID = frozenset(r for r in _POST if r.split("/")[-1] in {
    "data-count", "advance-count", "schedule-other-data", "getQuarterInfo", "schedule-shoot-list",
    "schedule-four-elements", "filterPage",
})  # fmt: skip
_SEASON_ROUTES = frozenset(
    {
        "com-code-tables/getTeam",
        "team-count/season",
        "player-base-list",
        "player-offensive-list",
        "player-defensive-list",
        "player-shoot-list",
    }
)
_PLAYER_LISTS = frozenset({"player-base-list", "player-offensive-list", "player-defensive-list", "player-shoot-list"})
_MOBILE = re.compile(r"^1[3-9]\d{9}$")
_REDACT_KEYS = frozenset({"createdBy", "lastModifiedBy"})

#: Per-process caches of finished games (header and box), keyed by scheduleId.
_INFO: Dict[int, Dict[str, Any]] = {}
_BOX: Dict[int, Dict[str, Any]] = {}


def wire_season(season: Any) -> int:
    """The API's START-year ``season`` for the public END year (2026 -> 2025).

    Args:
        season: the END year (2026 = the 2025-26 season).

    Returns:
        The START year the API takes.

    Raises:
        ValueError: ``season`` is not an integer.
    """
    return _int_arg(season, "season") - 1


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().lstrip("-").isdigit():
        raise ValueError(f"{what} must be an integer; got {value!r}")
    return int(value)


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _redact(v) for k, v in obj.items() if k not in _REDACT_KEYS}
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    if isinstance(obj, str) and _MOBILE.match(obj):
        return "<redacted>"
    return obj


def _retry_after(resp: Any) -> float:
    try:
        return min(max(float((getattr(resp, "headers", None) or {}).get("Retry-After", 0)), 0.0), MAX_RETRY_AFTER)
    except (TypeError, ValueError):
        return 0.0


def _dropped_at_connect(exc: BaseException) -> bool:
    """True when ``exc`` failed while OPENING the connection: a connect timeout, or a refused / unreachable connect.

    That is how the host turns away a datacenter IP. A connection that opened and then broke mid-response (a reset,
    ``RemoteDisconnected``) or a read timeout is an ordinary transient failure and is retried.
    """
    if isinstance(exc, requests.ConnectTimeout):
        return True
    seen: set = set()
    todo: List[Optional[BaseException]] = [exc]
    while todo:
        err = todo.pop()
        if err is None or id(err) in seen:
            continue
        seen.add(id(err))
        if isinstance(err, (ConnectionRefusedError, NewConnectionError)):
            return True
        reason = getattr(err, "reason", None)
        todo += [err.__cause__, err.__context__, reason if isinstance(reason, BaseException) else None]
        todo += [a for a in getattr(err, "args", ()) if isinstance(a, BaseException)]
    return False


# ----------------------------------------------------------------------------- transport
def _fetch(route: str, query: Dict[str, Any], body: Optional[Dict[str, Any]]) -> Any:
    """One request through the throttled session (every attempt waits its turn); returns the decrypted JSON."""
    url = f"{API}/api/{route}"
    where = _rt._where(url)
    method = "POST" if body is not None else "GET"
    proxies = proxy_from_env(PROXY_ENV)
    fast = proxies is None and os.environ.get(DIRECT_ENV, "").strip() != "1"
    timeout: Any = (FAST_CONNECT_TIMEOUT, TIMEOUT) if fast else TIMEOUT
    resp = None
    for attempt in range(ATTEMPTS):
        try:
            resp = SESSION.request(
                method, url, params=query or None, json=body, headers=HEADERS, timeout=timeout, proxies=proxies,
            )  # fmt: skip
        except requests.RequestException as exc:  # the message can carry the proxy URL: class name only
            dropped = fast and _dropped_at_connect(exc)  # turned away at connect: never retried without a proxy
            if attempt == ATTEMPTS - 1 or dropped:
                hint = (
                    f" (set {PROXY_ENV}: the host blocks datacenter IPs; {DIRECT_ENV}=1 if direct access works here)"
                    if proxies is None
                    else ""
                )
                tries = "on the first attempt" if dropped else f"after {ATTEMPTS} attempts"
                raise AssetFetchError(f"{where}: fetch failed {tries}: {type(exc).__name__}{hint}") from None
            continue
        if getattr(resp, "status_code", None) in RETRY_STATUSES and attempt < ATTEMPTS - 1:
            time.sleep(max(_retry_after(resp), min(2.0 * 2**attempt, MAX_RETRY_AFTER)))  # backoff, or Retry-After
            continue
        break
    status = getattr(resp, "status_code", None)
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    if status == 500:
        raise AssetFetchError(f"{where} answered HTTP 500 (the API refuses a body it dislikes; not retried)")
    if status != 200:
        raise AssetFetchError(f"{where} answered HTTP {status}, not 200")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise AssetFetchError(f"{where} answered HTTP 200 with an empty body (missing or invalid request fields?)")
    try:
        return _redact(decrypt_body(text, _KEY))
    except Undecryptable as exc:
        raise AssetFetchError(
            f"{where}: the body did not decrypt ({exc}); an error envelope or a rotated key"
        ) from None


# ------------------------------------------------------------------------------ game scope
def _info(sid: int) -> Dict[str, Any]:
    """The game report header of a FINISHED game (cached); anything else raises."""
    if sid in _INFO:
        return _INFO[sid]
    route = f"team-match-datas/getMatchDataReport/baseInfo/{sid}"
    body = _fetch(route, {}, None)
    where = _rt._where(f"{API}/api/{route}")
    if not isinstance(body, dict):
        raise AssetFetchError(f"{where}: the body is not an object")
    ints = [body.get(k) for k in ("hostScore", "guestScore", "hostTeamId", "guestTeamId")]
    if any(isinstance(v, bool) or not isinstance(v, int) for v in ints) or not (
        body["hostScore"] or body["guestScore"]
    ):
        raise AssetFetchError(
            f"{where}: game {sid} is not final (no two integer scores / team ids); CBA game wrappers serve finished games only"
        )  # noqa: E501
    _INFO[sid] = body
    return body


def _ids(rows: Any, key: str) -> set:
    return {r.get(key) for r in rows if isinstance(r, dict) and r.get(key) is not None}


def _box(sid: int) -> Dict[str, Any]:
    if sid not in _BOX:
        _BOX[sid] = _checked("player-match-datas/data-count", sid, {"scheduleId": str(sid)})
    return _BOX[sid]


def _scope_error(route: str, body: Any, sid: int, info: Dict[str, Any]) -> Optional[str]:
    """Why ``body`` is not game ``sid``'s answer, or ``None``. Fails closed."""
    host, guest = info["hostTeamId"], info["guestTeamId"]
    short = route.split("/")[-1] if route not in _PATH_ID else route
    if route.startswith("team-match-datas/getMatchDataReport/baseInfo"):
        return None
    if route.startswith("team-match-datas/getMatchDataReportStatues"):
        return None if isinstance(body, dict) and isinstance(body.get("isReport"), bool) else "no isReport flag"
    if route.startswith("team-match-datas/getMatchDataReport/bestPlayer"):
        ok = isinstance(body, dict) and any(
            isinstance(body.get(k), dict) for k in ("hostBestPlayer", "guestBestPlayer")
        )
        return None if ok else "no hostBestPlayer / guestBestPlayer object"
    if short == "data-count":
        if not isinstance(body, dict):
            return "the body is not an object"
        for key, score in (("homeCounts", info["hostScore"]), ("visitingCounts", info["guestScore"])):
            rows = body.get(key)
            if not isinstance(rows, list) or not rows:
                return f"no {key} rows"
            pts = sum(r.get("points") or 0 for r in rows if isinstance(r, dict) and r.get("playerId") is not None)
            if pts != score:
                return f"out of scope: {key} points {pts} != the game score {score}"
        return None
    if short == "getQuarterInfo":
        for key, score in (("home", info["hostScore"]), ("visiting", info["guestScore"])):
            blk = body.get(key) if isinstance(body, dict) else None
            if not isinstance(blk, dict) or sum(v for v in blk.values() if isinstance(v, int)) != score:
                return f"out of scope: {key} period points do not sum to the game score {score}"
        return None
    if short in ("advance-count", "schedule-other-data"):
        keys = ("homes", "visitings") if short == "advance-count" else ("homePlayers", "visitingPlayers")
        box = _ids(_box(sid).get("homeCounts", []) + _box(sid).get("visitingCounts", []), "playerId")
        if not isinstance(body, dict):
            return "the body is not an object"
        for key, team in zip(keys, (host, guest)):
            rows = body.get(key)
            if not isinstance(rows, list) or not rows:
                return f"no {key} rows"
            if not _ids(rows, "playerId") <= box:
                return f"out of scope: a {key} player is not in the game box score"
            if short == "advance-count" and _ids(rows, "teamId") - {team}:
                return f"out of scope: a {key} row is not team {team}'s"
        return None
    if short == "getPlayersByScheduleId" or route.startswith("schedules/getPlayersByScheduleId"):
        for key, team in (("hostPlayers", host), ("guestPlayers", guest)):
            rows = body.get(key) if isinstance(body, dict) else None
            if not isinstance(rows, list) or not rows or _ids(rows, "teamId") - {team}:
                return f"out of scope: {key} is empty or not team {team}'s"
        return None
    if route.startswith("team-match-datas/getMatchDataReport/bestDataQuota"):
        if not isinstance(body, dict):
            return "the body is not an object"
        box = {str(i) for i in _ids(_box(sid).get("homeCounts", []) + _box(sid).get("visitingCounts", []), "playerId")}
        for key in ("hostTeamBestData", "guestTeamBestData"):
            if _ids(body.get(key) or [], "playerId") - box:
                return f"out of scope: a {key} player is not in the game box score"
        return None
    if short == "schedule-shoot-list":
        rows = body if isinstance(body, list) else None
        if not rows or _ids(rows, "teamId") - {host, guest}:
            return "out of scope: rows are not the two teams of the game"
        return None
    if short == "schedule-four-elements":
        return None if isinstance(body, list) and body else "no rows"  # unscoped: previous-season team table
    return None


def _checked(
    route: str, sid: int, fields: Dict[str, Any], path_id: Optional[str] = None, qkeys: frozenset = frozenset()
) -> Any:
    """Fetch a game route (after the finished-game check) and refuse a body that is not this game's."""
    info = _info(sid)
    query = {k: v for k, v in fields.items() if k in qkeys}
    body = fields if route not in _POST else {k: v for k, v in fields.items() if k not in qkeys}
    full = f"{route}/{path_id}" if path_id is not None else route
    got = _fetch(full, query if route in _POST else fields, body if route in _POST else None)
    why = _scope_error(route, got, sid, info)
    if why:
        raise AssetFetchError(f"{_rt._where(f'{API}/api/{full}')}: {why}")
    return got


def _pbp(sid: int, quarter: Optional[int]) -> Dict[str, Any]:
    info = _info(sid)
    route = "schedule-eventses/filterPage"
    where = _rt._where(f"{API}/api/{route}")
    if quarter is None:
        periods = _checked("schedule-quarters/getQuarterInfo", sid, {"scheduleId": str(sid)})
        count = sum(1 for v in periods["home"].values() if isinstance(v, int))
        quarters = list(range(1, count + 1))
    else:
        quarters = [quarter]
    rows: List[Dict[str, Any]] = []
    for q in quarters:
        page = _fetch(route, {"startNum": 0}, {"scheduleId": str(sid), "quarterNum": q})
        recs = page.get("records") if isinstance(page, dict) else None
        if not isinstance(recs, list):
            raise AssetFetchError(f"{where}: period {q} has no records list")
        if not recs:
            if quarter is None and q == 1:
                raise NoDataError(f"{where}: game {sid} has no play-by-play (the site has none for 2009-10)")
            if quarter is not None:
                raise NoDataError(f"{where}: game {sid} has no plays in period {q}")
            raise AssetFetchError(f"{where}: period {q} is empty but period 1 is not (partial play-by-play)")
        if page.get("total") not in (None, 0, len(recs)):
            raise AssetFetchError(f"{where}: period {q} page is truncated ({len(recs)} of {page.get('total')} rows)")
        if any(r.get("quarterNum") != q for r in recs):
            raise AssetFetchError(f"{where}: out of scope: a row of period {q} echoes another period")
        clock = [(r.get("minutes"), r.get("seconds")) for r in recs]
        if any(not all(isinstance(x, int) for c in clock for x in c) for _ in (0,)) or clock != sorted(
            clock, reverse=True
        ):
            raise AssetFetchError(f"{where}: period {q} clock does not count down")
        rows.extend(recs)
    if quarter is None:
        home = max(r.get("homeScore") or 0 for r in rows)
        away = max(r.get("visitorScore") or 0 for r in rows)
        if (home, away) != (info["hostScore"], info["guestScore"]):
            raise AssetFetchError(
                f"{where}: the plays reach {home}-{away}, not the final {info['hostScore']}-{info['guestScore']}"
            )  # noqa: E501
    return {"records": rows, "total": len(rows)}


# ------------------------------------------------------------------------------ the getter
def _split(url: str) -> Tuple[str, Optional[str]]:
    parts = urlsplit(url)
    if parts.netloc != urlsplit(API).netloc or not parts.path.startswith("/api/"):
        raise ValueError(f"not a CBA route: {_rt._where(url)}")
    path = parts.path[len("/api/") :]
    for prefix in _PATH_ID:
        if path.startswith(prefix + "/"):
            return prefix, path[len(prefix) + 1 :]
    return path, None


def _get(url: str, params: Optional[dict] = None, json: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET or POST a CBA route, decrypt it, refuse every wrong answer, return the JSON with the request stamped.

    Args:
        url: the URL a generated wrapper built (``data-server.cbaleague.com/api/<route>``; a game id in the path for
            the GET game routes).
        params: wire keys for the query string (``pageNumber`` / ``pageSize`` / ``startNum`` on a POST route; every key
            on a GET route). ``season`` is the END year and is sent as the START year on the routes that take one.
            ``None`` values are dropped.
        json: the JSON body fields of a POST route (``scheduleId`` as a string); required on a POST route, as the
            generated wrappers send it.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The decrypted JSON. A game route that answers an object carries ``_request`` = ``{"schedule_id": id}``;
        play-by-play is merged over every period into ``{"records": [...], "total": n}``.

    Raises:
        NoDataError: see the module docstring (HTTP 404, no play-by-play).
        ValueError: a non-integer id or season, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer, including an unfinished game.
        TypeError: an unsupported keyword argument, or a POST route without ``json``.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a CBA request: {sorted(kwargs)}")
    route, path_id = _split(url)
    if route in _POST and json is None:
        raise TypeError(f"a CBA POST route takes its body as json=: {_rt._where(url)}")
    q = {k: v for k, v in {**(params or {}), **(json or {})}.items() if v is not None}
    qkeys = frozenset(k for k, v in (params or {}).items() if v is not None)
    if route in _SEASON_ROUTES and "season" in q:
        q["season"] = wire_season(q["season"])
    sid: Optional[int] = None
    if path_id is not None:
        sid = _int_arg(path_id, "schedule_id")
    elif route in _BODY_ID:
        if "scheduleId" not in q:
            raise ValueError("a CBA game route needs a schedule id")
        sid = _int_arg(q["scheduleId"], "schedule_id")
        q["scheduleId"] = str(sid)
    if route == "schedule-eventses/filterPage":
        quarter = _int_arg(q["quarterNum"], "quarter_num") if q.get("quarterNum") is not None else None
        out = _pbp(sid, quarter)  # type: ignore[arg-type]
    elif sid is not None:
        out = _checked(route, sid, q, path_id, qkeys)
    else:
        if route in _POST:
            query = {k: v for k, v in q.items() if k in qkeys}
            out = _fetch(route, query, {k: v for k, v in q.items() if k not in query})
        else:
            out = _fetch(route, q, None)
        _scope_list(route, out, q)
    if sid is not None and isinstance(out, dict):
        out[REQUEST_KEY] = {"schedule_id": sid}
    return out


def _scope_list(route: str, body: Any, q: Dict[str, Any]) -> None:
    """Season / team / day echo checks for the non-game routes."""
    where = f"{urlsplit(API).netloc}/api/{route}"
    rows: Any = body.get("records") if isinstance(body, dict) else body
    if route in _PLAYER_LISTS or route in ("team-match-datas/team-entirety-list", "team-match-datas/team-shoot-list"):
        if not isinstance(rows, list):
            raise AssetFetchError(f"{where}: no records list")
    if route in _PLAYER_LISTS:
        if any(r.get("season") != q.get("season") for r in rows):
            raise AssetFetchError(f"{where}: out of scope: a row echoes another season than {q.get('season')}")
        if q.get("teamId") is not None and _ids(rows, "teamId") - {q["teamId"]}:
            raise AssetFetchError(f"{where}: out of scope: a row is another team than {q['teamId']}")
    elif route in ("team-match-datas/team-entirety-list", "team-match-datas/team-shoot-list"):
        if _ids(rows, "teamId") - {q.get("teamId")}:
            raise AssetFetchError(f"{where}: out of scope: a row is another team than {q.get('teamId')}")
    elif route in ("teams/basic-data", "teams/four-elements"):
        if not isinstance(rows, list) or _ids(rows, "teamId") - {q.get("targetId")}:
            raise AssetFetchError(f"{where}: out of scope: a row is another team than {q.get('targetId')}")
    elif route == "schedules/day":
        if not isinstance(rows, list) or any(str(r.get("scheduleTime", ""))[:10] != str(q.get("day")) for r in rows):
            raise AssetFetchError(f"{where}: out of scope: a game is on another day than {q.get('day')} (auto mode?)")
