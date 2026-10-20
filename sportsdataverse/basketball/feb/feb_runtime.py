"""Runtime for the generated FEB wrappers (``intrafeb.feb.es`` JSON API + ``baloncestoenvivo.feb.es`` HTML).

Two hosts, one provider (the Spanish Basketball Federation). Measured against the live hosts
(sdv-internal-refs ``feb/``, 2026-10-12):

* **Bearer JWT.** Every LiveStats call needs ``Authorization: Bearer <JWT>``; without it the API
  answers 401 with an empty body. The token is public: the hidden input
  ``#contentToken > input`` of any ``baloncestoenvivo.feb.es`` page (this runtime reads the site
  root), an RS256 JWT of client ``baloncestoenvivoapp``, scope ``livestats.api``, valid 24 h.
  It is fetched at run time, held in memory only (one per process, renewed a minute before it
  expires, or once on a 401), and NEVER printed, logged, written or put in an error message: every
  message passes through :func:`_redact`, which replaces any JWT-shaped string.
* **Season key.** The schedule site keys a season by its START year ``t`` (``t=2026`` is
  2026-27). The public key is the END year; the schedule wrappers take ``season`` (2027 =
  2026-27) and this runtime sends ``t = season - 1``. The game routes are keyed by game id alone.
* **Silent wrong answers, all refused** (fail closed, type-strict compares):

  - a schedule page for another group or season (the site serves what it likes for a ``t`` it does
    not hold): the page's logo ``g`` / ``t``, selected season option and season label must equal
    the request;
  - an unknown group redirects to ``/error`` with HTTP 200: ``NoDataError``; any other final URL
    than ``/{calendario|resultados}/{g}/{t}`` is refused;
  - an envelope whose filled blocks are not the route's, or an unreadable one (a stub);
  - a play-by-play body whose lines name another game (every team line's ``logParam1`` embeds
    the game id).

* **Error vocabulary.** ``NoDataError``: HTTP 404 (an unknown game, or one outside the API's
  2020-21 onward coverage: ``application/problem+json``), an unknown group, a schedule page with no
  games, an empty ``/overview`` window. ``ValueError``: HTTP 400 / 422, a game id, group or season
  that is not an integer. ``AssetFetchError``: any other non-200 (429 / 5xx after three attempts,
  spaced by ``dl_utils.download`` and honouring ``Retry-After``), a dead connection, a bearer token
  the API still rejects after one renewal, a non-JSON or non-HTML body, an error envelope, a
  changed envelope, and a page for another scope or with a missing table or header drift.

Every attempt, retries included, goes through the shared ``basketball_runtime.SESSION`` and so waits
its turn (at least 1 s per host).
"""

from __future__ import annotations

import base64
import json
import re
import threading
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.basketball.feb.feb_parsers import _NotParsed, _page_record, _same
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["REQUEST_KEY", "token_from_page"]

SITE = "https://baloncestoenvivo.feb.es"
API = "https://intrafeb.feb.es/LiveStats.API/api/v1"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

JWT_RE = re.compile(r"eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}")
_REDACTED = "<redacted-jwt>"
_TOKEN_INPUT = re.compile(r'<div[^>]*id="contentToken"[^>]*>\s*<input[^>]*\bvalue="([^"]+)"')
#: A play's ``logParam1`` on team and player events: ``00000000-0000-000{side}-0000-{game id, 12 digits}``.
_GAME_GUID = re.compile(r"^00000000-0000-000([12])-0000-(\d{12})$")

#: Every API body is this envelope; each route fills only its own blocks.
_ENVELOPE = (
    "HEADER",
    "SCOREBOARD",
    "TEAMSTATS",
    "TICKER",
    "OVERVIEW",
    "PLAYBYPLAY",
    "BANNER",
    "BOXSCORE",
    "SHOTCHART",
    "RANKING",
)
#: route path -> (kind, filled blocks, the block the route exists for)
_API_ROUTES: Dict[str, Tuple[str, Tuple[str, ...], str]] = {
    "BoxScore": ("boxscore", ("HEADER", "TICKER", "BOXSCORE"), "BOXSCORE"),
    "KeyFacts": (
        "keyfacts",
        ("HEADER", "SCOREBOARD", "TEAMSTATS", "TICKER", "PLAYBYPLAY", "BANNER"),
        "PLAYBYPLAY",
    ),
    "ShotChart": ("shotchart", ("HEADER", "TICKER", "SHOTCHART"), "SHOTCHART"),
    "TeamStats": ("teamstats", ("HEADER", "TICKER", "TEAMSTATS"), "TEAMSTATS"),
    "Ranking": ("ranking", ("HEADER", "TICKER", "RANKING"), "RANKING"),
    "overview": ("overview", ("OVERVIEW",), "OVERVIEW"),
}
_PAGES = {"/calendario.aspx": ("calendar", "calendario"), "/resultados.aspx": ("results", "resultados")}
_ERROR_KEYS = frozenset({"type", "title", "status", "traceId", "detail", "errors", "error", "message"})

_tok: Dict[str, Any] = {"value": None, "exp": 0.0}
_tok_lock = threading.Lock()


def _redact(text: str) -> str:
    """``text`` with every JWT-shaped string replaced by ``<redacted-jwt>``."""
    return JWT_RE.sub(_REDACTED, text)


def _fail(msg: str) -> AssetFetchError:
    return AssetFetchError(_redact(msg))


# ----------------------------------------------------------------------------- transport
def _fetch(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries 429 / 5xx up to :data:`ATTEMPTS` times with a ``Retry-After``-aware
    delay and raises ``NoDataError`` on a 404. A connection failure that outlives the retries becomes
    an ``AssetFetchError`` naming only the exception class (never the URL's query or any header).
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
    raise _fail(f"{where} answered HTTP {status}, not 200")


def _text(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """The HTML response of ``url``: HTTP 200, an HTML content type and a non-empty body."""
    resp = _fetch(url, params)
    _check_status(url, resp)
    ctype = (getattr(resp, "headers", None) or {}).get("content-type", "") or ""
    if "html" not in ctype.lower():
        raise _fail(f"{_rt._where(url)} answered a non-HTML body ({ctype or 'no content type'})")
    if not (getattr(resp, "text", "") or "").strip():
        raise _fail(f"{_rt._where(url)} answered HTTP 200 with an empty body")
    return resp


# ------------------------------------------------------------------------------- bearer
def _b64json(part: str) -> Any:
    return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))


def token_from_page(html: str, now: Optional[float] = None) -> Tuple[str, float]:
    """The bearer JWT in a site page's ``#contentToken > input`` and its expiry (epoch seconds).

    Args:
        html: the page text.
        now: the clock to test expiry against (defaults to ``time.time()``).

    Returns:
        ``(token, exp)``.

    Raises:
        AssetFetchError: no token input, a value that is not a JWT, a JWT that is not a
            ``baloncestoenvivoapp`` ``livestats.api`` token, or one that expires within a minute.
            The message never carries the token.
    """
    m = _TOKEN_INPUT.search(html or "")
    if not m:
        raise _fail("the FEB site page carries no #contentToken > input")
    tok = m.group(1)
    if not JWT_RE.fullmatch(tok):
        raise _fail("the FEB site token is not a JWT")
    try:
        claims = _b64json(tok.split(".")[1])
    except Exception:  # noqa: BLE001 - any decode failure is the same refusal
        raise _fail("the FEB site token has an unreadable payload") from None
    if not isinstance(claims, dict):
        raise _fail("the FEB site token payload is not an object")
    scope = claims.get("scope")
    if not _same(claims.get("client_id"), "baloncestoenvivoapp") or not (
        isinstance(scope, list) and "livestats.api" in scope
    ):
        raise _fail("the FEB site token is not a baloncestoenvivoapp livestats.api token")
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)) or isinstance(exp, bool) or exp <= (time.time() if now is None else now) + 60:
        raise _fail("the FEB site token is expired or has no exp")
    return tok, float(exp)


def _bearer(*, renew: bool = False) -> str:
    """The cached bearer token, fetched from the site root when absent, near expiry or ``renew``."""
    with _tok_lock:
        if renew or _tok["value"] is None or _tok["exp"] <= time.time() + 60:
            html = _text(SITE + "/").text
            _tok["value"], _tok["exp"] = token_from_page(html)
        return str(_tok["value"])


# ---------------------------------------------------------------------------- API routes
def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be an integer; got {value!r}")
    return int(value)


def _error_envelope(body: Dict[str, Any]) -> bool:
    return bool(body) and set(body) <= _ERROR_KEYS


def _validate(kind: str, filled: Tuple[str, ...], main: str, body: Any, game_id: Optional[int], where: str) -> None:
    """Refuse everything that is not this route's envelope. Raises ``AssetFetchError`` / ``NoDataError``."""
    if not isinstance(body, dict):
        raise _fail(f"{where}: the body is not an object")
    if _error_envelope(body):
        raise _fail(f"{where}: the body is an error envelope")
    if set(body) != set(_ENVELOPE):
        raise _fail(f"{where}: not the LiveStats envelope (keys differ)")
    got = tuple(k for k in _ENVELOPE if body.get(k) is not None)
    want = tuple(k for k in _ENVELOPE if k in filled)
    if got != want:
        raise _fail(f"{where}: filled blocks {got} are not the route's {want}")
    block = body[main]
    if not isinstance(block, dict) or not block:
        raise _fail(f"{where}: the {main} block is empty")
    if kind == "overview":
        comps = block.get("COMPETITIONS")
        if not isinstance(comps, list) or not comps:
            raise NoDataError(f"{where}: no competitions have games in the window")
        for c in comps:
            games = c.get("GAMES") if isinstance(c, dict) else None
            if not isinstance(games, list) or any(
                not isinstance(g, dict) or not _same(g.get("CompID"), c.get("id")) for g in games
            ):
                raise _fail(f"{where}: a game lies outside its competition")
        return
    head = body["HEADER"]
    teams = head.get("TEAM") if isinstance(head, dict) else None
    if not isinstance(teams, list) or len(teams) != 2 or not all(isinstance(t, dict) and t.get("id") for t in teams):
        raise _fail(f"{where}: HEADER without two teams")
    if kind == "keyfacts":
        lines = block.get("LINES")
        if not isinstance(lines, list) or not lines:
            raise _fail(f"{where}: PLAYBYPLAY without a LINES list")
        want_id = f"{game_id:012d}" if game_id is not None else None
        for ln in lines:
            m = _GAME_GUID.match(str(ln.get("logParam1") or "")) if isinstance(ln, dict) else None
            if m and m.group(2) != want_id:
                raise _fail(f"{where}: out of scope: a play of game {int(m.group(2))}, asked for {game_id}")


def _api(path: str, name: str, game_id: Optional[int]) -> Dict[str, Any]:
    kind, filled, main = _API_ROUTES[name]
    url = API + path
    where = _rt._where(url)
    resp: Any = None
    for renew in (False, True):  # one renewal when the API rejects the token
        resp = _fetch(url, headers={"Authorization": f"Bearer {_bearer(renew=renew)}", "Accept": "application/json"})
        if getattr(resp, "status_code", None) != 401:
            break
    if getattr(resp, "status_code", None) == 401:
        raise _fail(f"{where} rejected the bearer token (HTTP 401) after a renewal")
    _check_status(url, resp)
    ctype = ((getattr(resp, "headers", None) or {}).get("content-type", "") or "").lower()
    if "json" not in ctype or "problem+json" in ctype:
        raise _fail(f"{where} answered {ctype or 'no content type'}, not JSON")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise _fail(f"{where} answered HTTP 200 with an empty body")
    try:
        body = json.loads(text)
    except ValueError:
        raise _fail(f"{where} answered a body that is not JSON") from None
    _validate(kind, filled, main, body, game_id, where)
    body[REQUEST_KEY] = {} if game_id is None else {"game_id": game_id}
    return body


# -------------------------------------------------------------------------- schedule pages
def _schedule(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    kind, segment = _PAGES[path]
    if params.get("group") is None or params.get("season") is None:
        raise ValueError("the FEB schedule pages need group (the site's menu group) and season (END year)")
    g, season = _int_arg(params["group"], "group"), _int_arg(params["season"], "season")
    t = season - 1  # the site keys a season by its START year
    url = SITE + path
    resp = _text(url, {"g": g, "t": t})
    final = urlsplit(str(getattr(resp, "url", "") or ""))
    want = f"/{segment}/{g}/{t}"
    if final.path.rstrip("/") == "/error":
        raise NoDataError(f"{_rt._where(url)}: the site has no group {g} (redirected to /error)")
    if final.netloc != urlsplit(SITE).netloc or final.path.rstrip("/") != want:
        raise _fail(f"{_rt._where(url)}: final URL {_rt._where(str(final.geturl()))} is not {want}")
    try:
        rec = _page_record(kind, resp.text, g, t)
    except _NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{_rt._where(url)}: {exc}") from None
        raise _fail(f"{_rt._where(url)}: not the page asked for: {exc}") from None
    rec[REQUEST_KEY] = {"group": g, "season": season, "season_start": t}
    return rec


# ------------------------------------------------------------------------------ the getter
@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a FEB route, refuse every wrong answer, return the envelope or page record with the request stamped.

    Args:
        url: the URL a generated wrapper built (``intrafeb.feb.es/LiveStats.API/api/v1/{route}[/{gameId}]``
            or ``baloncestoenvivo.feb.es/{calendario|resultados}.aspx``).
        params: query parameters; only the schedule pages use them: ``group`` (the site's menu group) and
            ``season`` (END year, mapped to the site's START-year ``t``). ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The parsed JSON envelope (all ten blocks) or the schedule page record (the refs capture shape), with
        ``_request`` = ``{"game_id"}`` or ``{"group", "season", "season_start"}``.

    Raises:
        NoDataError: see the module docstring (HTTP 404, an unknown group, an empty page or window).
        ValueError: a game id, group or season that is not an integer, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a FEB request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    parts = urlsplit(url)
    if parts.netloc == urlsplit(SITE).netloc and parts.path in _PAGES:
        return _schedule(parts.path, q)
    if parts.netloc == urlsplit(API).netloc and parts.path.startswith(urlsplit(API).path + "/"):
        rest = parts.path[len(urlsplit(API).path) + 1 :].strip("/").split("/")
        if rest[0] in _API_ROUTES and rest[0] != "overview" and len(rest) == 2:
            gid = _int_arg(rest[1], "game id")
            return _api(f"/{rest[0]}/{gid}", rest[0], gid)
        if rest == ["overview"]:
            return _api("/overview", "overview", None)
    raise ValueError(f"not a FEB route: {_rt._where(url)}")
