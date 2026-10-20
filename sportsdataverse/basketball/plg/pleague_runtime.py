"""Runtime for the generated ``pleague`` wrappers (P. LEAGUE+, Taiwan: ``pleagueofficial.com``).

Eleven frames over ten routes: five server-rendered pages (schedule per stage, game, standings, the player season
table, the player page) and the two PHP routes the game page calls (the box score, served as ``text/html`` JSON, and
the play-by-play, JSON whose ``data`` is an HTML table). Measured against the live site (sdv-internal-refs
``pleague/``, 2026-10-12 and 2026-10-13):

* **Transport.** The site sits behind Cloudflare, which answers plain ``requests`` with an HTTP 403 page, so pages are
  fetched with ``curl_cffi`` (``impersonate="chrome"``; an optional dependency, ``pip install curl_cffi`` or
  ``sportsdataverse[all]``). The shared ``basketball_runtime.SESSION`` is a ``requests`` session, so this runtime waits
  its turn itself: **every attempt, retries included, calls** ``_polite_wait`` (at least 1 s per host). 429 / 502 /
  503 / 504 are retried (three attempts, ``Retry-After`` honoured on 429 / 503); 500 is **not** (the site answers 500
  for an unknown id). ``robots.txt`` is only Cloudflare content-signal comment lines: no ``Crawl-delay``, nothing
  disallowed.
* **Season key.** The site names seasons by slug (``"2024-25"``). The public key is the END year: ``season=2025`` ->
  ``2024-25``. The first season is 2020-21 (``season=2021``); earlier slugs redirect.
* **Game key.** ``gid`` is one global integer sequence over every stage and season. The box and play-by-play routes
  echo no id, so this runtime fetches the game page first (cached per ``gid``: a finished game does not change) and
  checks the body against it (scores, every quarter, the players' points; the play-by-play's periods, final score and
  last scoring event).
* **Refused, all fail closed** (``AssetFetchError``): a page for another game, season, stage or player than asked; a
  game whose status badge is not ``已完賽`` (only finished games are served); a box or play-by-play that does not
  match its game page; a play-by-play phrase outside the observed vocabulary (layout drift); a body that is not the
  expected shape; an HTTP 5xx after the retries; a dead connection; a vendor access key surviving in a record.
* **Error vocabulary.** ``NoDataError``: HTTP 404, a redirect (the site answers an unknown ``gid`` or a season before
  2020-21 with a 302 to the home page), a stage the season does not have (a page with zero games), a play-by-play
  request for a game with no uuid (2020-21 and 2021-22 have a box but no play-by-play). ``ValueError``: HTTP 400 / 422
  or an argument that is not valid. ``AssetFetchError``: every other failed or wrong answer.
"""

from __future__ import annotations

import copy
import json
import re
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.basketball_runtime import _polite_wait
from sportsdataverse.basketball.plg import pleague_parsers as P
from sportsdataverse.basketball.plg.pleague_preciser_parsers import has_secret
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["HOST", "REQUEST_KEY", "clear_cache", "season_slug"]

#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
HOST = "https://pleagueofficial.com"
#: The first season the site serves (2020-21); earlier slugs redirect to the schedule index.
FIRST_SEASON = 2021
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 502, 503, 504})  # 500 is what the site answers for an unknown id: not retried
MAX_RETRY_AFTER = 60.0
_REDIRECTS = frozenset({301, 302, 303, 307, 308})
_PAGE_ID = re.compile(r"^[A-Za-z0-9]{4,16}$")

_GAMES: Dict[int, Dict[str, Any]] = {}
_BOXES: Dict[int, Dict[str, Any]] = {}


def clear_cache() -> None:
    """Forget the per-``gid`` game page and box score records this runtime keeps (finished games are immutable)."""
    _GAMES.clear()
    _BOXES.clear()


def season_slug(season: Any) -> str:
    """The site's slug for a season END year (2025 -> ``'2024-25'``).

    Args:
        season: the season END year.

    Returns:
        ``f"{season - 1}-{season % 100:02d}"``.

    Raises:
        NoDataError: ``season`` is before 2021 (the site's first season is 2020-21).
        ValueError: ``season`` is not an integer.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg.pleague_runtime import season_slug

            season_slug(2025)  # '2024-25'
    """
    n = _int_arg(season, "season")
    if n < FIRST_SEASON:
        raise NoDataError(f"the site's first season is 2020-21 (season={FIRST_SEASON}); got season={season}")
    return f"{n - 1}-{n % 100:02d}"


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be a non-negative integer; got {value!r}")
    return int(value)


# ----------------------------------------------------------------------------- transport
def _transport(url: str, params: Optional[Dict[str, Any]]) -> Any:
    """GET ``url`` with Chrome TLS (Cloudflare answers plain ``requests`` with a 403), redirects not followed."""
    try:
        from curl_cffi import requests as creq
    except ImportError as exc:  # pragma: no cover - exercised only on the live path
        raise ImportError(
            "pleagueofficial.com sits behind Cloudflare and needs curl_cffi. Install with: pip install curl_cffi "
            "or pip install sportsdataverse[all]"
        ) from exc
    return creq.get(url, params=params or None, impersonate="chrome", timeout=TIMEOUT, allow_redirects=False)


def _retry_after(resp: Any) -> float:
    try:
        return min(float((getattr(resp, "headers", None) or {}).get("retry-after", 0)), MAX_RETRY_AFTER)
    except (TypeError, ValueError):  # an HTTP-date falls back to the exponential delay
        return 0.0


def _fetch(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """The final response of a request; EVERY attempt waits its turn, 429 / 502 / 503 / 504 retry.

    ``Retry-After`` is honoured on 429 and 503. A connection failure that outlives the attempts is an
    ``AssetFetchError`` naming only the exception class.
    """
    for attempt in range(ATTEMPTS):
        _polite_wait(url)
        delay = 2.0 * 2**attempt
        try:
            resp = _transport(url, params)
        except OSError as exc:  # curl_cffi's RequestException subclasses OSError
            if attempt == ATTEMPTS - 1:
                raise AssetFetchError(
                    f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}"
                ) from None
        else:
            status = getattr(resp, "status_code", None)
            _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
            if status not in RETRY_STATUSES or attempt == ATTEMPTS - 1:
                return resp
            if status in (429, 503):
                delay = max(delay, _retry_after(resp))
        time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def _ok(url: str, resp: Any) -> str:
    """The body text of an HTTP 200 response; every other status follows the error vocabulary."""
    status = getattr(resp, "status_code", None)
    _rt._check_no_content(status)  # a 204 / 205 is {} (EX-SYNC-a)
    where = _rt._where(url)
    if status in _REDIRECTS:
        loc = urlsplit(str((getattr(resp, "headers", None) or {}).get("location", ""))).path or "/"
        raise NoDataError(f"{where}: the site redirected to {loc} (it has no such page)")
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(f"{where} rejected the request: HTTP {status}")
    if status != 200:
        raise AssetFetchError(f"{where} answered HTTP {status}, not 200")
    text = getattr(resp, "text", "") or ""
    if not text.strip():
        raise AssetFetchError(f"{where} answered HTTP 200 with an empty body")
    return text


def _page(url: str, kind: str, **want: Any) -> Dict[str, Any]:
    """Fetch one HTML page and return its scoped record."""
    resp = _fetch(url)
    text = _ok(url, resp)
    where = _rt._where(url)
    if "html" not in str((getattr(resp, "headers", None) or {}).get("content-type", "")).lower():
        raise AssetFetchError(f"{where} answered a body that is not HTML")
    try:
        return P.record_for(kind, text, **want)
    except P._NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{where}: {exc}") from None
        raise AssetFetchError(f"{where}: not the page asked for: {exc}") from None


def _json(url: str, params: Optional[Dict[str, Any]]) -> Any:
    text = _ok(url, _fetch(url, params))
    try:
        return json.loads(text)
    except ValueError:
        raise AssetFetchError(f"{_rt._where(url)} answered a body that is not JSON") from None


# ----------------------------------------------------------------------------- the game context
@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-1)
def game_record(gid: int) -> Dict[str, Any]:
    """The verified game page record of a FINISHED game (cached per ``gid``).

    Raises:
        NoDataError: the site has no such game.
        AssetFetchError: the page is another game's, or the game is not final, or the layout drifted.
    """
    if gid not in _GAMES:
        rec = _page(f"{HOST}/game/{gid}", "game", gid=gid)
        why = P.game_error(rec, gid)
        if why:
            raise AssetFetchError(f"{_rt._where(HOST + '/game/' + str(gid))}: {why}")
        _GAMES[gid] = rec
    return copy.deepcopy(_GAMES[gid])


@_rt._no_content_is_empty  # a data body: its 204 / 205 is {}
def box_data(gid: int) -> Dict[str, Any]:
    """The box body of a finished game, checked against its game page (cached per ``gid``).

    Raises:
        AssetFetchError: the body is not the final box of the game (the route echoes no id).
    """
    if gid not in _BOXES:
        game = game_record(gid)
        url = f"{HOST}/api/boxscore.preciser.php"
        body = _json(url, {"id": gid, "away_tab": "total", "home_tab": "total"})
        why = P.box_error(body, game)
        if why:
            raise AssetFetchError(f"{_rt._where(url)}: not the box of game {gid}: {why}")
        _BOXES[gid] = body
    return copy.deepcopy(_BOXES[gid])


def _pbp(gid: int) -> Dict[str, Any]:
    game = game_record(gid)
    uuid = game.get("uuid")
    if not uuid:
        raise NoDataError(
            f"game {gid} has no play-by-play: the page embeds an empty uuid (2020-21 and 2021-22 have a box only)"
        )
    url = f"{HOST}/api/playbyplay.php"
    body = _json(url, {"uuid": uuid})
    where = _rt._where(url)
    if not isinstance(body, dict) or body.get("error") not in ("", None):
        raise AssetFetchError(f"{where}: error field {body.get('error') if isinstance(body, dict) else body!r}")
    try:
        rec = P.pbp_record(body)
    except P._NotParsed as exc:
        raise AssetFetchError(f"{where}: not a play-by-play I can read: {exc}") from None
    why = P.pbp_error(rec, game)
    if why:
        raise AssetFetchError(f"{where}: not the play-by-play of game {gid}: {why}")
    if has_secret(rec):
        raise AssetFetchError(f"{where}: a vendor access key survived in the record")
    rec[REQUEST_KEY] = {"kind": "pbp", "gid": gid, "uuid": uuid}
    return rec


# ------------------------------------------------------------------------------ the getter
def _stage(q: Dict[str, Any]) -> str:
    stage = q.pop("stage", "regular-season")
    if stage not in P.STAGES:
        raise ValueError(f"stage must be one of {sorted(P.STAGES)}; got {stage!r}")
    return stage


def _split(path: str) -> Tuple[str, ...]:
    return tuple(s for s in path.split("/") if s)


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a P. LEAGUE+ route, refuse every wrong answer, return the record with the request stamped.

    Args:
        url: the URL a generated wrapper built: ``/schedule/{season}``, ``/game/{gid}``, ``/standings/{season}``,
            ``/stat-player/{season}/{stage_sn}``, ``/player/{player_page_id}``, ``/api/boxscore.preciser.php`` or
            ``/api/playbyplay.php`` on ``pleagueofficial.com``. ``season`` segments are END years.
        params: query parameters. ``stage`` (schedule: ``pre-season`` / ``regular-season`` / ``playoffs`` /
            ``finals``) and ``gid`` (box and play-by-play: the game to read; the play-by-play's uuid is read from the
            game page). ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The record (the refs capture shape; the box as the site sends it) with ``_request`` =
        ``{"kind", "season", "season_slug", "gid" | "stage" | "stage_sn" | "player_page_id", ...}``.

    Raises:
        NoDataError: see the module docstring (HTTP 404, a redirect, an empty stage, no play-by-play).
        ValueError: an invalid argument or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer.
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a P. LEAGUE+ request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    seg = _split(urlsplit(url).path)

    if len(seg) == 2 and seg[0] == "schedule":
        stage = _stage(q)
        slug = season_slug(seg[1])
        rec = _page(f"{HOST}/schedule-{stage}/{slug}", "schedule", stage=stage, season=slug)
        rec[REQUEST_KEY] = {"kind": "schedule", "season": int(seg[1]), "season_slug": slug, "stage": stage}
        return rec
    if len(seg) == 2 and seg[0] == "game":
        gid = _int_arg(seg[1], "gid")
        rec = game_record(gid)
        rec[REQUEST_KEY] = {"kind": "game", "gid": gid}
        return rec
    if len(seg) == 2 and seg[0] == "standings":
        slug = season_slug(seg[1])
        rec = _page(f"{HOST}/standings/{slug}", "standings", season=slug)
        rec[REQUEST_KEY] = {"kind": "standings", "season": int(seg[1]), "season_slug": slug}
        return rec
    if len(seg) == 3 and seg[0] == "stat-player":
        slug = season_slug(seg[1])
        sn = _int_arg(seg[2], "stage_sn")
        if sn not in P.STAT_STAGES:
            raise ValueError(
                f"stage_sn must be one of {sorted(P.STAT_STAGES)} (2 regular season, 3 playoffs, 4 finals)"
            )
        rec = _page(f"{HOST}/stat-player/{slug}/{sn}", "stat_player", season=slug, stage_sn=sn)
        rec[REQUEST_KEY] = {"kind": "stat_player", "season": int(seg[1]), "season_slug": slug, "stage_sn": sn}
        return rec
    if len(seg) == 2 and seg[0] == "player":
        pid = seg[1]
        if not _PAGE_ID.match(pid):
            raise ValueError(f"player_page_id must be the 8-character page id of /player/{{id}}; got {pid!r}")
        rec = _page(f"{HOST}/player/{pid}", "player", pid=pid)
        rec[REQUEST_KEY] = {"kind": "player", "player_page_id": pid}
        return rec
    if seg == ("api", "boxscore.preciser.php"):
        gid = _int_arg(q.get("gid", q.get("id")), "gid")
        body = box_data(gid)
        body[REQUEST_KEY] = {"kind": "box", "gid": gid}
        return body
    if seg == ("api", "playbyplay.php"):
        return _pbp(_int_arg(q.get("gid"), "gid"))
    raise ValueError(f"not a P. LEAGUE+ route: {_rt._where(url)}")
