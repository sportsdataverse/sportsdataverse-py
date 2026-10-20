"""Runtime for the generated ABA wrappers (``www.aba-liga.com`` and ``druga.aba-liga.com``).

The ABA League (``lea=1``) and ABA League 2 (``lea=2``) run on one PHP site over one database; either host
serves either league, and this runtime asks the league's own (www for ABA League, druga for ABA League 2).
Seven routes: four server-rendered pages (match, calendar, standings, player) and three JSON helpers served as
``text/html`` (shot chart and two minute-by-minute score charts). Measured against the live sites
(sdv-internal-refs ``aba-liga/``, 2026-10-12):

* **Season key.** The site's ``sez`` is the START year minus 2000 (``25`` = 2025-26). The public key is the END
  year, so ``sez = season - 2001`` (``season=2026`` -> ``sez=25``; the first season, 2001-02, is ``season=2002``).
  The wrappers take ``season`` and ``league`` (``"aba"`` -> ``lea=1``, ``"aba2"`` -> ``lea=2``); this runtime builds
  the site's URL (``/match/{gameNo}/{sez}/{lea}/Overview``).
* **Silent wrong answers, all refused** (fail closed, type-strict compares):

  - a match page for another game, season or league than asked (the site serves what it likes for a wrong
    ``sez`` / ``lea`` with an existing game number): the hidden-input echo ``match_data_id`` / ``_sez`` / ``_lea``
    must equal the request, and every team and player link on the page must carry the requested ``sez`` / ``lea``;
  - a calendar, standings or player page whose selected season option is not the request;
  - a game that is not finished (an unplayed game prints `` : ``; the period scores, team totals and the
    play-by-play must add up to the final score);
  - the three JSON helpers echo nothing about the game, so the runtime fetches the SAME game's match page first
    (one more request) and checks the helper against it: minutes 0..40 (+5 per overtime) and the last row equal to
    the final score; the shot chart is tallied per shooter against the box score. The 40-row zero placeholder the
    score chart answers for an unplayed or unknown game is refused.

* **Error vocabulary.** ``NoDataError``: HTTP 404, an unknown game (the site redirects to ``/match-not-defined/``
  with HTTP 200), a season the site does not hold (no selected season option and no games), a helper that answers
  ``[]`` or blank (2016-17 and earlier have no shot chart or minute chart), a season before 2001-02.
  ``ValueError``: HTTP 400 / 422, an argument that is not an integer, a ``league`` other than ``aba`` / ``aba2``.
  ``AssetFetchError``: any other non-200 (429 / 5xx after three attempts, spaced by ``dl_utils.download`` and
  honouring ``Retry-After``), a dead connection, a non-HTML page or non-JSON helper, a page for another scope, an
  unfinished game, a redirect to anywhere else, a helper that does not belong to the game, and layout drift.

Every attempt, retries included, goes through the shared ``basketball_runtime.SESSION`` and so waits its turn (at
least 1 s per host). ``www.aba-liga.com/robots.txt`` is the single line ``User-agent: *`` (no ``Crawl-delay``, nothing
disallowed) and ``druga.aba-liga.com`` has no robots.txt, so the shared 1 s spacing is the only delay required.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball.aba import aba_parsers as P
from sportsdataverse.basketball.basketball_runtime import SESSION
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["HOSTS", "LEAGUE_IDS", "REQUEST_KEY", "sez_for_season"]

#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
#: The league's own host (either host serves either league).
HOSTS = {"aba": "https://www.aba-liga.com", "aba2": "https://druga.aba-liga.com"}
#: ``league`` argument -> the site's ``lea``.
LEAGUE_IDS = {"aba": 1, "aba2": 2}
ATTEMPTS = 3
TIMEOUT = 60
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
LIVE = "/live-match/rezultati-1718"
_HELPERS = {
    f"{LIVE}/create_shooting_chart.php": "shots",
    f"{LIVE}/create_json.php": "flow",
    f"{LIVE}/create_scoredifference.php": "scorediff",
}
_ROUTES = (
    ("match", re.compile(r"^/match/(?P<game_no>\d+)/(?P<season>\d+)/Overview$")),
    ("calendar", re.compile(r"^/calendar/(?P<season>\d+)/?$")),
    ("standings", re.compile(r"^/standings/(?P<season>\d+)/?$")),
    ("player", re.compile(r"^/player/(?P<player_id>\d+)/(?P<season>\d+)/?$")),
)


def sez_for_season(season: int) -> int:
    """The site's ``sez`` for a season END year (2026 -> 25).

    Args:
        season: the season END year (2026 = 2025-26).

    Returns:
        ``season - 2001``.

    Raises:
        NoDataError: ``season`` is before 2002 (the first ABA League season is 2001-02).
        ValueError: ``season`` is not an integer, or the result does not fit the site's two-digit key.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba.aba_runtime import sez_for_season

            sez_for_season(2026)  # 25
    """
    if isinstance(season, bool) or not isinstance(season, (int, str)) or not str(season).strip().isdigit():
        raise ValueError(f"season must be an integer END year; got {season!r}")
    sez = int(season) - 2001
    if sez < 1:
        raise NoDataError(f"the ABA League's first season is 2001-02 (season=2002); got season={season}")
    if sez > 99:
        raise ValueError(f"season {season} does not fit the site's two-digit season key")
    return sez


# ----------------------------------------------------------------------------- transport
def _fail(msg: str) -> AssetFetchError:
    return AssetFetchError(msg)


def _fetch(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """GET through the throttled session; every attempt waits its turn. Returns the final response.

    ``dl_utils.download`` retries 429 / 5xx up to :data:`ATTEMPTS` times with a ``Retry-After``-aware delay and
    raises ``NoDataError`` on a 404. A connection failure that outlives the retries becomes an ``AssetFetchError``
    naming only the exception class.
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


def _body(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    """(final path, text) of ``url``: HTTP 200 on one of the two ABA hosts with a non-empty body."""
    resp = _fetch(url, params)
    _check_status(url, resp)
    final = urlsplit(str(getattr(resp, "url", "") or url))
    if final.netloc not in {urlsplit(h).netloc for h in HOSTS.values()}:
        raise _fail(f"{_rt._where(url)}: redirected off the ABA sites to {final.netloc or 'an unknown host'}")
    return final.path, getattr(resp, "text", "") or ""


def _page(kind: str, url: str, want: Dict[str, int], final_ok: "re.Pattern[str]") -> Dict[str, Any]:
    """Fetch one HTML page and return its scoped record. Raises as the module docstring says."""
    path, text = _body(url)
    where = _rt._where(url)
    if "match-not-defined" in path:
        raise NoDataError(f"{where}: the site has no such game (redirected to {path})")
    if not final_ok.match(path):
        raise _fail(f"{where}: final path {path!r} is not the page asked for")
    if not text.strip():
        raise _fail(f"{where} answered HTTP 200 with an empty body")
    try:
        return P.record_for(kind, text, want)
    except P._NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{where}: {exc}") from None
        raise _fail(f"{where}: not the page asked for: {exc}") from None


def _helper(kind: str, url: str, game_no: int, sez: int, lea: int, match: Dict[str, Any]) -> Any:
    path, text = _body(url, {"id": game_no, "sez": sez, "lea": lea})
    where = _rt._where(url)
    if not text.strip():
        raise NoDataError(f"{where}: the helper answered a blank body (no data before 2017-18)")
    try:
        body = json.loads(text)
    except ValueError:
        raise _fail(f"{where} answered a body that is not JSON") from None
    if body == []:
        raise NoDataError(f"{where}: the helper answered [] (no data before 2017-18)")
    why = P.helper_scope_error(kind, body, match)
    if why:
        raise _fail(f"{where}: {why}")
    return body


def _int_arg(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"{what} must be an integer; got {value!r}")
    return int(value)


# ------------------------------------------------------------------------------ the getter
@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET an ABA route, refuse every wrong answer, return the page record or helper rows with the request stamped.

    Args:
        url: the URL a generated wrapper built: ``/match/{game_no}/{season}/Overview``, ``/calendar/{season}/``,
            ``/standings/{season}/``, ``/player/{player_id}/{season}/`` or a ``/live-match/rezultati-1718/create_*.php``
            helper, on ``www.aba-liga.com``. The season segment is the END year; the helpers take ``game_no`` (sent
            as ``id``) and ``season`` as query parameters.
        params: query parameters; ``league`` (``aba`` / ``aba2``, default ``aba``) picks the league and the host.
            ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The page record (the refs capture shape) for a page route, or ``{"rows": [...]}`` for a helper, with
        ``_request`` = ``{"kind", "season", "sez", "league", "league_id", "game_no" | "player_id"}``.

    Raises:
        NoDataError: see the module docstring (HTTP 404, an unknown game or season, a helper with no data).
        ValueError: an argument that is not an integer, an unknown ``league``, or HTTP 400 / 422.
        AssetFetchError: any other failed or wrong answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for an ABA request: {sorted(kwargs)}")
    q = {k: v for k, v in (params or {}).items() if v is not None}
    league = q.pop("league", "aba")
    if league not in LEAGUE_IDS:
        raise ValueError(f"league must be one of {sorted(LEAGUE_IDS)}; got {league!r}")
    lea, host = LEAGUE_IDS[league], HOSTS[league]
    path = urlsplit(url).path
    stamp: Dict[str, Any] = {"league": league, "league_id": lea}

    if path in _HELPERS:
        kind = _HELPERS[path]
        game_no, season = _int_arg(q.get("game_no", q.get("id")), "game_no"), q.get("season")
        sez = sez_for_season(season)  # type: ignore[arg-type]
        stamp.update(kind=kind, game_no=game_no, season=int(season), sez=sez)  # type: ignore[arg-type]
        murl = f"{host}/match/{game_no}/{sez}/{lea}/Overview"
        match = _page(
            "match",
            murl,
            {"game_no": game_no, "sez": sez, "lea": lea},
            re.compile(rf"^/match/{game_no}/{sez}/{lea}/Overview/?$"),
        )
        return {"rows": _helper(kind, host + path, game_no, sez, lea, match), REQUEST_KEY: stamp}

    for kind, pattern in _ROUTES:
        m = pattern.match(path)
        if not m:
            continue
        g = m.groupdict()
        season = _int_arg(g["season"], "season")
        sez = sez_for_season(season)
        stamp.update(kind=kind, season=season, sez=sez)
        want = {"sez": sez, "lea": lea}
        if kind == "match":
            gno = _int_arg(g["game_no"], "game_no")
            stamp["game_no"] = gno
            want["game_no"] = gno
            rec = _page(
                kind,
                f"{host}/match/{gno}/{sez}/{lea}/Overview",
                want,
                re.compile(rf"^/match/{gno}/{sez}/{lea}/Overview/?$"),
            )
        elif kind == "player":
            pid = _int_arg(g["player_id"], "player_id")
            stamp["player_id"] = pid
            want["player_id"] = pid
            rec = _page(
                kind,
                f"{host}/player/{pid}/{sez}/{lea}/",
                want,
                re.compile(rf"^/player/{pid}/{sez}/{lea}/(?:[\w-]+/)?$"),
            )
        else:
            rec = _page(kind, f"{host}/{kind}/{sez}/{lea}/", want, re.compile(rf"^/{kind}/{sez}/{lea}/?$"))
        rec[REQUEST_KEY] = stamp
        return rec
    raise ValueError(f"not an ABA route: {_rt._where(url)}")
