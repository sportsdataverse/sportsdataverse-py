"""Runtime for the generated VHL wrappers (``www.vhlru.ru`` + ``online.vhlru.ru``, keyless HTML).

The VHL has no JSON API, so this getter fetches the server-rendered page, refuses every page
that is not the one asked for, parses it into a record with
:func:`sportsdataverse.hockey.vhl.vhl_parsers._page_record` and returns that record (a dict,
the shape of the refs captures) with the request stamped under ``_request``. The parsers turn
the record (or the page's HTML) into frames. Measured against the live site (sdv-internal-refs
``vhl/``, 2026-10-12):

* **IP pin.** ``www.vhlru.ru`` has two A records and ``185.210.57.37`` stalls after about
  16 KB (the calendar and stats pages are 4-6 MB). Requests to that host only are pinned to
  ``188.128.49.29`` with curl's RESOLVE option (:data:`PIN`); ``online.vhlru.ru`` resolves
  normally. curl_cffi (Chrome TLS) is the transport, a lazy import (``tests`` / ``all``
  extras).
* **Season -> tn.** The provider keys a season STAGE by tournament id (``tn``): 2025/26
  regular season = 1374, playoffs = 1375. The public key is the END year + ``stage``
  (``regular`` / ``playoff``), mapped through the calendar's ``<select name="tournament">``
  (33 tns, 2010/11 = 189 / 190 to 2026/27 = 1430), fetched once per process and cached.
  The KHL even/odd rule does NOT hold here (2019/20 = 872 / 875). The wrappers put the END
  year in the tn path segment and the runtime rewrites it.
* **Silent wrong answers, all refused** (fail closed, type-strict compares):

  - an unknown tn serves the CURRENT season (1430) with HTTP 200: the calendar, standings and
    stats pages must select the requested tn, and every calendar game link and standings
    team link must carry it;
  - ``/standings/regular/{tn}/`` ignores the tn, so the standings wrapper reads
    ``/standings/league/{tn}/`` (regular season only);
  - an unknown game id (or a tn that does not hold it) answers an empty ~157 KB report shell:
    the game must be in the tn's calendar, and the report must name that calendar's season,
    stage and game number (the page carries no game id). The broadcast's title must carry
    the same game number;
  - an unknown player redirects to ``/404.php`` and an unknown broadcast to the online index:
    the final URL must equal the requested one. A player page must link its own id;
  - an unknown team id answers a ~158 KB shell without the team block. The team page names
    no id of its own, so only the shell is refused there.

* **Error vocabulary.** ``NoDataError``: HTTP 404, a season with no such stage, a game not in
  the calendar, a report / team shell, a redirect to the 404 page or the online index.
  ``ValueError``: HTTP 400 / 422, an unknown ``stage``, a missing ``idgame`` / ``season``.
  ``AssetFetchError``: any other non-200 (429 / 5xx after three attempts, 2 s / 4 s apart, or
  longer when a 429 / 503 sends ``Retry-After`` in seconds, capped at 120 s),
  a dead connection, a non-HTML or empty body, bytes that do not decode in the declared
  charset, any other redirect, and a page for another scope or with a missing table, header
  drift, zero rows or a row of the wrong width.

Every request waits for :func:`sportsdataverse.hockey.euro_hockey_runtime._polite_wait`
(at least 1 s per host).
"""

from __future__ import annotations

import functools
import re
import threading
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime
from sportsdataverse.hockey.vhl.vhl_parsers import _NotParsed, _page_record, _tournament_table

__all__ = ["STAGES", "vhl_tn"]

HOST = "https://www.vhlru.ru"
ONLINE = "https://online.vhlru.ru"
#: curl RESOLVE entry for ``www.vhlru.ru`` only (the other A record stalls).
PIN = "www.vhlru.ru:443:188.128.49.29"
#: The calendar page whose tournament select maps seasons to tns (it serves the current season).
CALENDAR_ROOT = f"{HOST}/en/calendar/"
#: Key the request is stamped under (the parsers read it).
REQUEST_KEY = "_request"
#: Season stages, as the wrappers name them.
STAGES = ("regular", "playoff")
_STAGE_LABEL = {"regular": "Regular Season", "playoff": "Playoffs"}
TIMEOUT = 90

#: (host, path pattern) -> page kind. The number is the season (END year) on tn routes.
_ROUTES: Tuple[Tuple[str, "re.Pattern[str]", str], ...] = (
    ("www.vhlru.ru", re.compile(r"^/en/calendar/(\d+)/0/?$"), "calendar"),
    ("www.vhlru.ru", re.compile(r"^/en/report/(\d+)/?$"), "report"),
    ("www.vhlru.ru", re.compile(r"^/en/standings/league/(\d+)/?$"), "standings"),
    ("www.vhlru.ru", re.compile(r"^/en/stats/players/(\d+)/?$"), "stats_players"),
    ("www.vhlru.ru", re.compile(r"^/en/players/(\d+)/?$"), "player"),
    ("www.vhlru.ru", re.compile(r"^/en/teams/(\d+)/?$"), "team"),
    ("online.vhlru.ru", re.compile(r"^/online/(\d+)\.html$"), "online"),
)
_TN_KINDS = frozenset({"calendar", "report", "standings", "stats_players"})

_games: Dict[int, Dict[int, Any]] = {}  # tn -> {game_id: game_number}, from the calendar
_games_lock = threading.Lock()


# ------------------------------------------------------------------------------ transport
def _transport(url: str) -> Any:
    """GET ``url`` with Chrome TLS; ``www.vhlru.ru`` pinned to the working IP (:data:`PIN`)."""
    try:
        from curl_cffi import CurlOpt
        from curl_cffi import requests as creq
    except ImportError as exc:  # pragma: no cover - exercised only on the live path
        raise ImportError(
            "Live vhlru.ru calls require curl_cffi. Install with: pip install curl_cffi "
            "or pip install sportsdataverse[all]"
        ) from exc
    opts = {CurlOpt.RESOLVE: [PIN]} if urlsplit(url).hostname == "www.vhlru.ru" else None
    return creq.get(url, impersonate="chrome", timeout=TIMEOUT, curl_options=opts)


def _is_missing_page(final: str) -> bool:
    """True for the site's "no such page" redirect targets: ``/404.php`` and the online index."""
    target = urlsplit(final)
    return target.path == "/404.php" or (target.hostname == "online.vhlru.ru" and target.path in ("", "/", "/online/"))


def _fetch(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    """The HTML text of ``url`` (+ ``params``), or the error the answer calls for (see ``get_text``)."""
    return euro_hockey_runtime.get_text(
        url, params, transport=lambda full: _transport(full), expect="html", is_missing=_is_missing_page
    )


def _page(kind: str, url: str, params: Optional[Dict[str, Any]], want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Fetch and parse one page; a refused parse becomes the error vocabulary."""
    text = _fetch(url, params)
    try:
        return _page_record(kind, text, want)
    except _NotParsed as exc:
        if exc.empty:
            raise NoDataError(f"{_rt._where(url)}: {exc}") from None
        raise AssetFetchError(f"{_rt._where(url)}: not the page asked for: {exc}") from None


# ---------------------------------------------------------------------------- season -> tn
def _season(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).strip().isdigit():
        raise ValueError(f"season must be an END year such as 2026 (= 2025/26); got {value!r}")
    return int(value)


def _stage(value: Any) -> str:
    if value not in STAGES:
        raise ValueError(f"stage must be one of {STAGES!r}; got {value!r}")
    return str(value)


@functools.lru_cache(maxsize=1)
def _tournaments() -> Dict[Tuple[int, str], int]:
    """``{(END season, stage): tn}`` from the calendar's tournament select (one fetch, cached)."""
    rows = _tournament_table(_fetch(CALENDAR_ROOT))
    if not rows:
        raise AssetFetchError(f"{_rt._where(CALENDAR_ROOT)}: no tournament select on the calendar page")
    return {(r["season"], r["stage"]): r["tn"] for r in rows}


def vhl_tn(season: int, stage: str = "regular") -> int:
    """The VHL tournament id (``tn``) of a season (END year) and stage.

    Read from the calendar's tournament select, fetched once per process. The tn is what the
    site keys every season page by; the wrappers map it for you, so you need it only to read
    ``tn`` columns or build a URL by hand.

    Args:
        season: END year of the season (2026 = 2025/26). The calendar covers 2010/11 on.
        stage: ``"regular"`` (default) or ``"playoff"``.

    Returns:
        int: the tn (``1374`` for the 2025/26 regular season).

    Raises:
        ValueError: ``season`` is not a year or ``stage`` is not in :data:`STAGES`.
        NoDataError: the calendar lists no such season and stage.
        AssetFetchError: the calendar page could not be fetched or has no tournament select.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_tn
            vhl_tn(2026)              # 1374
            vhl_tn(2026, "playoff")   # 1375

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_schedule` - a route this feeds.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    season, stage = _season(season), _stage(stage)
    table = _tournaments()
    tn = table.get((season, stage))
    if tn is None:
        years = sorted({s for s, _ in table})
        raise NoDataError(
            f"VHL lists no {stage} tournament for season {season} (END year; calendar covers {years[0]}-{years[-1]})"
        )
    return tn


def _game_number(tn: int, game_id: int) -> Any:
    """The game number the calendar of ``tn`` gives ``game_id`` (the report's identity check)."""
    with _games_lock:
        known = _games.get(tn, {})
    if game_id not in known:  # one fresh calendar per miss: a game scheduled since the last read
        _remember_games(tn, _page("calendar", f"{HOST}/en/calendar/{tn}/0/", None, {"tn": tn}))
        with _games_lock:
            known = _games.get(tn, {})
    if game_id not in known:
        raise NoDataError(f"VHL game {game_id} is not in the calendar of tournament {tn}")
    return known[game_id]


def _remember_games(tn: int, rec: Dict[str, Any]) -> None:
    index = {g["game_id"]: g.get("game_number") for g in rec.get("games") or [] if isinstance(g, dict)}
    with _games_lock:
        _games[tn] = index


# ------------------------------------------------------------------------------- the getter
def _route(url: str) -> Tuple[str, int]:
    parts = urlsplit(url)
    for host, pattern, kind in _ROUTES:
        m = pattern.match(parts.path)
        if parts.hostname == host and m:
            return kind, int(m.group(1))
    raise ValueError(f"not a VHL route: {_rt._where(url)}")


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a VHL page, refuse every wrong answer, return its parsed record with the request stamped.

    Args:
        url: the URL a generated wrapper built. On tn routes (calendar, report, standings,
            stats) the tn path segment holds the season END year, which is mapped to the tn.
        params: query parameters. ``stage`` (``regular`` / ``playoff``, default regular) and,
            for the broadcast, ``season`` are consumed here; ``idgame`` is sent to the report.
            ``None`` values are dropped.
        **kwargs: accepted from the generated wrappers; none is supported, so any raises.

    Returns:
        The page's parsed record (the refs capture shape) with ``_request`` =
        ``{"season", "stage", "tn", "game_id", "player_id", "team_page_id"}`` (those that apply).

    Raises:
        NoDataError: see the module docstring (404, unknown stage of a season, a game not in
            the calendar, a shell page, a redirect to the 404 page / online index).
        ValueError: a bad ``stage`` / ``season``, a missing ``idgame``, HTTP 400 / 422, or a
            URL that is not a VHL route.
        AssetFetchError: any other failed or wrong answer (see the module docstring).
        TypeError: an unsupported keyword argument.
    """
    if kwargs:
        raise TypeError(f"unsupported argument(s) for a VHL request: {sorted(kwargs)}")
    q: Dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
    stage = _stage(q.pop("stage", "regular"))
    kind, number = _route(url)
    req: Dict[str, Any] = {}
    want: Optional[Dict[str, Any]] = None
    if kind in _TN_KINDS:
        if kind == "standings" and stage != "regular":
            raise ValueError("VHL standings exist for the regular season only (stage='regular')")
        if kind == "report":  # checked before any request
            game_id = q.get("idgame")
            if game_id is None or isinstance(game_id, bool) or not str(game_id).strip().isdigit():
                raise ValueError(f"the VHL report needs an integer game id (idgame); got {game_id!r}")
            q["idgame"] = int(game_id)
        season = number
        tn = vhl_tn(season, stage)
        parts = urlsplit(url)
        url = urlunsplit(parts._replace(path=parts.path.replace(f"/{number}/", f"/{tn}/", 1)))
        req = {"season": season, "stage": stage, "tn": tn}
        want = {"tn": tn}
        if kind == "report":
            req["game_id"] = q["idgame"]
            want = {
                "season": f"{season - 1}/{season}",
                "stage": _STAGE_LABEL[stage],
                "game_number": _game_number(tn, q["idgame"]),
            }
    elif kind == "online":
        if q.get("season") is None:
            raise ValueError("the VHL broadcast needs the game's season (END year) to check the page")
        season = _season(q.pop("season"))
        tn = vhl_tn(season, stage)
        req = {"season": season, "stage": stage, "tn": tn, "game_id": number}
        want = {"game_number": _game_number(tn, number)}
    elif kind == "player":
        req, want = {"player_id": number}, {"player_id": number}
    else:  # team: the page names no id of its own; only the shell is refused
        req = {"team_page_id": number}
    rec = _page(kind, url, q, want)
    if kind == "calendar":
        _remember_games(req["tn"], rec)
    rec[REQUEST_KEY] = req
    return rec
