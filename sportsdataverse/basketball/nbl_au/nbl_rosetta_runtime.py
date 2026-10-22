"""Runtime for the generated NBL Rosetta wrappers (``prod.rosetta.nbl.com.au/get``).

Rosetta is the JSON proxy behind nbl.com.au. One host serves the NBL, the WNBL and NBL1 (the
``org`` path segment: ``nbl``, ``wnbl``, ``nbl1``). It is not an API key but a referer gate:
without ``Origin`` / ``Referer`` set to nbl.com.au every route answers 403
``{"error": "Access Denied", "code": "MISSING_API_KEY"}``. This runtime sends both headers and
treats that body as a failed fetch, never as data.

Every request goes through the shared international-basketball throttle
(:func:`sportsdataverse.basketball.basketball_runtime._get`: every ``download()`` attempt,
retries included, waits at least 1 s for its host; ``Retry-After`` is honoured on 429 / 503), so
the package error vocabulary holds: 404 -> ``NoDataError``, 400 / 422 -> ``ValueError``, any other
non-2xx, a dead connection or an empty / non-JSON 200 -> ``AssetFetchError``. On top of it,
measured against the live API (sdv-internal-refs ``nbl-rosetta/``, 2026-10-12):

* **Season key.** The public ``season`` is the END year (2026 = 2025-26). Rosetta's ``year`` is
  the START year for the NBL and WNBL from 1998-99 (``season/2025`` = 2025-26) and the calendar
  year for NBL1 and for the NBL / WNBL winter seasons 1979-1997 (:func:`start_year`). Year 1998
  holds two seasons (calendar 1998 and 1998-99); asking for END 1998 or END 1999 both read
  ``year=1998``, and the matches parser keeps the rows of the season asked for.
* **Unknown ids, orgs and types answer 200 with ``data: []``** (an unknown match id even says
  ``count: 1``): :class:`NoDataError`.
* **A body that is not the ``{type, fetched, ..., data}`` envelope** (a stub) or that carries an
  ``error`` / ``MISSING_API_KEY`` marker: :class:`AssetFetchError`.
* **Scope.** Every row of a scoped route must prove it answers the request, type-strictly
  (a row without the field is refused): the org shortcode, ``season.year``, the match id, the
  team, player or season id. A body for another org, season or entity is an
  :class:`AssetFetchError`, never an empty frame (:data:`SCOPES`).
* **Truncated pages.** Rosetta caches a body per path for ``ttlRemaining`` seconds and serves it
  whatever the query string, so a request can receive a page cut by an earlier caller's
  ``limit``. When the runtime supplied the limit (the caller sent none), a row count equal to
  that limit or to a known page size (:data:`PAGING`) is an :class:`AssetFetchError`; retry
  after the cache entry expires (``ttlRemaining``). A limit the caller sent is never second-guessed.
* ``org`` and ``type`` are checked before sending (:data:`ORGS`, :data:`TYPES`).

The request is stamped into the envelope under ``_request`` (``org``, ``season`` END year,
``year``, ``match_id``): the bodies of some routes do not say which org or season they answer,
and the parsers turn the stamp into ``org`` / ``season`` columns. The raw body
(``return_parsed=False``) carries that key too.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

HOST = "https://prod.rosetta.nbl.com.au/get"
#: The gate: Rosetta answers 403 MISSING_API_KEY unless the request looks like nbl.com.au.
GATE_HEADERS = {"Origin": "https://www.nbl.com.au", "Referer": "https://www.nbl.com.au/"}
#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"
ORGS = ("nbl", "wnbl", "nbl1")
#: ``{type}`` of the season routes (``all`` = every type of the year, the route without a type).
TYPES = ("all", "regular", "preseason", "in_season", "test")

# route key -> (path regex with named groups, limit the runtime sends when the caller sends none, page sizes that
# mean "cut short": the route default plus the limits nbl.com.au itself sends).
_O = r"(?P<org>[a-z0-9]+)"
_Y = r"(?P<year>\d+)"
ROUTES: Dict[str, "re.Pattern[str]"] = {
    "seasons": re.compile(rf"^/{_O}/seasons$"),
    "seasons_current": re.compile(rf"^/{_O}/seasons/current$"),
    "matches": re.compile(rf"^/{_O}/matches/in/season/{_Y}/(?P<type>[a-z_]+)$"),
    "teams": re.compile(rf"^/{_O}/teams$"),
    "match": re.compile(r"^/match/(?P<match_id>[^/]+)(?:/live/all)?$"),
    "standings": re.compile(rf"^/{_O}/standings/{_Y}/regular$"),
    "players": re.compile(rf"^/{_O}/players/in/season/{_Y}$"),
    "team_players": re.compile(rf"^/{_O}/players/for/team/(?P<team_id>[^/]+)/in/season/{_Y}$"),
    "outcomes": re.compile(rf"^/match/outcomes/for/{_O}/teams/in/season/{_Y}/(?P<type>[a-z_]+)$"),
    "player_boxscores": re.compile(rf"^/{_O}/player_boxscores/for/(?P<player_id>[^/]+)/in/season/{_Y}/regular$"),
    "player_statistics": re.compile(rf"^/{_O}/statistics/for/player/(?P<player_id>[^/]+)$"),
    "leaders": re.compile(rf"^/{_O}/stats/leaders/for/season/id/(?P<season_id>[^/]+)$"),
    "team_leaders": re.compile(rf"^/{_O}/stats/leaders/for/season/id/(?P<season_id>[^/]+)/team/(?P<team_code>[^/]+)$"),
    "team_stats": re.compile(rf"^/{_O}/team/stats/for/season/{_Y}/regular$"),
    "next_match": re.compile(rf"^/next/match/for/{_O}/{_Y}$"),
    "next_match_team": re.compile(rf"^/next/match/for/team/(?P<team_code>[^/]+)/{_O}/{_Y}$"),
    "next_matches": re.compile(rf"^/next/matches/for/teams/in/{_O}/{_Y}$"),
}
#: route -> (limit sent when the caller sends none, page sizes that mean a cut page). Measured 2026-10-12.
PAGING: Dict[str, Tuple[int, frozenset]] = {
    "seasons": (1000, frozenset({100})),
    "teams": (5000, frozenset({100})),
    "matches": (2000, frozenset({100, 500})),
    "standings": (1000, frozenset({100})),
    "players": (5000, frozenset({500})),
    "team_stats": (5000, frozenset({500})),
    "leaders": (5000, frozenset({5, 50, 150})),
    "team_leaders": (150, frozenset({5, 50})),
}

_SHORTCODE = "competition.league.organisation.shortcode"
#: route -> [(dotted row field, group it must equal, or "org" for the org shortcode)]; ``|`` lists accepted alternatives.
SCOPES: Dict[str, List[Tuple[str, str]]] = {
    "seasons": [(_SHORTCODE, "org")],
    "matches": [("season.year", "year_str"), (f"season.{_SHORTCODE}", "org")],
    "match": [("id", "match_id")],
    "standings": [("season.year", "year_str"), (f"season.{_SHORTCODE}", "org")],
    "team_players": [("team.id|team.external_id", "team_id"), (f"season.{_SHORTCODE}", "org")],
    "player_boxscores": [("match.season.year", "year_str"), ("player.id|player.external_id", "player_id")],
    "player_statistics": [("player.id|player.external_id", "player_id"), (f"season.{_SHORTCODE}", "org")],
    "leaders": [("season.id|season.external_id", "season_id"), (f"season.{_SHORTCODE}", "org")],
    "team_leaders": [
        ("season.id|season.external_id", "season_id"),
        ("team.team_code", "team_code"),
        (f"season.{_SHORTCODE}", "org"),
    ],
    "team_stats": [("season.year", "year_str"), (f"season.{_SHORTCODE}", "org")],
    "next_match": [("season.year", "year_str"), (f"season.{_SHORTCODE}", "org")],
    "next_match_team": [("season.year", "year_str"), (f"season.{_SHORTCODE}", "org")],
}


def start_year(org: str, season: int) -> int:
    """Rosetta's ``year`` for a season END year.

    Args:
        org: ``nbl``, ``wnbl`` or ``nbl1``.
        season: the season END year (2026 = 2025-26).

    Returns:
        The calendar year for NBL1 and for the NBL / WNBL winter seasons up to 1998; the START
        year (``season - 1``) for the NBL and WNBL from 1998-99 (END 1999) on. END 1998 and
        END 1999 both map to ``year=1998``, which holds the calendar-1998 and the 1998-99 season.

    Example:
        Quick start::

            start_year("nbl", 2026)   # 2025
            start_year("nbl1", 2025)  # 2025
    """
    return season if (org == "nbl1" or season <= 1998) else season - 1


def end_year(org: str, year: int, start_date: Optional[str] = None) -> Optional[int]:
    """The season END year of a Rosetta ``year`` (the inverse of :func:`start_year`).

    Args:
        org: ``nbl``, ``wnbl`` or ``nbl1``.
        year: Rosetta's ``year``.
        start_date: ISO date the season or match starts; only used to split year 1998.

    Returns:
        ``year`` for NBL1 and for years up to 1997, ``year + 1`` from 1999. For 1998 the start
        month decides (July or later = the 1998-99 season = 1999); without a date, ``None``.
    """
    if org == "nbl1" or year <= 1997:
        return year
    if year >= 1999:
        return year + 1
    month = re.match(r"^\d{4}-(\d{2})", start_date or "")
    if month is None:
        return None
    return 1999 if int(month.group(1)) >= 7 else 1998


def _route(path: str) -> Tuple[str, "re.Match[str]"]:
    for name, pattern in ROUTES.items():
        m = pattern.match(path)
        if m:
            return name, m
    raise ValueError(f"not an NBL Rosetta route: {path!r}")


def _dig(row: Any, dotted: str) -> Any:
    """Null-safe nested get: ``None`` when any hop is missing or not an object."""
    for key in dotted.split("."):
        row = row.get(key) if isinstance(row, dict) else None
    return row


def _scope(where: str, route: str, groups: Mapping[str, str], rows: List[Any]) -> None:
    """Raise :class:`AssetFetchError` unless every row proves it answers the request (type-strict, fail-closed)."""
    for field, group in SCOPES.get(route, []):
        want = groups.get("year") if group == "year_str" else groups.get(group)
        if group == "year_str":
            want = str(int(groups["year"]))  # Rosetta writes the year as a string
        for row in rows:
            got = [_dig(row, f) for f in field.split("|")]
            if not any(type(g) is str and g == want for g in got):
                raise AssetFetchError(f"{where}: out of scope: {field}={got[0]!r}, asked for {want!r}")


def _check(url: str, route: str, groups: Mapping[str, str], body: Any, sent_limit: Optional[int]) -> List[Any]:
    """Validate an envelope; return its rows. Raises as the module docstring says."""
    where = _rt._where(url)
    if isinstance(body, dict) and ("error" in body or body.get("code") == "MISSING_API_KEY"):
        raise AssetFetchError(f"{where}: Rosetta error envelope: {body.get('code') or body.get('error')!r}")
    if not isinstance(body, dict) or "data" not in body or "type" not in body:
        raise AssetFetchError(f"{where}: not the Rosetta {{type, ..., data}} envelope")
    data = body["data"]
    if not isinstance(data, (list, dict)):
        raise AssetFetchError(f"{where}: data is a {type(data).__name__}, not rows")
    rows = list(data.values()) if isinstance(data, dict) else data
    if not rows:
        raise NoDataError(f"{where}: Rosetta answered data: [] (an unknown id, org or type, or nothing yet)")
    if not all(isinstance(r, dict) for r in rows):
        raise AssetFetchError(f"{where}: data rows are not objects")
    _scope(where, route, groups, rows)
    if route in PAGING and sent_limit is not None and isinstance(data, list):
        sizes = PAGING[route][1] | {sent_limit}
        if len(rows) in sizes:
            raise AssetFetchError(
                f"{where}: {len(rows)} rows is a page size {sorted(sizes)}: a page cut by a cached request; "
                f"retry after the cache entry expires (ttlRemaining={body.get('ttlRemaining')!r}s)"
            )
    return rows


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a Rosetta route as JSON: gate headers, END->START season mapping, refusals, request stamp.

    Args:
        url: fully-built request URL; a season segment is the END year and is rewritten to
            Rosetta's ``year`` here.
        params: query-string parameters; ``None`` values are dropped. ``limit`` defaults per route
            (:data:`PAGING`) so a list is never the route's short default page.
        **kwargs: forwarded to the HTTP getter; ``headers`` merge over the gate headers.

    Returns:
        The envelope ``{type, fetched, ttlRemaining, count, source, data}`` with the request
        stamped under :data:`REQUEST_KEY`.

    Raises:
        NoDataError: the host answered 404 or ``data: []``.
        ValueError: ``org`` or ``type`` is not one of :data:`ORGS` / :data:`TYPES` (checked before
            sending), or the host answered 400 / 422.
        AssetFetchError: any other non-2xx (the 403 gate body included), a connection failure after
            retries, an empty / non-JSON / stub / error-shaped 200, a page cut by the path cache or a
            body for another org, season or entity than the one asked for.
    """
    parts = urlsplit(url)
    path = parts.path[len("/get") :] if parts.path.startswith("/get/") else parts.path
    route, match = _route(path)
    groups = match.groupdict()
    org = groups.get("org")
    if org is not None and org not in ORGS:
        raise ValueError(f"org must be one of {ORGS!r}; got {org!r}")
    if groups.get("type") is not None and groups["type"] not in TYPES:
        raise ValueError(f"type must be one of {TYPES!r}; got {groups['type']!r}")
    stamp: Dict[str, Any] = {k: v for k, v in groups.items() if k in ("org", "match_id")}
    if "year" in groups:
        end = int(groups["year"])
        year = start_year(org or "nbl", end)
        stamp.update(season=end, year=year)
        path = path[: match.start("year")] + str(year) + path[match.end("year") :]
        groups["year"] = str(year)
        url = urlunsplit((parts.scheme, parts.netloc, "/get" + path, parts.query, ""))
    params = dict(params or {})
    sent_limit: Optional[int] = None
    if route in PAGING:
        if params.get("limit") is None:
            params["limit"] = sent_limit = PAGING[route][0]
    headers = {**GATE_HEADERS, **(kwargs.pop("headers", None) or {})}
    body = basketball_runtime._fetch_json(url, params, headers=headers, **kwargs)
    _check(url, route, groups, body, sent_limit)
    body[REQUEST_KEY] = stamp
    return body
