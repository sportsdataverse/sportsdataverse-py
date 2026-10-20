"""Runtime for the generated Atrium wrappers (Sportradar embed API, keyless JSON).

``https://embed-api.eui.connect.sportradar.com/v1/embed/{websiteId}/{type}`` serves the view
model of the Sportradar "Connect" widget that league sites embed. It is the public endpoint the
widget calls from every visitor's browser: no key and no special headers. It is not an official
or supported API; stay polite (every request, retries included, goes through the shared
:data:`~sportsdataverse.basketball.basketball_runtime.SESSION`, at least 1 s apart per host).
Status codes follow the package vocabulary: 404 -> ``NoDataError``, 400 / 422 -> ``ValueError``
(the API answers 400 ``The parameter 'X' is not valid for this call`` to a misplaced parameter),
any other non-2xx, a dead connection or an empty / non-JSON 200 -> ``AssetFetchError``.

**Seasons.** The native season key is an opaque ``seasonId`` (a UUID); one competition in one year
is one season, and a site lists many (LNB lists Betclic ELITE, its playoffs and play-in, PROB,
Espoirs, ... for the same year). The wrappers take the sdv END year as ``season`` plus an optional
``competition_id`` and :func:`atrium_season_id` maps the pair through the site's own season list
(``/{websiteId}/fixtures``, fetched once an hour and cached). ``season_id`` skips the lookup.
What a site's ``year`` means is :data:`~sportsdataverse.basketball.atrium.atrium_parsers.SITES`:
the START year on site 12 (LNB), the calendar year on 14, 3, 13, 22 and 326, nothing on 236 (no
season list: pass ``season_id`` or no season). A site outside ``SITES`` cannot be mapped and
raises ``ValueError`` rather than guess. An END year several seasons share without a
``competition_id`` raises ``ValueError`` naming the candidates; one the site never listed raises
``NoDataError``. Without ``season`` / ``season_id`` the API serves the site's default season.

**Refusals** (``AssetFetchError``: the answer is unknown, never an empty frame). The API's error
body ``{"error": ..., "links": ...}``; a body without a ``data`` object or with ``meta.code`` other
than 200; a body whose ``links.self`` is another site or type than the one asked (a cache sits in
front of the API); a body that echoes another ``seasonId``, ``fixtureId``, ``personId``, team
(``entity_detail``) or ``sub`` than the request (an unknown ``sub`` is not an error: the API
returns a summary body, which must not be mistaken for a box score); a default ``fixtures`` page
whose season is not in its own season list. A well-formed body with no rows (an empty fixture
list, a game with no play-by-play yet, an empty ribbon) raises ``NoDataError``.
"""

from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.basketball.atrium.atrium_parsers import SITES, _link_ids, parse_atrium_seasons
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["atrium_season_id"]

BASE = "https://embed-api.eui.connect.sportradar.com/v1/embed"
#: Seconds a site's season list stays cached.
SEASONS_TTL = 3600.0

_SELF = re.compile(r"/v1/embed/(\d+)/([a-z_]+)(?:[?#]|$)")
_PATH = re.compile(r"/v1/embed/(\d+)/([a-z_]+)/?$")
#: Types whose ``competitionId`` is a real filter or key, not just a way to pick the season.
_COMPETITION_TYPES = frozenset({"statistics_competition_persons", "statistics_competition_entities"})
#: Types that echo no ``seasonId`` (the player and team pages).
_NO_SEASON_ECHO = frozenset({"entity_detail", "person_detail"})

_seasons_cache: Dict[int, Tuple[float, List[Dict[str, Any]]]] = {}


def _dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _fetch_seasons(website_id: int, **kwargs: Any) -> List[Dict[str, Any]]:
    """The site's season rows (cached), from its default ``/fixtures`` page."""
    hit = _seasons_cache.get(website_id)
    if hit is not None and time.monotonic() - hit[0] < SEASONS_TTL:
        return hit[1]
    url = f"{BASE}/{website_id}/fixtures"
    body = _bb._fetch_json(url, None, **kwargs)
    _check(url, website_id, "fixtures", {}, body, rows=False)
    rows = list(parse_atrium_seasons(body).iter_rows(named=True))
    _seasons_cache[website_id] = (time.monotonic(), rows)
    return rows


@_rt._no_content_is_unknown  # a lookup: its 204 / 205 is an unknown answer (R4.1b-d-1)
def atrium_season_id(website_id: int, season: int, competition_id: Optional[str] = None, **kwargs: Any) -> str:
    """The ``seasonId`` of a site's season, from its END year and optionally its competition.

    Args:
        website_id: the site (see ``SITES``: 12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 326 Montenegro).
        season: the sdv season END year (2026 = 2025-26 on LNB; the calendar year on the other sites).
        competition_id: the competition when the site lists several for that year (see
            ``atrium_seasons``). Omit it when the year is unique.
        **kwargs: forwarded to the HTTP getter when the season list has to be fetched.

    Returns:
        The season's UUID, for the ``season_id`` argument of every season-scoped wrapper.

    Raises:
        ValueError: ``season`` is not an integer, the site is not in ``SITES``, it lists no seasons
            (236), or the year is shared by several competitions and ``competition_id`` is missing.
        sportsdataverse.errors.NoDataError: the site lists no such season.
        AssetFetchError: the season list could not be fetched or is not the site's.

    Example:
        Quick start::

            from sportsdataverse.basketball.atrium import atrium_season_id
            atrium_season_id(14, 2026, competition_id="16be8396-24fb-11ef-9ab7-df6be196e88f")
    """
    if isinstance(season, bool) or not isinstance(season, int):
        raise ValueError(f"season must be the END year as an integer; got {season!r}")
    if website_id not in SITES:
        raise ValueError(
            f"website_id {website_id!r} is not a known Atrium site ({sorted(SITES)}): its season year cannot be mapped to an "
            "END year, so pass season_id instead"
        )
    if SITES[website_id][1] is None:
        raise ValueError(f"Atrium site {website_id} lists no seasons: pass season_id, or no season for its current one")
    rows = [
        r
        for r in _fetch_seasons(website_id, **kwargs)
        if r["season_end_year"] == season and (competition_id is None or r["competition_id"] == competition_id)
    ]
    if not rows:
        which = f" for competition {competition_id}" if competition_id else ""
        raise NoDataError(f"Atrium site {website_id} lists no season with END year {season}{which}")
    if len(rows) > 1:
        names = "; ".join(f"{r['competition_name']} ({r['competition_id']})" for r in rows[:8])
        raise ValueError(
            f"END year {season} is ambiguous on Atrium site {website_id}: {len(rows)} seasons ({names}"
            f"{'; ...' if len(rows) > 8 else ''}). Pass competition_id (see atrium_seasons) or season_id."
        )
    return rows[0]["season_id"]


def _rows_missing(typ: str, p: Dict[str, Any], data: Dict[str, Any]) -> bool:
    """True when a well-formed body has no rows of the kind the route exists to serve."""

    def nonempty(v: Any) -> bool:
        return isinstance(v, list) and any(isinstance(r, dict) and r for r in v)

    sub = p.get("sub")
    if typ in ("fixtures", "fixtures_ribbon"):
        return not nonempty(data.get("fixtures"))
    if typ == "fixture_detail":
        periods = [x for x in data.get("periodData") or [] if isinstance(x, dict)]
        if sub == "statistics":
            base = _dict(_dict(_dict(data.get("statistics")).get("data")).get("base"))
            return not any(
                nonempty(_dict(g).get("rows"))
                for side in ("home", "away")
                for g in _dict(base.get(side)).get("persons") or []
            )
        if sub in ("pbp", "shot_chart"):
            return not any(nonempty(x.get("events" if sub == "pbp" else "shots")) for x in periods)
        return False
    if typ in ("standings", "standings_fragment"):
        return not any(nonempty(_dict(g).get("rows")) for g in data.get("standings") or [])
    if typ == "entities":
        return not nonempty(data.get("teams"))
    if typ == "persons":
        return not nonempty(_dict(data.get("persons")).get("rows"))
    if typ == "person_detail":
        return not (nonempty(_dict(data.get("log")).get("rows")) if sub == "log" else _dict(data.get("person")))
    if typ == "entity_detail":
        return not _dict(data.get("team"))
    if typ.startswith("statistics_"):
        tables = data.get("statistics") or []
        if typ == "statistics_leaders":
            return not any(nonempty(_dict(t).get("data")) for t in tables)
        return not any(nonempty(_dict(t).get("statistics")) for t in tables)
    return False


def _check(url: str, website_id: int, typ: str, p: Dict[str, Any], body: Any, rows: bool = True) -> None:
    """Raise unless ``body`` is a real answer to the request (see the module docstring)."""
    where = _rt._where(url)
    if not isinstance(body, dict):
        raise AssetFetchError(f"{where}: the answer is not a JSON object")
    if "error" in body:
        raise AssetFetchError(f"{where}: the API answered an error object: {str(body['error'])[:120]}")
    data = body.get("data")
    code = _dict(body.get("meta")).get("code")
    if not isinstance(data, dict) or not (type(code) is int and code == 200):
        raise AssetFetchError(f"{where}: no data object / meta.code {code!r} (a stub or an answer of unknown meaning)")
    m = _SELF.search(str(_dict(body.get("links")).get("self") or ""))
    if not m:
        raise AssetFetchError(f"{where}: the body does not echo its request in links.self (scope unknown)")
    if int(m.group(1)) != website_id or m.group(2) != typ:
        raise AssetFetchError(f"{where}: asked for /{website_id}/{typ}, the body is /{m.group(1)}/{m.group(2)}")

    def echo(field: str, got: Any, want: Any) -> None:
        if want is not None and not (type(got) is type(want) and got == want):
            raise AssetFetchError(f"{where}: asked for {field}={want!r}, the body has {got!r}")

    if "sub" in p:
        echo("sub", data.get("sub"), p["sub"])
    if "seasonId" in p and typ not in _NO_SEASON_ECHO:
        echo("seasonId", data.get("seasonId"), p["seasonId"])
    if "fixtureId" in p:
        echo("fixtureId", _dict(data.get("fixture")).get("fixtureId"), p["fixtureId"])
    if "personId" in p:
        echo("personId", _dict(data.get("person")).get("id"), p["personId"])
    if typ == "entity_detail" and "entityId" in p:
        tabs = data.get("tabs") or []
        echo("entityId", _link_ids(_dict(tabs[0]).get("link")).get("e") if tabs else None, p["entityId"])
    if typ == "fixtures" and "seasonId" not in p:
        listed = {s.get("seasonId") for s in _dict(data.get("seasons")).get("seasons") or [] if isinstance(s, dict)}
        if listed and data.get("seasonId") not in listed:
            raise AssetFetchError(
                f"{where}: the default season {data.get('seasonId')!r} is not in the site's own season list"
            )
    if rows and _rows_missing(typ, p, data):
        raise NoDataError(f"{where}: the site has no rows for this request")


@_rt._no_content_is_empty
def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET an Atrium embed route as JSON; map ``season`` to ``seasonId`` and refuse every wrong answer.

    Args:
        url: ``.../v1/embed/{websiteId}/{type}``.
        params: wire parameters plus the sdv ``season`` (END year), which is replaced by the
            ``seasonId`` :func:`atrium_season_id` finds. When ``season`` finds the season, a
            ``competitionId`` given beside it only picked the season and is not sent (except to the
            two ``statistics_competition_*`` routes). ``None`` values are dropped.
        **kwargs: forwarded to the HTTP getter.

    Returns:
        The parsed JSON body.

    Raises:
        NoDataError: the host answered 404, the site lists no such season, or the body has no rows.
        ValueError: the host answered 400 / 422, ``season`` and ``seasonId`` were both given, or the
            season cannot be mapped (see :func:`atrium_season_id`).
        AssetFetchError: any other failed fetch, an error / stub / wrong-scope body, or a body of
            another site, type, season, game, player or team than the one asked for.
    """
    m = _PATH.search(urlsplit(url).path)
    if not m:
        raise ValueError(f"not an Atrium embed URL: {url}")
    site, typ = int(m.group(1)), m.group(2)
    p = {k: v for k, v in (params or {}).items() if v is not None}
    season = p.pop("season", None)
    if season is not None:
        if "seasonId" in p:
            raise ValueError("pass season (END year) or season_id, not both")
        p["seasonId"] = atrium_season_id(site, season, p.get("competitionId"), **kwargs)
        if typ not in _COMPETITION_TYPES:
            p.pop("competitionId", None)
    body = _bb._fetch_json(url, p, **kwargs)
    _check(url, site, typ, p, body)
    return body
