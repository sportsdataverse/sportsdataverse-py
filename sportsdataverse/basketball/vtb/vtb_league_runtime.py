"""Runtime for the generated ``vtb_league`` wrappers (VTB United League schedule API, ``api.vtb-league.com/v2``).

Every request goes through the shared :data:`sportsdataverse.basketball.basketball_runtime.SESSION`, which waits at least
one second before **every** attempt, retries included (``dl_utils.download`` honours ``Retry-After`` on 429 / 503). The
API needs no key, cookie or proxy (sdv-internal-refs ``vtb/README.md``: direct from the droplet at 1 request per second,
no throttling), so no proxy variable is read. ``api.vtb-league.com/robots.txt`` is a 404 (no rules).

**Owner flag.** The league is run from Russia and several clubs are owned or sponsored by sanctioned state companies (the
KHL precedent). Fetching on the user's own machine is fine; a published data release, hosted mirror or redistribution
needs owner sign-off first.

What the live API does (sdv-internal-refs ``vtb/``, probed 2026-10-12):

* **Every answer is an envelope** ``{"meta": {...}, "data": [...]}``. ``_get`` returns that envelope with the request
  stamped under ``_request`` (``league``, ``season``); the parsers read ``data`` and the stamp. A ``meta.status`` other
  than ``success`` or a ``meta.code`` other than 200, or ``data`` that is not ``meta.count`` rows, is ``AssetFetchError``.
* **Seasons are END years in the API too** (``season=2026`` is 2025-26), so no mapping is needed.
* **Pages.** ``meta.count`` is the number of rows *returned*, not a total, ``page`` is ignored and the default ``limit``
  is 20. The matches wrapper sends ``limit=1000`` and a body whose ``count`` reaches its ``limit`` may be cut:
  ``AssetFetchError``.
* **Scope.** A body must be the one asked for, else ``AssetFetchError`` (never an empty frame): every season row must
  carry the requested ``league``, every match row the requested ``league`` and ``season`` (type-strict).
* **Nothing here** is ``NoDataError``: an unknown league (the API answers an HTML 404) and a season with no rows (the API
  lists new seasons only, from 2024-25, so ``season=2024`` and earlier answer 200 with 0 rows).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse._codegen_runtime import _where
from sportsdataverse.basketball import basketball_runtime as _bb
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

BASE = "https://api.vtb-league.com"

#: Key the request is stamped under (see the module docstring).
REQUEST_KEY = "_request"

_ROUTES: List[Tuple[str, "re.Pattern[str]", Tuple[str, ...]]] = [
    ("/v2/leagues", re.compile(r"^/v2/leagues/?$"), ()),
    ("/v2/leagues/{league_slug}/seasons", re.compile(r"^/v2/leagues/([A-Za-z0-9_-]+)/seasons/?$"), ("league_slug",)),
    (
        "/v2/leagues/{league_slug}/seasons/{season}/matches",
        re.compile(r"^/v2/leagues/([A-Za-z0-9_-]+)/seasons/(\d+)/matches/?$"),
        ("league_slug", "season"),
    ),
]


def _route(url: str) -> Tuple[str, Dict[str, Any]]:
    """The route template of ``url`` and its path values; ``ValueError`` for a path the family does not wrap."""
    path = urlsplit(url).path
    for route, pattern, names in _ROUTES:
        m = pattern.match(path)
        if m:
            return route, {n: (int(g) if n == "season" else g) for n, g in zip(names, m.groups())}
    raise ValueError(f"not a VTB league route: {path!r}")


def _same(got: Any, want: Any) -> bool:
    """Type-strict equality (``True == 1`` in Python, so ``season: true`` must not pass for 1)."""
    return type(got) is type(want) and got == want


def _rows(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _check_envelope(where: str, body: Any) -> Mapping[str, Any]:
    if not isinstance(body, dict) or not isinstance(body.get("meta"), dict) or "data" not in body:
        raise AssetFetchError(f"{where}: no meta/data envelope")
    meta, data = body["meta"], body["data"]
    if not _same(meta.get("status"), "success") or not _same(meta.get("code"), 200):
        raise AssetFetchError(
            f"{where}: meta status {str(meta.get('status'))[:40]!r} code {str(meta.get('code'))[:10]!r}"
        )
    if not isinstance(data, list) or not isinstance(meta.get("count"), int) or len(data) != meta["count"]:
        raise AssetFetchError(f"{where}: data is not a list of meta.count rows")
    limit = meta.get("limit")
    if not isinstance(limit, int) or meta["count"] >= limit:
        raise AssetFetchError(
            f"{where}: possibly truncated page (count {meta.get('count')!r} reaches limit {limit!r}); ask for a larger limit"
        )
    return meta


def _check_scope(where: str, route: str, ids: Mapping[str, Any], data: List[Mapping[str, Any]]) -> None:
    if route == "/v2/leagues":
        return  # the body echoes nothing a request could be compared with
    for row in _rows(data):
        if not _same(row.get("league"), ids["league_slug"]):
            raise AssetFetchError(
                f"{where}: asked for league {ids['league_slug']!r}, got a row of {row.get('league')!r}"
            )
        if "season" in ids and not _same(row.get("season"), ids["season"]):
            raise AssetFetchError(
                f"{where}: asked for season {ids['season']}, got a row of season {row.get('season')!r}"
            )


@_rt._no_content_is_empty
def _get(url: str, params: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Any:
    """GET one wrapped route; return its envelope with the request stamped under ``_request``.

    Args:
        url: the full request URL built by the generated wrapper.
        params: query-string parameters (``limit`` on the matches route); ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.basketball.basketball_runtime._get`.

    Returns:
        ``{"meta": ..., "data": [...], "_request": {"league": ..., "season": ...}}``.

    Raises:
        ValueError: ``url`` is not a route of this family.
        NoDataError: an unknown league (HTTP 404), or a body with no rows.
        AssetFetchError: the fetch failed, the envelope is not a success, the page was cut, or a row belongs to another
            league or season than the one asked for.
    """
    route, ids = _route(url)
    where = _where(url)
    body = _bb._fetch_json(url, dict(params or {}), **kwargs)
    meta = _check_envelope(where, body)
    data = body["data"]
    if not data:
        raise NoDataError(f"{where}: the API answered no rows")
    _check_scope(where, route, ids, data)
    stamp = {k: v for k, v in ids.items()}
    return {"meta": dict(meta), "data": data, REQUEST_KEY: {**stamp, "route": route}}
