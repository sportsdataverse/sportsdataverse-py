"""Runtime getter for the generated ``on3`` wrappers.

**Primary path — the On3 Recruit Database (RDB).** The generated wrappers hit
the open, read-only, **auth-free** public gateway::

    https://api.on3.com/public/rdb/v1/...   (and one /rdb/v2/... route)

``_get`` is therefore a plain ``requests``-via-:func:`sportsdataverse.dl_utils.download`
GET with a browser UA: no buildId, no JWT, no query derivation. ``url`` arrives
already fully-built by the wrapper (``host`` + substituted ``path``); ``_get``
drops ``None``-valued params and returns the parsed JSON body, which the RDB
serves as either a ``dict`` (paged / single object) or a bare ``list``.

**Fallback path — the legacy On3 rankings scrape (``_scrape_get``).** Before the
RDB, the only public JSON surface was on3.com's Next.js data route::

    https://www.on3.com/_next/data/{buildId}/rivals/rankings/{rankingType}/{sport}/{year}.json

That machinery (buildId discovery + stale-buildId retry) is retained under
:func:`_scrape_get` and is used **only** by the 4 deprecated rankings shim
wrappers in :mod:`sportsdataverse.cfb.on3_rankings`, which keep working for
continuity. The RDB natives are the forward path.

``_scrape_get`` mechanics (unchanged from the pre-retarget ``_get``):

* **buildId discovery** — the ``{buildId}`` segment rotates on every On3
  deploy. It is scraped from the ``__NEXT_DATA__`` blob of the corresponding
  rankings HTML page and cached at module level for the process lifetime.
* **stale-buildId retry** — a rotated buildId makes the data route return
  HTTP 404 (which :func:`sportsdataverse.dl_utils.download` surfaces as
  :class:`~sportsdataverse.errors.NoESPNDataError`). ``_scrape_get`` treats that
  as "re-discover the buildId and retry once", so a deploy mid-process degrades
  to one extra page fetch instead of an error.

The data route also **requires** ``rankingType`` / ``sport`` / ``year`` as
query parameters (it 404s without them); ``_scrape_get`` derives them from the
resolved path so the shim wrapper signatures stay positional.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import NoESPNDataError

_HOST = "https://www.on3.com"
_BUILD_ID_RE = re.compile(r'"buildId":"([A-Za-z0-9_-]+)"')
_RANKINGS_PATH_RE = re.compile(r"^/rivals/rankings/([a-z0-9-]+)/([a-z0-9-]+)/([0-9]{4})\.json$")
# On3 serves plain requests fine but a browser UA keeps us off the generic-bot path.
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# Process-lifetime cache; refreshed automatically when a data-route 404 signals
# that On3 deployed (buildId rotated).
_build_id: Optional[str] = None


def _headers() -> Dict[str, str]:
    """Default request headers (browser UA) for on3.com fetches."""
    return {"User-Agent": _UA}


def _extract_build_id(text: str) -> Optional[str]:
    """Pull the Next.js ``buildId`` out of a rendered on3.com page.

    Args:
        text: HTML of any Next-rendered on3.com page (every page embeds the
            ``__NEXT_DATA__`` JSON blob, which carries ``"buildId":"..."``).

    Returns:
        The buildId string, or ``None`` when the marker is absent.
    """
    m = _BUILD_ID_RE.search(text or "")
    return m.group(1) if m else None


def _discover_build_id(page_url: str, **kwargs: Any) -> Optional[str]:
    """Fetch ``page_url`` and extract the current Next.js buildId.

    Args:
        page_url: an on3.com page expected to render (the rankings page that
            corresponds to the data route being requested).
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The current buildId, or ``None`` when the page 404s / carries no blob.
    """
    headers = {**_headers(), **kwargs.pop("headers", {})}
    try:
        resp = download(url=page_url, headers=headers, **kwargs)
    except NoESPNDataError:
        return None
    return _extract_build_id(getattr(resp, "text", "") or "")


def _get(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
    """GET an ``api.on3.com`` RDB route and return its parsed JSON (dict or list).

    The RDB ``/public/`` gateway is read-only and auth-free — no buildId, no JWT.
    ``url`` is already the full ``https://api.on3.com/public/rdb/v{1,2}/...`` route
    built by the generated wrapper; ``params`` are query args (``None``-valued
    dropped). The RDB serves both ``dict`` (paged / single object) and bare
    ``list`` bodies, so the return type is ``Any``.

    Args:
        url: full RDB route URL built by the generated wrapper.
        params: query parameters; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The parsed JSON ``dict`` or ``list``; ``{}`` when the route is
        unreachable (``NoESPNDataError``) or the body is not JSON.
    """
    headers = {**_headers(), **kwargs.pop("headers", {})}
    query = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        resp = download(url=url, params=query, headers=headers, **kwargs)
    except NoESPNDataError:
        return {}
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, (dict, list)) else {}


def _scrape_get(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict:
    """GET an on3.com Next.js data route and return its JSON body.

    ``url`` arrives from the generated wrapper as the *logical* route
    (``https://www.on3.com/rivals/rankings/{rankingType}/{sport}/{year}.json``);
    this getter injects the current ``/_next/data/{buildId}`` prefix, adds the
    required ``rankingType``/``sport``/``year`` query parameters derived from
    the path, and retries once with a re-discovered buildId when On3 has
    deployed since the cached one was scraped.

    Args:
        url: logical data-route URL built by the generated wrapper.
        params: extra query parameters (``page``, site filter passthroughs);
            ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The parsed JSON ``dict`` (``{"pageProps": {...}}``), or ``{}`` when the
        route cannot be resolved (unknown path shape, unreachable page, or a
        payload that is not JSON).
    """
    global _build_id

    path = url[len(_HOST) :] if url.startswith(_HOST) else url
    m = _RANKINGS_PATH_RE.match(path)
    if not m:
        # ponytail: only the rankings route family exists today; extend the
        # regex (or add a per-family mapping) when more On3 routes are wrapped.
        return {}
    ranking_type, sport, year = m.groups()
    page_url = f"{_HOST}/db/rankings/{ranking_type}/{sport}/{year}/"
    headers = {**_headers(), **kwargs.pop("headers", {})}
    # Path-derived values spread LAST: the path is the source of truth for the
    # three parameters the route 404s without — a stray caller value must not
    # desynchronize query from path.
    query: Dict[str, Any] = {
        **{k: v for k, v in (params or {}).items() if v is not None},
        "rankingType": ranking_type,
        "sport": sport,
        "year": year,
    }

    if _build_id is None:
        _build_id = _discover_build_id(page_url, headers=headers, **kwargs)
        if _build_id is None:
            return {}

    for attempt in range(2):
        data_url = f"{_HOST}/_next/data/{_build_id}{path}"
        try:
            resp = download(url=data_url, params=query, headers=headers, **kwargs)
        except NoESPNDataError:
            if attempt == 1:
                return {}
            # 404 on the data route == buildId rotated (On3 deployed). Refresh once;
            # an UNCHANGED buildId means the 404 is authoritative (the resource
            # genuinely doesn't exist) — don't burn a second data fetch on it.
            stale = _build_id
            _build_id = _discover_build_id(page_url, headers=headers, **kwargs)
            if _build_id is None or _build_id == stale:
                return {}
            continue
        try:
            body = resp.json()
        except ValueError:
            return {}
        return body if isinstance(body, dict) else {}
    return {}
