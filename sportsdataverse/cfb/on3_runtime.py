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
  :class:`~sportsdataverse.errors.NoDataError`). ``_scrape_get`` treats that
  as "re-discover the buildId and retry once", so a deploy mid-process degrades
  to one extra page fetch instead of an error.

The data route also **requires** ``rankingType`` / ``sport`` / ``year`` as
query parameters (it 404s without them); ``_scrape_get`` derives them from the
resolved path so the shim wrapper signatures stay positional.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from sportsdataverse._codegen_runtime import _json_body, _text_body, _transport_errors, _where
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

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


def _discover_build_id(page_url: str, **kwargs: Any) -> str:
    """Fetch ``page_url`` and extract the current Next.js buildId.

    Args:
        page_url: an on3.com page expected to render (the rankings page that
            corresponds to the data route being requested).
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The current buildId.

    Raises:
        NoDataError: the page answered 404 -- the ranking does not exist.
        AssetFetchError: the page answered any other non-2xx or a connection failure
            outlived the retries, or a 2xx page carries no buildId (a bot-challenge
            interstitial is a failed fetch, not an empty ranking).
    """
    headers = {**_headers(), **kwargs.pop("headers", {})}
    with _transport_errors(page_url):
        resp = download(url=page_url, headers=headers, **kwargs)
    build_id = _extract_build_id(_text_body(resp, page_url))
    if build_id is None:
        raise AssetFetchError(f"{_where(page_url)}: page carries no Next.js buildId (a challenge page?)")
    return build_id


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
        The parsed JSON ``dict`` or ``list``; ``{}`` for a 204/205.

    Raises:
        NoDataError: the route answered 404.
        ValueError: the RDB answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx or a connection failure after retries, or
            a 2xx whose body is empty (not 204/205) or not JSON -- the answer is
            unknown, not empty.
    """
    headers = {**_headers(), **kwargs.pop("headers", {})}
    query = {k: v for k, v in (params or {}).items() if v is not None}
    with _transport_errors(url):
        resp = download(url=url, params=query, headers=headers, **kwargs)
    body = _json_body(resp, url)
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
        The parsed JSON ``dict`` (``{"pageProps": {...}}``), or ``{}`` for a path
        shape no On3 route family matches.

    Raises:
        NoDataError: the ranking page 404s, or the data route 404s again after a
            buildId refresh (or the buildId did not change) -- the ranking does not
            exist.
        ValueError: On3 answered 400 / 422 -- the request is wrong.
        AssetFetchError: the page or data route answered any other non-2xx or a
            connection failure outlived the retries, the page carries no buildId,
            or a 2xx data body is empty or not JSON.
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

    for attempt in range(2):
        data_url = f"{_HOST}/_next/data/{_build_id}{path}"
        try:
            with _transport_errors(data_url):
                resp = download(url=data_url, params=query, headers=headers, **kwargs)
        except NoDataError:
            # The FIRST 404 on the data route means the buildId rotated (On3
            # deployed): refresh once. A second 404, or an UNCHANGED buildId, is
            # authoritative -- the resource genuinely does not exist.
            if attempt == 1:
                raise
            stale = _build_id
            _build_id = _discover_build_id(page_url, headers=headers, **kwargs)
            if _build_id == stale:
                raise
            continue
        body = _json_body(resp, data_url)
        return body if isinstance(body, dict) else {}
    raise AssertionError("unreachable: the second attempt returns or raises")
