"""Runtime for the generated ESPN Fantasy wrappers (``lm-api-reads.fantasy.espn.com``, ``site.api.espn.com``).

ESPN's fantasy read API narrows list members of a body with a JSON object sent in the
``X-Fantasy-Filter`` request **header**, not a query parameter. The generated wrappers put the
caller's ``fantasy_filter`` into the params under that header's name; :func:`_get` moves it to
the header (a dict is JSON-encoded) before the request goes out.

Only public data is read. No cookie is ever sent, so a private league answers HTTP 401
(``AUTH_LEAGUE_NOT_VISIBLE``). That is raised as :class:`~sportsdataverse.errors.AssetFetchError`
naming the cause, because reading a private league needs the owner's ``espn_s2`` / ``SWID``
cookies and this package does not take them. A wrong view or filter answers 400, which the shared
runtime raises as ``ValueError``.
"""

from __future__ import annotations

import json
from http.cookiejar import DefaultCookiePolicy
from typing import Any

import requests

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError

#: The request header that carries the filter object.
FILTER_HEADER = "X-Fantasy-Filter"

# No custom User-Agent: site.api.espn.com answers 403 to a browser-style UA carrying a URL (measured
# 2026-11-26), and both hosts accept the requests default.


#: This family's own HTTP session. It refuses every cookie, so no cookie set by another ESPN host in the same
#: process (the shared download session keeps a jar) can ever be replayed to the fantasy hosts.
SESSION = requests.Session()
SESSION.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))


def _transport(url: str, **kwargs: Any) -> Any:
    kwargs.setdefault("session", SESSION)
    resp = download(url=url, **kwargs)
    if getattr(resp, "status_code", None) == 401:
        raise AssetFetchError(
            f"{url} answered HTTP 401: the league is private. Only public leagues (for example the ones "
            "espn_fantasy_league_directory lists) can be read without the owner's cookies."
        )
    return resp


def _build_query(params: dict[str, Any] | None) -> dict[str, Any]:
    """The query string the host sees: every param but the filter, which travels as a header.

    Codegen calls this too, so a reference page's Example URL is the URL really requested.
    """
    return {k: v for k, v in (params or {}).items() if k != FILTER_HEADER}


def _get(url: str, params: dict[str, Any] | None = None, **kwargs: Any) -> Any:
    """GET ``url`` with the ``X-Fantasy-Filter`` param moved into the request header.

    Args:
        url: Endpoint URL.
        params: Query parameters; ``None`` values are dropped. A ``"X-Fantasy-Filter"`` entry (a JSON
            string or a dict) becomes the header.
        **kwargs: Forwarded to :func:`sportsdataverse._codegen_runtime._get`; a caller's ``headers``
            win key by key.

    Returns:
        The decoded JSON body.

    Raises:
        NoDataError: the host answered 404 (unknown game, season or id).
        ValueError: the host answered 400 (a bad view, filter or id).
        AssetFetchError: a private league (401), or any other non-2xx answer after retries.
    """
    flt = (params or {}).get(FILTER_HEADER)
    query = _build_query(params)
    headers = {"Accept": "application/json"}
    if flt is not None:
        headers[FILTER_HEADER] = flt if isinstance(flt, str) else json.dumps(flt, separators=(",", ":"))
    headers.update(kwargs.pop("headers", None) or {})
    # League bodies change daily and the response cache keys ignore the filter header: never cache.
    kwargs.setdefault("cache_ttl", 0)
    return _rt._get(url, query, headers=headers, transport=_transport, **kwargs)
