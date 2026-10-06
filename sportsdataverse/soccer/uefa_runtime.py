"""Runtime getter for the generated ``uefa`` wrappers (UEFA front-end APIs).

The four keyless hosts (``comp`` / ``match`` / ``standings`` / ``matchstats``
``.uefa.com``) sit behind Akamai, which answers ``403 Access Denied`` to the
shared runtime's default ``python-requests`` User-Agent on every route and ``200``
to the same request carrying a browser User-Agent (probed 2026-10-06 from the
droplet). This thin wrapper therefore sends a browser User-Agent plus the
``Accept`` / ``Origin`` pair the reference captures were taken with, and otherwise
defers to :func:`sportsdataverse.dl_utils.download` -- same retry budget, same
error vocabulary, no second HTTP path. A caller's ``headers=`` wins key-by-key.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from sportsdataverse._codegen_runtime import _json_body, _transport_errors
from sportsdataverse.dl_utils import download

__all__ = ["UEFA_HEADERS", "_get"]

UEFA_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://www.uefa.com",
}


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Union[Dict[str, Any], List[Any]]:
    """GET ``url`` as JSON with the browser headers the UEFA edge requires.

    Args:
        url: fully-built request URL (host + substituted path).
        params: query-string parameters; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`. A
            ``headers`` mapping is merged over :data:`UEFA_HEADERS`.

    Returns:
        The parsed JSON body -- a ``list`` for every captured UEFA route.

    Raises:
        sportsdataverse.errors.NoDataError: the host answered 404.
        ValueError: the host answered 400 / 422 -- the request is wrong.
        sportsdataverse.errors.AssetFetchError: any other non-2xx (a 403 means the
            edge rejected the client) or a connection failure after retries, or a
            2xx whose body is empty or not JSON.

    Example:
        Basic use::

            from sportsdataverse.soccer.uefa_runtime import _get

            body = _get("https://comp.uefa.com/v2/competitions", {"competitionIds": "1"})
            print(body[0]["code"])
    """
    headers = {**UEFA_HEADERS, **(kwargs.pop("headers", None) or {})}
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    with _transport_errors(url):
        resp = download(url=url, params=clean, headers=headers, **kwargs)
    return _json_body(resp, url)
