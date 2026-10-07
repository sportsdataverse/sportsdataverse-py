"""Runtime for the generated Football-Data.co.uk wrappers.

football-data.co.uk is a static **CSV** archive, not a JSON API. The shared
no-auth runtime (:mod:`sportsdataverse._codegen_runtime`) always does
``response.json()`` and answers ``{}`` on a non-JSON body, which would silently
drop every payload. This is the ``torvik_runtime`` pattern: return the parsed
JSON ``dict`` for a JSON body and the **raw text** for everything else, so
``parse_football_data`` receives the CSV it expects.

Courtesy: the site is a static archive on shared hosting -- stay at roughly one
request per second.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from sportsdataverse._codegen_runtime import (
    _check_response,
    _excerpt,
    _json_body,
    _text_body,
    _transport_errors,
    _where,
)
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError

_UA = "Mozilla/5.0 (sportsdataverse-py; +https://py.sportsdataverse.org)"


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Union[Dict, str]:
    """GET ``url`` and return JSON (``dict``) or raw CSV/text (``str``).

    Args:
        url: Fully-qualified file URL.
        params: Query parameters; ``None`` values are dropped.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        ``dict`` for a JSON response, ``str`` for a CSV/text response.

    Raises:
        NoDataError: football-data.co.uk answered 404 (no such season/division file).
        ValueError: the host answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx or a connection failure after retries, or an HTML
            body (this host answers an error page with HTTP 200, and HTML is never CSV).
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    headers = kwargs.pop("headers", None) or {"User-Agent": _UA}
    with _transport_errors(url):
        resp = download(url=url, params=clean, headers=headers, **kwargs)
    _check_response(resp, url)
    ctype = (resp.headers.get("content-type") or "").lower() if getattr(resp, "headers", None) else ""
    if "json" in ctype:
        # A JSON-labelled body that will not decode is a failed fetch, never text to parse.
        return _json_body(resp, url)
    if "html" in ctype:
        # Every legitimate body here is text/csv or text/plain, so HTML is an error page or an
        # interstitial -- and this host answers those with 200, not 404. Passed through as text it
        # parses to a 2x1 frame whose one column is ``<!doctype html>``: a failed fetch wearing
        # data's clothes. (This is where the torvik precedent must NOT be followed -- barttorvik
        # serves HTML as a legitimate shape, this host never does.)
        raise AssetFetchError(
            f"{_where(url)} answered HTTP {getattr(resp, 'status_code', 200)} with an HTML body "
            f"instead of CSV: {_excerpt(getattr(resp, 'text', '') or '')}"
        )
    return _text_body(resp, url)
