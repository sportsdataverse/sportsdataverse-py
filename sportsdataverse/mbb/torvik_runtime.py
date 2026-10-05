"""Runtime getter for the generated Bart Torvik wrappers (``torvik_*`` /
``bart_wbb_*``).

barttorvik.com serves plain data files, not JSON APIs: the T-Rank ratings and
four-factors endpoints are **CSV with a header row** (``text/csv`` or
``text/plain``), and other documented surfaces are positional JSON or HTML. The
shared no-auth runtime (:mod:`sportsdataverse._codegen_runtime`) always does
``response.json()`` and returns ``{}`` on any non-JSON body — which would
silently drop every CSV payload. This module supplies a drop-in ``_get`` that
returns the parsed JSON ``dict`` for JSON bodies and the **raw text** for
everything else, so the registered parsers receive the shape they expect.

Both ``tools/codegen/endpoints/torvik.yaml`` (men's, ``sportsdataverse.mbb``)
and ``endpoints/bart_wbb.yaml`` (women's ``/ncaaw`` mirror,
``sportsdataverse.wbb``) point ``getter_module`` here.

The interactive ``.php`` pages (``trank.php``, ``team-history.php``,
``teamsheets.php``, ``resume-compare*.php``) sit behind a JavaScript browser
challenge and are deliberately NOT wrapped — they cannot be fetched by a plain
HTTP client. See ``sdv-internal-refs/barttorvik/README.md``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from sportsdataverse._codegen_runtime import _check_response, _text_body, _transport_errors
from sportsdataverse.dl_utils import download

_UA = "Mozilla/5.0 (sportsdataverse-py; +https://py.sportsdataverse.org)"


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Union[Dict, str]:
    """GET ``url`` and return JSON (``dict``) or raw text (``str``).

    Content-type drives the shape: ``application/json`` bodies are parsed to a
    ``dict``; anything else (CSV / HTML / text) is returned as raw response
    text. ``None`` params are stripped.

    Args:
        url: Fully-qualified endpoint URL.
        params: Query parameters; ``None`` values are dropped.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        ``dict`` for JSON responses, ``str`` for CSV/HTML responses.

    Raises:
        NoDataError: barttorvik.com answered 404.
        ValueError: barttorvik.com answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx or a connection failure after retries, or
            the empty 200 barttorvik answers a blocked request with.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    headers = kwargs.pop("headers", None) or {"User-Agent": _UA}
    with _transport_errors(url):
        resp = download(url=url, params=clean, headers=headers, **kwargs)
    _check_response(resp, url)
    ctype = (resp.headers.get("content-type") or "").lower() if getattr(resp, "headers", None) else ""
    if "json" in ctype:
        try:
            return resp.json()
        except ValueError:
            pass
    return _text_body(resp, url)
